import array
import fcntl
import hashlib
import os
import signal
from pathlib import Path
import socket
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

from ops.authority_handoff import offer,HandoffHeld,DockerProcessAttestor
from ops.pr197_operation_fence import OperationFence
from processing.authority_lease import acquire_transferred,LeaseUnavailable


@unittest.skipUnless(os.name=='posix' and hasattr(os,'pidfd_open') and hasattr(socket,'SO_PEERCRED'),'Linux FD fixture required')
class AuthorityTransferTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.state=self.root/'state';self.state.mkdir(mode=0o700)
        self.leases=self.root/'leases';self.leases.mkdir(mode=0o700)
        self.authority='12345678-1234-1234-1234-123456789abc'
        (self.state/'coordinator.lock').touch(mode=0o600)
        (self.leases/(self.authority+'.lease')).touch(mode=0o600)
        db=sqlite3.connect(self.state/'jobs.sqlite3')
        db.execute('CREATE TABLE coordinator_identity(singleton INTEGER PRIMARY KEY, authority_id TEXT)')
        db.execute('INSERT INTO coordinator_identity VALUES(1,?)',(self.authority,));db.commit();db.close()
        self.mount=patch('ops.pr197_stopped_census._mount');self.mount.start();self.addCleanup(self.mount.stop)
        self.attestor=DockerProcessAttestor('a'*64,'sha256:'+'b'*64,self.root)
        self.attestor_patch=patch.object(self.attestor,'inspect',side_effect=lambda pid:123 if pid==os.getpid() else (_ for _ in ()).throw(ValueError()))
        self.attestor_patch.start();self.addCleanup(self.attestor_patch.stop);self.operation='c'*32
        self.intent=self.leases/(self.authority+'.handoff-intent')

    def test_fd_transfer_retains_lock_and_durable_ack(self):
        left,right=socket.socketpair();self.addCleanup(left.close);self.addCleanup(right.close)
        result=[];errors=[]
        with OperationFence(self.state,self.leases,self.authority,enabled=True) as fence:
            def send():
                try:result.append(offer(left,fence,self.attestor,self.intent,operation_id=self.operation,expected_uid=os.geteuid()))
                except Exception as exc:errors.append(type(exc).__name__)
            thread=threading.Thread(target=send);thread.start()
            lease=acquire_transferred(right,self.leases,self.state,supervisor_uid=os.geteuid(),operation_id=self.operation,
                container_id=self.attestor.container,image_id=self.attestor.image,experimental=True)
            thread.join(7);self.assertFalse(thread.is_alive());self.assertEqual(errors,[])
            self.assertEqual(result[0]['state'],'handoff_outcome_unknown')
            self.assertTrue(self.intent.exists());self.assertTrue(Path(str(self.intent)+'.ack').exists())
            fence.check();lease.close()

    def test_lost_ack_holds_original_fence_and_never_retries(self):
        left,right=socket.socketpair();self.addCleanup(left.close)
        errors=[]
        with OperationFence(self.state,self.leases,self.authority,enabled=True) as fence:
            def send():
                try:offer(left,fence,self.attestor,self.intent,operation_id=self.operation,expected_uid=os.geteuid())
                except Exception as exc:errors.append(type(exc).__name__)
            thread=threading.Thread(target=send);thread.start()
            _,ancillary,_,_=right.recvmsg(4100,socket.CMSG_SPACE(array.array('i').itemsize))
            for _level,_kind,data in ancillary:
                fds=array.array('i');fds.frombytes(data[:len(data)-len(data)%fds.itemsize])
                for fd in fds:os.close(fd)
            right.close();thread.join(7)
            self.assertFalse(thread.is_alive());self.assertEqual(errors,['HandoffHeld'])
            self.assertTrue(self.intent.exists());self.assertFalse(Path(str(self.intent)+'.ack').exists())
            fence.check()
            again,other=socket.socketpair()
            try:
                with self.assertRaises(HandoffHeld):
                    offer(again,fence,self.attestor,self.intent,operation_id=self.operation,expected_uid=os.geteuid())
            finally:again.close();other.close()
            probe=os.open(self.leases/(self.authority+'.lease'),os.O_RDONLY)
            try:
                with self.assertRaises(BlockingIOError):fcntl.flock(probe,fcntl.LOCK_EX|fcntl.LOCK_NB)
            finally:os.close(probe)

    def test_host_attestor_rejects_container_pid_and_cgroup_mismatches(self):
        from types import SimpleNamespace
        attestor=DockerProcessAttestor('a'*64,'sha256:'+'b'*64,self.root)
        good='"'+attestor.container+'" "'+attestor.image+'" 123 true\n'
        with patch('ops.authority_handoff.subprocess.run',return_value=SimpleNamespace(stdout=good)), \
             patch('ops.authority_handoff._starttime',return_value=456), \
             patch('pathlib.Path.read_text',return_value='0::/docker/'+attestor.container+'\n'):
            self.assertEqual(attestor.inspect(123),456)
            with self.assertRaises(HandoffHeld):attestor.inspect(124)
        with patch('ops.authority_handoff.subprocess.run',return_value=SimpleNamespace(stdout=good)), \
             patch('pathlib.Path.read_text',return_value='0::/docker/'+'c'*64+'\n'):
            with self.assertRaises(HandoffHeld):attestor.inspect(123)

    def test_truncated_many_descriptors_are_closed(self):
        left,right=socket.socketpair();self.addCleanup(left.close);self.addCleanup(right.close)
        original=os.open(self.leases/(self.authority+'.lease'),os.O_RDONLY)
        self.addCleanup(os.close,original)
        before=len(os.listdir('/proc/self/fd'))
        left.sendmsg([b'bad!'],[(socket.SOL_SOCKET,socket.SCM_RIGHTS,array.array('i',[original]*8))])
        with self.assertRaises(LeaseUnavailable):
            acquire_transferred(right,self.leases,self.state,supervisor_uid=os.geteuid(),operation_id=self.operation,
                container_id=self.attestor.container,image_id=self.attestor.image,experimental=True)
        self.assertEqual(len(os.listdir('/proc/self/fd')),before)

    def test_fragmented_ack_is_read_to_exact_boundary(self):
        left,right=socket.socketpair();self.addCleanup(left.close);self.addCleanup(right.close)
        result=[];errors=[]
        with OperationFence(self.state,self.leases,self.authority,enabled=True) as fence:
            def send():
                try:result.append(offer(left,fence,self.attestor,self.intent,operation_id=self.operation,expected_uid=os.geteuid()))
                except Exception as exc:errors.append(type(exc).__name__)
            thread=threading.Thread(target=send);thread.start()
            data,ancillary,_,_=right.recvmsg(4100,socket.CMSG_SPACE(array.array('i').itemsize))
            size=int.from_bytes(data[:4],'big')
            while len(data)<size+4:data+=right.recv(size+4-len(data))
            digest=hashlib.sha256(data[4:]).hexdigest().encode()+b'\n'
            right.sendall(digest[:10]);right.sendall(digest[10:])
            self.assertEqual(right.recv(3),b'GO\n')
            for _level,_kind,value in ancillary:
                fds=array.array('i');fds.frombytes(value[:len(value)-len(value)%fds.itemsize])
                for fd in fds:os.close(fd)
            thread.join(7);self.assertFalse(thread.is_alive());self.assertEqual(errors,[])
            self.assertEqual(result[0]['state'],'handoff_outcome_unknown');fence.check()

    @unittest.skipUnless(hasattr(os,'fork'),'separate-process fixture required')
    def test_descriptor_transfers_to_separate_process(self):
        path=str(self.root/'broker.sock')
        listener=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);listener.bind(path);listener.listen(1);listener.settimeout(5)
        self.addCleanup(listener.close)
        with OperationFence(self.state,self.leases,self.authority,enabled=True) as fence:
            pid=os.fork()
            if pid==0:
                signal.alarm(10)
                try:
                    for fd in fence.fds:os.close(fd)
                    sock=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);sock.connect(path);listener.close()
                    lease=acquire_transferred(sock,self.leases,self.state,supervisor_uid=os.geteuid(),operation_id=self.operation,
                        container_id=self.attestor.container,image_id=self.attestor.image,experimental=True)
                    lease.close();sock.close();os._exit(0)
                except BaseException:os._exit(1)
            reaped=False
            try:
                conn,_=listener.accept()
                with conn,patch.object(self.attestor,'inspect',return_value=123):
                    result=offer(conn,fence,self.attestor,self.intent,operation_id=self.operation,expected_uid=os.geteuid())
                _,status=os.waitpid(pid,0);reaped=True
                self.assertEqual(os.waitstatus_to_exitcode(status),0)
                self.assertEqual(result['state'],'handoff_outcome_unknown');fence.check()
            except BaseException:
                if not reaped:
                    os.kill(pid,signal.SIGKILL);os.waitpid(pid,0)
                raise
