import array
import errno
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

from ops.authority_handoff import offer,pending_status,HandoffHeld,DockerProcessAttestor
from ops.pr197_operation_fence import OperationFence,OperationFenceHeld
from processing.authority_lease import acquire_transferred,AuthorityLease,LeaseUnavailable


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
            status=pending_status(self.leases,self.authority,self.attestor)
            self.assertEqual((status['state'],status['ack'],status['lease']),('held','present','held'))
            self.assertFalse(status['startAuthorized']);self.assertFalse(status['cleanupAuthorized'])
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
        # Once the supervisor exits, the durable intent still blocks ordinary
        # startup on the newer binary even though the flock is available.
        with self.assertRaisesRegex(LeaseUnavailable,'authority_handoff_requires_reconciliation'):
            AuthorityLease(self.leases,self.authority)
        with self.assertRaisesRegex(OperationFenceHeld,'unresolved_handoff_requires_reconciliation'):
            with OperationFence(self.state,self.leases,self.authority,enabled=True):pass
        self.assertTrue(self.intent.exists())
        with patch.object(self.attestor,'inspect',side_effect=HandoffHeld('synthetic offline')):
            status=pending_status(self.leases,self.authority,self.attestor)
        self.assertEqual((status['state'],status['ack'],status['lease'],status['candidate']),('held','absent','free','unverified'))
        self.assertFalse(status['retryAuthorized'])

    def test_pending_status_rejects_tampered_ack(self):
        left,right=socket.socketpair();self.addCleanup(left.close);self.addCleanup(right.close)
        with OperationFence(self.state,self.leases,self.authority,enabled=True) as fence:
            errors=[]
            def send():
                try:offer(left,fence,self.attestor,self.intent,operation_id=self.operation,expected_uid=os.geteuid())
                except Exception as exc:errors.append(type(exc).__name__)
            thread=threading.Thread(target=send);thread.start()
            _,ancillary,_,_=right.recvmsg(4100,socket.CMSG_SPACE(array.array('i').itemsize))
            for _level,_kind,data in ancillary:
                fds=array.array('i');fds.frombytes(data[:len(data)-len(data)%fds.itemsize])
                for fd in fds:os.close(fd)
            right.close();thread.join(7);self.assertFalse(thread.is_alive());self.assertEqual(errors,['HandoffHeld'])
            ack=Path(str(self.intent)+'.ack');ack.write_bytes(b'not-a-valid-ack');ack.chmod(0o600)
            with self.assertRaisesRegex(HandoffHeld,'pending_ack_invalid'):
                pending_status(self.leases,self.authority,self.attestor)

    def test_pending_status_rejects_ack_create_remove_or_replacement_during_probe(self):
        import json
        info=(self.leases/(self.authority+'.lease')).stat()
        value=dict(version=1,operationId=self.operation,authorityId=self.authority,pid=os.getpid(),startTime=123,
            containerId=self.attestor.container,imageId=self.attestor.image,leaseDevice=info.st_dev,leaseInode=info.st_ino)
        raw=json.dumps(value,sort_keys=True,separators=(',',':')).encode()
        ack=Path(str(self.intent)+'.ack');payload=hashlib.sha256(raw).hexdigest().encode()+b'\n'
        for change in ('create','remove','replace'):
            with self.subTest(change=change):
                self.intent.write_bytes(raw);self.intent.chmod(0o600)
                if change!='create':ack.write_bytes(payload);ack.chmod(0o600)
                def mutate(_pid):
                    if change=='create':ack.write_bytes(payload);ack.chmod(0o600)
                    elif change=='remove':ack.unlink()
                    else:
                        replacement=self.root/'replacement';replacement.write_bytes(payload);replacement.chmod(0o600)
                        os.replace(replacement,ack)
                    return 123
                with patch.object(self.attestor,'inspect',side_effect=mutate):
                    with self.assertRaisesRegex(HandoffHeld,'handoff_ack_changed'):
                        pending_status(self.leases,self.authority,self.attestor)
                self.intent.unlink();ack.unlink(missing_ok=True)

    def test_marker_created_after_first_check_refuses_and_closes_lease(self):
        real=fcntl.flock;triggered=[]
        def race(fd,mode):
            real(fd,mode)
            if not triggered:
                triggered.append(True);self.intent.write_bytes(b'synthetic-uncertain-intent')
        with patch('fcntl.flock',side_effect=race):
            with self.assertRaisesRegex(LeaseUnavailable,'authority_handoff_requires_reconciliation'):
                AuthorityLease(self.leases,self.authority)
        self.assertTrue(self.intent.exists())
        probe=os.open(self.leases/(self.authority+'.lease'),os.O_RDONLY)
        try:real(probe,fcntl.LOCK_EX|fcntl.LOCK_NB)
        finally:os.close(probe)

    def test_orphan_ack_alone_also_blocks_startup(self):
        Path(str(self.intent)+'.ack').write_bytes(b'synthetic-ack')
        with self.assertRaisesRegex(LeaseUnavailable,'authority_handoff_requires_reconciliation'):
            AuthorityLease(self.leases,self.authority)

    def test_marker_stat_errors_fail_closed(self):
        original=Path.lstat
        for error in (errno.EIO,errno.EACCES):
            def fail(path,*args,**kwargs):
                if path==self.intent:raise OSError(error,'synthetic stat failure')
                return original(path,*args,**kwargs)
            with self.subTest(error=error),patch.object(Path,'lstat',fail):
                with self.assertRaisesRegex(LeaseUnavailable,'authority_handoff_marker_unavailable'):
                    AuthorityLease(self.leases,self.authority)

    def test_marker_stat_error_after_flock_closes_descriptor(self):
        original=Path.lstat;calls=[]
        def fail(path,*args,**kwargs):
            if path==self.intent:
                calls.append(True)
                if len(calls)==2:raise OSError(errno.EIO,'synthetic stat failure')
            return original(path,*args,**kwargs)
        with patch.object(Path,'lstat',fail):
            with self.assertRaisesRegex(LeaseUnavailable,'authority_handoff_marker_unavailable'):
                AuthorityLease(self.leases,self.authority)
        fd=os.open(self.leases/(self.authority+'.lease'),os.O_RDONLY)
        try:fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        finally:os.close(fd)

    @unittest.skipUnless(hasattr(os,'fork'),'separate-process fixture required')
    def test_killed_supervisor_after_intent_blocks_newer_startup(self):
        from ops import authority_handoff
        left,right=socket.socketpair();self.addCleanup(left.close);self.addCleanup(right.close)
        child=os.fork()
        if child==0:
            signal.alarm(10)
            try:
                original=authority_handoff._write_once
                def die(path,payload):
                    original(path,payload);os.kill(os.getpid(),signal.SIGKILL)
                with OperationFence(self.state,self.leases,self.authority,enabled=True) as fence, \
                     patch.object(authority_handoff,'_write_once',side_effect=die), \
                     patch.object(self.attestor,'inspect',return_value=123):
                    offer(left,fence,self.attestor,self.intent,operation_id=self.operation,expected_uid=os.geteuid())
            finally:os._exit(3)
        _,status=os.waitpid(child,0)
        self.assertTrue(os.WIFSIGNALED(status));self.assertTrue(self.intent.exists())
        with self.assertRaisesRegex(LeaseUnavailable,'authority_handoff_requires_reconciliation'):
            AuthorityLease(self.leases,self.authority)
        with self.assertRaisesRegex(OperationFenceHeld,'unresolved_handoff_requires_reconciliation'):
            with OperationFence(self.state,self.leases,self.authority,enabled=True):pass

    @unittest.skipUnless(hasattr(os,'fork'),'separate-process fixture required')
    def test_supervisor_death_after_go_preserves_candidate_lock_and_blocks_restart(self):
        left,right=socket.socketpair();ready_r,ready_w=os.pipe();release_r,release_w=os.pipe()
        candidate=os.fork()
        if candidate==0:
            signal.alarm(15);left.close();os.close(ready_r);os.close(release_w)
            try:
                lease=acquire_transferred(right,self.leases,self.state,supervisor_uid=os.geteuid(),operation_id=self.operation,
                    container_id=self.attestor.container,image_id=self.attestor.image,experimental=True)
                os.write(ready_w,b'1');os.read(release_r,1);lease.close();os._exit(0)
            except BaseException:os._exit(2)
        supervisor=os.fork()
        if supervisor==0:
            signal.alarm(15);right.close();os.close(ready_r);os.close(ready_w);os.close(release_r);os.close(release_w)
            try:
                with OperationFence(self.state,self.leases,self.authority,enabled=True) as fence, \
                     patch.object(self.attestor,'inspect',return_value=123):
                    offer(left,fence,self.attestor,self.intent,operation_id=self.operation,expected_uid=os.geteuid())
                    os.kill(os.getpid(),signal.SIGKILL)
            finally:os._exit(3)
        left.close();right.close();os.close(ready_w);os.close(release_r)
        supervisor_reaped=False;candidate_reaped=False
        try:
            _,status=os.waitpid(supervisor,0);supervisor_reaped=True;self.assertTrue(os.WIFSIGNALED(status))
            self.assertEqual(os.read(ready_r,1),b'1')
            probe=os.open(self.leases/(self.authority+'.lease'),os.O_RDONLY)
            try:
                with self.assertRaises(BlockingIOError):fcntl.flock(probe,fcntl.LOCK_EX|fcntl.LOCK_NB)
            finally:os.close(probe)
            with self.assertRaisesRegex(LeaseUnavailable,'authority_handoff_requires_reconciliation'):
                AuthorityLease(self.leases,self.authority)
            with self.assertRaisesRegex(OperationFenceHeld,'unresolved_handoff_requires_reconciliation'):
                with OperationFence(self.state,self.leases,self.authority,enabled=True):pass
            self.assertTrue(Path(str(self.intent)+'.ack').exists())
            os.write(release_w,b'1');_,status=os.waitpid(candidate,0);candidate_reaped=True
            self.assertEqual(os.waitstatus_to_exitcode(status),0)
        finally:
            os.close(ready_r);os.close(release_w)
            for pid,reaped in ((supervisor,supervisor_reaped),(candidate,candidate_reaped)):
                if not reaped:
                    try:os.kill(pid,signal.SIGKILL)
                    except ProcessLookupError:pass
                    os.waitpid(pid,0)

    @unittest.skipUnless(hasattr(os,'fork'),'separate-process fixture required')
    def test_supervisor_death_before_go_leaves_durable_hold(self):
        from ops import authority_handoff
        left,right=socket.socketpair();candidate=os.fork()
        if candidate==0:
            signal.alarm(15);left.close()
            try:
                acquire_transferred(right,self.leases,self.state,supervisor_uid=os.geteuid(),operation_id=self.operation,
                    container_id=self.attestor.container,image_id=self.attestor.image,experimental=True)
            except LeaseUnavailable:os._exit(0)
            except BaseException:os._exit(2)
            os._exit(3)
        supervisor=os.fork()
        if supervisor==0:
            signal.alarm(15);right.close();original=authority_handoff._write_once
            def die(path,payload):
                original(path,payload)
                if str(path).endswith('.ack'):os.kill(os.getpid(),signal.SIGKILL)
            try:
                with OperationFence(self.state,self.leases,self.authority,enabled=True) as fence, \
                     patch.object(self.attestor,'inspect',return_value=123), \
                     patch.object(authority_handoff,'_write_once',side_effect=die):
                    offer(left,fence,self.attestor,self.intent,operation_id=self.operation,expected_uid=os.geteuid())
            finally:os._exit(4)
        left.close();right.close();reaped=set()
        try:
            _,status=os.waitpid(supervisor,0);reaped.add(supervisor);self.assertTrue(os.WIFSIGNALED(status))
            _,status=os.waitpid(candidate,0);reaped.add(candidate);self.assertEqual(os.waitstatus_to_exitcode(status),0)
            self.assertTrue(self.intent.exists());self.assertTrue(Path(str(self.intent)+'.ack').exists())
            with self.assertRaisesRegex(LeaseUnavailable,'authority_handoff_requires_reconciliation'):
                AuthorityLease(self.leases,self.authority)
            with self.assertRaisesRegex(OperationFenceHeld,'unresolved_handoff_requires_reconciliation'):
                with OperationFence(self.state,self.leases,self.authority,enabled=True):pass
        finally:
            for pid in (supervisor,candidate):
                if pid not in reaped:
                    try:os.kill(pid,signal.SIGKILL)
                    except ProcessLookupError:pass
                    os.waitpid(pid,0)

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
