import array
import fcntl
import os
from pathlib import Path
import signal
import socket
import tempfile
import unittest


@unittest.skipUnless(os.name=='posix' and hasattr(os,'fork') and hasattr(socket,'SCM_RIGHTS'),
                     'Linux Unix descriptor fixture required')
class DescriptorUnlockTests(unittest.TestCase):
    def test_received_descriptor_can_unlock_supervisor_flock(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'lease';path.touch(mode=0o600)
            owner=os.open(path,os.O_RDWR|os.O_NOFOLLOW)
            fcntl.flock(owner,fcntl.LOCK_EX|fcntl.LOCK_NB)
            left,right=socket.socketpair();left.settimeout(5)
            child=os.fork()
            if child==0:
                signal.alarm(8);left.close();os.close(owner)
                try:
                    _,ancillary,_,_=right.recvmsg(1,socket.CMSG_SPACE(array.array('i').itemsize))
                    descriptors=array.array('i')
                    for level,kind,data in ancillary:
                        if level==socket.SOL_SOCKET and kind==socket.SCM_RIGHTS:
                            descriptors.frombytes(data[:len(data)-len(data)%descriptors.itemsize])
                    if len(descriptors)!=1:os._exit(2)
                    fcntl.flock(descriptors[0],fcntl.LOCK_UN)
                    right.sendall(b'u');os.close(descriptors[0]);os._exit(0)
                except BaseException:os._exit(3)
            right.close();reaped=False
            try:
                left.sendmsg([b'x'],[(socket.SOL_SOCKET,socket.SCM_RIGHTS,array.array('i',[owner]))])
                self.assertEqual(left.recv(1),b'u')
                competitor=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
                try:
                    # This succeeds while the supervisor's original FD is
                    # still open, proving the lock was released by the peer.
                    fcntl.flock(competitor,fcntl.LOCK_EX|fcntl.LOCK_NB)
                finally:os.close(competitor)
                _,status=os.waitpid(child,0);reaped=True
                self.assertEqual(os.waitstatus_to_exitcode(status),0)
            finally:
                left.close();os.close(owner)
                if not reaped:
                    try:os.kill(child,signal.SIGKILL)
                    except ProcessLookupError:pass
                    os.waitpid(child,0)


if __name__=='__main__':unittest.main()
