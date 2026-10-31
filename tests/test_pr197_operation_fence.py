import fcntl
import os
from pathlib import Path
import signal
import tempfile
import unittest
from unittest.mock import patch

from ops.pr197_operation_fence import OperationFence,OperationFenceHeld


@unittest.skipUnless(os.name=='posix' and hasattr(os,'fork'),'Linux lock fixture required')
class OperationFenceTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.state=self.root/'state';self.state.mkdir()
        self.leases=self.root/'leases';self.leases.mkdir()
        self.authority='12345678-1234-1234-1234-123456789abc'
        self.lock=self.state/'coordinator.lock';self.lock.touch()
        self.lease=self.leases/(self.authority+'.lease');self.lease.touch()
        self.mount=patch('ops.pr197_stopped_census._mount');self.mount.start();self.addCleanup(self.mount.stop)

    def fence(self):return OperationFence(self.state,self.leases,self.authority,enabled=True)

    def test_competing_process_refused_while_operation_holds_both_locks(self):
        with self.fence() as fence:
            pid=os.fork()
            if pid==0:
                signal.alarm(5)
                try:
                    for path in (self.lease,self.lock):
                        fd=os.open(path,os.O_RDONLY|os.O_NONBLOCK)
                        try:
                            try:fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB);os._exit(2)
                            except BlockingIOError:pass
                        finally:os.close(fd)
                    os._exit(0)
                except BaseException:os._exit(3)
            _,status=os.waitpid(pid,0)
            self.assertEqual(os.waitstatus_to_exitcode(status),0)
            fence.check()

    def test_killed_owner_loses_locks_and_never_authorizes_handoff(self):
        read,write=os.pipe();pid=os.fork()
        if pid==0:
            os.close(read);signal.alarm(10)
            try:
                with self.fence() as fence:
                    fence.check();os.write(write,b'1');signal.pause()
            finally:os._exit(3)
        os.close(write)
        try:
            self.assertEqual(os.read(read,1),b'1')
            os.kill(pid,signal.SIGKILL)
            _,status=os.waitpid(pid,0)
            self.assertTrue(os.WIFSIGNALED(status))
            # This demonstrates why an external supervisor must stop cutover;
            # reacquirable locks are not authority to start a candidate.
            with self.fence() as replacement:replacement.check()
        finally:os.close(read)

    def test_replaced_lock_is_detected(self):
        with self.fence() as fence:
            self.lock.unlink();self.lock.touch()
            with self.assertRaisesRegex(OperationFenceHeld,'operation_lock_lost_or_replaced'):fence.check()
