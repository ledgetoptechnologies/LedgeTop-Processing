import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from processing.reference_worker_fence_v2 import WorkerFence, WorkerFenceHeld

@unittest.skipUnless(os.name == 'posix', 'POSIX flock required')
class WorkerFenceTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)/'private';self.root.mkdir(mode=0o700)
        fd=os.open(self.root/'worker.lock',os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600);os.close(fd)

    def test_exclusion_and_process_death(self):
        code='import sys,time; from processing.reference_worker_fence_v2 import WorkerFence; f=WorkerFence(sys.argv[1]); print("ready",flush=True); time.sleep(60)'
        child=subprocess.Popen([sys.executable,'-u','-c',code,str(self.root)],stdout=subprocess.PIPE,text=True)
        try:
            self.assertEqual(child.stdout.readline().strip(),'ready')
            with self.assertRaises(WorkerFenceHeld): WorkerFence(self.root)
            child.kill();child.wait(timeout=5)
            with WorkerFence(self.root) as fence: fence.check()
        finally:
            if child.poll() is None: child.kill();child.wait(timeout=5)
            child.stdout.close()

    def test_forked_child_does_not_extend_parent_lock_after_death(self):
        import signal
        code = """import os,sys,time
from processing.reference_worker_fence_v2 import WorkerFence
f=WorkerFence(sys.argv[1]);child=os.fork()
if child==0:
    time.sleep(15);os._exit(0)
print(child,flush=True)
time.sleep(60)
"""
        parent=subprocess.Popen([sys.executable,'-u','-c',code,str(self.root)],stdout=subprocess.PIPE,text=True)
        child=None
        try:
            child=int(parent.stdout.readline().strip())
            with self.assertRaises(WorkerFenceHeld):WorkerFence(self.root)
            parent.kill();parent.wait(timeout=5)
            os.kill(child,0)  # child is still alive when acquiring the fence
            with WorkerFence(self.root) as fence:fence.check()
        finally:
            if parent.poll() is None:parent.kill();parent.wait(timeout=5)
            if child is not None:
                try:os.kill(child,signal.SIGTERM)
                except ProcessLookupError:pass
            parent.stdout.close()

    def test_fork_waits_for_inflight_initialization(self):
        import signal
        code = """import os,sys,time,threading,fcntl
from processing.reference_worker_fence_v2 import WorkerFence
entered=threading.Event();release=threading.Event();original=fcntl.flock
holder=[]
def slow(fd,op):
 original(fd,op)
 if op & fcntl.LOCK_EX: entered.set();release.wait(5)
fcntl.flock=slow
t=threading.Thread(target=lambda:holder.append(WorkerFence(sys.argv[1])));t.start()
if not entered.wait(5):sys.exit(2)
threading.Timer(.2,release.set).start()
child=os.fork()
if child==0:time.sleep(15);os._exit(0)
t.join(5)
if t.is_alive():sys.exit(3)
print(child,flush=True);time.sleep(60)
"""
        parent=subprocess.Popen([sys.executable,'-u','-c',code,str(self.root)],stdout=subprocess.PIPE,text=True)
        child=None
        try:
            child=int(parent.stdout.readline().strip())
            with self.assertRaises(WorkerFenceHeld):WorkerFence(self.root)
            parent.kill();parent.wait(timeout=5);os.kill(child,0)
            with WorkerFence(self.root) as fence:fence.check()
        finally:
            if parent.poll() is None:parent.kill();parent.wait(timeout=5)
            if child is not None:
                try:os.kill(child,signal.SIGTERM)
                except ProcessLookupError:pass
            parent.stdout.close()

    def test_replaced_inode_detected_and_unsafe_files_rejected(self):
        with WorkerFence(self.root) as fence:
            (self.root/'worker.lock').unlink()
            fd=os.open(self.root/'worker.lock',os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600);os.close(fd)
            with self.assertRaises(WorkerFenceHeld):fence.check()
        (self.root/'worker.lock').unlink();(self.root/'worker.lock').symlink_to('/dev/null')
        with self.assertRaises((WorkerFenceHeld,OSError)):WorkerFence(self.root)
