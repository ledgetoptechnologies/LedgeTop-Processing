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

    def test_replaced_inode_detected_and_unsafe_files_rejected(self):
        with WorkerFence(self.root) as fence:
            (self.root/'worker.lock').unlink()
            fd=os.open(self.root/'worker.lock',os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600);os.close(fd)
            with self.assertRaises(WorkerFenceHeld):fence.check()
        (self.root/'worker.lock').unlink();(self.root/'worker.lock').symlink_to('/dev/null')
        with self.assertRaises((WorkerFenceHeld,OSError)):WorkerFence(self.root)
