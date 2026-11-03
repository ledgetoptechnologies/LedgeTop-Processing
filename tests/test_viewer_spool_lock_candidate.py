import os
import subprocess
import sys
import tempfile
import queue
import threading
import unittest
from pathlib import Path
from processing.viewer_spool_lock_candidate import SpoolLock,SpoolHeld


@unittest.skipUnless(os.name=='posix','synthetic POSIX flock test')
class SpoolLockTests(unittest.TestCase):
    def test_separate_process_contention_and_abandoned_hold(self):
        code='''import sys
from processing.viewer_spool_lock_candidate import SpoolLock
with SpoolLock(sys.argv[1]).acquire():
 print('owned',flush=True);sys.stdin.read(1)
'''
        with tempfile.TemporaryDirectory() as root:
            child=subprocess.Popen([sys.executable,'-c',code,root],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
            try:
                lines=queue.Queue();reader=threading.Thread(target=lambda:lines.put(child.stdout.readline()),daemon=True);reader.start()
                self.assertEqual(lines.get(timeout=10).strip(),'owned');reader.join(timeout=5)
                with self.assertRaises(SpoolHeld):
                    with SpoolLock(root).acquire(): pass
                child.kill();child.wait(timeout=10)
                self.assertTrue((Path(root)/'viewer-spool.dirty').exists())
                with self.assertRaises(SpoolHeld):
                    with SpoolLock(root).acquire(): pass
            finally:
                if child.poll() is None:child.kill();child.wait(timeout=10)
                child.communicate(timeout=5)

    def test_normal_exit_allows_next_operation(self):
        with tempfile.TemporaryDirectory() as root:
            with SpoolLock(root).acquire(): self.assertTrue((Path(root)/'viewer-spool.dirty').exists())
            with SpoolLock(root).acquire(): pass

    def test_body_file_exists_propagates_and_keeps_dirty_hold(self):
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaisesRegex(FileExistsError,'upload collision'):
                with SpoolLock(root).acquire(): raise FileExistsError('upload collision')
            self.assertTrue((Path(root)/'viewer-spool.dirty').exists())
            with self.assertRaisesRegex(SpoolHeld,'reconciliation'):
                with SpoolLock(root).acquire(): pass
