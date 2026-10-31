import os, signal, subprocess, sys, tempfile, time, unittest
from pathlib import Path
from processing.legacy_state_gate import acquire, check_distinct

@unittest.skipUnless(os.name=='posix','POSIX flock required')
class LegacyFenceTests(unittest.TestCase):
 def test_mutual_exclusion_and_crash_recovery(self):
  with tempfile.TemporaryDirectory() as temp:
   p=Path(temp)/'coordinator.lock';p.touch()
   code='import os,sys,time; from processing.legacy_state_gate import acquire; fd=acquire(sys.argv[1]); print("held",flush=True); time.sleep(30)'
   first=subprocess.Popen([sys.executable,'-c',code,str(p)],stdout=subprocess.PIPE,text=True)
   try:
    self.assertEqual(first.stdout.readline().strip(),'held')
    with self.assertRaisesRegex(RuntimeError,'legacy_fence_held'):acquire(p)
    first.kill();first.wait(timeout=5)
    fd=acquire(p);os.close(fd)
   finally:
    if first.poll() is None:first.kill();first.wait(timeout=5)
    first.stdout.close()
 def test_exec_inherits_lock_and_kill_releases_it(self):
  with tempfile.TemporaryDirectory() as temp:
   p=Path(temp)/'coordinator.lock';p.touch()
   inner='import time; print("exec-held",flush=True); time.sleep(30)'
   code='import os,sys; from processing.legacy_state_gate import acquire; fd=acquire(sys.argv[1]); os.set_inheritable(fd,True); os.execv(sys.executable,[sys.executable,"-c",'+repr(inner)+'])'
   child=subprocess.Popen([sys.executable,'-c',code,str(p)],stdout=subprocess.PIPE,text=True)
   try:
    self.assertEqual(child.stdout.readline().strip(),'exec-held')
    with self.assertRaisesRegex(RuntimeError,'legacy_fence_held'):acquire(p)
    child.kill();child.wait(timeout=5);fd=acquire(p);os.close(fd)
   finally:
    if child.poll() is None:child.kill();child.wait(timeout=5)
    child.stdout.close()
 def test_missing_or_symlink_refused(self):
  with tempfile.TemporaryDirectory() as temp:
   p=Path(temp)/'coordinator.lock'
   with self.assertRaises(RuntimeError):acquire(p)
   p.touch();symlink=Path(temp)/'link';symlink.symlink_to(p)
   with self.assertRaises(RuntimeError):acquire(symlink)
 def test_candidate_cannot_use_same_legacy_lock_inode(self):
  with tempfile.TemporaryDirectory() as temp:
   root=Path(temp);(root/'coordinator.lock').touch();fd=acquire(root/'coordinator.lock')
   try:
    with self.assertRaisesRegex(RuntimeError,'legacy_fence_same_candidate_lock'):check_distinct(fd,str(root))
    check_distinct(fd,str(root/'separate'))
   finally:os.close(fd)
if __name__=='__main__':unittest.main()
