import os, shutil, sqlite3, subprocess, sys, tempfile, unittest
from pathlib import Path
from unittest.mock import patch
from processing import legacy_fence_enforcement as f
from processing.legacy_state_gate import acquire
from processing.prototype import Queue
from processing.config import Config
from processing.app import make_handler

@unittest.skipUnless(os.name=='posix','POSIX flock required')
class EnforcementTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
  self.root=Path(self.tmp.name);self.state=self.root/'state';self.legacy=self.root/'legacy';self.legacy.mkdir();self.old=self.legacy/'coordinator.lock';self.old.touch()
  q=Queue(self.state);q.close()
  shutil.copy2(self.state/'jobs.sqlite3',self.legacy/'jobs.sqlite3')
 def test_enrolled_marker_requires_matching_lock_after_every_start(self):
  self.assertIsNone(f.enforce(self.state,str(self.old)))
  with self.assertRaisesRegex(RuntimeError,'marker_required'):f.enforce(self.state,str(self.old),required=True)
  f.enroll_offline(self.state,self.old);fd=f.enforce(self.state,str(self.old))
  try:
   with self.assertRaisesRegex(RuntimeError,'legacy_fence_held'):acquire(self.old)
  finally:os.close(fd)
  replacement=self.legacy/'other.lock';replacement.touch()
  with self.assertRaisesRegex(RuntimeError,'identity_mismatch'):f.enforce(self.state,str(replacement))
  self.old.unlink()
  with self.assertRaisesRegex(RuntimeError,'unavailable'):f.enforce(self.state,str(self.old))
 def test_app_acquires_before_queue_and_releases_after_close(self):
  f.enroll_offline(self.state,self.old)
  config=Config('x'*32,'','',1000,100000,prototype_state_dir=str(self.state))
  enforce=f.enforce
  with patch('processing.legacy_fence_enforcement.enforce',side_effect=lambda state,required:enforce(state,str(self.old),required=required)):
   handler=make_handler(config)
   with self.assertRaisesRegex(RuntimeError,'legacy_fence_held'):acquire(self.old)
   handler.prototype_queue.close()
   fd=acquire(self.old);os.close(fd)
   fd=acquire(self.old)
   try:
    with self.assertRaisesRegex(RuntimeError,'legacy_fence_held'):make_handler(config)
   finally:os.close(fd)
 def test_marker_corruption_holds_startup(self):
  f.enroll_offline(self.state,self.old)
  with sqlite3.connect(self.state/'jobs.sqlite3') as db:db.execute('CREATE TRIGGER bad_legacy AFTER UPDATE ON legacy_fence_requirement BEGIN SELECT 1; END')
  with self.assertRaisesRegex(RuntimeError,'marker_invalid'):f.enforce(self.state,str(self.old))
 def test_enrollment_requires_both_state_locks(self):
  fd=acquire(self.old)
  try:
   with self.assertRaisesRegex(RuntimeError,'legacy_fence_held'):f.enroll_offline(self.state,self.old)
  finally:os.close(fd)
  self.assertIsNone(f.requirement(self.state))
 def test_wrong_authority_and_missing_marker_fail_closed(self):
  with sqlite3.connect(self.legacy/'jobs.sqlite3') as db:
   db.execute("UPDATE coordinator_identity SET authority_id='00000000-0000-4000-8000-000000000000'")
  with self.assertRaisesRegex(RuntimeError,'authority_mismatch'):f.enroll_offline(self.state,self.old)
  with self.assertRaisesRegex(RuntimeError,'marker_required'):f.enforce(self.state,str(self.old),required=True)
  config=Config('x'*32,'','',1000,100000,prototype_state_dir=str(self.state),legacy_fence_required=True)
  with self.assertRaisesRegex(RuntimeError,'marker_required'):make_handler(config)
  with self.assertRaisesRegex(RuntimeError,'marker_required'):make_handler(Config('x'*32,'','',1000,100000,legacy_fence_required=True))
 def test_candidate_process_death_releases_enrolled_gate(self):
  f.enroll_offline(self.state,self.old)
  code='''import sys,time
from unittest.mock import patch
from processing import legacy_fence_enforcement as f
from processing.config import Config
from processing.app import make_handler
state,old=sys.argv[1:]; enforce=f.enforce
with patch('processing.legacy_fence_enforcement.enforce',side_effect=lambda state,required:enforce(state,old,required=required)):
 handler=make_handler(Config('x'*32,'','',1000,100000,prototype_state_dir=state,legacy_fence_required=True))
 print('ready',flush=True);time.sleep(30)
'''
  child=subprocess.Popen([sys.executable,'-c',code,str(self.state),str(self.old)],stdout=subprocess.PIPE,text=True)
  try:
   self.assertEqual(child.stdout.readline().strip(),'ready')
   with self.assertRaisesRegex(RuntimeError,'legacy_fence_held'):acquire(self.old)
   child.kill();child.wait(timeout=5)
   fd=acquire(self.old);os.close(fd)
  finally:
   if child.poll() is None:child.kill();child.wait(timeout=5)
   child.stdout.close()
 def test_viewer_initialization_failure_releases_fence(self):
  f.enroll_offline(self.state,self.old);enforce=f.enforce
  config=Config('x'*32,'','',1000,100000,prototype_state_dir=str(self.state),viewer_enabled=True)
  with patch('processing.legacy_fence_enforcement.enforce',side_effect=lambda state,required:enforce(state,str(self.old),required=required)):
   with patch('processing.viewer_service.ViewerService',side_effect=RuntimeError('synthetic startup failure')):
    with self.assertRaisesRegex(RuntimeError,'synthetic startup failure'):make_handler(config)
  fd=acquire(self.old);os.close(fd)
if __name__=='__main__':unittest.main()
