import os,shutil,tempfile,unittest
from pathlib import Path
from processing.prototype import Queue
from processing.viewer_tasks import TaskStore
from processing.legacy_state_gate import acquire
from processing.legacy_fence_enforcement import enroll_offline
from ops.enrolled_offline_quiescence import stopped_enrolled_state

@unittest.skipUnless(os.name=='posix','POSIX flock required')
class OfflineTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);root=Path(self.tmp.name)
  self.state=root/'state';self.legacy=root/'legacy';q=Queue(self.state);TaskStore(q,self.state/'viewer-tasks');q.close()
  self.legacy.mkdir();shutil.copy2(self.state/'jobs.sqlite3',self.legacy/'jobs.sqlite3');(self.legacy/'coordinator.lock').touch()
  enroll_offline(self.state,self.legacy/'coordinator.lock')
 def test_idle_holds_both_locks_until_context_exit(self):
  with stopped_enrolled_state(self.state,self.legacy) as proof:
   self.assertTrue(proof['heldStateChecked'])
   for p in (self.state/'coordinator.lock',self.legacy/'coordinator.lock'):
    with self.assertRaisesRegex(RuntimeError,'legacy_fence_held'):acquire(p)
  for p in (self.state/'coordinator.lock',self.legacy/'coordinator.lock'):
   fd=acquire(p);os.close(fd)
 def test_active_job_blocks_and_releases_locks(self):
  import sqlite3,uuid
  with sqlite3.connect(self.state/'jobs.sqlite3') as db:
   db.execute("INSERT INTO jobs(id,request_id,environment,profile,status) VALUES(?,?,'staging','treedar-synthetic-v1','queued')",(str(uuid.uuid4()),'synthetic'))
  with self.assertRaisesRegex(RuntimeError,'production_hold_idle_state_unavailable'):
   with stopped_enrolled_state(self.state,self.legacy):self.fail('admitted')
 def test_unacknowledged_reference_outbox_holds(self):
  from processing.reference_store import ReferenceStore
  q=Queue(self.state,reference_enabled=True)
  try:
   ReferenceStore(q)
   with q._db() as db:db.execute("INSERT INTO reference_outbox(identity,kind,payload_json,digest,acknowledged) VALUES ('test','test','{}','test',0)")
  finally:q.close()
  with self.assertRaisesRegex(RuntimeError,'offline_reference_outbox_unsettled'):
   with stopped_enrolled_state(self.state,self.legacy):self.fail('admitted')
 def test_partial_reference_schema_holds(self):
  import sqlite3
  with sqlite3.connect(self.state/'jobs.sqlite3') as db:db.execute('CREATE TABLE reference_jobs (phase TEXT)')
  with self.assertRaisesRegex(RuntimeError,'offline_reference_state_unavailable'):
   with stopped_enrolled_state(self.state,self.legacy):self.fail('admitted')
 def test_other_reference_table_without_pair_holds(self):
  import sqlite3
  with sqlite3.connect(self.state/'jobs.sqlite3') as db:db.execute('CREATE TABLE reference_claims (synthetic TEXT)')
  with self.assertRaisesRegex(RuntimeError,'offline_reference_state_unavailable'):
   with stopped_enrolled_state(self.state,self.legacy):self.fail('admitted')
 def test_sidecars_on_either_database_are_rejected_before_first_sqlite_read(self):
  from unittest.mock import patch
  for root in (self.state,self.legacy):
   sidecar=root/'jobs.sqlite3-wal';sidecar.touch()
   try:
    with patch('ops.enrolled_offline_quiescence.requirement',side_effect=AssertionError('unexpected SQLite read')):
     with self.assertRaisesRegex(RuntimeError,'production_hold_idle_state_unavailable'):
      with stopped_enrolled_state(self.state,self.legacy):self.fail('admitted')
   finally:sidecar.unlink()
  fd=acquire(self.legacy/'coordinator.lock');os.close(fd)
 def test_running_legacy_or_sidecar_refuses(self):
  fd=acquire(self.legacy/'coordinator.lock')
  try:
   with self.assertRaisesRegex(RuntimeError,'legacy_fence_held'):
    with stopped_enrolled_state(self.state,self.legacy):self.fail('admitted')
  finally:os.close(fd)
  (self.state/'jobs.sqlite3-wal').touch()
  with self.assertRaisesRegex(RuntimeError,'production_hold_idle_state_unavailable'):
   with stopped_enrolled_state(self.state,self.legacy):self.fail('admitted')
if __name__=='__main__':unittest.main()
