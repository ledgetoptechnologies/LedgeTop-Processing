import io
import sqlite3
import tempfile
import unittest
from pathlib import Path
from processing.paged_frame_receiver_candidate import initialize,stage,ReceiverHeld
from processing.viewer_selected_frames_candidate import binding_digest
from processing.large_paged_manifest_candidate import _digest
from tests import test_viewer_selected_frames_candidate as selected_fixture

class FrameReceiverTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  selected_fixture.SelectedFramesTests.setUpClass();cls.fixture=selected_fixture.SelectedFramesTests()
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory(prefix='frame-receiver-');self.addCleanup(self.temp.cleanup)
  self.path=Path(self.temp.name)/'candidate.sqlite';self.db=sqlite3.connect(self.path,isolation_level=None);self.addCleanup(self.db.close);initialize(self.db)
  f=self.fixture;self.binding=f.binding(f.source_pins,len(f.source),f.selected_pins,f.input_hash,_digest(f.closure));self.pin=binding_digest(self.binding)
 def run_stage(self,source=None,selected=None,fault=None):
  f=self.fixture
  return stage(self.db,'test-stage',trusted_binding=self.binding,expected_binding_sha256=self.pin,source_stream=io.BytesIO(f.source_wire if source is None else source),selected_stream=io.BytesIO(f.selected_wire if selected is None else selected),processing_closure=f.closure,processing_pages=f.pages,_fault=fault)
 def counts(self):
  return self.db.execute('SELECT (SELECT count(*) FROM candidate_frame_receipts),(SELECT count(*) FROM candidate_frame_pages)').fetchone()
 def test_large_commit_and_reopen_replay_are_held(self):
  result=self.run_stage();self.assertEqual((result['created'],result['selectedObjects'],result['state']),(True,16834,'held_authenticated_source_unavailable'))
  before=self.counts();self.db.close();self.db=sqlite3.connect(self.path,isolation_level=None);self.addCleanup(self.db.close)
  replay=self.run_stage(source=b'',selected=b'');self.assertFalse(replay['created']);self.assertEqual(self.counts(),before)
 def test_truncation_rolls_back_and_retry_commits(self):
  with self.assertRaises(ReceiverHeld):self.run_stage(source=self.fixture.source_wire[:-20])
  self.assertEqual(self.counts(),(0,0));self.db.close();self.db=sqlite3.connect(self.path,isolation_level=None);self.addCleanup(self.db.close)
  self.assertTrue(self.run_stage()['created'])
 def test_unhandled_interruption_and_connection_close_roll_back(self):
  def interrupted(kind,index):
   if kind=='source' and index==2:raise SystemExit('synthetic interruption')
  with self.assertRaises(SystemExit):self.run_stage(fault=interrupted)
  self.db.close();self.db=sqlite3.connect(self.path,isolation_level=None);self.addCleanup(self.db.close)
  self.assertEqual(self.counts(),(0,0));self.assertTrue(self.run_stage()['created'])
 def test_unsafe_synchronous_setting_holds(self):
  self.db.execute('PRAGMA synchronous=OFF')
  with self.assertRaises(ReceiverHeld):self.run_stage()
  self.assertEqual(self.counts(),(0,0))
 def test_failed_initialization_rolls_back_its_schema_transaction(self):
  db=sqlite3.connect(Path(self.temp.name)/'other.sqlite',isolation_level=None)
  try:
   db.set_authorizer(lambda action,name,*_: sqlite3.SQLITE_DENY if action==sqlite3.SQLITE_CREATE_TABLE and name=='candidate_frame_pages' else sqlite3.SQLITE_OK)
   with self.assertRaises(ReceiverHeld):initialize(db)
   db.set_authorizer(None)
   self.assertFalse(db.in_transaction)
   self.assertIsNone(db.execute("SELECT name FROM sqlite_master WHERE name='candidate_frame_receipts'").fetchone())
  finally:db.close()
 def test_preexisting_transaction_is_not_rolled_back(self):
  self.db.execute('CREATE TABLE unrelated_work(value TEXT)')
  self.db.execute('BEGIN IMMEDIATE');self.db.execute("INSERT INTO unrelated_work VALUES('pending')")
  with self.assertRaises(ReceiverHeld):initialize(self.db)
  self.assertTrue(self.db.in_transaction)
  with self.assertRaises(ReceiverHeld):self.run_stage()
  self.assertTrue(self.db.in_transaction)
  self.assertEqual(self.db.execute('SELECT value FROM unrelated_work').fetchall(),[('pending',)])
  self.db.rollback()
 def test_synthetic_disk_full_rolls_back(self):
  def full(kind,index):
   if kind=='source' and index==2:raise sqlite3.OperationalError('synthetic disk full')
  with self.assertRaises(ReceiverHeld):self.run_stage(fault=full)
  self.assertEqual(self.counts(),(0,0))
 def test_persisted_corruption_holds_on_reopen(self):
  self.run_stage();self.db.execute("UPDATE candidate_frame_pages SET wire=? WHERE stage_id='test-stage' AND kind='source' AND ordinal=0",(b'bad',))
  self.db.close();self.db=sqlite3.connect(self.path,isolation_level=None);self.addCleanup(self.db.close);before=self.counts()
  with self.assertRaises(ReceiverHeld):self.run_stage(source=b'',selected=b'')
  self.assertEqual(self.counts(),before)

if __name__=='__main__':unittest.main()
