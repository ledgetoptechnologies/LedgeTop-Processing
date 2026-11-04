import io
import json
import queue
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from processing.gcp_evidence_staging_candidate import initialize,stage,EvidenceStageHeld
from tests import test_gcp_evidence_stream_candidate as fixture

class EvidenceStagingTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory(prefix='evidence-stage-');self.addCleanup(self.temp.cleanup)
  self.root=Path(self.temp.name);self.path=self.root/'stage.sqlite';self.db=sqlite3.connect(self.path,isolation_level=None);self.addCleanup(self.db.close);initialize(self.db)
  self.data=b'a'*46079+'\u96ea'.encode()+b'z';sha,pages=fixture.evidence_frames(self.data);self.wire,pins,sizes=fixture.encoded(pages)
  self.metadata=dict(statementSha256='f'*64,attemptId=fixture.ATTEMPT,evidenceSha256=sha,pageHashes=pins,pageBytes=sizes,decodedBytes=len(self.data))
 def run_stage(self,wire=None,fault=None):return stage(self.db,'stage-1',metadata=self.metadata,stream=io.BytesIO(self.wire if wire is None else wire),_fault=fault)
 def counts(self):return self.db.execute('SELECT (SELECT COUNT(*) FROM candidate_evidence),(SELECT COUNT(*) FROM candidate_evidence_pages)').fetchone()
 def test_truncation_rolls_back_and_retry_replays_after_reopen(self):
  with self.assertRaises(EvidenceStageHeld):self.run_stage(self.wire[:-4])
  self.assertEqual(self.counts(),(0,0));self.assertTrue(self.run_stage()['created'])
  self.db.close();self.db=sqlite3.connect(self.path,isolation_level=None);self.addCleanup(self.db.close)
  self.assertFalse(self.run_stage(b'')['created'])
 def test_disk_full_and_unrelated_transaction_are_not_committed(self):
  def full(index):
   if index==0:raise sqlite3.OperationalError('synthetic disk full')
  with self.assertRaises(EvidenceStageHeld):self.run_stage(fault=full)
  self.assertEqual(self.counts(),(0,0))
  self.db.execute('CREATE TABLE unrelated(value TEXT)');self.db.execute('BEGIN');self.db.execute("INSERT INTO unrelated VALUES ('pending')")
  with self.assertRaises(EvidenceStageHeld):self.run_stage()
  self.assertTrue(self.db.in_transaction);self.assertEqual(self.db.execute('SELECT value FROM unrelated').fetchall(),[('pending',)]);self.db.rollback()
 def test_corruption_holds_after_reopen(self):
  self.run_stage();self.db.execute("UPDATE candidate_evidence_pages SET wire=? WHERE stage_id='stage-1' AND ordinal=0",(b'bad',));before=self.counts()
  self.db.close();self.db=sqlite3.connect(self.path,isolation_level=None);self.addCleanup(self.db.close)
  with self.assertRaises(EvidenceStageHeld):self.run_stage(b'')
  self.assertEqual(self.counts(),before)
 def test_killed_child_rolls_back_partial_pages(self):
  (self.root/'evidence.frames').write_bytes(self.wire);(self.root/'metadata.json').write_text(json.dumps(self.metadata),encoding='utf8')
  script="""import json,sqlite3,sys
from pathlib import Path
from processing.gcp_evidence_staging_candidate import stage
r=Path(sys.argv[1]);db=sqlite3.connect(r/'stage.sqlite',isolation_level=None);m=json.loads((r/'metadata.json').read_text(encoding='utf8'))
def pause(index):
 if index==0:print('partial-evidence',flush=True);sys.stdin.read(1)
with (r/'evidence.frames').open('rb') as stream:stage(db,'stage-1',metadata=m,stream=stream,_fault=pause)
"""
  proc=subprocess.Popen([sys.executable,'-c',script,str(self.root)],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
  try:
   messages=queue.Queue();reader=threading.Thread(target=lambda:messages.put(proc.stdout.readline()),daemon=True);reader.start()
   try:signal=messages.get(timeout=15)
   except queue.Empty:self.fail('child did not reach staging')
   self.assertEqual(signal.strip(),'partial-evidence');proc.kill();proc.wait(timeout=10);self.assertNotEqual(proc.returncode,0);reader.join(timeout=5);self.assertFalse(reader.is_alive())
  finally:
   if proc.poll() is None:proc.kill();proc.wait(timeout=10)
   proc.communicate(timeout=5)
  self.db.close();self.db=sqlite3.connect(self.path,isolation_level=None);self.addCleanup(self.db.close)
  self.assertEqual(self.counts(),(0,0));self.assertTrue(self.run_stage()['created'])

if __name__=='__main__':unittest.main()
