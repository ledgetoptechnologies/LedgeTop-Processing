import sqlite3,unittest
from unittest.mock import patch
from tests.test_reference_v2_precharge_draft import PrechargeTests
from processing.reference_v2_indexed_precharge_draft import IndexedPrecharge
from processing.reference_v2_precharge_draft import PrechargeHeld

class IndexedPrechargeTests(PrechargeTests):
 def ledger(self):
  result=IndexedPrecharge(self.path,inode=self.inode,epoch='epoch-1',clock=lambda:self.now)
  self.addCleanup(result.close);return result
 def test_cache_avoids_full_history_scan_and_detects_other_writer(self):
  l=self.ledger()
  with patch.object(l,'_validate',wraps=l._validate) as validate:
   l.precharge(request_id='r1',object_id='empty',start=0,end=0)
   self.assertEqual(l.precharge(request_id='r1',object_id='empty',start=0,end=0)['state'],'replay_no_body')
   self.assertEqual(validate.call_count,0)
   db=sqlite3.connect(self.path)
   try:db.execute("UPDATE charges SET object_id='outside'");db.commit()
   finally:db.close()
   with self.assertRaises(PrechargeHeld):l.precharge(request_id='r1',object_id='empty',start=0,end=0)
   self.assertGreater(validate.call_count,0)
 def test_local_record_limit_rejects_next_insert(self):
  with patch('processing.reference_v2_indexed_precharge_draft.MAX_LOCAL_CHARGES',1):
   l=self.ledger();l.precharge(request_id='r1',object_id='empty',start=0,end=0)
   with self.assertRaisesRegex(PrechargeHeld,'record limit'):l.precharge(request_id='r2',object_id='empty',start=0,end=0)
 def test_unknown_commit_is_not_replayed_as_free_body(self):
  class UnknownCommit(sqlite3.Connection):
   commits=0
   def execute(self,sql,*args):
    result=super().execute(sql,*args)
    if sql=='COMMIT':
     self.commits+=1
     if self.commits==2:raise sqlite3.OperationalError('unknown commit')
    return result
  l=self.ledger();l._conn.close();l._conn=sqlite3.connect(self.path,timeout=10,isolation_level=None,factory=UnknownCommit);l._conn.execute('PRAGMA synchronous=FULL');l._version=None
  with self.assertRaises(sqlite3.OperationalError):l.precharge(request_id='r1',object_id='cloud',start=0,end=4)
  self.assertEqual(self.ledger().precharge(request_id='r1',object_id='cloud',start=0,end=4)['state'],'replay_no_body')

 def test_process_exit_after_charge_and_replay(self):
  import subprocess,sys
  code="""import os,sys
from processing.reference_v2_indexed_precharge_draft import IndexedPrecharge
l=IndexedPrecharge(sys.argv[1],inode=(int(sys.argv[2]),int(sys.argv[3])),epoch='epoch-1',clock=lambda:1793361600000)
l.precharge(request_id='r1',object_id='cloud',start=0,end=4);os._exit(73)
"""
  child=subprocess.run([sys.executable,'-c',code,str(self.path),str(self.inode[0]),str(self.inode[1])],timeout=10)
  self.assertEqual(child.returncode,73)
  self.assertEqual(self.ledger().precharge(request_id='r1',object_id='cloud',start=0,end=4)['state'],'replay_no_body')

 def test_indexed_separate_processes_contend_for_last_request(self):
  import subprocess,sys
  code="""import sys
from processing.reference_v2_indexed_precharge_draft import IndexedPrecharge
from processing.reference_v2_precharge_draft import PrechargeHeld
l=IndexedPrecharge(sys.argv[1],inode=(int(sys.argv[2]),int(sys.argv[3])),epoch='epoch-1',clock=lambda:1793361600000)
try:l.precharge(request_id=sys.argv[4],object_id='cloud',start=0,end=4);print('charged')
except PrechargeHeld:print('held')
finally:l.close()
"""
  processes=[subprocess.Popen([sys.executable,'-c',code,str(self.path),str(self.inode[0]),str(self.inode[1]),name],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True) for name in ('r1','r2')]
  outputs=[]
  try:
   for child in processes:
    out,err=child.communicate(timeout=10);self.assertEqual(child.returncode,0,err);outputs.append(out.strip())
  finally:
   for child in processes:
    if child.poll() is None:child.kill();child.communicate(timeout=5)
  self.assertEqual(sorted(outputs),['charged','held'])

 def test_two_open_connections_refresh_after_other_commit(self):
  a=self.ledger();b=self.ledger();a.precharge(request_id='r1',object_id='empty',start=0,end=0)
  with self.assertRaises(PrechargeHeld):b.precharge(request_id='r2',object_id='empty',start=0,end=0)
