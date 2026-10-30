import json,os,sqlite3,subprocess,sys,tempfile,unittest
from pathlib import Path
from processing import reference_protocol as p
from processing.reference_input_grant_v2_draft import verify_candidate_grant
from processing.reference_v2_precharge_draft import PrechargeLedger,PrechargeHeld,SCHEMA

class PrechargeTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);root=Path(self.tmp.name);self.path=root/'ledger.sqlite'
  manifest=(Path(__file__).resolve().parents[1]/'contracts/input-v2-manifest-canonical.json').read_bytes().strip()
  m=json.loads(manifest);vectors=json.loads((Path(__file__).resolve().parents[1]/'contracts/input-v2-transfer-vectors.json').read_text(encoding='utf8'))
  g=vectors['canonicalGrant']['payload'];g.update(manifestSha256=m['sha256'],rangeBytes=4,totalRequestBudget=1,totalByteBudget=4)
  raw=p.canonical_json(g);sha=verify_candidate_grant(raw)['sha256'];db=sqlite3.connect(self.path)
  for q in SCHEMA:db.execute(q)
  db.execute('INSERT INTO transfer VALUES(1,?,?,?,?,0,0)',(raw,manifest,sha,0));db.commit();db.close();self.path.chmod(0o600)
  st=self.path.stat();self.inode=(st.st_dev,st.st_ino);self.now=1793361600000
 def ledger(self):return PrechargeLedger(self.path,inode=self.inode,epoch='epoch-1',clock=lambda:self.now)
 def test_once_replay_identity_and_budget(self):
  l=self.ledger();self.assertEqual(l.precharge(request_id='r1',object_id='cloud',start=0,end=4)['state'],'charged_no_authority')
  self.assertEqual(l.precharge(request_id='r1',object_id='cloud',start=0,end=4)['state'],'replay_no_body')
  with self.assertRaises(PrechargeHeld):l.precharge(request_id='r1',object_id='cloud',start=0,end=3)
  with self.assertRaises(PrechargeHeld):l.precharge(request_id='r2',object_id='cloud',start=4,end=8)
 def test_expiry_denial_commits_highwater_across_clock_rollback(self):
  self.now=1793362200001;l=self.ledger()
  with self.assertRaisesRegex(PrechargeHeld,'expired'):l.precharge(request_id='r1',object_id='empty',start=0,end=0)
  self.now=1793361600000;l=self.ledger()
  with self.assertRaisesRegex(PrechargeHeld,'expired'):l.precharge(request_id='r1',object_id='empty',start=0,end=0)
 def test_unknown_commit_is_not_replayed_as_free_body(self):
  class UnknownCommit(sqlite3.Connection):
   commits=0
   def execute(self,sql,*args):
    result=super().execute(sql,*args)
    if sql=='COMMIT':
     self.commits+=1
     if self.commits==2:raise sqlite3.OperationalError('simulated unknown commit result')
    return result
  l=self.ledger()
  def open_unknown():
   db=sqlite3.connect(self.path,timeout=10,isolation_level=None,factory=UnknownCommit)
   db.execute('PRAGMA synchronous=FULL');return db
  l._db=open_unknown
  with self.assertRaises(sqlite3.OperationalError):l.precharge(request_id='r1',object_id='cloud',start=0,end=4)
  self.assertEqual(self.ledger().precharge(request_id='r1',object_id='cloud',start=0,end=4)['state'],'replay_no_body')

 def test_expiry_during_transaction_commits_clock_without_charge(self):
  ticks=iter([1793361600000,1793361600000,1793362200001]);l=PrechargeLedger(self.path,inode=self.inode,epoch='epoch-1',clock=lambda:next(ticks))
  with self.assertRaisesRegex(PrechargeHeld,'expired'):l.precharge(request_id='r1',object_id='cloud',start=0,end=4)
  self.now=1793361600000
  with self.assertRaisesRegex(PrechargeHeld,'expired'):self.ledger().precharge(request_id='r2',object_id='cloud',start=0,end=4)
  db=sqlite3.connect(self.path)
  try:self.assertEqual(db.execute('SELECT COUNT(*) FROM charges').fetchone()[0],0)
  finally:db.close()

 def test_contended_lock_observes_clock_after_wait(self):
  clockfile=Path(self.tmp.name)/'clock';clockfile.write_text('1793361600000')
  holder_code="""import sqlite3,sys
c=sqlite3.connect(sys.argv[1],isolation_level=None);c.execute('BEGIN IMMEDIATE');print('locked',flush=True);sys.stdin.readline();c.execute('COMMIT')
"""
  charge_code="""import sys
from pathlib import Path
from processing.reference_v2_precharge_draft import PrechargeLedger,PrechargeHeld
l=PrechargeLedger(sys.argv[1],inode=(int(sys.argv[2]),int(sys.argv[3])),epoch='epoch-1',clock=lambda:int(Path(sys.argv[4]).read_text()))
print('started',flush=True)
try:l.precharge(request_id='r1',object_id='cloud',start=0,end=4);print('charged')
except PrechargeHeld:print('held')
"""
  holder=subprocess.Popen([sys.executable,'-u','-c',holder_code,str(self.path)],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
  charge=None
  try:
   self.assertEqual(holder.stdout.readline().strip(),'locked')
   charge=subprocess.Popen([sys.executable,'-u','-c',charge_code,str(self.path),str(self.inode[0]),str(self.inode[1]),str(clockfile)],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
   self.assertEqual(charge.stdout.readline().strip(),'started');clockfile.write_text('1793362200001');holder.stdin.write('release\n');holder.stdin.flush()
   out,err=charge.communicate(timeout=10);self.assertEqual(charge.returncode,0,err);self.assertEqual(out.strip(),'held')
   holder.communicate(timeout=10);self.now=1793361600000
   with self.assertRaisesRegex(PrechargeHeld,'expired'):self.ledger().precharge(request_id='r2',object_id='cloud',start=0,end=4)
  finally:
   for child in (holder,charge):
    if child and child.poll() is None:child.kill();child.communicate(timeout=5)

 def test_corrupt_charge_object_is_rejected_on_reopen(self):
  self.ledger().precharge(request_id='r1',object_id='cloud',start=0,end=4)
  db=sqlite3.connect(self.path)
  try:db.execute("UPDATE charges SET object_id='outside'");db.commit()
  finally:db.close()
  with self.assertRaisesRegex(PrechargeHeld,'accounting invalid'):self.ledger()

 def test_local_record_limit_rejects_next_insert(self):
  from unittest.mock import patch
  with patch('processing.reference_v2_precharge_draft.MAX_LOCAL_CHARGES',1):
   l=self.ledger();l.precharge(request_id='r1',object_id='empty',start=0,end=0)
   with self.assertRaisesRegex(PrechargeHeld,'record limit'):l.precharge(request_id='r2',object_id='empty',start=0,end=0)
   self.assertEqual(l.precharge(request_id='r1',object_id='empty',start=0,end=0)['state'],'replay_no_body')

 def test_business_denial_persists_second_observation(self):
  ticks=iter([1793361600000,1793361602000]);l=PrechargeLedger(self.path,inode=self.inode,epoch='epoch-1',clock=lambda:next(ticks))
  with self.assertRaisesRegex(PrechargeHeld,'outside manifest'):l.precharge(request_id='r1',object_id='absent',start=0,end=1)
  self.now=1793361600000;self.ledger().precharge(request_id='r2',object_id='empty',start=0,end=0)
  db=sqlite3.connect(self.path)
  try:self.assertEqual(db.execute('SELECT clock_ms FROM transfer').fetchone()[0],1793361602000)
  finally:db.close()

 def test_zero_length_is_one_request(self):
  l=self.ledger();self.assertEqual(l.precharge(request_id='r1',object_id='empty',start=0,end=0)['bytes'],0)
  with self.assertRaisesRegex(PrechargeHeld,'budget'):l.precharge(request_id='r2',object_id='empty',start=0,end=0)
 def test_process_exit_after_charge_and_replay(self):
  code="""import os,sys
from processing.reference_v2_precharge_draft import PrechargeLedger
l=PrechargeLedger(sys.argv[1],inode=(int(sys.argv[2]),int(sys.argv[3])),epoch='epoch-1',clock=lambda:1793361600000)
l.precharge(request_id='r1',object_id='cloud',start=0,end=4);os._exit(73)
"""
  result=subprocess.run([sys.executable,'-c',code,str(self.path),str(self.inode[0]),str(self.inode[1])],timeout=10)
  self.assertEqual(result.returncode,73);self.assertEqual(self.ledger().precharge(request_id='r1',object_id='cloud',start=0,end=4)['state'],'replay_no_body')
 def test_concurrent_processes_do_not_overspend(self):
  code="""import sys
from processing.reference_v2_precharge_draft import PrechargeLedger,PrechargeHeld
l=PrechargeLedger(sys.argv[1],inode=(int(sys.argv[2]),int(sys.argv[3])),epoch='epoch-1',clock=lambda:1793361600000)
try:l.precharge(request_id=sys.argv[4],object_id='cloud',start=0,end=4);print('charged')
except PrechargeHeld:print('held')
"""
  procs=[subprocess.Popen([sys.executable,'-c',code,str(self.path),str(self.inode[0]),str(self.inode[1]),name],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True) for name in ('r1','r2')]
  results=[]
  try:
   for child in procs:
    out,err=child.communicate(timeout=10);self.assertEqual(child.returncode,0,err);results.append(out.strip())
  finally:
   for child in procs:
    if child.poll() is None:child.kill();child.wait(timeout=5)
  self.assertEqual(sorted(results),['charged','held'])
