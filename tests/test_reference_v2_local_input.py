import hashlib,io,json,os,sqlite3,tempfile,unittest,uuid
from pathlib import Path
from unittest.mock import patch
from processing import reference_protocol as p
from processing.reference_manifest_v2 import DOMAIN
from processing.reference_input_journal import InputJournal
from processing.reference_worker_fence_v2 import WorkerFence
from processing.reference_v2_local_input import publish_next_input,verify_complete_input_set,InputHeld
from processing.scratch_policy import ScratchLedger

@unittest.skipUnless(os.name=='posix','POSIX only')
class LocalInputTest(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
  self.lock=self.root/'lock';self.lock.mkdir(mode=0o700);fd=os.open(self.lock/'worker.lock',os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600);os.close(fd)
  self.scratch=self.root/'scratch';self.scratch.mkdir(mode=0o700)
  self.db=sqlite3.connect(self.root/'journal');self.addCleanup(self.db.close);self.journal=InputJournal(self.db)
  self.ledger=ScratchLedger(self.root/'ledger',self.scratch,capacity_probe=lambda _:10000,safety_floor_bytes=0,cleanup_grace_seconds=0)
  self.fence=WorkerFence(self.lock);self.addCleanup(self.fence.close)
 def setup_input(self,content,reserve_bytes=None,second=b''):
  objects=[dict(id='a',name='a.jpg',byteSize=len(content),sha256=hashlib.sha256(content).hexdigest(),kind='image',mediaType='image/jpeg'),dict(id='b',name='b.jpg',byteSize=len(second),sha256=hashlib.sha256(second).hexdigest(),kind='image',mediaType='image/jpeg')]
  m=dict(id='fixture',revision=1,totalBytes=len(content)+len(second),objectCount=2,inputCounts=dict(images=2,lidarObjects=0,points=0),objects=objects)
  m['sha256']=hashlib.sha256(DOMAIN+p.canonical_json(m)).hexdigest();self.raw=p.canonical_json(m)
  projection=[{k:o[k] for k in ('id','byteSize','sha256')} for o in objects]
  self.binding=dict(jobId=str(uuid.uuid4()),environment='staging',generation=1,manifestSha256=m['sha256'],objectsSha256=p.digest(projection),maxAttempts=3)
  self.ledger.reserve(job_id=self.binding['jobId'],generation=1,reservation_id='r1',manifest_sha256=m['sha256'],input_bytes=len(content)+len(second) if reserve_bytes is None else reserve_bytes,output_budget=100,environment='staging',ttl_seconds=300,worker_ready=True)
 def run_input(self,source):
  return publish_next_input(fence=self.fence,journal=self.journal,ledger=self.ledger,reservation_id='r1',binding=self.binding,manifest_bytes=self.raw,source=source)
 def test_multiple_zero_objects_keep_hold(self):
  self.setup_input(b'');self.run_input(io.BytesIO(b''));self.run_input(io.BytesIO(b''))
  self.assertEqual((self.scratch/'jobs/r1/a.input').read_bytes(),b'')
  self.assertEqual((self.scratch/'jobs/r1/b.input').read_bytes(),b'')
  with self.assertRaisesRegex(InputHeld,'hold remains'):self.run_input(None)
 def test_ledger_checkpoint_interruption_reconciles_existing_file(self):
  self.setup_input(b'synthetic')
  original=self.journal.complete
  with patch.object(self.journal,'complete',side_effect=RuntimeError('interrupted')):
   with self.assertRaises(RuntimeError):self.run_input(io.BytesIO(b'synthetic'))
  self.journal.complete=original
  self.run_input(None)
  with self.ledger._transaction() as db:self.assertEqual(self.ledger._reservation(db,'r1')['allocated'],9)
 def test_process_exit_after_scratch_commit_recovers(self):
  import subprocess,sys
  self.setup_input(b'synthetic');self.fence.close();self.db.close()
  code="""import io,json,os,sqlite3,sys
from pathlib import Path
from processing.reference_input_journal import InputJournal
from processing.reference_worker_fence_v2 import WorkerFence
from processing.reference_v2_local_input import publish_next_input
from processing.scratch_policy import ScratchLedger
root=Path(sys.argv[1]);binding=json.loads(sys.argv[2]);raw=bytes.fromhex(sys.argv[3])
f=WorkerFence(root/'lock');db=sqlite3.connect(root/'journal');j=InputJournal(db)
l=ScratchLedger(root/'ledger',root/'scratch',capacity_probe=lambda _:10000,safety_floor_bytes=0,cleanup_grace_seconds=0)
original=l.record_allocated
def crash(*a,**kw):original(*a,**kw);os._exit(73)
l.record_allocated=crash
publish_next_input(fence=f,journal=j,ledger=l,reservation_id='r1',binding=binding,manifest_bytes=raw,source=io.BytesIO(b'synthetic'))
"""
  proc=subprocess.run([sys.executable,'-c',code,str(self.root),json.dumps(self.binding),self.raw.hex()],timeout=10)
  self.assertEqual(proc.returncode,73)
  self.db=sqlite3.connect(self.root/'journal');self.addCleanup(self.db.close);self.journal=InputJournal(self.db)
  self.fence=WorkerFence(self.lock);self.addCleanup(self.fence.close)
  self.run_input(None)
  with self.ledger._transaction() as db:self.assertEqual(self.ledger._reservation(db,'r1')['allocated'],9)

 def test_undersized_reservation_rejected_before_files(self):
  from processing.scratch_policy import PolicyConflict
  self.setup_input(b'expected',reserve_bytes=1)
  with self.assertRaises(PolicyConflict):self.run_input(io.BytesIO(b'expected'))
  self.assertFalse((self.scratch/'jobs').exists())

 def test_two_inputs_resume_after_second_allocation_commit(self):
  self.setup_input(b'abc',second=b'defg');self.run_input(io.BytesIO(b'abc'))
  self.db.close();self.db=sqlite3.connect(self.root/'journal');self.addCleanup(self.db.close);self.journal=InputJournal(self.db)
  with patch.object(self.journal,'complete',side_effect=RuntimeError('interrupted')):
   with self.assertRaises(RuntimeError):self.run_input(io.BytesIO(b'defg'))
  self.db.close();self.db=sqlite3.connect(self.root/'journal');self.addCleanup(self.db.close);self.journal=InputJournal(self.db)
  self.run_input(None)
  self.assertEqual((self.scratch/'jobs/r1/b.input').read_bytes(),b'defg')
  with self.ledger._transaction() as db:self.assertEqual(self.ledger._reservation(db,'r1')['allocated'],7)
  with self.assertRaisesRegex(InputHeld,'hold remains'):self.run_input(None)

 def test_full_barrier_rejects_removed_corrupted_and_extra_files(self):
  self.setup_input(b'abc',second=b'def');self.run_input(io.BytesIO(b'abc'));self.run_input(io.BytesIO(b'def'))
  def barrier():return verify_complete_input_set(fence=self.fence,journal=self.journal,ledger=self.ledger,reservation_id='r1',binding=self.binding,manifest_bytes=self.raw)
  self.assertEqual(barrier()['objectCount'],2)
  first=self.scratch/'jobs/r1/a.input';first.unlink()
  with self.assertRaises(InputHeld):barrier()
  first.write_bytes(b'xyz');first.chmod(0o600)
  with self.assertRaises(InputHeld):barrier()
  first.write_bytes(b'abc');extra=self.scratch/'jobs/r1/unexpected';extra.write_bytes(b'x')
  with self.assertRaises(InputHeld):barrier()
  extra.unlink();self.assertEqual(barrier()['state'],'input_set_verified_hold_retained')

 def test_node_gate_persists_on_failure_and_exact_replay(self):
  from processing.node_gate import NodeGate,GateError
  from processing.reference_v2_node_preflight import retain_node_for_verified_input
  self.setup_input(b'abc',second=b'def');self.run_input(io.BytesIO(b'abc'));self.run_input(io.BytesIO(b'def'))
  authority=str(uuid.uuid4()); key=b'k'*32; engine='synthetic-engine'; origin='https://coordinator.example'
  args=dict(fence=self.fence,journal=self.journal,ledger=self.ledger,reservation_id='r1',binding=self.binding,manifest_bytes=self.raw,
   engine_id=engine,node_id='main',origin=origin,authority_id=authority,generation=1,worker_id='worker',operation_key=key)
  with patch.dict(os.environ,{'XDG_STATE_HOME':str(self.root/'gate-state')}):
   with NodeGate(engine) as gate:gate.provision('main',origin,authority)
   from processing.reference_v2_node_preflight import operation_receipt
   receipt=operation_receipt(operation_key=key,binding=self.binding,reservation_id='r1',engine_id=engine,node_id='main',origin=origin,authority_id=authority,generation=1,worker_id='worker')
   first=self.scratch/'jobs/r1/a.input';first.unlink()
   with self.assertRaises(InputHeld):retain_node_for_verified_input(**args)
   with NodeGate(engine) as gate:self.assertEqual(gate.snapshot()['reservation']['receiptId'],receipt)
   with self.assertRaises(GateError):retain_node_for_verified_input(**dict(args,operation_key=b'z'*32))
   with self.assertRaises(GateError):retain_node_for_verified_input(**dict(args,reservation_id='another'))
   with self.assertRaises(GateError):retain_node_for_verified_input(**dict(args,binding=dict(self.binding,manifestSha256='f'*64)))
   first.write_bytes(b'abc');first.chmod(0o600)
   self.assertEqual(retain_node_for_verified_input(**args)['state'],'node_and_input_held')
   with NodeGate(engine) as gate:self.assertEqual(gate.snapshot()['reservation']['job']['id'],self.binding['jobId'])

 def test_last_ack_receipt_cannot_be_reused(self):
  from processing.node_gate import NodeGate,GateError
  from processing.reference_v2_node_preflight import retain_node_for_verified_input,operation_receipt
  self.setup_input(b'abc');authority=str(uuid.uuid4());key=b'x'*32;engine='synthetic-engine';origin='https://coordinator.example'
  args=dict(fence=self.fence,journal=self.journal,ledger=self.ledger,reservation_id='r1',binding=self.binding,manifest_bytes=self.raw,
   engine_id=engine,node_id='main',origin=origin,authority_id=authority,generation=1,worker_id='worker',operation_key=key)
  receipt=operation_receipt(operation_key=key,binding=self.binding,reservation_id='r1',engine_id=engine,node_id='main',origin=origin,authority_id=authority,generation=1,worker_id='worker')
  with patch.dict(os.environ,{'XDG_STATE_HOME':str(self.root/'gate-state')}):
   with NodeGate(engine) as gate:
    gate.provision('main',origin,authority);gate.reserve('main',origin,authority,1,receipt);gate.release_empty(receipt,{'job':None})
   with self.assertRaisesRegex(GateError,'previously resolved'):retain_node_for_verified_input(**args)
   with NodeGate(engine) as gate:
    state=gate.snapshot();state.pop('lastAcknowledgment');state['lastResolution']={'reservation':{'receiptId':receipt}};gate._save(state)
   with self.assertRaisesRegex(GateError,'previously resolved'):retain_node_for_verified_input(**args)

 def test_gate_reservation_survives_process_exit_before_bind(self):
  import subprocess,sys
  from processing.node_gate import NodeGate
  from processing.reference_v2_node_preflight import retain_node_for_verified_input
  self.setup_input(b'abc',second=b'def');self.run_input(io.BytesIO(b'abc'));self.run_input(io.BytesIO(b'def'))
  authority=str(uuid.uuid4()); key=b'k'*32; engine='synthetic-engine';origin='https://coordinator.example'
  from processing.reference_v2_node_preflight import operation_receipt
  receipt=operation_receipt(operation_key=key,binding=self.binding,reservation_id='r1',engine_id=engine,node_id='main',origin=origin,authority_id=authority,generation=1,worker_id='worker')
  with patch.dict(os.environ,{'XDG_STATE_HOME':str(self.root/'gate-state')}):
   with NodeGate(engine) as gate:gate.provision('main',origin,authority)
   code="""import os,sys
from processing.node_gate import NodeGate
with NodeGate('synthetic-engine') as g:
 g.reserve('main','https://coordinator.example',sys.argv[1],1,sys.argv[2])
 os._exit(73)
"""
   proc=subprocess.run([sys.executable,'-c',code,authority,receipt],timeout=10)
   self.assertEqual(proc.returncode,73)
   result=retain_node_for_verified_input(fence=self.fence,journal=self.journal,ledger=self.ledger,reservation_id='r1',binding=self.binding,manifest_bytes=self.raw,
    engine_id=engine,node_id='main',origin=origin,authority_id=authority,generation=1,worker_id='worker',operation_key=key)
   self.assertEqual(result['state'],'node_and_input_held')

 def make_init_database(self):
  from processing.reference_v2_init_boundary import DDL,META_DDL
  path=self.root/'init.sqlite';db=sqlite3.connect(path)
  db.execute(META_DDL);db.execute(DDL);db.execute('INSERT INTO v2_init_epoch VALUES(1,?)',('f'*64,));db.commit()
  st=path.stat()
  return db,(st.st_dev,st.st_ino)

 def init_claim(self):
  return dict(jobId=self.binding['jobId'],manifestSha256=self.binding['manifestSha256'],generation=1,engineId='synthetic-engine',engineOrigin='http://127.0.0.1:3001',options=[dict(name='test-flag',value=True)])

 @unittest.skip("hard-disabled pending SQLite connection-origin qualification")
 def test_initialize_lost_response_never_repeats(self):
  from processing.node_gate import NodeGate
  from processing.reference_v2_node_preflight import retain_node_for_verified_input
  from processing.reference_v2_init_boundary import initialize_once,InitHeld
  self.setup_input(b'abc');self.run_input(io.BytesIO(b'abc'));self.run_input(io.BytesIO(b''))
  authority=str(uuid.uuid4());key=b'k'*32;engineid='synthetic-engine';origin='https://coordinator.example'
  shared=dict(fence=self.fence,journal=self.journal,ledger=self.ledger,reservation_id='r1',binding=self.binding,manifest_bytes=self.raw,
   engine_id=engineid,node_id='main',origin=origin,authority_id=authority,generation=1,worker_id='worker',operation_key=key)
  class Lost:
   origin='http://127.0.0.1:3001'
   calls=0
   def initialize(self,task):self.calls+=1;self.task=task;raise OSError('response lost')
  engine=Lost();db,inode=self.make_init_database();self.addCleanup(db.close)
  with patch.dict(os.environ,{'XDG_STATE_HOME':str(self.root/'gate-state')}):
   with NodeGate(engineid) as gate:gate.provision('main',origin,authority)
   retain_node_for_verified_input(**shared)
   shared.update(expected_epoch='f'*64,expected_inode=inode,authenticated_claim=self.init_claim())
   with self.assertRaises(InitHeld):initialize_once(enabled=True,db=db,engine=engine,**dict(shared,expected_inode=(0,0)))
   with self.assertRaises(InitHeld):initialize_once(enabled=True,db=db,engine=engine,**dict(shared,expected_epoch='e'*64))
   with self.assertRaises(InitHeld):initialize_once(enabled=True,db=db,engine=engine,**dict(shared,authenticated_claim=dict(self.init_claim(),engineOrigin='http://127.0.0.1:1')))
   with self.assertRaises(InitHeld):initialize_once(enabled=True,db=db,engine=engine,**dict(shared,authenticated_claim=dict(self.init_claim(),manifestSha256='a'*64)))
   empty=sqlite3.connect(self.root/'empty.sqlite');self.addCleanup(empty.close);st=(self.root/'empty.sqlite').stat()
   with self.assertRaises(InitHeld):initialize_once(enabled=True,db=empty,engine=engine,**dict(shared,expected_inode=(st.st_dev,st.st_ino)))
   self.assertEqual(engine.calls,0)
   with self.assertRaises(OSError):initialize_once(enabled=True,db=db,engine=engine,**shared)
   with self.assertRaisesRegex(InitHeld,'already attempted'):initialize_once(enabled=True,db=db,engine=engine,**shared)
   self.assertEqual(engine.calls,1);self.assertEqual(engine.task['options'],self.init_claim()['options'])
   with NodeGate(engineid) as gate:self.assertIsNotNone(gate.snapshot()['reservation'])
   with self.ledger._transaction() as sd:self.assertIsNotNone(sd.execute("SELECT 1 FROM input_write_holds WHERE reservation='r1'").fetchone())

 @unittest.skip("hard-disabled pending SQLite connection-origin qualification")
 def test_initialize_process_death_never_repeats(self):
  import subprocess,sys
  from processing.node_gate import NodeGate
  from processing.reference_v2_node_preflight import retain_node_for_verified_input
  from processing.reference_v2_init_boundary import initialize_once,InitHeld
  self.setup_input(b'abc');self.run_input(io.BytesIO(b'abc'));self.run_input(io.BytesIO(b''))
  authority=str(uuid.uuid4());key=b'k'*32;engineid='synthetic-engine';origin='https://coordinator.example'
  shared=dict(fence=self.fence,journal=self.journal,ledger=self.ledger,reservation_id='r1',binding=self.binding,manifest_bytes=self.raw,
   engine_id=engineid,node_id='main',origin=origin,authority_id=authority,generation=1,worker_id='worker',operation_key=key)
  with patch.dict(os.environ,{'XDG_STATE_HOME':str(self.root/'gate-state')}):
   with NodeGate(engineid) as gate:gate.provision('main',origin,authority)
   retain_node_for_verified_input(**shared);self.fence.close();self.db.close()
   initdb,inode=self.make_init_database();initdb.close();claim=self.init_claim()
   code="""import os,sys,json,sqlite3
from pathlib import Path
from processing.reference_input_journal import InputJournal
from processing.reference_worker_fence_v2 import WorkerFence
from processing.reference_v2_init_boundary import initialize_once
from processing.scratch_policy import ScratchLedger
root=Path(sys.argv[1]);binding=json.loads(sys.argv[2]);raw=bytes.fromhex(sys.argv[3]);authority=sys.argv[4]
f=WorkerFence(root/'lock');jdb=sqlite3.connect(root/'journal');j=InputJournal(jdb)
l=ScratchLedger(root/'ledger',root/'scratch',capacity_probe=lambda _:10000,safety_floor_bytes=0,cleanup_grace_seconds=0)
db=sqlite3.connect(root/'init.sqlite')
class Engine:
 origin='http://127.0.0.1:3001'
 def initialize(self,task):os._exit(73)
initialize_once(enabled=True,db=db,engine=Engine(),fence=f,journal=j,ledger=l,reservation_id='r1',binding=binding,manifest_bytes=raw,engine_id='synthetic-engine',node_id='main',origin='https://coordinator.example',authority_id=authority,generation=1,worker_id='worker',operation_key=b'k'*32,expected_epoch='f'*64,expected_inode=tuple(json.loads(sys.argv[5])),authenticated_claim=json.loads(sys.argv[6]))
"""
   proc=subprocess.run([sys.executable,'-c',code,str(self.root),json.dumps(self.binding),self.raw.hex(),authority,json.dumps(inode),json.dumps(claim)],timeout=10)
   self.assertEqual(proc.returncode,73)
   self.db=sqlite3.connect(self.root/'journal');self.addCleanup(self.db.close);self.journal=InputJournal(self.db)
   self.fence=WorkerFence(self.lock);self.addCleanup(self.fence.close);shared.update(fence=self.fence,journal=self.journal)
   shared.update(expected_epoch='f'*64,expected_inode=inode,authenticated_claim=claim)
   class Engine:
    origin='http://127.0.0.1:3001'
    def initialize(self,task):raise AssertionError('duplicate initialize')
   db=sqlite3.connect(self.root/'init.sqlite');self.addCleanup(db.close)
   with self.assertRaisesRegex(InitHeld,'already attempted'):initialize_once(enabled=True,db=db,engine=Engine(),**shared)

 def test_old_open_connection_with_replaced_path_is_refused(self):
  from processing.reference_v2_init_boundary import initialize_once,InitHeld
  path=self.root/'swap.sqlite';old=sqlite3.connect(path);self.addCleanup(old.close)
  old.execute('CREATE TABLE original (id INTEGER)');old.commit()
  os.rename(path,self.root/'old.sqlite')
  new=sqlite3.connect(path);new.execute('CREATE TABLE replacement (id INTEGER)');new.commit();new.close()
  st=path.stat()
  class Engine:
   def initialize(self,task):raise AssertionError('must not issue initialize')
  with self.assertRaisesRegex(InitHeld,'connection-origin'):
   initialize_once(enabled=True,db=old,engine=Engine(),fence=None,journal=None,ledger=None,reservation_id='r',binding={},manifest_bytes=b'',engine_id='e',node_id='n',origin='x',authority_id='a',generation=1,worker_id='w',operation_key=b'k'*32,expected_epoch='f'*64,expected_inode=(st.st_dev,st.st_ino),authenticated_claim={})

 def test_insufficient_remaining_blocks_second_file(self):
  from processing.scratch_policy import PolicyConflict
  self.setup_input(b'abc',second=b'defg');self.run_input(io.BytesIO(b'abc'))
  db=sqlite3.connect(self.root/'ledger')
  try:db.execute("UPDATE reservations SET remaining=0 WHERE id='r1'");db.commit()
  finally:db.close()
  with self.assertRaises(PolicyConflict):self.run_input(io.BytesIO(b'defg'))
  self.assertFalse((self.scratch/'jobs/r1/b.part').exists())

 def test_bad_bytes_leave_partial_and_hold(self):
  self.setup_input(b'expected')
  with self.assertRaises(InputHeld):self.run_input(io.BytesIO(b'wrong'))
  with self.assertRaisesRegex(InputHeld,'partial'):self.run_input(io.BytesIO(b'expected'))
