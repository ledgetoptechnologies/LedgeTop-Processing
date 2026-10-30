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
