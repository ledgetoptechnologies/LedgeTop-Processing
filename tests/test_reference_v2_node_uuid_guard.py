import io,json,os,sqlite3,subprocess,sys,unittest,uuid
from unittest.mock import patch
from tests.test_reference_v2_supervised_init import SupervisedInitTest
from processing.node_gate import NodeGate
from processing.reference_v2_node_uuid_guard import guarded_initialize,NodeUuidHeld
from processing.reference_v2_supervised_init import InitHeld

@unittest.skipUnless(os.name=='posix','POSIX required')
class UuidGuardTest(SupervisedInitTest):
 def prepare_guard(self):
  self.prepare();self.tmpdir=self.root/'node-tmp';self.datadir=self.root/'node-data';self.tmpdir.mkdir();self.datadir.mkdir()
  def ident(p):st=p.stat();return (st.st_dev,st.st_ino)
  self.store=self.open_intent();self.addCleanup(self.store.close)
  self.gate=NodeGate(self.engineid);self.gate.__enter__();self.addCleanup(self.gate.__exit__,None,None,None)
  self.candidate=uuid.UUID('11111111-2222-4333-8444-555555555555')
  class Engine:
   origin='http://127.0.0.1:3001';calls=0;tasks=[]
   def read(self,path):return self.tasks
   def initialize(self,task):self.calls+=1;self.last=task;raise OSError('response lost')
  self.engine=Engine();self.args=dict(enabled=True,intent=self.store,gate=self.gate,engine=self.engine,job_id=self.binding['jobId'],task_name='synthetic',authenticated_claim=self.claim,tmp_dir=self.tmpdir,data_dir=self.datadir,tmp_identity=ident(self.tmpdir),data_identity=ident(self.datadir),**self.shared)
 def test_guard_rejects_namespace_collision_and_mismatch(self):
  with patch.dict(os.environ,{'XDG_STATE_HOME':str(self.root/'gate-state')}):
   self.prepare_guard()
   with patch('processing.reference_v2_node_uuid_guard.uuid.uuid4',return_value=self.candidate):
    for directory in (self.tmpdir,self.datadir):
     path=directory/str(self.candidate);path.mkdir()
     with self.assertRaises(NodeUuidHeld):guarded_initialize(**self.args)
     path.rmdir()
    self.engine.tasks=[dict(uuid=str(self.candidate))]
    with self.assertRaises(NodeUuidHeld):guarded_initialize(**self.args)
    self.engine.tasks=[]
    with self.assertRaises((NodeUuidHeld,InitHeld)):guarded_initialize(**dict(self.args,authenticated_claim=dict(self.claim,manifestSha256='f'*64)))
    with self.assertRaises(NodeUuidHeld):guarded_initialize(**dict(self.args,reservation_id='other'))
   self.assertEqual(self.engine.calls,0);self.assertIsNone(self.store.lookup(self.binding['jobId']))
 def test_process_death_after_uuid_intent_prevents_repeat(self):
  from processing.reference_input_journal import InputJournal
  from processing.reference_worker_fence_v2 import WorkerFence
  with patch.dict(os.environ,{'XDG_STATE_HOME':str(self.root/'gate-state')}):
   self.prepare_guard();self.gate.__exit__(None,None,None);self.store.close();self.fence.close();self.db.close()
   code="""import os,sys,json,sqlite3
from pathlib import Path
from processing.reference_input_journal import InputJournal
from processing.reference_worker_fence_v2 import WorkerFence
from processing.reference_v2_controlled_sqlite import ControlledSQLite
from processing.reference_v2_node_uuid_guard import guarded_initialize
from processing.node_gate import NodeGate
from processing.scratch_policy import ScratchLedger
root=Path(sys.argv[1]);binding=json.loads(sys.argv[2]);raw=bytes.fromhex(sys.argv[3]);authority=sys.argv[4];claim=json.loads(sys.argv[5]);inode=tuple(json.loads(sys.argv[6]))
def ident(path):s=path.stat();return (s.st_dev,s.st_ino)
f=WorkerFence(root/'lock');jdb=sqlite3.connect(root/'journal');j=InputJournal(jdb)
l=ScratchLedger(root/'ledger',root/'scratch',capacity_probe=lambda _:10000,safety_floor_bytes=0,cleanup_grace_seconds=0)
i=ControlledSQLite(fence=f,path=root/'intent/db',expected_inode=inode,expected_epoch='epoch-1')
class Engine:
 origin='http://127.0.0.1:3001'
 def read(self,path):return []
 def initialize(self,task):os._exit(73)
with NodeGate('synthetic-engine') as gate:
 guarded_initialize(enabled=True,intent=i,fence=f,gate=gate,engine=Engine(),job_id=binding['jobId'],task_name='synthetic',binding=binding,manifest_bytes=raw,journal=j,ledger=l,reservation_id='r1',engine_id='synthetic-engine',node_id='main',origin='https://coordinator.example',authority_id=authority,generation=1,worker_id='worker',operation_key=b'k'*32,authenticated_claim=claim,tmp_dir=root/'node-tmp',data_dir=root/'node-data',tmp_identity=ident(root/'node-tmp'),data_identity=ident(root/'node-data'))
"""
   result=subprocess.run([sys.executable,'-c',code,str(self.root),json.dumps(self.binding),self.raw.hex(),self.authority,json.dumps(self.claim),json.dumps(self.inode)],timeout=10);self.assertEqual(result.returncode,73)
   self.db=sqlite3.connect(self.root/'journal');self.addCleanup(self.db.close);self.journal=InputJournal(self.db)
   self.fence=WorkerFence(self.lock);self.addCleanup(self.fence.close);self.store=self.open_intent();self.addCleanup(self.store.close)
   self.gate=NodeGate(self.engineid);self.gate.__enter__();self.addCleanup(self.gate.__exit__,None,None,None)
   self.args.update(fence=self.fence,journal=self.journal,intent=self.store,gate=self.gate)
   with self.assertRaises(NodeUuidHeld):guarded_initialize(**self.args)
   self.assertEqual(self.engine.calls,0)

 def test_lost_response_retains_uuid_and_never_repeats(self):
  with patch.dict(os.environ,{'XDG_STATE_HOME':str(self.root/'gate-state')}):
   self.prepare_guard()
   with patch('processing.reference_v2_node_uuid_guard.uuid.uuid4',return_value=self.candidate):
    with self.assertRaises(OSError):guarded_initialize(**self.args)
    with self.assertRaises(NodeUuidHeld):guarded_initialize(**self.args)
   self.assertEqual(self.engine.calls,1);self.assertEqual(self.engine.last['engine_uuid'],str(self.candidate));self.assertIn(str(self.candidate),self.store.lookup(self.binding['jobId']))
