import io,json,os,sqlite3,subprocess,sys,uuid,unittest
from unittest.mock import patch
from tests.test_reference_v2_local_input import LocalInputTest
from processing.node_gate import NodeGate
from processing.reference_v2_node_preflight import retain_node_for_verified_input
from processing.reference_v2_controlled_sqlite import ControlledSQLite,EPOCH_DDL,INTENT_DDL
from processing.reference_v2_supervised_init import initialize_once,InitHeld

@unittest.skipUnless(os.name=='posix','POSIX only')
class SupervisedInitTest(LocalInputTest):
 def prepare(self):
  self.setup_input(b'abc');self.run_input(io.BytesIO(b'abc'));self.run_input(io.BytesIO(b''))
  self.authority=str(uuid.uuid4());self.origin='https://coordinator.example';self.engineid='synthetic-engine';self.key=b'k'*32
  self.shared=dict(fence=self.fence,journal=self.journal,ledger=self.ledger,reservation_id='r1',binding=self.binding,manifest_bytes=self.raw,engine_id=self.engineid,node_id='main',origin=self.origin,authority_id=self.authority,generation=1,worker_id='worker',operation_key=self.key)
  self.claim=dict(jobId=self.binding['jobId'],manifestSha256=self.binding['manifestSha256'],generation=1,engineId=self.engineid,engineOrigin='http://127.0.0.1:3001',options=[dict(name='feature-quality',value='high')])
  self.intentdir=self.root/'intent';self.intentdir.mkdir(mode=0o700);self.intentpath=self.intentdir/'db'
  db=sqlite3.connect(self.intentpath);db.execute(EPOCH_DDL);db.execute(INTENT_DDL);db.execute("INSERT INTO trusted_epoch VALUES(1,'epoch-1')");db.commit();db.close();self.intentpath.chmod(0o600)
  st=self.intentpath.stat();self.inode=(st.st_dev,st.st_ino)
  with NodeGate(self.engineid) as gate:gate.provision('main',self.origin,self.authority)
  retain_node_for_verified_input(**self.shared)
 def open_intent(self):return ControlledSQLite(fence=self.fence,path=self.intentpath,expected_inode=self.inode,expected_epoch='epoch-1')
 def test_lost_reply_never_duplicates_and_preserves_options(self):
  with patch.dict(os.environ,{'XDG_STATE_HOME':str(self.root/'gate-state')}):
   self.prepare();i=self.open_intent()
   class Lost:
    origin='http://127.0.0.1:3001';calls=0
    def initialize(self,task):self.calls+=1;self.task=task;raise OSError('lost')
   engine=Lost()
   try:
    with self.assertRaises(OSError):initialize_once(enabled=True,intent=i,engine=engine,authenticated_claim=self.claim,**self.shared)
    with self.assertRaises(InitHeld):initialize_once(enabled=True,intent=i,engine=engine,authenticated_claim=dict(self.claim,options=[]),**self.shared)
    self.assertEqual(engine.calls,1);self.assertEqual(engine.task['options'],self.claim['options'])
   finally:i.close()
 def test_process_exit_after_intent_prevents_retry(self):
  with patch.dict(os.environ,{'XDG_STATE_HOME':str(self.root/'gate-state')}):
   self.prepare();self.fence.close();self.db.close()
   code="""import os,sys,json,sqlite3
from pathlib import Path
from processing.reference_input_journal import InputJournal
from processing.reference_worker_fence_v2 import WorkerFence
from processing.reference_v2_controlled_sqlite import ControlledSQLite
from processing.reference_v2_supervised_init import initialize_once
from processing.scratch_policy import ScratchLedger
root=Path(sys.argv[1]);binding=json.loads(sys.argv[2]);raw=bytes.fromhex(sys.argv[3]);authority=sys.argv[4];inode=tuple(json.loads(sys.argv[5]));claim=json.loads(sys.argv[6])
f=WorkerFence(root/'lock');jdb=sqlite3.connect(root/'journal');j=InputJournal(jdb)
l=ScratchLedger(root/'ledger',root/'scratch',capacity_probe=lambda _:10000,safety_floor_bytes=0,cleanup_grace_seconds=0)
i=ControlledSQLite(fence=f,path=root/'intent/db',expected_inode=inode,expected_epoch='epoch-1')
class Engine:
 origin='http://127.0.0.1:3001'
 def initialize(self,task):os._exit(73)
initialize_once(enabled=True,intent=i,engine=Engine(),fence=f,journal=j,ledger=l,reservation_id='r1',binding=binding,manifest_bytes=raw,engine_id='synthetic-engine',node_id='main',origin='https://coordinator.example',authority_id=authority,generation=1,worker_id='worker',operation_key=b'k'*32,authenticated_claim=claim)
"""
   result=subprocess.run([sys.executable,'-c',code,str(self.root),json.dumps(self.binding),self.raw.hex(),self.authority,json.dumps(self.inode),json.dumps(self.claim)],timeout=10);self.assertEqual(result.returncode,73)
   self.db=sqlite3.connect(self.root/'journal');self.addCleanup(self.db.close);self.journal=__import__('processing.reference_input_journal',fromlist=['InputJournal']).InputJournal(self.db)
   self.fence=__import__('processing.reference_worker_fence_v2',fromlist=['WorkerFence']).WorkerFence(self.lock);self.addCleanup(self.fence.close);self.shared.update(fence=self.fence,journal=self.journal)
   i=self.open_intent()
   class Engine:
    origin='http://127.0.0.1:3001'
    def initialize(self,task):raise AssertionError('duplicate')
   try:
    with self.assertRaises(InitHeld):initialize_once(enabled=True,intent=i,engine=Engine(),authenticated_claim=self.claim,**self.shared)
   finally:i.close()
