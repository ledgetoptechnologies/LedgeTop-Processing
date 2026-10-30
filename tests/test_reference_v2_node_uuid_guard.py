import os,sqlite3,unittest,uuid
from pathlib import Path
from unittest.mock import patch
from tests.test_reference_v2_controlled_sqlite import ControlledSQLiteTest
from processing.reference_v2_controlled_sqlite import ControlledSQLite
from processing.reference_v2_node_uuid_guard import guarded_initialize,NodeUuidHeld
from processing.node_gate import NodeGate

@unittest.skipUnless(os.name=='posix','POSIX required')
class UuidGuardTest(ControlledSQLiteTest):
 def prepare(self):
  self.tmpdir=self.root/'node-tmp';self.datadir=self.root/'node-data';self.tmpdir.mkdir();self.datadir.mkdir()
  def ident(p):st=p.stat();return (st.st_dev,st.st_ino)
  self.store=ControlledSQLite(fence=self.fence,path=self.path,expected_inode=self.inode,expected_epoch='epoch-1');self.addCleanup(self.store.close)
  self.job=str(uuid.uuid4());self.candidate=uuid.UUID('11111111-2222-4333-8444-555555555555');authority=str(uuid.uuid4())
  self.gate=NodeGate('synthetic');self.gate.__enter__();self.addCleanup(self.gate.__exit__,None,None,None)
  self.gate.provision('main','https://coordinator.example',authority);self.gate.reserve('main','https://coordinator.example',authority,1,'a'*32)
  self.gate.bind('a'*32,dict(id=self.job,workerId='worker',nodeId='main',authorityId=authority))
  class Engine:
   calls=0;tasks=[]
   def read(self,path):return self.tasks
   def initialize(self,task):self.calls+=1;self.last=task;raise OSError('response lost')
  self.engine=Engine();self.args=dict(enabled=True,intent=self.store,fence=self.fence,gate=self.gate,engine=self.engine,job_id=self.job,task_name='synthetic',options=[],tmp_dir=self.tmpdir,data_dir=self.datadir,tmp_identity=ident(self.tmpdir),data_identity=ident(self.datadir))
 def test_existing_tmp_and_data_refuse_without_intent(self):
  with patch.dict(os.environ,{'XDG_STATE_HOME':str(self.root/'gate-state')}):
   self.prepare()
   with patch('processing.reference_v2_node_uuid_guard.uuid.uuid4',return_value=self.candidate):
    for directory in (self.tmpdir,self.datadir):
     path=directory/str(self.candidate);path.mkdir()
     with self.assertRaises(NodeUuidHeld):guarded_initialize(**self.args)
     path.rmdir()
   self.assertEqual(self.engine.calls,0);self.assertIsNone(self.store.lookup(self.job))
 def test_existing_managed_uuid_and_lost_response_never_repeat(self):
  with patch.dict(os.environ,{'XDG_STATE_HOME':str(self.root/'gate-state')}):
   self.prepare()
   with patch('processing.reference_v2_node_uuid_guard.uuid.uuid4',return_value=self.candidate):
    self.engine.tasks=[dict(uuid=str(self.candidate))]
    with self.assertRaises(NodeUuidHeld):guarded_initialize(**self.args)
    self.engine.tasks=[]
    with self.assertRaises(OSError):guarded_initialize(**self.args)
    with self.assertRaises(NodeUuidHeld):guarded_initialize(**self.args)
   self.assertEqual(self.engine.calls,1);self.assertEqual(self.engine.last['engine_uuid'],str(self.candidate));self.assertIn(str(self.candidate),self.store.lookup(self.job))
