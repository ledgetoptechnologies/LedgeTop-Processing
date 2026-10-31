import json, socket, tempfile, unittest
from pathlib import Path
from unittest.mock import patch
from processing.node_gate import NodeGate,GateBusy
from processing.synthetic_gate_rpc import SyntheticGateRpc,SyntheticRpcHeld
from ops.native_node_fixture import NativeNodeFixture

class SyntheticGateRpcTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);root=Path(self.tmp.name)
  self.patch=patch('processing.node_gate.canonical_gate_path',return_value=root/'gate');self.patch.start();self.addCleanup(self.patch.stop)
  self.authority='11111111-1111-4111-8111-111111111111';self.receipt='a'*32
  self.job=dict(id='22222222-2222-4222-8222-222222222222',workerId='worker',nodeId='node',authorityId=self.authority)
  with NodeGate('synthetic-engine') as gate:
   gate.provision('node','http://127.0.0.1:18080',self.authority);gate.reserve('node','http://127.0.0.1:18080',self.authority,1,self.receipt);gate.bind(self.receipt,self.job)
  self.fixture=NativeNodeFixture(root/'fixture.db',synthetic_enabled=True).start();self.addCleanup(self.fixture.close)
  self.token='b'*64
 def server(self):
  return SyntheticGateRpc('synthetic-engine',self.fixture,node='node',origin='http://127.0.0.1:18080',authority=self.authority,generation=1,receipt=self.receipt,job=self.job,token=self.token,enabled=True)
 def call(self,server,**change):
  a,b=socket.socketpair()
  try:
   req=dict(token=self.token,jobId=self.job['id'],operation='initialize');req.update(change)
   b.sendall(json.dumps(req).encode()+b'\n');server.serve_one(a);return b.recv(1024)
  finally:a.close();b.close()
 def test_gate_owned_identity_and_one_mutation(self):
  with self.server() as server:
   with self.assertRaises(GateBusy):
    with NodeGate('synthetic-engine'):pass
   for change in (dict(token='c'*64),dict(jobId='33333333-3333-4333-8333-333333333333'),dict(operation='commit')):
    with self.assertRaises(SyntheticRpcHeld):self.call(server,**change)
   self.assertEqual(self.fixture.counts()['initialize'],0)
   self.assertIn(b'initialized_no_dispatch',self.call(server))
   with self.assertRaises(FileExistsError):self.call(server)
   self.assertEqual(self.fixture.counts()['initialize'],1)
 def test_competing_call_is_refused_while_mutation_runs(self):
  import threading
  from processing.viewer_strict_mutations import initialize
  entered=threading.Event();release=threading.Event();errors=[]
  def blocked(*args):
   entered.set()
   if not release.wait(3):raise RuntimeError('test timeout')
   initialize(*args)
  with self.server() as server,patch('processing.synthetic_gate_rpc.initialize',side_effect=blocked):
   def first():
    try:self.call(server)
    except BaseException as error:errors.append(error)
   thread=threading.Thread(target=first);thread.start()
   try:
    self.assertTrue(entered.wait(3))
    with self.assertRaisesRegex(SyntheticRpcHeld,'busy'):self.call(server)
   finally:release.set();thread.join(5)
   self.assertFalse(thread.is_alive());self.assertEqual(errors,[])
   self.assertEqual(self.fixture.counts()['initialize'],1)
 def test_uncertain_mutation_never_replays_after_restart(self):
  with self.server() as server,patch('processing.synthetic_gate_rpc.initialize',side_effect=OSError('synthetic loss')):
   with self.assertRaises(OSError):self.call(server)
  with self.server() as server:
   with self.assertRaises(FileExistsError):self.call(server)
  self.assertEqual(self.fixture.counts()['initialize'],0)
 def test_missing_gate_or_untrusted_fixture_fails_closed(self):
  server=self.server();server.receipt='f'*32
  with self.assertRaises(SyntheticRpcHeld):
   with server:pass
  with self.assertRaises(SyntheticRpcHeld):
   SyntheticGateRpc('synthetic-engine',object(),node='node',origin='http://127.0.0.1:18080',authority=self.authority,generation=1,receipt=self.receipt,job=self.job,token=self.token,enabled=True)
  self.assertEqual(self.fixture.counts()['initialize'],0)
