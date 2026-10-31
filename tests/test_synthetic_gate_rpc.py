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
 def test_listener_refuses_preexisting_fixture_database_without_touching_it(self):
  import hashlib,os
  from processing.synthetic_gate_rpc import SyntheticGateServer
  root=Path(self.tmp.name)/'private';root.mkdir(mode=0o700)
  sentinel=root/'synthetic-engine.sqlite';sentinel.write_bytes(b'untouched sentinel');sentinel.chmod(0o640)
  before=(sentinel.stat().st_mode,sentinel.stat().st_ino,hashlib.sha256(sentinel.read_bytes()).digest())
  server=SyntheticGateServer(root,'synthetic-engine',node='node',origin='http://127.0.0.1:18080',authority=self.authority,generation=1,receipt=self.receipt,job=self.job,token=self.token,enabled=True)
  with self.assertRaises(FileExistsError):server.__enter__()
  self.assertEqual((sentinel.stat().st_mode,sentinel.stat().st_ino,hashlib.sha256(sentinel.read_bytes()).digest()),before)
 def test_concurrent_server_creation_does_not_share_fixture_database(self):
  import threading
  from concurrent.futures import ThreadPoolExecutor
  from processing.synthetic_gate_rpc import SyntheticGateServer
  root=Path(self.tmp.name)/'private';root.mkdir(mode=0o700);barrier=threading.Barrier(2);servers=[]
  def create():
   server=SyntheticGateServer(root,'synthetic-engine',node='node',origin='http://127.0.0.1:18080',authority=self.authority,generation=1,receipt=self.receipt,job=self.job,token=self.token,enabled=True)
   barrier.wait(timeout=5)
   try:server.__enter__();servers.append(server);return 'started'
   except (FileExistsError,SyntheticRpcHeld):return 'held'
  try:
   with ThreadPoolExecutor(max_workers=2) as pool:
    results=list(pool.map(lambda _:create(),range(2)))
   self.assertEqual(sorted(results),['held','started']);self.assertEqual(len(servers),1)
   self.assertTrue((root/'synthetic-engine.sqlite').is_file())
  finally:
   for server in servers:server.close()
 def test_missing_gate_or_untrusted_fixture_fails_closed(self):
  server=self.server();server.receipt='f'*32
  with self.assertRaises(SyntheticRpcHeld):
   with server:pass
  with self.assertRaises(SyntheticRpcHeld):
   SyntheticGateRpc('synthetic-engine',object(),node='node',origin='http://127.0.0.1:18080',authority=self.authority,generation=1,receipt=self.receipt,job=self.job,token=self.token,enabled=True)
  self.assertEqual(self.fixture.counts()['initialize'],0)

@unittest.skipUnless(hasattr(socket,'SO_PEERCRED'),'Linux peer credentials required')
class SyntheticGateProcessTests(unittest.TestCase):
 def test_peer_credential_rejects_different_uid(self):
  import os,struct
  from unittest.mock import Mock
  from processing.synthetic_gate_rpc import SyntheticGateServer
  connection=Mock();server=object.__new__(SyntheticGateServer)
  connection.getsockopt.return_value=struct.pack('3i',123,os.geteuid()+1,0)
  self.assertFalse(server._same_uid(connection))
  connection.getsockopt.return_value=struct.pack('3i',123,os.geteuid(),0)
  self.assertTrue(server._same_uid(connection))
 def test_process_kill_leaves_intent_and_stale_socket_held(self):
  import os,select,signal,subprocess,sys,time
  from processing.node_gate import canonical_gate_path
  with tempfile.TemporaryDirectory() as temp:
   root=Path(temp);os.chmod(root,0o700);state=root/'state';state.mkdir(mode=0o700)
   authority='11111111-1111-4111-8111-111111111111';receipt='a'*32;job=dict(id='22222222-2222-4222-8222-222222222222',workerId='worker',nodeId='node',authorityId=authority)
   with patch.dict(os.environ,{'XDG_STATE_HOME':str(state)}):
    gate_path=canonical_gate_path('owned-synthetic')
    with NodeGate('owned-synthetic') as gate:
     gate.provision('node','http://127.0.0.1:18080',authority);gate.reserve('node','http://127.0.0.1:18080',authority,1,receipt);gate.bind(receipt,job)
   code="""import json,os,sys,threading,time
from processing.synthetic_gate_rpc import SyntheticGateServer
import processing.synthetic_gate_rpc as module
job=json.loads(sys.argv[2])
if sys.argv[3]=='block':
 def block(*args):time.sleep(30)
 module.initialize=block
with SyntheticGateServer(sys.argv[1],'owned-synthetic',node='node',origin='http://127.0.0.1:18080',authority=job['authorityId'],generation=1,receipt='a'*32,job=job,token='b'*64,enabled=True) as server:
 print('ready',flush=True);server.serve(threading.Event())
"""
   env=dict(os.environ,XDG_STATE_HOME=str(state));child=None;sockpath=root/'synthetic-gate.sock';client=None
   try:
    child=subprocess.Popen([sys.executable,'-c',code,str(root),json.dumps(job),'block'],env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
    self.assertTrue(select.select([child.stdout],[],[],5)[0]);self.assertEqual(child.stdout.readline().strip(),'ready')
    info=sockpath.lstat();identity=(info.st_dev,info.st_ino)
    denied=socket.socket(socket.AF_UNIX);denied.settimeout(5);denied.connect(str(sockpath))
    denied.sendall(json.dumps(dict(token='c'*64,jobId=job['id'],operation='initialize')).encode()+b'\n')
    self.assertIn(b'held',denied.recv(1024));denied.close()
    self.assertFalse((gate_path/('synthetic-rpc-init-'+job['id']+'.intent')).exists())
    client=socket.socket(socket.AF_UNIX);client.settimeout(5);client.connect(str(sockpath))
    client.sendall(json.dumps(dict(token='b'*64,jobId=job['id'],operation='initialize')).encode()+b'\n')
    marker=gate_path/('synthetic-rpc-init-'+job['id']+'.intent');deadline=time.monotonic()+5
    while not marker.exists() and time.monotonic()<deadline:time.sleep(.02)
    self.assertTrue(marker.exists());child.kill();child.wait(timeout=5);child.stdout.close();child.stderr.close();child=None;client.close();client=None
    # Stale socket is held, never silently replaced.
    probe=subprocess.run([sys.executable,'-c',code,str(root),json.dumps(job),'normal'],env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=5)
    self.assertNotEqual(probe.returncode,0)
    current=sockpath.lstat();self.assertEqual((current.st_dev,current.st_ino),identity);sockpath.unlink()
    # Preserve the task-owned engine DB under a verified new name; server never replaces it.
    db=root/'synthetic-engine.sqlite';db_stat=db.lstat();preserved=root/'previous-synthetic-engine.sqlite'
    self.assertFalse(preserved.exists());db.rename(preserved);self.assertEqual((preserved.stat().st_dev,preserved.stat().st_ino),(db_stat.st_dev,db_stat.st_ino))
    child=subprocess.Popen([sys.executable,'-c',code,str(root),json.dumps(job),'normal'],env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
    self.assertTrue(select.select([child.stdout],[],[],5)[0]);self.assertEqual(child.stdout.readline().strip(),'ready')
    client=socket.socket(socket.AF_UNIX);client.settimeout(5);client.connect(str(sockpath));client.sendall(json.dumps(dict(token='b'*64,jobId=job['id'],operation='initialize')).encode()+b'\n')
    self.assertIn(b'held',client.recv(1024));self.assertTrue(marker.exists())
   finally:
    if client is not None:client.close()
    if child is not None:
     if child.poll() is None:child.kill()
     child.wait(timeout=5);child.stdout.close();child.stderr.close()
