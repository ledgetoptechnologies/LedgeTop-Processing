"""Loopback pair: real worker/provider and facade, synthetic repo/capability.

Viewer SQLite declaration is real; coordinator restarts within the test process.
Capability advertisement and storage budget are synthetic; no live auth.
"""
import json,os,subprocess,tempfile,threading,unittest
from pathlib import Path
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from types import SimpleNamespace
from processing.prototype import Queue
from processing.viewer_service import ViewerService
from processing.viewer_http import ViewerFacade
from processing.scratch_policy import ScratchLedger
from processing.viewer_upload_capacity_candidate import ViewerUploadCapacity
from processing.viewer_limits import MAX_UPLOAD_BYTES
from tests.test_viewer_service import FakeFleet,FakeEngine,OPTIONS

TOKEN='synthetic-capacity-test-token-123456789'
class ViewerCapacityPair(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name);self.server=None
  self.spool=self.root/'spool';self.spool.mkdir(mode=0o700);self.start();self.addCleanup(self.stop)
 def start(self):
  root=self.root;config=SimpleNamespace(prototype_state_dir=str(root/'state'),viewer_max_input_bytes=10000,viewer_max_archive_bytes=4*1024**2,local_node_url='http://fake.invalid',local_node_token='fake',main_node_url='http://fake.invalid',main_node_token='fake',local_max_photos=1000,local_max_input_bytes=10000)
  self.queue=Queue(root/'state',node_ids=('cluster','main'),treedar_node_ids=('main',),viewer_enabled=True)
  fleet=FakeFleet();engines={n:FakeEngine(n,fleet) for n in ('cluster','main')};self.service=ViewerService(self.queue,config,engines=engines,probe=lambda *args:{'ready':True,'maxImages':1000,'taskQueueCount':0})
  self.service._catalog=lambda:{'ready':True,'commonOptions':[dict(x) for x in OPTIONS]};self.service._selected_options=lambda _:[dict(x) for x in OPTIONS]
  original=self.service.capabilities;self.service.capabilities=lambda:{**original(),'ledgetopUploadAdmission':{'version':1,'declaredInputBytesHeader':'X-LTDS-Declared-Input-Bytes'}} # fixture only
  self.ledger=ScratchLedger(root/'capacity.sqlite',root,capacity_probe=lambda _:MAX_UPLOAD_BYTES+100000,safety_floor_bytes=100)
  gate=ViewerUploadCapacity(self.ledger,self.service.store,self.spool,synthetic_only=True);facade=ViewerFacade(self.service,TOKEN,temp_dir=self.spool,upload_capacity=gate);self.facade=facade
  class Handler(BaseHTTPRequestHandler):
   def log_message(self,*_):pass
   def dispatch(self):
    if not facade.handle(self):self.send_error(404)
   do_GET=do_POST=dispatch
  self.server=ThreadingHTTPServer(('127.0.0.1',0),Handler);self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start()
 def stop(self):
  if self.server is not None:
   self.server.shutdown();self.server.server_close();self.thread.join(timeout=5);self.service.close();self.queue.close();self.server=None
 def run_node(self,mode='valid'):
  viewer=os.environ.get('PINNED_VIEWER_ROOT')
  if not viewer: self.fail('PINNED_VIEWER_ROOT required; no skip')
  expected=os.environ.get('VIEWER_COMMIT','5a26155d2a2e4a52f8369d96d255ef9966dca2b5')
  self.assertEqual(subprocess.check_output(['git','rev-parse','HEAD'],cwd=viewer,text=True).strip(),expected)
  self.assertFalse(subprocess.check_output(['git','status','--porcelain'],cwd=viewer,text=True).strip())
  args=dict(viewer=viewer,origin='http://127.0.0.1:'+str(self.server.server_port),token=TOKEN,root=str(self.root),mode=mode)
  proc=subprocess.run(['node','tests/support/viewer_capacity_pair.cjs'],input=json.dumps(args),capture_output=True,text=True,timeout=30)
  self.assertEqual(proc.returncode,0,msg=proc.stderr);return json.loads(proc.stdout)
 def test_real_worker_and_provider(self):
  result=self.run_node();self.assertTrue(result['completed'],msg=str(result));task=self.service.store.task(result['uuid']);self.assertEqual(task['input_bytes'],result['expectedTotal']);self.assertIsNotNone(task['job_id']);self.assertTrue(result['commitReplay']);self.assertGreaterEqual(len(result['observations']),3);self.assertTrue(all(r['total']=='10' for r in result['observations'] if 'total' in r))
  # The worker Node process already exited. Reopen the real Viewer SQLite
  # repository in another process and test the database immutability trigger.
  recheck="const {openDatabase}=require(process.argv[1]+'/apps/viewer/server/database');const {ProcessingRepository}=require(process.argv[1]+'/apps/viewer/server/processingRepository');let db=openDatabase(process.argv[2]);let r=new ProcessingRepository(db),a=r.getAttempt(process.argv[3]),blocked=false;try{db.prepare('UPDATE processing_attempts SET declared_input_bytes=11 WHERE id=?').run(a.id)}catch{blocked=true}process.stdout.write(JSON.stringify({total:a.declaredInputBytes,version:a.uploadAdmissionVersion,blocked}));db.close()"
  proc=subprocess.run(['node','-e',recheck,os.environ['PINNED_VIEWER_ROOT'],str(self.root/'viewer.sqlite'),result['attemptId']],capture_output=True,text=True,timeout=20)
  self.assertEqual(proc.returncode,0,msg=proc.stderr);self.assertEqual(json.loads(proc.stdout),{'total':10,'version':1,'blocked':True})
  self.stop();self.start()
  script="const {NodeOdmProvider}=require(process.argv[1]);let p=new NodeOdmProvider({endpoint:process.argv[2],token:process.argv[3]});p.commit(process.argv[4],{declaredInputBytes:10}).then(()=>process.stdout.write('ok')).catch(e=>{console.error(e);process.exitCode=1})"
  provider=str(Path(os.environ['PINNED_VIEWER_ROOT'])/'apps/viewer/server/nodeOdmProvider.js');proc=subprocess.run(['node','-e',script,provider,'http://127.0.0.1:'+str(self.server.server_port),TOKEN,result['uuid']],capture_output=True,text=True,timeout=20)
  self.assertEqual(proc.returncode,0,msg=proc.stderr);self.assertEqual(proc.stdout,'ok');self.assertEqual(self.service.store.task(result['uuid'])['job_id'],task['job_id'])

 def test_missing_header_never_queues(self):
  result=self.run_node('missing');self.assertFalse(result['completed'],msg=str(result));task=self.service.store.task(result['uuid']);self.assertIsNone(task['job_id']);self.assertEqual(task['input_bytes'],0)
 def test_changed_commit_never_queues(self):
  result=self.run_node('changed');self.assertFalse(result['completed'],msg=str(result));task=self.service.store.task(result['uuid']);self.assertIsNone(task['job_id']);self.assertEqual(task['input_bytes'],10)

 def test_persisted_contract_loss_never_uploads(self):
  result=self.run_node('lost');self.assertFalse(result['completed']);self.assertEqual(result['error']['code'],'upload_admission_capability_unavailable');self.assertFalse(any('/upload/' in r.get('route','') for r in result['observations']))
  with self.queue._db() as db:self.assertEqual(db.execute('SELECT count(*) FROM viewer_tasks').fetchone()[0],0)

 def test_initial_probe_failure_or_malformed_capability_holds(self):
  result=self.run_node('probe-error');self.assertFalse(result['completed']);self.assertEqual(result['error']['code'],'upload_admission_capability_unavailable');self.assertFalse(any('/upload/' in r.get('route','') for r in result['observations']))
 def test_malformed_capability_holds(self):
  result=self.run_node('malformed');self.assertFalse(result['completed']);self.assertEqual(result['error']['code'],'upload_admission_capability_unavailable');self.assertFalse(any('/upload/' in r.get('route','') for r in result['observations']))

 def test_capability_outage_is_deferred_then_retries(self):
  result=self.run_node('outage-retry');self.assertTrue(result['completed'],msg=str(result));self.assertEqual(result['retry']['firstUploadCount'],0);self.assertEqual(result['retry']['firstStatus'],'admitted');self.assertEqual(result['retry']['firstJob'],'pending');self.assertEqual(result['retry']['afterPhase'],'committed');self.assertEqual(self.service.store.task(result['uuid'])['input_bytes'],10)
