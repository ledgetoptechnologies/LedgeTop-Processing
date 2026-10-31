import http.client,json,os,unittest
from ops.reference_worker_daemon import DaemonStateLock
from processing.viewer_local_result import send_completion
from processing.viewer_restart_census import SyntheticRestartCensus
from tests.test_viewer_completion_projection import ViewerCompletionProjectionTests

@unittest.skipUnless(os.name=='posix' and hasattr(os,'posix_fallocate'),'Linux required')
class RestartCensusTests(unittest.TestCase):
 def setUp(self):
  self.t=ViewerCompletionProjectionTests('test_completion_status_requires_exact_durable_attestation_and_spool');self.addCleanup(self.t.doCleanups);self.t.setUp()
  with DaemonStateLock(self.t.node.worker) as owner:send_completion(self.t.worker(owner),synthetic_fixture=True)
  self.f=self.t.fixture;self.spool=self.t.base.base.spool;self.id=self.t.base.base.identity();self.job=self.f.job
  with self.f.queue._db() as db:
   self.token=db.execute('SELECT token FROM viewer_worker_claims WHERE job_id=?',(self.job,)).fetchone()[0]
   db.execute("UPDATE jobs SET status='held',held_reason='coordinator_restart' WHERE id=?",(self.job,))
  self.f.api.viewer_restart_census=SyntheticRestartCensus(self.spool,synthetic_fixture=True)
 def request(self,token=None,bearer=None):
  c=http.client.HTTPSConnection('127.0.0.1',self.f.server.server_port,context=self.f.client,timeout=5)
  try:
   c.request('GET','/ltds/worker/v1/viewer-claims/'+self.job+'/restart-evidence',headers={'Authorization':'Bearer '+(bearer or 'a'*32),'X-Claim-Token':token or self.token});r=c.getresponse();return r.status,json.loads(r.read())
  finally:c.close()
 def test_read_only_tls_and_tamper(self):
  with self.f.queue._db() as db: before=list(db.iterdump())
  status,value=self.request();self.assertEqual(status,200);self.assertTrue(value['coordinatorArchiveVerified']);self.assertFalse(value['automaticResumeAuthorized']);self.assertIn('physical_engine_fence_unproven',value['blockers'])
  self.assertNotEqual(self.request(token='wrong')[0],200);self.assertNotEqual(self.request(bearer='m'*32)[0],200)
  with self.f.queue._db() as db:self.assertEqual(before,list(db.iterdump()))
  path=next(self.t.base.base.root.glob('*.alloc'));path.write_bytes(b'x'*path.stat().st_size)
  self.assertNotEqual(self.request()[0],200)
  self.assertEqual(self.f.queue.get(self.job)['status'],'held')
 def test_actual_coordinator_kill_restart_census(self):
  import pathlib,signal,subprocess,sys,tempfile,time
  from processing.prototype import Queue
  from processing.viewer_tasks import TaskStore
  from processing.viewer_worker_claim import ViewerWorkerClaims
  from processing.viewer_archive_spool import ViewerArchiveSpool
  code='''import json,os,sys,time\nfrom pathlib import Path\nfrom tests.test_viewer_completion_projection import ViewerCompletionProjectionTests\nfrom ops.reference_worker_daemon import DaemonStateLock\nfrom processing.viewer_local_result import send_completion\nt=ViewerCompletionProjectionTests("test_completion_status_requires_exact_durable_attestation_and_spool");t.setUp()\nwith DaemonStateLock(t.node.worker) as owner:send_completion(t.worker(owner),synthetic_fixture=True)\nf=t.fixture;s=t.base.base\nwith f.queue._db() as db: token=db.execute("SELECT token FROM viewer_worker_claims WHERE job_id=?",(f.job,)).fetchone()[0]\nPath(sys.argv[1]).write_text(json.dumps(dict(root=str(f.root),spool=str(s.root),identity=s.root_identity,job=f.job,token=token)))\nwhile True:time.sleep(1)\n'''
  with tempfile.TemporaryDirectory() as tmp:
   ready=pathlib.Path(tmp)/'ready';child=subprocess.Popen([sys.executable,'-c',code,str(ready)],env={**os.environ,'TMPDIR':tmp,'PYTHONDONTWRITEBYTECODE':'1'},stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,start_new_session=True)
   try:
    deadline=time.monotonic()+20
    while not ready.exists() and time.monotonic()<deadline:
     if child.poll() is not None:self.fail(child.stderr.read().decode())
     time.sleep(.05)
    self.assertTrue(ready.exists());info=json.loads(ready.read_text());os.killpg(child.pid,signal.SIGKILL);self.assertEqual(child.wait(timeout=5),-signal.SIGKILL)
    state=pathlib.Path(info['root'])/'state';q=Queue(state,node_ids=('cluster','main'),viewer_enabled=True,reference_enabled=True)
    try:
     store=TaskStore(q,state/'viewer-tasks');claims=ViewerWorkerClaims(q,store,local_max_photos=1000,local_max_input_bytes=100000,node_environments={'cluster':('production','staging'),'main':('production','staging')})
     spool=ViewerArchiveSpool(claims,root=info['spool'],root_identity=info['identity'],max_archive_bytes=65536,enabled=True)
     report=SyntheticRestartCensus(spool,synthetic_fixture=True).report(info['job'],dict(node_id='cluster',worker_id='worker-a',worker_instance_id='instance-a'),info['token'])
     self.assertTrue(report['coordinatorArchiveVerified']);self.assertEqual(q.get(info['job'])['heldReason'],'coordinator_restart');self.assertFalse(report['automaticResumeAuthorized'])
    finally:q.close()
   finally:
    if child.poll() is None:os.killpg(child.pid,signal.SIGKILL);child.wait(timeout=5)
    if child.stderr:child.stderr.close()
 def test_hold_during_hash_withholds_report(self):
  import threading
  from concurrent.futures import ThreadPoolExecutor
  from unittest.mock import patch
  from processing.viewer_archive_spool import ViewerArchiveSpool
  entered=threading.Event();release=threading.Event();original=ViewerArchiveSpool._digest
  def pause(slot,length):
   entered.set()
   if not release.wait(5):raise RuntimeError('barrier timeout')
   return original(slot,length)
  with patch.object(ViewerArchiveSpool,'_digest',staticmethod(pause)):
   with ThreadPoolExecutor(max_workers=1) as pool:
    pending=pool.submit(self.request)
    try:
     self.assertTrue(entered.wait(5))
     with self.f.queue._db() as db:db.execute("INSERT OR REPLACE INTO viewer_worker_fences(job_id,stop_requested) VALUES(?,1)",(self.job,))
    finally:release.set()
    self.assertNotEqual(pending.result(timeout=5)[0],200)
  self.assertEqual(self.f.queue.get(self.job)['status'],'held')
