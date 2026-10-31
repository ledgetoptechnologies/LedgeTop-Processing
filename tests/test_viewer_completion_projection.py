import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from ops.reference_worker_daemon import DaemonStateLock
from processing.viewer_local_result import send_completion
from processing.viewer_node_enrollment import EnrollmentHeld,ViewerNodeEnrollment
from processing.viewer_archive_spool import ViewerArchiveSpool
from processing.reference_http import create_reference_http_server
from processing.viewer_worker_claim import ViewerWorkerClaims
from tests.test_viewer_spool_provider import ViewerSpoolProviderTests


class ViewerCompletionProjectionTests(unittest.TestCase):
    def setUp(self):
        self.base=ViewerSpoolProviderTests('test_authenticated_provider_download_keeps_running_and_retains_data')
        self.base.setUp();self.addCleanup(self.base.doCleanups)
        self.fixture=self.base.fixture;self.base.service.viewer_completion_projection=True
        self.node=self.base.base.base.base.base.base

    def worker(self,owner):return self.base.base.base.worker(owner)

    def info(self):
        status,_,body=self.base.get('/task/22222222-2222-4222-8222-222222222222/info')
        self.assertEqual(status,200);return json.loads(body)

    def test_completion_status_requires_exact_durable_attestation_and_spool(self):
        self.assertNotEqual(self.info()['status']['code'],40)
        with DaemonStateLock(self.node.worker) as owner:
            worker=self.worker(owner);receipt=send_completion(worker,synthetic_fixture=True)
            self.assertEqual(send_completion(worker,synthetic_fixture=True),receipt)
        self.assertEqual(self.info()['status']['code'],40)
        status,_,data=self.base.get('/task/22222222-2222-4222-8222-222222222222/download/all.zip')
        self.assertEqual(status,200);self.assertEqual(data,self.base.base.base.base.archive)
        self.assertEqual(self.fixture.queue.get(self.fixture.job)['status'],'running')
        self.assertEqual(len(list(self.base.base.root.glob('*.alloc'))),1)

    def test_projection_is_disabled_by_default_even_with_attestation(self):
        self.base.service.viewer_completion_projection=False
        with DaemonStateLock(self.node.worker) as owner:send_completion(self.worker(owner),synthetic_fixture=True)
        self.assertNotEqual(self.info()['status']['code'],40)
        self.base.service.viewer_completion_projection=True
        self.assertEqual(self.info()['status']['code'],40)

    def test_spool_tamper_or_hold_withholds_completion(self):
        with DaemonStateLock(self.node.worker) as owner:
            worker=self.worker(owner);send_completion(worker,synthetic_fixture=True)
            self.assertEqual(self.info()['status']['code'],40)
            with worker._db() as db:claim=json.loads(db.execute('SELECT claim FROM intent').fetchone()[0])
            # Hold is a separate operator state; no completion projection.
            worker._request({},job_id=claim['jobId'],claim_token=claim['claimToken'],kind='mutation-hold')
        self.assertNotEqual(self.info()['status']['code'],40)

    def test_spool_content_tamper_withholds_completion(self):
        with DaemonStateLock(self.node.worker) as owner:send_completion(self.worker(owner),synthetic_fixture=True)
        path=next(self.base.base.root.glob('*.alloc'));data=path.read_bytes();path.write_bytes(b'x'*len(data))
        self.assertNotEqual(self.info()['status']['code'],40)

    def test_changed_grant_or_active_mutation_withholds_completion(self):
        with DaemonStateLock(self.node.worker) as owner:send_completion(self.worker(owner),synthetic_fixture=True)
        with self.fixture.queue._db() as db:
            old=db.execute('SELECT grant_value FROM viewer_worker_grants WHERE job_id=?',(self.fixture.job,)).fetchone()[0]
            value=json.loads(old);value['taskName']='forged'
            db.execute('UPDATE viewer_worker_grants SET grant_value=? WHERE job_id=?',(json.dumps(value),self.fixture.job))
        self.assertNotEqual(self.info()['status']['code'],40)
        with self.fixture.queue._db() as db:
            db.execute('UPDATE viewer_worker_grants SET grant_value=? WHERE job_id=?',(old,self.fixture.job))
            db.execute("INSERT INTO viewer_worker_permits VALUES(?,?,'upload','active')",(self.fixture.job,'later'))
        self.assertNotEqual(self.info()['status']['code'],40)

    def test_changed_local_observation_is_not_attested(self):
        path=next(Path(self.node.config['output_root']).glob('*.viewer-completion-observation'))
        path.write_text('{}')
        with DaemonStateLock(self.node.worker) as owner:
            with self.assertRaises(Exception):send_completion(self.worker(owner),synthetic_fixture=True)
        self.assertNotEqual(self.info()['status']['code'],40)

    def test_hold_during_archive_hash_prevents_completion_status(self):
        with DaemonStateLock(self.node.worker) as owner:
            worker=self.worker(owner);send_completion(worker,synthetic_fixture=True)
            with worker._db() as db:claim=json.loads(db.execute('SELECT claim FROM intent').fetchone()[0])
            entered=threading.Event();release=threading.Event();original=ViewerArchiveSpool._digest
            def pause(slot,length):
                entered.set()
                if not release.wait(5):raise RuntimeError('test barrier timeout')
                return original(slot,length)
            with patch.object(ViewerArchiveSpool,'_digest',staticmethod(pause)):
                with ThreadPoolExecutor(max_workers=2) as pool:
                    status=pool.submit(self.info)
                    try:
                        self.assertTrue(entered.wait(5))
                        hold=pool.submit(worker._request,{},job_id=claim['jobId'],claim_token=claim['claimToken'],kind='mutation-hold')
                        self.assertEqual(hold.result(timeout=5)['state'],'held')
                    finally:release.set()
                    self.assertNotEqual(status.result(timeout=5)['status']['code'],40)
        self.assertNotEqual(self.info()['status']['code'],40)

    def test_lost_completion_ack_replays_exact_receipt(self):
        original=ViewerNodeEnrollment._request
        def lose(worker,payload,**kwargs):
            value=original(worker,payload,**kwargs)
            if kwargs.get('kind')=='completion-attestation':raise EnrollmentHeld('synthetic_ack_lost')
            return value
        with DaemonStateLock(self.node.worker) as owner:
            worker=self.worker(owner)
            with patch.object(ViewerNodeEnrollment,'_request',lose):
                with self.assertRaisesRegex(EnrollmentHeld,'synthetic_ack_lost'):send_completion(worker,synthetic_fixture=True)
            self.assertEqual(send_completion(worker,synthetic_fixture=True)['kind'],'viewer-completion-attestation-receipt')
        self.assertEqual(self.info()['status']['code'],40)
        with self.fixture.queue._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM viewer_worker_completions').fetchone()[0],1)

    def test_worker_kill_after_attestation_reply_replays(self):
        code='''import json,os,signal,sys
from ops.reference_worker_daemon import DaemonStateLock
from processing.viewer_node_enrollment import ViewerNodeEnrollment
import processing.viewer_local_result as result
root,config=sys.argv[1:]
original=result._persist
def persist(root_fd,root_id,name,record):
 if name.endswith('.viewer-completion-receipt'):os.kill(os.getpid(),signal.SIGKILL)
 return original(root_fd,root_id,name,record)
result._persist=persist
with DaemonStateLock(root) as owner:
 result.send_completion(ViewerNodeEnrollment(owner,**json.loads(config)),synthetic_fixture=True)
'''
        child=subprocess.run([sys.executable,'-c',code,str(self.node.worker),json.dumps(self.node.config)],
                             stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=15)
        self.assertEqual(child.returncode,-signal.SIGKILL)
        with DaemonStateLock(self.node.worker) as owner:send_completion(self.worker(owner),synthetic_fixture=True)
        self.assertEqual(self.info()['status']['code'],40)

    def test_wrong_completion_claim_or_observation_is_rejected(self):
        with DaemonStateLock(self.node.worker) as owner:
            worker=self.worker(owner);send_completion(worker,synthetic_fixture=True)
            with worker._db() as db:claim=json.loads(db.execute('SELECT claim FROM intent').fetchone()[0])
            with self.fixture.queue._db() as db:
                row=db.execute('SELECT observation,spool_receipt FROM viewer_worker_completions').fetchone()
            payload=dict(observation=json.loads(row[0]),spoolReceipt=json.loads(row[1]))
            with self.assertRaises(EnrollmentHeld):
                worker._request(payload,job_id=claim['jobId'],claim_token='wrong',kind='completion-attestation')
            payload['observation']['engineStatus']=20
            with self.assertRaises(EnrollmentHeld):
                worker._request(payload,job_id=claim['jobId'],claim_token=claim['claimToken'],kind='completion-attestation')

    def test_coordinator_kill_after_attestation_commit_restores_held(self):
        port=self.fixture.server.server_port;self.fixture.stop_server();self.fixture.queue.close();self.fixture.queue=None
        ready=self.fixture.root/'completion-child-ready';barrier=self.fixture.root/'completion-child-committed'
        spool=self.base.base.root;identity=self.base.base.root_identity
        code='''import json,ssl,sys,time
from pathlib import Path
from processing.prototype import Queue
from processing.viewer_tasks import TaskStore
from processing.viewer_worker_claim import ViewerWorkerClaims
from processing.viewer_archive_spool import ViewerArchiveSpool
from processing.reference_store import ReferenceStore
from processing.reference_delivery import DeliveryReceiver
from processing.reference_worker_api import ReferenceWorkerAPI
from processing.reference_http import create_reference_http_server
root,cert,key,port,spool,identity,ready,barrier=sys.argv[1:];root=Path(root)
q=Queue(root/'state',node_ids=('cluster','main'),viewer_enabled=True,reference_enabled=True)
with q._db() as db:db.execute("UPDATE jobs SET status='running',held_reason=NULL WHERE status='held' AND held_reason='coordinator_restart'")
t=TaskStore(q,root/'state'/'viewer-tasks')
c=ViewerWorkerClaims(q,t,local_max_photos=1000,local_max_input_bytes=100000,node_environments={'cluster':('production','staging'),'main':('production','staging')})
sp=ViewerArchiveSpool(c,root=spool,root_identity=json.loads(identity),max_archive_bytes=65536,enabled=True)
s=ReferenceStore(q);api=ReferenceWorkerAPI(s,DeliveryReceiver(s),bearer_workers={'a'*32:dict(node_id='cluster',worker_id='worker-a',worker_instance_id='instance-a')},viewer_claims=c,viewer_spool=sp)
class Delay:
 def handle(self,method,path,*args,**kwargs):
  response=api.handle(method,path,*args,**kwargs)
  if path.endswith('/completion-attestation') and response.status==200:Path(barrier).write_text('committed');time.sleep(30)
  return response
 def handle_archive(self,*args,**kwargs):return api.handle_archive(*args,**kwargs)
tls=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);tls.load_cert_chain(cert,key)
server=create_reference_http_server(Delay(),port=int(port),ssl_context=tls)
Path(ready).write_text('ready');server.serve_forever(poll_interval=.05)
'''
        child=subprocess.Popen([sys.executable,'-c',code,str(self.fixture.root),str(self.fixture.cert),str(self.fixture.key),str(port),
                                str(spool),json.dumps(identity),str(ready),str(barrier)],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        def wait_file(path):
            deadline=time.monotonic()+8
            while not path.exists() and time.monotonic()<deadline:
                if child.poll() is not None:self.fail('coordinator child exited before barrier')
                time.sleep(.02)
            self.assertTrue(path.exists())
        try:
            wait_file(ready)
            with DaemonStateLock(self.node.worker) as owner:
                worker=self.worker(owner)
                with ThreadPoolExecutor(max_workers=1) as pool:
                    pending=pool.submit(send_completion,worker,synthetic_fixture=True)
                    wait_file(barrier);child.kill();child.wait(timeout=5)
                    with self.assertRaises(EnrollmentHeld):pending.result(timeout=5)
        finally:
            if child.poll() is None:child.kill();child.wait(timeout=5)
        self.fixture.start_queue()
        self.fixture.claims=ViewerWorkerClaims(self.fixture.queue,self.fixture.store,local_max_photos=1000,local_max_input_bytes=100000,
                                               node_environments={'cluster':('production','staging'),'main':('production','staging')})
        self.base.base.spool=ViewerArchiveSpool(self.fixture.claims,root=spool,root_identity=identity,max_archive_bytes=65536,enabled=True)
        def same_port(api,*,ssl_context):return create_reference_http_server(api,port=port,ssl_context=ssl_context)
        with patch('tests.test_viewer_worker_claim.create_reference_http_server',same_port):self.fixture.start_server()
        self.fixture.api.viewer_spool=self.base.base.spool
        self.base.service.queue=self.fixture.queue;self.base.service.store=self.fixture.store;self.base.service.viewer_spool=self.base.base.spool
        with self.fixture.queue._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM viewer_worker_completions').fetchone()[0],1)
        self.assertEqual(self.fixture.queue.get(self.fixture.job)['status'],'held')
        self.assertNotEqual(self.info()['status']['code'],40)
