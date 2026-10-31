import json
import os
import signal
import subprocess
import sys
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import HTTPSHandler,ProxyHandler,Request,build_opener
from unittest.mock import patch

from ops.reference_worker_daemon import DaemonStateLock
from processing.reference_http import create_reference_http_server
from processing.viewer_local_result import capture,register,send_metadata
from processing.viewer_node_enrollment import EnrollmentHeld,ViewerNodeEnrollment
from processing.viewer_service import NativeEngine
from processing.viewer_worker_claim import ViewerWorkerClaims
from processing.node_gate import NodeGate
from tests.test_viewer_synthetic_submission import SyntheticSubmissionTests


class ViewerResultMetadataTests(unittest.TestCase):
    def setUp(self):
        self.base=SyntheticSubmissionTests('test_exact_submission_once_and_replay_without_cleanup')
        self.base.setUp();self.addCleanup(self.base.doCleanups)
        with DaemonStateLock(self.base.base.base.worker) as owner:
            inputs=self.base.prepare(owner)
            from processing.viewer_synthetic_submission import submit
            submit(inputs,NativeEngine(self.base.origin),synthetic_fixture=True)
            self.record=capture(inputs.worker,NativeEngine(self.base.origin),synthetic_fixture=True)
        self.fixture=self.base.base.base.fixture

    def worker(self,owner):return ViewerNodeEnrollment(owner,**self.base.base.base.config)

    def test_sealed_local_resume_after_kill_with_coordinator_offline(self):
        self.fixture.stop_server()
        code='''import json,os,signal,sys
from ops.reference_worker_daemon import DaemonStateLock
from processing.viewer_node_enrollment import ViewerNodeEnrollment
from processing.viewer_local_result import resume_sealed,resume_capture_status
root,config,mode=sys.argv[1:]
with DaemonStateLock(root) as owner:
 result=resume_capture_status(ViewerNodeEnrollment(owner,**json.loads(config)),synthetic_fixture=True)
 if mode=='kill':os.kill(os.getpid(),signal.SIGKILL)
 print(json.dumps(result))
'''
        root=self.base.base.base.worker;config=json.dumps(self.base.base.base.config)
        killed=subprocess.run([sys.executable,'-c',code,str(root),config,'kill'],capture_output=True,timeout=15)
        self.assertEqual(killed.returncode,-signal.SIGKILL)
        replay=subprocess.run([sys.executable,'-c',code,str(root),config,'resume'],capture_output=True,text=True,timeout=15)
        self.assertEqual(replay.returncode,0,replay.stderr)
        result=json.loads(replay.stdout);self.assertEqual(result['state'],'sealed_no_dispatch')
        self.assertEqual(result['archiveSha256'],self.record['archiveSha256'])
        self.assertEqual(result['archiveBytes'],self.record['archiveBytes'])

    def test_sealed_local_resume_fails_closed_when_seal_missing(self):
        from processing.viewer_local_result import resume_sealed
        seal=next(self.base.base.base.output.glob('*.seal'));seal.unlink()
        with DaemonStateLock(self.base.base.base.worker) as owner:
            with self.assertRaises(Exception):resume_sealed(self.worker(owner),synthetic_fixture=True)
        self.assertFalse(seal.exists())

    def test_sealed_local_resume_detects_mutated_archive_and_owner_contention(self):
        from processing.viewer_local_result import resume_sealed
        slot=next(self.base.base.base.output.glob('*.alloc'))
        with DaemonStateLock(self.base.base.base.worker) as owner:
            worker=self.worker(owner)
            with self.assertRaises(Exception):
                with DaemonStateLock(self.base.base.base.worker):pass
            with slot.open('r+b') as stream:stream.write(b'X');stream.flush();os.fsync(stream.fileno())
            with self.assertRaises(Exception):resume_sealed(worker,synthetic_fixture=True)

    def test_metadata_receipt_exact_replay_does_not_complete_viewer(self):
        with DaemonStateLock(self.base.base.base.worker) as owner:
            worker=self.worker(owner);first=send_metadata(worker,synthetic_fixture=True)
            self.assertEqual(send_metadata(worker,synthetic_fixture=True),first)
            self.assertEqual(first['kind'],'viewer-result-metadata-receipt')
            self.assertEqual(first['archiveSha256'],self.record['archiveSha256'])
            with self.assertRaises(EnrollmentHeld):
                worker._request(self.record,job_id=self.record['jobId'],claim_token='wrong',kind='result-metadata')
            changed=dict(self.record,archiveSha256='f'*64)
            with worker._db() as db:claim=json.loads(db.execute('SELECT claim FROM intent').fetchone()[0])
            request=Request(self.fixture.origin+'/ltds/worker/v1/viewer-claims/'+self.record['jobId']+'/result-metadata',
                            data=json.dumps(self.record).encode(),method='POST',headers={'Authorization':'Bearer '+'b'*32,
                            'X-Claim-Token':claim['claimToken'],'Content-Type':'application/json'})
            with self.assertRaises(HTTPError) as error:
                build_opener(ProxyHandler({}),HTTPSHandler(context=self.fixture.client)).open(request,timeout=3)
            self.assertEqual(error.exception.code,409);error.exception.close()
            with self.assertRaises(EnrollmentHeld):
                worker._request(changed,job_id=self.record['jobId'],claim_token=claim['claimToken'],kind='result-metadata')
            changed=dict(self.record,viewerGeneration=True)
            with self.assertRaises(EnrollmentHeld):
                worker._request(changed,job_id=self.record['jobId'],claim_token=claim['claimToken'],kind='result-metadata')
        with self.fixture.queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM viewer_worker_result_metadata').fetchone()[0],1)
        self.assertEqual(self.fixture.queue.get(self.fixture.job)['status'],'running')
        self.assertEqual(len(list(self.base.base.inputs.rglob('*.alloc'))),1)

    def test_metadata_can_be_registered_for_a_held_job_without_completion(self):
        with DaemonStateLock(self.base.base.base.worker) as owner:
            worker=self.worker(owner)
            with worker._db() as db:claim=json.loads(db.execute('SELECT claim FROM intent').fetchone()[0])
            held=worker._request({},job_id=claim['jobId'],claim_token=claim['claimToken'],kind='mutation-hold')
            self.assertEqual(held['state'],'held')
            self.assertEqual(send_metadata(worker,synthetic_fixture=True)['archiveSha256'],self.record['archiveSha256'])
        self.assertEqual(self.fixture.queue.get(self.fixture.job)['status'],'held')

    def test_gate_loss_after_remote_commit_holds_local_receipt(self):
        original=ViewerNodeEnrollment._request;old_state=[]
        def lose_gate(worker,payload,**kwargs):
            value=original(worker,payload,**kwargs)
            if kwargs.get('kind')=='result-metadata':
                with NodeGate('fixture-engine') as gate:
                    state=gate.snapshot();old_state.append(state.copy());state['reservation']=None;gate._save(state)
            return value
        with DaemonStateLock(self.base.base.base.worker) as owner:
            worker=self.worker(owner)
            with patch.object(ViewerNodeEnrollment,'_request',lose_gate):
                with self.assertRaises(RuntimeError):send_metadata(worker,synthetic_fixture=True)
            with self.fixture.queue._db() as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM viewer_worker_result_metadata').fetchone()[0],1)
            with NodeGate('fixture-engine') as gate:gate._save(old_state[0])
            self.assertEqual(send_metadata(worker,synthetic_fixture=True)['archiveSha256'],self.record['archiveSha256'])

    def test_offline_retry_preserves_exact_result(self):
        port=self.fixture.server.server_port;self.fixture.stop_server()
        with DaemonStateLock(self.base.base.base.worker) as owner:
            worker=self.worker(owner)
            with self.assertRaises(EnrollmentHeld):send_metadata(worker,synthetic_fixture=True)
            self.assertEqual(register(worker),self.record)
            self.fixture.server=create_reference_http_server(self.fixture.api,port=port,ssl_context=self.fixture.tls)
            self.fixture.thread=threading.Thread(target=self.fixture.server.serve_forever,kwargs={'poll_interval':.05},daemon=True)
            self.fixture.thread.start()
            self.assertEqual(send_metadata(worker,synthetic_fixture=True)['archiveSha256'],self.record['archiveSha256'])

    def test_lost_ack_replays_single_coordinator_receipt(self):
        original=ViewerNodeEnrollment._request
        def lose(worker,payload,**kwargs):
            value=original(worker,payload,**kwargs)
            if kwargs.get('kind')=='result-metadata':raise EnrollmentHeld('synthetic_ack_lost')
            return value
        with DaemonStateLock(self.base.base.base.worker) as owner:
            worker=self.worker(owner)
            with patch.object(ViewerNodeEnrollment,'_request',lose):
                with self.assertRaisesRegex(EnrollmentHeld,'synthetic_ack_lost'):send_metadata(worker,synthetic_fixture=True)
            first=send_metadata(worker,synthetic_fixture=True)
            self.assertEqual(send_metadata(worker,synthetic_fixture=True),first)
        with self.fixture.queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM viewer_worker_result_metadata').fetchone()[0],1)

    def test_worker_kill_after_remote_receipt_replays(self):
        code='''import json,os,signal,sys
from ops.reference_worker_daemon import DaemonStateLock
from processing.viewer_node_enrollment import ViewerNodeEnrollment
from processing.viewer_local_result import send_metadata
root,config=sys.argv[1:]
original=ViewerNodeEnrollment._request
def request(self,payload,**kwargs):
 result=original(self,payload,**kwargs)
 if kwargs.get('kind')=='result-metadata':os.kill(os.getpid(),signal.SIGKILL)
 return result
ViewerNodeEnrollment._request=request
with DaemonStateLock(root) as owner:
 worker=ViewerNodeEnrollment(owner,**json.loads(config))
 send_metadata(worker,synthetic_fixture=True)
'''
        result=subprocess.run([sys.executable,'-c',code,str(self.base.base.base.worker),json.dumps(self.base.base.base.config)],
                              stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=15)
        self.assertEqual(result.returncode,-signal.SIGKILL)
        with DaemonStateLock(self.base.base.base.worker) as owner:
            receipt=send_metadata(self.worker(owner),synthetic_fixture=True)
        self.assertEqual(receipt['archiveSha256'],self.record['archiveSha256'])
        with self.fixture.queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM viewer_worker_result_metadata').fetchone()[0],1)

    def test_coordinator_kill_after_metadata_commit_before_response(self):
        port=self.fixture.server.server_port;self.fixture.stop_server();self.fixture.queue.close();self.fixture.queue=None
        ready=self.fixture.root/'metadata-child-ready';committed=self.fixture.root/'metadata-child-committed'
        code='''import ssl,sys,time
from pathlib import Path
from processing.prototype import Queue
from processing.viewer_tasks import TaskStore
from processing.viewer_worker_claim import ViewerWorkerClaims
from processing.reference_store import ReferenceStore
from processing.reference_delivery import DeliveryReceiver
from processing.reference_worker_api import ReferenceWorkerAPI
from processing.reference_http import create_reference_http_server
root,cert,key,port,ready,committed=sys.argv[1:];root=Path(root)
q=Queue(root/'state',node_ids=('cluster','main'),viewer_enabled=True,reference_enabled=True)
t=TaskStore(q,root/'state'/'viewer-tasks')
c=ViewerWorkerClaims(q,t,local_max_photos=1000,local_max_input_bytes=100000,node_environments={'cluster':('production','staging'),'main':('production','staging')})
s=ReferenceStore(q);api=ReferenceWorkerAPI(s,DeliveryReceiver(s),bearer_workers={'a'*32:dict(node_id='cluster',worker_id='worker-a',worker_instance_id='instance-a')},viewer_claims=c)
class Delay:
 def handle(self,method,path,*args,**kwargs):
  response=api.handle(method,path,*args,**kwargs)
  if path.endswith('/result-metadata') and response.status==200:Path(committed).write_text('committed');time.sleep(30)
  return response
tls=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);tls.load_cert_chain(cert,key)
server=create_reference_http_server(Delay(),port=int(port),ssl_context=tls)
Path(ready).write_text('ready');server.serve_forever(poll_interval=.05)
'''
        child=subprocess.Popen([sys.executable,'-c',code,str(self.fixture.root),str(self.fixture.cert),str(self.fixture.key),str(port),str(ready),str(committed)],
                               stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        def wait_file(path):
            deadline=time.monotonic()+8
            while not path.exists() and time.monotonic()<deadline:
                if child.poll() is not None:self.fail('coordinator child exited before barrier')
                time.sleep(.02)
            self.assertTrue(path.exists())
        try:
            wait_file(ready)
            with DaemonStateLock(self.base.base.base.worker) as owner:
                worker=self.worker(owner)
                with ThreadPoolExecutor(max_workers=1) as pool:
                    future=pool.submit(send_metadata,worker,synthetic_fixture=True)
                    wait_file(committed);child.kill();child.wait(timeout=5)
                    with self.assertRaises(EnrollmentHeld):future.result(timeout=5)
        finally:
            if child.poll() is None:child.kill();child.wait(timeout=5)
        self.fixture.start_queue()
        self.fixture.claims=ViewerWorkerClaims(self.fixture.queue,self.fixture.store,local_max_photos=1000,local_max_input_bytes=100000,
                                               node_environments={'cluster':('production','staging'),'main':('production','staging')})
        def same_port(api,*,ssl_context):return create_reference_http_server(api,port=port,ssl_context=ssl_context)
        with patch('tests.test_viewer_worker_claim.create_reference_http_server',same_port):self.fixture.start_server()
        with DaemonStateLock(self.base.base.base.worker) as owner:
            receipt=send_metadata(self.worker(owner),synthetic_fixture=True)
        self.assertEqual(receipt['archiveSha256'],self.record['archiveSha256'])
        self.assertEqual(self.fixture.queue.get(self.fixture.job)['status'],'held')
        with self.fixture.queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM viewer_worker_result_metadata').fetchone()[0],1)
