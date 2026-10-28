"""Opt-in actual Viewer867 Node/Python TLS flow; synthetic backend, no publication.

Run inside cached Node24/Python Linux image with VIEWER_REFERENCE_ROOT pointing
to the immutable867 snapshot. No Viewer source is copied into this repository.
"""
from contextlib import ExitStack,contextmanager,closing
import hashlib
import json
import os
from pathlib import Path
import selectors
import sqlite3
import ssl
import subprocess
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from urllib.request import HTTPSHandler,ProxyHandler,Request,build_opener

from ops.native_node_fixture import NativeNodeFixture,ARCHIVE
from ops.reference_worker_daemon import DaemonStateLock
from processing.node_gate import NodeGate
from processing.prototype import Queue
from processing.reference_control import ReferenceControlAPI
from processing.reference_api import Response
from processing.reference_driver import ReferenceDriver
from processing.reference_engine_cleanup import EngineCacheCleanup
from processing.reference_engine_worker import ReferenceEngineWorker
from processing.reference_failure import FailureOutbox
from processing.reference_http import create_reference_http_server
from processing.reference_readers import ReaderSession
from processing.reference_result_api import ReferenceResultAPI
from processing.reference_result_proxy import ResultProxy
from processing.reference_results import ReferenceResults
from processing.reference_store import ReferenceStore
from processing.reference_transfer import BrokerClient
from processing.scratch_cleanup import ScratchCleanupExecutor
from processing.scratch_policy import ScratchLedger
from processing.viewer_service import NativeEngine
from processing.worker_outbox import WorkerOutbox
from tests import test_reference_parent_guard as tls_fixture

VIEWER_COMMIT='867dd3f26c16203b5fdceec3af1671846ffa208c'
PINS={
 'server/referenceProtocol.js':'17d505c268b53ff5e17c61bd6f89fff8081b9089d35ca00cef6d96906d934159',
 'server/referenceProvider.js':'3440ecb8b9523252ec6bdf1dcede3b341125c645b4f204951a1ae760b14aec7c',
 'server/referenceCaTransport.js':'3ea6ed955d40727d0c8d2bdc6d7f34fbb39c4802c130534da81c51465a3c9d19',
 'server/referenceTransferBroker.js':'7aca3037d72a728443c645b9358b0fe4daf301d2879c62a3020fe8a736df7361',
 'server/referenceHttp.js':'b0e249ace9ce78953c9ddd50127076426f9237c283f0f079d2837d77fe042474',
 'server/referenceAcceptance.js':'5c2c2908eed79283a655456fdc68de88449bcfe33a085b9d26dff63c910248fa',
 'server/referenceSourceRetention.js':'515e6ebb511aba4797f3999dd67733006f3fbf9be39a5fd3c8cfc16b4aaf9c3e',
 'test/helpers/reference-joint-harness.cjs':'a3a81a630c724d1d9952b369fea9aadcf8ac71c01b3bcc0165a3e32e27c02a0e',
 'test/fixtures/reference-v1.contract.json':'2c49ff86b8e20384c400ff56c8365418e643108aa9e6c29b46e938554e989b37',
}
WORKER='synthetic-joint-worker-credential'
VIEWER='synthetic-joint-viewer-credential'
READER='synthetic-joint-result-read-credential'
FIXTURE='synthetic-joint-fixture-control-credential'


@unittest.skipUnless(os.name=='posix' and os.environ.get('VIEWER_REFERENCE_ROOT'),'opt-in pinned Viewer867 Linux/Node24 snapshot required')
class ReferenceJointFlowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        tls_fixture.ParentGuardTests.setUpClass.__func__(cls)
        cls.viewer_root=Path(os.environ['VIEWER_REFERENCE_ROOT'])/'apps/viewer'
        for relative,expected in PINS.items():
            if hashlib.sha256((cls.viewer_root/relative).read_bytes()).hexdigest()!=expected:raise ValueError('pinned_viewer_source_changed:'+relative)
        major=subprocess.check_output(['node','--version'],text=True,timeout=5).strip().split('.')[0]
        if major!='v24':raise ValueError('cached_node24_required')

    def setUp(self):
        self.stack=ExitStack();self.addCleanup(self.stack.close)
        self.root=Path(self.stack.enter_context(tempfile.TemporaryDirectory()));self.root.chmod(0o700)
        self.owner=self.stack.enter_context(DaemonStateLock(self.root))
        self.stack.enter_context(patch('processing.node_gate.canonical_gate_path',return_value=self.root/'gate'))
        self.queue=Queue(self.root/'coordinator',reference_enabled=True);self.stack.callback(self.queue.close)
        self.store=ReferenceStore(self.queue)
        self.native_fixture=NativeNodeFixture(self.root/'native.sqlite',engine_version='synthetic-joint-v1',synthetic_enabled=True).start();self.stack.callback(self.native_fixture.close)
        self.native=NativeEngine(self.native_fixture.origin)
        self.gate_origin='http://127.0.0.1:18080'
        with NodeGate('joint-engine') as gate:gate.provision('cluster',self.gate_origin,self.queue.authority_id)
        scratch=self.root/'scratch';scratch.mkdir(mode=0o700)
        self.ledger=ScratchLedger(self.root/'ledger.sqlite',scratch,capacity_probe=lambda _:1024**3,safety_floor_bytes=0,cleanup_grace_seconds=0)
        self.outbox_now=time.time();self.outbox=WorkerOutbox(self.root/'outbox.sqlite',clock=lambda:self.outbox_now);self.failures=FailureOutbox(self.root/'failures.sqlite')
        self.worker=ReferenceEngineWorker(self.root/'worker.sqlite',ledger=self.ledger,outbox=self.outbox,engine=self.native,engine_id='joint-engine',node_id='cluster',authority_id=self.queue.authority_id,coordinator_origin=self.gate_origin,gate_generation=1,worker_id='joint-worker',engine_version='synthetic-joint-v1')
        self.reader=self.stack.enter_context(ReaderSession(self.ledger))
        self.results=ReferenceResults(self.worker,reader_session=self.reader)
        result_api=ReferenceResultAPI(self.results,bearer_readers={READER:dict(node_id='cluster',worker_id='joint-worker',environments=('staging',),share_policy=lambda _:True)})
        self.tls=ssl.create_default_context(cafile=str(self.cert))
        worker_origin=self.serve(result_api)
        self.streams=0
        @contextmanager
        def admit(binding):
            if self.streams or binding['byteSize']>4*1024**2:raise ValueError('synthetic_stream_capacity')
            self.streams+=1
            try:yield
            finally:self.streams-=1
        proxy=ResultProxy({'cluster':worker_origin},worker_bearers={'cluster':READER},admit_read=admit,ssl_context=self.tls)
        frozen=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())
        self.control=ReferenceControlAPI(self.store,viewer_bearers={VIEWER:'staging'},worker_bearers={WORKER:dict(node_id='cluster',worker_id='joint-worker',worker_instance_id='joint-instance')},capability_fingerprint=frozen['capabilities']['capabilityFingerprint'],options_fingerprint=frozen['capabilities']['engines']['odm']['optionsFingerprint'],result_proxy=proxy)
        self.drop_result_ack=False;self.completion_envelopes=[];test=self
        class ControlEndpoint:
            def handle(endpoint,method,path,headers,body,**kwargs):
                response=test.control.handle(method,path,headers,body,**kwargs)
                if method=='POST' and path=='/ltds/worker/v1/results':
                    # Lose only the transport acknowledgement after the actual
                    # delivery receiver transaction; never synthesize its result.
                    test.completion_envelopes.append(json.loads(body))
                    if test.drop_result_ack and response.status==200:
                        test.drop_result_ack=False;return Response(503,{'error':'synthetic_lost_completion_ack'})
                return response
        self.control_origin=self.serve(ControlEndpoint())
        self.process=None;self.viewer_state=self.root/'viewer';self.stack.callback(self.stop_node)
        self.start_node()
        self.engine_cleanup=EngineCacheCleanup(self.worker,path=self.root/'engine-cleanup.sqlite')
        self.driver=ReferenceDriver(self.worker,self.ledger,self.outbox,BrokerClient(self.node_origin,ssl_context=self.tls),self.control_origin,WORKER,state_path=self.root/'driver.sqlite',ssl_context=self.tls,output_budget=4*1024**2,transfer_ttl=240,safety_floor=0,failure_outbox=self.failures,worker_instance_id='joint-instance',engine_cleanup=self.engine_cleanup,cleanup_executor=ScratchCleanupExecutor(self.ledger),state_owner=self.owner)
        self.job=self.call('submit')['jobId']
        self.original_claim=None;self.original_receipt=None

    def serve(self,api):
        context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.load_cert_chain(self.cert,self.key)
        server=create_reference_http_server(api,ssl_context=context)
        thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':0.05});thread.start()
        def stop():server.shutdown();thread.join(5);server.server_close();self.assertFalse(thread.is_alive())
        self.stack.callback(stop)
        return 'https://127.0.0.1:'+str(server.server_port)

    def start_node(self,*,fail_after_commit=False):
        config=dict(keyPath=str(self.key),certPath=str(self.cert),controlEndpoint=self.control_origin,controlToken=VIEWER,fixtureControlToken=FIXTURE,environment='staging',host='127.0.0.1',port=0,fixtureRoot=str(self.viewer_state),failAfterLedgerCommit=fail_after_commit)
        path=self.root/'fixture-config.json';path.write_text(json.dumps(config));path.chmod(0o600)
        env=dict(os.environ,REFERENCE_SYNTHETIC_FIXTURE='1',NODE_EXTRA_CA_CERTS=str(self.cert));env.pop('NODE_TLS_REJECT_UNAUTHORIZED',None)
        self.process=subprocess.Popen(['node',str(self.viewer_root/'test/helpers/reference-joint-harness.cjs'),str(path)],stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=env)
        ready=selectors.DefaultSelector()
        try:
            ready.register(self.process.stdout,selectors.EVENT_READ)
            if not ready.select(8):raise AssertionError('synthetic_node_start_deadline')
            line=self.process.stdout.readline(16384)
            if not line:raise AssertionError('synthetic_node_start_failed')
            value=json.loads(line);self.assertTrue(value['ready']);self.node_origin='https://127.0.0.1:'+str(value['port'])
        finally:ready.close()

    def stop_node(self):
        if self.process is not None:
            if self.process.poll() is None:
                self.process.terminate()
                try:self.process.wait(5)
                except subprocess.TimeoutExpired:self.process.kill();self.process.wait(5)
            self.process.stdout.close();self.process.stderr.close();self.process=None

    def call(self,action):
        request=Request(self.node_origin+'/fixture/'+action,headers={'Authorization':'Bearer '+FIXTURE,'Content-Type':'application/json'},data=None if action=='state' else b'{}',method='GET' if action=='state' else 'POST')
        with build_opener(ProxyHandler({}),HTTPSHandler(context=self.tls)).open(request,timeout=8) as response:return json.loads(response.read(4*1024**2))

    def delivered(self):
        for _ in range(50):
            self.call('pump');result=self.driver.step()
            self.assertNotEqual(result['state'],'held',result)
            private=self.store.get(self.job,private=True)
            if private.get('claimToken'):
                current={key:private[key] for key in ('jobId','generation','claimToken','manifestSha256','pipelineDigest')}
                with self.driver._db() as db:current['claimRequestId']=db.execute('SELECT claim_request FROM driver_jobs WHERE job=?',(self.job,)).fetchone()[0]
                if self.original_claim is None:self.original_claim=current
                else:self.assertEqual(current,self.original_claim)
            with NodeGate('joint-engine') as gate:
                receipt=gate.snapshot()['reservation']
                if receipt:
                    if self.original_receipt is None:self.original_receipt=receipt['receiptId']
                    else:self.assertEqual(receipt['receiptId'],self.original_receipt)
            with self.driver._db() as db:stage=db.execute('SELECT stage FROM driver_jobs WHERE job=?',(self.job,)).fetchone()
            if private['phase']=='awaiting_acceptance' and stage[0]=='awaiting_acceptance':return
            # Advance only the explicit outbox retry clock, never expiration or
            # coordinator/engine time. No SQL retry-state rewrite or wall sleep.
            self.outbox_now+=10
        self.fail('actual_joint_delivery_deadline')

    def assert_retained_before_acceptance(self):
        self.assertEqual(self.native_fixture.counts().get('remove',0),0)
        self.assertEqual(len(list(self.ledger.root.glob('jobs/*/all.zip'))),1)
        self.assertEqual(len(list(self.ledger.root.glob('jobs/*/*.input'))),1)
        self.assertEqual(self.streams,0)
        with closing(sqlite3.connect(self.ledger.path)) as db:self.assertEqual(db.execute("SELECT COUNT(*) FROM pins WHERE state!='released'").fetchone()[0],0)

    def finish_and_check(self):
        private=self.store.get(self.job,private=True);result=private['resultManifest']
        self.assertEqual(private['phase'],'completed')
        self.assertEqual(result['engineVersions'],{'odm':'synthetic-joint-v1'})
        saved=list((self.viewer_state/'results').glob('*.zip'));self.assertEqual(len(saved),1);self.assertEqual(saved[0].read_bytes(),ARCHIVE)
        for _ in range(3):self.driver.step()
        self.assertEqual(self.native_fixture.counts().get('remove',0),1)
        self.assertEqual(len(list(self.ledger.root.glob('jobs/*/all.zip'))),0)
        inputs=list(self.ledger.root.glob('jobs/*/*.input'));self.assertEqual(len(inputs),1);self.assertEqual(inputs[0].read_bytes(),b'abc')
        self.assertEqual((self.viewer_state/'input/Survey_001.JPG').read_bytes(),b'abc')
        with closing(sqlite3.connect(self.viewer_state/'acceptance.sqlite')) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM acceptances').fetchone()[0],1)
            self.assertEqual(db.execute('SELECT acked FROM ack_outbox').fetchone()[0],1)
        self.assertEqual(self.streams,0)
        self.assertIsNotNone(self.original_claim);self.assertIsNotNone(self.original_receipt)
        with self.driver._db() as db:data=json.loads(db.execute('SELECT data FROM driver_jobs WHERE job=?',(self.job,)).fetchone()[0])
        self.assertEqual(data['claim']['claimToken'],self.original_claim['claimToken'])
        with closing(sqlite3.connect(self.ledger.path)) as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM reservations').fetchone()[0],1)
        with closing(sqlite3.connect(self.viewer_state/'broker.sqlite')) as db:
            rows=db.execute('SELECT metadata,used_bytes,reserved_bytes,in_flight FROM reference_grants').fetchall();self.assertEqual(len(rows),1)
            grant=json.loads(rows[0][0]);self.assertEqual(grant['reservationId'],data['reservation']['reservationId']);self.assertEqual(grant['byteBudget'],9)
            self.assertEqual(rows[0][1:],(3,0,0))
        with NodeGate('joint-engine') as gate:self.assertIsNone(gate.snapshot()['reservation'])
        self.assertEqual({key:self.native_fixture.counts().get(key,0) for key in ('initialize','upload','commit','download','remove')},{'initialize':1,'upload':1,'commit':1,'download':1,'remove':1})

    def test_real_node_broker_provider_acceptance_and_python_worker_cleanup_order(self):
        self.delivered();self.driver.step();self.assert_retained_before_acceptance()
        reply=self.call('ingest');self.assertTrue(reply['durableCopy']);self.assertTrue(reply['acknowledged']);self.assertFalse(reply['published'])
        replay=self.call('replay-acceptance');self.assertEqual(replay['replayed'],0)
        self.finish_and_check()

    def test_node_crash_after_durable_acceptance_replays_original_receipt_before_cleanup(self):
        self.delivered();self.stop_node();self.start_node(fail_after_commit=True)
        with self.assertRaises(Exception):self.call('ingest')
        self.assertEqual(self.process.wait(8),86);self.stop_node()
        self.assertEqual(self.store.get(self.job)['phase'],'awaiting_acceptance')
        self.driver.step();self.assert_retained_before_acceptance()
        with closing(sqlite3.connect(self.viewer_state/'acceptance.sqlite')) as db:original=db.execute('SELECT payload FROM ack_outbox').fetchone()[0]
        self.start_node();reply=self.call('replay-acceptance');self.assertEqual(reply['replayed'],1)
        self.assertEqual(reply['receipts'][0]['acceptance'],json.loads(original));self.assertFalse(reply['receipts'][0]['published'])
        self.finish_and_check()

    def test_worker_completion_ack_loss_replays_same_durable_outbox_before_viewer_acceptance(self):
        self.drop_result_ack=True;self.delivered()
        self.assertEqual(len(self.completion_envelopes),2)
        self.assertEqual(self.completion_envelopes[0],self.completion_envelopes[1])
        self.assert_retained_before_acceptance()
        reply=self.call('ingest');self.assertTrue(reply['acknowledged']);self.assertFalse(reply['published'])
        self.finish_and_check()


if __name__=='__main__':unittest.main()
