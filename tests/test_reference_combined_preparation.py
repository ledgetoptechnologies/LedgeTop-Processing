"""Actual Driver.step/TLS coordinator+broker/native HTTP; no processing runs."""
from contextlib import ExitStack,closing
from dataclasses import replace
import hashlib
import io
import json
import os
from pathlib import Path
import socket
import sqlite3
import ssl
import tempfile
import threading
import unittest
from unittest.mock import patch

from ops.reference_worker_daemon import DaemonStateLock
from ops.native_node_fixture import NativeNodeFixture
from processing import reference_protocol as p
from processing.reference_combined_preparation import CombinedPreparationPolicy
from processing.reference_worker_recovery import RecoveryPolicy
from processing.reference_pipeline import PipelinePolicy
from processing.reference_odm_stage import NativeRuntimePolicy,NativeRuntimeProbe
from processing.reference_driver import ReferenceDriver
from processing.reference_store import ReferenceStore
from processing.reference_worker_api import ReferenceWorkerAPI
from processing.reference_tls import verified_client_context
from processing.reference_delivery import DeliveryReceiver
from processing.reference_recovery import RecoveryNodeBinding
from processing.reference_http import create_reference_http_server
from processing.reference_binary import BinaryResponse
from processing.reference_api import Response
from processing.reference_transfer import BrokerClient,OBJECT_PREFIX
from processing.reference_engine_worker import ReferenceEngineWorker
from processing.reference_failure import FailureOutbox
from processing.worker_outbox import WorkerOutbox
from processing.scratch_policy import ScratchLedger
from processing.node_gate import NodeGate
from processing.prototype import Queue
from processing.viewer_service import NativeEngine
from tests import test_reference_parent_guard as tls_fixture
from tests import test_reference_odm_stage as docker_fixture

TOKEN='synthetic-combined-worker-credential'
GRANT='synthetic-original-input-grant'


@unittest.skipUnless(os.name=='posix','actual main Linux preparation')
class CombinedPreparationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):tls_fixture.ParentGuardTests.setUpClass.__func__(cls)

    def setUp(self):
        self.stack=ExitStack();self.addCleanup(self.stack.close)
        self.root=Path(self.stack.enter_context(tempfile.TemporaryDirectory()));self.root.chmod(0o700)
        self.owner=self.stack.enter_context(DaemonStateLock(self.root))
        self.queue=Queue(self.root/'queue',node_ids=('cluster','main'),treedar_node_ids=('main',),reference_enabled=True);self.stack.callback(self.queue.close);self.store=ReferenceStore(self.queue)
        self.native_fixture=NativeNodeFixture(self.root/'native-http.sqlite',engine_version='3.8.5',synthetic_enabled=True).start();self.stack.callback(self.native_fixture.close)
        self.native=NativeEngine(self.native_fixture.origin,'synthetic-native-token')
        sock=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);sock.bind(str(self.root/'docker.sock'));self.stack.callback(sock.close)
        compose=dict(services=dict(nodeodm=dict(image='sha256:'+'a'*64,container_name='native-node')),networks={})
        path=self.root/'compose.json';path.write_text(json.dumps(compose));path.chmod(0o600);base=json.loads(json.dumps(compose));base['services']['nodeodm'].pop('image')
        runtime=NativeRuntimePolicy('/usr/bin/docker','unix://'+str(self.root/'docker.sock'),'physical-main',str(path),p.digest(base),'native-project','nodeodm','native-node','sha256:'+'a'*64,self.native.origin,'odm','3.8.5')
        recovery=RecoveryPolicy(preparation_allowance=3*(128+128)*4096+65536)
        pipeline=PipelinePolicy('main-worker','original-instance',self.queue.authority_id,'physical-main','http://127.0.0.1:18080',1,'3.8.5','0.0.1',runtime.sha256,'b'*64,'c'*64,100,100,recovery_control_budget=recovery.control_allowance)
        self.policy=CombinedPreparationPolicy(pipeline,recovery,runtime,driver_page_cap=128,pipeline_page_cap=128)
        self.docker=docker_fixture.DockerProbeTransport(runtime);self.probe=NativeRuntimeProbe(runtime,runner=self.docker)
        self.stack.enter_context(patch('processing.node_gate.canonical_gate_path',return_value=self.root/'gate'))
        with NodeGate('physical-main') as gate:gate.provision('main',pipeline.coordinator_origin,pipeline.authority_id)
        scratch=self.root/'scratch';scratch.mkdir(mode=0o700)
        self.ledger=ScratchLedger(self.root/'scratch.sqlite',scratch,capacity_probe=lambda _:1024**3,safety_floor_bytes=0)
        self.outbox=WorkerOutbox(self.root/'outbox.sqlite');self.failures=FailureOutbox(self.root/'failures.sqlite')
        self.worker=ReferenceEngineWorker(self.root/'worker.sqlite',ledger=self.ledger,outbox=self.outbox,engine=self.native,engine_id='physical-main',node_id='main',authority_id=pipeline.authority_id,coordinator_origin=pipeline.coordinator_origin,gate_generation=1,worker_id='main-worker',engine_version='3.8.5')
        self.worker_api=self.api();self.calls=[];self.claim_requests=[];self.grant_ready=False
        test=self
        class Endpoint:
            def handle(endpoint,method,path,headers,body,**kwargs):
                test.calls.append((method,path))
                if method=='POST' and path.endswith('/claim'):test.claim_requests.append(json.loads(body)['claimRequestId'])
                if method=='GET' and path.startswith(OBJECT_PREFIX):
                    if headers.get('Authorization')!='Bearer '+GRANT:return Response(401,{'error':'synthetic_auth'})
                    class Stream:
                        def chunks(stream):yield b'abc'
                        def close(stream):pass
                    return BinaryResponse(Stream(),headers={'Content-Type':'application/octet-stream','Content-Length':'3'})
                return test.worker_api.handle(method,path,headers,body,**kwargs)
        context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.load_cert_chain(self.cert,self.key)
        self.server=create_reference_http_server(Endpoint(),ssl_context=context);thread=threading.Thread(target=self.server.serve_forever);thread.start()
        def stop():self.server.shutdown();thread.join(5);self.server.server_close()
        self.stack.callback(stop);self.origin='https://127.0.0.1:'+str(self.server.server_port);self.tls=verified_client_context(str(self.cert))
        self.broker=BrokerClient(self.origin,ssl_context=self.tls)
        self.driver=self.make_driver()
        raw=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())['submission']
        raw['manifest']['objects'].append(dict(id='original-lidar',name='original.las',byteSize=3,sha256=hashlib.sha256(b'abc').hexdigest(),kind='lidar',mediaType='application/octet-stream'))
        raw['manifest']['objectCount']+=1;raw['manifest']['totalBytes']+=3;raw['manifest']['inputCounts'].update(lidarObjects=1,points=10);raw['manifest']['sha256']=p.manifest_sha256(raw['manifest'])
        raw['pipeline']['treedar']=dict(requested=True,inputObjectIds=['original-lidar'],provenanceReceiptId='unavailable-original-receipt')
        checked=p.validate_submission(raw,environment='staging',capability_fingerprint=raw['capabilityFingerprint'],available_engines=('odm','treedar'),allowed_odm_options=set())
        self.job=self.store.submit(checked)[0]

    def api(self):
        core=self.policy.pipeline
        return ReferenceWorkerAPI(self.store,DeliveryReceiver(self.store),bearer_workers={TOKEN:dict(node_id='main',worker_id='main-worker',worker_instance_id='original-instance')},recovery_bindings={'main':RecoveryNodeBinding('main',core.engine_id,core.authority_id,core.coordinator_origin,1,core.output_budget)})

    def make_driver(self,policy=None):
        return ReferenceDriver(self.worker,self.ledger,self.outbox,self.broker,self.origin,TOKEN,state_path=self.root/'driver.sqlite',ssl_context=self.tls,output_budget=self.policy.pipeline.output_budget,transfer_ttl=600,safety_floor=0,failure_outbox=self.failures,worker_instance_id='original-instance',state_owner=self.owner,combined_policy=policy or self.policy,combined_runtime_probe=self.probe)

    def drive(self,until='combined_prepared_held'):
        states=[]
        for _ in range(25):
            result=self.driver.step();states.append(result)
            if result['state']=='awaiting_grant' and not self.grant_ready:
                private=self.store.get(self.job['jobId'],private=True);request=private['transferRequest']
                grant=dict(request,jobId=private['jobId'],environment=private['environment'],objectIds=[o['id'] for o in private['submission']['manifest']['objects']],token=GRANT)
                self.store.receive_transfer_grant(private['jobId'],grant);self.grant_ready=True
            if result['state']==until:return result
            if result['state'] in ('held','retry_wait'):
                self.fail('actual preparation stopped: '+repr(result)+'; states='+repr(states))
        self.fail('actual preparation did not reach '+until+': '+repr(states))

    def test_actual_driver_tls_fetch_and_original_gate_preparation_smoke(self):
        result=self.drive();self.assertEqual(result['reason'],'combined_metadata_and_final_transport_unqualified')
        with closing(sqlite3.connect(self.driver.path)) as db:
            row=db.execute('SELECT data FROM driver_jobs').fetchone();data=json.loads(row[0]);identity=json.loads(db.execute('SELECT identity FROM combined_parent_preparations').fetchone()[0])
        parent=identity['parentBinding'];self.assertEqual(parent['reservationId'],data['reservation']['reservationId'])
        self.assertEqual(set(data['inputPaths']),{o['id'] for o in data['claim']['submission']['manifest']['objects']})
        with NodeGate('physical-main') as gate:
            held=gate.snapshot()['reservation'];self.assertEqual(held['receiptId'],parent['gateReceiptId']);self.assertEqual(held['job']['id'],self.job['jobId']);self.assertIsNone(held['completionDigest'])
        receipt=self.ledger.read_control_charge(parent['reservationId']);self.assertIsNotNone(receipt['publicationDescriptor'])
        self.assertEqual(self.native_fixture.counts()['initialize'],0)
        self.assertEqual(self.driver.step()['state'],'combined_prepared_held')

    def intent(self):
        with closing(sqlite3.connect(self.driver.path)) as db:return json.loads(db.execute('SELECT identity FROM combined_parent_preparations').fetchone()[0])

    def test_lost_reserve_reply_reconciles_exact_original_snapshot_without_second_reserve(self):
        self.drive('execution_pending');reserve=NodeGate.reserve;calls=[]
        def lose(gate,*args,**kwargs):calls.append(args);reserve(gate,*args,**kwargs);raise OSError('synthetic lost local reserve reply')
        with patch.object(NodeGate,'reserve',new=lose):self.assertEqual(self.driver.step()['state'],'retry_wait')
        original=self.intent();self.driver=self.make_driver();self.assertEqual(self.driver.step()['state'],'combined_prepared_held')
        self.assertEqual(self.intent()['parentBinding'],original['parentBinding']);self.assertEqual(len(calls),1);self.assertEqual(len(self.claim_requests),1)

    def test_lost_bind_reply_reconciles_original_job_receipt_without_second_bind(self):
        self.drive('execution_pending');bind=NodeGate.bind;calls=[]
        def lose(gate,*args,**kwargs):calls.append(args);bind(gate,*args,**kwargs);raise OSError('synthetic lost local bind reply')
        with patch.object(NodeGate,'bind',new=lose):self.assertEqual(self.driver.step()['state'],'retry_wait')
        original=self.intent();self.driver=self.make_driver();self.assertEqual(self.driver.step()['state'],'combined_prepared_held')
        self.assertEqual(self.intent()['parentBinding'],original['parentBinding']);self.assertEqual(len(calls),1)

    def test_absent_unknown_reserve_and_uncommitted_bind_never_blindly_replay(self):
        self.drive('execution_pending');calls=[]
        def fail(*args,**kwargs):calls.append(True);raise OSError('unknown before reserve outcome')
        with patch.object(NodeGate,'reserve',new=fail):self.assertEqual(self.driver.step()['state'],'retry_wait')
        self.assertEqual(self.driver.step()['state'],'held');self.assertEqual(len(calls),1)
        with NodeGate('physical-main') as gate:self.assertIsNone(gate.snapshot()['reservation'])

    def test_unknown_uncommitted_bind_preserves_original_reservation_without_retry(self):
        self.drive('execution_pending');calls=[]
        def fail(*args,**kwargs):calls.append(True);raise OSError('unknown bind outcome')
        with patch.object(NodeGate,'bind',new=fail):self.assertEqual(self.driver.step()['state'],'retry_wait')
        original=self.intent();self.assertEqual(self.driver.step()['state'],'held');self.assertEqual(len(calls),1)
        with NodeGate('physical-main') as gate:
            held=gate.snapshot()['reservation'];self.assertEqual(held['receiptId'],original['parentBinding']['gateReceiptId']);self.assertIsNone(held['job'])

    def test_fresh_coordinator_operator_hold_between_reserve_and_bind_blocks_bind(self):
        self.drive('execution_pending');reserve=NodeGate.reserve
        def hold(gate,*args,**kwargs):
            result=reserve(gate,*args,**kwargs);self.queue.operator(self.job['jobId'],'hold',{'note':'synthetic between local operations'});return result
        with patch.object(NodeGate,'reserve',new=hold):self.assertEqual(self.driver.step()['state'],'held')
        with NodeGate('physical-main') as gate:self.assertIsNone(gate.snapshot()['reservation']['job'])
        self.assertEqual(self.native_fixture.counts()['initialize'],0)

    def test_reservation_before_full_claim_save_loss_uses_same_claim_and_reservation(self):
        self.drive('claim_pending');connect=sqlite3.connect;failed=[]
        class FailingSave(sqlite3.Connection):
            def execute(connection,statement,parameters=()):
                if statement.startswith('UPDATE driver_jobs SET stage=') and parameters[0]=='reservation_pending' and not failed:
                    failed.append(True);raise OSError('synthetic full claim write failure')
                return super().execute(statement,parameters)
        def fault(*args,**kwargs):kwargs['factory']=FailingSave;return connect(*args,**kwargs)
        with patch('sqlite3.connect',side_effect=fault):self.assertEqual(self.driver.step()['state'],'retry_wait')
        with closing(sqlite3.connect(self.ledger.path)) as db:original=db.execute('SELECT id,settings FROM reservations').fetchone()
        self.driver=self.make_driver();self.assertEqual(self.driver.step()['state'],'reservation_pending')
        with closing(sqlite3.connect(self.ledger.path)) as db:self.assertEqual(db.execute('SELECT id,settings FROM reservations').fetchall(),[original])
        self.assertEqual(len(self.claim_requests),2);self.assertEqual(self.claim_requests[0],self.claim_requests[1])
        self.drive()

    def test_prepared_restart_keeps_original_gate_and_recovers_original_coordinator_claim(self):
        self.drive();original=self.intent();token=self.store.get(self.job['jobId'],private=True)['claimToken']
        self.queue.close();self.queue=Queue(self.root/'queue',node_ids=('cluster','main'),treedar_node_ids=('main',),reference_enabled=True);self.stack.callback(self.queue.close)
        self.store=ReferenceStore(self.queue);self.worker_api=self.api();self.driver=self.make_driver()
        self.assertEqual(self.driver.step()['state'],'combined_prepared_held');self.assertEqual(self.intent()['parentBinding'],original['parentBinding'])
        self.assertEqual(self.store.get(self.job['jobId'],private=True)['claimToken'],token);self.assertEqual(self.native_fixture.counts()['initialize'],0)

    def test_partial_core_construction_retries_without_cached_half_components(self):
        self.drive('execution_pending')
        with patch('processing.reference_combined_preparation.DriverClaimTokens',side_effect=OSError('synthetic component construction loss')):self.assertEqual(self.driver.step()['state'],'retry_wait')
        self.assertIsNone(self.driver.combined_preparation.journal);self.assertEqual(self.driver.step()['state'],'combined_prepared_held')

    def test_profile_drift_and_driver_page_cap_fail_without_history_rewrite(self):
        self.drive();before=self.driver.path.read_bytes()
        recovery=replace(self.policy.recovery,per_job_attempts=7);changed=replace(self.policy,recovery=recovery)
        with self.assertRaises(Exception):self.make_driver(changed)
        self.assertEqual(before,self.driver.path.read_bytes())
        with self.driver.path.open('r+b') as stream:stream.truncate(self.policy.driver_page_cap*4096+4096)
        before=self.driver.path.read_bytes()
        with self.assertRaises(Exception):self.make_driver()
        self.assertEqual(before,self.driver.path.read_bytes())
