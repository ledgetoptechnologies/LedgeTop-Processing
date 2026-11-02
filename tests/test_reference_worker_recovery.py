"""Actual TLS recovery service and ProductionParentGuard; synthetic parent data."""
from contextlib import closing,ExitStack,contextmanager
import copy
from datetime import datetime,timedelta,timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import socket
import ssl
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from processing import reference_worker_recovery as m
from processing.reference_api import Response
from processing.reference_delivery import DeliveryReceiver
from processing.reference_recovery import RecoveryNodeBinding
from processing.reference_store import ReferenceStore
from processing.reference_worker_api import ReferenceWorkerAPI
from processing.reference_tls import verified_client_context
from processing.reference_parent_guard import ClaimStatePolicy,ClaimStateClient,WorkerCredential,DriverClaimTokens,ProductionParentGuard,tls_trust_sha256
from processing.reference_pipeline import PipelineJournal,PipelinePolicy
from processing.reference_http import create_reference_http_server
from processing.node_gate import NodeGate
from processing.scratch_policy import ScratchLedger
from processing.prototype import Queue
from ops.reference_worker_daemon import DaemonStateLock
from tests import test_reference_parent_guard as fixtures
from tests.test_reference_pipeline import ActualGuard
from tests.test_reference_pipeline import SyntheticStage
from tests import test_reference_odm_stage as native_fixtures
from processing.reference_odm_stage import NativeODMStage,NativeRuntimePolicy,NativeRuntimeProbe
from processing.viewer_service import NativeEngine
from ops.native_node_fixture import NativeNodeFixture


class LargeRecoveryFixture:
    """Provision realistic control headroom before immutable parent creation."""
    def __init__(self,cert,key,*,native=False):
        self.stack=ExitStack();root=Path(self.stack.enter_context(tempfile.TemporaryDirectory()));root.chmod(0o700)
        self.owner=self.stack.enter_context(DaemonStateLock(root));h=self.host=SimpleNamespace(root=root)
        h.queue=Queue(root/'queue',node_ids=('cluster','main'),treedar_node_ids=('main',),reference_enabled=True);self.stack.callback(h.queue.close)
        h.store=ReferenceStore(h.queue)
        native_sha='1'*64
        if native:
            h.native_fixture=NativeNodeFixture(root/'native-http.sqlite',engine_version='3.8.5',synthetic_enabled=True).start();self.stack.callback(h.native_fixture.close)
            h.native=NativeEngine(h.native_fixture.origin,'synthetic-native-credential')
            docker_socket=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);docker_socket.bind(str(root/'docker.sock'));self.stack.callback(docker_socket.close)
            compose=dict(services=dict(nodeodm=dict(image='sha256:'+'a'*64,container_name='native-node')),networks={})
            compose_file=root/'compose.json';compose_file.write_text(json.dumps(compose));compose_file.chmod(0o600)
            baseline=copy.deepcopy(compose);baseline['services']['nodeodm'].pop('image')
            h.runtime=NativeRuntimePolicy('/usr/bin/docker','unix://'+str(root/'docker.sock'),'physical-main',str(compose_file),m.p.digest(baseline),'native-project','nodeodm','native-node','sha256:'+'a'*64,h.native.origin,'odm','3.8.5')
            h.docker=native_fixtures.DockerProbeTransport(h.runtime);native_sha=h.runtime.sha256
        raw=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())['submission']
        raw['manifest']['objects'].append(dict(id='original-lidar',name='original.las',byteSize=3,sha256=hashlib.sha256(b'abc').hexdigest(),kind='lidar',mediaType='application/octet-stream'))
        raw['manifest']['objectCount']+=1;raw['manifest']['totalBytes']+=3;raw['manifest']['inputCounts'].update(lidarObjects=1,points=10)
        raw['manifest']['sha256']=m.p.manifest_sha256(raw['manifest']);raw['pipeline']['treedar']=dict(requested=True,inputObjectIds=['original-lidar'],provenanceReceiptId='original-source')
        checked=m.p.validate_submission(raw,environment='staging',capability_fingerprint=raw['capabilityFingerprint'],available_engines=('odm','treedar'),allowed_odm_options=set(raw['pipeline']['odm']['options']))
        public,_=h.store.submit(checked);h.claim=h.store.claim(public['jobId'],node_id='main',worker_id='main-worker',worker_instance_id='instance-one',claim_request_id='pipeline-claim')
        h.policy=PipelinePolicy('main-worker','instance-one',h.queue.authority_id,'physical-main','http://127.0.0.1:18080',1,'3.8.5','0.0.1',native_sha,'2'*64,'3'*64,100,100,recovery_control_budget=m.RecoveryPolicy().control_allowance)
        self.stack.enter_context(patch('processing.node_gate.canonical_gate_path',return_value=root/'gate'))
        with NodeGate(h.policy.engine_id) as gate:
            gate.provision('main',h.policy.coordinator_origin,h.policy.authority_id);gate.reserve('main',h.policy.coordinator_origin,h.policy.authority_id,1,'a'*32)
            gate.bind('a'*32,dict(id=h.claim['jobId'],workerId='main-worker',nodeId='main',authorityId=h.policy.authority_id))
        scratch=root/'scratch';scratch.mkdir(mode=0o700)
        h.ledger=ScratchLedger(root/'scratch.sqlite',scratch,capacity_probe=lambda _:64*1024**2,safety_floor_bytes=0)
        output=h.policy.output_budget
        h.ledger.reserve(job_id=h.claim['jobId'],generation=1,reservation_id='parent-reservation',manifest_sha256=h.claim['manifestSha256'],input_bytes=checked['manifest']['totalBytes'],output_budget=output,environment='staging',ttl_seconds=600,worker_ready=True)
        h.ledger.record_allocated('parent-reservation',checked['manifest']['totalBytes'])
        directory=scratch/'jobs'/'parent-reservation';directory.mkdir(mode=0o700,parents=True);directory.parent.chmod(0o700)
        input_paths={}
        for obj in checked['manifest']['objects']:
            relative='jobs/parent-reservation/'+obj['id']+'.input';path=scratch/relative
            with path.open('wb') as stream:stream.write(b'abc');stream.flush();os.fsync(stream.fileno())
            path.chmod(0o600);input_paths[obj['id']]=relative
        h.input_paths=input_paths
        h.journal=PipelineJournal(root/'pipeline.sqlite',h.policy,verify_provenance=lambda _:False);h.guard=ActualGuard(h)
        h.plan=h.journal.bind_parent(h.claim,claim_request_id='pipeline-claim',reservation_id='parent-reservation',gate_receipt_id='a'*32,guard=h.guard);self.parent=h.plan['parentBinding']
        transfer=dict(reservationId='parent-reservation',generation=1,expiresAt=(datetime.now(timezone.utc)+timedelta(seconds=240)).isoformat(timespec='milliseconds').replace('+00:00','Z'),manifestSha256=h.claim['manifestSha256'],byteBudget=checked['manifest']['totalBytes'],nonce='synthetic-transfer')
        h.store.reserve(h.claim['jobId'],transfer,node_id='main');h.store.update_phase(h.claim['jobId'],'fetching');h.store.update_phase(h.claim['jobId'],'processing')
        self.api=ReferenceWorkerAPI(h.store,DeliveryReceiver(h.store),bearer_workers={fixtures.WORKER:dict(node_id='main',worker_id='main-worker',worker_instance_id='instance-one')})
        serverctx=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);serverctx.load_cert_chain(cert,key)
        self.server=create_reference_http_server(self.api,ssl_context=serverctx);thread=threading.Thread(target=self.server.serve_forever);thread.start()
        def stop():self.server.shutdown();thread.join(5);self.server.server_close()
        self.stack.callback(stop);context=verified_client_context(str(cert))
        self.policy=ClaimStatePolicy('https://127.0.0.1:'+str(self.server.server_port),h.policy.authority_id,'main','main-worker','instance-one','worker-main',tls_trust_sha256(context))
        self.client=ClaimStateClient(self.policy,WorkerCredential('worker-main',fixtures.WORKER),ssl_context=context)
        binding=dict(origin=self.policy.origin,nodeId='main',workerId='main-worker',workerInstanceId='instance-one',authorityId=h.policy.authority_id,engineId=h.policy.engine_id,gateOrigin=h.policy.coordinator_origin,gateGeneration=1,outputBudget=output,environments=sorted(h.policy.allowed_environments))
        self.driver=root/'driver.sqlite'
        with closing(sqlite3.connect(self.driver)) as db,db:
            db.execute('CREATE TABLE driver_binding(singleton INTEGER PRIMARY KEY,binding TEXT)');db.execute('INSERT INTO driver_binding VALUES(1,?)',(json.dumps(binding),))
            db.execute('CREATE TABLE driver_jobs(job TEXT PRIMARY KEY,claim_request TEXT,data TEXT)');db.execute('INSERT INTO driver_jobs VALUES(?,?,?)',(h.claim['jobId'],'pipeline-claim',json.dumps(dict(claim=h.store.get(h.claim['jobId'],private=True),reservation=transfer,inputPaths=input_paths))))
        self.driver.chmod(0o600);tokens=DriverClaimTokens(self.driver,journal=h.journal,policy=self.policy,state_owner=self.owner)
        self.guard=ProductionParentGuard(journal=h.journal,ledger=h.ledger,client=self.client,tokens=tokens,state_owner=self.owner)
        if native:
            h.native_stage=NativeODMStage(root/'native-stage.sqlite',h.runtime,journal=h.journal,ledger=h.ledger,engine=h.native,parent_guard=self.guard,state_owner=self.owner,input_resolver=native_fixtures.OriginalInputs(h),runtime_probe=NativeRuntimeProbe(h.runtime,runner=h.docker))
    def doCleanups(self):self.stack.close()


@unittest.skipUnless(os.name=='posix','real Linux worker/coordinator ownership')
class WorkerRecoveryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fixtures.ParentGuardTests.setUpClass.__func__(cls)

    def setUp(self):
        self.base=LargeRecoveryFixture(self.cert,self.key)
        self.addCleanup(self.base.doCleanups);self.host=self.base.host;self.parent=self.base.parent
        self.calls=[];self.fault=None;self.bodies=[];self.response_fault=None
        self.client=self.open();self.client.prepare(self.parent)
        self.host.ledger.set_engine_state('parent-reservation','engine_started');self.activate()
        def dispatch(method,path,headers,body,**kwargs):
            operation=path.rsplit('/',1)[-1];self.calls.append((method,operation))
            if method=='POST' and operation=='recover':
                payload=json.loads(body);self.bodies.append(copy.deepcopy(payload))
                # Observe actual journal from a different read-only connection
                # before the server can receive an authorized mutation.
                with closing(sqlite3.connect(self.client.path.as_uri()+'?mode=ro',uri=True)) as db:
                    row=db.execute('SELECT body,phase FROM worker_recovery_attempts WHERE request_id=?',(payload['requestId'],)).fetchone()
                self.assertEqual(row,(m.encoded(payload),'issued'))
                if self.fault=='before_post':return Response(503,{'error':'synthetic_unavailable'})
            response=self.api.handle(method,path,headers,body,**kwargs)
            if method=='POST' and operation=='recover' and self.fault=='after_post':return Response(503,{'error':'synthetic_lost_reply'})
            if self.response_fault:return self.response_fault(method,path,response)
            return response
        self.base.api.handle=dispatch

    def activate(self):
        h=self.host;policy=h.policy
        self.api=ReferenceWorkerAPI(h.store,DeliveryReceiver(h.store),bearer_workers={fixtures.WORKER:dict(node_id='main',worker_id='main-worker',worker_instance_id='instance-one')},
            recovery_bindings={'main':RecoveryNodeBinding('main',policy.engine_id,policy.authority_id,policy.coordinator_origin,1,policy.output_budget)})

    def restart(self):
        h=self.host;h.queue.close();h.queue=Queue(h.root/'queue',node_ids=('cluster','main'),treedar_node_ids=('main',),reference_enabled=True)
        self.addCleanup(h.queue.close);h.store=ReferenceStore(h.queue);self.activate()

    def open(self):return m.OriginalClaimRecovery(self.host.root/'worker-recovery.sqlite',parent_guard=self.base.guard)
    def recover(self):return self.client.recover(self.parent)
    def rows(self):
        with closing(sqlite3.connect(self.client.path)) as db:
            db.row_factory=sqlite3.Row;return [dict(r) for r in db.execute('SELECT * FROM worker_recovery_attempts ORDER BY rowid')]
    def held(self):
        with self.assertRaises(m.WorkerRecoveryHeld) as caught:self.recover()
        self.assertNotIn(fixtures.WORKER,str(caught.exception));self.assertNotIn(self.host.claim['claimToken'],str(caught.exception))

    def test_actual_original_claim_recovers_with_full_intent_and_fresh_guard(self):
        token=self.host.claim['claimToken'];before=self.host.ledger.path.read_bytes();gate=(self.host.root/'gate'/'gate.json').read_bytes()
        result=self.recover();self.assertIs(result['executable'],True);self.assertEqual(self.host.queue.get(self.parent['jobId'])['status'],'running')
        self.assertEqual(len(self.bodies),1);self.assertEqual(self.rows()[0]['phase'],'receipted')
        self.assertEqual(self.host.ledger.path.read_bytes(),before);self.assertEqual((self.host.root/'gate'/'gate.json').read_bytes(),gate)
        self.assertNotIn(token.encode(),self.client.path.read_bytes());self.assertNotIn(fixtures.WORKER.encode(),self.client.path.read_bytes())
        self.assertEqual(self.host.store.get(self.parent['jobId'],private=True)['claimToken'],token)
        self.assertIsNone(self.base.guard.check(self.parent,mutation=True))

    def test_committed_lost_response_is_reconciled_by_get_without_second_post(self):
        self.fault='after_post';self.held();request=self.bodies[0];self.assertEqual(self.rows()[0]['phase'],'issued')
        self.fault=None;self.client=self.open();self.assertTrue(self.recover()['executable']);self.assertEqual(len(self.bodies),1)
        self.assertIn(('GET',request['requestId']),self.calls);self.assertEqual(self.rows()[0]['phase'],'receipted')

    def test_uncommitted_lost_response_uses_receipt_lookup_then_same_id_same_body(self):
        self.fault='before_post';self.held();original=copy.deepcopy(self.bodies[0]);self.fault=None
        self.client=self.open();self.assertTrue(self.recover()['executable']);self.assertEqual(self.bodies,[original,original])
        self.assertEqual(self.rows()[0]['phase'],'receipted')

    def test_later_restart_requires_new_epoch_request_after_old_receipt_reconciliation(self):
        self.fault='after_post';self.held();old=self.bodies[0];self.restart();self.fault=None
        self.assertTrue(self.recover()['executable']);self.assertEqual(len(self.bodies),2)
        self.assertNotEqual(old['requestId'],self.bodies[1]['requestId']);self.assertNotEqual(old['coordinatorEpoch'],self.bodies[1]['coordinatorEpoch'])
        self.assertEqual([r['phase'] for r in self.rows()],['receipted','receipted'])

    def test_no_receipt_new_epoch_retires_only_with_fresh_eligible_fence(self):
        self.fault='before_post';self.held();old=self.bodies[0];self.restart();self.fault=None
        self.assertTrue(self.recover()['executable']);rows=self.rows();self.assertEqual(rows[0]['phase'],'absent_prior_epoch')
        self.assertIsNone(rows[0]['receipt']);self.assertEqual(rows[0]['body'],m.encoded(old));self.assertEqual(rows[1]['phase'],'receipted')

    def test_advanced_maintenance_revision_can_fence_old_absent_attempt(self):
        self.fault='before_post';self.held();old=self.bodies[0]
        with self.host.queue._db() as db:db.execute("UPDATE nodes SET state='draining',revision=revision+1 WHERE id='main'")
        self.fault=None;self.assertTrue(self.recover()['executable']);rows=self.rows()
        self.assertEqual(rows[0]['phase'],'absent_prior_revision');self.assertIsNone(rows[0]['receipt'])
        self.assertNotEqual(old['requestId'],self.bodies[1]['requestId']);self.assertEqual(old['coordinatorEpoch'],self.bodies[1]['coordinatorEpoch'])
        self.assertGreater(self.bodies[1]['expectedNodeRevision'],old['expectedNodeRevision'])

    def test_old_post_commits_between_missing_receipt_and_state_without_competing_resume(self):
        self.fault='before_post';self.held();original=copy.deepcopy(self.bodies[0]);self.fault=None
        api=self.api.handle;fired=[]
        def race(method,path,headers,body,**kwargs):
            response=api(method,path,headers,body,**kwargs)
            if method=='GET' and '/recovery-receipts/' in path and response.status==404 and not fired:
                fired.append(True)
                accepted=api('POST','/ltds/worker/v1/jobs/'+self.parent['jobId']+'/recover',headers,m.p.canonical_json(original),**kwargs)
                self.assertEqual(accepted.status,200)
            return response
        with patch.object(self.api,'handle',side_effect=race):self.assertTrue(self.recover()['executable'])
        self.assertEqual(len(self.bodies),1);self.assertEqual(self.rows()[0]['phase'],'issued')
        self.assertTrue(self.recover()['executable']);self.assertEqual(self.rows()[0]['phase'],'receipted');self.assertEqual(len(self.bodies),1)

    def test_missing_lookup_or_network_failure_never_means_failed_post(self):
        self.fault='before_post';self.held();self.fault=None
        def lose(method,path,response):return Response(503,{'error':'synthetic'}) if '/recovery-receipts/' in path else response
        self.response_fault=lose;self.held();self.assertEqual(len(self.bodies),1);self.assertEqual(self.rows()[0]['phase'],'issued')

    def test_historical_receipt_does_not_clear_operator_hold_or_authorize_execution(self):
        self.fault='after_post';self.held();self.fault=None
        self.host.queue.operator(self.parent['jobId'],'hold',{'note':'synthetic operator'})
        self.held();self.assertEqual(len(self.bodies),1);self.assertEqual(self.rows()[0]['phase'],'receipted')
        self.assertEqual(self.host.queue.get(self.parent['jobId'])['heldReason'],'operator')

    def test_local_fence_or_reservation_drift_prevents_post_and_new_claims(self):
        self.host.ledger.set_engine_state('parent-reservation','terminal',terminal_attested=True)
        with patch.object(self.host.store,'claim',side_effect=AssertionError('no new claim')),patch.object(self.host.ledger,'reserve',side_effect=AssertionError('no reserve')):self.held()
        self.assertEqual(self.bodies,[])

    def test_changed_original_transfer_digest_holds_without_resume(self):
        with closing(sqlite3.connect(self.base.driver)) as db,db:
            data=json.loads(db.execute('SELECT data FROM driver_jobs').fetchone()[0]);data['reservation']['nonce']='changed'
            db.execute('UPDATE driver_jobs SET data=?',(json.dumps(data),))
        self.held();self.assertEqual(self.bodies,[])

    def test_closed_state_and_receipt_bindings_are_checked(self):
        def corrupt(method,path,response):
            if path.endswith('/recovery-state'):
                body=dict(response.body);body['extra']='bad';return Response(response.status,body)
            return response
        self.response_fault=corrupt;self.held();self.assertEqual(self.bodies,[])
        def receipt_corrupt(method,path,response):
            if method=='POST':
                body=dict(response.body);body['workerEvidenceSha256']='0'*64;return Response(response.status,body)
            return response
        self.response_fault=receipt_corrupt;self.held();self.assertEqual(self.rows()[0]['phase'],'issued')
        self.response_fault=None;self.assertTrue(self.recover()['executable']);self.assertEqual(len(self.bodies),1)

    def test_old_server_404_never_triggers_claim_or_post_fallback(self):
        self.response_fault=lambda method,path,response:Response(404,{'error':'not_found'})
        self.held();self.assertEqual(self.bodies,[]);self.assertTrue(all(method=='GET' for method,_ in self.calls))

    def test_response_receipt_does_not_replace_fresh_mutation_guard(self):
        self.host.ledger.set_engine_state('parent-reservation','uncertain');self.held()
        self.assertEqual(self.rows()[0]['phase'],'receipted');self.assertEqual(self.host.queue.get(self.parent['jobId'])['status'],'running')
        with closing(sqlite3.connect(self.host.ledger.path)) as db:
            self.assertEqual(db.execute('SELECT state FROM reservations WHERE id=?',('parent-reservation',)).fetchone()[0],'uncertain')

    def test_durable_unknown_intent_corruption_and_pid_owner_drift_hold(self):
        self.fault='before_post';self.held();self.fault=None
        with closing(sqlite3.connect(self.client.path)) as db,db:db.execute("UPDATE worker_recovery_attempts SET evidence='{}'")
        self.held();self.assertEqual(len(self.bodies),1)
        with patch.object(self.client,'_pid',-1):self.held()

    def test_concurrent_clients_cannot_issue_different_attempts(self):
        second=self.open();entered=threading.Event();release=threading.Event();original=self.client._http;errors=[]
        def wait(*args,**kwargs):
            entered.set();release.wait(3);return original(*args,**kwargs)
        def run():
            try:self.client.recover(self.parent)
            except Exception as error:errors.append(error)
        with patch.object(self.client,'_http',side_effect=wait):
            thread=threading.Thread(target=run);thread.start();self.assertTrue(entered.wait(3))
            with self.assertRaises(m.WorkerRecoveryHeld):second.recover(self.parent)
            release.set();thread.join(5);self.assertFalse(thread.is_alive())
        self.assertEqual(errors,[]);self.assertEqual(len(self.bodies),1)

    def fresh(self,policy=None,*,native=False):
        base=LargeRecoveryFixture(self.cert,self.key,native=native);self.addCleanup(base.doCleanups)
        return base,m.OriginalClaimRecovery(base.host.root/'worker-recovery.sqlite',parent_guard=base.guard,policy=policy or m.RecoveryPolicy())

    def activate_fixture(self,base):
        h=base.host;p=h.policy
        api=ReferenceWorkerAPI(h.store,DeliveryReceiver(h.store),bearer_workers={fixtures.WORKER:dict(node_id='main',worker_id='main-worker',worker_instance_id='instance-one')},
            recovery_bindings={'main':RecoveryNodeBinding('main',p.engine_id,p.authority_id,p.coordinator_origin,1,p.output_budget)})
        base.api.handle=api.handle
        return api

    def restart_fixture(self,base):
        h=base.host;h.queue.close();h.queue=Queue(h.root/'queue',node_ids=('cluster','main'),treedar_node_ids=('main',),reference_enabled=True)
        base.stack.callback(h.queue.close);h.store=ReferenceStore(h.queue);return self.activate_fixture(base)

    def test_prepare_charges_additive_controls_once_before_any_stage(self):
        base,client=self.fresh();h=base.host
        self.assertFalse(client.path.exists());first=client.prepare(base.parent);second=client.prepare(base.parent)
        self.assertEqual(first,second);self.assertEqual(first['allowanceBytes'],client.policy.control_allowance)
        self.assertIsNotNone(first['publicationSha256'])
        with closing(sqlite3.connect(h.ledger.path)) as db:
            self.assertEqual(db.execute('SELECT allocated FROM reservations').fetchone()[0],base.parent['inputBytes']+client.policy.control_allowance)
        self.assertEqual([stage['outputBudget'] for stage in h.plan['stages']],[100,100])
        self.assertEqual(base.parent['outputBudget'],200+h.policy.recovery_control_budget)

    def test_typed_charge_committed_lost_ack_can_finish_without_counter_inference(self):
        base,client=self.fresh();original=base.host.ledger.prepare_control_charge
        def lose(*args,**kwargs):original(*args,**kwargs);raise OSError('lost control charge ACK')
        with patch.object(base.host.ledger,'prepare_control_charge',side_effect=lose),self.assertRaises(m.WorkerRecoveryHeld):client.prepare(base.parent)
        self.assertFalse(client.path.exists());receipt=base.host.ledger.read_control_charge(base.parent['reservationId']);self.assertIsNone(receipt['publicationSha256'])
        recovered=client.prepare(base.parent);self.assertEqual(recovered['allocatedAfter'],receipt['allocatedAfter'])
        self.assertEqual(recovered['receiptSha256'],receipt['receiptSha256'])

    def test_control_publication_ack_loss_uses_exact_existing_files_and_receipt(self):
        base,client=self.fresh();original=base.host.ledger.publish_control_charge
        def lose(*args,**kwargs):original(*args,**kwargs);raise OSError('lost publication ACK')
        with patch.object(base.host.ledger,'publish_control_charge',side_effect=lose),self.assertRaises(m.WorkerRecoveryHeld):client.prepare(base.parent)
        inode=client.path.stat().st_ino;receipt=base.host.ledger.read_control_charge(base.parent['reservationId'])
        reopened=m.OriginalClaimRecovery(client.path,parent_guard=base.guard)
        self.assertEqual(receipt,reopened.prepare(base.parent));self.assertEqual(reopened.path.stat().st_ino,inode)

    def test_legacy_issued_job_cannot_retrofit_recovery_control_charge(self):
        base,client=self.fresh();h=base.host
        h.journal.advance(h.claim['jobId'],'odm',SyntheticStage(h.journal,'odm',h.root),h.guard)
        with self.assertRaises(m.WorkerRecoveryHeld):client.prepare(base.parent)
        with self.assertRaises(m.WorkerRecoveryHeld):client.recover(base.parent)
        self.assertIsNone(h.ledger.read_control_charge(base.parent['reservationId']));self.assertFalse(client.path.exists())

    def test_published_missing_or_foreign_partial_journal_is_never_reset(self):
        base,client=self.fresh();client.prepare(base.parent);client.path.unlink()
        with self.assertRaises(m.WorkerRecoveryHeld):m.OriginalClaimRecovery(client.path,parent_guard=base.guard)
        self.assertFalse(client.path.exists())

    def test_foreign_empty_files_are_not_claimed_as_new_initialization(self):
        base,client=self.fresh();client.path.touch(mode=0o600);client.lock_path.touch(mode=0o600)
        with self.assertRaises(m.WorkerRecoveryHeld):client.prepare(base.parent)
        self.assertEqual(client.path.stat().st_size,0)

    def test_per_job_attempt_cap_holds_without_pruning_prior_receipts(self):
        base,client=self.fresh(m.RecoveryPolicy(per_job_attempts=2));h=base.host;client.prepare(base.parent)
        h.ledger.set_engine_state(base.parent['reservationId'],'engine_started');self.activate_fixture(base)
        for number in range(2):
            self.assertTrue(client.recover(base.parent)['executable'])
            h.queue.close();h.queue=Queue(h.root/'queue',node_ids=('cluster','main'),treedar_node_ids=('main',),reference_enabled=True);base.stack.callback(h.queue.close)
            h.store=ReferenceStore(h.queue);self.activate_fixture(base)
        with self.assertRaises(m.WorkerRecoveryHeld):client.recover(base.parent)
        with closing(sqlite3.connect(client.path)) as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM worker_recovery_attempts').fetchone()[0],2)
        with h.queue._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_recovery_receipts').fetchone()[0],2)

    def test_oversized_row_is_detected_by_aggregate_before_historical_fetch(self):
        self.fault='before_post';self.held();self.fault=None
        with closing(sqlite3.connect(self.client.path)) as db,db:db.execute('UPDATE worker_recovery_attempts SET evidence=?',('x'*(self.client.policy.max_row_bytes+1),))
        queries=[];connect=sqlite3.connect
        class Observed(sqlite3.Connection):
            def execute(connection,sql,*args):queries.append(sql);return super().execute(sql,*args)
        def observed(*args,**kwargs):kwargs['factory']=Observed;return connect(*args,**kwargs)
        with patch('sqlite3.connect',side_effect=observed):self.held()
        self.assertTrue(any('COALESCE(MAX(' in sql for sql in queries))
        self.assertFalse(any(sql.startswith('SELECT * FROM worker_recovery_attempts WHERE job') for sql in queries))

    def test_global_count_and_serialized_caps_and_page_file_cap(self):
        with closing(sqlite3.connect(':memory:')) as db:
            for sql in m.SCHEMA:db.execute(sql)
            db.execute('INSERT INTO worker_recovery_binding VALUES(1,?)',(self.client._binding,))
            for number in range(self.client.policy.global_attempts+1):
                db.execute('INSERT INTO worker_recovery_attempts VALUES(?,?,?,?,?,?,?,NULL,NULL)',(str(number),'job-'+str(number),'epoch',number,'{}','{}','issued'))
            with self.assertRaises(m.WorkerRecoveryHeld):self.client._census(db)
            db.execute('DELETE FROM worker_recovery_attempts')
            for number in range(20):db.execute('INSERT INTO worker_recovery_attempts VALUES(?,?,?,?,?,?,?,NULL,NULL)',(str(number),'job-'+str(number),'epoch',number,'x'*7000,'{}','issued'))
            with self.assertRaises(m.WorkerRecoveryHeld):self.client._census(db)
        with self.client.path.open('r+b') as stream:stream.truncate(self.client.policy.max_db_pages*4096+4096)
        self.held();self.assertEqual(self.bodies,[])

    def test_charge_ack_loss_then_coordinator_restart_finishes_preexecution_without_engine_started(self):
        base,client=self.fresh();h=base.host;original=h.ledger.prepare_control_charge;token=h.claim['claimToken']
        def lose(*args,**kwargs):original(*args,**kwargs);raise OSError('lost charge ACK')
        with patch.object(h.ledger,'prepare_control_charge',side_effect=lose),self.assertRaises(m.WorkerRecoveryHeld):client.prepare(base.parent)
        receipt=h.ledger.read_control_charge(base.parent['reservationId']);self.restart_fixture(base)
        with self.assertRaises(Exception):base.guard.check(base.parent,mutation=False)
        published=client.prepare(base.parent);self.assertEqual(published['receiptSha256'],receipt['receiptSha256'])
        self.assertTrue(client.recover(base.parent)['executable']);self.assertEqual(h.store.get(h.claim['jobId'],private=True)['claimToken'],token)
        with closing(sqlite3.connect(h.ledger.path)) as db:self.assertEqual(db.execute('SELECT state FROM reservations').fetchone()[0],'reserved')
        with closing(sqlite3.connect(h.journal.path)) as db:self.assertEqual([row[0] for row in db.execute('SELECT state FROM pipeline_stages')],['new','new'])

    def test_completed_preparation_before_first_stage_survives_coordinator_restart(self):
        base,client=self.fresh();client.prepare(base.parent);self.restart_fixture(base)
        with self.assertRaises(Exception):base.guard.check(base.parent,mutation=False)
        self.assertTrue(client.recover(base.parent)['executable']);self.assertIsNone(base.guard.check(base.parent,mutation=True))
        with closing(sqlite3.connect(base.host.ledger.path)) as db:self.assertEqual(db.execute('SELECT state FROM reservations').fetchone()[0],'reserved')

    def test_control_enabled_engine_guard_and_stage_issue_require_published_controls(self):
        base,client=self.fresh();h=base.host
        self.assertEqual(h.journal.bind_parent(h.claim,claim_request_id='pipeline-claim',reservation_id='parent-reservation',gate_receipt_id='a'*32,guard=base.guard),h.plan)
        with self.assertRaises(Exception):base.guard.check(base.parent,mutation=True)
        adapter=SyntheticStage(h.journal,'odm',h.root)
        with self.assertRaises(Exception):h.journal.advance(h.claim['jobId'],'odm',adapter,base.guard)
        with closing(sqlite3.connect(h.journal.path)) as db:self.assertEqual(db.execute("SELECT state FROM pipeline_stages WHERE stage='odm'").fetchone()[0],'new')
        client.prepare(base.parent);self.assertIsNone(base.guard.check(base.parent,mutation=True))

    def test_preexecution_scope_rejects_corrupt_input_and_stale_or_fabricated_lease(self):
        base,client=self.fresh();h=base.host
        with h.journal._operation() as lease:
            self.assertIsNone(base.guard.check_preexecution_recovery(base.parent,lease=lease))
        with self.assertRaises(Exception):base.guard.check_preexecution_recovery(base.parent,lease=lease)
        with h.journal._operation() as live:
            from processing.reference_pipeline import PipelineOperationLease
            with self.assertRaises(Exception):base.guard.check_preexecution_recovery(base.parent,lease=PipelineOperationLease(h.journal,live.descriptor))
        path=next((h.ledger.root/'jobs'/'parent-reservation').iterdir());path.write_bytes(b'XYZ')
        with self.assertRaises(m.WorkerRecoveryHeld):client.prepare(base.parent)
        self.assertIsNone(h.ledger.read_control_charge(base.parent['reservationId']))

    def test_unpublished_initialized_controls_finish_after_preexecution_restart(self):
        base,client=self.fresh();h=base.host
        with patch.object(h.ledger,'publish_control_charge',side_effect=OSError('before publish')),self.assertRaises(m.WorkerRecoveryHeld):client.prepare(base.parent)
        self.assertTrue(client.path.exists());inode=client.path.stat().st_ino
        reopened=m.OriginalClaimRecovery(client.path,parent_guard=base.guard);self.restart_fixture(base)
        published=reopened.prepare(base.parent);self.assertIsNotNone(published['publicationSha256']);self.assertEqual(reopened.path.stat().st_ino,inode)
        self.assertTrue(reopened.recover(base.parent)['executable'])

    def test_prior_scope_publication_cannot_be_replaced_by_unpublished_new_parent(self):
        h=self.host;fee=self.client.policy.control_allowance
        h.ledger.reserve(job_id='other-job',generation=1,reservation_id='other-res',manifest_sha256='a'*64,input_bytes=1,output_budget=fee+200,environment='staging',ttl_seconds=600,worker_ready=True)
        h.ledger.record_allocated('other-res',1)
        args=dict(charge_id='other-control',scope_sha256=self.client.scope_sha256,parent_binding_sha256='f'*64)
        h.ledger.prepare_control_charge('other-res',**args,expected_allocated=1,allowance_bytes=fee,maximum_scope_charges=64)
        changed=copy.deepcopy(json.loads(self.client._binding));changed['files'][str(self.client.path)][1]+=1
        h.ledger.publish_control_charge('other-res',**args,publication_descriptor=changed)
        self.held()
        with self.assertRaises(m.WorkerRecoveryHeld):self.open()

    def test_missing_published_metadata_blocks_guard_and_stage_before_intent(self):
        base,client=self.fresh(native=True);h=base.host;receipt=client.prepare(base.parent)
        self.assertEqual(receipt['publicationDescriptor'],json.loads(client._binding));client.path.unlink()
        with self.assertRaises(Exception):base.guard.check(base.parent,mutation=True)
        with self.assertRaises(Exception):h.journal.advance(h.claim['jobId'],'odm',h.native_stage,base.guard)
        with closing(sqlite3.connect(h.journal.path)) as db:self.assertEqual(db.execute("SELECT state FROM pipeline_stages WHERE stage='odm'").fetchone()[0],'new')
        counts=h.native_fixture.counts();self.assertEqual([counts[name] for name in ('initialize','upload','commit')],[0,0,0])
        self.assertFalse(client.path.exists())

    def test_missing_metadata_after_issued_intent_blocks_actual_native_start_without_replay(self):
        base,client=self.fresh(native=True);h=base.host;client.prepare(base.parent)
        class Capture:
            binding=h.journal.adapter_binding('odm')
            def start(capture,intent):capture.intent=intent;return None
            def lookup(capture,intent):return None
            def verify(capture,intent,proof):raise AssertionError('no proof')
        capture=Capture();h.journal.advance(h.claim['jobId'],'odm',capture,base.guard);client.path.unlink()
        with self.assertRaises(Exception):h.native_stage.start(capture.intent)
        counts=h.native_fixture.counts();self.assertEqual([counts[name] for name in ('initialize','upload','commit')],[0,0,0])
        with closing(sqlite3.connect(h.journal.path)) as db:self.assertEqual(db.execute("SELECT state FROM pipeline_stages WHERE stage='odm'").fetchone()[0],'issued')

    def test_published_validator_is_readonly_and_rejects_replacement_schema_and_caps(self):
        self.recover()
        paths=[self.client.path,self.client.lock_path,self.host.ledger.path,self.host.journal.path]
        before=[path.read_bytes() for path in paths];queries=[];connect=sqlite3.connect
        class Observed(sqlite3.Connection):
            def execute(connection,sql,*args):queries.append(sql);return super().execute(sql,*args)
        def observed(*args,**kwargs):kwargs['factory']=Observed;return connect(*args,**kwargs)
        original_open=os.open
        def readonly_open(path,flags,*args,**kwargs):
            self.assertFalse(flags&(os.O_WRONLY|os.O_RDWR|os.O_CREAT|os.O_TRUNC));return original_open(path,flags,*args,**kwargs)
        with patch('sqlite3.connect',side_effect=observed),patch('os.open',side_effect=readonly_open),patch('os.fsync',side_effect=AssertionError('no fsync')):
            self.assertIsNone(self.base.guard.check(self.parent,mutation=True))
        self.assertEqual(before,[path.read_bytes() for path in paths])
        self.assertFalse(any(sql.lstrip().split(' ',1)[0].upper() in ('CREATE','INSERT','UPDATE','DELETE','DROP','ALTER') for sql in queries))
        content=self.client.path.read_bytes();self.client.path.rename(self.client.path.with_suffix('.retained-original'))
        self.client.path.write_bytes(content);self.client.path.chmod(0o600)
        with self.assertRaises(Exception):self.base.guard.check(self.parent,mutation=True)

    def test_actual_published_descriptor_schema_corruption_holds_normal_engine_permission(self):
        with closing(sqlite3.connect(self.client.path)) as db,db:db.execute('CREATE TABLE foreign_data(value)')
        with self.assertRaises(Exception):self.base.guard.check(self.parent,mutation=True)

    def test_inplace_sqlite_page_size_change_rejected_by_admission_and_client(self):
        self.recover();inode=self.client.path.stat().st_ino
        with closing(sqlite3.connect(self.client.path)) as db:
            db.execute('PRAGMA page_size=8192');db.execute('VACUUM')
            self.assertEqual(db.execute('PRAGMA page_size').fetchone()[0],8192)
        self.assertEqual(self.client.path.stat().st_ino,inode)
        self.assertLess(self.client.path.stat().st_size,self.client.policy.max_db_pages*4096)
        with self.assertRaises(Exception):self.base.guard.check(self.parent,mutation=True)
        self.held()

    def test_valid_schema_with_corrupt_current_parent_history_cannot_authorize_engine(self):
        self.recover()
        with closing(sqlite3.connect(self.client.path)) as db,db:db.execute("UPDATE worker_recovery_attempts SET receipt='{}'")
        with self.assertRaises(Exception):self.base.guard.check(self.parent,mutation=True)
        self.held()

