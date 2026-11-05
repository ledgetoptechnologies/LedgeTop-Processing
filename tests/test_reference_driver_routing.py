"""Per-job driver routing with real durable components and synthetic transports.

The three-byte input and generated reports test orchestration only. They do not
exercise LiDAR parsing, geometry or a real TreeDar/ODM engine.
"""
import copy
from dataclasses import replace
import io
import json
from pathlib import Path
import ssl
import sys
import time
import unittest
from unittest.mock import patch
import uuid

from processing import reference_protocol as p
from processing.node_gate import NodeGate
from processing.prototype import Queue
from processing.reference_control import ReferenceControlAPI
from processing.reference_driver import ReferenceDriver, DriverHeld
from processing.reference_engine_worker import ReferenceEngineWorker
from processing.reference_execution import ExecutionRouter
from processing.reference_provenance import LidarProvenanceClient, PREFIX as PROVENANCE_PREFIX
from processing.reference_failure import FailureOutbox
from processing.reference_store import ReferenceStore
from processing.reference_transfer import BrokerClient, OBJECT_PREFIX
from processing.treedar_delivery import TreeDarDelivery
from processing.treedar_engine import TreeDarEngine, REQUIRED_OUTPUTS
from processing.viewer_service import NativeEngine
from processing.worker_outbox import WorkerOutbox
from tests.test_reference_driver import EngineTransport, TOKEN, VIEWER
from tests import test_treedar_engine as engine_fixture


@unittest.skipUnless(sys.platform.startswith('linux'), 'Linux physical engine fixture required')
class ReferenceDriverRoutingTests(unittest.TestCase):
    def setUp(self):
        self.host = engine_fixture.TreeDarEngineTests(methodName='runTest')
        self.host.setUp()
        self.addCleanup(self.host.doCleanups)
        self.host.now = time.time()
        self.root, self.ledger = self.host.root, self.host.ledger
        self.queue = Queue(self.root/'queue', reference_enabled=True, node_ids=('cluster','main'), treedar_node_ids=('main',))
        self.addCleanup(self.queue.close)
        self.store = ReferenceStore(self.queue, local_max_input_bytes=1)
        self.fixture = json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())
        self.api = ReferenceControlAPI(self.store, viewer_bearers={VIEWER:'staging'},
            worker_bearers={TOKEN:dict(node_id='main',worker_id='worker-main',worker_instance_id='instance-main')},
            capability_fingerprint=self.fixture['submission']['capabilityFingerprint'],
            options_fingerprint=self.fixture['submission']['pipeline']['odm']['optionsFingerprint'])
        with NodeGate(self.host.config.engine_id) as gate:
            gate.handover('main',self.host.config.coordinator_origin,self.queue.authority_id,1,
                old_worker_stopped=True,note='isolated routing test')
        self.config = replace(self.host.config, authority_id=self.queue.authority_id, gate_generation=2)
        self.outbox = WorkerOutbox(self.root/'private'/'outbox.sqlite', clock=lambda:self.host.now)
        self.failures = FailureOutbox(self.root/'private'/'failures.sqlite', clock=lambda:self.host.now)
        self.native = NativeEngine('http://127.0.0.1:3001')
        self.transport = EngineTransport()
        for method in ('initialize','upload','action','read','download'):
            mocked = patch.object(self.native, method, side_effect=getattr(self.transport,method))
            mocked.start(); self.addCleanup(mocked.stop)
        self.odm = ReferenceEngineWorker(self.root/'private'/'odm.sqlite',ledger=self.ledger,outbox=self.outbox,
            engine=self.native,engine_id=self.config.engine_id,node_id='main',authority_id=self.queue.authority_id,
            coordinator_origin=self.config.coordinator_origin,gate_generation=2,worker_id='worker-main',engine_version=self.transport.version,
            engine_probe=lambda deadline:self.config.engine_id)
        self.context = ssl.create_default_context()
        self.broker = BrokerClient('https://broker.invalid',ssl_context=self.context,clock=lambda:self.host.now,
            opener=self.broker_open)
        self.docker = engine_fixture.FakeDocker(self.config)
        self.provenance = LidarProvenanceClient(self.broker,state_path=self.root/'private'/'provenance.sqlite',clock=lambda:self.host.now)
        self.open_tree()
        self.router = ExecutionRouter(self.root/'private'/'router.sqlite',self.odm,self.tree,failure_outbox=self.failures)
        self.controls, self.broker_calls, self.receipts = [], [], {}
        self.provenance_denied = False
        self.driver = self.open_driver()

    def open_tree(self):
        verifier = self.provenance.verify if self.provenance is not None else lambda receipt:False
        self.controller = TreeDarEngine(self.root/'private'/'tree.sqlite',ledger=self.ledger,config=self.config,
            verify_provenance=verifier,runner=self.docker,clock=lambda:self.host.now)
        self.tree = TreeDarDelivery(self.root/'private'/'tree-delivery.sqlite',controller=self.controller,
            outbox=self.outbox,failure_outbox=self.failures)

    def open_driver(self, *, router=True, path=None, provenance=True):
        driver = ReferenceDriver(self.odm,self.ledger,self.outbox,self.broker,'https://coordinator.invalid',TOKEN,
            state_path=path or self.root/'private'/'driver.sqlite',ssl_context=self.context,output_budget=1024**2,
            transfer_ttl=600,safety_floor=0,failure_outbox=self.failures,worker_instance_id='instance-main',
            execution_router=self.router if router else None,provenance_client=self.provenance if router and provenance else None,
            clock=lambda:self.host.now)
        driver._request = self.control
        driver.sender._open = self.sender_open
        return driver

    @staticmethod
    def response(request, payload, content_type='application/json'):
        raw = payload if isinstance(payload,bytes) else p.canonical_json(payload)
        class Reply(io.BytesIO):
            status = 200
            headers = {'Content-Type':content_type,'Content-Length':str(len(raw))}
            def geturl(self): return request.full_url
        return Reply(raw)

    def broker_open(self, request, timeout):
        self.broker_calls.append(request.full_url)
        if request.full_url.startswith(self.broker.origin+OBJECT_PREFIX):
            return self.response(request,b'abc','application/octet-stream')
        if request.full_url.startswith(self.broker.origin+PROVENANCE_PREFIX):
            if self.provenance_denied: raise OSError('synthetic issuer unavailable')
            headers={key.lower():value for key,value in request.header_items()}
            job=self.store.get(headers['x-ltds-job-id'],private=True)
            obj=job['submission']['manifest']['objects'][0]
            receipt=dict(schemaVersion=1,receiptId=job['submission']['pipeline']['treedar']['provenanceReceiptId'],
                jobId=job['jobId'],generation=1,environment='staging',objectId=obj['id'],objectSha256=obj['sha256'],
                byteSize=obj['byteSize'],sourceKind='lidar',verticalUnits='metre',verticalDatum='ellipsoidal')
            self.receipts[job['jobId']]=receipt
            return self.response(request,dict(schemaVersion=1,inputManifestSha256=job['manifestSha256'],
                pipelineDigest=job['pipelineDigest'],provenance=receipt))
        raise AssertionError('unexpected broker metadata route')

    def control(self, method, suffix, payload=None, *, claim_token=None):
        self.controls.append((method,suffix))
        headers = {'Authorization':'Bearer '+TOKEN}
        if claim_token is not None: headers['X-Claim-Token']=claim_token
        response = self.api.handle(method,'/ltds/worker/v1'+suffix,headers,
            b'' if payload is None else p.canonical_json(payload),protected_transport=True)
        if response.status == 409: return 409,None
        if response.status != 200: raise DriverHeld('test_control_rejected')
        return 200,response.body

    def sender_open(self, request, timeout):
        response = self.api.handle('POST','/ltds/worker/v1/results',{'Authorization':'Bearer '+TOKEN},
            request.data,protected_transport=True)
        if response.status != 200: raise ValueError('test_result_rejected')
        return self.response(request,response.body)

    def submit(self, kind='odm'):
        raw = copy.deepcopy(self.fixture['submission'])
        raw['submissionId'] = str(uuid.uuid4())
        if kind in ('treedar','combined'):
            obj=raw['manifest']['objects'][0]
            obj.update(kind='lidar',name='transport-fixture.las',mediaType='application/vnd.las')
            raw['manifest']['inputCounts'].update(images=0,lidarObjects=1)
            raw['manifest']['sha256']=p.manifest_sha256(raw['manifest'])
            raw['pipeline']['odm']['requested']=kind=='combined'
            raw['pipeline']['treedar'].update(requested=True,inputObjectIds=[obj['id']],provenanceReceiptId='proof-'+uuid.uuid4().hex)
        return self.store.submit(p.validate_submission(raw,environment='staging',
            capability_fingerprint=raw['capabilityFingerprint'],available_engines=('odm','treedar')))[0]

    def supply_grants(self):
        for job in self.store.list(private=True):
            if job['phase']=='awaiting_transfer' and self.store.private_transfer_grant(job['jobId']) is None:
                self.store.receive_transfer_grant(job['jobId'],dict(job['transferRequest'],jobId=job['jobId'],
                    environment=job['environment'],objectIds=[obj['id'] for obj in job['submission']['manifest']['objects']],
                    token='synthetic-transfer-token'),clock=lambda:self.host.now)

    def advance(self, job, target):
        for _ in range(50):
            self.supply_grants()
            state=self.driver.step()
            with self.driver._db() as db:
                row=db.execute('SELECT stage FROM driver_jobs WHERE job=?',(job['jobId'],)).fetchone()
            if row and row[0]==target: return state
            self.assertNotEqual(state['state'],'held',state)
        self.fail('target stage unavailable: '+target)

    def acceptance(self, job):
        result=json.loads(self.router._row(job['jobId'])['result'])
        return dict(schemaVersion=1,receiptId='accepted-'+uuid.uuid4().hex,storageReceiptId='storage-'+uuid.uuid4().hex,
            jobId=job['jobId'],generation=1,environment='staging',inputManifestSha256=job['manifestSha256'],
            pipelineDigest=job['pipelineDigest'],resultManifestSha256=result['resultManifestSha256'],
            acceptedObjectIds=[obj['id'] for obj in result['objects']],acceptedAt='2026-10-28T12:00:00Z',validationVersion='ltds-reference-v1')

    def tree_proof(self, job):
        # The synthetic engine transport supplies a terminal output proof; the
        # production driver never enables synthetic LiDAR or runs this helper.
        self.host.engine,self.host.job,self.host.docker=self.controller,self.store.get(job['jobId'],private=True),self.docker
        self.host.obj=self.host.job['submission']['manifest']['objects'][0]
        output=self.host.make_proof()
        proof=json.loads((output/'proof.json').read_bytes())
        proof['admission']={key:self.receipts[job['jobId']][key] for key in
            ('jobId','generation','environment','objectId','objectSha256','byteSize','sourceKind','verticalUnits','verticalDatum')}
        (output/'proof.json').write_bytes(p.canonical_json(proof))

    def test_alternating_tree_and_odm_preserve_per_job_background_acceptance(self):
        tree=self.submit('treedar')
        self.advance(tree,'executing')
        self.assertEqual(sum(PROVENANCE_PREFIX in url for url in self.broker_calls),2)
        self.tree_proof(tree)
        self.advance(tree,'awaiting_acceptance')
        self.assertIsNotNone(self.controller._row(tree['jobId'])['proof'])
        with NodeGate(self.config.engine_id) as gate:
            self.assertIsNone(gate.snapshot()['reservation'])
        self.driver=self.open_driver()
        photo=self.submit('odm')
        self.advance(photo,'executing')
        tree_receipt=self.acceptance(tree)
        self.store.accept(tree['jobId'],tree_receipt)
        self.driver.step()  # Accept old TreeDar results while ODM owns the gate.
        self.advance(photo,'awaiting_acceptance')
        photo_receipt=self.acceptance(photo)
        self.store.accept(photo['jobId'],photo_receipt)
        self.assertEqual(self.driver.step()['state'],'completed')
        self.assertEqual(self.router.kind_for(tree['jobId']),'treedar')
        self.assertEqual(self.router.kind_for(photo['jobId']),'odm')
        self.assertIs(self.driver.worker,self.odm)
        self.assertEqual(sum(call[0]=='create' for call in self.docker.calls),1)
        self.assertEqual(sum(call[0]=='initialize' for call in self.transport.calls),1)
        for job in (tree,photo):
            self.assertTrue(self.store.acceptance_receipts(job['jobId'])['fullyAccepted'])
            self.assertEqual(self.router._row(job['jobId'])['phase'],'delivered')
            with self.driver._db() as db:
                self.assertEqual(db.execute('SELECT stage FROM driver_jobs WHERE job=?',(job['jobId'],)).fetchone()[0],'completed')

    def test_tree_lost_start_response_and_missing_projection_never_relaunch(self):
        job=self.submit('treedar')
        self.advance(job,'execution_pending')
        self.docker.fail='start'
        self.assertEqual(self.driver.step()['state'],'held')
        self.assertEqual(self.controller._row(job['jobId'])['phase'],'start_pending')
        with self.tree._db() as db:
            self.assertIsNone(db.execute('SELECT 1 FROM executions WHERE job=?',(job['jobId'],)).fetchone())
        self.docker.fail=None
        self.driver=self.open_driver()
        self.assertEqual(self.driver.step()['state'],'executing')
        self.assertEqual(self.driver.step()['state'],'executing')
        self.assertEqual(sum(call[0]=='create' for call in self.docker.calls),1)
        self.assertEqual(sum(call[0]=='start' for call in self.docker.calls),1)
        self.tree_proof(job)
        self.advance(job,'awaiting_acceptance')
        self.assertFalse(any(call[0]=='initialize' for call in self.transport.calls))

    def test_lost_success_and_failure_ack_checkpoint_reuses_exact_tree_tombstone(self):
        for failed in (False,True):
            with self.subTest(failed=failed):
                job=self.submit('treedar')
                self.advance(job,'executing')
                if failed:
                    self.docker.state,self.docker.exit_code='exited',1
                else:
                    self.tree_proof(job)
                self.advance(job,'failure_delivering' if failed else 'delivering')
                original=self.tree.acknowledge_delivery
                def lost(job_id):
                    original(job_id)
                    raise OSError('synthetic crash after durable ACK')
                with patch.object(self.tree,'acknowledge_delivery',side_effect=lost):
                    self.assertEqual(self.driver.step()['state'],'retry_wait')
                self.assertEqual(self.tree._row(job['jobId'])['phase'],'delivered')
                with NodeGate(self.config.engine_id) as gate:
                    self.assertIsNone(gate.snapshot()['reservation'])
                self.open_tree()
                self.router=ExecutionRouter(self.root/'private'/'router.sqlite',self.odm,self.tree,failure_outbox=self.failures)
                self.driver=self.open_driver()
                expected='awaiting_failure_acceptance' if failed else 'awaiting_acceptance'
                self.assertEqual(self.driver.step()['state'],expected)
        self.assertEqual(sum(call[0]=='create' for call in self.docker.calls),2)
        self.assertEqual(sum(call[0]=='start' for call in self.docker.calls),2)

    def test_advice_identity_is_checked_before_claim_and_pipeline_after_claim(self):
        job=self.submit('treedar')
        original=self.driver._request
        def altered(method,suffix,*args,**kwargs):
            status,body=original(method,suffix,*args,**kwargs)
            if suffix=='/next':
                body=copy.deepcopy(body)
                body['execution']['generation']+=1
            return status,body
        self.driver._request=altered
        self.assertEqual(self.driver.step(),dict(state='held',reason='execution_advice_binding_changed'))
        self.assertNotIn(('POST','/claim'),self.controls)
        def wrong_kind(method,suffix,*args,**kwargs):
            status,body=original(method,suffix,*args,**kwargs)
            if suffix=='/next':
                body=copy.deepcopy(body);body['execution']['kind']='odm'
            return status,body
        self.driver._request=wrong_kind
        self.assertEqual(self.driver.step()['state'],'claim_pending')
        self.assertEqual(self.driver.step(),dict(state='held',reason='execution_advice_pipeline_changed'))
        with self.router._db() as db:
            self.assertIsNone(db.execute('SELECT 1 FROM execution_routes WHERE job=?',(job['jobId'],)).fetchone())
        self.assertEqual(self.broker_calls,[])
        self.assertFalse(any(call[0]=='create' for call in self.docker.calls))

    def test_provenance_is_required_before_transfer_and_rechecked_before_launch(self):
        job=self.submit('treedar')
        self.advance(job,'awaiting_grant')
        self.supply_grants()
        self.provenance_denied=True
        self.assertEqual(self.driver.step(),dict(state='held',reason='lidar_provenance_unavailable'))
        self.assertFalse(any(OBJECT_PREFIX in url for url in self.broker_calls))
        self.provenance_denied=False
        self.advance(job,'execution_pending')
        self.provenance_denied=True
        self.driver=self.open_driver()
        self.assertEqual(self.driver.step(),dict(state='held',reason='lidar_provenance_unavailable'))
        self.assertFalse(self.provenance.verify(self.receipts[job['jobId']]))
        self.assertFalse(any(call[0]=='create' for call in self.docker.calls))
        self.assertFalse(any(call[0]=='initialize' for call in self.transport.calls))

    def test_expired_tree_grant_never_refreshes_or_launches(self):
        job=self.submit('treedar')
        self.advance(job,'execution_pending')
        self.host.now+=601
        before=list(self.broker_calls)
        self.assertEqual(self.driver.step(),dict(state='held',reason='lidar_transfer_grant_unavailable'))
        self.assertEqual(self.broker_calls,before)
        self.assertFalse(any(call[0]=='create' for call in self.docker.calls))

    def test_tree_failure_metadata_ack_and_terminal_viewer_acceptance_release_no_files(self):
        job=self.submit('treedar')
        self.advance(job,'executing')
        self.docker.state,self.docker.exit_code='exited',1
        self.advance(job,'awaiting_failure_acceptance')
        local=self.tree._row(job['jobId'])
        self.assertEqual(local['phase'],'delivered')
        self.assertIsNone(local['result'])
        self.assertEqual(self.queue.get(job['jobId'])['status'],'failed')
        with NodeGate(self.config.engine_id) as gate:
            self.assertIsNone(gate.snapshot()['reservation'])
        with self.driver._db() as db:
            data=json.loads(db.execute('SELECT data FROM driver_jobs WHERE job=?',(job['jobId'],)).fetchone()[0])
        relative=next(iter(data['inputPaths'].values()))
        self.assertEqual((self.ledger.root/relative).read_bytes(),b'abc')
        failure=data['failure']
        receipt={key:failure[key] for key in ('jobId','generation','environment','inputManifestSha256','pipelineDigest','failureSha256')}
        receipt.update(schemaVersion=1,receiptId='terminal-ack',storageReceiptId='terminal-storage',
            acceptedAt='2026-10-28T12:00:00Z',validationVersion='ltds-reference-v1')
        self.api.worker.terminal.accept(job['jobId'],receipt,'staging')
        self.driver=self.open_driver()
        self.assertEqual(self.driver.step()['state'],'completed')
        self.assertEqual((self.ledger.root/relative).read_bytes(),b'abc')
        self.assertFalse(any(call[0]=='rm' for call in self.docker.calls))

    def test_routed_odm_uses_original_worker_and_retains_receipts_after_restart(self):
        job=self.submit()
        self.advance(job,'awaiting_acceptance')
        self.assertIs(self.driver.worker,self.odm)
        self.assertEqual(self.router.kind_for(job['jobId']),'odm')
        self.assertFalse(any(call[0] in ('create','start') for call in self.docker.calls))
        self.driver=self.open_driver()
        receipt=self.acceptance(job)
        self.store.accept(job['jobId'],receipt)
        self.assertEqual(self.driver.step()['state'],'completed')
        self.assertEqual(self.store.acceptance_receipts(job['jobId'])['receipts'],[receipt])

    def test_combined_and_unconfigured_tree_refused_before_claim(self):
        self.driver=self.open_driver(path=self.root/'private'/'unconfigured.sqlite',provenance=False)
        for kind in ('combined','treedar'):
            with self.subTest(kind=kind):
                job=self.submit(kind)
                self.controls.clear()
                self.assertEqual(self.driver.step()['state'],'held')
                self.assertNotIn(('POST','/claim'),self.controls)
                self.assertEqual(self.queue.get(job['jobId'])['status'],'queued')
                with self.queue._db() as db:
                    db.execute("UPDATE jobs SET status='cancelled' WHERE id=?",(job['jobId'],))
        self.assertEqual(self.broker_calls,[])
        self.assertFalse(any(call[0] in ('create','start') for call in self.docker.calls))

    def test_routing_mode_and_path_are_immutable_and_legacy_claims_not_adopted(self):
        with self.assertRaises(ValueError): self.open_driver(router=False)
        self.router=ExecutionRouter(self.root/'private'/'different-router.sqlite',self.odm,self.tree,failure_outbox=self.failures)
        with self.assertRaises(ValueError): self.open_driver()
        path=self.root/'private'/'legacy.sqlite'
        legacy=self.open_driver(router=False,path=path)
        job=self.submit()
        self.assertEqual(legacy.step()['state'],'claim_pending')
        with self.assertRaises(ValueError): self.open_driver(path=path)
        self.assertEqual(self.queue.get(job['jobId'])['status'],'queued')

