"""Actual daemon factory/driver migration with synthetic engine transports only.

Three-byte input and generated files prove orchestration, never LiDAR geometry.
"""
import copy
import hashlib
import io
import json
from pathlib import Path
import sys
import unittest
import uuid

from processing import reference_protocol as p
from processing.node_gate import NodeGate
from processing.prototype import Queue
from processing.reference_control import ReferenceControlAPI
from processing.reference_driver import DriverHeld
from processing.reference_results import ResultAuthorization
from processing.reference_store import ReferenceStore
from processing.reference_transfer import OBJECT_PREFIX
from processing.reference_provenance import PREFIX as PROVENANCE_PREFIX
from processing.treedar_engine import REQUIRED_OUTPUTS
from tests import test_reference_worker_daemon_routing as factory_fixture
from tests import test_treedar_engine as engine_fixture


@unittest.skipUnless(sys.platform.startswith('linux'),'Linux factory ownership required')
class TreeDarFactoryMigrationTests(unittest.TestCase):
    def setUp(self):
        self.host=factory_fixture.WorkerDaemonRoutingTests(methodName='runTest')
        self.host.setUp();self.addCleanup(self.host.doCleanups)
        self.queue=Queue(self.host.root/'coordinator',reference_enabled=True,node_ids=('cluster','main'),treedar_node_ids=('main',))
        self.addCleanup(self.queue.close)
        self.store=ReferenceStore(self.queue,local_max_input_bytes=1)
        self.raw=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())['submission']
        self.host.config['authorityId']=self.queue.authority_id
        self.host.route()
        config=self.host.config
        self.api=ReferenceControlAPI(self.store,viewer_bearers={'synthetic-viewer-credential':'staging'},
            worker_bearers={config['workerBearer']:dict(node_id='main',worker_id=config['workerId'],worker_instance_id=config['workerInstanceId'])},
            capability_fingerprint=self.raw['capabilityFingerprint'],options_fingerprint=self.raw['pipeline']['odm']['optionsFingerprint'])
        self.driver,self.server=self.open(provision=True)

    @staticmethod
    def reply(request,body,kind='application/json'):
        raw=body if isinstance(body,bytes) else p.canonical_json(body)
        class Reply(io.BytesIO):
            status=200
            headers={'Content-Type':kind,'Content-Length':str(len(raw))}
            def geturl(self):return request.full_url
        return Reply(raw)

    def broker(self,request,timeout):
        if request.full_url.startswith(self.driver.broker.origin+OBJECT_PREFIX):
            return self.reply(request,b'abc','application/octet-stream')
        self.assertTrue(request.full_url.startswith(self.driver.broker.origin+PROVENANCE_PREFIX))
        headers={key.lower():value for key,value in request.header_items()}
        job=self.store.get(headers['x-ltds-job-id'],private=True);obj=job['submission']['manifest']['objects'][0]
        receipt=dict(schemaVersion=1,receiptId=job['submission']['pipeline']['treedar']['provenanceReceiptId'],
            jobId=job['jobId'],generation=1,environment='staging',objectId=obj['id'],objectSha256=obj['sha256'],byteSize=3,
            sourceKind='lidar',verticalUnits='metre',verticalDatum='ellipsoidal')
        return self.reply(request,dict(schemaVersion=1,inputManifestSha256=job['manifestSha256'],pipelineDigest=job['pipelineDigest'],provenance=receipt))

    def control(self,method,suffix,payload=None,*,claim_token=None):
        headers={'Authorization':'Bearer '+self.host.config['workerBearer']}
        if claim_token is not None:headers['X-Claim-Token']=claim_token
        response=self.api.handle(method,'/ltds/worker/v1'+suffix,headers,
            b'' if payload is None else p.canonical_json(payload),protected_transport=True)
        if response.status==409:return 409,None
        if response.status!=200:raise DriverHeld('fixture_control_rejected')
        return 200,response.body

    def sender(self,request,timeout):
        response=self.api.handle('POST','/ltds/worker/v1/results',{'Authorization':'Bearer '+self.host.config['workerBearer']},
            request.data,protected_transport=True)
        self.assertEqual(response.status,200)
        return self.reply(request,response.body)

    def open(self,*,provision=False,runner=None):
        driver,server=self.host.build(provision=provision)
        controller=driver.execution_router.treedar_worker.controller
        controller.runner=runner or engine_fixture.FakeDocker(controller.config)
        controller.runner.id=hashlib.sha256(str(controller.path).encode()).hexdigest()
        driver._request=self.control;driver.sender._open=self.sender;driver.broker._open=self.broker
        return driver,server

    def submit(self,kind='treedar'):
        raw=copy.deepcopy(self.raw);raw['submissionId']=str(uuid.uuid4())
        if kind=='treedar':
            obj=raw['manifest']['objects'][0]
            obj.update(kind='lidar',name='transport-only.las',mediaType='application/vnd.las')
            raw['manifest']['inputCounts'].update(images=0,lidarObjects=1)
            raw['manifest']['sha256']=p.manifest_sha256(raw['manifest'])
            raw['pipeline']['odm']['requested']=False
            raw['pipeline']['treedar'].update(requested=True,inputObjectIds=[obj['id']],provenanceReceiptId='proof-'+uuid.uuid4().hex)
        return self.store.submit(p.validate_submission(raw,environment='staging',
            capability_fingerprint=raw['capabilityFingerprint'],available_engines=('odm','treedar')))[0]

    def advance(self,job,target):
        for _ in range(60):
            current=self.store.get(job['jobId'],private=True)
            if current['phase']=='awaiting_transfer' and self.store.private_transfer_grant(job['jobId']) is None:
                self.store.receive_transfer_grant(job['jobId'],dict(current['transferRequest'],jobId=job['jobId'],
                    environment='staging',objectIds=[obj['id'] for obj in current['submission']['manifest']['objects']],token='synthetic-grant'))
            state=self.driver.step()
            with self.driver._db() as db:row=db.execute('SELECT stage FROM driver_jobs WHERE job=?',(job['jobId'],)).fetchone()
            if row and row[0]==target:return state
            self.assertNotEqual(state['state'],'held',state)
        self.fail('driver did not reach '+target)

    def proof(self,job):
        controller=self.driver.execution_router.treedar_worker.controller
        identity=json.loads(controller._row(job['jobId'])['identity'])
        output=self.driver.ledger.root/identity['outputRelativePath'];run=output/'run';run.mkdir()
        objects=[]
        for name in sorted(REQUIRED_OUTPUTS):
            data=b'synthetic orchestration output';(run/name).write_bytes(data)
            relative='run/'+name
            objects.append(dict(id='treedar-'+hashlib.sha256(relative.encode()).hexdigest()[:32],role='report',
                byteSize=len(data),sha256=hashlib.sha256(data).hexdigest(),relativePath=relative))
        receipt=identity['provenance']
        admission={key:receipt[key] for key in ('jobId','generation','environment','objectId','objectSha256','byteSize','sourceKind','verticalUnits','verticalDatum')}
        (output/'proof.json').write_bytes(p.canonical_json(dict(schemaVersion=1,job=identity['job'],
            engineVersion=controller.config.engine_version,admission=admission,objects=objects)))
        controller.runner.state='exited'

    def migrate(self):
        self.server.server_close()
        self.host.config['executionRouting']['treedar'].update(imageId='sha256:'+'c'*64,engineVersion='0.0.2')
        self.driver,self.server=self.open()

    def test_old_results_and_receipts_work_after_upgrade_and_new_engines_keep_their_pins(self):
        old=self.submit();self.advance(old,'executing');self.proof(old);self.advance(old,'awaiting_acceptance')
        old_controller=self.driver.execution_router.treedar_worker.controller
        old_runtime=self.driver.execution_router.describe(old['jobId'])['runtimeBinding']
        self.migrate()
        self.assertNotEqual(old_controller.path,self.driver.execution_router.treedar_worker.controller.path)
        results=self.host.listeners[-1].results
        metadata=results.metadata(old['jobId']);obj=metadata['objects'][0]
        authorization=ResultAuthorization(node_id='main',worker_id=self.host.config['workerId'],job_id=old['jobId'],
            generation=1,environment='staging',result_manifest_sha256=metadata['resultManifestSha256'],object_ids=(obj['id'],),share_approved=True)
        with results.open(old['jobId'],generation=1,object_id=obj['id'],result_manifest_sha256=metadata['resultManifestSha256'],authorization=authorization) as stream:
            self.assertEqual(b''.join(stream.chunks()),b'synthetic orchestration output')
        receipt=dict(schemaVersion=1,receiptId='old-storage-proof',storageReceiptId='old-storage-receipt',jobId=old['jobId'],generation=1,
            environment='staging',inputManifestSha256=old['manifestSha256'],pipelineDigest=old['pipelineDigest'],
            resultManifestSha256=metadata['resultManifestSha256'],acceptedObjectIds=[item['id'] for item in metadata['objects']],
            acceptedAt='2026-10-28T12:00:00Z',validationVersion='ltds-reference-v1')
        self.store.accept(old['jobId'],receipt);self.driver.step()
        self.assertTrue(self.store.acceptance_receipts(old['jobId'])['fullyAccepted'])
        self.assertEqual(self.driver.execution_router.describe(old['jobId'])['runtimeBinding'],old_runtime)
        with self.assertRaises(ValueError):self.driver.execution_router.worker_for(old['jobId'],for_execution=True)
        new=self.submit();self.advance(new,'executing');self.proof(new);self.advance(new,'awaiting_acceptance')
        self.assertEqual(json.loads(self.driver.execution_router._row(new['jobId'])['result'])['engineVersions'],{'treedar':'0.0.2'})
        photo=self.submit('odm');self.advance(photo,'awaiting_acceptance')
        self.assertEqual(self.driver.execution_router.kind_for(photo['jobId']),'odm')
        self.assertEqual(old_controller.config.engine_version,'0.0.1')

    def test_same_runtime_factory_restart_resumes_uncertain_start_without_relaunch(self):
        job=self.submit();self.advance(job,'execution_pending')
        controller=self.driver.execution_router.treedar_worker.controller;runner=controller.runner
        runner.fail='start';self.assertEqual(self.driver.step()['state'],'held');runner.fail=None
        self.assertEqual(controller._row(job['jobId'])['phase'],'start_pending')
        self.server.server_close()
        self.driver,self.server=self.open(runner=runner)
        self.assertEqual(self.driver.step()['state'],'executing')
        self.assertEqual(self.driver.step()['state'],'executing')
        self.assertEqual(sum(call[0]=='create' for call in runner.calls),1)
        self.assertEqual(sum(call[0]=='start' for call in runner.calls),1)
        self.proof(job);self.advance(job,'awaiting_acceptance')

    def test_changed_runtime_rejects_pending_gate_without_publishing_pointer(self):
        job=self.submit();self.advance(job,'executing')
        manager=self.driver.execution_router.treedar_worker.runtime_manager
        before=manager.snapshot();self.server.server_close()
        self.host.config['executionRouting']['treedar']['imageId']='sha256:'+'c'*64
        with self.assertRaises(ValueError):self.open()
        self.assertEqual(manager.snapshot(),before)
        with NodeGate(self.host.config['engineId']) as gate:self.assertIsNotNone(gate.snapshot()['reservation'])

    def test_pending_unknown_driver_stage_and_orphan_outbox_refuse_idle_publication(self):
        job=self.submit();self.advance(job,'executing');self.proof(job);self.advance(job,'awaiting_acceptance')
        manager=self.driver.execution_router.treedar_worker.runtime_manager
        before=manager.snapshot();self.server.server_close()
        self.host.config['executionRouting']['treedar']['imageId']='sha256:'+'c'*64
        for stage in ('delivering','unknown-stage'):
            with self.subTest(stage=stage):
                with self.driver._db() as db:db.execute('UPDATE driver_jobs SET stage=? WHERE job=?',(stage,job['jobId']))
                with self.assertRaises(ValueError):self.open()
                self.assertEqual(manager.snapshot(),before)
        with self.driver._db() as db:db.execute("UPDATE driver_jobs SET stage='awaiting_acceptance' WHERE job=?",(job['jobId'],))
        with self.driver.outbox._transaction() as db:
            row=db.execute('SELECT * FROM completions LIMIT 1').fetchone()
            values=dict(row);values.update(id='e'*64,job=str(uuid.uuid4()))
            columns=list(values)
            db.execute('INSERT INTO completions('+','.join(columns)+') VALUES('+','.join('?' for _ in columns)+')',list(values.values()))
        with self.assertRaises(ValueError):self.open()
        self.assertEqual(manager.snapshot(),before)

    def test_orphan_reserved_scratch_refuses_idle_publication(self):
        job=self.submit();self.advance(job,'executing');self.proof(job);self.advance(job,'awaiting_acceptance')
        manager=self.driver.execution_router.treedar_worker.runtime_manager
        before=manager.snapshot();self.server.server_close()
        self.driver.ledger.reserve(job_id=str(uuid.uuid4()),generation=1,reservation_id='orphan-reservation',
            manifest_sha256=job['manifestSha256'],input_bytes=3,output_budget=1024,environment='staging',ttl_seconds=600,worker_ready=True)
        self.host.config['executionRouting']['treedar']['imageId']='sha256:'+'c'*64
        with self.assertRaises(ValueError):self.open()
        self.assertEqual(manager.snapshot(),before)

    def test_stale_router_cannot_bind_new_job_after_runtime_pointer_changes(self):
        job=self.submit();self.advance(job,'executing');self.proof(job);self.advance(job,'awaiting_acceptance')
        stale=self.driver.execution_router
        self.migrate()
        pending=self.submit()
        claim=self.store.claim(pending['jobId'],node_id='main',worker_id=self.host.config['workerId'],
            worker_instance_id=self.host.config['workerInstanceId'],claim_request_id='new-after-upgrade')
        with self.assertRaises(ValueError):stale.bind(claim)
        with self.assertRaises(ValueError):self.driver.execution_router.describe(pending['jobId'])

