import copy
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
import uuid

from processing import reference_protocol as p
from processing.reference_execution import ExecutionRouter, RouteConflict
from processing.reference_engine_worker import ReferenceEngineWorker
from processing.reference_results import ReferenceResults, ResultAuthorization
from processing.reference_store import ReferenceStore
from processing.reference_delivery import DeliveryReceiver
from processing.worker_outbox import WorkerOutbox
from processing.reference_failure import FailureOutbox
from processing.viewer_service import NativeEngine
from processing.node_gate import NodeGate
from processing.treedar_delivery import TreeDarDelivery
from tests import test_treedar_delivery as delivery_fixture
from tests import test_reference_driver as driver_fixture


@unittest.skipUnless(sys.platform.startswith('linux'),'Linux shared physical-gate fixture required')
class ExecutionRouterTests(unittest.TestCase):
    def setUp(self):
        self.fixture=delivery_fixture.TreeDarDeliveryTests(methodName='runTest')
        with patch.object(TreeDarDelivery,'start',return_value='deferred_until_router_bound'):
            self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.root,self.ledger,self.tree=self.fixture.root,self.fixture.ledger,self.fixture.delivery
        self.outbox,self.failures=self.fixture.outbox,self.fixture.failures
        self.transport=driver_fixture.EngineTransport()
        self.native=NativeEngine('http://127.0.0.1:3001')
        for name in ('initialize','upload','action','read','download'):
            mocked=patch.object(self.native,name,side_effect=getattr(self.transport,name))
            mocked.start();self.addCleanup(mocked.stop)
        self.odm=self.make_odm()
        self.router=self.open()

    def make_odm(self):
        c=self.fixture.config
        observed=self.native.read('/info')['engineVersion']
        return ReferenceEngineWorker(self.root/'private'/'odm.sqlite',ledger=self.ledger,outbox=self.outbox,
            engine=self.native,engine_id=c.engine_id,node_id='main',authority_id=c.authority_id,
            coordinator_origin=c.coordinator_origin,gate_generation=c.gate_generation,
            worker_id=c.worker_id,engine_version=observed)

    def open(self):
        return ExecutionRouter(self.root/'private'/'routes.sqlite',self.odm,self.tree,failure_outbox=self.failures)

    def finish_tree(self):
        job=self.fixture.job
        self.router.bind(job)
        self.tree.start(job,reservation_id=self.fixture.reservation,
            input_paths={'lidar1':self.fixture.relative_input},provenance=self.fixture.provenance,allow_synthetic=True)
        self.fixture.success()
        identifier=self.tree.poll(job['jobId'])
        claim=self.outbox.claim_next()
        envelope={key:claim[key] for key in ('outboxId','payloadSha256','payload')}
        receipt=self.fixture.receiver.receive(envelope,**self.fixture.identity)
        self.tree.acknowledge_receipt(job['jobId'],receipt)
        return job,identifier

    def new_odm(self):
        self.fixture.store=ReferenceStore(self.fixture.queue,local_max_photos=1)
        raw=copy.deepcopy(json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())['submission'])
        raw['submissionId']=str(uuid.uuid4())
        raw['manifest']['objects'][0].update(name='survey_0042.DNG',mediaType='image/x-adobe-dng')
        second=copy.deepcopy(raw['manifest']['objects'][0])
        second.update(id=second['id']+'-second',name='survey_0043.DNG')
        raw['manifest']['objects'].append(second)
        raw['manifest']['objectCount']=2
        raw['manifest']['inputCounts']['images']=2
        raw['manifest']['totalBytes']=6
        raw['manifest']['sha256']=p.manifest_sha256(raw['manifest'])
        submitted=self.fixture.store.submit(p.validate_submission(raw,environment='staging',
            capability_fingerprint=raw['capabilityFingerprint']))[0]
        identity={**self.fixture.identity,'claim_request_id':'odm-'+uuid.uuid4().hex}
        del identity['claim_token']
        job=self.fixture.store.claim(submitted['jobId'],**identity)
        identity['claim_token']=job['claimToken']
        reservation='odm-'+uuid.uuid4().hex
        self.router.bind(job)  # No transfer or native mutation has occurred.
        self.ledger.reserve(job_id=job['jobId'],generation=1,reservation_id=reservation,
            manifest_sha256=job['manifestSha256'],input_bytes=6,output_budget=1024**2,
            environment='staging',ttl_seconds=1000,worker_ready=True)
        folder=self.ledger.root/'jobs'/reservation
        folder.mkdir(mode=0o700)
        paths={}
        for number,obj in enumerate(job['submission']['manifest']['objects']):
            path=folder/('photo-'+str(number)+'.input');path.write_bytes(b'abc')
            paths[obj['id']]=str(path.relative_to(self.ledger.root))
        self.ledger.record_allocated(reservation,6)
        transfer=copy.deepcopy(json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())['transferRequest'])
        transfer.update(reservationId=reservation,manifestSha256=job['manifestSha256'],byteBudget=6)
        self.fixture.store.reserve(job['jobId'],transfer,node_id='main')
        self.fixture.store.update_phase(job['jobId'],'fetching')
        self.fixture.store.update_phase(job['jobId'],'processing')
        job=self.fixture.store.get(job['jobId'],private=True)
        return job,identity,reservation,paths

    def finish_odm(self):
        job,identity,reservation,paths=self.new_odm()
        worker=self.router.worker_for(job['jobId'],for_execution=True)
        worker.start(job,reservation_id=reservation,input_paths=paths)
        identifier=worker.poll(job['jobId'])
        claim=self.outbox.claim_next()
        receipt=DeliveryReceiver(self.fixture.store).receive({key:claim[key] for key in ('outboxId','payloadSha256','payload')},**identity)
        self.outbox.acknowledge(identifier,receipt)
        worker.acknowledge_delivery(job['jobId'])
        return job,reservation,paths

    def read_result(self,job):
        results=ReferenceResults(self.router)
        metadata=results.metadata(job['jobId'])
        obj=metadata['objects'][0]
        authorization=ResultAuthorization(node_id='main',worker_id=self.router.worker_id,job_id=job['jobId'],
            generation=1,environment='staging',result_manifest_sha256=metadata['resultManifestSha256'],
            object_ids=(obj['id'],),share_approved=True)
        with results.open(job['jobId'],generation=1,object_id=obj['id'],authorization=authorization,
            result_manifest_sha256=metadata['resultManifestSha256']) as stream:
            return b''.join(stream.chunks())

    def acceptance(self,job,suffix):
        result=json.loads(self.router._row(job['jobId'])['result'])
        return dict(schemaVersion=1,receiptId='viewer-'+suffix,storageReceiptId='viewer-storage-'+suffix,
            jobId=job['jobId'],generation=1,environment='staging',inputManifestSha256=job['manifestSha256'],
            pipelineDigest=job['pipelineDigest'],resultManifestSha256=result['resultManifestSha256'],
            acceptedObjectIds=[obj['id'] for obj in result['objects']],acceptedAt='2026-10-28T12:00:00Z',validationVersion='ltds-reference-v1')

    def test_bind_is_durable_idempotent_and_has_no_engine_or_transfer_effect(self):
        before=list(self.fixture.docker.calls),list(self.transport.calls)
        descriptor=self.router.bind(self.fixture.job)
        self.assertEqual(descriptor['executionKind'],'treedar')
        self.assertEqual(self.router.bind(self.fixture.job),descriptor)
        self.router=self.open()
        self.assertEqual(self.router.describe(self.fixture.job['jobId']),descriptor)
        self.assertEqual((self.fixture.docker.calls,self.transport.calls),before)
        self.assertIs(self.router.worker_for(self.fixture.job['jobId']),self.tree)

    def test_alternating_engines_retained_results_and_receipts_route_per_job(self):
        tree_job,_=self.finish_tree()
        odm_job,_,_=self.finish_odm()
        self.router=self.open()
        self.assertEqual(self.router.kind_for(tree_job['jobId']),'treedar')
        self.assertEqual(self.router.kind_for(odm_job['jobId']),'odm')
        self.assertEqual(self.read_result(tree_job),b'synthetic output')
        self.assertEqual(self.read_result(odm_job),self.transport.archive)
        self.assertEqual([call.args[1]['name'] for call in self.native.upload.call_args_list],
            ['survey_0042.DNG','survey_0043.DNG'])
        for index,job in enumerate((tree_job,odm_job)):
            result=json.loads(self.router._row(job['jobId'])['result'])
            receipt=dict(schemaVersion=1,receiptId=f'viewer-{index}',storageReceiptId=f'viewer-storage-{index}',
                jobId=job['jobId'],generation=1,environment='staging',inputManifestSha256=job['manifestSha256'],
                pipelineDigest=job['pipelineDigest'],resultManifestSha256=result['resultManifestSha256'],
                acceptedObjectIds=[obj['id'] for obj in result['objects']],acceptedAt='2026-10-28T12:00:00Z',validationVersion='ltds-reference-v1')
            self.assertTrue(self.router.apply_viewer_acceptance(job['jobId'],[receipt])['fullyAccepted'])
        self.assertEqual(self.router.odm_worker,self.odm)
        self.assertEqual(self.router.treedar_worker,self.tree)

    def test_idle_odm_version_upgrade_keeps_old_results_but_blocks_old_execution(self):
        self.finish_tree()
        old_job,_,_=self.finish_odm()
        old_version=self.router.describe(old_job['jobId'])['runtimeBinding']['engineVersion']
        self.transport.version='observed-new-engine-version'
        self.odm=self.make_odm()
        self.router=self.open()
        self.assertEqual(self.router.describe(old_job['jobId'])['runtimeBinding']['engineVersion'],old_version)
        calls=list(self.transport.calls)
        self.assertEqual(self.read_result(old_job),self.transport.archive)
        self.assertEqual(self.transport.calls,calls)
        self.assertTrue(self.router.apply_viewer_acceptance(old_job['jobId'],[self.acceptance(old_job,'after-upgrade')])['fullyAccepted'])
        with self.assertRaisesRegex(RouteConflict,'runtime_binding_changed'):
            self.router.worker_for(old_job['jobId'],for_execution=True)
        new_job,_,_,_=self.new_odm()
        self.assertEqual(self.router.describe(new_job['jobId'])['runtimeBinding']['engineVersion'],self.transport.version)

    def test_pending_odm_route_cannot_resume_under_new_runtime(self):
        self.finish_tree()
        job,_,_,_=self.new_odm()
        self.transport.version='new-engine-before-start'
        self.odm=self.make_odm()
        self.router=self.open()
        with self.assertRaisesRegex(RouteConflict,'pending_execution_runtime_binding_changed'):
            self.router.worker_for(job['jobId'])
        with self.assertRaises(RouteConflict):
            self.router.bind(job)
        self.assertFalse(any(call[0]=='initialize' for call in self.transport.calls))

    def test_unknown_changed_generation_environment_or_pipeline_cannot_rebind(self):
        with self.assertRaisesRegex(RouteConflict,'unknown_execution_route'):
            self.router.worker_for(str(uuid.uuid4()))
        self.router.bind(self.fixture.job)
        for update in ({'generation':2},{'environment':'production'},{'manifestSha256':'f'*64},{'pipelineDigest':'f'*64}):
            with self.subTest(update=update),self.assertRaises(RouteConflict):
                self.router.bind({**self.fixture.job,**update})
        with self.assertRaises(RouteConflict):
            self.router.bind({**self.fixture.job,'nodeId':'cluster'})

    def test_wrong_shared_components_and_native_impostor_are_rejected(self):
        other=FailureOutbox(self.root/'private'/'different-failure.sqlite')
        with self.assertRaises(ValueError):
            ExecutionRouter(self.root/'private'/'wrong.sqlite',self.odm,self.tree,failure_outbox=other)
        actual=self.odm.engine
        self.odm.engine=object()
        with self.assertRaises(ValueError):
            self.open()
        self.odm.engine=actual
        outbox=self.tree.outbox
        self.tree.outbox=WorkerOutbox(self.root/'private'/'different-results.sqlite')
        with self.assertRaises(ValueError):
            self.open()
        self.tree.outbox=outbox
        for name,value in (('node_id','cluster'),('authority_id',str(uuid.uuid4())),
                           ('gate_generation',99),('origin','http://127.0.0.1:9999')):
            previous=getattr(self.tree,name)
            setattr(self.tree,name,value)
            with self.subTest(field=name),self.assertRaises(ValueError):
                self.open()
            setattr(self.tree,name,previous)

    def test_combined_pipeline_remains_explicitly_unsupported(self):
        combined=copy.deepcopy(self.fixture.job)
        combined['submission']['pipeline']['odm']['requested']=True
        combined['pipelineDigest']=p.digest(combined['submission']['pipeline'])
        with self.assertRaisesRegex(RouteConflict,'combined_or_empty_pipeline_requires_stage_plan'):
            self.router.bind(combined)
        self.assertEqual(self.fixture.docker.calls,[])

    def test_reservation_file_facade_uses_bound_job_and_expected_digest(self):
        self.router.bind(self.fixture.job)
        _,size,sha=self.router._file(self.fixture.reservation,self.fixture.relative_input,3,self.fixture.obj['sha256'])
        self.assertEqual((size,sha),(3,self.fixture.obj['sha256']))
        with self.assertRaises(ValueError):
            self.router._file('r1','jobs/r1/cloud.input',3,self.fixture.obj['sha256'])

    def test_optional_treedar_can_be_added_without_rebinding_existing_odm_journal(self):
        path=self.root/'private'/'initially-odm-only.sqlite'
        first=ExecutionRouter(path,self.odm,failure_outbox=self.failures)
        second=ExecutionRouter(path,self.odm,self.tree,failure_outbox=self.failures)
        self.assertIs(first.odm_worker,second.odm_worker)
        second.bind(self.fixture.job)
        self.assertEqual(second.kind_for(self.fixture.job['jobId']),'treedar')

    def test_unbound_old_engine_intent_is_not_automatically_backfilled(self):
        self.tree.start(self.fixture.job,reservation_id=self.fixture.reservation,
            input_paths={'lidar1':self.fixture.relative_input},provenance=self.fixture.provenance,allow_synthetic=True)
        with self.assertRaisesRegex(RouteConflict,'legacy_execution_requires_explicit_adoption'):
            self.router.bind(self.fixture.job)

    def test_actual_gate_generation_change_blocks_new_route(self):
        with NodeGate(self.router.engine_id) as gate:
            state=gate.snapshot()
            state['generation']+=1
            gate._save(state)
        with self.assertRaisesRegex(RouteConflict,'physical_gate_binding_changed'):
            self.router.bind(self.fixture.job)

    def test_actual_route_connection_is_full_sync_and_transactional(self):
        with self.router._db() as db:
            self.assertTrue(db.in_transaction)
            self.assertEqual(db.execute('PRAGMA synchronous').fetchone()[0],2)


if __name__=='__main__':
    unittest.main()
