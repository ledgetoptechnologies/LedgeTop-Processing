import copy
from dataclasses import replace
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
import uuid

from processing import reference_protocol as p
from processing.prototype import Queue
from processing.reference_store import ReferenceStore
from processing.reference_delivery import DeliveryReceiver
from processing.reference_failure import FailureReceiver, FailureOutbox
from processing.reference_engine_worker import WorkerHeld
from processing.reference_results import ReferenceResults, ResultAuthorization, ResultUnavailable
from processing.node_gate import NodeGate
from processing.treedar_engine import TreeDarEngine, TreeDarHeld
from processing.treedar_delivery import TreeDarDelivery
from processing.worker_outbox import WorkerOutbox, OutboxConflict
from tests import test_treedar_engine as engine_fixture


@unittest.skipUnless(sys.platform.startswith('linux'), 'Linux host controller and physical-gate ownership fixture required')
class TreeDarDeliveryTests(unittest.TestCase):
    def setUp(self):
        self.fixture = engine_fixture.TreeDarEngineTests(methodName='runTest')
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.root,self.ledger=self.fixture.root,self.fixture.ledger
        self.queue=Queue(self.root/'coordinator',reference_enabled=True,node_ids=('cluster','main'),treedar_node_ids=('main',))
        self.addCleanup(lambda:self.queue.close())
        self.store=ReferenceStore(self.queue)
        raw=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())['submission']
        obj=raw['manifest']['objects'][0]
        obj.update(id='lidar1',kind='lidar',name='cloud.las',mediaType='application/vnd.las')
        raw['manifest']['inputCounts'].update(images=0,lidarObjects=1,points=4555)
        raw['manifest']['sha256']=p.manifest_sha256(raw['manifest'])
        raw['pipeline']['odm']['requested']=False
        raw['pipeline']['treedar'].update(requested=True,inputObjectIds=['lidar1'],provenanceReceiptId='prov1')
        submitted=self.store.submit(p.validate_submission(raw,environment='staging',
            capability_fingerprint=raw['capabilityFingerprint'],available_engines=('odm','treedar')))[0]
        self.identity=dict(node_id='main',worker_id='worker-main',worker_instance_id='instance-main',claim_request_id='claim-delivery')
        self.job=self.store.claim(submitted['jobId'],**self.identity)
        self.identity['claim_token']=self.job['claimToken']
        transfer=copy.deepcopy(json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())['transferRequest'])
        transfer.update(reservationId='delivery-reservation',manifestSha256=self.job['manifestSha256'])
        self.store.reserve(self.job['jobId'],transfer,node_id='main')
        self.store.update_phase(self.job['jobId'],'fetching')
        self.store.update_phase(self.job['jobId'],'processing')
        self.job=self.store.get(self.job['jobId'],private=True)
        self.obj=self.job['submission']['manifest']['objects'][0]
        self.reservation='delivery-reservation'
        self.ledger.reserve(job_id=self.job['jobId'],generation=1,reservation_id=self.reservation,
            manifest_sha256=self.job['manifestSha256'],input_bytes=3,output_budget=1024**2,
            environment='staging',ttl_seconds=1000,worker_ready=True)
        folder=self.root/'jobs'/self.reservation
        folder.mkdir(mode=0o700)
        self.input=folder/'cloud.input'
        self.input.write_bytes(b'abc')
        self.ledger.record_allocated(self.reservation,3)
        self.provenance={**self.fixture.provenance,'jobId':self.job['jobId']}
        with NodeGate(self.fixture.config.engine_id) as gate:
            gate.handover('main',self.fixture.config.coordinator_origin,self.queue.authority_id,1,
                old_worker_stopped=True,note='isolated synthetic test binding')
        self.config=replace(self.fixture.config,authority_id=self.queue.authority_id,gate_generation=2)
        self.docker=engine_fixture.FakeDocker(self.config)
        self.outbox=WorkerOutbox(self.root/'private'/'results.sqlite',clock=lambda:self.fixture.now)
        self.failures=FailureOutbox(self.root/'private'/'failures.sqlite',clock=lambda:self.fixture.now)
        self.controller=self.open_controller()
        self.delivery=self.open_delivery()
        self.receiver=DeliveryReceiver(self.store)
        self.failure_receiver=FailureReceiver(self.store)
        self.delivery.start(self.job,reservation_id=self.reservation,input_paths={'lidar1':self.relative_input},
            provenance=self.provenance,allow_synthetic=True)

    @property
    def relative_input(self):
        return 'jobs/'+self.reservation+'/cloud.input'

    def open_controller(self):
        return TreeDarEngine(self.root/'private'/'controller.sqlite',ledger=self.ledger,config=self.config,
            verify_provenance=lambda value:value==self.provenance,runner=self.docker,clock=lambda:self.fixture.now)

    def open_delivery(self):
        return TreeDarDelivery(self.root/'private'/'delivery.sqlite',controller=self.controller,
            outbox=self.outbox,failure_outbox=self.failures)

    def restart(self, *, coordinator=False):
        if coordinator:
            self.queue.close()
            self.queue=Queue(self.root/'coordinator',reference_enabled=True,node_ids=('cluster','main'),treedar_node_ids=('main',))
            self.store=ReferenceStore(self.queue)
            self.receiver=DeliveryReceiver(self.store)
            self.failure_receiver=FailureReceiver(self.store)
        self.controller=self.open_controller()
        self.delivery=self.open_delivery()

    def success(self):
        # Real controller persists and validates its stopped-container terminal
        # proof; only its Docker transport and native file contents are fixtures.
        self.fixture.engine=self.controller
        self.fixture.job,self.fixture.obj,self.fixture.docker=self.job,self.obj,self.docker
        return self.fixture.make_proof()

    def claim_delivery(self, *, failed=False):
        box=self.failures if failed else self.outbox
        claim=box.claim_next()
        return {key:claim[key] for key in ('outboxId','payloadSha256','payload')}

    def gate(self):
        with NodeGate(self.config.engine_id) as gate:
            return gate.snapshot()

    def acceptance(self,result,ids):
        return dict(schemaVersion=1,receiptId='viewer-1',storageReceiptId='viewer-storage-1',jobId=self.job['jobId'],
            generation=1,environment='staging',inputManifestSha256=self.job['manifestSha256'],
            pipelineDigest=self.job['pipelineDigest'],resultManifestSha256=result['resultManifestSha256'],
            acceptedObjectIds=ids,acceptedAt='2026-10-28T10:00:00Z',validationVersion='ltds-reference-v1')

    def verify_copy(self,receipt):
        """The ledger's half of a verified Viewer copy; signature checks live in the worker tests."""
        self.ledger.accept_receipt(self.reservation,receipt_id=receipt['receiptId'],
            storage_receipt_id=receipt['storageReceiptId'],job_id=receipt['jobId'],generation=receipt['generation'],
            manifest_sha256=receipt['inputManifestSha256'],pipeline_sha256=receipt['pipelineDigest'],
            result_manifest_sha256=receipt['resultManifestSha256'],accepted_object_ids=receipt['acceptedObjectIds'],
            viewer_receipt={'synthetic':'signature-verified-by-fixture'},verified_copy=True)

    def test_existing_result_wire_ack_frees_only_gate_and_preserves_outputs(self):
        output=self.success()
        identifier=self.delivery.poll(self.job['jobId'])
        self.assertEqual(self.delivery.completion(self.job['jobId']),{'kind':'result','outboxId':identifier})
        envelope=self.claim_delivery()
        self.assertEqual(envelope['payload']['resultManifest']['engineVersions'],{'treedar':'0.0.1'})
        self.assertFalse(any(obj['role']=='odm_archive' for obj in envelope['payload']['resultManifest']['objects']))
        self.assertNotIn('localObjects',envelope['payload'])
        self.assertIsNotNone(self.gate()['reservation'])
        receipt=self.receiver.receive(envelope,**self.identity)
        self.delivery.acknowledge_receipt(self.job['jobId'],receipt)
        self.assertIsNone(self.gate()['reservation'])
        self.assertEqual(self.store.get(self.job['jobId'])['phase'],'awaiting_acceptance')
        self.assertEqual(self.ledger.cleanup_candidates(),[])
        self.assertTrue(self.input.exists())
        self.assertTrue((output/'run'/'dtm.tif').exists())

    def test_results_compatibility_reads_before_acceptance_then_pins_block_cleanup(self):
        self.success();self.delivery.poll(self.job['jobId'])
        results=ReferenceResults(self.delivery)
        metadata=results.metadata(self.job['jobId'])
        obj=metadata['objects'][0]
        authorization=ResultAuthorization(node_id='main',worker_id='worker-main',job_id=self.job['jobId'],
            generation=1,environment='staging',result_manifest_sha256=metadata['resultManifestSha256'],
            object_ids=(obj['id'],),share_approved=True)
        stream=results.open(self.job['jobId'],generation=1,object_id=obj['id'],authorization=authorization,
            result_manifest_sha256=metadata['resultManifestSha256'])
        self.assertEqual(stream.headers['Content-Type'],'application/octet-stream')
        receipt=self.acceptance(json.loads(self.delivery._row(self.job['jobId'])['result']),[obj['id']])
        coverage=self.delivery.apply_viewer_acceptance(self.job['jobId'],[receipt])
        self.assertFalse(coverage['fullyAccepted'])
        self.assertEqual(self.ledger.cleanup_candidates(),[])
        self.verify_copy(receipt)
        self.assertEqual(self.ledger.cleanup_candidates(),[])
        self.assertEqual(b''.join(stream.chunks()),b'synthetic output')
        self.assertEqual([candidate['id'] for candidate in self.ledger.cleanup_candidates()],[obj['id']])

    def test_unsigned_acceptance_alone_never_makes_output_cleanable(self):
        self.success();self.delivery.poll(self.job['jobId'])
        result=json.loads(self.delivery._row(self.job['jobId'])['result'])
        ids=[item['id'] for item in result['objects']]
        self.delivery.apply_viewer_acceptance(self.job['jobId'],[self.acceptance(result,ids)])
        self.assertEqual(self.ledger.cleanup_candidates(),[])

    def test_crash_after_projection_before_registration_restarts_idempotently(self):
        self.success()
        with patch.object(self.ledger,'register_result',side_effect=OSError('synthetic register gap')):
            with self.assertRaises(OSError):
                self.delivery.poll(self.job['jobId'])
        self.assertEqual(self.delivery._row(self.job['jobId'])['phase'],'proof_durable')
        self.restart()
        identifier=self.delivery.poll(self.job['jobId'])
        self.assertEqual(self.delivery.poll(self.job['jobId']),identifier)
        self.assertEqual(sum(args[0]=='start' for args in self.docker.calls),1)

    def test_crash_after_registration_and_after_outbox_commit_reuses_exact_payload(self):
        self.success()
        original=self.outbox.persist_terminal
        persisted=[]
        def crash(**kwargs):
            persisted.append(original(**kwargs))
            raise OSError('synthetic outbox commit gap')
        with patch.object(self.outbox,'persist_terminal',crash):
            with self.assertRaises(OSError):
                self.delivery.poll(self.job['jobId'])
        self.assertIsNone(self.delivery._row(self.job['jobId'])['outbox_id'])
        with self.ledger._transaction() as db:
            self.assertIsNotNone(db.execute('SELECT identity FROM results WHERE reservation=?',(self.reservation,)).fetchone())
        payload=self.outbox.get(persisted[0])['payload']
        self.restart()
        self.assertEqual(self.delivery.poll(self.job['jobId']),persisted[0])
        self.assertEqual(self.outbox.get(persisted[0])['payload'],payload)

    def test_wrong_or_generic_metadata_ack_cannot_release_gate(self):
        self.success();self.delivery.poll(self.job['jobId'])
        envelope=self.claim_delivery()
        receipt=self.receiver.receive(envelope,**self.identity)
        for wrong in ({'status':200},{**receipt,'resultManifestSha256':'f'*64}):
            with self.assertRaises(OutboxConflict):
                self.delivery.acknowledge_receipt(self.job['jobId'],wrong)
            self.assertIsNotNone(self.gate()['reservation'])
        with self.assertRaises(WorkerHeld):
            self.delivery.acknowledge_delivery(self.job['jobId'])

    def test_persisted_outbox_binding_corruption_cannot_release_original_gate(self):
        self.success();identifier=self.delivery.poll(self.job['jobId'])
        receipt=self.receiver.receive(self.claim_delivery(),**self.identity)
        with self.outbox._transaction() as db:
            db.execute('UPDATE completions SET digest=? WHERE id=?',('f'*64,identifier))
        with self.assertRaisesRegex(WorkerHeld,'terminal_outbox_binding_changed'):
            self.delivery.acknowledge_receipt(self.job['jobId'],receipt)
        self.assertIsNotNone(self.gate()['reservation'])

    def test_sealed_result_version_cannot_change_after_projection(self):
        self.success();self.delivery.poll(self.job['jobId'])
        result=json.loads(self.delivery._row(self.job['jobId'])['result'])
        result['engineVersions']={'odm':'fake'}
        result['resultManifestSha256']=p.digest({key:value for key,value in result.items() if key!='resultManifestSha256'})
        with self.delivery._db() as db:
            db.execute('UPDATE executions SET result=? WHERE job=?',(p.canonical_json(result).decode(),self.job['jobId']))
        with self.assertRaisesRegex(WorkerHeld,'sealed_delivery_projection_changed'):
            self.delivery.poll(self.job['jobId'])
        self.assertIsNotNone(self.gate()['reservation'])

    def test_lost_coordinator_ack_restart_keeps_exact_payload_and_files(self):
        output=self.success();identifier=self.delivery.poll(self.job['jobId'])
        envelope=self.claim_delivery()
        first=self.receiver.receive(envelope,**self.identity)
        self.restart(coordinator=True)
        self.assertIsNotNone(self.gate()['reservation'])
        self.assertEqual(self.delivery.poll(self.job['jobId']),identifier)
        replay=self.receiver.receive(envelope,**self.identity)
        self.assertEqual(replay,first)
        self.delivery.acknowledge_receipt(self.job['jobId'],replay)
        self.assertTrue((output/'run'/'dtm.tif').exists())
        self.assertTrue(self.input.exists())

    def test_crash_after_gate_ack_before_local_phase_uses_exact_tombstone(self):
        self.success();self.delivery.poll(self.job['jobId'])
        receipt=self.receiver.receive(self.claim_delivery(),**self.identity)
        with patch.object(self.delivery,'_phase',side_effect=OSError('synthetic released-gate gap')):
            with self.assertRaises(OSError):
                self.delivery.acknowledge_receipt(self.job['jobId'],receipt)
        self.assertIsNone(self.gate()['reservation'])
        self.restart()
        self.delivery.poll(self.job['jobId'])
        self.assertEqual(self.delivery._row(self.job['jobId'])['phase'],'delivered')

    def test_exact_old_ack_replay_does_not_touch_next_job_gate_reservation(self):
        self.success();self.delivery.poll(self.job['jobId'])
        receipt=self.receiver.receive(self.claim_delivery(),**self.identity)
        self.delivery.acknowledge_receipt(self.job['jobId'],receipt)
        next_receipt='d'*32
        with NodeGate(self.config.engine_id) as gate:
            gate.reserve('main',self.config.coordinator_origin,self.config.authority_id,2,next_receipt)
            gate.bind(next_receipt,dict(id=str(uuid.uuid4()),workerId=self.config.worker_id,nodeId='main',authorityId=self.config.authority_id))
        self.delivery.acknowledge_receipt(self.job['jobId'],receipt)
        self.assertEqual(self.gate()['reservation']['receiptId'],next_receipt)

    def test_gate_release_crash_then_next_job_recovers_old_exact_tombstone(self):
        self.success();self.delivery.poll(self.job['jobId'])
        receipt=self.receiver.receive(self.claim_delivery(),**self.identity)
        with patch.object(self.delivery,'_phase',side_effect=OSError('synthetic marker crash')):
            with self.assertRaises(OSError):
                self.delivery.acknowledge_receipt(self.job['jobId'],receipt)
        next_receipt='e'*32
        with NodeGate(self.config.engine_id) as gate:
            gate.reserve('main',self.config.coordinator_origin,self.config.authority_id,2,next_receipt)
            gate.bind(next_receipt,dict(id=str(uuid.uuid4()),workerId=self.config.worker_id,nodeId='main',authorityId=self.config.authority_id))
        before=self.gate()
        self.restart()
        self.delivery.poll(self.job['jobId'])
        self.assertEqual(self.delivery._row(self.job['jobId'])['phase'],'delivered')
        self.assertEqual(self.gate(),before)  # No write/ACK to the next reservation.

    def test_gate_release_crash_without_exact_old_tombstone_remains_held(self):
        self.success();self.delivery.poll(self.job['jobId'])
        receipt=self.receiver.receive(self.claim_delivery(),**self.identity)
        with patch.object(self.delivery,'_phase',side_effect=OSError('synthetic marker crash')):
            with self.assertRaises(OSError):
                self.delivery.acknowledge_receipt(self.job['jobId'],receipt)
        next_receipt='f'*32
        with NodeGate(self.config.engine_id) as gate:
            gate.reserve('main',self.config.coordinator_origin,self.config.authority_id,2,next_receipt)
            gate.bind(next_receipt,dict(id=str(uuid.uuid4()),workerId=self.config.worker_id,nodeId='main',authorityId=self.config.authority_id))
            state=gate.snapshot()
            state['lastAcknowledgment']['completionDigest']='f'*64
            gate._save(state)
        before=self.gate()
        self.restart()
        with self.assertRaises(ValueError):
            self.delivery.poll(self.job['jobId'])
        self.assertEqual(self.gate(),before)
        self.assertNotEqual(self.delivery._row(self.job['jobId'])['phase'],'delivered')

    def test_failure_reuses_existing_failure_protocol_and_no_file_acceptance(self):
        self.docker.state,self.docker.exit_code='exited',1
        identifier=self.delivery.poll(self.job['jobId'])
        self.assertEqual(self.delivery.completion(self.job['jobId']),{'kind':'failure','outboxId':identifier})
        envelope=self.claim_delivery(failed=True)
        self.assertNotIn('resultManifest',envelope['payload'])
        receipt=self.failure_receiver.receive(envelope,**self.identity)
        self.delivery.acknowledge_receipt(self.job['jobId'],receipt)
        self.assertIsNone(self.gate()['reservation'])
        self.assertEqual(self.store.get(self.job['jobId'])['phase'],'awaiting_acceptance')
        self.assertEqual(self.failure_receiver.get(self.job['jobId'],1)['deliveryState'],'awaiting_viewer_failure_acceptance')
        self.assertTrue(self.input.exists())
        self.assertEqual(self.ledger.cleanup_candidates(),[])
        with self.assertRaises(ResultUnavailable):
            ReferenceResults(self.delivery).metadata(self.job['jobId'])

    def test_running_or_unavailable_engine_does_not_invent_terminal_failure(self):
        self.assertIsNone(self.delivery.poll(self.job['jobId']))
        self.docker.fail='inspect'
        with self.assertRaises(TreeDarHeld):
            self.delivery.poll(self.job['jobId'])
        self.assertIsNone(self.outbox.claim_next())
        self.assertIsNone(self.failures.claim_next())
        self.assertIsNotNone(self.gate()['reservation'])
        self.assertTrue(self.input.exists())

    def test_mixed_odm_pipeline_is_not_reinterpreted_as_treedar_only(self):
        mixed=copy.deepcopy(self.job)
        mixed['submission']['pipeline']['odm']['requested']=True
        mixed['pipelineDigest']=p.digest(mixed['submission']['pipeline'])
        with self.assertRaisesRegex(WorkerHeld,'treedar_only_job_required'):
            self.delivery.start(mixed,reservation_id=self.reservation,input_paths={'lidar1':self.relative_input},
                provenance=self.provenance,allow_synthetic=True)
        self.assertEqual(sum(args[0]=='start' for args in self.docker.calls),1)

    def test_failure_outbox_commit_gap_replays_without_engine_restart(self):
        self.docker.state,self.docker.exit_code='exited',1
        original=self.failures.persist
        committed=[]
        def crash(*args,**kwargs):
            committed.append(original(*args,**kwargs))
            raise OSError('synthetic failure outbox gap')
        with patch.object(self.failures,'persist',crash):
            with self.assertRaises(OSError):
                self.delivery.poll(self.job['jobId'])
        self.restart()
        self.assertEqual(self.delivery.poll(self.job['jobId']),committed[0])
        self.assertEqual(sum(args[0]=='start' for args in self.docker.calls),1)

    def test_changed_controller_version_cannot_rebind_delivery_projection(self):
        self.success();self.delivery.poll(self.job['jobId'])
        old=self.controller.config
        self.controller.config=replace(old,engine_version='different')
        with self.assertRaises(WorkerHeld):
            self.open_delivery()
        self.controller.config=old


if __name__=='__main__':
    unittest.main()
