import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from processing.prototype import Queue
from processing.reference_store import ReferenceStore
from processing.reference_delivery import DeliveryReceiver
from processing.reference_protocol import validate_submission, digest
from processing.worker_outbox import WorkerOutbox
from processing.maintenance import Conflict


class ReferenceDeliveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.queue = Queue(self.root / 'coordinator', reference_enabled=True)
        self.store = ReferenceStore(self.queue)
        self.fixture = json.loads((Path(__file__).resolve().parents[1] /
            'contracts/reference-v1.contract.json').read_text())
        raw = self.fixture['submission']
        self.job = self.store.submit(validate_submission(raw, environment='staging',
            capability_fingerprint=raw['capabilityFingerprint']))[0]
        self.identity = dict(node_id='cluster', worker_id='worker',
            worker_instance_id='instance', claim_request_id='claim-1')
        claim = self.store.claim(self.job['jobId'], **self.identity)
        self.identity['claim_token'] = claim['claimToken']
        self.store.reserve(self.job['jobId'], self.fixture['transferRequest'], node_id='cluster')
        self.store.update_phase(self.job['jobId'], 'fetching')
        self.store.update_phase(self.job['jobId'], 'processing')
        self.result = copy.deepcopy(self.fixture['resultManifest'])
        self.result.update(jobId=self.job['jobId'], pipelineDigest=self.job['pipelineDigest'])
        self.result['objects'][0]['role'] = 'odm_archive'
        self.result['resultManifestSha256'] = digest({k:v for k,v in self.result.items()
            if k != 'resultManifestSha256'})
        self.outbox = WorkerOutbox(self.root / 'worker' / 'outbox.sqlite')
        self.outbox_id = self.outbox.persist_terminal(job_id=self.job['jobId'], generation=1,
            environment='staging', input_manifest_sha256=self.job['manifestSha256'],
            pipeline_digest=self.job['pipelineDigest'],
            result_manifest_sha256=self.result['resultManifestSha256'], result_manifest=self.result,
            owned_prefix='jobs/reservation-1', local_objects={obj['id']:
                'jobs/reservation-1/' + obj['id'] + '.bin' for obj in self.result['objects']})
        self.delivery = self.outbox.claim_next()
        self.envelope = {k: self.delivery[k] for k in ('outboxId', 'payloadSha256', 'payload')}
        self.receiver = DeliveryReceiver(self.store)

    def tearDown(self):
        self.queue.close()
        self.temp.cleanup()

    def restart(self):
        self.queue.close()
        self.queue = Queue(self.root / 'coordinator', reference_enabled=True)
        self.store = ReferenceStore(self.queue)
        self.receiver = DeliveryReceiver(self.store)

    def test_offline_worker_returns_after_coordinator_restart_and_lost_ack(self):
        self.restart()
        self.assertEqual(self.queue.get(self.job['jobId'])['status'], 'held')
        receipt = self.receiver.receive(self.envelope, **self.identity)
        self.assertEqual(self.queue.get(self.job['jobId'])['status'], 'succeeded')
        self.assertEqual(self.store.get(self.job['jobId'])['phase'], 'awaiting_acceptance')
        self.restart()  # Response was lost; resend the identical sealed completion.
        replay = self.receiver.receive(self.envelope, **self.identity)
        self.assertEqual(replay, receipt)
        self.outbox.acknowledge(self.outbox_id, replay)
        self.assertEqual(self.outbox.get(self.outbox_id)['state'], 'acknowledged')
        # Coordinator receipt is not Viewer acceptance or permission to delete.
        self.assertEqual(self.store.get(self.job['jobId'])['phase'], 'awaiting_acceptance')
        self.assertIsNotNone(self.outbox.get(self.outbox_id)['payload'])
        acceptance = copy.deepcopy(self.fixture['acceptance'])
        acceptance.update(jobId=self.job['jobId'], pipelineDigest=self.job['pipelineDigest'],
            inputManifestSha256=self.job['manifestSha256'],
            resultManifestSha256=self.result['resultManifestSha256'])
        self.assertFalse(self.store.accept(self.job['jobId'], acceptance)['fullyAccepted'])
        acceptance['receiptId'] = 'all-outputs'
        acceptance['storageReceiptId'] = 'all-outputs-storage'
        acceptance['acceptedObjectIds'] = [o['id'] for o in self.result['objects']]
        self.assertTrue(self.store.accept(self.job['jobId'], acceptance)['fullyAccepted'])

    def test_lost_viewer_acceptance_response_replays_after_restart_without_cleanup(self):
        coordinator_receipt = self.receiver.receive(self.envelope, **self.identity)
        self.outbox.acknowledge(self.outbox_id, coordinator_receipt)
        acceptance = copy.deepcopy(self.fixture['acceptance'])
        acceptance.update(jobId=self.job['jobId'], pipelineDigest=self.job['pipelineDigest'],
            inputManifestSha256=self.job['manifestSha256'],
            resultManifestSha256=self.result['resultManifestSha256'],
            acceptedObjectIds=[obj['id'] for obj in self.result['objects']])

        accepted = self.store.accept(self.job['jobId'], acceptance)
        self.assertTrue(accepted['fullyAccepted'])
        self.assertTrue(accepted['created'])
        self.assertEqual(accepted['job']['phase'], 'completed')
        retained = self.outbox.get(self.outbox_id)
        self.assertEqual(retained['state'], 'acknowledged')
        self.assertEqual(set(retained['local_metadata']['localObjects']),
                         {obj['id'] for obj in self.result['objects']})

        self.restart()  # Viewer committed; the acceptance HTTP response was lost.
        replay = self.store.accept(self.job['jobId'], acceptance)
        self.assertTrue(replay['fullyAccepted'])
        self.assertFalse(replay['created'])
        self.assertEqual(replay['job']['phase'], 'completed')

        changed = copy.deepcopy(acceptance)
        changed['acceptedAt'] = '2026-10-29T12:00:00Z'
        with self.assertRaises(Conflict):
            self.store.accept(self.job['jobId'], changed)
        # Even full Viewer acceptance is not a worker-file deletion receipt.
        self.assertEqual(self.outbox.get(self.outbox_id)['local_metadata'],
                         retained['local_metadata'])

    def test_crash_after_manifest_commit_before_slot_release_replays(self):
        with patch.object(self.queue, 'complete', side_effect=OSError('synthetic crash')):
            with self.assertRaises(OSError):
                self.receiver.receive(self.envelope, **self.identity)
        self.assertEqual(self.store.get(self.job['jobId'])['phase'], 'waiting_for_delivery')
        self.assertEqual(self.outbox.get(self.outbox_id)['state'], 'sending')
        self.restart()
        receipt = self.receiver.receive(self.envelope, **self.identity)
        self.outbox.acknowledge(self.outbox_id, receipt)
        self.assertEqual(self.queue.get(self.job['jobId'])['status'], 'succeeded')

    def test_bad_worker_cannot_release_slot_or_read_receipt(self):
        with self.assertRaises(Conflict):
            self.receiver.receive(self.envelope, **{**self.identity, 'claim_token': 'wrong'})
        self.assertEqual(self.queue.get(self.job['jobId'])['status'], 'running')
        self.receiver.receive(self.envelope, **self.identity)
        with self.assertRaises(Conflict):
            self.receiver.receive(self.envelope, **{**self.identity, 'worker_instance_id': 'other'})

    def test_missing_archive_or_wrong_engine_cannot_complete(self):
        for mutation in ('archive', 'engine'):
            envelope = copy.deepcopy(self.envelope)
            payload = envelope['payload']
            result = payload['resultManifest']
            if mutation == 'archive':
                result['objects'][0]['role'] = 'report'
            else:
                result['engineVersions'] = {'treedar': 'test-v1'}
            result['resultManifestSha256'] = digest({k:v for k,v in result.items()
                if k != 'resultManifestSha256'})
            payload['resultManifestSha256'] = result['resultManifestSha256']
            envelope['outboxId'] = digest({k: payload[k] for k in ('jobId', 'generation',
                'environment', 'inputManifestSha256', 'pipelineDigest', 'resultManifestSha256')})
            envelope['payloadSha256'] = digest(payload)
            with self.subTest(mutation=mutation), self.assertRaises(Conflict):
                self.receiver.receive(envelope, **self.identity)
            self.assertEqual(self.queue.get(self.job['jobId'])['status'], 'running')

    def test_crash_after_slot_release_before_delivery_phase_update(self):
        with patch.object(self.store, 'update_phase', side_effect=OSError('synthetic crash')):
            with self.assertRaises(OSError):
                self.receiver.receive(self.envelope, **self.identity)
        self.assertEqual(self.queue.get(self.job['jobId'])['status'], 'succeeded')
        self.restart()
        receipt = self.receiver.receive(self.envelope, **self.identity)
        self.outbox.acknowledge(self.outbox_id, receipt)
        self.assertEqual(self.store.get(self.job['jobId'])['phase'], 'awaiting_acceptance')

    def test_crash_after_phase_commit_before_receipt_commit(self):
        original = self.store._quota
        def fail_receipt(db, extra_bytes=0):
            if db.execute('SELECT phase FROM reference_jobs').fetchone()[0] == 'awaiting_acceptance':
                raise OSError('synthetic receipt commit crash')
            return original(db, extra_bytes)
        with patch.object(self.store, '_quota', side_effect=fail_receipt):
            with self.assertRaises(OSError):
                self.receiver.receive(self.envelope, **self.identity)
        self.restart()
        receipt = self.receiver.receive(self.envelope, **self.identity)
        self.outbox.acknowledge(self.outbox_id, receipt)


if __name__ == '__main__':
    unittest.main()
