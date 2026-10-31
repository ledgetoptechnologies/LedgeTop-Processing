import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import uuid

from processing.prototype import Queue
from processing.reference_store import ReferenceStore
from processing.reference_protocol import validate_submission, ProtocolError, digest, canonical_json
from processing.reference_failure import (FailureReceiver, FailureOutbox, make_failure,
    validate_failure, failure_sha256, BINDING)
from processing.maintenance import Conflict
from processing.worker_outbox import OutboxConflict


class ReferenceFailureTests(unittest.TestCase):
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
        self.now = 100
        self.outbox = self.open_outbox()
        self.failure = make_failure(job_id=self.job['jobId'], generation=1, environment='staging',
            input_manifest_sha256=self.job['manifestSha256'], pipeline_digest=self.job['pipelineDigest'],
            terminal_state='failed', error_code='engine_failed')
        self.outbox_id = self.outbox.persist(self.failure, engine_terminal=True)
        self.delivery = self.outbox.claim_next()
        self.envelope = {key: self.delivery[key] for key in ('outboxId', 'payloadSha256', 'payload')}
        self.receiver = FailureReceiver(self.store)

    def tearDown(self):
        self.queue.close()
        self.temp.cleanup()

    def open_outbox(self):
        return FailureOutbox(self.root / 'worker' / 'failures.sqlite', clock=lambda: self.now,
                             random_source=lambda: 1, retry_base_seconds=4, retry_max_seconds=16)

    def restart(self):
        self.queue.close()
        self.queue = Queue(self.root / 'coordinator', reference_enabled=True)
        self.store = ReferenceStore(self.queue)
        self.receiver = FailureReceiver(self.store)

    def reseal_envelope(self, record):
        payload = dict(failure=record, engineTerminal=True)
        return dict(outboxId=digest({key: record[key] for key in BINDING}),
                    payloadSha256=digest(payload), payload=payload)

    def test_closed_canonical_failure_record_has_no_path_or_freeform_error(self):
        self.assertEqual(failure_sha256(self.failure), self.failure['failureSha256'])
        checked = validate_failure(self.failure, job_id=self.job['jobId'], generation=1,
            environment='staging', input_manifest_sha256=self.job['manifestSha256'],
            pipeline_digest=self.job['pipelineDigest'])
        checked['errorCode'] = 'mutated'
        self.assertEqual(self.failure['errorCode'], 'engine_failed')
        for key, value in (('path', '/tmp/output'), ('message', 'sensitive exception'),
                           ('errorCode', 'https://bad.invalid/secret'), ('generation', True)):
            changed = copy.deepcopy(self.failure)
            changed[key] = value
            changed['failureSha256'] = failure_sha256(changed)
            with self.subTest(field=key), self.assertRaises(ProtocolError):
                self.outbox.persist(changed, engine_terminal=True)
        with self.assertRaises(ProtocolError):
            self.outbox.persist(self.failure, engine_terminal=False)

    def test_failure_releases_execution_without_viewer_ack_and_retains_metadata(self):
        receipt = self.receiver.receive(self.envelope, **self.identity)
        self.assertEqual(self.queue.get(self.job['jobId'])['status'], 'failed')
        record = self.receiver.get(self.job['jobId'], 1)
        self.assertEqual(record['failure'], self.failure)
        self.assertEqual(record['deliveryState'], 'awaiting_viewer_failure_acceptance')
        self.assertIsNone(self.store.get(self.job['jobId'], private=True)['resultManifest'])
        self.outbox.acknowledge(self.outbox_id, receipt)
        self.assertEqual(self.open_outbox().get(self.outbox_id)['state'], 'acknowledged')
        self.assertEqual(self.open_outbox().get(self.outbox_id)['payload']['failure'], self.failure)
        raw = copy.deepcopy(self.fixture['submission'])
        raw['submissionId'] = str(uuid.uuid4())
        other = self.store.submit(validate_submission(raw, environment='staging',
            capability_fingerprint=raw['capabilityFingerprint']))[0]
        claim = self.store.claim(other['jobId'], **{key: value for key, value in self.identity.items()
            if key not in ('claim_token', 'claim_request_id')}, claim_request_id='claim-2')
        self.assertEqual(claim['jobId'], other['jobId'])

    def test_sealed_failure_bytes_count_toward_later_ordinary_submission_quota(self):
        original_limit = self.store.max_metadata_bytes
        # Find existing usage through the quota boundary itself, without mirroring
        # its table accounting or hard-coding metadata/token representation sizes.
        low, high = 1, original_limit
        while low < high:
            midpoint = (low + high) // 2
            self.store.max_metadata_bytes = midpoint
            try:
                with self.queue._db() as db:
                    self.store._quota(db)
            except Conflict:
                low = midpoint + 1
            else:
                high = midpoint
        baseline = low
        self.store.max_metadata_bytes = original_limit

        raw = copy.deepcopy(self.fixture['submission'])
        raw['submissionId'] = str(uuid.uuid4())
        submission = validate_submission(raw, environment='staging',
            capability_fingerprint=raw['capabilityFingerprint'])
        # This budget would fit the next submission before failure metadata was
        # sealed, with one spare byte. The terminal failure must consume that room.
        later_limit = baseline + len(canonical_json(submission)) + 1
        self.receiver.receive(self.envelope, **self.identity)
        self.store.max_metadata_bytes = later_limit
        with self.assertRaisesRegex(Conflict, 'metadata quota'):
            self.store.submit(submission)
        self.assertEqual(self.receiver.get(self.job['jobId'], 1)['failure'], self.failure)
        self.store.max_metadata_bytes = original_limit
        _, created = self.store.submit(submission)
        self.assertTrue(created)  # Rejected submission created no partial job.

    def test_failure_and_success_seals_are_mutually_exclusive_across_commit_gap(self):
        result = copy.deepcopy(self.fixture['resultManifest'])
        result.update(jobId=self.job['jobId'], pipelineDigest=self.job['pipelineDigest'])
        result['resultManifestSha256'] = digest({k:v for k,v in result.items() if k != 'resultManifestSha256'})
        with patch.object(self.queue, 'complete', side_effect=OSError('synthetic crash')):
            with self.assertRaises(OSError):
                self.receiver.receive(self.envelope, **self.identity)
        self.assertEqual(self.queue.get(self.job['jobId'])['status'], 'running')
        with self.assertRaises(Conflict):
            self.store.set_result_manifest(self.job['jobId'], result)
        self.restart()
        self.receiver.receive(self.envelope, **self.identity)
        self.assertIsNone(self.store.get(self.job['jobId'], private=True)['resultManifest'])

    def test_failed_delivery_offline_resends_same_payload_after_restart(self):
        self.outbox.failed_attempt(self.outbox_id, self.delivery['claimToken'])
        self.assertIsNone(self.open_outbox().claim_next())
        self.now = 104
        retry = self.open_outbox().claim_next()
        self.assertEqual(retry['payload'], self.delivery['payload'])
        self.assertEqual(retry['outboxId'], self.outbox_id)
        self.restart()
        receipt = self.receiver.receive(self.envelope, **self.identity)
        self.restart()
        self.assertEqual(self.receiver.receive(self.envelope, **self.identity), receipt)
        self.assertEqual(self.receiver.get(self.job['jobId'], 1)['failure'], self.failure)

    def test_persist_before_queue_complete_crash_replays_exact_claim(self):
        with patch.object(self.queue, 'complete', side_effect=OSError('synthetic crash')):
            with self.assertRaises(OSError):
                self.receiver.receive(self.envelope, **self.identity)
        self.assertEqual(self.receiver.get(self.job['jobId'], 1)['deliveryState'], 'pending_queue_completion')
        self.assertEqual(self.queue.get(self.job['jobId'])['status'], 'running')
        self.restart()
        self.assertEqual(self.queue.get(self.job['jobId'])['status'], 'held')
        receipt = self.receiver.receive(self.envelope, **self.identity)
        self.outbox.acknowledge(self.outbox_id, receipt)
        self.assertEqual(self.queue.get(self.job['jobId'])['status'], 'failed')

    def test_crash_after_queue_complete_before_failure_state_update_replays(self):
        original = self.receiver._authenticate
        calls = 0
        def authenticate(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise OSError('synthetic crash after slot release')
            return original(*args, **kwargs)
        with patch.object(self.receiver, '_authenticate', authenticate):
            with self.assertRaises(OSError):
                self.receiver.receive(self.envelope, **self.identity)
        self.assertEqual(self.queue.get(self.job['jobId'])['status'], 'failed')
        self.restart()
        receipt = self.receiver.receive(self.envelope, **self.identity)
        self.assertEqual(self.receiver.receive(self.envelope, **self.identity), receipt)

    def test_wrong_claim_cannot_persist_or_read_exact_old_receipt(self):
        for changed in ({'claim_token': 'wrong'}, {'worker_instance_id': 'other'},
                        {'node_id': 'main'}, {'claim_request_id': 'other'}):
            with self.assertRaises(Conflict):
                self.receiver.receive(self.envelope, **{**self.identity, **changed})
        with self.assertRaises(Conflict):
            self.receiver.get(self.job['jobId'], 1)
        self.receiver.receive(self.envelope, **self.identity)
        with self.assertRaises(Conflict):
            self.receiver.receive(self.envelope, **{**self.identity, 'claim_token': 'wrong'})

    def test_operator_hold_and_missing_engine_attestation_do_not_release(self):
        missing_terminal = copy.deepcopy(self.envelope)
        missing_terminal['payload']['engineTerminal'] = False
        missing_terminal['payloadSha256'] = digest(missing_terminal['payload'])
        with self.assertRaises(ProtocolError):
            self.receiver.receive(missing_terminal, **self.identity)
        self.queue.operator(self.job['jobId'], 'hold', {'note': 'synthetic inspection'})
        with self.assertRaises(Conflict):
            self.receiver.receive(self.envelope, **self.identity)
        self.assertEqual(self.queue.get(self.job['jobId'])['status'], 'held')

    def test_changed_failure_or_stale_binding_cannot_replace_sealed_record(self):
        self.receiver.receive(self.envelope, **self.identity)
        changed = copy.deepcopy(self.failure)
        changed['errorCode'] = 'different_code'
        changed['failureSha256'] = failure_sha256(changed)
        with self.assertRaises(OutboxConflict):
            self.outbox.persist(changed, engine_terminal=True)
        with self.assertRaises(Conflict):
            self.receiver.receive(self.reseal_envelope(changed), **self.identity)
        changed['generation'] = 2
        changed['failureSha256'] = failure_sha256(changed)
        with self.assertRaises(ProtocolError):
            self.receiver.receive(self.reseal_envelope(changed), **self.identity)
        self.assertEqual(self.receiver.get(self.job['jobId'], 1)['failure'], self.failure)

    def test_worker_cancelled_record_is_preserved_without_success_manifest(self):
        failure = copy.deepcopy(self.failure)
        failure.update(terminalState='cancelled', errorCode='cancelled_by_worker')
        failure['failureSha256'] = failure_sha256(failure)
        envelope = self.reseal_envelope(failure)
        receipt = self.receiver.receive(envelope, **self.identity)
        self.assertEqual(receipt['terminalState'], 'cancelled')
        queued = self.queue.get(self.job['jobId'])
        self.assertEqual(queued['status'], 'failed')
        self.assertEqual(queued['result']['terminalState'], 'cancelled')
        self.assertEqual(self.receiver.get(self.job['jobId'], 1)['failure'], failure)
        self.assertIsNone(self.store.get(self.job['jobId'], private=True)['resultManifest'])

    def test_generic_or_wrong_ack_keeps_outbox_for_retry(self):
        receipt = self.receiver.receive(self.envelope, **self.identity)
        for invalid in ({'status': 200}, {**receipt, 'failureSha256': 'f'*64},
                        {**receipt, 'terminalState': 'cancelled'}, {**receipt, 'durable': 1}):
            with self.assertRaises(OutboxConflict):
                self.outbox.acknowledge(self.outbox_id, invalid)
        self.assertEqual(self.open_outbox().get(self.outbox_id)['state'], 'sending')
        self.outbox.acknowledge(self.outbox_id, receipt)
        self.open_outbox().acknowledge(self.outbox_id, receipt)


if __name__ == '__main__':
    unittest.main()
