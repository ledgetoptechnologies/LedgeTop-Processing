import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import uuid

from processing.prototype import Queue
from processing.reference_store import ReferenceStore
from processing.reference_protocol import validate_submission, ProtocolError, canonical_json
from processing.reference_failure import FailureReceiver, FailureOutbox, make_failure
from processing.reference_terminal import TerminalService
from processing.maintenance import Conflict


class ReferenceTerminalTests(unittest.TestCase):
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
        self.receiver = FailureReceiver(self.store)
        self.terminal = TerminalService(self.store)
        self.failure = make_failure(job_id=self.job['jobId'], generation=1,
            environment='staging', input_manifest_sha256=self.job['manifestSha256'],
            pipeline_digest=self.job['pipelineDigest'], terminal_state='failed', error_code='engine_failed')
        outbox = FailureOutbox(self.root / 'worker' / 'failure.sqlite')
        outbox.persist(self.failure, engine_terminal=True)
        delivery = outbox.claim_next()
        self.envelope = {key: delivery[key] for key in ('outboxId', 'payloadSha256', 'payload')}

    def tearDown(self):
        self.queue.close()
        self.temp.cleanup()

    def confirm(self):
        self.receiver.receive(self.envelope, **self.identity)

    def receipt(self, **updates):
        result = {key: self.failure[key] for key in ('jobId', 'generation', 'environment',
            'inputManifestSha256', 'pipelineDigest', 'failureSha256')}
        result.update(schemaVersion=1, receiptId='terminal-1', storageReceiptId='viewer-terminal-1',
            acceptedAt='2026-10-28T03:00:00Z', validationVersion='ltds-reference-v1')
        result.update(updates)
        return result

    def accept(self, receipt=None, environment='staging'):
        return self.terminal.accept(self.job['jobId'], receipt or self.receipt(), environment)

    def restart(self):
        self.queue.close()
        self.queue = Queue(self.root / 'coordinator', reference_enabled=True)
        self.store = ReferenceStore(self.queue)
        self.receiver = FailureReceiver(self.store)
        self.terminal = TerminalService(self.store)

    def test_exact_failure_outcome_and_durable_receipt_replay(self):
        self.confirm()
        self.assertEqual(self.terminal.outcome(self.job['jobId'], 'staging'), self.failure)
        detached = self.terminal.outcome(self.job['jobId'], 'staging')
        detached['errorCode'] = 'changed'
        first = self.accept()
        self.assertTrue(first['created'])
        self.assertTrue(first['terminalAccepted'])
        self.assertEqual(first['job']['phase'], 'completed')
        self.restart()  # Initial successful response was lost.
        replay = self.accept()
        self.assertFalse(replay['created'])
        self.assertTrue(replay['terminalAccepted'])
        self.assertEqual(self.terminal.outcome(self.job['jobId'], 'staging'), self.failure)
        self.assertIsNone(self.store.get(self.job['jobId'], private=True)['resultManifest'])

    def test_changed_or_stale_receipt_conflicts_without_replacing_first(self):
        self.confirm()
        self.accept()
        for changed in ({'acceptedAt': '2026-10-28T03:01:00Z'}, {'receiptId': 'terminal-2'},
                        {'storageReceiptId': 'other-storage'}, {'failureSha256': 'f'*64},
                        {'generation': 2}, {'inputManifestSha256': 'f'*64}):
            with self.subTest(change=changed), self.assertRaises(Conflict):
                self.accept(self.receipt(**changed))
        self.assertFalse(self.accept()['created'])

    def test_schema_rejects_file_claims_exception_text_and_unzoned_time(self):
        self.confirm()
        for changed in ({'acceptedObjectIds': []}, {'message': 'sensitive exception'},
                        {'schemaVersion': True}, {'acceptedAt': '2026-10-28T03:00:00'},
                        {'validationVersion': 'unknown'}, {'generation': True}):
            with self.subTest(change=changed), self.assertRaises(ProtocolError):
                self.accept(self.receipt(**changed))
        self.assertNotEqual(self.store.get(self.job['jobId'])['phase'], 'completed')

    def test_connection_loss_and_operator_cancel_do_not_create_failure_outcome(self):
        self.restart()  # Running claim becomes held because coordinator restarted.
        with self.assertRaises(Conflict):
            self.terminal.outcome(self.job['jobId'], 'staging')
        with self.assertRaises(Conflict):
            self.accept()
        self.queue.operator(self.job['jobId'], 'resolve', {'note': 'synthetic inspection', 'workerStopped': True})
        self.assertEqual(self.queue.get(self.job['jobId'])['status'], 'failed')
        with self.assertRaises(Conflict):
            self.terminal.outcome(self.job['jobId'], 'staging')

    def test_failure_persistence_before_queue_complete_is_not_ready(self):
        with patch.object(self.queue, 'complete', side_effect=OSError('synthetic connection loss')):
            with self.assertRaises(OSError):
                self.confirm()
        with self.assertRaises(Conflict):
            self.terminal.outcome(self.job['jobId'], 'staging')
        with self.assertRaises(Conflict):
            self.accept()

    def test_preengine_cancellation_without_worker_terminal_record_is_not_failure_outcome(self):
        raw = copy.deepcopy(self.fixture['submission'])
        raw['submissionId'] = str(uuid.uuid4())
        other = self.store.submit(validate_submission(raw, environment='staging',
            capability_fingerprint=raw['capabilityFingerprint']))[0]
        self.store.update_phase(other['jobId'], 'cancelled')
        self.assertEqual(self.queue.get(other['jobId'])['status'], 'cancelled')
        with self.assertRaises(Conflict):
            self.terminal.outcome(other['jobId'], 'staging')

    def test_confirmed_worker_cancellation_accepts_metadata_without_output_claims(self):
        self.failure = make_failure(job_id=self.job['jobId'], generation=1,
            environment='staging', input_manifest_sha256=self.job['manifestSha256'],
            pipeline_digest=self.job['pipelineDigest'], terminal_state='cancelled', error_code='cancelled_by_worker')
        outbox = FailureOutbox(self.root / 'worker' / 'cancelled.sqlite')
        outbox.persist(self.failure, engine_terminal=True)
        delivery = outbox.claim_next()
        self.receiver.receive({key: delivery[key] for key in ('outboxId', 'payloadSha256', 'payload')}, **self.identity)
        self.assertEqual(self.terminal.outcome(self.job['jobId'], 'staging')['terminalState'], 'cancelled')
        self.assertTrue(self.accept()['terminalAccepted'])
        self.assertIsNone(self.store.get(self.job['jobId'], private=True)['resultManifest'])

    def test_queue_failed_before_failure_delivery_state_commit_is_not_ready(self):
        original = self.receiver._authenticate
        calls = 0
        def crash(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise OSError('synthetic post-completion crash')
            return original(*args, **kwargs)
        with patch.object(self.receiver, '_authenticate', crash):
            with self.assertRaises(OSError):
                self.confirm()
        self.assertEqual(self.queue.get(self.job['jobId'])['status'], 'failed')
        with self.assertRaises(Conflict):
            self.terminal.outcome(self.job['jobId'], 'staging')
        self.restart()
        self.confirm()
        self.assertEqual(self.terminal.outcome(self.job['jobId'], 'staging'), self.failure)

    def test_wrong_environment_is_rejected_for_outcome_and_acceptance(self):
        self.confirm()
        with self.assertRaises(Conflict):
            self.terminal.outcome(self.job['jobId'], 'production')
        with self.assertRaises(Conflict):
            self.accept(environment='production')
        with self.assertRaises(Conflict):
            self.accept(self.receipt(environment='production'))

    def test_queue_terminal_summary_must_match_exact_failure(self):
        self.confirm()
        with self.queue._db() as db:
            db.execute("UPDATE jobs SET result=? WHERE id=?", ('{"failureSha256":"wrong"}', self.job['jobId']))
        with self.assertRaises(Conflict):
            self.terminal.outcome(self.job['jobId'], 'staging')

    def test_success_receipt_ids_cannot_be_reused_in_either_role(self):
        self.confirm()
        success = copy.deepcopy(self.fixture['acceptance'])
        success.update(receiptId='success-receipt', storageReceiptId='success-storage')
        with self.queue._db() as db:
            db.execute("INSERT INTO reference_acceptances(receipt_id,job_id,digest,receipt_json) VALUES(?,?,?,?)",
                (success['receiptId'], self.job['jobId'], 'a'*64, canonical_json(success).decode()))
        for changed in ({'receiptId': 'success-receipt'}, {'receiptId': 'success-storage'},
                        {'storageReceiptId': 'success-receipt'}, {'storageReceiptId': 'success-storage'}):
            with self.subTest(change=changed), self.assertRaises(Conflict):
                self.accept(self.receipt(**changed))

    def test_terminal_ids_cannot_be_rebound_to_another_job(self):
        self.confirm()
        self.accept()
        raw = copy.deepcopy(self.fixture['submission'])
        raw['submissionId'] = str(uuid.uuid4())
        other = self.store.submit(validate_submission(raw, environment='staging',
            capability_fingerprint=raw['capabilityFingerprint']))[0]
        other_identity = {**self.identity, 'claim_request_id': 'claim-2'}
        del other_identity['claim_token']
        claim = self.store.claim(other['jobId'], **other_identity)
        other_identity['claim_token'] = claim['claimToken']
        failure = make_failure(job_id=other['jobId'], generation=1, environment='staging',
            input_manifest_sha256=other['manifestSha256'], pipeline_digest=other['pipelineDigest'],
            terminal_state='failed', error_code='engine_failed')
        outbox = FailureOutbox(self.root / 'worker' / 'other.sqlite')
        outbox.persist(failure, engine_terminal=True)
        delivery = outbox.claim_next()
        self.receiver.receive({key: delivery[key] for key in ('outboxId', 'payloadSha256', 'payload')}, **other_identity)
        receipt = self.receipt()
        receipt.update(jobId=other['jobId'], failureSha256=failure['failureSha256'])
        for receipt_id, storage_id in [('terminal-1', 'new-storage'), ('new-id', 'viewer-terminal-1'),
                                       ('viewer-terminal-1', 'new-storage'), ('new-id', 'terminal-1')]:
            with self.subTest(ids=(receipt_id, storage_id)), self.assertRaises(Conflict):
                self.terminal.accept(other['jobId'], {**receipt, 'receiptId': receipt_id,
                    'storageReceiptId': storage_id}, 'staging')

    def test_receipt_and_phase_rollback_together_if_response_preparation_crashes(self):
        self.confirm()
        with patch.object(self.store, '_public', side_effect=OSError('synthetic precommit crash')):
            with self.assertRaises(OSError):
                self.accept()
        self.assertNotEqual(self.store.get(self.job['jobId'])['phase'], 'completed')
        self.restart()
        self.assertTrue(self.accept()['created'])


if __name__ == '__main__':
    unittest.main()
