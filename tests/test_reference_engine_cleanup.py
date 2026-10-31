import copy
import json
from unittest.mock import patch
import unittest
import uuid

from processing.node_gate import NodeGate, GateBusy
from processing.reference_engine_cleanup import EngineCacheCleanup
from processing.viewer_service import NativeEngine
from tests.test_reference_results import ResultsFixture


class EngineCacheCleanupFixture(ResultsFixture):
    def setUp(self):
        super().setUp()
        self.present, self.code, self.wrong_uuid = True, 40, False
        self.fail_remove, self.remove_calls, self.active_foreign = None, [], False
        self.native = NativeEngine('http://127.0.0.1:3001')
        self.worker.engine = self.native
        self.read_patch = patch.object(self.native, 'read', side_effect=self.read)
        self.action_patch = patch.object(self.native, 'action', side_effect=self.action)
        self.read_patch.start()
        self.action_patch.start()
        self.addCleanup(self.read_patch.stop)
        self.addCleanup(self.action_patch.stop)
        self.cleanup = EngineCacheCleanup(self.worker, path=self.root / 'engine-cleanup.sqlite')

    def read(self, route, *, missing=False):
        if route == '/info':
            return {'taskQueueCount': 0, 'engineVersion': self.worker.version}
        if route == '/task/list':
            result = [{'uuid': self.job['jobId']}] if self.present else []
            if self.active_foreign:
                result.append({'uuid': '00000000-0000-4000-8000-000000000099'})
            return result
        identifier = route.split('/')[2]
        if identifier != self.job['jobId']:
            return {'uuid': identifier, 'status': {'code': 20}}
        if not self.present:
            return None
        return {'uuid': str(uuid.uuid4()) if self.wrong_uuid else identifier, 'status': {'code': self.code}}

    def action(self, action, identifier):
        self.assertEqual(action, 'remove')
        self.assertEqual(identifier, self.job['jobId'])
        # Prove the physical OS gate is still locked throughout the mutation.
        with self.assertRaises(GateBusy):
            with NodeGate('physical-engine'):
                self.fail('cache removal released its physical gate early')
        self.remove_calls.append((action, identifier))
        if self.fail_remove == 'before':
            raise OSError('synthetic uncertain transport before outcome')
        self.present = False
        if self.fail_remove == 'after':
            raise OSError('synthetic lost response after exact removal')
        return {'success': True}

    def metadata_ack(self):
        row = self.worker._row(self.job['jobId'])
        completion = self.outbox.get(row['outbox_id'])
        result, payload = completion['payload']['resultManifest'], completion['payload']
        receipt = dict(type='durable-result-receipt', durable=True, receiptId='delivery-receipt', storageReceiptId='coordinator-storage',
            outboxId=row['outbox_id'], payloadSha256=completion['digest'], acceptedObjectIds=[o['id'] for o in result['objects']],
            **{key: payload[key] for key in ('jobId','generation','environment','inputManifestSha256','pipelineDigest','resultManifestSha256')})
        self.outbox.claim_next()
        self.outbox.acknowledge(row['outbox_id'], receipt)
        self.worker.acknowledge_delivery(self.job['jobId'])

    def viewer_acceptance(self, *, full=True):
        receipt = dict(schemaVersion=1, receiptId='viewer-receipt', storageReceiptId='viewer-storage', jobId=self.job['jobId'],
            generation=1, environment='staging', inputManifestSha256=self.job['manifestSha256'], pipelineDigest=self.job['pipelineDigest'],
            resultManifestSha256=self.metadata['resultManifestSha256'], acceptedObjectIds=[self.object['id']] if full else [],
            acceptedAt='2026-10-27T22:00:00Z', validationVersion='ltds-reference-v1')
        self.worker.apply_viewer_acceptance(self.job['jobId'], [receipt])
        return [receipt]

    def admitted(self):
        self.metadata_ack()
        return self.viewer_acceptance()


class EngineCacheCleanupTests(EngineCacheCleanupFixture, unittest.TestCase):
    def test_success_exact_owned_cache_removed_once_worker_files_retained(self):
        proof = self.admitted()
        before = self.archive.read_bytes()
        result = self.cleanup.remove(self.job['jobId'], viewer_receipts=proof)
        self.assertEqual(result['state'], 'removed')
        self.assertEqual(self.remove_calls, [('remove', self.job['jobId'])])
        self.assertEqual(self.archive.read_bytes(), before)
        self.assertEqual((self.directory / 'image-1.jpg').read_bytes(), b'abc')
        self.assertEqual(self.cleanup.remove(self.job['jobId'])['state'], 'removed')
        self.assertEqual(len(self.remove_calls), 1)

    def test_no_metadata_ack_or_full_viewer_ack_never_removes(self):
        proof = self.viewer_acceptance()
        self.assertEqual(self.cleanup.remove(self.job['jobId'], viewer_receipts=proof)['state'], 'held')
        self.metadata_ack()
        partial = copy.deepcopy(proof)
        partial[0]['acceptedObjectIds'] = []
        self.assertEqual(self.cleanup.remove(self.job['jobId'], viewer_receipts=partial)['reason'], 'full_viewer_acceptance_required')
        self.assertEqual(self.remove_calls, [])

    def test_relay_boolean_or_unapplied_receipt_cannot_authorize_removal(self):
        self.metadata_ack()
        self.assertEqual(self.cleanup.remove(self.job['jobId'], viewer_receipts={'fullyAccepted': True})['state'], 'held')
        proof = self.viewer_acceptance()
        proof[0]['receiptId'] = 'not-durable'
        self.assertEqual(self.cleanup.remove(self.job['jobId'], viewer_receipts=proof)['reason'], 'viewer_receipt_not_durable_locally')
        self.assertEqual(self.remove_calls, [])

    def test_changed_uuid_failed_or_cancelled_engine_is_never_removed(self):
        proof = self.admitted()
        self.wrong_uuid = True
        self.assertEqual(self.cleanup.remove(self.job['jobId'], viewer_receipts=proof)['state'], 'held')
        self.wrong_uuid = False
        for code in (30,50):
            self.code = code
            self.assertEqual(self.cleanup.remove(self.job['jobId'], viewer_receipts=proof)['reason'], 'successful_engine_task_required')
        self.assertEqual(self.remove_calls, [])

    def test_active_gate_or_foreign_active_task_blocks_cleanup(self):
        proof = self.admitted()
        with NodeGate('physical-engine') as gate:
            gate.reserve('cluster','http://127.0.0.1:8000',self.authority,1,'a'*32)
            gate.bind('a'*32, dict(id=str(uuid.uuid4()), workerId='worker',nodeId='cluster',authorityId=self.authority))
        self.assertEqual(self.cleanup.remove(self.job['jobId'], viewer_receipts=proof)['reason'], 'physical_gate_has_reserved_work')
        self.assertEqual(self.remove_calls, [])

    def test_foreign_active_engine_work_blocks_even_empty_gate(self):
        proof = self.admitted()
        self.active_foreign = True
        self.assertEqual(self.cleanup.remove(self.job['jobId'], viewer_receipts=proof)['reason'], 'engine_has_active_or_uncertain_work')
        self.assertEqual(self.remove_calls, [])

    def test_lost_remove_response_reconciles_absence_without_repeating_mutation(self):
        proof = self.admitted()
        self.fail_remove = 'after'
        self.assertEqual(self.cleanup.remove(self.job['jobId'], viewer_receipts=proof)['state'], 'uncertain')
        self.cleanup = EngineCacheCleanup(self.worker, path=self.root / 'engine-cleanup.sqlite')
        self.assertEqual(self.cleanup.remove(self.job['jobId'])['state'], 'removed')
        self.assertEqual(len(self.remove_calls), 1)

    def test_uncertain_remove_with_task_still_present_is_read_only_forever(self):
        proof = self.admitted()
        self.fail_remove = 'before'
        self.assertEqual(self.cleanup.remove(self.job['jobId'], viewer_receipts=proof)['state'], 'uncertain')
        self.fail_remove = None
        for _ in range(3):
            self.assertEqual(self.cleanup.remove(self.job['jobId'])['state'], 'uncertain')
        self.assertEqual(len(self.remove_calls), 1)

    def test_missing_task_without_persisted_intent_is_not_claimed_as_cleanup(self):
        proof = self.admitted()
        self.present = False
        self.assertEqual(self.cleanup.remove(self.job['jobId'], viewer_receipts=proof)['reason'], 'missing_engine_task_without_intent')
        with self.cleanup._db() as db:
            self.assertEqual(db.execute('SELECT phase FROM removals').fetchone()[0], 'waiting_policy')
        self.assertEqual(self.remove_calls, [])

    def test_intent_commit_crash_prevents_remove_and_pending_crash_never_retries(self):
        proof = self.admitted()
        with self.cleanup._db() as db:
            db.execute("CREATE TRIGGER reject_intent BEFORE UPDATE OF phase ON removals WHEN NEW.phase='remove_pending' BEGIN SELECT RAISE(ABORT,'synthetic crash'); END")
        self.assertEqual(self.cleanup.remove(self.job['jobId'], viewer_receipts=proof)['state'], 'held')
        self.assertEqual(self.remove_calls, [])
        with self.cleanup._db() as db:
            db.execute('DROP TRIGGER reject_intent')
        with patch.object(self.native, 'action', side_effect=SystemExit('synthetic crash after intent')):
            self.assertEqual(self.cleanup.remove(self.job['jobId'], viewer_receipts=proof)['state'], 'uncertain')
        self.assertEqual(self.cleanup.remove(self.job['jobId'])['state'], 'uncertain')
        self.assertEqual(self.remove_calls, [])

    def test_remove_requires_explicit_success_and_readonly_absence_confirmation(self):
        proof = self.admitted()
        with patch.object(self.native, 'action', return_value={}):
            self.assertEqual(self.cleanup.remove(self.job['jobId'], viewer_receipts=proof)['reason'], 'explicit_remove_ack_missing')
        self.assertEqual(self.cleanup.reconcile_one()['state'], 'uncertain')
        self.assertEqual(self.remove_calls, [])

    def test_retained_input_corruption_and_receipt_binding_change_block_removal(self):
        proof = self.admitted()
        (self.directory / 'image-1.jpg').write_bytes(b'bad')
        self.assertEqual(self.cleanup.remove(self.job['jobId'], viewer_receipts=proof)['reason'], 'retained_inputs_not_verified')
        (self.directory / 'image-1.jpg').write_bytes(b'abc')
        proof[0]['environment'] = 'production'
        self.assertEqual(self.cleanup.remove(self.job['jobId'], viewer_receipts=proof)['state'], 'held')
        self.assertEqual(self.remove_calls, [])

    def test_gate_generation_and_ownership_change_fail_closed(self):
        proof = self.admitted()
        with NodeGate('physical-engine') as gate:
            gate.handover('cluster','http://127.0.0.1:8000',self.authority,1,old_worker_stopped=True,note='synthetic maintenance')
        self.assertEqual(self.cleanup.remove(self.job['jobId'], viewer_receipts=proof)['reason'], 'physical_gate_binding_changed')
        self.assertEqual(self.remove_calls, [])


class EngineCacheCleanupGraceTests(EngineCacheCleanupFixture, unittest.TestCase):
    cleanup_grace_seconds = 10

    def test_grace_and_pins_defer_with_durable_proof_then_reconcile_without_viewer_poll(self):
        proof = self.admitted()
        now = self.ledger.clock()
        self.assertEqual(self.cleanup.remove(self.job['jobId'], viewer_receipts=proof)['reason'], 'acceptance_grace_not_elapsed')
        self.assertEqual(self.cleanup.pending(), [self.job['jobId']])
        self.ledger.clock = lambda: now + 11
        self.ledger.set_pin('reservation-1', self.object['id'], 'reader', kind='read', state='uncertain')
        self.assertEqual(self.cleanup.reconcile_one()['reason'], 'active_or_uncertain_result_pins')
        self.ledger.set_pin('reservation-1', self.object['id'], 'reader', kind='read', state='released')
        self.assertEqual(self.cleanup.reconcile_one()['state'], 'removed')
        self.assertEqual(self.cleanup.pending(), [])
        self.assertEqual(self.cleanup.reconcile_one(), {'state':'idle'})


if __name__ == '__main__':
    unittest.main()
