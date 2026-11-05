import copy
import json
from unittest.mock import patch
import unittest
import uuid

from processing.node_gate import NodeGate, GateBusy
from processing.reference_engine_cleanup import EngineCacheCleanup
from processing.reference_engine_worker import WorkerHeld
from processing.viewer_service import NativeEngine
from tests.test_reference_results import ResultsFixture
from tests.viewer_receipt_signer import SyntheticViewerSigner
from tests.viewer_v2_receipt_signer import SyntheticViewerCopyV2Signer


class EngineCacheCleanupFixture(ResultsFixture):
    def setUp(self):
        super().setUp()
        self.present, self.code, self.wrong_uuid = True, 40, False
        self.fail_remove, self.remove_calls, self.active_foreign = None, [], False
        self.native = NativeEngine('http://127.0.0.1:3001')
        self.worker.engine = self.native
        self.viewer_signer = SyntheticViewerSigner()
        self.worker.viewer_receipt_keys = self.viewer_signer.trusted_keys
        self.signed_result_manifest = json.loads(self.worker._row(self.job['jobId'])['result'])
        self.read_patch = patch.object(self.native, 'read', side_effect=self.read)
        self.action_patch = patch.object(self.native, 'action', side_effect=self.action)
        self.read_patch.start()
        self.action_patch.start()
        self.addCleanup(self.read_patch.stop)
        self.addCleanup(self.action_patch.stop)
        self.cleanup = EngineCacheCleanup(self.worker, path=self.root / 'engine-cleanup.sqlite',
            viewer_receipt_keys=self.viewer_signer.trusted_keys)

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
        envelope = self.viewer_signer.envelope(receipt, self.signed_result_manifest)
        self.worker.apply_viewer_acceptance(self.job['jobId'], [envelope])
        return [envelope]

    def admitted(self):
        self.metadata_ack()
        return self.viewer_acceptance()

    def prepare_v2_job(self):
        row = self.worker._row(self.job['jobId'])
        identity = json.loads(row['identity'])
        job = identity['job']
        job.update(sourceIntakeVersion=2, authorityId=self.authority)
        job['submission']['sourceIntakeVersion'] = 2
        digest = 'a' * 64
        submission_id = job['submission']['submissionId']
        binding = dict(coordinatorAuthorityId=self.authority,
            viewerPrincipalId='synthetic-viewer-principal', submissionId=submission_id,
            activationId=str(uuid.uuid4()), viewerTaskId=str(uuid.uuid4()),
            viewerAttemptId=submission_id, providerTaskId=job['jobId'],
            preparedEnvelopeSha256=digest, jobId=job['jobId'], generation=job['generation'],
            environment=job['environment'], inputManifestSha256=job['manifestSha256'],
            pipelineDigest=job['pipelineDigest'], sourceManifestSha256='b' * 64,
            derivedManifestSha256='c' * 64, sourceReceiptSha256='d' * 64, derivedReceiptSha256='e' * 64)
        job['sourceAcceptanceBinding'] = binding
        identity['sourceAcceptanceBinding'] = binding
        with self.worker._db() as db:
            db.execute('UPDATE executions SET identity=? WHERE job=?',
                (json.dumps(identity, sort_keys=True, separators=(',', ':')), self.job['jobId']))
        self.v2_signer = SyntheticViewerCopyV2Signer()
        self.worker.viewer_receipt_keys = self.v2_signer.trusted_keys
        self.cleanup = EngineCacheCleanup(self.worker, path=self.root / 'engine-cleanup-v2.sqlite',
            viewer_receipt_keys=self.v2_signer.trusted_keys)
        return binding

    def v2_viewer_acceptance(self, binding):
        result = json.loads(self.worker._row(self.job['jobId'])['result'])
        acceptance = dict(schemaVersion=1, receiptId='outer-v2-acceptance',
            storageReceiptId='outer-v2-storage', jobId=self.job['jobId'], generation=1,
            environment='staging', inputManifestSha256=self.job['manifestSha256'],
            pipelineDigest=self.job['pipelineDigest'], resultManifestSha256=result['resultManifestSha256'],
            acceptedObjectIds=[item['id'] for item in result['objects']],
            acceptedAt='2026-11-04T12:00:00.000Z', validationVersion='ltds-reference-v1')
        expected = dict(binding, resultManifestSha256=result['resultManifestSha256'])
        return result, self.v2_signer.envelope(expected, acceptance, result)


class EngineCacheCleanupTests(EngineCacheCleanupFixture, unittest.TestCase):
    def test_partial_source_metadata_never_uses_v1_cleanup_fallback(self):
        proof = self.admitted()
        original = json.loads(self.worker._row(self.job['jobId'])['identity'])
        digest = 'a' * 64
        submission_id = original['job']['submission']['submissionId']
        binding = dict(coordinatorAuthorityId=self.authority,
            viewerPrincipalId='synthetic-viewer-principal', submissionId=submission_id,
            activationId=str(uuid.uuid4()), viewerTaskId=str(uuid.uuid4()),
            viewerAttemptId=submission_id, providerTaskId=self.job['jobId'],
            preparedEnvelopeSha256=digest, jobId=self.job['jobId'], generation=1,
            environment='staging', inputManifestSha256=self.job['manifestSha256'],
            pipelineDigest=self.job['pipelineDigest'], sourceManifestSha256='b' * 64,
            derivedManifestSha256='c' * 64, sourceReceiptSha256='d' * 64, derivedReceiptSha256='e' * 64)
        cases = []
        top_null_nested_v2 = json.loads(json.dumps(original))
        top_null_nested_v2['job']['sourceIntakeVersion'] = None
        top_null_nested_v2['job']['submission']['sourceIntakeVersion'] = 2
        cases.append(('null-top-marker-with-v2-nested-marker', top_null_nested_v2))
        binding_without_markers = json.loads(json.dumps(original))
        binding_without_markers['job']['sourceAcceptanceBinding'] = binding
        binding_without_markers['sourceAcceptanceBinding'] = binding
        cases.append(('binding-without-markers', binding_without_markers))
        nested_null = json.loads(json.dumps(original))
        nested_null['job']['sourceIntakeVersion'] = 2
        nested_null['job']['submission']['sourceIntakeVersion'] = None
        nested_null['job']['sourceAcceptanceBinding'] = binding
        nested_null['sourceAcceptanceBinding'] = binding
        cases.append(('v2-top-with-null-nested-marker', nested_null))

        for label, identity in cases:
            with self.subTest(label=label):
                with self.worker._db() as db:
                    db.execute('UPDATE executions SET identity=? WHERE job=?',
                        (json.dumps(identity, sort_keys=True, separators=(',', ':')), self.job['jobId']))
                result = self.cleanup.remove(self.job['jobId'], viewer_receipts=proof)
                self.assertEqual((result['state'], result['reason']),
                    ('held', 'source_acceptance_binding_unavailable'))
                self.assertEqual(self.remove_calls, [])

    def test_v2_signed_copy_receipt_is_required_and_unlocks_only_exact_cleanup(self):
        binding = self.prepare_v2_job()
        self.metadata_ack()
        result, envelope = self.v2_viewer_acceptance(binding)
        acceptance = envelope['acceptance']
        with self.assertRaisesRegex(WorkerHeld, 'signed V2 Viewer accepted-copy receipt required'):
            self.worker.apply_viewer_acceptance(self.job['jobId'], [acceptance])
        self.assertEqual(self.cleanup.remove(self.job['jobId'], viewer_receipts=[acceptance])['state'], 'held')
        self.assertEqual(self.remove_calls, [])
        self.worker.apply_viewer_acceptance(self.job['jobId'], [envelope])
        outcome = self.cleanup.remove(self.job['jobId'], viewer_receipts=[envelope])
        self.assertEqual(outcome['state'], 'removed')
        self.assertEqual(self.remove_calls, [('remove', self.job['jobId'])])

    def test_v2_job_rejects_v1_signed_receipt_without_legacy_fallback(self):
        binding = self.prepare_v2_job()
        self.metadata_ack()
        result = json.loads(self.worker._row(self.job['jobId'])['result'])
        acceptance = dict(schemaVersion=1, receiptId='outer-v1-acceptance',
            storageReceiptId='outer-v1-storage', jobId=self.job['jobId'], generation=1,
            environment='staging', inputManifestSha256=self.job['manifestSha256'],
            pipelineDigest=self.job['pipelineDigest'], resultManifestSha256=result['resultManifestSha256'],
            acceptedObjectIds=[item['id'] for item in result['objects']],
            acceptedAt='2026-11-04T12:00:00Z', validationVersion='ltds-reference-v1')
        v1 = self.viewer_signer.envelope(acceptance, result)
        with self.assertRaisesRegex(WorkerHeld, 'Viewer accepted-copy signature or binding is invalid'):
            self.worker.apply_viewer_acceptance(self.job['jobId'], [v1])
        self.assertEqual(self.cleanup.remove(self.job['jobId'], viewer_receipts=[v1])['state'], 'held')
        self.assertEqual(self.remove_calls, [])

    def test_invalid_signature_or_unpinned_cleanup_never_removes_engine_cache(self):
        self.metadata_ack()
        valid = self.viewer_acceptance()
        envelope = copy.deepcopy(valid[0])
        envelope['viewerReceipt']['signature'] = 'A' * 86
        with self.assertRaises(WorkerHeld):
            self.worker.apply_viewer_acceptance(self.job['jobId'], [envelope])
        unpinned = EngineCacheCleanup(self.worker, path=self.root / 'unpinned-cleanup.sqlite')
        result = unpinned.remove(self.job['jobId'], viewer_receipts=valid)
        self.assertEqual(result['reason'], 'viewer_receipt_keys_not_configured')
        self.assertEqual(self.remove_calls, [])

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
        partial = [self.viewer_signer.envelope(
            dict(proof[0]['acceptance'], acceptedObjectIds=[]), self.signed_result_manifest)]
        self.assertEqual(self.cleanup.remove(self.job['jobId'], viewer_receipts=partial)['state'], 'held')
        self.assertEqual(self.remove_calls, [])

    def test_relay_boolean_or_unapplied_receipt_cannot_authorize_removal(self):
        self.metadata_ack()
        self.assertEqual(self.cleanup.remove(self.job['jobId'], viewer_receipts={'fullyAccepted': True})['state'], 'held')
        proof = self.viewer_acceptance()
        proof[0]['acceptance']['receiptId'] = 'not-durable'
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
        self.cleanup = EngineCacheCleanup(self.worker, path=self.root / 'engine-cleanup.sqlite',
            viewer_receipt_keys=self.viewer_signer.trusted_keys)
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
        proof[0]['acceptance']['environment'] = 'production'
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
