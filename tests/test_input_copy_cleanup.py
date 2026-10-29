"""Opt-in processing-input copy accounting, release proofs and exact cleanup."""
import copy
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
import unittest

from processing import reference_protocol as protocol
from processing.reference_failure import make_failure
from processing.scratch_policy import ScratchLedger, PolicyConflict, MAX_INPUT_COPIES

MANIFEST = 'a' * 64
PIPELINE = 'b' * 64


def sha(data):
    return hashlib.sha256(data).hexdigest()


class InputCopyLedgerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.now = 100
        self.free = 10**9
        self.ledger = self.open()
        self.ledger.enable_input_copy_accounting()

    def open(self, inputs=True):
        return ScratchLedger(self.root / 'private' / 'ledger.sqlite', self.root,
            capacity_probe=lambda _: self.free, clock=lambda: self.now,
            safety_floor_bytes=100, cleanup_grace_seconds=10, input_copy_accounting=inputs)

    def reserve(self, reservation='r1', job='j1', input_bytes=6, environment='production'):
        return self.ledger.reserve(job_id=job, generation=1, reservation_id=reservation,
            manifest_sha256=MANIFEST, input_bytes=input_bytes, output_budget=300,
            environment=environment, ttl_seconds=20, worker_ready=True)

    def closure(self, reservation='r1', items=(('i1', b'abc'), ('i2', b'def'))):
        return [dict(object_id=name, byte_size=len(data), sha256=sha(data),
                     relative_path='jobs/' + reservation + '/' + name + '.input') for name, data in items]

    def bind(self, reservation='r1', job='j1', items=(('i1', b'abc'), ('i2', b'def')), native=True, **kwargs):
        values = dict(job_id=job, generation=1, environment='production', manifest_sha256=MANIFEST,
            pipeline_sha256=PIPELINE, native_task_possible=native, objects=self.closure(reservation, items))
        values.update(kwargs)
        self.ledger.register_input_binding(reservation, **values)

    def fetch(self, reservation='r1', items=(('i1', b'abc'), ('i2', b'def'))):
        for name, data in items:
            path = self.root / 'jobs' / reservation / (name + '.input')
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            self.ledger.confirm_input_copy(reservation, name, byte_size=len(data), sha256=sha(data))

    def start(self, reservation='r1'):
        self.ledger.set_engine_state(reservation, 'engine_started')

    RESULT = (('odm-archive', 10, 'd'*64, 'all.zip'),)

    def manifest(self, job='j1', objects=RESULT):
        value = dict(schemaVersion=1, jobId=job, generation=1, environment='production', pipelineDigest=PIPELINE,
            engineVersions={'odm': 'test-1'}, objects=[dict(id=name, role='odm_archive', byteSize=size, sha256=digest)
                for name, size, digest, _ in objects])
        value['resultManifestSha256'] = protocol.digest(value)
        return value

    def viewer_receipt(self, reservation='r1', job='j1', objects=RESULT, ids=None, suffix=''):
        return dict(schemaVersion=1, receiptId='viewer-' + reservation + suffix, storageReceiptId='nas-' + reservation + suffix,
            jobId=job, generation=1, environment='production', inputManifestSha256=MANIFEST, pipelineDigest=PIPELINE,
            resultManifestSha256=self.manifest(job, objects)['resultManifestSha256'],
            acceptedObjectIds=list(ids if ids is not None else [o[0] for o in objects]),
            acceptedAt='2026-10-29T03:00:00Z', validationVersion=protocol.VALIDATION_VERSION)

    def accept(self, receipt, reservation='r1'):
        self.ledger.accept_receipt(reservation, receipt_id=receipt['receiptId'], storage_receipt_id=receipt['storageReceiptId'],
            job_id=receipt['jobId'], generation=1, manifest_sha256=MANIFEST, pipeline_sha256=PIPELINE,
            result_manifest_sha256=receipt['resultManifestSha256'], accepted_object_ids=receipt['acceptedObjectIds'])

    def succeed(self, reservation='r1', job='j1', accept=True, objects=RESULT):
        self.ledger.register_result(reservation, job_id=job, generation=1, manifest_sha256=MANIFEST,
            pipeline_sha256=PIPELINE, result_manifest_sha256=self.manifest(job, objects)['resultManifestSha256'],
            objects=[dict(object_id=name, bytes=size, sha256=digest, relative_path='jobs/' + reservation + '/' + path)
                     for name, size, digest, path in objects])
        self.ledger.set_engine_state(reservation, 'terminal', terminal_attested=True)
        if accept:
            self.accept(self.viewer_receipt(reservation, job, objects), reservation)

    def removal(self, reservation='r1', job='j1', items=(('i1', b'abc'), ('i2', b'def')), **changes):
        identity = dict(engineId='e', jobId=job, taskUUID=job, reservationId=reservation, generation=1,
            environment='production', inputManifestSha256=MANIFEST, pipelineDigest=PIPELINE,
            resultManifestSha256='c'*64, inputs=[dict(id=name, byteSize=len(data), sha256=sha(data),
                relativePath='jobs/' + reservation + '/' + name + '.input') for name, data in items])
        identity.update(changes)
        return dict(kind='removed', phase='removed', identity=identity)

    def success_proof(self, reservation='r1', job='j1', objects=RESULT, **kwargs):
        proof = dict(kind='success', resultManifest=self.manifest(job, objects),
            receipts=[self.viewer_receipt(reservation, job, objects)], nativeRemoval=self.removal(reservation, job))
        proof.update(kwargs)
        return proof

    def failure_proof(self, terminal_state='failed', **changes):
        failure = make_failure(job_id='j1', generation=1, environment='production',
            input_manifest_sha256=MANIFEST, pipeline_digest=PIPELINE, terminal_state=terminal_state,
            error_code='engine_failed')
        receipt = dict(schemaVersion=1, receiptId='terminal-viewer', storageReceiptId='terminal-storage',
            jobId='j1', generation=1, environment='production', inputManifestSha256=MANIFEST,
            pipelineDigest=PIPELINE, failureSha256=failure['failureSha256'],
            acceptedAt='2026-10-29T03:00:00Z', validationVersion=protocol.VALIDATION_VERSION)
        proof = dict(kind='terminal', receipt=receipt, failure=failure, nativeRemoval=self.removal())
        proof.update(changes)
        return proof

    def ready(self):
        """One fetched, succeeded, fully accepted and released two-copy job."""
        self.reserve(); self.bind(); self.fetch(); self.start(); self.succeed()
        self.assertEqual(self.ledger.release_input_copies('r1', proof=self.success_proof()), 2)

    def candidates(self):
        return [(c['reservation'], c['id']) for c in self.ledger.input_cleanup_candidates()]

    # ----- Marker and profile fencing -----------------------------------------

    def config(self):
        with closing(sqlite3.connect(self.ledger.path)) as db:
            return json.loads(db.execute("SELECT value FROM metadata WHERE key='config'").fetchone()[0])

    def test_default_profile_schema_is_unchanged_and_methods_refuse(self):
        other = Path(self.temp.name) / 'plain'
        other.mkdir()
        plain = ScratchLedger(other / 'ledger.sqlite', other, capacity_probe=lambda _: 10**9, clock=lambda: 1)
        with closing(sqlite3.connect(plain.path)) as db:
            tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            config = json.loads(db.execute("SELECT value FROM metadata WHERE key='config'").fetchone()[0])
        self.assertFalse(tables & {'input_bindings', 'input_copies', 'input_releases', 'input_pins', 'input_cleanup_intents'})
        self.assertNotIn('inputCopyAccountingVersion', config)
        with self.assertRaises(PolicyConflict):
            plain.enable_input_copy_accounting()
        opted = ScratchLedger(other / 'ledger.sqlite', other, capacity_probe=lambda _: 10**9, clock=lambda: 1,
                              input_copy_accounting=True)
        with self.assertRaises(PolicyConflict):
            opted.input_cleanup_candidates()  # Opt-in profile alone does not latch.
        with self.assertRaises(ValueError):
            ScratchLedger(other / 'ledger.sqlite', other, input_copy_accounting=1)

    def test_latched_marker_fences_default_profile_cold_and_live(self):
        self.assertEqual(self.config()['inputCopyAccountingVersion'], 1)
        with self.assertRaises(PolicyConflict):
            self.open(inputs=False)
        live = self.open()
        live.input_copy_accounting = False  # A compatible writer that never opted in.
        with self.assertRaises(PolicyConflict):
            live.reserve(job_id='j9', generation=1, reservation_id='r9', manifest_sha256=MANIFEST, input_bytes=6,
                output_budget=300, environment='production', ttl_seconds=20, worker_ready=True)
        self.ledger.enable_input_copy_accounting()  # Idempotent.
        self.assertEqual(self.config()['inputCopyAccountingVersion'], 1)

    def test_original_source_validator_rejects_marked_ledger(self):
        """The pre-change validate_accounting accepts only base or base+accountingVersion."""
        base = dict(root=str(self.ledger.root), floor=100, grace=10)
        accepted = {json.dumps(value, sort_keys=True, separators=(',', ':'))
                    for value in (base, dict(base, accountingVersion=1))}
        self.assertNotIn(json.dumps(self.config(), sort_keys=True, separators=(',', ':')), accepted)

    def test_control_latch_preserves_input_marker(self):
        self.reserve()
        self.ledger.record_allocated('r1', 6)
        self.ledger.prepare_control_charge('r1', charge_id='c1', scope_sha256='e'*64, parent_binding_sha256='f'*64,
            expected_allocated=6, allowance_bytes=10, maximum_scope_charges=1)
        config = self.config()
        self.assertEqual((config['accountingVersion'], config['inputCopyAccountingVersion']), (1, 1))
        self.open()  # Both markers validate together.

    def test_enable_after_control_latch_preserves_control_marker(self):
        other = Path(self.temp.name) / 'control'
        other.mkdir()
        ledger = ScratchLedger(other / 'l.sqlite', other, capacity_probe=lambda _: 10**9, clock=lambda: 1,
                               safety_floor_bytes=0, input_copy_accounting=True)
        ledger.reserve(job_id='j1', generation=1, reservation_id='r1', manifest_sha256=MANIFEST, input_bytes=6,
            output_budget=300, environment='production', ttl_seconds=20, worker_ready=True)
        ledger.record_allocated('r1', 6)
        ledger.prepare_control_charge('r1', charge_id='c1', scope_sha256='e'*64, parent_binding_sha256='f'*64,
            expected_allocated=6, allowance_bytes=10, maximum_scope_charges=1)
        ledger.enable_input_copy_accounting()
        with closing(sqlite3.connect(ledger.path)) as db:
            config = json.loads(db.execute("SELECT value FROM metadata WHERE key='config'").fetchone()[0])
        self.assertEqual((config['accountingVersion'], config['inputCopyAccountingVersion']), (1, 1))

    def test_changed_input_schema_is_refused(self):
        with closing(sqlite3.connect(self.ledger.path)) as db:
            db.execute('DROP TABLE input_pins')
            db.execute('CREATE TABLE input_pins (reservation TEXT)'); db.commit()
        with self.assertRaises(PolicyConflict):
            self.open()

    # ----- Binding and confirmation -------------------------------------------

    def test_binding_must_match_original_reservation(self):
        self.reserve()
        for changes in (dict(job_id='j2'), dict(generation=2), dict(environment='staging'),
                        dict(manifest_sha256='f'*64)):
            with self.assertRaises(PolicyConflict):
                self.bind(**changes)
        with self.assertRaises(PolicyConflict):
            self.bind(items=(('i1', b'abc'),))  # Closure bytes differ from the reservation.
        self.bind()
        self.bind()  # Exact replay.
        with self.assertRaises(PolicyConflict):
            self.bind(pipeline_sha256='e'*64)
        with self.assertRaises(PolicyConflict):
            self.bind(native=False)

    def test_binding_requires_live_reservation_and_owned_canonical_paths(self):
        with self.assertRaises(PolicyConflict):
            self.bind()
        self.reserve()
        for path in ('jobs/r2/i1.input', '../i1.input', 'jobs/r1/../r2/i1.input', 'i1.input'):
            objects = self.closure()
            objects[0]['relative_path'] = path
            with self.assertRaises(ValueError):
                self.bind(objects=objects)
        with self.assertRaises(ValueError):
            self.bind(objects=[dict(self.closure()[0], extra=1)])
        self.ledger.set_engine_state('r1', 'terminal', terminal_attested=True)
        with self.assertRaises(PolicyConflict):
            self.bind()

    def test_expired_reservation_cannot_be_bound(self):
        self.reserve()
        self.now += 20
        with self.assertRaises(PolicyConflict):
            self.bind()

    def test_aliases_span_inputs_and_results(self):
        self.reserve()
        with self.assertRaises(PolicyConflict):
            self.bind(items=(('i1', b'abc'), ('I1', b'def')))  # Case-insensitive identity.
        objects = self.closure()
        objects[1]['relative_path'] = objects[0]['relative_path']
        with self.assertRaises(PolicyConflict):
            self.bind(objects=objects)
        self.bind(); self.start()
        with self.assertRaises(PolicyConflict):
            self.ledger.register_result('r1', job_id='j1', generation=1, manifest_sha256=MANIFEST,
                pipeline_sha256=PIPELINE, result_manifest_sha256='c'*64,
                objects=[dict(object_id='odm-archive', bytes=3, sha256='d'*64, relative_path='jobs/r1/i1.input')])
        # The same ID in both namespaces is fine: they never share fences.
        self.ledger.register_result('r1', job_id='j1', generation=1, manifest_sha256=MANIFEST,
            pipeline_sha256=PIPELINE, result_manifest_sha256='c'*64,
            objects=[dict(object_id='i1', bytes=3, sha256='d'*64, relative_path='jobs/r1/all.zip')])

    def test_hardlinked_existing_file_is_an_alias(self):
        self.reserve(); self.reserve(reservation='r2', job='j2')
        self.bind(); self.fetch()
        other = self.root / 'jobs' / 'r2'
        other.mkdir(parents=True)
        (other / 'k1.input').hardlink_to(self.root / 'jobs' / 'r1' / 'i1.input')
        with self.assertRaises(PolicyConflict):
            self.bind(reservation='r2', job='j2', items=(('k1', b'abc'), ('k2', b'def')))

    def test_confirmation_is_exact_idempotent_and_live_only(self):
        self.reserve(); self.bind()
        with self.assertRaises(PolicyConflict):
            self.ledger.confirm_input_copy('r1', 'i1', byte_size=3, sha256=sha(b'xyz'))
        with self.assertRaises(PolicyConflict):
            self.ledger.confirm_input_copy('r1', 'missing', byte_size=3, sha256=sha(b'abc'))
        self.fetch(items=(('i1', b'abc'),))
        self.fetch(items=(('i1', b'abc'),))  # Fetch-before-checkpoint crash replay.
        self.ledger.set_engine_state('r1', 'terminal', terminal_attested=True)
        with self.assertRaises(PolicyConflict):
            self.ledger.confirm_input_copy('r1', 'i2', byte_size=3, sha256=sha(b'def'))
        self.ledger.confirm_input_copy('r1', 'i1', byte_size=3, sha256=sha(b'abc'))

    # ----- Release proofs ------------------------------------------------------

    def test_success_release_needs_terminal_full_acceptance_and_exact_receipts(self):
        self.reserve(); self.bind(); self.fetch(); self.start()
        with self.assertRaises(PolicyConflict):
            self.ledger.release_input_copies('r1', proof=self.success_proof())  # Not terminal.
        self.succeed(accept=False)
        with self.assertRaises(PolicyConflict):
            self.ledger.release_input_copies('r1', proof=self.success_proof())  # Receipt not durable locally.
        self.accept(self.viewer_receipt())
        with self.assertRaises(PolicyConflict):  # Receipt IDs alone are not proof.
            self.ledger.release_input_copies('r1', proof=dict(kind='success', receiptIds=['viewer-r1'],
                nativeRemoval=self.removal()))
        with self.assertRaises(PolicyConflict):
            self.ledger.release_input_copies('r1', proof=dict(self.success_proof(), accepted=True))
        self.assertEqual(self.ledger.release_input_copies('r1', proof=self.success_proof()), 2)
        self.assertEqual(self.ledger.release_input_copies('r1', proof=self.success_proof()), 2)

    def test_success_proof_is_revalidated_and_matched_to_durable_rows(self):
        self.reserve(); self.bind(); self.fetch(); self.start(); self.succeed()
        tampered = self.manifest(); tampered['objects'][0]['byteSize'] = 11  # Digest no longer matches.
        other = self.manifest(objects=(('odm-archive', 11, 'd'*64, 'all.zip'),))  # Valid, but not registered.
        changed = self.viewer_receipt(); changed['storageReceiptId'] = 'nas-other'
        foreign = self.viewer_receipt(); foreign['inputManifestSha256'] = 'f'*64
        extra = self.viewer_receipt(suffix='-extra')
        for proof in (self.success_proof(resultManifest=tampered), self.success_proof(resultManifest=other),
                      self.success_proof(receipts=[]), self.success_proof(receipts=[changed]),
                      self.success_proof(receipts=[foreign]), self.success_proof(receipts=[self.viewer_receipt(), extra]),
                      self.success_proof(receipts=[self.viewer_receipt(), self.viewer_receipt()]),
                      self.success_proof(receipts=[self.viewer_receipt(ids=[])])):
            with self.assertRaises(PolicyConflict):
                self.ledger.release_input_copies('r1', proof=proof)
        self.assertEqual(self.ledger.release_input_copies('r1', proof=self.success_proof()), 2)

    def test_partial_acceptance_is_not_a_release(self):
        objects = (('a', 1, 'd'*64, 'a.bin'), ('b', 1, 'e'*64, 'b.bin'))
        self.reserve(); self.bind(); self.fetch(); self.start(); self.succeed(accept=False, objects=objects)
        partial = self.viewer_receipt(objects=objects, ids=['a'])
        self.accept(partial)
        with self.assertRaises(PolicyConflict):
            self.ledger.release_input_copies('r1', proof=self.success_proof(objects=objects, receipts=[partial]))
        rest = self.viewer_receipt(objects=objects, ids=['b'], suffix='-b')
        self.accept(rest)
        self.assertEqual(self.ledger.release_input_copies('r1',
            proof=self.success_proof(objects=objects, receipts=[partial, rest])), 2)

    def test_native_removal_must_bind_exact_task_and_closure(self):
        self.reserve(); self.bind(); self.fetch(); self.start(); self.succeed()
        bad = (self.removal(jobId='j2'), self.removal(taskUUID='other'), self.removal(reservationId='r2'),
               self.removal(generation=2), self.removal(environment='staging'), self.removal(pipelineDigest='e'*64),
               self.removal(inputManifestSha256='e'*64), self.removal(items=(('i1', b'abc'),)),
               self.removal(items=(('i1', b'abc'), ('i2', b'xyz'))), dict(kind='removed', identity=None),
               dict(kind='uncertain', phase='removed', identity={}), dict(self.removal(), phase='remove_pending'),
               dict(kind='removed', identity=self.removal()['identity']), dict(kind='no_native_task'),
               dict(self.removal(), extra=1))
        for removal in bad:
            with self.assertRaises(PolicyConflict, msg=repr(removal)[:80]):
                self.ledger.release_input_copies('r1', proof=self.success_proof(nativeRemoval=removal))
        unordered = self.removal()
        unordered['identity']['inputs'].reverse()
        self.assertEqual(self.ledger.release_input_copies('r1', proof=self.success_proof(nativeRemoval=unordered)), 2)

    def test_no_native_task_release_is_refused_until_its_evidence_is_agreed(self):
        self.reserve(); self.bind(native=False); self.fetch(); self.start(); self.succeed()
        with self.assertRaises(PolicyConflict):
            self.ledger.release_input_copies('r1', proof=self.success_proof(nativeRemoval=dict(kind='no_native_task')))
        self.now += 10
        self.assertEqual(self.candidates(), [])

    def test_failed_and_cancelled_release_with_validated_terminal_acceptance(self):
        for state in ('failed', 'cancelled'):
            with self.subTest(state=state):
                self.setUp()
                self.reserve(); self.bind(); self.fetch(); self.start()
                self.ledger.set_engine_state('r1', 'terminal', terminal_attested=True)
                proof = self.failure_proof(state)
                broken = copy.deepcopy(proof); broken['receipt']['failureSha256'] = 'f'*64
                with self.assertRaises(PolicyConflict):
                    self.ledger.release_input_copies('r1', proof=broken)
                broken = copy.deepcopy(proof); broken['failure']['errorCode'] = 'other'
                with self.assertRaises(PolicyConflict):
                    self.ledger.release_input_copies('r1', proof=broken)  # Failure digest no longer matches.
                broken = copy.deepcopy(proof); broken['receipt']['jobId'] = 'j2'
                with self.assertRaises(PolicyConflict):
                    self.ledger.release_input_copies('r1', proof=broken)
                self.assertEqual(self.ledger.release_input_copies('r1', proof=proof), 2)

    def test_changed_release_proof_conflicts_after_storage(self):
        self.ready()
        other = self.success_proof(nativeRemoval=self.removal(engineId='changed'))
        with self.assertRaises(PolicyConflict):
            self.ledger.release_input_copies('r1', proof=other)

    # ----- Eligibility, pins and fences ---------------------------------------

    def test_release_then_grace_then_candidates(self):
        self.ready()
        self.assertEqual(self.candidates(), [])
        self.now += 10
        self.assertEqual(self.candidates(), [('r1', 'i1'), ('r1', 'i2')])

    def test_unfetched_copy_is_never_a_candidate(self):
        self.reserve(); self.bind(); self.fetch(items=(('i1', b'abc'),)); self.start(); self.succeed()
        self.ledger.release_input_copies('r1', proof=self.success_proof())
        self.now += 10
        self.assertEqual(self.candidates(), [('r1', 'i1')])

    def test_active_and_uncertain_pins_retain_and_refuse_after_intent(self):
        self.ready(); self.now += 10
        for state in ('active', 'uncertain'):
            self.ledger.set_input_pin('r1', 'i1', 'stage-two', kind='downstream', state=state)
            self.assertEqual(self.candidates(), [('r1', 'i2')])
        with self.assertRaises(PolicyConflict):
            self.ledger.set_input_pin('r1', 'i1', 'stage-two', kind='read', state='released')  # Kind is fixed.
        self.ledger.set_input_pin('r1', 'i1', 'stage-two', kind='downstream', state='released')
        self.assertEqual(self.candidates(), [('r1', 'i1'), ('r1', 'i2')])
        self.ledger.begin_input_cleanup('r1', 'i1', generation=1, sha256=sha(b'abc'))
        with self.assertRaises(PolicyConflict):
            self.ledger.set_input_pin('r1', 'i1', 'late-reader', kind='read', state='active')
        with self.assertRaises(ValueError):
            self.ledger.set_input_pin('r1', 'i2', 'x', kind='other', state='active')

    def test_fence_recovery_and_confirmation_are_exact(self):
        self.ready(); self.now += 10
        with self.assertRaises(PolicyConflict):
            self.ledger.begin_input_cleanup('r1', 'i1', generation=1, sha256=sha(b'xyz'))
        intent = self.ledger.begin_input_cleanup('r1', 'i1', generation=1, sha256=sha(b'abc'))
        with self.assertRaises(PolicyConflict):
            self.ledger.begin_input_cleanup('r1', 'i1', generation=1, sha256=sha(b'abc'))
        self.assertEqual(self.candidates(), [('r1', 'i2')])
        self.now += 60
        recovered = self.ledger.input_cleanup_intent('r1', 'i1')
        self.assertEqual((recovered['state'], recovered['intent_token'], recovered['path']),
                         ('uncertain', intent['intent_token'], 'jobs/r1/i1.input'))
        for token, attested in (('0'*64, True), (intent['intent_token'], False)):
            with self.assertRaises(PolicyConflict):
                self.ledger.mark_input_cleaned('r1', 'i1', generation=1, sha256=sha(b'abc'),
                    intent_token=token, executor_attested=attested)
        self.ledger.mark_input_cleaned('r1', 'i1', generation=1, sha256=sha(b'abc'),
            intent_token=intent['intent_token'], executor_attested=True)
        self.ledger.mark_input_cleaned('r1', 'i1', generation=1, sha256=sha(b'abc'),
            intent_token=intent['intent_token'], executor_attested=True)
        with self.assertRaises(PolicyConflict):
            self.ledger.set_input_pin('r1', 'i1', 'x', kind='read', state='active')
        with closing(sqlite3.connect(self.ledger.path)) as db:
            self.assertEqual(db.execute("SELECT cleaned FROM input_copies WHERE id='i1'").fetchone()[0], 170)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM cleanup_intents").fetchone()[0], 0)

    def test_result_cleanup_fences_are_unaffected(self):
        self.ready(); self.now += 10
        self.assertEqual([c['id'] for c in self.ledger.cleanup_candidates()], ['odm-archive'])
        with self.assertRaises(PolicyConflict):
            self.ledger.begin_cleanup('r1', 'i1', generation=1, sha256=sha(b'abc'))

    def test_hard_count_and_byte_limits_with_oversized_report(self):
        self.reserve(input_bytes=7); self.bind(items=(('i1', b'a'*5), ('i2', b'b'*2)))
        self.fetch(items=(('i1', b'a'*5), ('i2', b'b'*2))); self.start(); self.succeed()
        self.ledger.release_input_copies('r1', proof=self.success_proof(
            nativeRemoval=self.removal(items=(('i1', b'a'*5), ('i2', b'b'*2)))))
        self.now += 10
        def ids(**limits):
            return [c['id'] for c in self.ledger.input_cleanup_candidates(**limits)]
        self.assertEqual(ids(max_count=10, max_bytes=1), [])  # Never silently exceeded.
        self.assertEqual([c['id'] for c in self.ledger.oversized_input_copies(max_bytes=1)], ['i1', 'i2'])
        self.assertEqual(ids(max_count=10, max_bytes=4), ['i2'])
        self.assertEqual([c['id'] for c in self.ledger.oversized_input_copies(max_bytes=4)], ['i1'])
        self.assertEqual(ids(max_count=10, max_bytes=6), ['i1'])
        self.assertEqual(ids(max_count=10, max_bytes=7), ['i1', 'i2'])
        self.assertEqual(ids(max_count=1, max_bytes=7), ['i1'])
        for limits in (dict(max_count=0, max_bytes=7), dict(max_count=1, max_bytes=-1)):
            with self.assertRaises(ValueError):
                self.ledger.input_cleanup_candidates(**limits)

    def test_oldest_release_first_across_jobs(self):
        self.reserve(); self.reserve(reservation='r2', job='j2')
        for reservation, job in (('r2', 'j2'), ('r1', 'j1')):
            self.bind(reservation=reservation, job=job); self.fetch(reservation=reservation)
            self.start(reservation); self.succeed(reservation=reservation, job=job)
        self.ledger.release_input_copies('r2', proof=self.success_proof('r2', 'j2'))
        self.now += 1
        self.ledger.release_input_copies('r1', proof=self.success_proof())
        self.now += 10
        self.assertEqual(self.candidates(), [('r2', 'i1'), ('r2', 'i2'), ('r1', 'i1'), ('r1', 'i2')])

    def test_capacity_recovers_only_after_input_copies_are_cleaned(self):
        """Reproducer: finished jobs used to hold their input bytes forever."""
        self.free = 1000
        self.reserve(input_bytes=500)
        data = b'x' * 500
        self.bind(items=(('i1', data),)); self.fetch(items=(('i1', data),))
        self.ledger.record_allocated('r1', 500); self.free -= 500
        self.start(); self.succeed()
        self.ledger.release_input_copies('r1', proof=self.success_proof(nativeRemoval=self.removal(items=(('i1', data),))))
        self.now += 10
        with self.assertRaises(PolicyConflict):
            self.reserve(reservation='r2', job='j2', input_bytes=500)
        intent = self.ledger.begin_input_cleanup('r1', 'i1', generation=1, sha256=sha(data))
        (self.root / 'jobs/r1/i1.input').unlink(); self.free += 500
        self.ledger.mark_input_cleaned('r1', 'i1', generation=1, sha256=sha(data),
            intent_token=intent['intent_token'], executor_attested=True)
        self.reserve(reservation='r2', job='j2', input_bytes=500)

    def test_ten_thousand_copy_closure_is_bounded(self):
        count = MAX_INPUT_COPIES
        items = [('p%05d' % index, b'z') for index in range(count)]
        self.reserve(input_bytes=count)
        started = time.monotonic()
        objects = [dict(object_id=name, byte_size=1, sha256=sha(b'z'), relative_path='jobs/r1/' + name + '.input')
                   for name, _ in items]
        self.ledger.register_input_binding('r1', job_id='j1', generation=1, environment='production',
            manifest_sha256=MANIFEST, pipeline_sha256=PIPELINE, native_task_possible=True, objects=objects)
        self.assertLess(time.monotonic() - started, 30)
        with self.assertRaises(ValueError):
            self.ledger.register_input_binding('r2', job_id='j2', generation=1, environment='production',
                manifest_sha256=MANIFEST, pipeline_sha256=PIPELINE, native_task_possible=True,
                objects=objects + [dict(objects[0], object_id='extra', relative_path='jobs/r2/extra.input')])
        with closing(sqlite3.connect(self.ledger.path)) as db:
            db.execute("UPDATE input_copies SET fetched=1")
            db.commit()
        self.start(); self.succeed()
        self.ledger.release_input_copies('r1', proof=self.success_proof(nativeRemoval=self.removal(items=items)))
        self.now += 10
        started = time.monotonic()
        batch = self.ledger.input_cleanup_candidates(max_count=100, max_bytes=1024**3)
        self.assertEqual(len(batch), 100)
        self.assertEqual(len(self.ledger.input_cleanup_candidates(max_count=10000, max_bytes=250)), 250)
        self.assertLess(time.monotonic() - started, 5)


def closing(db):
    from contextlib import closing as close
    return close(db)


@unittest.skipUnless(sys.platform.startswith('linux'), 'executor requires Linux descriptor and fsync semantics')
class InputCopyExecutorTests(unittest.TestCase):
    setUp = InputCopyLedgerTests.setUp
    open = InputCopyLedgerTests.open
    reserve = InputCopyLedgerTests.reserve
    closure = InputCopyLedgerTests.closure
    bind = InputCopyLedgerTests.bind
    fetch = InputCopyLedgerTests.fetch
    start = InputCopyLedgerTests.start
    succeed = InputCopyLedgerTests.succeed
    removal = InputCopyLedgerTests.removal
    success_proof = InputCopyLedgerTests.success_proof
    RESULT = InputCopyLedgerTests.RESULT
    manifest = InputCopyLedgerTests.manifest
    viewer_receipt = InputCopyLedgerTests.viewer_receipt
    accept = InputCopyLedgerTests.accept
    ready = InputCopyLedgerTests.ready

    def executor(self):
        from processing.scratch_cleanup import ScratchCleanupExecutor
        self.root.chmod(0o700)
        return ScratchCleanupExecutor(self.ledger)

    def run_one(self, item, **kwargs):
        return self.executor().execute_input_copy(item['reservation'], item['id'], generation=item['generation'],
            sha256=item['sha'], byte_size=item['bytes'], relative_path=item['path'], **kwargs)

    def test_removes_only_released_exact_copies_and_keeps_results_and_other_jobs(self):
        self.ready()
        self.reserve(reservation='r2', job='j2'); self.bind(reservation='r2', job='j2'); self.fetch(reservation='r2')
        (self.root / 'jobs/r1/all.zip').write_bytes(b'archive')
        self.now += 10
        for item in self.ledger.input_cleanup_candidates():
            self.assertEqual(self.run_one(item)['state'], 'cleaned')
        self.assertEqual(sorted(p.name for p in (self.root / 'jobs/r1').iterdir()), ['all.zip'])
        self.assertEqual((self.root / 'jobs/r2/i1.input').read_bytes(), b'abc')
        self.assertEqual(self.ledger.input_cleanup_candidates(), [])

    def test_replaced_copy_is_retained_with_its_fence(self):
        from processing.scratch_cleanup import CleanupRefused
        self.ready(); self.now += 10
        (self.root / 'jobs/r1/i1.input').write_bytes(b'abd')
        item = self.ledger.input_cleanup_candidates()[0]
        with self.assertRaises(CleanupRefused):
            self.run_one(item)
        self.assertEqual((self.root / 'jobs/r1/i1.input').read_bytes(), b'abd')
        self.assertEqual(self.ledger.input_cleanup_intent('r1', 'i1')['state'], 'cleanup_pending')

    def test_restart_after_unlink_before_ledger_confirmation_reconciles(self):
        self.ready(); self.now += 10
        item = self.ledger.input_cleanup_candidates()[0]
        original = self.ledger.mark_input_cleaned
        def lost(*args, **kwargs):
            raise RuntimeError('crash before confirmation')
        self.ledger.mark_input_cleaned = lost
        with self.assertRaises(RuntimeError):
            self.run_one(item)
        self.ledger.mark_input_cleaned = original
        self.assertFalse((self.root / 'jobs/r1/i1.input').exists())
        token = self.ledger.input_cleanup_intent('r1', 'i1')['intent_token']
        self.assertEqual(self.run_one(item, intent_token=token)['state'], 'cleaned')
        self.assertEqual([c['id'] for c in self.ledger.input_cleanup_candidates()], ['i2'])

    def test_missing_copy_without_journal_is_never_cleanup(self):
        from processing.scratch_cleanup import CleanupRefused
        self.ready(); self.now += 10
        (self.root / 'jobs/r1/i1.input').unlink()
        with self.assertRaises((CleanupRefused, FileNotFoundError)):
            self.run_one(self.ledger.input_cleanup_candidates()[0])
        with closing(sqlite3.connect(self.ledger.path)) as db:
            self.assertIsNone(db.execute("SELECT cleaned FROM input_copies WHERE id='i1'").fetchone()[0])

    def test_input_and_result_with_same_id_use_separate_journal_bindings(self):
        objects = (('i1', 3, sha(b'zip'), 'all.zip'),)
        self.reserve(); self.bind(); self.fetch(); self.start()
        self.succeed(objects=objects)
        (self.root / 'jobs/r1/all.zip').write_bytes(b'zip')
        self.ledger.release_input_copies('r1', proof=self.success_proof(objects=objects))
        self.now += 10
        result = self.ledger.cleanup_candidates()[0]
        self.executor().execute('r1', 'i1', generation=1, sha256=result['sha'], byte_size=3, relative_path='jobs/r1/all.zip')
        self.run_one([c for c in self.ledger.input_cleanup_candidates() if c['id'] == 'i1'][0])
        self.assertFalse((self.root / 'jobs/r1/all.zip').exists())
        self.assertFalse((self.root / 'jobs/r1/i1.input').exists())
        self.assertEqual((self.root / 'jobs/r1/i2.input').read_bytes(), b'def')
        with closing(sqlite3.connect(self.root / '.cleanup-journal' / 'journal.sqlite')) as db:
            rows = [(row[0], json.loads(row[1])) for row in db.execute('SELECT token,binding FROM deletions')]
        self.assertEqual(sorted((key.startswith('input-copy:'), binding.get('namespace', '')) for key, binding in rows),
                         [(False, ''), (True, 'input-copy')])


if __name__ == '__main__':
    unittest.main()
