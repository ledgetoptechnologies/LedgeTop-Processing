"""Opt-in processing-input copy accounting, authority-resolved release and exact cleanup."""
from contextlib import closing
import copy
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
import unittest

from processing.scratch_policy import (InputReleaseAuthority, MAX_INPUT_COPIES, MAX_RELEASE_RECORD_BYTES,
                                       PolicyConflict, ScratchLedger)

MANIFEST = 'a' * 64
PIPELINE = 'b' * 64
RESULT_SHA = 'c' * 64


def sha(data):
    return hashlib.sha256(data).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


class FakeAuthority(InputReleaseAuthority):
    """Test double: confirms exactly the records a test has registered as resolvable."""

    authority_id = 'test-authority'

    def __init__(self):
        self.records = {}

    def allow(self, record):
        self.records[record['releaseId']] = copy.deepcopy(record)

    def resolve_input_release(self, record):
        return copy.deepcopy(self.records[record['releaseId']])


class InputCopyLedgerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.now = 100
        self.free = 10**9
        self.authority = FakeAuthority()
        self.ledger = self.open()
        self.ledger.enable_input_copy_accounting()

    def open(self, inputs=True, authority=True):
        return ScratchLedger(self.root / 'private' / 'ledger.sqlite', self.root,
            capacity_probe=lambda _: self.free, clock=lambda: self.now,
            safety_floor_bytes=100, cleanup_grace_seconds=10, input_copy_accounting=inputs,
            release_authority=self.authority if inputs and authority else None)

    def reserve(self, reservation='r1', job='j1', input_bytes=6):
        return self.ledger.reserve(job_id=job, generation=1, reservation_id=reservation,
            manifest_sha256=MANIFEST, input_bytes=input_bytes, output_budget=300,
            environment='production', ttl_seconds=20, worker_ready=True)

    def closure(self, reservation='r1', items=(('i1', b'abc'), ('i2', b'def'))):
        return [dict(object_id=name, byte_size=len(data), sha256=sha(data),
                     relative_path='jobs/' + reservation + '/' + name + '.input') for name, data in items]

    def bind(self, reservation='r1', job='j1', items=(('i1', b'abc'), ('i2', b'def')), **kwargs):
        values = dict(job_id=job, generation=1, environment='production', manifest_sha256=MANIFEST,
            pipeline_sha256=PIPELINE, objects=self.closure(reservation, items))
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

    def succeed(self, reservation='r1', job='j1', accept=True):
        self.ledger.register_result(reservation, job_id=job, generation=1, manifest_sha256=MANIFEST,
            pipeline_sha256=PIPELINE, result_manifest_sha256=RESULT_SHA,
            objects=[dict(object_id='odm-archive', bytes=10, sha256='d'*64, relative_path='jobs/' + reservation + '/all.zip')])
        self.ledger.set_engine_state(reservation, 'terminal', terminal_attested=True)
        if accept:
            self.ledger.accept_receipt(reservation, receipt_id='viewer-' + reservation, storage_receipt_id='nas-' + reservation,
                job_id=job, generation=1, manifest_sha256=MANIFEST, pipeline_sha256=PIPELINE,
                result_manifest_sha256=RESULT_SHA, accepted_object_ids=['odm-archive'])

    def inputs_sha(self, reservation='r1', items=(('i1', b'abc'), ('i2', b'def'))):
        return hashlib.sha256(canonical(sorted(self.closure(reservation, items),
                                               key=lambda o: o['object_id'])).encode()).hexdigest()

    def record(self, reservation='r1', job='j1', items=(('i1', b'abc'), ('i2', b'def')), outcome='succeeded', **changes):
        value = dict(schemaVersion=1, purpose='input-copy-release', scope='worker_fetched_inputs_only',
            releaseId='release-' + reservation, authorityId='test-authority', reservationId=reservation, jobId=job,
            generation=1, environment='production', inputManifestSha256=MANIFEST, pipelineDigest=PIPELINE,
            inputsSha256=self.inputs_sha(reservation, items), outcome=outcome,
            outcomeSha256=RESULT_SHA if outcome == 'succeeded' else 'f'*64,
            terminalReceiptId='viewer-' + reservation if outcome == 'succeeded' else 'terminal-' + reservation,
            sourceCustodyReceiptId='custody-' + reservation, sourceStorageReceiptId='custody-store-' + reservation,
            pipelineClosureSha256='e'*64)
        value.update(changes)
        return value

    def release(self, reservation='r1', allow=True, **kwargs):
        record = self.record(reservation, **kwargs)
        if allow:
            self.authority.allow(record)
        return self.ledger.release_input_copies(reservation, record=record)

    def ready(self):
        """One fetched, succeeded, fully accepted and authority-released two-copy job."""
        self.reserve(); self.bind(); self.fetch(); self.start(); self.succeed()
        self.assertEqual(self.release(), 2)

    def candidates(self):
        return [(c['reservation'], c['id']) for c in self.ledger.input_cleanup_candidates()]

    def config(self):
        with closing(sqlite3.connect(self.ledger.path)) as db:
            return json.loads(db.execute("SELECT value FROM metadata WHERE key='config'").fetchone()[0])

    def sql(self, *statements):
        with closing(sqlite3.connect(self.ledger.path)) as db, db:
            for statement in statements:
                db.execute(statement)

    # ----- Marker, footprint and profile fencing ------------------------------

    def test_default_profile_schema_is_unchanged_and_methods_refuse(self):
        other = self.root / 'plain'
        other.mkdir()
        plain = ScratchLedger(other / 'ledger.sqlite', other, capacity_probe=lambda _: 10**9, clock=lambda: 1)
        with closing(sqlite3.connect(plain.path)) as db:
            tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            config = json.loads(db.execute("SELECT value FROM metadata WHERE key='config'").fetchone()[0])
        self.assertFalse({t for t in tables if t.startswith('input_')})
        self.assertNotIn('inputCopyAccountingVersion', config)
        with self.assertRaises(PolicyConflict):
            plain.enable_input_copy_accounting()
        opted = ScratchLedger(other / 'ledger.sqlite', other, capacity_probe=lambda _: 10**9, clock=lambda: 1,
                              input_copy_accounting=True)
        with self.assertRaises(PolicyConflict):
            opted.input_cleanup_candidates()  # Opt-in profile alone does not latch.
        with self.assertRaises(ValueError):
            ScratchLedger(other / 'ledger.sqlite', other, input_copy_accounting=1)
        with self.assertRaises(ValueError):
            ScratchLedger(other / 'ledger.sqlite', other, release_authority=self.authority)  # Needs opt-in.
        with self.assertRaises(ValueError):
            ScratchLedger(other / 'ledger.sqlite', other, input_copy_accounting=True, release_authority=object())

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

    def test_removed_marker_with_retained_input_state_is_refused(self):
        """Review repro: dropping only the config marker must not reopen writers."""
        self.reserve(); self.bind(); self.fetch()
        config = self.config(); config.pop('inputCopyAccountingVersion')
        with closing(sqlite3.connect(self.ledger.path)) as db, db:
            db.execute("UPDATE metadata SET value=? WHERE key='config'", (canonical(config),))
        with self.assertRaises(PolicyConflict):
            self.reserve(reservation='r2', job='j2')  # Ongoing opt-in writer.
        with self.assertRaises(PolicyConflict):
            self.open(inputs=False)  # Cold default-off reopen.
        with self.assertRaises(PolicyConflict):
            self.open()

    def test_marker_without_full_footprint_is_refused(self):
        self.sql("DELETE FROM input_accounting")
        with self.assertRaises(PolicyConflict):
            self.reserve()
        with self.assertRaises(PolicyConflict):
            self.open()

    def test_changed_input_schema_is_refused(self):
        self.sql('DROP TABLE input_pins', 'CREATE TABLE input_pins (reservation TEXT)')
        with self.assertRaises(PolicyConflict):
            self.open()

    def test_control_latch_preserves_input_marker(self):
        self.reserve()
        self.ledger.record_allocated('r1', 6)
        self.ledger.prepare_control_charge('r1', charge_id='c1', scope_sha256='e'*64, parent_binding_sha256='f'*64,
            expected_allocated=6, allowance_bytes=10, maximum_scope_charges=1)
        config = self.config()
        self.assertEqual((config['accountingVersion'], config['inputCopyAccountingVersion']), (1, 1))
        self.open()

    def test_enable_after_control_latch_preserves_control_marker(self):
        other = self.root / 'control'
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
                pipeline_sha256=PIPELINE, result_manifest_sha256=RESULT_SHA,
                objects=[dict(object_id='odm-archive', bytes=3, sha256='d'*64, relative_path='jobs/r1/I1.input')])
        # The same ID in both namespaces is fine: they never share fences.
        self.ledger.register_result('r1', job_id='j1', generation=1, manifest_sha256=MANIFEST,
            pipeline_sha256=PIPELINE, result_manifest_sha256=RESULT_SHA,
            objects=[dict(object_id='i1', bytes=3, sha256='d'*64, relative_path='jobs/r1/all.zip')])
        self.reserve(reservation='R1', job='j3')
        with self.assertRaises(PolicyConflict):  # Case variant of another reservation's result path.
            self.ledger.register_input_binding('R1', job_id='j3', generation=1, environment='production',
                manifest_sha256=MANIFEST, pipeline_sha256=PIPELINE, objects=[
                    dict(object_id='i1', byte_size=3, sha256=sha(b'abc'), relative_path='jobs/R1/ALL.ZIP'),
                    dict(object_id='i2', byte_size=3, sha256=sha(b'def'), relative_path='jobs/R1/i2.input')])

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

    # ----- Release authority ---------------------------------------------------

    def test_no_configured_authority_refuses_every_release(self):
        self.reserve(); self.bind(); self.fetch(); self.start(); self.succeed()
        self.ledger = self.open(authority=False)
        for outcome in ('succeeded', 'failed', 'cancelled'):
            with self.assertRaises(PolicyConflict):
                self.release(outcome=outcome)
        self.now += 10
        self.assertEqual(self.candidates(), [])

    def test_terminal_metadata_alone_is_not_deletion_authority(self):
        """Review repro: a well-formed failure record the authority does not resolve
        releases nothing, so no fetched copy can be deleted."""
        self.reserve(); self.bind(); self.fetch(); self.start()
        self.ledger.set_engine_state('r1', 'terminal', terminal_attested=True)
        for outcome in ('failed', 'cancelled'):
            with self.assertRaises(PolicyConflict):
                self.release(outcome=outcome, allow=False)
        self.now += 10
        self.assertEqual(self.candidates(), [])
        self.assertEqual(self.release(outcome='failed'), 2)  # Only an authority-resolved record releases.

    def test_authority_must_resolve_the_identical_record(self):
        self.reserve(); self.bind(); self.fetch(); self.start(); self.succeed()
        record = self.record()
        self.authority.allow(dict(record, pipelineClosureSha256='0'*64))
        with self.assertRaises(PolicyConflict):
            self.ledger.release_input_copies('r1', record=record)
        with self.assertRaises(PolicyConflict):
            self.release(authorityId='other-authority')

    def test_release_record_must_match_ledger_rows(self):
        self.reserve(); self.bind(); self.fetch(); self.start(); self.succeed(accept=False)
        with self.assertRaises(PolicyConflict):
            self.release()  # Not durably accepted.
        self.ledger.accept_receipt('r1', receipt_id='viewer-r1', storage_receipt_id='nas-r1', job_id='j1',
            generation=1, manifest_sha256=MANIFEST, pipeline_sha256=PIPELINE, result_manifest_sha256=RESULT_SHA,
            accepted_object_ids=['odm-archive'])
        bad = (dict(jobId='j2'), dict(reservationId='r2'), dict(generation=2), dict(environment='staging'),
               dict(inputManifestSha256='0'*64), dict(pipelineDigest='0'*64), dict(inputsSha256='0'*64),
               dict(outcomeSha256='0'*64), dict(terminalReceiptId='unknown'), dict(outcome='done'),
               dict(scope='everything'), dict(purpose='other'), dict(schemaVersion=2),
               dict(sourceCustodyReceiptId='viewer-r1'), dict(sourceStorageReceiptId='nas-r1'),
               dict(sourceCustodyReceiptId='release-r1'), dict(generation=True))
        for changes in bad:
            with self.assertRaises(PolicyConflict, msg=repr(changes)):
                self.release(**changes)
        with self.assertRaises(PolicyConflict):
            self.ledger.release_input_copies('r1', record=dict(self.record(), extra=1))
        self.assertEqual(self.release(), 2)
        self.assertEqual(self.release(), 2)  # Exact replay.
        with self.assertRaises(PolicyConflict):
            self.release(pipelineClosureSha256='0'*64)  # Changed after storage.

    def test_release_requires_original_terminal_reservation(self):
        self.reserve(); self.bind(); self.fetch(); self.start()
        with self.assertRaises(PolicyConflict):
            self.release(outcome='failed')

    def test_release_record_is_compact_for_a_legal_10k_closure(self):
        """Review repro: 10,000 objects with 128-character IDs still fit, because
        the record binds the closure by the ledger's own digest."""
        reservation = 'r' * 128
        items = [('i' + str(index).zfill(5) + 'x' * 122, b'z') for index in range(MAX_INPUT_COPIES)]
        self.reserve(reservation=reservation, input_bytes=len(items))
        self.ledger.register_input_binding(reservation, job_id='j1', generation=1, environment='production',
            manifest_sha256=MANIFEST, pipeline_sha256=PIPELINE, objects=self.closure(reservation, items))
        self.start(reservation); self.succeed(reservation=reservation, accept=False)
        receipt_id = 'viewer-' + 'x' * 121
        self.ledger.accept_receipt(reservation, receipt_id=receipt_id, storage_receipt_id='store-x',
            job_id='j1', generation=1, manifest_sha256=MANIFEST, pipeline_sha256=PIPELINE,
            result_manifest_sha256=RESULT_SHA, accepted_object_ids=['odm-archive'])
        record = self.record(reservation, items=items, releaseId='rel-' + 'x' * 124, terminalReceiptId=receipt_id,
                             sourceCustodyReceiptId='cus-' + 'x' * 124, sourceStorageReceiptId='sto-' + 'x' * 124)
        self.assertLess(len(canonical(record).encode()), MAX_RELEASE_RECORD_BYTES)
        self.authority.allow(record)
        self.assertEqual(self.ledger.release_input_copies(reservation, record=record), MAX_INPUT_COPIES)

    # ----- Stored release revalidation -----------------------------------------

    def test_tampered_stored_release_retains_every_copy(self):
        """Review repro: a changed stored record with a stale digest is never eligible."""
        for statement in ("UPDATE input_releases SET record='{}'",
                          "UPDATE input_releases SET record_sha='" + '0'*64 + "'",
                          "UPDATE input_releases SET authority='other-authority'",
                          "DELETE FROM input_releases",
                          "UPDATE input_bindings SET identity=replace(identity,'production','staging')",
                          "UPDATE objects SET accepted=NULL",
                          "DELETE FROM receipts"):
            with self.subTest(statement=statement):
                self.doCleanups(); self.setUp()
                self.ready(); self.now += 10
                self.assertEqual(len(self.candidates()), 2)
                self.sql(statement)
                self.assertEqual(self.candidates(), [])
                with self.assertRaises(PolicyConflict):
                    self.ledger.begin_input_cleanup('r1', 'i1', generation=1, sha256=sha(b'abc'))

    def test_stored_record_with_matching_forged_digest_still_needs_original_identity(self):
        self.ready(); self.now += 10
        forged = canonical(self.record(jobId='j2'))
        self.sql("UPDATE input_releases SET record='%s',record_sha='%s'" % (forged, hashlib.sha256(forged.encode()).hexdigest()))
        self.assertEqual(self.candidates(), [])

    CLOSURE_TAMPERS = (
        "UPDATE input_copies SET path='jobs/r1/unlisted.input',path_key='jobs/r1/unlisted.input' WHERE id='i1'",
        "UPDATE input_copies SET path_key='jobs/r1/other.input' WHERE id='i1'",
        "UPDATE input_copies SET bytes=4 WHERE id='i1'",
        "UPDATE input_copies SET sha='" + '0' * 64 + "' WHERE id='i1'",
        "UPDATE input_copies SET id='i9' WHERE id='i1'",
        "DELETE FROM input_copies WHERE id='i2'",
        "INSERT INTO input_copies VALUES('r1','i3',3,'" + '0' * 64 + "','jobs/r1/i3.input','jobs/r1/i3.input',1,NULL)",
        "UPDATE input_copies SET path='jobs/r1/./i1.input',path_key='jobs/r1/./i1.input' WHERE id='i1'")

    def test_tampered_input_closure_retains_every_copy_without_a_fence(self):
        """Review repro: row identity changed under an unchanged binding and release."""
        for statement in self.CLOSURE_TAMPERS:
            with self.subTest(statement=statement):
                self.doCleanups(); self.setUp()
                self.ready(); self.now += 10
                (self.root / 'jobs/r1/unlisted.input').write_bytes(b'abc')
                self.sql(statement)
                self.assertEqual(self.candidates(), [])
                self.assertEqual(self.ledger.oversized_input_copies(max_bytes=0), [])
                for name, data in (('i1', b'abc'), ('i2', b'def')):
                    with self.assertRaises(PolicyConflict):
                        self.ledger.begin_input_cleanup('r1', name, generation=1, sha256=sha(data))
                with closing(sqlite3.connect(self.ledger.path)) as db:
                    self.assertEqual(db.execute('SELECT COUNT(*) FROM input_cleanup_intents').fetchone()[0], 0)

    def test_oversized_closure_stops_at_the_first_excess_row(self):
        """Review repro: a counting cursor proves no row past the limit is consumed."""
        consumed = []

        class Cursor:
            def __iter__(self):
                for index in range(MAX_INPUT_COPIES + 137):
                    consumed.append(index)
                    name = 'p%05d' % index
                    yield dict(id=name, bytes=1, sha=sha(b'z'), path='jobs/r1/' + name + '.input',
                               path_key='jobs/r1/' + name + '.input')

        class Database:
            def execute(self, query, parameters):
                return Cursor()
        self.assertFalse(self.ledger._closure_matches(Database(), 'r1', dict(inputsSha256='0' * 64)))
        self.assertEqual(len(consumed), MAX_INPUT_COPIES + 1)

    def test_actual_oversized_closure_rows_retain_every_copy(self):
        self.ready(); self.now += 10
        with closing(sqlite3.connect(self.ledger.path)) as db, db:
            db.executemany("INSERT INTO input_copies VALUES('r1',?,1,?,?,?,1,NULL)",
                [('x%05d' % index, sha(b'z'), 'jobs/r1/x%05d.input' % index, 'jobs/r1/x%05d.input' % index)
                 for index in range(MAX_INPUT_COPIES + 135)])
        started = time.monotonic()
        self.assertEqual(self.candidates(), [])
        with self.assertRaises(PolicyConflict):
            self.ledger.begin_input_cleanup('r1', 'i1', generation=1, sha256=sha(b'abc'))
        self.assertLess(time.monotonic() - started, 10)

    def test_bookkeeping_changes_do_not_alter_the_closure(self):
        self.ready(); self.now += 10
        self.sql("UPDATE input_copies SET fetched=fetched+1")
        self.assertEqual(self.candidates(), [('r1', 'i1'), ('r1', 'i2')])

    def test_row_changed_after_fence_refuses_recovery_and_confirmation(self):
        self.ready(); self.now += 10
        intent = self.ledger.begin_input_cleanup('r1', 'i1', generation=1, sha256=sha(b'abc'))
        self.sql("UPDATE input_copies SET path='jobs/r1/unlisted.input',path_key='jobs/r1/unlisted.input' WHERE id='i1'")
        with self.assertRaises(PolicyConflict):
            self.ledger.input_cleanup_intent('r1', 'i1')
        with self.assertRaises(PolicyConflict):
            self.ledger.mark_input_cleaned('r1', 'i1', generation=1, sha256=sha(b'abc'),
                intent_token=intent['intent_token'], executor_attested=True)

    def test_authority_revocation_before_fence_retains_copies(self):
        self.ready(); self.now += 10
        self.authority.records.clear()
        with self.assertRaises(PolicyConflict):
            self.ledger.begin_input_cleanup('r1', 'i1', generation=1, sha256=sha(b'abc'))
        with closing(sqlite3.connect(self.ledger.path)) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM input_cleanup_intents').fetchone()[0], 0)

    # ----- Eligibility, pins and fences ---------------------------------------

    def test_release_then_grace_then_candidates(self):
        self.ready()
        self.assertEqual(self.candidates(), [])
        self.now += 10
        self.assertEqual(self.candidates(), [('r1', 'i1'), ('r1', 'i2')])

    def test_unfetched_copy_is_never_a_candidate(self):
        self.reserve(); self.bind(); self.fetch(items=(('i1', b'abc'),)); self.start(); self.succeed()
        self.release()
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
        items = (('i1', b'a'*5), ('i2', b'b'*2))
        self.reserve(input_bytes=7); self.bind(items=items); self.fetch(items=items); self.start(); self.succeed()
        self.release(items=items)
        self.now += 10

        def ids(**limits):
            return [c['id'] for c in self.ledger.input_cleanup_candidates(**limits)]
        self.assertEqual(ids(max_count=10, max_bytes=1), [])  # Never silently exceeded.
        self.assertEqual([c['id'] for c in self.ledger.oversized_input_copies(max_bytes=1)], ['i1', 'i2'])
        self.assertEqual([c['id'] for c in self.ledger.oversized_input_copies(max_bytes=1, max_count=1)], ['i1'])
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
        self.release('r2', job='j2')
        self.now += 1
        self.release()
        self.now += 10
        self.assertEqual(self.candidates(), [('r2', 'i1'), ('r2', 'i2'), ('r1', 'i1'), ('r1', 'i2')])

    def test_capacity_recovers_only_after_input_copies_are_cleaned(self):
        """Reproducer: finished jobs used to hold their input bytes forever."""
        self.free = 1000
        items = (('i1', b'x' * 500),)
        self.reserve(input_bytes=500)
        self.bind(items=items); self.fetch(items=items)
        self.ledger.record_allocated('r1', 500); self.free -= 500
        self.start(); self.succeed()
        self.release(items=items)
        self.now += 10
        with self.assertRaises(PolicyConflict):
            self.reserve(reservation='r2', job='j2', input_bytes=500)
        intent = self.ledger.begin_input_cleanup('r1', 'i1', generation=1, sha256=sha(items[0][1]))
        (self.root / 'jobs/r1/i1.input').unlink(); self.free += 500
        self.ledger.mark_input_cleaned('r1', 'i1', generation=1, sha256=sha(items[0][1]),
            intent_token=intent['intent_token'], executor_attested=True)
        self.reserve(reservation='r2', job='j2', input_bytes=500)

    def test_ten_thousand_copy_registration_and_batch_are_bounded(self):
        items = [('p%05d' % index, b'z') for index in range(MAX_INPUT_COPIES)]
        self.reserve(input_bytes=len(items))
        started = time.monotonic()
        self.bind(items=items)
        self.assertLess(time.monotonic() - started, 30)
        with self.assertRaises(ValueError):
            self.ledger.register_input_binding('r2', job_id='j2', generation=1, environment='production',
                manifest_sha256=MANIFEST, pipeline_sha256=PIPELINE,
                objects=self.closure('r2', items + [('extra', b'z')]))
        self.sql("UPDATE input_copies SET fetched=1")
        self.start(); self.succeed()
        self.release(items=items)
        self.now += 10
        started = time.monotonic()
        self.assertEqual(len(self.ledger.input_cleanup_candidates(max_count=100, max_bytes=1024**3)), 100)
        self.assertEqual(len(self.ledger.input_cleanup_candidates(max_count=10000, max_bytes=250)), 250)
        self.assertLess(time.monotonic() - started, 10)


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
    inputs_sha = InputCopyLedgerTests.inputs_sha
    record = InputCopyLedgerTests.record
    release = InputCopyLedgerTests.release
    ready = InputCopyLedgerTests.ready
    sql = InputCopyLedgerTests.sql

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

    def test_unresolved_terminal_release_never_reaches_the_executor(self):
        """Review repro, end to end: failure metadata without authority deletes nothing."""
        self.reserve(); self.bind(); self.fetch(); self.start()
        self.ledger.set_engine_state('r1', 'terminal', terminal_attested=True)
        with self.assertRaises(PolicyConflict):
            self.release(outcome='failed', allow=False)
        self.now += 10
        with self.assertRaises(PolicyConflict):
            self.executor().execute_input_copy('r1', 'i1', generation=1, sha256=sha(b'abc'), byte_size=3,
                                               relative_path='jobs/r1/i1.input')
        self.assertEqual((self.root / 'jobs/r1/i1.input').read_bytes(), b'abc')
        self.assertEqual((self.root / 'jobs/r1/i2.input').read_bytes(), b'def')

    def test_tampered_closure_row_never_deletes_an_unlisted_file(self):
        """Review repro, end to end with the actual executor."""
        self.ready(); self.now += 10
        victim = self.root / 'jobs/r1/unlisted-copy.input'
        victim.write_bytes(b'abc')
        self.sql("UPDATE input_copies SET path='jobs/r1/unlisted-copy.input',path_key='jobs/r1/unlisted-copy.input' "
                 "WHERE reservation='r1' AND id='i1'")
        self.assertEqual(self.ledger.input_cleanup_candidates(), [])
        for relative in ('jobs/r1/unlisted-copy.input', 'jobs/r1/i1.input'):
            with self.assertRaises(PolicyConflict):
                self.executor().execute_input_copy('r1', 'i1', generation=1, sha256=sha(b'abc'), byte_size=3,
                                                   relative_path=relative)
        self.assertEqual(victim.read_bytes(), b'abc')
        self.assertEqual((self.root / 'jobs/r1/i1.input').read_bytes(), b'abc')
        self.assertEqual((self.root / 'jobs/r1/i2.input').read_bytes(), b'def')

    def test_row_changed_after_fence_stops_the_executor(self):
        from processing.scratch_cleanup import CleanupRefused
        self.ready(); self.now += 10
        intent = self.ledger.begin_input_cleanup('r1', 'i1', generation=1, sha256=sha(b'abc'))
        victim = self.root / 'jobs/r1/unlisted.input'
        victim.write_bytes(b'abc')
        self.sql("UPDATE input_copies SET path='jobs/r1/unlisted.input',path_key='jobs/r1/unlisted.input' WHERE id='i1'")
        for relative in ('jobs/r1/unlisted.input', 'jobs/r1/i1.input'):
            with self.assertRaises((PolicyConflict, CleanupRefused)):
                self.executor().execute_input_copy('r1', 'i1', generation=1, sha256=sha(b'abc'), byte_size=3,
                                                   relative_path=relative, intent_token=intent['intent_token'])
        self.assertEqual(victim.read_bytes(), b'abc')
        self.assertEqual((self.root / 'jobs/r1/i1.input').read_bytes(), b'abc')

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

    def test_input_and_result_with_same_id_use_separate_journal_keys(self):
        self.reserve(); self.bind(); self.fetch(); self.start()
        self.ledger.register_result('r1', job_id='j1', generation=1, manifest_sha256=MANIFEST,
            pipeline_sha256=PIPELINE, result_manifest_sha256=RESULT_SHA,
            objects=[dict(object_id='i1', bytes=3, sha256=sha(b'zip'), relative_path='jobs/r1/all.zip')])
        (self.root / 'jobs/r1/all.zip').write_bytes(b'zip')
        self.ledger.set_engine_state('r1', 'terminal', terminal_attested=True)
        self.ledger.accept_receipt('r1', receipt_id='viewer-r1', storage_receipt_id='nas-r1', job_id='j1',
            generation=1, manifest_sha256=MANIFEST, pipeline_sha256=PIPELINE, result_manifest_sha256=RESULT_SHA,
            accepted_object_ids=['i1'])
        self.release()
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
