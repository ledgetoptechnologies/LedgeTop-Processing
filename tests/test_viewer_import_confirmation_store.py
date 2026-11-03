import concurrent.futures
from contextlib import closing
import copy
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import textwrap
import threading
import unittest
from unittest.mock import patch

from processing import reference_protocol as protocol
from processing.viewer_import_confirmation import VerifiedImportConfirmation
from processing.viewer_import_confirmation_store import ImportConfirmationStore, ImportConfirmationStoreHeld
import tests.test_viewer_import_confirmation as verifier_fixture


def fixture_loader(db, scope):
    row = db.execute('SELECT payload FROM trusted_import_fixture WHERE job_id=? AND generation=? AND environment=?',
                     (scope['jobId'], scope['generation'], scope['environment'])).fetchone()
    if row is None:
        raise ValueError('no independently provisioned trusted fixture')
    return json.loads(row[0])


class ImportConfirmationStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='lt-import-proof-store-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root / 'proof.sqlite'
        self.fixture = verifier_fixture.ViewerImportConfirmationTests('test_valid_signature_full_ordered_closure_and_exact_replay')
        self.fixture.setUp()
        self.scope = {'jobId': 'job-1', 'generation': 1, 'environment': 'staging'}
        self.context = dict(expected=self.fixture.expected, result_manifest=self.fixture.manifest,
            accepted_envelopes=self.fixture.envelopes, trusted_import_keys=self.fixture.import_keys,
            trusted_copy_keys=self.fixture.copy_signer.trusted_keys)
        self.store = ImportConfirmationStore(self.path, trusted_loader=fixture_loader)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('CREATE TABLE trusted_import_fixture(job_id TEXT, generation INTEGER, environment TEXT, payload TEXT)')
            db.execute('INSERT INTO trusted_import_fixture VALUES (?,?,?,?)',
                (self.scope['jobId'], self.scope['generation'], self.scope['environment'],
                 protocol.canonical_json(self.context).decode('utf-8')))

    def update_context(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('UPDATE trusted_import_fixture SET payload=?',
                       (protocol.canonical_json(self.context).decode('utf-8'),))

    def stored_rows(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            return db.execute('SELECT confirmation_id,binding_sha256,confirmation_sha256,canonical_bytes '
                              'FROM viewer_import_confirmations_candidate_v1').fetchall()

    def test_exact_first_write_identical_replay_and_restart(self):
        proof = self.fixture.confirmation
        first = self.store.record(self.scope, proof)
        self.assertEqual(first.state, 'recorded')
        self.assertEqual(first.sha256, hashlib.sha256(protocol.canonical_json(proof)).hexdigest())
        self.assertEqual(self.store.record(self.scope, proof).state, 'replayed')
        rows = self.stored_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][3], protocol.canonical_json(proof))
        reopened = ImportConfirmationStore(self.path, trusted_loader=fixture_loader)
        loaded = reopened.load_verified(self.scope)
        self.assertEqual(loaded.sha256, first.sha256)
        self.assertEqual(loaded.canonical_bytes, rows[0][3])
        self.assertEqual(reopened.record(self.scope, proof).state, 'replayed')

    def test_changed_valid_signed_proof_is_held_without_overwrite(self):
        self.store.record(self.scope, self.fixture.confirmation)
        previous = self.stored_rows()
        self.fixture.confirmation['confirmationId'] = 'changed-confirmation'
        self.fixture.sign()
        changed = copy.deepcopy(self.fixture.confirmation)
        with self.assertRaisesRegex(ImportConfirmationStoreHeld, 'confirmation_unverified_or_changed'):
            self.store.record(self.scope, changed)
        self.assertEqual(self.stored_rows(), previous)

    def test_precommit_failure_rolls_back_and_lost_commit_response_replays(self):
        proof = self.fixture.confirmation
        with patch('processing.viewer_import_confirmation_store._after_insert', side_effect=RuntimeError('crash')):
            with self.assertRaisesRegex(RuntimeError, 'crash'):
                self.store.record(self.scope, proof)
        self.assertEqual(self.stored_rows(), [])
        with patch('processing.viewer_import_confirmation_store._after_commit', side_effect=RuntimeError('lost response')):
            with self.assertRaisesRegex(RuntimeError, 'lost response'):
                self.store.record(self.scope, proof)
        self.assertEqual(len(self.stored_rows()), 1)
        self.assertEqual(self.store.record(self.scope, proof).state, 'replayed')

    def test_trusted_binding_writer_cannot_interleave_before_first_commit(self):
        original = protocol.canonical_json(self.context).decode('utf-8')
        attempted = []
        def concurrent_write():
            with closing(sqlite3.connect(self.path, timeout=0.1)) as other, other:
                with self.assertRaises(sqlite3.OperationalError):
                    other.execute('UPDATE trusted_import_fixture SET payload=?', ('{}',))
                attempted.append(True)
        with patch('processing.viewer_import_confirmation_store._after_insert', side_effect=concurrent_write):
            self.assertEqual(self.store.record(self.scope, self.fixture.confirmation).state, 'recorded')
        self.assertEqual(attempted, [True])
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT payload FROM trusted_import_fixture').fetchone()[0], original)
        self.assertEqual(self.store.record(self.scope, self.fixture.confirmation).state, 'replayed')

    def test_process_death_before_commit_leaves_no_confirmation(self):
        proof_path = self.root / 'synthetic-confirmation.json'
        proof_path.write_bytes(protocol.canonical_json(self.fixture.confirmation))
        code = textwrap.dedent('''
            import json, os, sys
            from unittest.mock import patch
            from processing.viewer_import_confirmation_store import ImportConfirmationStore
            def loader(db, scope):
                row = db.execute('SELECT payload FROM trusted_import_fixture WHERE job_id=? AND generation=? AND environment=?',
                    (scope['jobId'], scope['generation'], scope['environment'])).fetchone()
                return json.loads(row[0])
            store = ImportConfirmationStore(sys.argv[1], trusted_loader=loader)
            with open(sys.argv[2], 'rb') as source: proof = json.load(source)
            with patch('processing.viewer_import_confirmation_store._after_insert', side_effect=lambda: os._exit(71)):
                store.record({'jobId':'job-1','generation':1,'environment':'staging'}, proof)
        ''')
        child = subprocess.run([sys.executable, '-c', code, str(self.path), str(proof_path)],
                               capture_output=True, text=True, timeout=20)
        self.assertEqual(child.returncode, 71, child.stderr)
        reopened = ImportConfirmationStore(self.path, trusted_loader=fixture_loader)
        self.assertIsNone(reopened.load_verified(self.scope))
        self.assertEqual(reopened.record(self.scope, self.fixture.confirmation).state, 'recorded')

    def test_concurrent_identical_first_writes_commit_once(self):
        barrier = threading.Barrier(2)
        def run():
            barrier.wait(timeout=5)
            return self.store.record(self.scope, self.fixture.confirmation)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: run(), range(2)))
        self.assertEqual(sorted(item.state for item in results), ['recorded', 'replayed'])
        self.assertEqual(results[0].sha256, results[1].sha256)
        self.assertEqual(len(self.stored_rows()), 1)

    def test_concurrent_distinct_valid_proofs_cannot_overwrite(self):
        first = copy.deepcopy(self.fixture.confirmation)
        self.fixture.confirmation['confirmationId'] = 'other-confirmation'
        self.fixture.sign()
        second = copy.deepcopy(self.fixture.confirmation)
        barrier = threading.Barrier(2)
        def run(value):
            barrier.wait(timeout=5)
            try:
                return self.store.record(self.scope, value).state
            except ImportConfirmationStoreHeld:
                return 'held'
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(run, (first, second)))
        self.assertEqual(sorted(results), ['held', 'recorded'])
        rows = self.stored_rows()
        self.assertEqual(len(rows), 1)
        self.assertIn(rows[0][3], [protocol.canonical_json(first), protocol.canonical_json(second)])

    def test_changed_binding_and_revoked_pin_never_overwrite_proof(self):
        proof = self.fixture.confirmation
        self.store.record(self.scope, proof)
        previous = self.stored_rows()
        self.context['expected']['viewerAttemptId'] = 'other-attempt'
        self.update_context()
        with self.assertRaises(ImportConfirmationStoreHeld):
            self.store.record(self.scope, proof)
        self.assertEqual(self.stored_rows(), previous)
        self.context['expected']['viewerAttemptId'] = 'attempt-1'
        pins = self.context['trusted_import_keys']
        self.context['trusted_import_keys'] = {}
        self.update_context()
        with self.assertRaises(ImportConfirmationStoreHeld):
            self.store.load_verified(self.scope)
        self.assertEqual(self.stored_rows(), previous)
        self.context['trusted_import_keys'] = pins
        self.context['trusted_import_keys']['extra-key'] = copy.deepcopy(pins['import-key'])
        self.update_context()
        self.assertEqual(self.store.record(self.scope, proof).state, 'replayed')

    def test_corrupted_stored_bytes_are_not_repaired_or_trusted(self):
        proof = self.fixture.confirmation
        self.store.record(self.scope, proof)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('UPDATE viewer_import_confirmations_candidate_v1 SET canonical_bytes=?',
                       (protocol.canonical_json(proof) + b' ',))
        before = self.stored_rows()
        with self.assertRaisesRegex(ImportConfirmationStoreHeld, 'stored_confirmation_conflict'):
            self.store.record(self.scope, proof)
        with self.assertRaisesRegex(ImportConfirmationStoreHeld, 'stored_confirmation_conflict'):
            self.store.load_verified(self.scope)
        self.assertEqual(self.stored_rows(), before)

    def test_stored_duplicate_json_keys_are_rejected_even_with_matching_digest(self):
        self.store.record(self.scope, self.fixture.confirmation)
        bad = b'{"duplicate":1,"duplicate":2}'
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('UPDATE viewer_import_confirmations_candidate_v1 SET canonical_bytes=?,confirmation_sha256=?',
                       (bad, hashlib.sha256(bad).hexdigest()))
        before = self.stored_rows()
        with self.assertRaisesRegex(ImportConfirmationStoreHeld, 'stored_confirmation_invalid'):
            self.store.load_verified(self.scope)
        self.assertEqual(self.stored_rows(), before)

    def test_missing_binding_and_caller_constructed_verified_instance_fail_closed(self):
        with self.assertRaisesRegex(ImportConfirmationStoreHeld, 'trusted_binding_unavailable'):
            self.store.record({'jobId': 'missing', 'generation': 1, 'environment': 'staging'},
                              self.fixture.confirmation)
        raw = b'{}'
        forged = VerifiedImportConfirmation(raw, hashlib.sha256(raw).hexdigest())
        with self.assertRaisesRegex(ImportConfirmationStoreHeld, 'input_invalid'):
            self.store.record(self.scope, forged)
        self.assertEqual(self.stored_rows(), [])

    def test_loader_sql_mutation_and_commit_are_denied(self):
        for operation in ('DELETE FROM trusted_import_fixture', 'COMMIT'):
            with self.subTest(operation=operation):
                def writing_loader(db, scope):
                    db.execute(operation)
                    return self.context
                store = ImportConfirmationStore(self.path, trusted_loader=writing_loader)
                with self.assertRaisesRegex(ImportConfirmationStoreHeld, 'trusted_binding_unavailable'):
                    store.record(self.scope, self.fixture.confirmation)
        self.assertEqual(self.stored_rows(), [])
        self.assertEqual(self.store.record(self.scope, self.fixture.confirmation).state, 'recorded')

    def test_oversized_stored_blob_is_not_loaded_as_a_valid_receipt(self):
        proof = self.fixture.confirmation
        self.store.record(self.scope, proof)
        actual_size = len(protocol.canonical_json(proof))
        with patch('processing.viewer_import_confirmation_store.MAX_INPUT_BYTES', actual_size - 1):
            with self.assertRaisesRegex(ImportConfirmationStoreHeld, 'stored_confirmation_invalid'):
                self.store.load_verified(self.scope)
            with self.assertRaisesRegex(ImportConfirmationStoreHeld, 'stored_confirmation_conflict'):
                self.store.record(self.scope, proof)

    def test_confirmation_identity_cannot_be_reused_by_another_job(self):
        self.store.record(self.scope, self.fixture.confirmation)
        first_rows = self.stored_rows()
        other = verifier_fixture.ViewerImportConfirmationTests('test_valid_signature_full_ordered_closure_and_exact_replay')
        other.setUp()
        other.manifest['jobId'] = 'job-2'
        other.manifest['resultManifestSha256'] = protocol.digest(
            {k: v for k, v in other.manifest.items() if k != 'resultManifestSha256'})
        other.expected['jobId'] = 'job-2'
        other.expected['resultManifestSha256'] = other.manifest['resultManifestSha256']
        envelopes = []
        for item in other.envelopes:
            acceptance = copy.deepcopy(item['acceptance'])
            acceptance['jobId'] = 'job-2'
            acceptance['resultManifestSha256'] = other.manifest['resultManifestSha256']
            envelopes.append(other.copy_signer.envelope(acceptance, other.manifest))
        other.envelopes = envelopes
        other.expected['acceptedEnvelopeSha256'] = sorted(protocol.digest(item) for item in envelopes)
        copies = []
        for item in envelopes:
            acceptance, receipt = item['acceptance'], item['viewerReceipt']
            copies.append(dict(acceptanceReceiptId=acceptance['receiptId'],
                acceptanceStorageReceiptId=acceptance['storageReceiptId'], viewerReceiptId=receipt['receiptId'],
                viewerStorageReceiptId=receipt['storageReceiptId'], envelopeSha256=protocol.digest(item)))
        other.confirmation['acceptedCopies'] = sorted(copies, key=lambda item: item['envelopeSha256'])
        other.confirmation['jobId'] = 'job-2'
        other.confirmation['resultManifestSha256'] = other.manifest['resultManifestSha256']
        other.sign()  # Intentionally reuse confirm-1 across two independently valid jobs.
        other_context = dict(expected=other.expected, result_manifest=other.manifest,
            accepted_envelopes=other.envelopes, trusted_import_keys=other.import_keys,
            trusted_copy_keys=other.copy_signer.trusted_keys)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('INSERT INTO trusted_import_fixture VALUES (?,?,?,?)',
                ('job-2', 1, 'staging', protocol.canonical_json(other_context).decode('utf-8')))
        with self.assertRaisesRegex(ImportConfirmationStoreHeld, 'confirmation_identity_conflict'):
            self.store.record({'jobId': 'job-2', 'generation': 1, 'environment': 'staging'}, other.confirmation)
        self.assertEqual(self.stored_rows(), first_rows)

    def test_database_schema_substitution_is_refused(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TABLE viewer_import_confirmations_candidate_v1')
            db.execute('CREATE TABLE viewer_import_confirmations_candidate_v1 (job_id TEXT)')
        with self.assertRaisesRegex(ImportConfirmationStoreHeld, 'store_schema_changed'):
            self.store.record(self.scope, self.fixture.confirmation)


if __name__ == '__main__':
    unittest.main()
