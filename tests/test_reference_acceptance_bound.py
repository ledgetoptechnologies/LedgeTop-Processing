"""Bounded native acceptance coverage and idempotent replay, in memory."""
from contextlib import contextmanager
import copy
import json
from pathlib import Path
import sqlite3
import threading
import unittest
import uuid
from unittest.mock import patch

from processing import reference_protocol as p
from processing.maintenance import Conflict
from processing.reference_store import ReferenceStore


class Queue:
    def __init__(self, result, input_digest):
        self.lock = threading.RLock()
        self.db = sqlite3.connect(':memory:', check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.executescript('''CREATE TABLE reference_jobs(job_id TEXT PRIMARY KEY, phase TEXT, updated_at TEXT);
            CREATE TABLE reference_acceptances(receipt_id TEXT PRIMARY KEY, job_id TEXT, digest TEXT, receipt_json TEXT);
            INSERT INTO reference_jobs(job_id,phase) VALUES ('job-1','awaiting_acceptance');''')
        self.result = result
        self.input_digest = input_digest

    @contextmanager
    def _db(self):
        with self.lock:
            try:
                yield self.db
                self.db.commit()
            except BaseException:
                self.db.rollback()
                raise


class AcceptanceBoundTests(unittest.TestCase):
    def setUp(self):
        fixture = json.loads((Path(__file__).resolve().parents[1] / 'contracts/reference-v1.contract.json').read_text())
        self.result = fixture['resultManifest']
        self.receipt = fixture['acceptance']
        self.queue = Queue(self.result, self.receipt['inputManifestSha256'])
        self.store = object.__new__(ReferenceStore)
        self.store.queue = self.queue
        self.store._quota = lambda *_: None
        self.store._row = lambda db, job: dict(job_id=job,
            phase=db.execute('SELECT phase FROM reference_jobs WHERE job_id=?', (job,)).fetchone()[0],
            result_json=json.dumps(self.result), manifest_sha=self.queue.input_digest)
        self.store._public = lambda row: {'phase': row['phase']}

    def tearDown(self):
        self.queue.db.close()

    def test_exact_replay_empty_and_no_new_coverage(self):
        first = self.store.accept('job-1', self.receipt)
        self.assertTrue(first['created'])
        self.assertFalse(first['fullyAccepted'])
        replay = self.store.accept('job-1', self.receipt)
        self.assertFalse(replay['created'])
        for response in (first,replay):
            self.assertEqual(response['receiptId'],self.receipt['receiptId'])
            self.assertEqual(response['receiptSha256'],p.digest(self.receipt))
        for objects in ([], ['result-1']):
            duplicate = dict(self.receipt, receiptId='other', storageReceiptId='other-storage', acceptedObjectIds=objects)
            with self.assertRaises(Conflict): self.store.accept('job-1', duplicate)
        final = dict(self.receipt, receiptId='last', storageReceiptId='last-storage',
                     acceptedObjectIds=['result-1', 'result-2'])
        self.assertTrue(self.store.accept('job-1', final)['fullyAccepted'])
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM reference_acceptances').fetchone()[0], 2)

    def test_rollback_before_commit_emits_no_success_and_exact_retry_is_safe(self):
        original = self.queue._db
        @contextmanager
        def failed_commit():
            with self.queue.lock:
                try:
                    yield self.queue.db
                    raise sqlite3.OperationalError('synthetic pre-commit loss')
                finally:
                    self.queue.db.rollback()
        self.queue._db = failed_commit
        try:
            with self.assertRaises(sqlite3.OperationalError): self.store.accept('job-1',self.receipt)
        finally:
            self.queue._db = original
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM reference_acceptances').fetchone()[0],0)
        response = self.store.accept('job-1',self.receipt)
        self.assertTrue(response['created'])
        self.assertEqual(response['receiptSha256'],p.digest(self.receipt))

    def test_concurrent_overlapping_receipts_add_coverage_at_most_once(self):
        receipts = [dict(self.receipt, receiptId='receipt-'+str(i), storageReceiptId='storage-'+str(i)) for i in range(2)]
        barrier = threading.Barrier(2); outcomes = []
        def accept(receipt):
            barrier.wait(timeout=2)
            try: outcomes.append(self.store.accept('job-1', receipt)['created'])
            except Conflict: outcomes.append('conflict')
        threads = [threading.Thread(target=accept, args=(receipt,)) for receipt in receipts]
        for thread in threads: thread.start()
        for thread in threads: thread.join(timeout=3); self.assertFalse(thread.is_alive())
        self.assertCountEqual(outcomes, [True, 'conflict'])

    def test_concurrent_exact_replays_are_idempotent(self):
        barrier = threading.Barrier(2); outcomes = []
        def accept():
            barrier.wait(timeout=2)
            outcomes.append(self.store.accept('job-1', self.receipt)['created'])
        threads = [threading.Thread(target=accept) for _ in range(2)]
        for thread in threads: thread.start()
        for thread in threads: thread.join(timeout=3); self.assertFalse(thread.is_alive())
        self.assertCountEqual(outcomes, [True, False])

    def test_result_manifests_cannot_be_empty(self):
        bad = copy.deepcopy(self.result); bad['objects'] = []
        bad['resultManifestSha256'] = p.digest({k:v for k,v in bad.items() if k != 'resultManifestSha256'})
        with self.assertRaises(p.ProtocolError):
            p.validate_result_manifest(bad, job_id='job-1', generation=1,
                environment='staging', pipeline_digest=bad['pipelineDigest'])

    def test_legacy_receipts_consume_capacity_but_exact_replay_still_works(self):
        legacy = dict(self.receipt, receiptId='legacy', storageReceiptId='legacy-store', acceptedObjectIds=[])
        legacy_json = p.canonical_json(legacy).decode()
        with self.queue._db() as db:
            db.execute('INSERT INTO reference_acceptances VALUES(?,?,?,?)',
                ('legacy', 'job-1', p.receipt_tombstone_digest(legacy), legacy_json))
        with patch.object(p, 'MAX_OBJECTS', 2):
            self.assertTrue(self.store.accept('job-1', self.receipt)['created'])
            self.assertFalse(self.store.accept('job-1', self.receipt)['created'])
            final = dict(self.receipt, receiptId='new', storageReceiptId='new-store', acceptedObjectIds=['result-2'])
            with self.assertRaises(Conflict): self.store.accept('job-1', final)
            self.assertFalse(self.store.accept('job-1', legacy)['created'])
            excess = dict(legacy, receiptId='legacy-excess', storageReceiptId='legacy-excess-store')
            with self.queue._db() as db:
                db.execute('INSERT INTO reference_acceptances VALUES(?,?,?,?)',
                    ('legacy-excess', 'job-1', p.receipt_tombstone_digest(excess), p.canonical_json(excess).decode()))
            with self.assertRaises(Conflict): self.store.accept('job-1', final)
            self.assertFalse(self.store.accept('job-1', self.receipt)['created'])
        with self.queue._db() as db:
            rows = db.execute('SELECT receipt_id,receipt_json FROM reference_acceptances ORDER BY receipt_id').fetchall()
        self.assertEqual([row['receipt_id'] for row in rows], ['legacy', 'legacy-excess', 'receipt-1'])
        self.assertEqual(rows[0]['receipt_json'], legacy_json)

    def test_relay_wire_byte_budget_preserves_existing_receipt_and_exact_replay(self):
        second = dict(self.receipt, receiptId='unicode', storageReceiptId='unicode-storage',
                      acceptedObjectIds=['result-2'], acceptedAt='2026-10-27🔥12:00:00+00:00')
        first_wire = json.dumps({'receipts':[self.receipt], 'fullyAccepted':False},
            ensure_ascii=True, allow_nan=False, separators=(',', ':')).encode()
        second_wire = json.dumps({'receipts':[self.receipt,second], 'fullyAccepted':False},
            ensure_ascii=True, allow_nan=False, separators=(',', ':')).encode()
        self.assertGreater(len(second_wire), len(first_wire))
        with patch.object(p, 'MAX_BODY_BYTES', len(first_wire)):
            self.assertTrue(self.store.accept('job-1', self.receipt)['created'])
            with self.assertRaises(Conflict): self.store.accept('job-1', second)
            self.assertFalse(self.store.accept('job-1', self.receipt)['created'])
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM reference_acceptances').fetchone()[0], 1)

    def test_independent_sqlite_connections_conflict_then_exact_replay(self):
        uri = 'file:acceptance-' + uuid.uuid4().hex + '?mode=memory&cache=shared'
        keeper = sqlite3.connect(uri, uri=True, check_same_thread=False)
        keeper.executescript('''CREATE TABLE reference_jobs(job_id TEXT PRIMARY KEY, phase TEXT, updated_at TEXT);
            CREATE TABLE reference_acceptances(receipt_id TEXT PRIMARY KEY, job_id TEXT, digest TEXT, receipt_json TEXT);
            INSERT INTO reference_jobs(job_id,phase) VALUES ('job-1','awaiting_acceptance');''')
        keeper.commit()
        class IndependentQueue:
            @contextmanager
            def _db(inner):
                db = sqlite3.connect(uri, uri=True, timeout=.2)
                db.row_factory = sqlite3.Row
                try:
                    with db: yield db
                finally: db.close()
        self.store.queue = IndependentQueue()
        entered, release = threading.Event(), threading.Event()
        original = self.store._row
        def block(db, job):
            if threading.current_thread().name == 'first-acceptance' and not entered.is_set():
                entered.set()
                if not release.wait(3): raise RuntimeError('test handoff timed out')
            return original(db, job)
        self.store._row = block
        outcomes = []
        def first():
            try: outcomes.append(self.store.accept('job-1', self.receipt)['created'])
            except BaseException as error: outcomes.append(error)
        thread = threading.Thread(name='first-acceptance', target=first)
        try:
            thread.start()
            self.assertTrue(entered.wait(2))
            with self.assertRaises(sqlite3.OperationalError):
                self.store.accept('job-1', self.receipt)
        finally:
            release.set(); thread.join(timeout=3)
            self.assertFalse(thread.is_alive())
        try:
            self.assertEqual(outcomes, [True])
            self.assertFalse(self.store.accept('job-1', self.receipt)['created'])
            self.assertEqual(keeper.execute('SELECT COUNT(*) FROM reference_acceptances').fetchone()[0], 1)
        finally:
            keeper.close()


if __name__ == '__main__': unittest.main()
