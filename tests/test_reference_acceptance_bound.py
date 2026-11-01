"""Bounded native acceptance coverage and idempotent replay, in memory."""
from contextlib import contextmanager
import copy
import json
from pathlib import Path
import sqlite3
import threading
import unittest

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
        self.assertFalse(self.store.accept('job-1', self.receipt)['created'])
        for objects in ([], ['result-1']):
            duplicate = dict(self.receipt, receiptId='other', storageReceiptId='other-storage', acceptedObjectIds=objects)
            with self.assertRaises(Conflict): self.store.accept('job-1', duplicate)
        final = dict(self.receipt, receiptId='last', storageReceiptId='last-storage',
                     acceptedObjectIds=['result-1', 'result-2'])
        self.assertTrue(self.store.accept('job-1', final)['fullyAccepted'])
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM reference_acceptances').fetchone()[0], 2)

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


if __name__ == '__main__': unittest.main()
