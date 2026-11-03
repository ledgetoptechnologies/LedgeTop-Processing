import sqlite3
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from processing.scratch_policy import ScratchLedger, PolicyConflict
from processing.viewer_upload_capacity_candidate import ViewerUploadCapacity
from processing.viewer_limits import MAX_UPLOAD_BYTES


class Store:
    max_input_bytes = 100
    def __init__(self, root):
        self.root = Path(root); self.queue = self
        self.db = sqlite3.connect(':memory:')
        self.db.row_factory = sqlite3.Row
        self.db.execute('CREATE TABLE viewer_tasks(uuid TEXT,environment TEXT,stage TEXT,job_id TEXT,generation INTEGER)')
        self.db.execute("INSERT INTO viewer_tasks VALUES('job1','production','uploading',NULL,1)")
        self.db.execute('CREATE TABLE viewer_files(uuid TEXT,name_key TEXT,bytes INTEGER)')
        self.db.commit()
    @contextmanager
    def _db(self): yield self.db
    def _row(self, db, identifier):
        return db.execute('SELECT * FROM viewer_tasks WHERE uuid=?',(identifier,)).fetchone()
    def _filename(self, name): return name.lower()


class UploadCapacityTests(unittest.TestCase):
    def test_reserve_recheck_partial_retry_and_commit(self):
        with tempfile.TemporaryDirectory() as root:
            store = Store(root)
            try:
                free = [MAX_UPLOAD_BYTES + 1000]
                ledger = ScratchLedger(Path(root)/'scratch.sqlite',root,capacity_probe=lambda _:free[0],safety_floor_bytes=100,clock=lambda:1)
                gate = ViewerUploadCapacity(ledger,store,root)
                with gate.upload('job1','production',20,10): gate.file('job1','production',20,'a.jpg',10)
                store.db.execute("INSERT INTO viewer_files VALUES('job1','a.jpg',10)");store.db.commit();free[0]-=10
                with gate.upload('job1','production',20,10): gate.file('job1','production',20,'a.jpg',10)
                with self.assertRaises(PolicyConflict):
                    with gate.commit('job1','production',20): pass
                with self.assertRaises(PolicyConflict):
                    with gate.upload('job1','production',21,10): pass
                with self.assertRaises(PolicyConflict): gate.file('job1','production',20,'b.jpg',11)
                store.db.execute("INSERT INTO viewer_files VALUES('job1','b.jpg',10)");store.db.commit();free[0]-=10
                with gate.commit('job1','production',20): pass
                free[0]=100
                with self.assertRaises(PolicyConflict):
                    with gate.upload('job1','production',20,10): pass
                # A restart cannot silently drop or enlarge the original hold.
                reopened = ScratchLedger(Path(root)/'scratch.sqlite',root,capacity_probe=lambda _:free[0],safety_floor_bytes=100,clock=lambda:90000)
                replacement = ViewerUploadCapacity(reopened,store,root)
                with self.assertRaises(PolicyConflict):
                    with replacement.upload('job1','production',21,10): pass
                with reopened._transaction() as db:
                    self.assertEqual(db.execute('SELECT state FROM reservations').fetchone()[0], 'uncertain')
            finally: store.db.close()
