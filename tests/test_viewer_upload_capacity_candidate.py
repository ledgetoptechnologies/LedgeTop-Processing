import sqlite3
import subprocess
import sys
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
    def test_actual_store_lost_commit_response_process_restart(self):
        script = r'''import io,json,os,sys
from pathlib import Path
from processing.prototype import Queue
from processing.viewer_tasks import TaskStore,VIEWER_PROFILE
from processing.scratch_policy import ScratchLedger
from processing.viewer_upload_capacity_candidate import ViewerUploadCapacity
from processing.viewer_limits import MAX_UPLOAD_BYTES
root=Path(sys.argv[1]); phase=sys.argv[2]; ident='11111111-2222-4333-8444-555555555555'
q=Queue(root/'queue');q.allowed_profiles.add(VIEWER_PROFILE)
try:
 store=TaskStore(q,root/'inputs'); ledger=ScratchLedger(root/'ledger.sqlite',root,capacity_probe=lambda _:MAX_UPLOAD_BYTES+1000,safety_floor_bytes=100,clock=lambda:1)
 gate=ViewerUploadCapacity(ledger,store,root,synthetic_only=True)
 if phase=='first':
  store.initialize(ident,'synthetic',[],None,'production')
  with gate.upload(ident,'production',3,3):
   gate.file(ident,'production',3,'a.jpg',3);store.upload(ident,'a.jpg',io.BytesIO(b'abc'),3)
 with gate.commit(ident,'production',3): store.commit(ident)
 job=store.task(ident)['job_id'];print(json.dumps({'job':job}),flush=True)
 if phase=='first':os._exit(73)
finally:q.close()
'''
        with tempfile.TemporaryDirectory() as root:
            first=subprocess.run([sys.executable,'-c',script,root,'first'],capture_output=True,text=True,timeout=20)
            self.assertEqual(first.returncode,73,msg=first.stderr)
            second=subprocess.run([sys.executable,'-c',script,root,'retry'],capture_output=True,text=True,timeout=20)
            self.assertEqual(second.returncode,0,msg=second.stderr)
            self.assertEqual(first.stdout,second.stdout)

    def test_reserve_recheck_partial_retry_and_commit(self):
        with tempfile.TemporaryDirectory() as root:
            store = Store(root)
            try:
                free = [MAX_UPLOAD_BYTES + 1000]
                ledger = ScratchLedger(Path(root)/'scratch.sqlite',root,capacity_probe=lambda _:free[0],safety_floor_bytes=100,clock=lambda:1)
                with self.assertRaisesRegex(ValueError,'cross-process'):
                    ViewerUploadCapacity(ledger,store,root)
                gate = ViewerUploadCapacity(ledger,store,root,synthetic_only=True)
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
                replacement = ViewerUploadCapacity(reopened,store,root,synthetic_only=True)
                with self.assertRaises(PolicyConflict):
                    with replacement.upload('job1','production',21,10): pass
                with reopened._transaction() as db:
                    self.assertEqual(db.execute('SELECT state FROM reservations').fetchone()[0], 'uncertain')
                for invalid in (float('nan'), -1, True, 1.5):
                    free[0] = invalid
                    with self.subTest(free=invalid), self.assertRaises((PolicyConflict, ValueError)):
                        with gate.upload('job1','production',20,10): pass
            finally: store.db.close()
