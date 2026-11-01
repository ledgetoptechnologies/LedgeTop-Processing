"""Small local SQLite/Queue tests; retained temp root, no recursive cleanup."""
import io
import sqlite3
import tempfile
from pathlib import Path
import threading
import time
import unittest
import uuid

from processing.prototype import Queue, VIEWER_PROFILE
from processing.viewer_tasks import TaskStore
from processing.transfer_fence import install_offline, attest, TransferFenceError, JOBS_DDL


class TransferFenceTests(unittest.TestCase):
    def test_running_upload_blocks_mutations_and_pins_coordinator(self):
        root = Path(tempfile.mkdtemp(prefix='lt-transfer-fence-candidate-')).resolve()
        queue = Queue(root / 'queue', viewer_enabled=True)
        try:
            store = TaskStore(queue, root / 'inputs')
            with queue._db() as db: install_offline(db, enabled=True)
            identifier = str(uuid.uuid4())
            store.initialize(identifier, 'Synthetic', [], None, 'production')
            store.upload(identifier, 'photo.jpg', io.BytesIO(b'x'), 1)
            store.commit(identifier)
            claim = queue.claim({'workerId': 'viewer-dispatch', 'nodeId': 'cluster', 'profiles': [VIEWER_PROFILE]})
            store.update(identifier, stage='running', phase='upload_pending', node_id='cluster',
                         engine_uuid=str(uuid.uuid4()), claim_token=claim['claimToken'])
            with queue._db() as db:
                db.execute("INSERT INTO engine_ingress_uploads VALUES(?,?,?,?,?,'started')",
                    (str(uuid.uuid4()), claim['id'], str(uuid.uuid4()), 1, 'a' * 64))
            with queue.pin_transfer(time.monotonic() + 2):
                with self.assertRaises(RuntimeError): queue.close()
                failures = []
                def competing():
                    for operation in (
                            lambda: queue.complete(claim['id'], {'claimToken': claim['claimToken'],
                                'status': 'succeeded', 'result': {}}),
                            lambda: queue.operator(claim['id'], 'hold', {'note': 'synthetic'}),
                            lambda: store.update(identifier, phase='processing'),
                            lambda: store.cancel(identifier), lambda: store.remove(identifier)):
                        try: operation(); failures.append('unexpected success')
                        except Exception as error: failures.append(type(error).__name__)
                thread = threading.Thread(target=competing); thread.start(); thread.join(3)
                self.assertFalse(thread.is_alive())
                self.assertEqual(len(failures), 5)
                self.assertNotIn('unexpected success', failures)
                self.assertEqual(queue.get(claim['id'])['status'], 'running')
                self.assertEqual(store.task(identifier)['phase'], 'upload_pending')
                with queue._db() as db:
                    attest(db)
                    self.assertEqual(db.execute("SELECT state FROM engine_ingress_uploads").fetchone()[0], 'started')
            queue.close()
            queue = Queue(root / 'queue', viewer_enabled=True)
            self.assertEqual(queue.get(claim['id'])['status'], 'held')
            self.assertEqual(queue.get(claim['id'])['heldReason'], 'coordinator_restart')
            with self.assertRaises(sqlite3.IntegrityError):
                queue.complete(claim['id'], {'claimToken': claim['claimToken'], 'status': 'succeeded', 'result': {}})
        finally:
            if not queue._lock.closed: queue.close()
        print('retained synthetic test root:', root)

    def test_schema_tampering_fails_attestation(self):
        db = sqlite3.connect(':memory:')
        try:
            db.execute(JOBS_DDL)
            db.execute('CREATE TABLE viewer_tasks(uuid TEXT,job_id TEXT)')
            db.execute('CREATE TABLE viewer_files(uuid TEXT,name TEXT)')
            install_offline(db, enabled=True)
            attest(db)
            db.execute("INSERT INTO jobs(id,request_id,environment,profile,status) VALUES('job','req1','staging','photo','running')")
            db.execute("INSERT INTO viewer_tasks VALUES('task','job')")
            db.execute("INSERT INTO viewer_files VALUES('task','photo')")
            db.execute("INSERT INTO engine_ingress_uploads VALUES('upload','job','engine',1,?,'started')", ('a' * 64,))
            db.execute("INSERT INTO engine_ingress_uploads VALUES('upload2','missing','engine',1,?,'started')", ('a' * 64,))
            with self.assertRaises(sqlite3.IntegrityError): db.execute("INSERT INTO jobs(id,request_id,environment,profile,status) VALUES('missing','req2','staging','photo','queued')")
            db.execute("INSERT INTO jobs(id,request_id,environment,profile,status) VALUES('other','req3','staging','photo','queued')")
            with self.assertRaises(sqlite3.IntegrityError): db.execute("UPDATE jobs SET id='missing' WHERE id='other'")
            with self.assertRaises(sqlite3.IntegrityError): db.execute("UPDATE jobs SET status='failed' WHERE id='job'")
            with self.assertRaises(sqlite3.IntegrityError): db.execute("UPDATE jobs SET status='held',held_reason=NULL WHERE id='job'")
            with self.assertRaises(sqlite3.IntegrityError): db.execute("UPDATE jobs SET node_id='cluster' WHERE id='job'")
            db.execute("ALTER TABLE jobs ADD COLUMN future_column TEXT")
            with self.assertRaises(TransferFenceError): attest(db)
            with self.assertRaises(sqlite3.IntegrityError): db.execute("UPDATE jobs SET status='held',held_reason='coordinator_restart',future_column='changed' WHERE id='job'")
            with self.assertRaises(sqlite3.IntegrityError): db.execute("DELETE FROM viewer_tasks WHERE job_id='job'")
            with self.assertRaises(sqlite3.IntegrityError): db.execute("INSERT INTO viewer_tasks VALUES('new','job')")
            db.execute("INSERT INTO viewer_tasks VALUES('other-task','other')")
            with self.assertRaises(sqlite3.IntegrityError): db.execute("UPDATE viewer_tasks SET job_id='job' WHERE job_id='other'")
            with self.assertRaises(sqlite3.IntegrityError): db.execute("DELETE FROM viewer_files WHERE uuid='task'")
            with self.assertRaises(sqlite3.IntegrityError): db.execute("INSERT INTO viewer_files VALUES('task','extra')")
            db.execute("INSERT INTO viewer_files VALUES('other-task','photo')")
            with self.assertRaises(sqlite3.IntegrityError): db.execute("UPDATE viewer_files SET uuid='task' WHERE uuid='other-task'")
            db.execute("UPDATE engine_ingress_uploads SET state='complete' WHERE upload_id='upload'")
            db.execute("UPDATE jobs SET status='succeeded' WHERE id='job'")
            db.execute('DROP TRIGGER guard_started_job_update')
            with self.assertRaises(TransferFenceError): attest(db)
        finally: db.close()

    def test_pin_acquisition_obeys_deadline(self):
        root = Path(tempfile.mkdtemp(prefix='lt-transfer-pin-candidate-')).resolve()
        queue = Queue(root / 'queue', viewer_enabled=True)
        locked, release = threading.Event(), threading.Event()
        def owner():
            with queue._lifecycle_lock:
                locked.set(); release.wait(2)
        thread = threading.Thread(target=owner); thread.start()
        try:
            self.assertTrue(locked.wait(1))
            start = time.monotonic()
            with self.assertRaises(RuntimeError):
                with queue.pin_transfer(start + .05): pass
            self.assertLess(time.monotonic() - start, .5)
        finally:
            release.set(); thread.join(2)
            self.assertFalse(thread.is_alive())
            queue.close()
        print('retained synthetic test root:', root)


if __name__ == '__main__': unittest.main()
