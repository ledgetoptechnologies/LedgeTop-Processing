"""Small local SQLite/Queue tests; retained temp root, no recursive cleanup."""
import io
import sqlite3
import tempfile
from pathlib import Path
import threading
import unittest
import uuid

from processing.prototype import Queue, VIEWER_PROFILE
from processing.viewer_tasks import TaskStore
from processing.transfer_fence import install_offline, attest, TransferFenceError


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
            with queue.pin_transfer():
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
        finally:
            queue.close()
        print('retained synthetic test root:', root)

    def test_schema_tampering_fails_attestation(self):
        db = sqlite3.connect(':memory:')
        try:
            db.execute('CREATE TABLE jobs(id TEXT PRIMARY KEY,status TEXT)')
            db.execute('CREATE TABLE viewer_tasks(job_id TEXT)')
            install_offline(db, enabled=True)
            attest(db)
            db.execute("INSERT INTO jobs VALUES('job','running')")
            db.execute("INSERT INTO viewer_tasks VALUES('job')")
            db.execute("INSERT INTO engine_ingress_uploads VALUES('upload','job','engine',1,?,'started')", ('a' * 64,))
            with self.assertRaises(sqlite3.IntegrityError): db.execute("UPDATE jobs SET status='failed' WHERE id='job'")
            with self.assertRaises(sqlite3.IntegrityError): db.execute("DELETE FROM viewer_tasks WHERE job_id='job'")
            db.execute("UPDATE engine_ingress_uploads SET state='complete' WHERE upload_id='upload'")
            db.execute("UPDATE jobs SET status='succeeded' WHERE id='job'")
            db.execute('DROP TRIGGER guard_started_job_update')
            with self.assertRaises(TransferFenceError): attest(db)
        finally: db.close()


if __name__ == '__main__': unittest.main()
