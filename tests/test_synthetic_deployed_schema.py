"""Non-production migration rehearsal against a separately exported predecessor."""
import hashlib
import json
import os
from dataclasses import replace
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest

from processing.prototype import Queue
from processing.app import make_handler
from processing.viewer_service import ViewerService
from processing.viewer_tasks import TaskStore


PREDECESSOR = os.environ.get('SYNTHETIC_PREDECESSOR_ROOT')


@unittest.skipUnless(PREDECESSOR, 'exact predecessor source export required')
class SyntheticDeployedSchemaTests(unittest.TestCase):
    def test_nonempty_schema_migration_restart_and_read_only_predecessor_inspection(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            state = root / 'state'
            data = state / 'viewer-tasks'
            seed = r'''
import io,json,sys,uuid,zipfile
from pathlib import Path
from processing.prototype import Queue
from processing.viewer_tasks import TaskStore
state,data=map(Path,sys.argv[1:]);q=Queue(state,node_ids=('cluster','main'),viewer_enabled=True)
s=TaskStore(q,data); ids=[str(uuid.uuid4()),str(uuid.uuid4())]
for identifier in ids:
 s.initialize(identifier, name='synthetic', options=[], environment='production')
 s.upload(identifier,'synthetic.jpg',io.BytesIO(b'photo-fixture'),13)
 s.commit(identifier)
first=s.task(ids[0]);second=s.task(ids[1])
archive=Path(first['directory'])/'all.zip'
with zipfile.ZipFile(archive,'w') as z:z.writestr('synthetic.txt','synthetic result')
with q._db() as db:
 db.execute("UPDATE jobs SET status='succeeded' WHERE id=?",(first['job_id'],))
 db.execute("UPDATE jobs SET status='running',node_id='cluster' WHERE id=?",(second['job_id'],))
 db.execute("UPDATE viewer_tasks SET stage='succeeded',phase='complete',node_id='cluster',engine_uuid=?,archive_path=? WHERE uuid=?",(str(uuid.uuid4()),str(archive),ids[0]))
 db.execute("UPDATE viewer_tasks SET stage='running',phase='processing',node_id='cluster',engine_uuid=?,claim_token='synthetic-claim' WHERE uuid=?",(str(uuid.uuid4()),ids[1]))
q.close();print(json.dumps({'ids':ids,'archive':str(archive)}))
'''
            env = dict(os.environ, PYTHONPATH=PREDECESSOR)
            result = subprocess.run([sys.executable, '-c', seed, str(state), str(data)], cwd=PREDECESSOR,
                                    env=env, capture_output=True, text=True, timeout=20, check=True)
            seeded = json.loads(result.stdout)
            archive = Path(seeded['archive'])
            before = hashlib.sha256(archive.read_bytes()).hexdigest()
            database = state / 'jobs.sqlite3'
            with sqlite3.connect(database) as db:
                old_columns = {row[1] for row in db.execute('PRAGMA table_info(viewer_tasks)')}
            self.assertNotIn('archive_state', old_columns)
            queue = Queue(state, node_ids=('cluster', 'main'), viewer_enabled=True)
            try:
                store = TaskStore(queue, data)
                store.enable_archive_status()
                complete, held = [store.task(identifier) for identifier in seeded['ids']]
                self.assertEqual(complete['job']['status'], 'succeeded')
                self.assertEqual(held['job']['status'], 'held')
                self.assertEqual(held['job']['heldReason'], 'coordinator_restart')
                for task in (complete, held):
                    self.assertEqual(Path(task['files'][0]['path']).read_bytes(), b'photo-fixture')
                self.assertEqual(hashlib.sha256(archive.read_bytes()).hexdigest(), before)
                with sqlite3.connect(database) as db:
                    columns = {row[1] for row in db.execute('PRAGMA table_info(viewer_tasks)')}
                self.assertEqual(columns - old_columns, {'archive_state', 'archive_reason'})
                # Receipt is candidate-era synthetic data. The predecessor is
                # never started as a writer and no state is deleted.
                receipt = archive.with_suffix('.zip.receipt')
                service = object.__new__(ViewerService)
                service.config = SimpleNamespace(viewer_max_archive_bytes=1024 ** 2)
                value = {**service._archive_identity(complete), 'bytes': archive.stat().st_size, 'sha256': before}
                receipt.write_text(json.dumps(value, sort_keys=True, separators=(',', ':')), encoding='ascii')
                receipt.chmod(0o600)
                self.assertEqual(service._archive_receipt(complete, receipt), value)
                receipt_hash = hashlib.sha256(receipt.read_bytes()).hexdigest()
            finally:
                queue.close()
            queue = Queue(state, node_ids=('cluster', 'main'), viewer_enabled=True)
            try:
                store = TaskStore(queue, data)
                store.enable_archive_status()
                self.assertEqual(store.task(seeded['ids'][1])['job']['status'], 'held')
                self.assertEqual(hashlib.sha256(receipt.read_bytes()).hexdigest(), receipt_hash)
                self.assertEqual(hashlib.sha256(archive.read_bytes()).hexdigest(), before)
            finally:
                queue.close()
            # Read-only SQLite inspection is not a predecessor rollback test.
            with sqlite3.connect(f'file:{database}?mode=ro', uri=True) as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM viewer_tasks').fetchone()[0], 2)
                self.assertEqual(db.execute("SELECT status FROM jobs WHERE status='held'").fetchone()[0], 'held')
            from tests.test_reference_runtime import ReferenceRuntimeTests
            fixture = ReferenceRuntimeTests('runTest')
            fixture.setUp()
            try:
                config = replace(fixture.config, prototype_state_dir=str(state), reference_config_path=str(fixture.path))
                first, second = fixture.tls_patches()
                with first, second:
                    handler = make_handler(config)
                try:
                    self.assertIsNotNone(handler.reference_listener)
                    self.assertEqual(handler.viewer_service.store.task(seeded['ids'][1])['job']['status'], 'held')
                    self.assertEqual(hashlib.sha256(archive.read_bytes()).hexdigest(), before)
                    self.assertEqual(hashlib.sha256(receipt.read_bytes()).hexdigest(), receipt_hash)
                finally:
                    handler.reference_listener.close()
                    handler.viewer_service.close()
                    handler.prototype_queue.close()
            finally:
                fixture.doCleanups()


if __name__ == '__main__':
    unittest.main()
