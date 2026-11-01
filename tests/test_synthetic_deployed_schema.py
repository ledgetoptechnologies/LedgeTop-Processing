"""Non-production migration rehearsal against a separately exported predecessor."""
import hashlib
import json
import os
from dataclasses import replace
from pathlib import Path
import re
import socket
import sqlite3
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
import uuid

from processing.prototype import Queue
from processing.app import make_handler
from processing.viewer_service import ViewerService
from processing.viewer_tasks import TaskStore


PREDECESSOR = os.environ.get('SYNTHETIC_PREDECESSOR_ROOT')


@unittest.skipUnless(PREDECESSOR, 'exact predecessor source export required')
class SyntheticDeployedSchemaTests(unittest.TestCase):
    def test_nonempty_schema_migration_restart_and_read_only_predecessor_inspection(self):
        # This fixture starts predecessor code. Refuse to run unless the test
        # container has no non-loopback interfaces (e.g. Docker --network none).
        # This alone does not prove absence of services listening on loopback.
        self.assertEqual([name for _, name in socket.if_nameindex()], ['lo'])
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
            self.assertEqual(len(seeded['ids']), 2)
            for identifier in seeded['ids']:
                self.assertEqual(str(uuid.UUID(identifier)), identifier)
            expected = data / (seeded['ids'][0] + '-1') / 'all.zip'
            self.assertEqual(archive, expected)
            for path in (root, state, data, expected.parent, expected):
                self.assertFalse(path.is_symlink())
            self.assertTrue(expected.resolve(strict=True).is_relative_to(root.resolve(strict=True)))
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
                    source = Path(task['files'][0]['path'])
                    self.assertEqual(task['files'][0]['name'], 'synthetic.jpg')
                    self.assertEqual(source.parent, data / (task['uuid'] + '-1'))
                    self.assertIsNotNone(re.fullmatch(r'input-[0-9a-f]{32}', source.name))
                    self.assertFalse(source.is_symlink())
                    self.assertTrue(source.resolve(strict=True).is_relative_to(root.resolve(strict=True)))
                    self.assertEqual(source.read_bytes(), b'photo-fixture')
                self.assertEqual(hashlib.sha256(archive.read_bytes()).hexdigest(), before)
                with sqlite3.connect(database) as db:
                    columns = {row[1] for row in db.execute('PRAGMA table_info(viewer_tasks)')}
                self.assertEqual(columns - old_columns, {'archive_state', 'archive_reason'})
                # Receipt is candidate-era synthetic data; no state is deleted.
                receipt = archive.with_suffix('.zip.receipt')
                self.assertFalse(receipt.exists() or receipt.is_symlink())
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
                # Exercise the exact predecessor dispatcher only after candidate
                # shutdown, in the network-isolated synthetic test container.
                # An orphan partial is retained without issuing a download.
                partial = archive.parent / 'all.zip.partial'
                self.assertFalse(partial.exists() or partial.is_symlink())
                partial.write_bytes(b'synthetic-uncertain-partial')
                old_restart = r'''
import json,ssl,sys,time
from unittest.mock import patch
from processing.app import make_handler
from processing.config import Config
state,reference,complete,held=sys.argv[1:]
config=Config(api_token='legacy-api-'+'a'*32,local_node_url='http://127.0.0.1:19001',
 main_node_url='http://127.0.0.1:19002',local_max_photos=1000,local_max_input_bytes=20*1024**3,
 prototype_state_dir=state,prototype_node_ids=('cluster','main'),treedar_node_ids=('main',),
 viewer_enabled=True,viewer_token='legacy-native-viewer-'+'b'*32,reference_config_path=reference)
client_context=ssl.create_default_context()
with patch('processing.reference_runtime.ssl.SSLContext.load_cert_chain'), \
     patch('processing.reference_runtime.verified_client_context',return_value=client_context), \
     patch('processing.viewer_service.NativeEngine.download',side_effect=AssertionError('unexpected download')) as downloads:
 h=make_handler(config)
 try:
  assert h.reference_listener is not None
  h.viewer_service.start();time.sleep(3)
  done=h.viewer_service.store.task(complete);active=h.viewer_service.store.task(held)
  print(json.dumps({'complete':done['job']['status'],'held':active['job']['status'],
                    'reason':active['job']['heldReason'],'phase':active['phase'],
                    'jobs':len(h.prototype_queue.list()),'downloads':downloads.call_count}))
 finally:
  h.reference_listener.close();h.viewer_service.close();h.prototype_queue.close()
'''
                old = subprocess.run([sys.executable, '-c', old_restart, str(state), str(fixture.path), *seeded['ids']],
                                     cwd=PREDECESSOR, env=env, capture_output=True, text=True, timeout=15)
                self.assertEqual(old.returncode, 0, 'synthetic predecessor failed:\n' + old.stderr[-4000:])
                observed = json.loads(old.stdout)
                self.assertEqual(observed, {'complete': 'succeeded', 'held': 'held',
                                            'reason': 'coordinator_restart', 'phase': 'processing', 'jobs': 2,
                                            'downloads': 0})
                self.assertEqual(partial.read_bytes(), b'synthetic-uncertain-partial')
                self.assertEqual(hashlib.sha256(archive.read_bytes()).hexdigest(), before)
                self.assertEqual(hashlib.sha256(receipt.read_bytes()).hexdigest(), receipt_hash)
            finally:
                fixture.doCleanups()


if __name__ == '__main__':
    unittest.main()
