"""Disposable exact-source upgrade/rollback investigation; never uses live state."""
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import unittest
import uuid

from processing.prototype import Queue
from processing.viewer_tasks import TaskStore

MAIN = '7200fbf49908820f6b76d77a164fd23e191655ae'
PREDECESSOR = 'ad7f0f57ee9c8e0803430ea817a1349e12c94e01'

MAIN_SEED = r'''
import json,sys
from processing.prototype import Queue, PROFILE
q=Queue(sys.argv[1], node_ids=('cluster','main'))
done=q.submit({'requestId':'terminal','environment':'staging','profile':PROFILE})[0]
claim=q.claim({'workerId':'legacy','profiles':[PROFILE]})
q.complete(done['id'], {'claimToken':claim['claimToken'],'status':'succeeded','result':{'synthetic':'retained'}})
running=q.submit({'requestId':'running','environment':'staging','profile':PROFILE})[0]
q.claim({'workerId':'legacy','profiles':[PROFILE]})
queued=q.submit({'requestId':'queued','environment':'staging','profile':PROFILE})[0]
print(json.dumps({'terminal':done['id'],'running':running['id'],'queued':queued['id'],'authority':q.authority_id}))
q.close()
'''

VIEWER_SEED = r'''
import hashlib,io,json,sys,uuid
from pathlib import Path
from processing.prototype import Queue, VIEWER_PROFILE
from processing.viewer_tasks import TaskStore
root=Path(sys.argv[1]);q=Queue(root,node_ids=('cluster','main'),viewer_enabled=True)
s=TaskStore(q,root/'viewer-tasks');ids=[str(uuid.uuid4()) for _ in range(3)]
def create(identifier):
 s.initialize(identifier,'Synthetic',[],None,'staging')
 s.upload(identifier,'photo.jpg',io.BytesIO(b'synthetic-photo'),15)
 s.commit(identifier)
create(ids[0]);first=q.claim({'workerId':'legacy-viewer','nodeId':'cluster','profiles':[VIEWER_PROFILE]})
assert first['id']==s.task(ids[0])['job_id']
s.update(ids[0],stage='running',phase='processing',node_id='cluster',engine_uuid=str(uuid.uuid4()),claim_token=first['claimToken'])
q.complete(first['id'],{'claimToken':first['claimToken'],'status':'succeeded','result':{}})
archive=Path(s.task(ids[0])['directory'])/'all.zip';archive.write_bytes(b'synthetic-archive')
digest=hashlib.sha256(archive.read_bytes()).hexdigest()
s.update(ids[0],stage='succeeded',phase='complete',archive_path=str(archive),archive_sha256=digest)
receipt={'eventId':'synthetic-event','providerTaskId':ids[0],'attemptId':'synthetic-attempt',
 'archiveSha256':digest,'resultModelId':'synthetic-model','resultModelVersionId':'synthetic-version',
 'ingestedAt':'2026-11-10T00:00:00Z'}
assert s.accept_viewer_delivery(ids[0],'staging',receipt)['accepted']
create(ids[1]);second=q.claim({'workerId':'legacy-viewer','nodeId':'cluster','profiles':[VIEWER_PROFILE]})
assert second['id']==s.task(ids[1])['job_id']
s.update(ids[1],stage='running',phase='processing',node_id='cluster',engine_uuid=str(uuid.uuid4()),claim_token=second['claimToken'])
create(ids[2]);print(json.dumps({'terminal':ids[0],'running':ids[1],'queued':ids[2],'authority':q.authority_id,'digest':digest,'receipt':receipt}))
q.close()
'''

OLD_LOCK_CHECK = r'''
import json,sys
from processing.prototype import Queue
try: q=Queue(sys.argv[1],node_ids=('cluster','main'))
except RuntimeError as e:
 print(json.dumps({'blocked':'already owned' in str(e)}))
else:
 q.close();print(json.dumps({'blocked':False}))
'''

OLD_AFTER_RELEASE = r'''
import json,sys
from processing.prototype import Queue,PROFILE
q=Queue(sys.argv[1],node_ids=('cluster','main'))
job,created=q.submit({'requestId':'legacy-post-release','environment':'staging','profile':PROFILE})
print(json.dumps({'reopened':True,'created':created,'id':job['id']}));q.close()
'''


@unittest.skipUnless(os.environ.get('PHOTO_COMPAT_GIT_DIR'), 'requires explicit local Git object store')
class PhotoStateCompatibilityCandidate(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='photo-state-compat-')
        self.root = Path(self.temp.name)
        self.addCleanup(self.temp.cleanup)

    def snapshot(self, commit, *, viewer):
        target = self.root/('predecessor' if viewer else 'old-main')
        target.mkdir()
        paths = ['processing/__init__.py', 'processing/prototype.py', 'processing/maintenance.py']
        if viewer:
            paths.append('processing/viewer_tasks.py')
        result = subprocess.run(['git', '--git-dir='+os.environ['PHOTO_COMPAT_GIT_DIR'], 'archive',
                                 '--format=tar', commit, *paths], capture_output=True, check=True, timeout=20)
        with tarfile.open(fileobj=io.BytesIO(result.stdout), mode='r:') as archive:
            archive.extractall(target, filter='data')
        return target

    def invoke(self, source, script, state):
        env = dict(os.environ, PYTHONPATH=str(source), PYTHONDONTWRITEBYTECODE='1')
        result = subprocess.run([sys.executable, '-B', '-c', script, str(state)], cwd=self.root,
                                env=env, capture_output=True, text=True, timeout=15, check=True)
        return json.loads(result.stdout)

    def test_missing_legacy_queue_columns_are_migrated_without_losing_jobs(self):
        state = self.root/'older-schema'
        state.mkdir()
        with sqlite3.connect(state/'jobs.sqlite3') as db:
            db.execute('''CREATE TABLE jobs(seq INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT UNIQUE NOT NULL,
                request_id TEXT UNIQUE NOT NULL,environment TEXT NOT NULL,profile TEXT NOT NULL,status TEXT NOT NULL,
                worker_id TEXT,claim_hash TEXT,result TEXT,created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP)''')
            for name, status, result in (('queued','queued',None), ('running','running',None),
                                         ('terminal','succeeded','{"synthetic":"retained"}')):
                db.execute('INSERT INTO jobs(id,request_id,environment,profile,status,result) VALUES(?,?,?,?,?,?)',
                           (name,name,'staging','treedar-synthetic-v1',status,result))
        queue = Queue(state, node_ids=('cluster','main'), viewer_enabled=True)
        try:
            with queue._db() as db:
                columns = {row['name'] for row in db.execute('PRAGMA table_info(jobs)')}
                self.assertTrue({'node_id', 'held_reason'} <= columns)
                self.assertEqual(db.execute('PRAGMA integrity_check').fetchone()[0], 'ok')
            self.assertEqual(queue.get('queued')['status'], 'queued')
            self.assertEqual((queue.get('running')['status'],queue.get('running')['nodeId']), ('held','cluster'))
            self.assertEqual(queue.get('terminal')['result'], {'synthetic':'retained'})
        finally:
            queue.close()

    def test_main_origin_state_is_retained_and_running_is_held(self):
        old = self.snapshot(MAIN, viewer=False)
        state = self.root/'main-state'
        original = self.invoke(old, MAIN_SEED, state)
        queue = Queue(state, node_ids=('cluster','main'), viewer_enabled=True)
        try:
            store = TaskStore(queue, state/'viewer-tasks')
            self.assertEqual(queue.authority_id, original['authority'])
            self.assertEqual(queue.get(original['terminal'])['status'], 'succeeded')
            self.assertEqual(queue.get(original['terminal'])['result'], {'synthetic':'retained'})
            self.assertEqual(queue.get(original['running'])['status'], 'held')
            self.assertEqual(queue.get(original['running'])['heldReason'], 'coordinator_restart')
            self.assertEqual(queue.get(original['queued'])['status'], 'queued')
            self.assertEqual(store.list_tasks(), [])
            with queue._db() as db:
                self.assertEqual(db.execute('PRAGMA integrity_check').fetchone()[0], 'ok')
        finally:
            queue.close()

    def test_predecessor_viewer_state_and_receipt_survive_candidate_startup(self):
        predecessor = self.snapshot(PREDECESSOR, viewer=True)
        state = self.root/'viewer-state'
        original = self.invoke(predecessor, VIEWER_SEED, state)
        queue = Queue(state, node_ids=('cluster','main'), viewer_enabled=True)
        try:
            store = TaskStore(queue, state/'viewer-tasks')
            terminal = store.task(original['terminal'])
            running = store.task(original['running'])
            queued = store.task(original['queued'])
            self.assertEqual(queue.authority_id, original['authority'])
            self.assertEqual((terminal['job']['status'], terminal['stage'], terminal['phase']), ('succeeded','succeeded','complete'))
            self.assertEqual((running['job']['status'], running['job']['heldReason']), ('held','coordinator_restart'))
            self.assertEqual((queued['job']['status'], queued['phase']), ('queued','queued'))
            for task in (terminal, running, queued):
                self.assertEqual(Path(task['files'][0]['path']).read_bytes(), b'synthetic-photo')
            self.assertEqual(Path(terminal['archive_path']).read_bytes(), b'synthetic-archive')
            self.assertEqual(terminal['archive_sha256'], original['digest'])
            self.assertTrue(store.accept_viewer_delivery(original['terminal'],'staging',original['receipt'])['replayed'])
            with queue._db() as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM viewer_delivery_acceptances').fetchone()[0], 1)
                self.assertEqual(db.execute('PRAGMA integrity_check').fetchone()[0], 'ok')
        finally:
            queue.close()

    def test_old_writer_is_locked_only_while_candidate_owns_state(self):
        for source, viewer in ((MAIN, False), (PREDECESSOR, True)):
            with self.subTest(source=source):
                old = self.snapshot(source, viewer=viewer)
                state = self.root/('predecessor-lock' if viewer else 'main-lock')
                queue = Queue(state, node_ids=('cluster','main'), viewer_enabled=True)
                try:
                    store = TaskStore(queue, state/'viewer-tasks')
                    task_id = str(uuid.uuid4())
                    store.initialize(task_id, 'Synthetic', [], None, 'staging')
                    store.upload(task_id, 'photo.jpg', io.BytesIO(b'synthetic-photo'), 15)
                    store.commit(task_id)
                    running = queue.claim({'workerId':'candidate', 'nodeId':'cluster', 'profiles':['odm-viewer-v1']})
                    self.assertEqual(running['id'], store.task(task_id)['job_id'])
                    self.assertEqual(self.invoke(old, OLD_LOCK_CHECK, state), {'blocked':True})
                finally:
                    queue.close()
                # This is an intentionally documented gap, not proof rollback is safe:
                # the old binary opens and mutates the upgraded state after release.
                after = self.invoke(old, OLD_AFTER_RELEASE, state)
                self.assertTrue(after['reopened'])
                self.assertTrue(after['created'])
                reopened = Queue(state, node_ids=('cluster','main'), viewer_enabled=True)
                try:
                    task = TaskStore(reopened, state/'viewer-tasks').task(task_id)
                    self.assertEqual(task['job']['status'], 'held')
                    self.assertEqual(Path(task['files'][0]['path']).read_bytes(), b'synthetic-photo')
                finally:
                    reopened.close()


if __name__ == '__main__':
    unittest.main()
