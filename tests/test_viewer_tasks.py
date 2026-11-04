import io
import os
import subprocess
import sys
import threading
from pathlib import Path
import tempfile
import unittest
import uuid

from processing.maintenance import Conflict
from processing.prototype import PROFILE, Queue
from processing.viewer_tasks import InputLimitExceeded, TaskStore, VIEWER_PROFILE


class ViewerTaskStoreTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.queue = Queue(self.root / 'queue', node_ids=('cluster', 'main'), treedar_node_ids=('main',),
                           viewer_outbound_parallel=self._testMethodName == 'test_stalled_upload_releases_store_but_serializes_other_uploads')
        self.queue.allowed_profiles.add(VIEWER_PROFILE)
        self.store = TaskStore(self.queue, self.root / 'inputs')
        self.identifier = str(uuid.uuid4())

    def tearDown(self):
        self.queue.close()
        self.temporary.cleanup()

    def initialize(self, identifier=None, environment='production'):
        identifier = identifier or self.identifier
        return self.store.initialize(identifier, 'Synthetic survey', [{'name': 'pc-ept', 'value': True}],
                                     ['all.zip'], environment)

    def photo(self, identifier=None, name='photo.jpg', data=b'owned synthetic bytes'):
        identifier = identifier or self.identifier
        return self.store.upload(identifier, name, io.BytesIO(data), len(data))

    def ready(self, identifier=None, environment='production'):
        identifier = identifier or self.identifier
        self.initialize(identifier, environment)
        self.photo(identifier)
        self.store.commit(identifier)
        return self.store.task(identifier)

    def claim(self, node='cluster'):
        return self.queue.claim({'workerId': 'viewer-broker', 'nodeId': node, 'profiles': [VIEWER_PROFILE]})

    def test_stalled_upload_releases_store_but_serializes_other_uploads(self):
        other = str(uuid.uuid4())
        self.initialize()
        self.initialize(other)
        direct_store = TaskStore(self.queue, self.root / 'inputs')
        entered, release, second_entered = threading.Event(), threading.Event(), threading.Event()
        errors = []

        class SlowInput(io.BytesIO):
            def read(inner, n=-1):
                entered.set()
                if not release.wait(5): raise RuntimeError('fixture timed out')
                return super().read(n)

        class SecondInput(io.BytesIO):
            def read(inner, n=-1):
                second_entered.set()
                return super().read(n)

        def run(identifier, stream):
            try: (self.store if identifier == self.identifier else direct_store).upload(identifier, 'photo.jpg', stream, 3)
            except BaseException as error: errors.append(error)

        first = threading.Thread(target=run, args=(self.identifier, SlowInput(b'abc')))
        second = threading.Thread(target=run, args=(other, SecondInput(b'def')))
        first.start()
        try:
            self.assertTrue(entered.wait(2))
            second.start()
            self.store.update(other, progress=1)
            with self.assertRaises(Conflict): direct_store.commit(self.identifier)
            self.assertFalse(second_entered.wait(0.2))
        finally:
            release.set()
            first.join(5)
            if second.ident is not None: second.join(5)
        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertEqual(errors, [])
        self.assertTrue(second_entered.is_set())

    def test_initialization_is_durable_idempotent_and_settings_bound(self):
        self.initialize()
        self.initialize()
        store = TaskStore(self.queue, self.root / 'inputs')
        self.assertEqual(store.info(self.identifier)['status']['code'], 10)
        self.assertEqual(len(store.list_tasks()), 1)
        for settings in (('Different', [], None, 'production'),
                         ('Synthetic survey', [{'name': 'pc-ept', 'value': True}], ['all.zip'], 'staging')):
            with self.subTest(settings=settings), self.assertRaises(Conflict):
                store.initialize(self.identifier, *settings)

    def test_process_exit_after_directory_creation_preserves_orphan_and_reservation(self):
        root=self.root/'process-exit';identifier=str(uuid.uuid4());cap='YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWE'
        program="""import os,sys
from pathlib import Path
from unittest.mock import patch
from processing.prototype import Queue
from processing.viewer_tasks import TaskStore
root=Path(sys.argv[1]);identifier=sys.argv[2];cap=sys.argv[3]
queue=Queue(root/'queue',node_ids=('cluster','main'),treedar_node_ids=('main',),viewer_enabled=True)
store=TaskStore(queue,root/'inputs');store.reserve(identifier,'production',cap)
with patch('processing.viewer_tasks.sync_directory',side_effect=lambda _path:os._exit(73)):
 store.initialize(identifier,'synthetic',[],None,'production',cap,option_evidence=[])
"""
        proc=subprocess.run([sys.executable,'-c',program,str(root),identifier,cap],capture_output=True,timeout=15)
        self.assertEqual(proc.returncode,73)
        orphan=root/'inputs'/(identifier+'-1');self.assertTrue(orphan.is_dir())
        queue=Queue(root/'queue',node_ids=('cluster','main'),treedar_node_ids=('main',),viewer_enabled=True)
        try:
            store=TaskStore(queue,root/'inputs')
            with queue._db() as db:
                self.assertEqual(db.execute('SELECT count(*) FROM viewer_tasks WHERE uuid=?',(identifier,)).fetchone()[0],0)
                self.assertEqual(db.execute('SELECT state FROM viewer_reservations WHERE uuid=?',(identifier,)).fetchone()[0],'reserved')
            self.assertEqual(store.initialize(identifier,'synthetic',[],None,'production',cap,option_evidence=[]),{'uuid':identifier})
            with queue._db() as db:directory=db.execute('SELECT directory FROM viewer_tasks WHERE uuid=?',(identifier,)).fetchone()[0]
            self.assertTrue(orphan.is_dir());self.assertNotEqual(directory,orphan.name);self.assertTrue((root/'inputs'/directory).is_dir())
        finally:queue.close()

    def test_reserved_replay_rejects_changed_durable_evidence_and_missing_directory(self):
        cap='YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWE'
        self.store.reserve(self.identifier,'production',cap)
        self.store.initialize(self.identifier,'synthetic',[],None,'production',cap,option_evidence=[])
        args=(self.identifier,'synthetic',[],None,'production',cap)
        self.assertEqual(self.store.initialize(*args,replay_only=True),{'uuid':self.identifier})
        with self.queue._db() as db:
            original=db.execute('SELECT sha256 FROM viewer_option_evidence WHERE uuid=?',(self.identifier,)).fetchone()[0]
            db.execute('UPDATE viewer_option_evidence SET sha256=? WHERE uuid=?',('0'*64,self.identifier))
        with self.assertRaises(Conflict):self.store.initialize(*args,replay_only=True)
        with self.queue._db() as db:
            db.execute('UPDATE viewer_option_evidence SET sha256=? WHERE uuid=?',(original,self.identifier))
            name=db.execute('SELECT directory FROM viewer_tasks WHERE uuid=?',(self.identifier,)).fetchone()[0]
        (self.store.root/name).rename(self.store.root/'moved-fixture')
        with self.assertRaises(Conflict):self.store.initialize(*args,replay_only=True)

    def test_upload_hashes_exact_bytes_and_retries_do_not_duplicate(self):
        self.initialize()
        data = b'\0\xffbinary\r\n--not-a-multipart-boundary' * 10000
        self.photo(name='Photo.JPG', data=data)
        self.photo(name='photo.jpg', data=data)
        task = self.store.task(self.identifier)
        self.assertEqual(task['images_count'], 1)
        self.assertEqual(task['input_bytes'], len(data))
        self.assertEqual(Path(task['files'][0]['path']).read_bytes(), data)
        with self.assertRaises(Conflict):
            self.photo(name='PHOTO.jpg', data=b'different')
        self.assertEqual(self.store.info(self.identifier)['imagesCount'], 1)
        self.assertFalse(list(Path(task['directory']).glob('.incoming-*')))

    def test_separate_store_instance_cannot_commit_while_upload_is_in_flight(self):
        self.queue.close()
        self.queue = Queue(self.root / 'parallel-queue', node_ids=('cluster','main'),
                           viewer_enabled=True, parallel_viewer_uploads=True)
        self.store = TaskStore(self.queue, self.root / 'parallel-inputs')
        self.initialize()
        self.photo(name='first.jpg', data=b'first')
        second_store = TaskStore(self.queue, self.root / 'parallel-inputs')
        entered, release = threading.Event(), threading.Event()
        errors = []
        class SlowStream(io.BytesIO):
            def read(self, size=-1):
                if not entered.is_set():
                    entered.set()
                    if not release.wait(5):
                        raise TimeoutError('test upload was not released')
                return super().read(size)
        def upload():
            try:
                self.store.upload(self.identifier, 'second.jpg', SlowStream(b'second'), 6)
            except Exception as error:
                errors.append(error)
        thread = threading.Thread(target=upload)
        thread.start()
        try:
            self.assertTrue(entered.wait(5))
            with self.assertRaisesRegex(Conflict, 'upload is in progress'):
                second_store.commit(self.identifier)
            with self.assertRaises(RuntimeError): self.queue.close()
        finally:
            release.set()
            thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertTrue(second_store.commit(self.identifier)['success'])
        self.assertEqual([f['name'] for f in self.store.task(self.identifier)['files']], ['first.jpg','second.jpg'])

    def test_parallel_upload_does_not_block_another_task(self):
        # Isolated opt-in only; production configuration cannot enable it yet.
        self.queue.close()
        self.queue = Queue(self.root / 'parallel-queue', node_ids=('cluster','main'),
                           viewer_enabled=True, parallel_viewer_uploads=True)
        self.store = TaskStore(self.queue, self.root / 'parallel-inputs')
        self.initialize()
        other = str(uuid.uuid4())
        self.initialize(other)
        entered, release = threading.Event(), threading.Event()
        errors = []
        class SlowStream(io.BytesIO):
            def read(self, size=-1):
                if not entered.is_set():
                    entered.set()
                    if not release.wait(5): raise TimeoutError('test upload not released')
                return super().read(size)
        def upload():
            try: self.store.upload(self.identifier, 'first.jpg', SlowStream(b'first'), 5)
            except Exception as error: errors.append(error)
        thread = threading.Thread(target=upload)
        thread.start()
        try:
            self.assertTrue(entered.wait(5))
            self.photo(other, 'other.jpg', b'other')
            self.assertTrue(self.store.commit(other)['success'])
        finally:
            release.set()
            thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])

    def test_commit_cannot_overtake_registered_upload_waiter(self):
        self.queue.close()
        self.queue = Queue(self.root/'parallel-queue', node_ids=('cluster','main'),
                           viewer_enabled=True, parallel_viewer_uploads=True)
        self.store = TaskStore(self.queue, self.root/'parallel-inputs')
        self.initialize()
        self.photo(name='first.jpg', data=b'first')
        # Delay the upload after it registers as a waiter but before it
        # acquires the task lock. A non-FIFO mutex could otherwise let commit
        # overtake it during precisely this interval.
        registered, release = threading.Event(), threading.Event()
        class GateLock:
            def __init__(self): self.lock = threading.RLock()
            def acquire(self, blocking=True):
                if threading.current_thread().name == 'upload-waiter':
                    registered.set()
                    if not release.wait(5): raise TimeoutError('waiter not released')
                return self.lock.acquire(blocking=blocking)
            def release(self): self.lock.release()
        with self.queue._viewer_task_locks_guard:
            self.queue._viewer_task_locks[self.identifier] = [GateLock(), 0, 0]
        errors = []
        def upload():
            try: self.photo(name='second.jpg', data=b'second')
            except Exception as error: errors.append(error)
        worker = threading.Thread(target=upload, name='upload-waiter')
        worker.start()
        try:
            self.assertTrue(registered.wait(5))
            with self.assertRaisesRegex(Conflict, 'upload is in progress'):
                self.store.commit(self.identifier)
        finally:
            release.set()
            worker.join(5)
        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.assertTrue(self.store.commit(self.identifier)['success'])
        self.assertEqual(len(self.store.task(self.identifier)['files']), 2)

    def test_cancel_waits_for_inflight_upload_and_keeps_uploaded_bytes(self):
        self.queue.close()
        self.queue = Queue(self.root / 'parallel-queue', node_ids=('cluster','main'),
                           viewer_enabled=True, parallel_viewer_uploads=True)
        self.store = TaskStore(self.queue, self.root / 'parallel-inputs')
        self.initialize()
        entered, release = threading.Event(), threading.Event()
        class SlowStream(io.BytesIO):
            def read(self, size=-1):
                if not entered.is_set():
                    entered.set()
                    if not release.wait(5): raise TimeoutError('test upload not released')
                return super().read(size)
        upload = threading.Thread(target=lambda: self.store.upload(self.identifier, 'first.jpg', SlowStream(b'first'), 5))
        upload.start()
        try:
            self.assertTrue(entered.wait(5))
            with self.assertRaisesRegex(Conflict, 'upload is in progress'):
                self.store.cancel(self.identifier)
        finally:
            release.set()
            upload.join(5)
        self.assertFalse(upload.is_alive())
        self.assertTrue(self.store.cancel(self.identifier)['success'])
        task = self.store.task(self.identifier)
        self.assertEqual(task['stage'], 'cancelled')
        self.assertEqual(Path(task['files'][0]['path']).read_bytes(), b'first')

    def test_parallel_upload_process_death_keeps_partial_out_of_manifest(self):
        root = self.root / 'upload-exit'
        identifier = str(uuid.uuid4())
        program = '''import io,os,sys
from pathlib import Path
from processing.prototype import Queue
from processing.viewer_tasks import TaskStore
root=Path(sys.argv[1]);identifier=sys.argv[2]
queue=Queue(root/'queue',node_ids=('cluster','main'),viewer_enabled=True,parallel_viewer_uploads=True)
store=TaskStore(queue,root/'inputs');store.initialize(identifier,'synthetic',[],None,'production')
class DyingStream(io.BytesIO):
 def read(self,n=-1):
  if self.tell(): os._exit(73)
  return super().read(1)
store.upload(identifier,'image.jpg',DyingStream(b'abc'),3)
'''
        proc = subprocess.run([sys.executable, '-c', program, str(root), identifier], capture_output=True, timeout=15)
        self.assertEqual(proc.returncode, 73)
        queue = Queue(root/'queue', node_ids=('cluster','main'), viewer_enabled=True, parallel_viewer_uploads=True)
        try:
            store = TaskStore(queue, root/'inputs')
            task = store.task(identifier)
            self.assertEqual(task['files'], [])
            self.assertEqual(len(list(Path(task['directory']).glob('.incoming-*'))), 1)
            self.assertTrue(store.upload(identifier, 'image.jpg', io.BytesIO(b'abc'), 3)['success'])
            self.assertEqual(Path(store.task(identifier)['files'][0]['path']).read_bytes(), b'abc')
        finally:
            queue.close()

    def test_parallel_upload_process_death_after_rename_before_commit(self):
        root = self.root / 'rename-exit'
        identifier = str(uuid.uuid4())
        program = '''import io,os,sys
from pathlib import Path
from unittest.mock import patch
from processing.prototype import Queue
from processing.viewer_tasks import TaskStore
root=Path(sys.argv[1]);identifier=sys.argv[2]
queue=Queue(root/'queue',node_ids=('cluster','main'),viewer_enabled=True,parallel_viewer_uploads=True)
store=TaskStore(queue,root/'inputs');store.initialize(identifier,'synthetic',[],None,'production')
with patch('processing.viewer_tasks.sync_directory',side_effect=lambda _path:os._exit(73)):
 store.upload(identifier,'image.jpg',io.BytesIO(b'abc'),3)
'''
        proc = subprocess.run([sys.executable, '-c', program, str(root), identifier], capture_output=True, timeout=15)
        self.assertEqual(proc.returncode, 73)
        queue = Queue(root/'queue', node_ids=('cluster','main'), viewer_enabled=True, parallel_viewer_uploads=True)
        try:
            store = TaskStore(queue, root/'inputs')
            task = store.task(identifier)
            self.assertEqual(task['files'], [])
            directory = Path(task['directory'])
            files = [p for p in directory.iterdir() if p.is_file()]
            self.assertEqual(len(files), 1)
            self.assertEqual(files[0].read_bytes(), b'abc')
            self.assertTrue(store.upload(identifier, 'image.jpg', io.BytesIO(b'abc'), 3)['success'])
            self.assertEqual(files[0].read_bytes(), b'abc')
            self.assertEqual(Path(store.task(identifier)['files'][0]['path']).read_bytes(), b'abc')
        finally:
            queue.close()

    def test_truncated_or_overlong_stream_never_enters_manifest(self):
        self.initialize()
        for body, length in ((b'ab', 3), (b'abcd', 3)):
            with self.subTest(body=body), self.assertRaises(ValueError):
                self.store.upload(self.identifier, 'photo.jpg', io.BytesIO(body), length)
        self.assertEqual(self.store.task(self.identifier)['files'], [])
        self.assertFalse(list(Path(self.store.task(self.identifier)['directory']).glob('.incoming-*')))

    def test_input_ceiling_counts_task_bytes_and_duplicate_is_idempotent(self):
        self.store = TaskStore(self.queue, self.root / 'inputs', max_input_bytes=8)
        self.initialize()
        self.photo(data=b'12345678')
        self.photo(data=b'12345678')
        with self.assertRaises(InputLimitExceeded):
            self.photo(name='other.jpg', data=b'1')
        self.assertEqual(self.store.task(self.identifier)['input_bytes'], 8)
        with self.assertRaises(InputLimitExceeded):
            self.photo(name='large.jpg', data=b'123456789')
        self.assertEqual(len(self.store.task(self.identifier)['files']), 1)
        self.assertFalse(list(Path(self.store.task(self.identifier)['directory']).glob('.incoming-*')))
        # A failed later upload preserves earlier bytes and is safe to retry
        # after reopening the store; the limit does not remove the source.
        self.store = TaskStore(self.queue, self.root / 'inputs', max_input_bytes=8)
        self.photo(data=b'12345678')
        self.assertEqual(self.store.task(self.identifier)['input_bytes'], 8)
        with self.assertRaises(InputLimitExceeded):
            self.photo(name='other.jpg', data=b'1')
        self.assertEqual(self.store.task(self.identifier)['files'][0]['bytes'], 8)

    def test_unsafe_names_identifiers_and_settings_rejected(self):
        for identifier in ('../task', self.identifier.upper(), 'not-a-uuid'):
            with self.subTest(identifier=identifier), self.assertRaises(ValueError):
                self.store.initialize(identifier)
        self.initialize()
        for name in ('../photo.jpg', '/photo.jpg', 'a\\b.jpg', '.hidden.jpg', 'evil.py', 'photo.jpg.', 'p\n.jpg'):
            with self.subTest(name=name), self.assertRaises(ValueError):
                self.photo(name=name)
        for options in ([{'name': 'x', 'value': float('nan')}], [{'name': '../x', 'value': True}],
                        [{'name': 'x', 'value': 1}, {'name': 'x', 'value': 2}]):
            with self.subTest(options=options), self.assertRaises(ValueError):
                self.store.initialize(str(uuid.uuid4()), options=options)

    def test_auxiliary_files_do_not_count_as_photos_or_allow_empty_commit(self):
        self.initialize()
        self.store.upload(self.identifier, 'geo.txt', io.BytesIO(b'geo'), 3)
        self.assertEqual(self.store.info(self.identifier)['imagesCount'], 0)
        with self.assertRaises(ValueError):
            self.store.commit(self.identifier)
        self.photo()
        self.store.commit(self.identifier)
        self.assertEqual(self.store.info(self.identifier)['imagesCount'], 1)

    def test_commit_is_atomic_idempotent_and_shares_queue_profile(self):
        task = self.ready()
        self.store.commit(self.identifier)
        self.assertEqual(len(self.queue.list()), 1)
        self.assertEqual(task['job']['requestId'], self.identifier)
        self.assertEqual(task['job']['profile'], VIEWER_PROFILE)
        self.assertEqual(task['job']['environment'], 'production')
        with self.assertRaises(Conflict):
            self.photo()
        claimed = self.claim()
        self.assertEqual(claimed['id'], task['job_id'])
        self.assertEqual(self.store.info(self.identifier)['status']['code'], 10)

    def test_preexisting_enqueue_is_reconciled_without_duplicate_job(self):
        self.initialize()
        self.photo()
        old, _ = self.queue.submit({'requestId': self.identifier, 'environment': 'production',
                                    'profile': VIEWER_PROFILE})
        self.store.commit(self.identifier)
        self.assertEqual(self.store.task(self.identifier)['job_id'], old['id'])
        self.assertEqual(len(self.queue.list()), 1)

    def test_disabled_profile_and_identity_collision_fail_closed(self):
        self.initialize()
        self.photo()
        self.queue.allowed_profiles.remove(VIEWER_PROFILE)
        with self.assertRaises(Conflict):
            self.store.commit(self.identifier)
        self.assertEqual(len(self.queue.list()), 0)
        self.queue.allowed_profiles.add(VIEWER_PROFILE)
        self.queue.submit({'requestId': self.identifier, 'environment': 'staging', 'profile': PROFILE})
        with self.assertRaises(Conflict):
            self.store.commit(self.identifier)
        self.assertIsNone(self.store.task(self.identifier)['job_id'])

    def test_tree_claim_and_held_work_block_viewer_claim_globally(self):
        tree, _ = self.queue.submit({'requestId': 'tree-first', 'environment': 'production', 'profile': PROFILE})
        self.ready(environment='staging')
        claimed = self.queue.claim({'workerId': 'trees', 'nodeId': 'main'})
        self.assertEqual(claimed['id'], tree['id'])
        self.assertIsNone(self.claim())
        self.queue.operator(tree['id'], 'hold', {'note': 'synthetic uncertain engine'})
        self.assertIsNone(self.claim())
        self.queue.operator(tree['id'], 'resolve', {'note': 'verified stopped', 'workerStopped': True})
        self.assertIsNotNone(self.claim())

    def test_priority_with_shared_jobs_and_native_status_after_restart(self):
        staging = str(uuid.uuid4())
        self.ready(staging, 'staging')
        self.ready(self.identifier, 'production')
        claimed = self.claim()
        self.assertEqual(claimed['id'], self.store.task(self.identifier)['job_id'])
        self.queue.close()
        self.queue = Queue(self.root / 'queue', node_ids=('cluster', 'main'), treedar_node_ids=('main',))
        self.queue.allowed_profiles.add(VIEWER_PROFILE)
        self.store = TaskStore(self.queue, self.root / 'inputs')
        self.assertEqual(self.store.info(self.identifier)['status']['code'], 10)
        self.assertIs(self.store.info(self.identifier)['ledgetopHeld'], True)
        self.assertIsNone(self.claim())
        self.queue.complete(claimed['id'], {'claimToken': claimed['claimToken'], 'status': 'succeeded', 'result': {}})
        self.assertEqual(self.store.info(self.identifier)['status']['code'], 40)
        self.assertNotIn('ledgetopHeld', self.store.info(self.identifier))
        self.assertIsNotNone(self.claim())

    def test_cancel_queued_is_atomic_and_repeated_cancel_is_safe(self):
        task = self.ready()
        self.store.cancel(self.identifier)
        self.store.cancel(self.identifier)
        self.assertEqual(self.queue.get(task['job_id'])['status'], 'failed')
        self.assertEqual(self.store.info(self.identifier)['status']['code'], 50)
        self.assertIsNone(self.claim())
        with self.assertRaises(Conflict):
            self.store.commit(self.identifier)

    def test_recoverable_restart_reports_running_until_exact_engine_reconciliation(self):
        self.ready()
        claim = self.claim()
        self.store.update(self.identifier, phase='processing', engine_uuid=str(uuid.uuid4()),
                          node_id='cluster', claim_token=claim['claimToken'])
        self.queue.close()
        self.queue = Queue(self.root / 'queue', node_ids=('cluster', 'main'),
                           treedar_node_ids=('main',), viewer_enabled=True)
        self.store = TaskStore(self.queue, self.root / 'inputs')
        self.assertEqual(self.store.info(self.identifier)['status']['code'], 10)
        self.assertEqual(self.store.task(self.identifier)['job']['status'], 'held')
        self.queue.operator(claim['id'], 'hold', {'note': 'operator uncertainty requires inspection'})
        self.assertEqual(self.store.info(self.identifier)['status']['code'], 10)
        self.assertIs(self.store.info(self.identifier)['ledgetopHeld'], True)
        self.assertNotIn('operator uncertainty requires inspection', str(self.store.info(self.identifier)))

    def test_cancel_completion_is_truthful_before_metadata_stage_update(self):
        self.ready()
        claim = self.claim()
        self.queue.complete(claim['id'], {'claimToken': claim['claimToken'], 'status': 'failed',
                                         'result': {'cancelled': True}})
        self.assertEqual(self.store.task(self.identifier)['stage'], 'queued')
        self.assertEqual(self.store.info(self.identifier)['status']['code'], 50)

    def test_active_and_held_task_never_cancel_or_remove_without_engine_proof(self):
        self.ready()
        claimed = self.claim()
        for transition in ('running', 'held'):
            if transition == 'held':
                self.queue.operator(claimed['id'], 'hold', {'note': 'inspect engine'})
            with self.subTest(transition=transition):
                with self.assertRaises(Conflict):
                    self.store.cancel(self.identifier)
                with self.assertRaises(Conflict):
                    self.store.remove(self.identifier)
        self.assertEqual(self.queue.get(claimed['id'])['status'], 'held')
        self.store.update(self.identifier, stage='cancelled')
        self.assertEqual(self.store.info(self.identifier)['status']['code'], 10)
        self.assertIs(self.store.info(self.identifier)['ledgetopHeld'], True)
        self.store.update(self.identifier, stage='held')
        # A stale retry may query or repeat commit, but cannot replace inputs,
        # release the held slot, or create another execution.
        before = self.store.task(self.identifier)
        self.assertEqual(self.store.commit(self.identifier), {'success': True, 'uuid': self.identifier})
        with self.assertRaises(Conflict):
            self.store.upload(self.identifier, 'late.jpg', io.BytesIO(b'x'), 1)
        after = self.store.task(self.identifier)
        self.assertEqual(after['job_id'], before['job_id'])
        self.assertEqual(after['generation'], before['generation'])
        self.assertEqual(after['files'], before['files'])
        self.assertEqual(self.queue.get(claimed['id'])['status'], 'held')
        self.assertIsNone(self.claim())

    def test_remove_reset_uuid_retains_old_inputs_and_execution_history(self):
        old = self.ready()
        old_path = Path(old['files'][0]['path'])
        self.store.remove(self.identifier)
        self.store.remove(self.identifier)
        with self.assertRaises(KeyError):
            self.store.info(self.identifier)
        self.ready()
        new = self.store.task(self.identifier)
        self.assertNotEqual(new['directory'], old['directory'])
        self.assertNotEqual(new['job_id'], old['job_id'])
        self.assertTrue(old_path.is_file())
        self.assertEqual(len(self.queue.list()), 2)
        self.assertEqual(new['generation'], 2)
        with self.queue._db() as db:
            archived = db.execute('SELECT * FROM viewer_incarnations WHERE uuid=?',
                                  (self.identifier,)).fetchone()
        self.assertEqual(archived['generation'], 1)
        self.assertIn(old['job_id'], archived['metadata'])

    def test_removed_uuid_retains_original_environment_across_reset(self):
        self.ready(environment='staging')
        self.store.remove(self.identifier)
        with self.assertRaises(Conflict):
            self.initialize(environment='production')
        with self.assertRaises(KeyError):
            self.store.info(self.identifier)
        self.initialize(environment='staging')
        task = self.store.task(self.identifier)
        self.assertEqual(task['environment'], 'staging')
        self.assertEqual(task['generation'], 2)

    @unittest.skipIf(os.name == 'nt', 'POSIX database permissions')
    def test_database_is_private_before_claim_metadata_is_written(self):
        self.queue.path.chmod(0o644)
        self.store = TaskStore(self.queue, self.root / 'inputs')
        self.assertEqual(self.queue.path.stat().st_mode & 0o777, 0o600)
        self.ready()
        claim = self.claim()
        self.store.update(self.identifier, claim_token=claim['claimToken'])
        self.assertEqual(self.queue.path.stat().st_mode & 0o777, 0o600)

    def test_private_updates_output_cursor_and_owned_archive(self):
        self.ready()
        engine_id = str(uuid.uuid4())
        self.store.update(self.identifier, engine_uuid=engine_id, phase='init_pending',
                          node_id='cluster', claim_token='private-claim', progress=12,
                          output_lines=['line', '', 'a,b'])
        self.assertEqual(self.store.output(self.identifier, 1), ['', 'a,b'])
        info = self.store.info(self.identifier)
        self.assertNotIn('claim_token', info)
        self.assertNotIn('engine_uuid', info)
        self.assertNotIn('directory', info)
        with self.assertRaises(Conflict):
            self.store.update(self.identifier, engine_uuid=str(uuid.uuid4()))
        for fields in ({'error': 'https://private/?token=secret'}, {'progress': float('inf')},
                       {'stage': 'not-a-stage'}, {'node_id': 'other'}, {'unknown': True}):
            with self.subTest(fields=fields), self.assertRaises(ValueError):
                self.store.update(self.identifier, **fields)
        archive = Path(self.store.task(self.identifier)['directory']) / 'all.zip'
        archive.write_bytes(b'owned archive')
        self.store.update(self.identifier, archive_path=str(archive))
        with self.assertRaises(ValueError):
            self.store.update(self.identifier, archive_path=str(self.root / 'outside.zip'))
        self.store.remove(self.identifier)
        self.assertTrue(archive.exists())

    @unittest.skipIf(os.name == 'nt', 'symlink setup requires host privilege on Windows')
    def test_symlink_input_directory_is_rejected(self):
        target = self.root / 'target'
        target.mkdir()
        link = self.root / 'link'
        link.symlink_to(target, target_is_directory=True)
        with self.assertRaises(ValueError):
            TaskStore(self.queue, link)


if __name__ == '__main__':
    unittest.main()
