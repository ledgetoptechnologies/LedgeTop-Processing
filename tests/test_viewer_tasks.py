import io
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import uuid

from processing.maintenance import Conflict
from processing.prototype import PROFILE, Queue
from processing.viewer_tasks import StorageBackpressure, TaskStore, VIEWER_PROFILE


class ViewerTaskStoreTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.queue = Queue(self.root / 'queue', node_ids=('cluster', 'main'), treedar_node_ids=('main',))
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
        with self.assertRaises(ValueError):
            self.photo(name='other.jpg', data=b'1')
        self.assertEqual(self.store.task(self.identifier)['input_bytes'], 8)

    def test_low_disk_space_rejects_before_reading_or_spooling_upload(self):
        self.store = TaskStore(self.queue, self.root / 'inputs', max_input_bytes=1024,
                               min_free_bytes=4096)
        self.initialize()
        class Unreadable:
            def read(self, *_):
                raise AssertionError('backpressured upload body must not be read')
        length = 100
        required = length + self.store.min_free_bytes + 64 * 1024 ** 2
        with patch('processing.viewer_tasks.shutil.disk_usage',
                   return_value=type('Usage', (), {'free': required - 1})()):
            with self.assertRaises(StorageBackpressure):
                self.store.upload(self.identifier, 'photo.jpg', Unreadable(), length)
        task = self.store.task(self.identifier)
        self.assertEqual(task['files'], [])
        self.assertFalse(list(Path(task['directory']).glob('.incoming-*')))

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

    def test_public_status_stays_queued_until_engine_start_is_confirmed(self):
        task = self.ready()
        claimed = self.claim()
        self.assertEqual(claimed['id'], task['job_id'])
        self.store.update(self.identifier, stage='running', phase='upload_pending',
                          node_id='cluster', claim_token=claimed['claimToken'],
                          engine_uuid=str(uuid.uuid4()))
        self.assertEqual(self.store.info(self.identifier)['status']['code'], 10)
        self.store.update(self.identifier, phase='processing')
        self.assertEqual(self.store.info(self.identifier)['status']['code'], 20)

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

    def test_tree_work_on_main_does_not_block_cluster_and_held_slot_stays_local(self):
        tree, _ = self.queue.submit({'requestId': 'tree-first', 'environment': 'production', 'profile': PROFILE})
        self.ready(environment='staging')
        tree_claim = self.queue.claim({'workerId': 'trees', 'nodeId': 'main'})
        self.assertEqual(tree_claim['id'], tree['id'])
        first_photo = self.claim()
        self.assertEqual(first_photo['nodeId'], 'cluster')
        self.queue.operator(tree['id'], 'hold', {'note': 'synthetic uncertain engine'})
        self.assertIsNone(self.claim())
        second_identifier = str(uuid.uuid4())
        self.ready(second_identifier, 'staging')
        self.queue.complete(first_photo['id'], {'claimToken': first_photo['claimToken'],
            'status': 'succeeded', 'result': {}})
        second_photo = self.claim()
        self.assertEqual(second_photo['nodeId'], 'cluster')
        self.queue.complete(second_photo['id'], {'claimToken': second_photo['claimToken'],
            'status': 'succeeded', 'result': {}})
        self.queue.operator(tree['id'], 'resolve', {'note': 'verified stopped', 'workerStopped': True})

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
        self.assertEqual(self.store.info(self.identifier)['status']['code'], 30)
        self.assertEqual(self.store.info(self.identifier)['status']['errorMessage'], 'execution_held')
        self.assertIsNone(self.claim())
        self.queue.complete(claimed['id'], {'claimToken': claimed['claimToken'], 'status': 'succeeded', 'result': {}})
        self.assertEqual(self.store.info(self.identifier)['status']['code'], 40)
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
        self.assertEqual(self.store.info(self.identifier)['status']['code'], 20)
        self.assertEqual(self.store.task(self.identifier)['job']['status'], 'held')
        self.queue.operator(claim['id'], 'hold', {'note': 'operator uncertainty requires inspection'})
        self.assertEqual(self.store.info(self.identifier)['status']['code'], 30)

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
