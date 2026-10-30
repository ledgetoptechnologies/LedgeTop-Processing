"""Authority lease and production admission hold used for the d738 cutover."""

import os
import sqlite3
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest
from unittest.mock import patch

from processing import authority_lease
from processing.config import from_environment
from processing.prototype import Queue
from processing.viewer_tasks import TaskStore
from processing.viewer_http import ViewerFacade
from tests.test_viewer_http import IDENTIFIER, STAGING, TOKEN, MemoryHandler, Service

POSIX = os.name == 'posix'


@unittest.skipUnless(POSIX, 'flock lease is POSIX-only')
class AuthorityLeaseTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        base = Path(self.temporary.name)
        self.state, self.copy, self.leases = base / 'state', base / 'copy', base / 'leases'
        self.leases.mkdir()
        Queue(self.state).close()
        self.copy.mkdir()
        (self.copy / 'jobs.sqlite3').write_bytes((self.state / 'jobs.sqlite3').read_bytes())
        self.authority = authority_lease.existing_authority(self.state)

    def tearDown(self):
        self.temporary.cleanup()

    def test_copy_with_same_authority_cannot_lease_while_original_holds(self):
        self.assertEqual(authority_lease.existing_authority(self.copy), self.authority)
        first = authority_lease.acquire(self.leases, self.state)
        with self.assertRaises(authority_lease.LeaseUnavailable) as raised:
            authority_lease.acquire(self.leases, self.copy)
        self.assertEqual(str(raised.exception), 'authority_lease_held')
        first.close()
        authority_lease.acquire(self.leases, self.copy).close()

    def test_lease_is_released_when_the_holder_process_dies(self):
        holder = subprocess.Popen([sys.executable, '-c', textwrap.dedent('''
            import sys, time
            from processing import authority_lease
            lease = authority_lease.acquire(sys.argv[1], sys.argv[2])
            print('held', flush=True)
            time.sleep(60)
        '''), str(self.leases), str(self.state)], stdout=subprocess.PIPE, text=True, cwd=os.getcwd())
        try:
            self.assertEqual(holder.stdout.readline().strip(), 'held')
            with self.assertRaises(authority_lease.LeaseUnavailable):
                authority_lease.acquire(self.leases, self.copy)
        finally:
            holder.kill()
            holder.wait()
        authority_lease.acquire(self.leases, self.copy).close()

    def test_refuses_fresh_state_symlinked_directory_and_read_only_before_writes(self):
        with self.assertRaises(authority_lease.LeaseUnavailable):
            authority_lease.acquire(self.leases, Path(self.temporary.name) / 'empty')
        link = Path(self.temporary.name) / 'link'
        link.symlink_to(self.leases)
        with self.assertRaises(authority_lease.LeaseUnavailable):
            authority_lease.acquire(link, self.state)
        before = (self.state / 'jobs.sqlite3').read_bytes()
        authority_lease.existing_authority(self.state)
        authority_lease.required(self.state)
        self.assertEqual(before, (self.state / 'jobs.sqlite3').read_bytes())

    def test_marker_makes_the_lease_mandatory(self):
        lease = authority_lease.acquire(self.leases, self.state)
        queue = Queue(self.state)
        authority_lease.record_required(queue, lease)
        authority_lease.record_required(queue, lease)  # idempotent
        queue.close()
        lease.close()
        self.assertTrue(authority_lease.required(self.state))
        self.assertFalse(authority_lease.required(self.copy))


@unittest.skipUnless(POSIX, 'flock lease is POSIX-only')
class StartupEnforcementTest(unittest.TestCase):
    """Real make_handler startup with environment configuration."""

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        base = Path(self.temporary.name)
        self.state, self.copy, self.leases = base / 'state', base / 'copy', base / 'leases'
        self.leases.mkdir()
        Queue(self.state).close()
        self.copy.mkdir()
        (self.copy / 'jobs.sqlite3').write_bytes((self.state / 'jobs.sqlite3').read_bytes())

    def tearDown(self):
        self.temporary.cleanup()

    def environment(self, state, lease=True):
        values = {'PROCESSING_API_TOKEN': 'a' * 40, 'PROCESSING_PROTOTYPE_STATE_DIR': str(state),
                  'PROCESSING_PROTOTYPE_OPERATOR_TOKEN': 'o' * 40, 'PROCESSING_PROTOTYPE_OBSERVER_TOKEN': 'b' * 40}
        if lease:
            values['PROCESSING_AUTHORITY_LEASE_DIR'] = str(self.leases)
        return values

    def handler(self, state, lease=True):
        from processing.app import make_handler
        with patch.dict(os.environ, self.environment(state, lease), clear=True):
            return make_handler(from_environment())

    def test_second_coordinator_on_a_copy_refuses_before_writing(self):
        first = self.handler(self.state)
        before = (self.copy / 'jobs.sqlite3').read_bytes()
        with self.assertRaises(authority_lease.LeaseUnavailable):
            self.handler(self.copy)
        self.assertEqual(before, (self.copy / 'jobs.sqlite3').read_bytes())
        self.assertFalse((self.copy / 'coordinator.lock').exists())
        first.prototype_queue.close()

    def test_leased_state_refuses_to_start_without_a_lease(self):
        first = self.handler(self.state)
        first.prototype_queue.authority_lease.close()
        first.prototype_queue.close()
        with self.assertRaisesRegex(RuntimeError, 'authority_lease_configuration_required'):
            self.handler(self.state, lease=False)

    def test_config_rejects_relative_lease_and_unknown_production_mode(self):
        for extra in ({'PROCESSING_AUTHORITY_LEASE_DIR': 'relative'},
                      {'PROCESSING_VIEWER_PRODUCTION_ADMISSION': 'closed'}):
            with patch.dict(os.environ, {**self.environment(self.state, lease=False), **extra}, clear=True):
                with self.assertRaises(ValueError):
                    from_environment()


class HeldStartupIdleTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.state = Path(self.temporary.name) / 'state'
        self.queue = Queue(self.state, viewer_enabled=True)
        self.store = TaskStore(self.queue, self.state / 'viewer-tasks')

    def tearDown(self):
        self.queue.close()
        self.temporary.cleanup()

    def rejected_without_database_change(self):
        self.queue.close()
        database = self.state / 'jobs.sqlite3'
        before = database.read_bytes()
        with self.assertRaisesRegex(RuntimeError, 'production_hold_idle_state_unavailable'):
            Queue(self.state, viewer_enabled=True, require_idle_existing=True)
        self.assertEqual(database.read_bytes(), before)
        # The failed startup must release its state lock.
        self.queue = Queue(self.state, viewer_enabled=True)
        self.queue.close()

    def test_refuses_queued_and_running_jobs_in_either_environment(self):
        for environment in ('production', 'staging'):
            for status in ('queued', 'running'):
                with self.subTest(environment=environment, status=status):
                    job, _ = self.queue.submit({'requestId': environment + status, 'environment': environment, 'profile': 'treedar-synthetic-v1'})
                    with self.queue._db() as db: db.execute('UPDATE jobs SET status=? WHERE id=?', (status, job['id']))
                    self.rejected_without_database_change()
                    self.queue = Queue(self.state, viewer_enabled=True)
                    with self.queue._db() as db: db.execute('DELETE FROM jobs')

    def test_refuses_uploading_task_and_unresolved_reservation(self):
        self.store.initialize(IDENTIFIER, 'synthetic', [], None, 'production')
        self.rejected_without_database_change()

    def test_refuses_reservation_without_task(self):
        self.store.reserve(IDENTIFIER, 'production', 'YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWE')
        self.rejected_without_database_change()

    def test_refuses_unrecognized_state(self):
        job, _ = self.queue.submit({'requestId': 'unknown', 'environment': 'production', 'profile': 'treedar-synthetic-v1'})
        with self.queue._db() as db: db.execute("UPDATE jobs SET status='unexpected' WHERE id=?", (job['id'],))
        self.rejected_without_database_change()

    def test_idle_state_starts_and_can_accept_staging_later(self):
        self.queue.close()
        self.queue = Queue(self.state, viewer_enabled=True, require_idle_existing=True)
        job, _ = self.queue.submit({'requestId': 'later', 'environment': 'staging', 'profile': 'treedar-synthetic-v1'})
        self.assertEqual(job['status'], 'queued')
        self.queue.close()

    def test_missing_database_fails_closed(self):
        self.queue.close()
        with self.assertRaisesRegex(RuntimeError, 'production_hold_idle_state_unavailable'):
            Queue(Path(self.temporary.name) / 'missing', require_idle_existing=True)

    def test_wal_with_uncheckpointed_job_is_refused_without_creating_sidecars(self):
        self.queue.close()
        database = self.state / 'jobs.sqlite3'
        writer = sqlite3.connect(database)
        try:
            self.assertEqual(writer.execute('PRAGMA journal_mode=WAL').fetchone()[0], 'wal')
            writer.execute("INSERT INTO jobs(id,request_id,environment,profile,status) VALUES('wal-job','wal-job','production','treedar-synthetic-v1','queued')")
            writer.commit()
            paths = [database, Path(str(database)+'-wal'), Path(str(database)+'-shm')]
            before = {path: path.read_bytes() for path in paths}
            with self.assertRaisesRegex(RuntimeError, 'production_hold_idle_state_unavailable'):
                Queue(self.state, viewer_enabled=True, require_idle_existing=True)
            self.assertEqual({path: path.read_bytes() for path in paths}, before)
        finally:
            writer.close()

    def test_actual_handler_refuses_before_migration_and_releases_authority_lease(self):
        from processing.app import make_handler
        job, _ = self.queue.submit({'requestId': 'queued', 'environment': 'production', 'profile': 'treedar-synthetic-v1'})
        self.queue.close()
        leases = Path(self.temporary.name) / 'leases'; leases.mkdir()
        database = self.state / 'jobs.sqlite3'; before = database.read_bytes()
        values = {'PROCESSING_API_TOKEN': 'a'*40, 'PROCESSING_PROTOTYPE_STATE_DIR': str(self.state),
                  'PROCESSING_AUTHORITY_LEASE_DIR': str(leases), 'PROCESSING_VIEWER_PRODUCTION_ADMISSION': 'held'}
        with patch.dict(os.environ, values, clear=True):
            with self.assertRaisesRegex(RuntimeError, 'production_hold_idle_state_unavailable'):
                make_handler(from_environment())
        self.assertEqual(database.read_bytes(), before)
        authority_lease.acquire(leases, self.state).close()


class ProductionHoldTest(unittest.TestCase):
    def setUp(self):
        self.spool = tempfile.TemporaryDirectory()
        self.service = Service()
        self.service.tasks[IDENTIFIER] = {'uuid': IDENTIFIER, 'status': {'code': 10}, 'progress': 0, 'imagesCount': 0}

    def tearDown(self):
        self.spool.cleanup()

    def facade(self, held):
        return ViewerFacade(self.service, TOKEN, STAGING, temp_dir=self.spool.name, production_held=held)

    def post(self, facade, path, token, body=b'uuid=' + IDENTIFIER.encode()):
        handler = MemoryHandler(path + '?token=' + token, body,
                                content_type='application/x-www-form-urlencoded', content_length=str(len(body)),
                                set_uuid=IDENTIFIER)
        facade.handle(handler)
        return handler

    MUTATIONS = ('/task/new/init', '/task/new/upload/' + IDENTIFIER, '/task/new/commit/' + IDENTIFIER,
                 '/task/cancel', '/task/remove', '/task/ledgetop/reserve', '/task/ledgetop/cancel')

    def test_held_production_mutations_refuse_before_any_service_call_or_body_read(self):
        facade = self.facade(True)
        for path in self.MUTATIONS:
            handler = self.post(facade, path, TOKEN)
            self.assertEqual(handler.status, 503, path)
            self.assertIn(b'production_admission_held', handler.wfile.getvalue())
            self.assertTrue(handler.close_connection)
            self.assertEqual(handler.rfile.tell(), 0, path)
        self.assertEqual(self.service.calls, [])

    def test_held_production_can_still_read(self):
        facade = self.facade(True)
        handler = MemoryHandler('/info?token=' + TOKEN, method='GET')
        facade.handle(handler)
        self.assertEqual(handler.status, 200)
        handler = MemoryHandler('/task/' + IDENTIFIER + '/info?token=' + TOKEN, method='GET')
        facade.handle(handler)
        self.assertEqual(handler.status, 200)

    def test_staging_is_not_held(self):
        facade = self.facade(True)
        handler = self.post(facade, '/task/cancel', STAGING)
        self.assertEqual(handler.status, 200)
        self.assertEqual(self.service.tasks[IDENTIFIER]['status'], {'code': 50})

    def test_held_production_download_never_invokes_service_but_staging_can_download(self):
        facade = self.facade(True)
        path = '/task/' + IDENTIFIER + '/download/all.zip?token='
        handler = MemoryHandler(path + TOKEN, method='GET')
        with patch.object(self.service, 'download', wraps=self.service.download) as download:
            facade.handle(handler)
            self.assertEqual(handler.status, 503)
            self.assertIn(b'production_admission_held', handler.wfile.getvalue())
            download.assert_not_called()
            handler = MemoryHandler(path + STAGING, method='GET')
            facade.handle(handler)
            self.assertEqual(handler.status, 200)
            download.assert_called_once_with(IDENTIFIER)

    def test_open_production_can_download(self):
        handler = MemoryHandler('/task/' + IDENTIFIER + '/download/all.zip?token=' + TOKEN, method='GET')
        self.facade(False).handle(handler)
        self.assertEqual(handler.status, 200)

    def test_open_by_default(self):
        handler = self.post(self.facade(False), '/task/cancel', TOKEN)
        self.assertEqual(handler.status, 200)
        with self.assertRaises(ValueError):
            ViewerFacade(self.service, TOKEN, STAGING, production_held='yes')


if __name__ == '__main__':
    unittest.main()
