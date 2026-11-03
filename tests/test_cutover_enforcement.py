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


def leave_hot_journal(database, statements):
    """Run statements in a write transaction, then die without commit or rollback."""
    subprocess.run([sys.executable, '-c', textwrap.dedent('''
        import os, sqlite3, sys
        db = sqlite3.connect(sys.argv[1], isolation_level=None)
        db.execute('PRAGMA cache_size=1')  # spill uncommitted pages into the database file
        db.execute('BEGIN IMMEDIATE')
        for statement in sys.argv[2:]:
            db.execute(statement)
        for _ in range(200):
            db.execute('INSERT INTO hot_filler VALUES(randomblob(4000))')
        os._exit(9)
    '''), str(database), 'CREATE TABLE hot_filler(x BLOB)', *statements], check=False)
    journal = Path(str(database) + '-journal')
    assert journal.is_file() and journal.stat().st_size > 0, 'no hot journal left'
    return journal


class HotJournalPreflightTest(unittest.TestCase):
    """A coordinator killed inside a write transaction must not block its own restart."""

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.state = Path(self.temporary.name) / 'state'
        Queue(self.state).close()
        self.database = self.state / 'jobs.sqlite3'
        self.authority = authority_lease.existing_authority(self.state)

    def tearDown(self):
        self.temporary.cleanup()

    def snapshot(self, journal):
        return {path: path.read_bytes() for path in (self.database, journal)}

    def test_reads_committed_state_without_touching_database_or_journal(self):
        journal = leave_hot_journal(self.database, [authority_lease.MARKER_DDL,
                                                    'INSERT INTO coordinator_authority_lease VALUES(1,1)'])
        before = self.snapshot(journal)
        reader = sqlite3.connect('file:' + str(self.database) + '?mode=ro', uri=True)
        with self.assertRaises(sqlite3.OperationalError):  # the failure this guards against
            reader.execute('SELECT 1 FROM sqlite_master').fetchone()
        reader.close()
        self.assertFalse(authority_lease.required(self.state))  # the marker was never committed
        self.assertEqual(authority_lease.existing_authority(self.state), self.authority)
        self.assertEqual(self.snapshot(journal), before)

    def test_committed_marker_is_still_required_through_a_hot_journal(self):
        db = sqlite3.connect(self.database)
        with db:
            db.execute(authority_lease.MARKER_DDL)
            db.execute('INSERT INTO coordinator_authority_lease VALUES(1,1)')
        db.close()
        journal = leave_hot_journal(self.database, ['DROP TABLE coordinator_authority_lease'])
        before = self.snapshot(journal)
        self.assertTrue(authority_lease.required(self.state))
        self.assertEqual(self.snapshot(journal), before)

    def test_fails_closed_without_private_space_and_leaves_state_untouched(self):
        journal = leave_hot_journal(self.database, [])
        before = self.snapshot(journal)
        with patch.object(authority_lease.tempfile, 'TemporaryDirectory', side_effect=OSError('read-only')):
            for check in (authority_lease.required, authority_lease.existing_authority):
                with self.subTest(check=check.__name__), self.assertRaisesRegex(
                        authority_lease.LeaseUnavailable, 'authority_preflight_hot_journal_unreadable'):
                    check(self.state)
        self.assertEqual(self.snapshot(journal), before)

    def test_state_changing_during_the_private_copy_fails_closed(self):
        journal = leave_hot_journal(self.database, [])
        real_copy = authority_lease.shutil.copyfile

        def mutate_appending(path):
            with open(path, 'ab') as stream:
                stream.write(b'\x00')

        def mutate_mtime_only(path):
            info = os.stat(path)
            os.utime(path, ns=(info.st_atime_ns, info.st_mtime_ns + 5_000_000_000))

        for target, mutate in ((self.database, mutate_appending), (journal, mutate_appending),
                               (self.database, mutate_mtime_only), (journal, mutate_mtime_only)):
            with self.subTest(target=target.name, mutation=mutate.__name__):
                before = self.snapshot(journal)
                mutated = []

                def copy_then_mutate(source, destination):
                    result = real_copy(source, destination)
                    if not mutated:  # a writer touches the state once, mid-copy
                        mutate(target)
                        mutated.append(True)
                    return result

                expected = dict(before)
                with patch.object(authority_lease.shutil, 'copyfile', side_effect=copy_then_mutate):
                    with self.assertRaisesRegex(authority_lease.LeaseUnavailable,
                                                'authority_preflight_hot_journal_unreadable'):
                        authority_lease.required(self.state)
                self.assertEqual(mutated, [True])
                if mutate is mutate_appending:
                    expected[target] = before[target] + b'\x00'
                self.assertEqual(self.snapshot(journal), expected)  # only the simulated write changed anything
                if mutate is mutate_appending:  # undo the simulation for the next case
                    with open(target, 'r+b') as stream:
                        stream.truncate(len(before[target]))

    def test_unrelated_read_errors_are_not_masked(self):
        self.database.write_bytes(b'not a database' * 100)
        with self.assertRaises(sqlite3.DatabaseError):
            authority_lease.required(self.state)


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

    def test_restarts_after_a_kill_inside_a_write_transaction(self):
        first = self.handler(self.state)
        first.prototype_queue.authority_lease.close()
        first.prototype_queue.close()
        journal = leave_hot_journal(self.state / 'jobs.sqlite3', [])
        before = (self.state / 'jobs.sqlite3').read_bytes()
        with self.assertRaisesRegex(RuntimeError, 'authority_lease_configuration_required'):
            self.handler(self.state, lease=False)  # still refused, and still before any write
        self.assertEqual((self.state / 'jobs.sqlite3').read_bytes(), before)
        self.assertTrue(journal.exists())
        second = self.handler(self.state)  # lease first, then SQLite rolls the journal back
        self.assertFalse(journal.exists())
        self.assertNotIn('hot_filler', {row[0] for row in sqlite3.connect(self.state / 'jobs.sqlite3').execute(
            "SELECT name FROM sqlite_master WHERE type='table'")})
        with self.assertRaises(authority_lease.LeaseUnavailable):
            authority_lease.acquire(self.leases, self.copy)
        second.prototype_queue.authority_lease.close()
        second.prototype_queue.close()

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

    @unittest.skipUnless(POSIX, 'open WAL sidecar unlink test is POSIX-specific')
    def test_handler_rejects_wal_before_authority_sqlite_with_or_without_lease(self):
        from processing.app import make_handler
        self.queue.close()
        database = self.state / 'jobs.sqlite3'; wal = Path(str(database)+'-wal'); shm = Path(str(database)+'-shm')
        leases = Path(self.temporary.name) / 'leases'; leases.mkdir()
        writer = sqlite3.connect(database)
        try:
            self.assertEqual(writer.execute('PRAGMA journal_mode=WAL').fetchone()[0], 'wal')
            writer.execute("INSERT INTO jobs(id,request_id,environment,profile,status) VALUES('wal-job','wal-job','production','treedar-synthetic-v1','queued')")
            writer.commit()
            shm.unlink()
            before = {path: path.read_bytes() for path in (database, wal)}
            for use_lease in (False, True):
                values = {'PROCESSING_API_TOKEN': 'a'*40, 'PROCESSING_PROTOTYPE_STATE_DIR': str(self.state),
                          'PROCESSING_VIEWER_PRODUCTION_ADMISSION': 'held'}
                if use_lease: values['PROCESSING_AUTHORITY_LEASE_DIR'] = str(leases)
                with self.subTest(lease=use_lease), patch.dict(os.environ, values, clear=True):
                    with self.assertRaisesRegex(RuntimeError, 'production_hold_idle_state_unavailable'):
                        make_handler(from_environment())
                self.assertFalse(shm.exists())
                self.assertEqual({path: path.read_bytes() for path in (database, wal)}, before)
            self.assertEqual(list(leases.iterdir()), [])
        finally:
            writer.close()


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
