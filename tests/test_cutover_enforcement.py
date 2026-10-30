"""Authority lease and production admission hold used for the d738 cutover."""

import os
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

    def test_held_production_download_get_is_refused_before_the_service(self):
        facade = self.facade(True)
        handler = MemoryHandler('/task/' + IDENTIFIER + '/download/all.zip?token=' + TOKEN, method='GET')
        facade.handle(handler)
        self.assertEqual(handler.status, 503)
        self.assertIn(b'production_admission_held', handler.wfile.getvalue())
        self.assertIsNone(self.service.download_stream)

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

    def test_open_by_default(self):
        handler = self.post(self.facade(False), '/task/cancel', TOKEN)
        self.assertEqual(handler.status, 200)
        with self.assertRaises(ValueError):
            ViewerFacade(self.service, TOKEN, STAGING, production_held='yes')


if __name__ == '__main__':
    unittest.main()
