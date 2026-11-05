"""Exact installed marker and real Linux external flock; synthetic state only."""
import hashlib
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile
import unittest
import uuid
from contextlib import closing
import subprocess
import sys

from processing import authority_lease
from processing.prototype import Queue


class EnrolledStateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.state = self.root / 'state'; self.state.mkdir(mode=0o700)
        self.leases = self.root / 'lease'; self.leases.mkdir(mode=0o700)
        self.authority = str(uuid.uuid4())
        self.database = self.state / 'jobs.sqlite3'
        with closing(sqlite3.connect(self.database)) as db, db:
            db.execute('CREATE TABLE coordinator_identity(singleton INTEGER PRIMARY KEY CHECK(singleton=1),authority_id TEXT NOT NULL)')
            db.execute('INSERT INTO coordinator_identity VALUES(1,?)', (self.authority,))
            db.execute(authority_lease.MARKER_DDL)
            db.execute('INSERT INTO coordinator_authority_lease VALUES(1,1)')
        self.file = self.leases / (self.authority + '.lease')
        self.file.write_bytes(b'legacy diagnostic bytes are not ownership proof\n')
        self.file.chmod(0o600)
        self.queue = self.lease = None

    def tearDown(self):
        if self.queue: self.queue.close()
        if self.lease: self.lease.close()
        self.temp.cleanup()

    def test_required_marker_refuses_missing_configuration_before_any_state_write(self):
        before = self.database.read_bytes()
        with self.assertRaisesRegex(authority_lease.LeaseUnavailable, 'configuration_required'):
            Queue(self.state)
        self.assertEqual(self.database.read_bytes(), before)
        self.assertFalse((self.state / 'coordinator.lock').exists())

    @unittest.skipUnless(os.name == 'posix', 'actual enrolled Linux flock')
    def test_original_external_lease_protects_copy_and_closes_after_queue(self):
        diagnostic = self.file.read_bytes()
        self.lease = authority_lease.acquire(self.leases, self.state)
        self.queue = Queue(self.state, authority_lease=self.lease)
        authority_lease.record_required(self.queue, self.lease)
        self.assertEqual(self.queue.authority_id, self.authority)
        clone = self.root / 'copy'; shutil.copytree(self.state, clone)
        before = (clone / 'jobs.sqlite3').read_bytes()
        with self.assertRaises(authority_lease.LeaseUnavailable):
            authority_lease.acquire(self.leases, clone)
        self.assertEqual((clone / 'jobs.sqlite3').read_bytes(), before)
        with self.queue.coordinator_operation('http'):
            self.queue.submit(dict(requestId='synthetic', profile='treedar-synthetic-v1', environment='staging'))
        self.queue.close()
        self.assertIsNone(self.lease._descriptor)
        self.assertEqual(self.file.read_bytes(), diagnostic)
        reacquired = authority_lease.acquire(self.leases, self.state)
        reacquired.close()

    @unittest.skipUnless(os.name == 'posix', 'actual enrolled Linux flock')
    def test_wrong_state_or_fake_lease_cannot_mutate_enrolled_database(self):
        before = self.database.read_bytes()
        with self.assertRaises(authority_lease.LeaseUnavailable):
            Queue(self.state, authority_lease=object())
        self.assertEqual(self.database.read_bytes(), before)
        self.lease = authority_lease.acquire(self.leases, self.state)
        clone = self.root / 'copy'; shutil.copytree(self.state, clone)
        before_clone = (clone / 'jobs.sqlite3').read_bytes()
        with self.assertRaises(authority_lease.LeaseUnavailable):
            Queue(clone, authority_lease=self.lease)
        self.assertEqual((clone / 'jobs.sqlite3').read_bytes(), before_clone)

    @unittest.skipUnless(os.name == 'posix', 'actual enrolled Linux flock')
    def test_required_original_lease_file_is_not_recreated_if_missing(self):
        self.file.unlink()
        before = self.database.read_bytes()
        with self.assertRaises(authority_lease.LeaseUnavailable):
            authority_lease.acquire(self.leases, self.state)
        self.assertFalse(self.file.exists())
        self.assertEqual(self.database.read_bytes(), before)

    @unittest.skipUnless(os.name == 'posix', 'actual Linux factory compatibility')
    def test_actual_handler_restores_installed_external_lease_configuration(self):
        from processing.app import make_handler
        from processing.config import Config
        handler = make_handler(Config('synthetic-api-' + 'x' * 32, '', '', 1000, 2000,
            prototype_state_dir=str(self.state), authority_lease_dir=str(self.leases)))
        self.queue = handler.prototype_queue
        self.lease = self.queue.authority_lease
        self.assertIsInstance(self.lease, authority_lease.AuthorityLease)
        self.assertEqual(self.lease.authority, self.authority)
        with self.queue._db() as db:
            self.assertEqual(tuple(db.execute('SELECT singleton,required FROM coordinator_authority_lease').fetchone()), (1, 1))

    @unittest.skipUnless(os.name == 'posix', 'actual Linux process death and original external lease')
    def test_process_death_releases_both_original_enrolled_and_main_leases(self):
        code = '''
import os,sys
from processing import authority_lease
from processing.prototype import Queue
lease=authority_lease.acquire(sys.argv[1],sys.argv[2])
queue=Queue(sys.argv[2],authority_lease=lease)
print('owned',flush=True)
os._exit(43)
'''
        child = subprocess.run([sys.executable, '-c', code, str(self.leases), str(self.state)],
                               capture_output=True, text=True, timeout=5)
        self.assertEqual(child.returncode, 43, child.stderr)
        self.assertEqual(child.stdout.strip(), 'owned')
        self.lease = authority_lease.acquire(self.leases, self.state)
        self.queue = Queue(self.state, authority_lease=self.lease)
        self.assertEqual(self.queue._coordinator_freeze.status()['phase'], 'open')
        with self.queue._db() as db:
            states = [row[0] for row in db.execute('SELECT state FROM coordinator_operation_epochs')]
            self.assertCountEqual(states, ['retired', 'active'])

    def test_malformed_marker_is_not_treated_as_unenrolled(self):
        with closing(sqlite3.connect(self.database)) as db, db:
            db.execute('DROP TABLE coordinator_authority_lease')
            db.execute('CREATE VIEW coordinator_authority_lease AS SELECT 1 AS required')
        before = self.database.read_bytes()
        with self.assertRaises(authority_lease.LeaseUnavailable): Queue(self.state)
        self.assertEqual(self.database.read_bytes(), before)


if __name__ == '__main__': unittest.main()
