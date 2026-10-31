import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock

from ops import stopped_viewer_backup as backup

AUTHORITY = '12345678-1234-1234-1234-123456789abc'
KEY = bytes(range(32))


@unittest.skipUnless(os.name == 'posix', 'requires POSIX locks and readonly mount')
class BackupTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name).resolve()
        self.source = self.base / 'source'
        self.source.mkdir()
        self.leases = self.base / 'leases'
        self.leases.mkdir()
        (self.leases / (AUTHORITY + '.lease')).touch()
        (self.source / 'coordinator.lock').touch()
        self.out = self.base / 'out'
        self.out.mkdir(mode=0o700)
        self.destination = self.out / 'snapshot'
        db = sqlite3.connect(self.source / 'jobs.sqlite3')
        db.executescript('''CREATE TABLE jobs(id INTEGER PRIMARY KEY,status TEXT);
            CREATE TABLE viewer_tasks(uuid TEXT,stage TEXT,removed INTEGER);
            CREATE TABLE coordinator_identity(singleton INTEGER PRIMARY KEY,authority_id TEXT);
            INSERT INTO jobs VALUES(1,'succeeded');
            INSERT INTO viewer_tasks VALUES('synthetic-task','succeeded',0);''')
        db.execute('INSERT INTO coordinator_identity VALUES(1,?)', (AUTHORITY,))
        db.commit()
        db.close()
        task = self.source / 'viewer-tasks' / 'synthetic-task'
        task.mkdir(parents=True)
        (task / 'photo.bin').write_bytes(bytes(range(256)) * 9)

    def tearDown(self):
        self.tmp.cleanup()

    def run_backup(self, **kwargs):
        # Real readonly bind mount is covered separately; this mocks only the
        # mount flag so file/SQLite/lock behavior can be exercised in CI.
        with mock.patch.object(backup.os, 'statvfs') as mounted:
            mounted.return_value.f_flag = os.ST_RDONLY
            return backup.backup(self.source, self.destination, self.leases, AUTHORITY, KEY,
                                 stopped_writers_confirmed=True, **kwargs)

    def test_nonempty_snapshot_preserves_bytes_and_rows(self):
        result = self.run_backup()
        self.assertEqual(result['source']['database']['rowCounts']['viewer_tasks'], 1)
        self.assertEqual((self.destination / 'viewer-tasks/synthetic-task/photo.bin').read_bytes(),
                         (self.source / 'viewer-tasks/synthetic-task/photo.bin').read_bytes())
        self.assertEqual((self.destination / 'jobs.sqlite3').read_bytes(), (self.source / 'jobs.sqlite3').read_bytes())
        db = sqlite3.connect(self.destination / 'jobs.sqlite3')
        try:
            self.assertEqual(db.execute('SELECT * FROM viewer_tasks').fetchall(), [('synthetic-task', 'succeeded', 0)])
        finally:
            db.close()

    def test_requires_confirmation_and_readonly_mount(self):
        with self.assertRaisesRegex(backup.BackupRefused, 'explicit_writer'):
            backup.backup(self.source, self.destination, self.leases, AUTHORITY, KEY)
        with mock.patch.object(backup.os, 'statvfs') as mount:
            mount.return_value.f_flag = 0
            with self.assertRaisesRegex(backup.BackupRefused, 'not_readonly'):
                backup.backup(self.source, self.destination, self.leases, AUTHORITY, KEY, stopped_writers_confirmed=True)
        self.assertFalse(self.destination.exists())

    def test_refuses_active_and_sidecar(self):
        db = sqlite3.connect(self.source / 'jobs.sqlite3')
        db.execute("UPDATE viewer_tasks SET stage='running'")
        db.commit(); db.close()
        with self.assertRaisesRegex(backup.BackupRefused, 'quiescent'):
            self.run_backup()
        db = sqlite3.connect(self.source / 'jobs.sqlite3')
        db.execute("UPDATE viewer_tasks SET stage='succeeded'")
        db.commit(); db.close()
        (self.source / 'jobs.sqlite3-wal').touch()
        with self.assertRaisesRegex(backup.BackupRefused, 'quiescent'):
            self.run_backup()
        self.assertFalse(self.destination.exists())

    def test_rejects_symlink_and_existing_destination(self):
        (self.source / 'unknown').symlink_to(self.base)
        with self.assertRaisesRegex(backup.BackupRefused, 'quiescent'):
            self.run_backup()
        (self.source / 'unknown').unlink()
        self.destination.mkdir()
        with self.assertRaisesRegex(backup.BackupRefused, 'new_path'):
            self.run_backup()

    def test_held_authority_lock_refuses(self):
        import fcntl
        with (self.leases / (AUTHORITY + '.lease')).open('rb') as lease:
            fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(BlockingIOError):
                self.run_backup()
        self.assertFalse(self.destination.exists())

    def test_unknown_root_and_unknown_status_refused(self):
        (self.source / 'mystery').write_bytes(b'preserve')
        with self.assertRaisesRegex(backup.BackupRefused, 'unknown_root'):
            self.run_backup()
        (self.source / 'mystery').unlink()
        db = sqlite3.connect(self.source / 'jobs.sqlite3')
        db.execute("UPDATE jobs SET status='unrecognized'")
        db.commit(); db.close()
        with self.assertRaisesRegex(backup.BackupRefused, 'quiescent'):
            self.run_backup()

    def test_mutated_file_fails_and_preserves_source(self):
        original = backup._copy_file
        changed = False
        def mutate(source, target, info):
            nonlocal changed
            if source.name == 'photo.bin' and not changed:
                changed = True
                source.write_bytes(b'changed')
            return original(source, target, info)
        with mock.patch.object(backup, '_copy_file', side_effect=mutate):
            with self.assertRaises(backup.BackupRefused):
                self.run_backup()
        self.assertTrue(changed)
        self.assertEqual((self.source / 'viewer-tasks/synthetic-task/photo.bin').read_bytes(), b'changed')


if __name__ == '__main__':
    unittest.main()
