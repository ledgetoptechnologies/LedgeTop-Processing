import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from ops import archive_state_preflight, nonempty_upgrade_backup, state_census, stopped_viewer_backup
from processing.prototype import Queue
from processing.viewer_tasks import TaskStore


@unittest.skipUnless(os.name == 'posix', 'offline backup requires POSIX locks')
class NonemptyUpgradeBackupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / 'source'
        queue = Queue(self.source, node_ids=('cluster', 'main'), viewer_enabled=True)
        TaskStore(queue, self.source / 'viewer-tasks')
        self.authority = queue.authority_id
        queue.close()
        self.result = self.source / 'viewer-tasks' / 'result.zip'
        self.result.write_bytes(b'synthetic retained result')
        self.leases = self.root / 'leases'
        self.leases.mkdir(mode=0o700)
        (self.leases / (self.authority + '.lease')).touch(mode=0o600)
        self.destination = self.root / 'backup'
        self.key = b'k' * 32
        self.baseline = state_census.census(self.source, self.key)
        self.schema = self.baseline['database']['objects'].copy()

    def prepare(self, authority=None):
        # These tests exercise actual copy and locks on synthetic temporary
        # files; they do not establish real read-only/separate mount behavior.
        with patch('ops.stopped_viewer_backup.os.statvfs', return_value=SimpleNamespace(f_flag=os.ST_RDONLY)), \
             patch('ops.stopped_viewer_backup._require_isolated_mounts'):
            return nonempty_upgrade_backup.prepare(
                self.source, self.destination, self.leases, authority or self.authority,
                self.key, self.baseline, self.schema, stopped_writers_confirmed=True,
                independent_destination_confirmed=True)

    def test_nonempty_backup_preserves_source_and_exact_result(self):
        result = self.prepare()
        self.assertEqual(result['source'], self.baseline)
        self.assertEqual((self.destination / 'viewer-tasks' / 'result.zip').read_bytes(), b'synthetic retained result')
        self.assertEqual(state_census.census(self.source, self.key), self.baseline)

    def test_changed_source_refuses_before_destination_creation(self):
        self.result.write_bytes(b'new candidate data')
        with self.assertRaises(archive_state_preflight.PreflightHold):
            self.prepare()
        self.assertFalse(self.destination.exists())
        self.assertEqual(self.result.read_bytes(), b'new candidate data')

    def test_wrong_authority_refuses_without_destination(self):
        with self.assertRaisesRegex(stopped_viewer_backup.BackupRefused, 'authority_identity_mismatch'):
            self.prepare('00000000-0000-4000-8000-000000000000')
        self.assertFalse(self.destination.exists())
        self.assertEqual(self.result.read_bytes(), b'synthetic retained result')

    def test_change_after_preflight_is_refused_under_backup_locks(self):
        original = archive_state_preflight.inspect
        def change_after_scan(*args):
            result = original(*args)
            self.result.write_bytes(b'changed after preflight')
            return result
        with patch('ops.nonempty_upgrade_backup.archive_state_preflight.inspect', side_effect=change_after_scan):
            with self.assertRaisesRegex(stopped_viewer_backup.BackupRefused, 'expected_inventory_mismatch'):
                self.prepare()
        self.assertFalse(self.destination.exists())
        self.assertEqual(self.result.read_bytes(), b'changed after preflight')

    def test_copy_failure_retains_incomplete_destination_and_source(self):
        def fail_after_create(_source, target, _info):
            target.write_bytes(b'incomplete synthetic copy')
            raise OSError('synthetic write failure')
        with patch('ops.stopped_viewer_backup._copy_file', side_effect=fail_after_create):
            with self.assertRaises(OSError):
                self.prepare()
        self.assertTrue(self.destination.exists())
        self.assertTrue(any(path.is_file() for path in self.destination.rglob('*')))
        self.assertEqual(self.result.read_bytes(), b'synthetic retained result')

    def test_existing_destination_is_never_adopted_or_changed(self):
        self.destination.mkdir()
        marker = self.destination / 'existing'
        marker.write_bytes(b'prior data')
        with self.assertRaises(stopped_viewer_backup.BackupRefused):
            self.prepare()
        self.assertEqual(marker.read_bytes(), b'prior data')
        self.assertEqual(self.result.read_bytes(), b'synthetic retained result')

    def test_nonempty_reference_state_holds_even_when_in_baseline(self):
        with sqlite3.connect(self.source / 'jobs.sqlite3') as db:
            db.execute('CREATE TABLE reference_synthetic (id TEXT)')
            db.execute("INSERT INTO reference_synthetic VALUES ('synthetic')")
        self.baseline = state_census.census(self.source, self.key)
        self.schema = self.baseline['database']['objects'].copy()
        with self.assertRaisesRegex(stopped_viewer_backup.BackupRefused, 'reference_history_requires_inspection'):
            self.prepare()
        self.assertFalse(self.destination.exists())
        self.assertEqual(self.result.read_bytes(), b'synthetic retained result')


if __name__ == '__main__':
    unittest.main()
