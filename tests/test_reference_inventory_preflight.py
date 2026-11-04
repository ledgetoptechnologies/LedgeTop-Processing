import sqlite3
from contextlib import closing
import hashlib
import tempfile
import unittest
from pathlib import Path

from ops.reference_inventory_preflight import inspect_snapshot


class OfflineSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'snapshot.sqlite3'
        with closing(sqlite3.connect(self.path)) as db, db:
            db.executescript('''
                CREATE TABLE jobs(status TEXT);
                CREATE TABLE viewer_tasks(phase TEXT);
                CREATE TABLE coordinator_identity(id TEXT);
                CREATE TABLE coordinator_authority_lease(id TEXT);
                CREATE TABLE reference_jobs(id TEXT);
                CREATE TABLE reference_future_state(id TEXT);
                INSERT INTO coordinator_identity VALUES('owner');
                INSERT INTO coordinator_authority_lease VALUES('lease');
                INSERT INTO jobs VALUES('succeeded');
                INSERT INTO viewer_tasks VALUES('complete');
            ''')

    def test_counts_unrecognized_reference_tables_and_active_work(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("INSERT INTO reference_future_state VALUES('opaque')")
            db.execute("INSERT INTO jobs VALUES('held')")
        before = hashlib.sha256(self.path.read_bytes()).hexdigest()
        value = inspect_snapshot(self.path)
        self.assertEqual(before, hashlib.sha256(self.path.read_bytes()).hexdigest())
        self.assertFalse(value['snapshotHasNoReferenceRows'])
        self.assertFalse(value['snapshotHasNoActiveJobs'])
        self.assertEqual(value['referenceTableCounts']['reference_future_state'], 1)
        self.assertEqual(value['authorityLeaseRows'], 1)
        self.assertTrue(value['advisoryOnly'])

    def test_unmerged_wal_and_missing_schema_fail_closed(self):
        Path(str(self.path) + '-wal').touch()
        with self.assertRaises(ValueError):
            inspect_snapshot(self.path)
        Path(str(self.path) + '-wal').unlink()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TABLE coordinator_authority_lease')
        with self.assertRaises(ValueError):
            inspect_snapshot(self.path)


if __name__ == '__main__':
    unittest.main()
