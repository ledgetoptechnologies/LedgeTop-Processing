"""Real SQLite read-only fences for an as-yet-unqualified maintenance ABI."""
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest

from ops import reference_worker_maintenance as m


@unittest.skipUnless(os.name == 'posix', 'actual private POSIX worker files')
class IndexedMaintenanceFenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.root.chmod(0o700)
        for filename, table in m._HISTORY.items():
            path = self.root / filename
            with sqlite3.connect(path) as db:
                if table == 'driver_jobs':
                    db.execute('CREATE TABLE driver_jobs(job TEXT,stage TEXT,data TEXT)')
                    db.execute("INSERT INTO driver_jobs VALUES('retained','completed','{}')")
                    db.execute('CREATE TABLE driver_binding(singleton INTEGER PRIMARY KEY,binding TEXT NOT NULL)')
                    db.execute('INSERT INTO driver_binding VALUES(1,?)', (json.dumps({'inputCopyAccounting': {'schemaVersion': 1}}),))
                else:
                    db.execute('CREATE TABLE ' + table + '(job TEXT,state TEXT,phase TEXT)')
            path.chmod(0o600)
        path = self.root / 'scratch.sqlite'
        with sqlite3.connect(path) as db:
            db.execute('CREATE TABLE pins(reservation TEXT,object TEXT,reference TEXT,kind TEXT,state TEXT)')
            db.execute('CREATE TABLE reservations(state TEXT)')
            db.execute('CREATE TABLE reader_owned_pins(reservation TEXT,object TEXT,reference TEXT,state TEXT)')
            db.execute('CREATE TABLE reader_session_epochs(epoch TEXT,state TEXT,successor TEXT)')
            db.execute("INSERT INTO reader_session_epochs VALUES('closed-epoch','closed',NULL)")
            db.execute('CREATE TABLE reader_session_binding(id INTEGER,current_epoch TEXT)')
            db.execute("INSERT INTO reader_session_binding VALUES(1,'closed-epoch')")
        path.chmod(0o600)

    def change_driver(self, sql, args=()):
        with sqlite3.connect(self.root / 'driver.sqlite') as db:
            db.execute(sql, args)

    def snapshot(self):
        return {p.name: (p.stat().st_ino, hashlib.sha256(p.read_bytes()).hexdigest())
                for p in self.root.iterdir()}

    def assert_refused_without_writes(self):
        before = self.snapshot()
        for reader in (m.offline_census, m.immutable_history):
            with self.subTest(reader=reader.__name__):
                with self.assertRaisesRegex(m.WorkerMaintenanceHeld, '^indexed_worker_maintenance_not_qualified$'):
                    reader(self.root)
                self.assertEqual(self.snapshot(), before)

    def test_retained_legacy_history_is_still_read(self):
        before = self.snapshot()
        history = m.immutable_history(self.root)
        self.assertEqual(set(history), set(m._HISTORY))
        self.assertEqual(history['driver.sqlite'], m.digest([dict(job='retained', stage='completed', data='{}')]))
        self.assertEqual(m.offline_census(self.root)['driver.sqlite']['driver_jobs'], history['driver.sqlite'])
        self.assertEqual(self.snapshot(), before)

    def test_empty_profile_table_alone_refuses(self):
        self.change_driver('CREATE TABLE indexed_profile(singleton INTEGER PRIMARY KEY,record TEXT NOT NULL)')
        self.assert_refused_without_writes()

    def test_orphan_normalized_history_without_profile_refuses(self):
        self.change_driver('CREATE TABLE indexed_attempts(job TEXT,attempt INTEGER)')
        self.change_driver("INSERT INTO indexed_attempts VALUES('retained',1)")
        self.assert_refused_without_writes()

    def test_binding_without_tables_refuses_even_null_marker(self):
        self.change_driver('UPDATE driver_binding SET binding=?', (json.dumps({'indexedFetchJournal': None}),))
        self.assert_refused_without_writes()

    def test_stripped_binding_does_not_hide_tables(self):
        self.change_driver('DELETE FROM driver_binding')
        self.change_driver('CREATE TABLE indexed_objects(job TEXT,object TEXT)')
        self.assert_refused_without_writes()

    def test_sqlite_case_insensitive_table_alias_refuses(self):
        self.change_driver('DELETE FROM driver_binding')
        self.change_driver('CREATE TABLE INDEXED_PROFILE(singleton INTEGER,record TEXT)')
        self.assert_refused_without_writes()


if __name__ == '__main__':
    unittest.main()
