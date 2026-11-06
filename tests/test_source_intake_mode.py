import sqlite3
from contextlib import contextmanager
import tempfile
import threading
import unittest
from pathlib import Path

from processing import source_intake_mode as mode
from processing.maintenance import Conflict


@contextmanager
def open_db(path):
    db = sqlite3.connect(path, timeout=5)
    db.row_factory = sqlite3.Row
    try:
        with db:
            yield db
    finally:
        db.close()


class SourceIntakeModeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = str(Path(self.temp.name) / 'queue.sqlite')
        with open_db(self.path) as db:
            db.execute('CREATE TABLE reference_source_intakes(job_id TEXT PRIMARY KEY)')
            db.execute("INSERT INTO reference_source_intakes VALUES('legacy')")

    def test_one_time_migration_and_immutable_mode(self):
        with open_db(self.path) as db:
            db.execute('BEGIN IMMEDIATE')
            mode.initialize(db)
            mode.require(db, 'legacy', 'spool-v1')
            with self.assertRaises(Conflict):
                mode.claim(db, 'legacy', 'direct-v2')
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute("UPDATE reference_source_intake_modes SET mode='direct-v2'")
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute('DELETE FROM reference_source_intake_modes')
        with open_db(self.path) as db:
            mode.initialize(db)
            mode.require(db, 'legacy', 'spool-v1')

    def test_rollback_does_not_publish_mode_or_job(self):
        with open_db(self.path) as db:
            mode.initialize(db)
        with open_db(self.path) as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute("INSERT INTO reference_source_intakes VALUES('new')")
            mode.claim(db, 'new', 'direct-v2')
            db.rollback()
        with open_db(self.path) as db:
            self.assertIsNone(db.execute("SELECT 1 FROM reference_source_intake_modes WHERE job_id='new'").fetchone())
            self.assertIsNone(db.execute("SELECT 1 FROM reference_source_intakes WHERE job_id='new'").fetchone())

    def test_missing_mode_on_existing_schema_is_not_silently_backfilled(self):
        with open_db(self.path) as db:
            mode.initialize(db)
            db.execute("INSERT INTO reference_source_intakes VALUES('unclassified')")
        with open_db(self.path) as db:
            with self.assertRaisesRegex(Conflict, 'mode missing'):
                mode.initialize(db)
            with self.assertRaisesRegex(Conflict, 'mode mismatch'):
                mode.require(db, 'unclassified', 'spool-v1')

    def test_direct_mode_refuses_spool_access(self):
        with open_db(self.path) as db:
            mode.initialize(db)
            db.execute("INSERT INTO reference_source_intakes VALUES('direct')")
            mode.claim(db, 'direct', 'direct-v2')
            mode.require(db, 'direct', 'direct-v2')
            with self.assertRaisesRegex(Conflict, 'mode mismatch'):
                mode.require(db, 'direct', 'spool-v1')

    def test_competing_modes_have_one_transactional_winner(self):
        with open_db(self.path) as db:
            mode.initialize(db)
            db.execute("INSERT INTO reference_source_intakes VALUES('new')")
        barrier = threading.Barrier(2)
        outcomes = []
        def attempt(value):
            with open_db(self.path) as db:
                barrier.wait(timeout=3)
                db.execute('BEGIN IMMEDIATE')
                try:
                    mode.claim(db, 'new', value)
                    outcomes.append(value)
                except Conflict:
                    outcomes.append('conflict')
        threads = [threading.Thread(target=attempt, args=(value,)) for value in ('spool-v1','direct-v2')]
        for thread in threads: thread.start()
        for thread in threads: thread.join(timeout=5)
        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual(outcomes.count('conflict'), 1)
        self.assertEqual(len(outcomes), 2)


if __name__ == '__main__':
    unittest.main()
