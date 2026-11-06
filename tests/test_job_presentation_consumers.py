"""Cutover and update censuses against the presentation_start row.

The table being in each census proves nothing about this kind of row, so each one
is run on a database that holds it. They all stop closed. None adds a stop a real
job would not already meet: this row needs a claim, and the same check refuses that.
"""
from contextlib import closing
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest

from tests import test_coordinator_candidate_projection as candidate
from tests import test_coordinator_predecessor_census as predecessor
from ops import update_reference_coordinator as update

OUTBOX = ("INSERT INTO reference_outbox(identity,kind,payload_json,digest,acknowledged) "
    "VALUES('presentation-start:synthetic','presentation_start','{}','digest',1)")
CLAIM = ("INSERT INTO reference_claims(request_id,job_id,node_id,worker_id,instance_id,token) "
    "VALUES('request','synthetic','cluster','worker','instance','token')")


@unittest.skipUnless(os.name == 'posix', 'Linux custody ownership')
class PredecessorCensusTests(unittest.TestCase):
    def setUp(self):
        self.case = predecessor.ClosedPredecessorCensusTests('runTest')
        self.addCleanup(self.case.doCleanups)
        self.case.setUp()

    def test_control_clean_fixture_passes(self):
        self.assertEqual(self.case.census()['tables']['reference_outbox']['count'], 0)

    def test_a_presentation_row_is_refused_as_unsettled_execution(self):
        self.case.mutate(OUTBOX)
        with self.assertRaisesRegex(predecessor.c.CensusRefused, 'unsettled_predecessor_execution'):
            self.case.census()

    def test_the_claim_such_a_row_needs_is_refused_the_same_way_without_it(self):
        self.case.mutate(CLAIM)
        with self.assertRaisesRegex(predecessor.c.CensusRefused, 'detached_retained_reference|unsettled_predecessor_execution'):
            self.case.census()


@unittest.skipUnless(os.name == 'posix', 'Linux custody ownership')
class CandidateProjectionTests(unittest.TestCase):
    def setUp(self):
        self.case = candidate.CandidateProjectionTests('runTest')
        self.addCleanup(self.case.doCleanups)
        self.case.setUp()
        with self.case.fixture.db() as db:
            names = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertIn('reference_outbox', names)

    def test_a_presentation_row_is_refused_as_changed_legacy_history(self):
        with self.case.fixture.db() as db:
            objects = [list(row) for row in db.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name')]
        for phase in self.case.policy['phases'].values():
            phase['objects'] = objects
        self.case.fixture.mutate(OUTBOX)
        with self.assertRaisesRegex(candidate.old.CensusRefused, 'legacy_rows_changed'):
            self.case.verify()


@unittest.skipUnless(os.name == 'posix', 'Linux ownership and offline helper contract')
class VolumeCensusTests(unittest.TestCase):
    """A minimal volume, because a current coordinator's schema stops this census earlier.

    Its allowlist predates tables such as coordinator_freeze_state, so this builds
    only tables it allows, which is the one way to reach the row check.
    """

    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.volume = Path(directory.name) / 'volume'
        self.volume.mkdir(mode=0o700)
        with closing(sqlite3.connect(self.volume / 'jobs.sqlite3')) as db, db:
            db.executescript('''
                CREATE TABLE jobs(id TEXT PRIMARY KEY, status TEXT);
                CREATE TABLE coordinator_identity(singleton INTEGER PRIMARY KEY, authority_id TEXT);
                CREATE TABLE nodes(id TEXT PRIMARY KEY);
                CREATE TABLE reference_jobs(job_id TEXT PRIMARY KEY);
                CREATE TABLE viewer_tasks(uuid TEXT PRIMARY KEY);
                CREATE TABLE reference_outbox(identity TEXT PRIMARY KEY, kind TEXT, payload_json TEXT, digest TEXT,
                    attempts INTEGER DEFAULT 0, next_attempt REAL DEFAULT 0, acknowledged INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP);
                CREATE TABLE reference_claims(request_id TEXT PRIMARY KEY, job_id TEXT, node_id TEXT, worker_id TEXT,
                    instance_id TEXT, token TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP);
                INSERT INTO coordinator_identity VALUES(1,'authority');
            ''')

    def execute(self, sql):
        with closing(sqlite3.connect(self.volume / 'jobs.sqlite3')) as db, db:
            db.execute(sql)

    def census(self):
        return update.volume_census(self.volume, uid=os.geteuid())

    def test_control_clean_volume_passes(self):
        self.assertEqual(self.census()['tables']['reference_outbox']['count'], 0)

    def test_a_presentation_row_stops_the_census(self):
        self.execute(OUTBOX)
        with self.assertRaises(AssertionError):
            self.census()

    def test_the_claim_such_a_row_needs_stops_the_census_without_it(self):
        self.execute(CLAIM)
        with self.assertRaises(AssertionError):
            self.census()


if __name__ == '__main__':
    unittest.main()
