"""The fixture driver's fetch fences like the main driver; real ledger and journal."""
import hashlib
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from ops import reference_joint_driver as m
from processing.scratch_policy import ScratchLedger


class JointDriverFenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / 'scratch').mkdir(mode=0o700)
        self.ledger = ScratchLedger(self.root / 'ledger.sqlite', self.root / 'scratch', capacity_probe=lambda _: 10**9, safety_floor_bytes=0)
        self.ledger.reserve(job_id='job', generation=1, reservation_id='fixture-r', manifest_sha256='a' * 64, input_bytes=m.INPUT_BYTES,
            output_budget=m.OUTPUT_BUDGET, environment='staging', ttl_seconds=m.TTL_SECONDS, worker_ready=True)
        self.events, self.past = [], lambda: time.time() + 10**6
        driver = self.driver = object.__new__(m.JointDriver)
        driver.ledger, driver.clock, driver._lock, driver.path = self.ledger, time.time, threading.Lock(), self.root / 'driver.sqlite'
        driver.broker = self
        grant = patch.object(m, 'validate_grant', side_effect=lambda grant, **_: grant)
        grant.start()
        self.addCleanup(grant.stop)
        fence, original = patch.object(self.ledger, 'begin_transfer'), self.ledger.begin_transfer
        fence.start().side_effect = lambda reservation: (original(reservation), self.events.append('fence'))
        self.addCleanup(fence.stop)
        claim = dict(claimToken='synthetic', submission=dict(manifest=dict(objects=[dict(id='o1', byteSize=m.INPUT_BYTES, sha256=m.INPUT_SHA256)])))
        with driver._db() as db:
            db.execute('CREATE TABLE driver_jobs(created INTEGER PRIMARY KEY AUTOINCREMENT,job TEXT UNIQUE,claim_request TEXT UNIQUE,stage TEXT,data TEXT)')
            db.execute("INSERT INTO driver_jobs(job,claim_request,stage,data) VALUES('job','request','fetching',?)",
                (json.dumps(dict(claim=claim, grant={}, reservation=dict(reservationId='fixture-r'))),))

    def download_object(self, grant, object_id, destination):
        self.events.append('byte')
        self.ledger.clock = self.past
        Path(destination).write_bytes(b'abc')

    def row(self):
        with self.driver._db() as db:
            stage, data = db.execute('SELECT stage,data FROM driver_jobs').fetchone()
        with self.ledger._transaction() as db:
            return stage, json.loads(data), dict(self.ledger._reservation(db, 'fixture-r'))

    def test_fixture_transfer_is_fenced_before_its_first_byte_and_survives_the_lease(self):
        self.assertEqual(self.driver.step(), {'state': 'processing_pending', 'jobId': 'job'})
        self.assertEqual(self.events, ['fence', 'byte'])
        stage, data, reservation = self.row()
        self.assertEqual((stage, reservation['state'], reservation['allocated']), ('processing_pending', 'reserved', m.INPUT_BYTES))
        self.ledger.set_engine_state('fixture-r', 'engine_started')

    def test_fixture_lease_expired_before_first_byte_is_named_and_writes_nothing(self):
        self.ledger.clock = self.past
        self.assertEqual(self.driver.step(), {'state': 'held', 'reason': 'reservation_expired'})
        stage, data, reservation = self.row()
        self.assertEqual((stage, reservation['state'], self.events), ('fetching', 'expired', []))
        self.assertNotIn('fetchAttempts', data)
        self.assertFalse((self.root / 'scratch' / 'jobs' / 'fixture-r' / 'o1.input').exists())
