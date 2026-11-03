"""Synthetic daemon composition: a guarded worker retains ambiguous results.

This uses the operational worker builder and a loopback synthetic engine, not
the production configuration or a real processing host. The daemon's TLS
listener is stubbed as in the underlying composition fixture.
"""
import json
import sys
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from ops import reference_worker_daemon as daemon
from processing.host_mutation_journal import HostMutationJournal
from processing.node_gate import NodeGate
from processing.odx_smoke_worker import SmokeError
from processing.reference_engine_worker import ReferenceEngineWorker, WorkerHeld
from processing.reference_protocol import validate_submission
from tests import test_reference_worker_daemon_routing as common


@unittest.skipUnless(sys.platform.startswith('linux'), 'Linux daemon composition fixture required')
class GuardedDaemonFixtureTests(unittest.TestCase):
    setUp = common.WorkerDaemonRoutingTests.setUp
    load = common.WorkerDaemonRoutingTests.load
    provision = common.WorkerDaemonRoutingTests.provision
    listener = common.WorkerDaemonRoutingTests.listener
    docker = common.WorkerDaemonRoutingTests.docker
    build = common.WorkerDaemonRoutingTests.build

    def exercise_commit(self, ambiguous):
        original = self.backend._dispatch

        def dispatch(method, path, headers, body):
            status, value, binary = original(method, path, headers, body)
            if method == 'POST' and path.startswith('/task/new/commit/') and status == 200:
                # The mutation has occurred, but the second scenario loses its
                # matching acknowledgment, so the outcome must stay held.
                value = {'uuid': str(uuid.uuid4()) if ambiguous else path.rsplit('/', 1)[-1]}
            return status, value, binary

        self.backend._dispatch = dispatch
        def construct(*args, **kwargs):
            return ReferenceEngineWorker(*args, host_mutation_journal=True, **kwargs)
        with patch.object(daemon, 'ReferenceEngineWorker', side_effect=construct):
            driver, _ = self.build(provision=True)
        worker, ledger = driver.worker, driver.ledger
        raw = json.loads((Path(__file__).resolve().parents[1] / 'contracts/reference-v1.contract.json').read_text())['submission']
        submission = validate_submission(raw, environment='staging', capability_fingerprint=raw['capabilityFingerprint'])
        job = dict(jobId=str(uuid.uuid4()), generation=1, environment='staging', status='running',
            nodeId='main', manifestSha256=submission['manifestSha256'], pipelineDigest=submission['pipelineDigest'],
            submission=submission, claimToken='synthetic-private-claim')
        ledger.reserve(job_id=job['jobId'], generation=1, reservation_id='reservation-1',
            manifest_sha256=job['manifestSha256'], input_bytes=3, output_budget=1024,
            environment='staging', ttl_seconds=3600, worker_ready=True)
        directory = ledger.root / 'jobs' / 'reservation-1'
        directory.mkdir(parents=True, mode=0o700)
        (directory / 'image-1.jpg').write_bytes(b'abc')
        ledger.record_allocated('reservation-1', 3)
        paths = {'image-1': 'jobs/reservation-1/image-1.jpg'}
        if ambiguous:
            with self.assertRaises((WorkerHeld, SmokeError)):
                worker.start(job, reservation_id='reservation-1', input_paths=paths)
        else:
            self.assertEqual(worker.start(job, reservation_id='reservation-1', input_paths=paths), 'processing')
        row = worker._row(job['jobId'])
        with NodeGate(self.config['engineId']) as gate:
            journal = HostMutationJournal(gate, row['receipt']).open()
            try:
                operations = journal.db.execute('SELECT kind,state FROM operations ORDER BY seq').fetchall()
                self.assertEqual(operations, [('init', 'done'), ('upload', 'done'),
                    ('commit', 'pending' if ambiguous else 'done')])
                self.assertEqual(journal.job['id'], job['jobId'])
            finally:
                journal.close()
        with self.assertRaises(WorkerHeld):
            worker.poll(job['jobId'])
        # A replay observes the persisted phase instead of sending mutations.
        worker.start(job, reservation_id='reservation-1', input_paths=paths)
        self.assertEqual(self.backend.counts(), dict(initialize=1, upload=1, commit=1, download=0))

    def test_guarded_daemon_worker_journals_real_http_submission(self):
        self.exercise_commit(False)

    def test_guarded_daemon_worker_holds_lost_commit_ack(self):
        self.exercise_commit(True)
