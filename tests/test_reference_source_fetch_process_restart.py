"""v2 fetch recovery across real process death: SIGKILL, then fresh processes.

The coordinator and worker are separate OS processes talking verified TLS through
the real listener, driver request path and range client. The hold comes from the
real Queue startup and is lifted only by the worker's real recovery. The test
reads the databases and never writes them. See source_fetch_process_fixture.
"""
from contextlib import closing
import json
import os
from pathlib import Path
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

from processing import reference_protocol as protocol
from processing.node_gate import NodeGate
from processing.source_spool import SourceSpool
from tests.source_fetch_process_fixture import CLAIM_REQUEST, ENGINE_LOG, GATE_ORIGIN
from tests import test_reference_source_intake as intake
from tests.test_viewer_result_v2_roundtrip import _write_tls_material

REPOSITORY = Path(__file__).resolve().parent.parent
REPLY_SECONDS = 60  # a step takes milliseconds; past this the child is stuck
REAP_SECONDS = 10


def process_census():
    """Each process in /proc as pid -> (parent, process group, state); the image has no ps."""
    census = {}
    for entry in os.listdir('/proc'):
        if entry.isdigit():
            try:
                stat = Path('/proc', entry, 'stat').read_text(errors='replace')
            except OSError:
                continue
            state, parent, group = stat[stat.rindex(')') + 2:].split()[:3]
            census[int(entry)] = (int(parent), int(group), state)
    return census


@unittest.skipIf(os.name == 'nt', 'SIGKILL and the private ledger paths are POSIX')
class ProcessHarness(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spawned = cls.reaped = 0

    @classmethod
    def tearDownClass(cls):
        left = sorted(pid for pid, row in process_census().items() if row[0] == os.getpid())
        print(f'reap census {cls.__name__}: spawned={cls.spawned} reaped={cls.reaped} remaining={left}',
              file=sys.stderr, flush=True)
        if left or cls.spawned != cls.reaped:
            raise AssertionError('children outlived ' + cls.__name__)

    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.children = []
        self.addCleanup(self.assert_reaped)  # runs after every child's own cleanup
        self.root = Path(directory.name)
        case = intake.ReferenceSourceIntakeTests('runTest')
        case.setUp()
        case.payload['manifest']['id'] = 'dataset-process-restart'
        case.rehash()
        spool_root = self.root / 'spool'
        spool_root.mkdir(mode=0o700)
        marker = spool_root / '.ltds-source-spool-id'
        marker.write_text('a' * 64, encoding='ascii')
        marker.chmod(0o600)
        (self.root / 'scratch').mkdir(mode=0o700)
        with patch('processing.source_spool.os.path.ismount', return_value=True):
            spool = SourceSpool(spool_root, mount_id='a' * 64, safety_floor_bytes=0, max_object_bytes=1024, chunk_bytes=3)
            queue, _store, _control, job, _token = case._make_reserved_source_job(
                str(self.root), spool, 'process', source_range_v2=True, claim=False)
        authority = queue.authority_id
        self.database = Path(queue.path)
        queue.close()
        with patch('processing.node_gate.canonical_gate_path', return_value=self.root / 'gate'), NodeGate('engine') as gate:
            gate.provision('cluster', GATE_ORIGIN, authority)
        key, cert = _write_tls_material(self.root)
        with closing(socket.socket()) as probe:
            probe.bind(('127.0.0.1', 0))
            port = probe.getsockname()[1]
        self.job_id = job['jobId']
        self.config = self.root / 'process-config.json'
        self.config.write_text(json.dumps(dict(root=str(self.root), port=port, cert=str(cert), key=str(key),
            authorityId=authority, jobId=self.job_id, trustedKeys=case.trusted_keys,
            optionsFingerprint=case.payload['pipeline']['odm']['optionsFingerprint'])))

    def spawn(self, role, **env):
        child = subprocess.Popen([sys.executable, '-m', 'tests.source_fetch_process_fixture', role, str(self.config)],
            cwd=REPOSITORY, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, env={**os.environ, **env},
            start_new_session=True)
        self.children.append(child)
        type(self).spawned += 1
        self.addCleanup(self.kill, child)
        self.assertEqual(self.reply(child).strip(), 'ready', role + ' did not start')
        return child

    @staticmethod
    def kill_group(child):
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    def reply(self, child):
        """One line, or EOF once a stuck child's whole group has been killed."""
        watchdog = threading.Timer(REPLY_SECONDS, self.kill_group, (child,))
        watchdog.start()
        try:
            return child.stdout.readline()
        finally:
            watchdog.cancel()

    def kill(self, child):
        self.kill_group(child)
        child.wait(REAP_SECONDS)
        child.stdin.close()
        child.stdout.close()

    def descendants(self):
        """pid -> state of this process's children and anything left in their groups."""
        groups = {child.pid for child in self.children}
        return {pid: state for pid, (parent, group, state) in process_census().items()
                if parent == os.getpid() or group in groups}

    def tearDown(self):
        """Before teardown: the live children are exactly those not yet reaped, none a zombie."""
        census = self.descendants()
        self.assertEqual(set(census), {child.pid for child in self.children if child.returncode is None})
        self.assertNotIn('Z', census.values())

    def assert_reaped(self):
        self.assertEqual(self.descendants(), {})
        self.assertEqual([child.pid for child in self.children if child.returncode is None], [])
        type(self).reaped += len(self.children)

    def step(self, worker):
        worker.stdin.write('step\n')
        worker.stdin.flush()
        line = self.reply(worker)
        return json.loads(line) if line else None

    def advance(self, worker, *states):
        for state in states:
            self.assertEqual(self.step(worker), {'state': state, 'jobId': self.job_id})

    def read(self, path, query, *values):
        with closing(sqlite3.connect('file:' + str(path) + '?mode=ro', uri=True)) as db:
            return db.execute(query, values).fetchall()

    def status(self):
        return self.read(self.database, 'SELECT status,held_reason FROM jobs WHERE id=?', self.job_id)[0]

    def receipts(self):
        return [json.loads(row[0]) for row in self.read(self.database, 'SELECT receipt_json FROM reference_recovery_receipts')]

    def journal(self):
        stage, data = self.read(self.root / 'driver-journal.sqlite', 'SELECT stage,data FROM driver_jobs WHERE claim_request=?',
            CLAIM_REQUEST)[0]
        return stage, json.loads(data)

    def to_first_object(self, worker):
        self.advance(worker, 'awaiting_grant', 'fetching_pending', 'fetching', 'fetching')
        self.assertEqual(len(self.journal()[1]['inputPaths']), 1)

    def assert_recovered(self, worker, *, receipts=1):
        """The receipt binds what step() posted, and the row still reaches the engine's door."""
        stage, data = self.journal()
        self.assertEqual(self.status(), ('running', None))
        self.assertEqual(len(self.receipts()), receipts)
        self.assertEqual({item['transferRequestSha256'] for item in self.receipts()}, {protocol.digest(data['reservation'])})
        self.assertEqual(data['sourceRecovery'], {'attempts': 0, 'pending': None})
        fence = self.read(self.root / 'ledger.sqlite', 'SELECT COUNT(*) FROM transfer_fences WHERE reservation=?',
            data['reservation']['reservationId'])
        self.assertEqual(fence, [(1,)])
        while self.journal()[0] == 'fetching':
            self.assertIn(self.step(worker)['state'], ('fetching', 'processing_pending'))
        self.advance(worker, 'execution_pending')
        self.assertEqual(self.read(self.root / 'ledger.sqlite', 'SELECT COUNT(*) FROM reservations'), [(1,)])

    def engine_calls(self):
        log = self.root / ENGINE_LOG
        return log.read_text().split() if log.exists() else []

    def fetched(self, worker):
        self.to_first_object(worker)
        while self.journal()[0] == 'fetching':
            self.assertIn(self.step(worker)['state'], ('fetching', 'processing_pending'))

    def restart_both(self, worker, coordinator):
        self.kill(worker)
        self.kill(coordinator)
        self.spawn('coordinator')
        self.assertEqual(self.status(), ('held', 'coordinator_restart'))

    def assert_started_once(self, worker):
        """Stops at the first polls: FileEngine reports status 20 and never finishes."""
        for _ in range(12):
            if self.journal()[0] == 'executing':
                break
            result = self.step(worker)
            self.assertNotIn(result['state'], ('held', 'retry_wait'), result)
        self.assertEqual(self.journal()[0], 'executing')
        for _ in range(3):
            self.assertEqual(self.step(worker)['state'], 'executing')
        self.assertEqual(self.status(), ('running', None))
        self.assertEqual(self.engine_calls().count('initialize'), 1)
        self.assertEqual(self.engine_calls().count('commit'), 1)


class ProcessRestartTests(ProcessHarness):
    def test_control_no_restart_starts_once(self):
        self.spawn('coordinator')
        worker = self.spawn('worker')
        self.fetched(worker)
        self.assert_started_once(worker)
        self.assertEqual(self.receipts(), [])

    def test_killed_at_execution_pending_recovers_and_starts_once(self):
        coordinator = self.spawn('coordinator')
        worker = self.spawn('worker')
        self.fetched(worker)
        self.advance(worker, 'execution_pending')
        self.restart_both(worker, coordinator)
        worker = self.spawn('worker')
        self.advance(worker, 'execution_pending')
        self.assertEqual(self.engine_calls(), [])
        self.assertEqual(self.receipts()[0]['engineClass'], 'unstarted')
        self.assert_started_once(worker)

    def test_killed_once_processing_committed_recovers_and_starts_once(self):
        coordinator = self.spawn('coordinator')
        dying = self.spawn('worker', SOURCE_FETCH_DIE='after_processing_phase')
        self.to_first_object(dying)
        while self.journal()[0] == 'fetching':
            self.assertEqual(self.step(dying)['jobId'], self.job_id)
        self.assertIsNone(self.step(dying))
        self.assertEqual(self.journal()[0], 'processing_pending')
        self.restart_both(dying, coordinator)
        worker = self.spawn('worker')
        self.advance(worker, 'processing_pending')
        self.assertEqual(self.receipts()[0]['executionPhase'], 'processing')
        self.assert_started_once(worker)

    def test_killed_before_the_first_byte_recovers_and_starts_once(self):
        coordinator = self.spawn('coordinator')
        worker = self.spawn('worker')
        self.advance(worker, 'awaiting_grant')
        self.restart_both(worker, coordinator)
        worker = self.spawn('worker')
        self.advance(worker, 'awaiting_grant')
        self.assertEqual(self.receipts()[0]['executionPhase'], 'awaiting_transfer')
        self.assertIsNone(self.receipts()[0]['grantSha256'])
        self.assert_started_once(worker)

    def test_a_running_engine_is_visible_again_after_both_processes_die(self):
        coordinator = self.spawn('coordinator')
        worker = self.spawn('worker')
        self.fetched(worker)
        self.assert_started_once(worker)
        self.restart_both(worker, coordinator)
        worker = self.spawn('worker')
        self.advance(worker, 'executing')
        self.assertEqual(self.status(), ('running', None))
        self.assertEqual(self.receipts()[0]['engineClass'], 'started')
        self.assert_started_once(worker)

    def assert_died_in_engine(self, call, *, starts, receipts=0):
        coordinator = self.spawn('coordinator')
        dying = self.spawn('worker', SOURCE_ENGINE_DIE=call)
        self.fetched(dying)
        self.advance(dying, 'execution_pending')
        self.assertIsNone(self.step(dying))
        self.assertEqual(dying.wait(REAP_SECONDS), -9)
        self.restart_both(dying, coordinator)
        worker = self.spawn('worker')
        for _ in range(4):
            self.step(worker)
        self.assertEqual(self.engine_calls().count('initialize'), starts)
        self.assertEqual(len(self.receipts()), receipts)
        self.assertEqual(self.status(), ('running', None) if receipts else ('held', 'coordinator_restart'))

    def test_killed_inside_initialize_is_never_started_again(self):
        self.assert_died_in_engine('initialize', starts=1)

    def test_killed_inside_upload_is_never_started_again(self):
        self.assert_died_in_engine('upload', starts=1)

    def test_killed_inside_commit_is_never_started_again(self):
        # Output proves this commit reached the engine, so it is visible, never restarted.
        self.assert_died_in_engine('commit', starts=1, receipts=1)

    def test_both_processes_killed_mid_fetch_resume_the_original_claim(self):
        coordinator, worker = self.spawn('coordinator'), None
        worker = self.spawn('worker')
        self.to_first_object(worker)
        token = self.journal()[1]['claim']['claimToken']
        self.kill(worker)
        self.kill(coordinator)
        self.assertEqual(self.status(), ('running', None))
        self.spawn('coordinator')
        self.assertEqual(self.status(), ('held', 'coordinator_restart'))
        worker = self.spawn('worker')
        self.advance(worker, 'fetching')
        self.assertEqual(self.journal()[1]['claim']['claimToken'], token)
        self.assert_recovered(worker)

    def test_a_worker_outlives_a_killed_coordinator_and_spends_nothing_waiting(self):
        coordinator = self.spawn('coordinator')
        worker = self.spawn('worker')
        self.to_first_object(worker)
        self.kill(coordinator)
        for _ in range(3):
            self.assertEqual(self.step(worker), {'state': 'retry_wait', 'reason': 'coordinator_unavailable'})
        self.assertNotIn('sourceRecovery', self.journal()[1])
        self.spawn('coordinator')
        self.advance(worker, 'fetching')
        self.assert_recovered(worker)

    def test_worker_killed_before_its_post_sends_the_saved_intent_from_a_new_process(self):
        coordinator = self.spawn('coordinator')
        worker = self.spawn('worker')
        self.to_first_object(worker)
        self.kill(worker)
        self.kill(coordinator)
        self.spawn('coordinator')
        dying = self.spawn('worker', SOURCE_FETCH_DIE='before_post')
        self.assertIsNone(self.step(dying))
        self.assertEqual(dying.wait(REAP_SECONDS), -9)
        recovery = self.journal()[1]['sourceRecovery']
        self.assertEqual(recovery['attempts'], 1)
        self.assertEqual(self.status(), ('held', 'coordinator_restart'))
        worker = self.spawn('worker')
        self.advance(worker, 'fetching')
        self.assertEqual(self.receipts()[0]['requestId'], recovery['pending']['request']['requestId'])
        self.assert_recovered(worker)

    def test_worker_killed_after_a_committed_post_replays_the_one_receipt(self):
        coordinator = self.spawn('coordinator')
        worker = self.spawn('worker')
        self.to_first_object(worker)
        self.kill(worker)
        self.kill(coordinator)
        self.spawn('coordinator')
        dying = self.spawn('worker', SOURCE_FETCH_DIE='after_post')
        self.assertIsNone(self.step(dying))
        self.assertEqual(dying.wait(REAP_SECONDS), -9)
        self.assertIsNotNone(self.journal()[1]['sourceRecovery']['pending'])
        self.assertEqual((self.status(), len(self.receipts())), (('running', None), 1))
        worker = self.spawn('worker')
        self.advance(worker, 'fetching')
        self.assert_recovered(worker)

    def test_sigkill_after_claim_commit_before_reply_recovers_original_claim(self):
        coordinator = self.spawn('coordinator')
        dying = self.spawn('worker', SOURCE_NORMAL_CLAIM='1', SOURCE_FETCH_DIE='after_claim')
        self.assertIsNone(self.step(dying))
        self.assertEqual(dying.wait(REAP_SECONDS), -9)
        self.assertEqual(self.journal()[0], 'claim_pending')
        claims = self.read(self.database,'SELECT request_id,token FROM reference_claims')
        self.assertEqual(len(claims),1)
        self.restart_both(dying,coordinator)
        worker = self.spawn('worker')
        self.advance(worker,'claimed')
        self.assert_started_once(worker)
        self.assertEqual(self.read(self.database,'SELECT request_id,token FROM reference_claims'),claims)
        self.assertEqual(self.receipts()[0]['schemaVersion'],4)

    def test_sigkill_before_reservation_post_recovers_original_reservation(self):
        coordinator = self.spawn('coordinator')
        dying = self.spawn('worker', SOURCE_FETCH_DIE='before_reservation')
        self.assertIsNone(self.step(dying))
        self.assertEqual(dying.wait(REAP_SECONDS),-9)
        reservation = self.journal()[1]['reservation']
        self.assertEqual(self.read(self.database,'SELECT transfer_json FROM reference_jobs'),[(None,)])
        self.restart_both(dying,coordinator)
        worker = self.spawn('worker')
        self.assert_started_once(worker)
        self.assertEqual(self.journal()[1]['reservation'],reservation)
        self.assertEqual(self.receipts()[0]['schemaVersion'],4)

    def test_sigkill_after_reservation_commit_uses_existing_recovery(self):
        coordinator = self.spawn('coordinator')
        dying = self.spawn('worker', SOURCE_FETCH_DIE='after_reservation')
        self.assertIsNone(self.step(dying))
        self.assertEqual(dying.wait(REAP_SECONDS),-9)
        reservation = self.journal()[1]['reservation']
        self.assertEqual(json.loads(self.read(self.database,'SELECT transfer_json FROM reference_jobs')[0][0]),reservation)
        self.restart_both(dying,coordinator)
        worker = self.spawn('worker')
        self.assert_started_once(worker)
        self.assertEqual(self.journal()[1]['reservation'],reservation)
        self.assertEqual(self.receipts()[0]['schemaVersion'],3)


if __name__ == '__main__':
    unittest.main()
