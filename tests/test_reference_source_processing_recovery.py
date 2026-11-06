"""A restart between the fetch and the engine resumes the original claim, once.

Real step(), recovery, routed v2 coordinator, fenced ledger, journal, node gate and
ReferenceEngineWorker. Only NativeEngine's transport is synthetic, so every start
is counted at the edge. Ambiguous local state stays held and is never adopted.
"""
import threading
from unittest.mock import patch

from processing import reference_protocol as protocol
from processing import reference_source_intake_store as intake_store
from processing.maintenance import Conflict
from processing.node_gate import NodeGate
from processing.reference_driver import ReferenceDriver
from processing.reference_engine_worker import ReferenceEngineWorker
from processing.reference_failure import FailureOutbox
from processing.reference_recovery import SOURCE_RECEIPT_PROTOCOL, ClaimRecoveryService
from processing.reference_sender import ReferenceSender
from processing.scratch_policy import ScratchLedger
from processing.viewer_service import NativeEngine
from processing.worker_outbox import WorkerOutbox
from tests import test_reference_driver as driver_tests
from tests import test_reference_source_fetch_recovery as fetch_tests
from tests.test_reference_source_grant_renewal import WORKER_TOKEN, _ControlOpener
from tests.viewer_v2_receipt_signer import SyntheticViewerCopyV2Signer

GATE_ORIGIN = 'http://127.0.0.1:18080'


class Transport(driver_tests.EngineTransport):
    """Counts starts. Status 20 by default, so a started task simply keeps running."""

    def __init__(self):
        super().__init__()
        self.code, self.version, self.down, self.die_on, self.output = 20, 'pinned-engine-1', False, None, []
        self.options = [dict(name='numeric-probe', type='float', value=1, help='Numeric setting.')]

    def initialize(self, task):
        super().initialize(task)
        if self.die_on == 'initialize':
            raise OSError('synthetic worker death during initialize')

    def upload(self, job, item):
        super().upload(job, item)
        if self.die_on == 'upload':
            raise OSError('synthetic worker death during upload')

    def action(self, action, job):
        super().action(action, job)
        if self.die_on == action:
            raise OSError('synthetic worker death during ' + action)

    def read(self, route, **kwargs):
        if self.down and route.startswith('/task'):
            raise OSError('synthetic engine outage')
        return super().read(route, **kwargs)

    def output_page(self, job, *, line, limit):
        self.calls.append(('output', job))
        return self.output[line:line + limit], False

    def starts(self):
        return sum(call[0] == 'initialize' for call in self.calls)


class ProcessingHarness(fetch_tests.StepHarness):
    extra_objects = 0

    def real_driver(self):
        gate = patch('processing.node_gate.canonical_gate_path', return_value=self.root / 'gate')
        gate.start()
        self.addCleanup(gate.stop)
        with NodeGate('engine') as physical:
            physical.provision('cluster', GATE_ORIGIN, self.queue.authority_id)
        self.transport = Transport()
        self.native = NativeEngine('http://127.0.0.1:3001')
        for method in ('initialize', 'upload', 'action', 'read', 'download', 'output_page'):
            patched = patch.object(self.native, method, side_effect=getattr(self.transport, method))
            patched.start()
            self.addCleanup(patched.stop)
        driver = super().real_driver()
        self.attach_worker(driver)
        return driver

    def attach_worker(self, driver):
        driver.worker = ReferenceEngineWorker(self.root / 'worker.sqlite', ledger=driver.ledger,
            outbox=WorkerOutbox(self.root / 'outbox.sqlite'), engine=self.native, engine_id='engine', node_id='cluster',
            authority_id=self.queue.authority_id, coordinator_origin=GATE_ORIGIN, gate_generation=1, worker_id='worker',
            engine_version='pinned-engine-1', allowed_odm_options={'numeric-probe'}, engine_probe=lambda _deadline: 'engine',
            viewer_receipt_keys=self.viewer_receipt_keys)
        driver.outbox, driver.failure_outbox = driver.worker.outbox, FailureOutbox(self.root / 'failures.sqlite')
        driver.allowed_environments = frozenset(('production', 'staging'))
        driver._option_definitions = {}

    def new_worker(self):
        """A worker process that died: only what reached disk comes back."""
        driver = object.__new__(ReferenceDriver)
        driver.__dict__.update(self.driver.__dict__)
        driver._lock, driver._active_step, driver._active_job = threading.Lock(), None, None
        self.ledger = ScratchLedger(self.root / 'ledger.sqlite', self.root / 'scratch',
            capacity_probe=lambda _: 10**9, safety_floor_bytes=0)
        self.ledger.clock = lambda: self.now[0]
        driver.ledger = self.ledger
        self.attach_worker(driver)
        self.driver = driver

    def to_processing_pending(self):
        self.to_fetching()
        self.finish_fetch()
        self.assertEqual(self.row()[0], 'processing_pending')

    def to_execution_pending(self):
        self.to_processing_pending()
        self.step('execution_pending')

    def lost_processing_reply(self):
        self.to_processing_pending()
        self.lose_reply.add('phase')
        self.assertEqual(self.driver.step()['state'], 'retry_wait')
        self.assertEqual((self.row()[0], self.store.get(self.job_id)['phase']), ('processing_pending', 'processing'))

    def run_to_engine(self):
        """Step until the row is executing; a refusal on the way fails the test."""
        for _ in range(12):
            if self.row()[0] == 'executing':
                break
            result = self.driver.step()
            self.assertNotIn(result['state'], ('held', 'retry_wait'), result)
        self.assertEqual(self.row()[0], 'executing')
        self.assertEqual(self.transport.starts(), 1)
        for _ in range(3):
            self.driver.step()
        self.assertEqual(self.transport.starts(), 1)
        self.assertEqual(self.status(), ('running', None))

    def assert_unstarted_receipt(self, count=1):
        receipts = self.receipts()
        self.assertEqual(len(receipts), count)
        self.assertEqual((receipts[-1]['schemaVersion'], receipts[-1]['protocol'], receipts[-1]['executionPhase'],
            receipts[-1]['engineClass']), (3, SOURCE_RECEIPT_PROTOCOL, 'processing', 'unstarted'))
        self.assertEqual(receipts[-1]['transferRequestSha256'], protocol.digest(self.row()[1]['reservation']))

    def assert_stays_held(self, reason, *, watch=True):
        """Nothing moves, on either side, however many times the worker asks."""
        before, row = self.rows(), self.row()[0]
        for _ in range(3):
            self.assertEqual(self.driver.step(), {'state': 'held', 'reason': reason})
        after = self.rows()
        after['jobs'] = before['jobs'] if watch else after['jobs']
        self.assertEqual(after, before)
        self.assertEqual((self.status(), self.row()[0], self.transport.starts()), (('held', 'coordinator_restart'), row, 0))
        self.assertEqual(self.receipts(), [])
        self.assertEqual((self.row()[1].get('sourceRecovery') or {'attempts': 0})['attempts'], 0)


class PhaseTableTests(ProcessingHarness):
    """One test per table row that reaches an engine. Fetch rows are in fetch recovery."""

    def test_control_no_restart_starts_once(self):
        self.to_execution_pending()
        self.run_to_engine()
        self.assertEqual(self.receipts(), [])

    def test_fetching_pending_with_awaiting_transfer_running_retries(self):
        self.step('awaiting_grant')
        self.step('fetching_pending')
        self.down.add('phase')
        self.assertEqual(self.driver.step()['state'], 'retry_wait')
        self.assertEqual(self.store.get(self.job_id)['phase'], 'awaiting_transfer')
        self.down.clear()
        self.run_to_engine()

    def test_fetching_pending_with_fetching_running_is_idempotent(self):
        self.step('awaiting_grant')
        self.step('fetching_pending')
        self.lose_reply.add('phase')
        self.assertEqual(self.driver.step()['state'], 'retry_wait')
        self.run_to_engine()
        self.assertEqual(self.receipts(), [])

    def test_fetching_pending_with_fetching_held_recovers(self):
        self.step('awaiting_grant')
        self.step('fetching_pending')
        self.lose_reply.add('phase')
        self.driver.step()
        self.restart_coordinator()
        self.step('fetching_pending')
        self.assertEqual(self.receipts()[0]['schemaVersion'], 2)
        self.run_to_engine()

    def test_processing_pending_with_fetching_held_recovers_on_schema_2(self):
        self.to_processing_pending()
        self.restart_coordinator()
        self.step('processing_pending')
        self.assertEqual(self.receipts()[0]['schemaVersion'], 2)
        self.run_to_engine()

    def test_processing_pending_with_processing_running_is_idempotent(self):
        self.lost_processing_reply()
        self.run_to_engine()
        self.assertEqual(self.receipts(), [])

    def test_processing_pending_with_processing_held_recovers_unstarted(self):
        self.lost_processing_reply()
        self.restart_coordinator()
        self.step('processing_pending')
        self.assert_unstarted_receipt()
        self.assertEqual(self.transport.starts(), 0)
        self.run_to_engine()

    def test_execution_pending_no_row_with_processing_held_recovers_unstarted(self):
        self.to_execution_pending()
        self.restart_coordinator()
        self.step('execution_pending')
        self.assert_unstarted_receipt()
        self.assertEqual(self.transport.starts(), 0)
        self.run_to_engine()

    def test_recovery_past_the_lease_still_starts_once(self):
        self.to_execution_pending()
        self.now[0] = fetch_tests.stamp(self.row()[1]['reservation']['expiresAt']) + 3600
        self.restart_coordinator()
        self.step('execution_pending')
        self.run_to_engine()

    def test_worker_ahead_of_a_coordinator_in_fetching_never_starts(self):
        self.to_execution_pending()
        self.execute("UPDATE reference_jobs SET phase='fetching' WHERE job_id=?", self.job_id)
        for _ in range(3):
            self.assertEqual(self.driver.step(), {'state': 'held', 'reason': 'source_execution_ahead_of_coordinator'})
        self.assertEqual(self.transport.starts(), 0)

    def test_held_worker_ahead_of_a_coordinator_in_fetching_is_not_recovered(self):
        self.to_execution_pending()
        self.execute("UPDATE reference_jobs SET phase='fetching' WHERE job_id=?", self.job_id)
        self.restart_coordinator()
        for _ in range(3):
            self.assertEqual(self.driver.step()['state'], 'held')
        self.assertEqual((self.receipts(), self.transport.starts()), ([], 0))
        self.assertEqual(self.status(), ('held', 'coordinator_restart'))

    def test_poll_outlives_a_coordinator_outage(self):
        self.to_execution_pending()
        self.step('executing')
        self.down.update(('claim-state', 'recovery-state', 'recover', 'claim'))
        self.assertEqual(self.driver.step()['state'], 'executing')
        self.assertEqual(self.transport.starts(), 1)


class StartedRecoveryTests(ProcessingHarness):
    """A running engine stays visible across a restart; nothing here can start one."""

    def started(self, *, restart=True):
        self.to_execution_pending()
        self.step('executing')
        if restart:
            self.restart_coordinator()

    def assert_started_receipt(self, count=1):
        receipts = self.receipts()
        self.assertEqual(len(receipts), count)
        self.assertEqual((receipts[-1]['schemaVersion'], receipts[-1]['executionPhase'], receipts[-1]['engineClass']),
            (3, 'processing', 'started'))
        self.assertEqual(self.status(), ('running', None))
        self.assertEqual(self.transport.starts(), 1)

    def assert_polls_on_unrecovered(self):
        """Refused evidence never holds the row or stops the poll."""
        for _ in range(3):
            self.assertEqual(self.driver.step(), {'state': 'executing', 'jobId': self.job_id})
        self.assertEqual((self.receipts(), self.status(), self.transport.starts()), ([], ('held', 'coordinator_restart'), 1))
        self.assertNotIn('hold', self.row()[1])

    def test_a_running_task_returns_to_running_and_starts_nothing(self):
        self.started()
        self.step('executing')
        self.assert_started_receipt()

    def test_second_restart_recovers_again(self):
        self.started()
        self.step('executing')
        self.restart_coordinator()
        self.step('executing')
        self.assert_started_receipt(2)
        self.assertEqual(self.row()[1]['sourceRecovery'], {'attempts': 0, 'pending': None})

    def test_a_worker_death_at_each_point_replays_the_same_intent(self):
        self.started()
        self.down.add('recover')
        self.step('executing')
        request = self.row()[1]['sourceRecovery']['pending']['request']
        self.new_worker()
        self.down.clear()
        self.lose_reply.add('recover')
        self.step('executing')
        self.assertEqual(self.status(), ('running', None))
        self.new_worker()
        self.step('executing')
        self.assert_started_receipt()
        self.assertEqual(self.receipts()[0]['requestId'], request['requestId'])

    def test_a_finished_task_needs_no_recovery_to_reach_delivery(self):
        self.started()
        self.transport.tasks[self.job_id] = 40
        self.assertEqual(self.driver.step()['state'], 'delivering')
        self.assertEqual((self.receipts(), self.transport.starts()), ([], 1))

    def test_no_restart_asks_for_nothing(self):
        self.started(restart=False)
        for _ in range(3):
            self.step('executing')
        self.assertEqual(self.receipts(), [])

    def test_status_20_with_a_commit_pending_row_and_no_output_is_not_started(self):
        self.to_execution_pending()
        self.transport.die_on = 'commit'
        self.driver.step()
        self.transport.die_on = None
        self.new_worker()
        self.step_to_executing()
        self.restart_coordinator()
        self.assert_polls_on_unrecovered()

    def test_commit_pending_with_output_is_started(self):
        self.to_execution_pending()
        self.transport.die_on = 'commit'
        self.driver.step()
        self.transport.die_on = None
        self.new_worker()
        self.step_to_executing()
        self.transport.output = ['synthetic engine line']
        self.restart_coordinator()
        self.step('executing')
        self.assert_started_receipt()

    def step_to_executing(self):
        self.assertEqual(self.driver.worker._row(self.job_id)['phase'], 'commit_pending')
        self.assertEqual(self.row()[0], 'execution_pending')
        self.step('executing')

    def test_changed_gate_receipt_is_not_started(self):
        self.started()
        with self.driver.worker._db() as db:
            db.execute('UPDATE executions SET receipt=?', ('c' * 32,))
        self.assert_polls_on_unrecovered()

    def test_a_prepared_completion_is_not_started(self):
        self.started()
        with self.driver.worker._db() as db:
            receipt = db.execute('SELECT receipt FROM executions').fetchone()[0]
        with NodeGate('engine') as gate:
            gate.prepare_completion(receipt, {'status': 'succeeded', 'result': {'synthetic': True}})
        self.assert_polls_on_unrecovered()

    def test_a_ledger_not_engine_started_is_not_started(self):
        self.started()
        with self.ledger._transaction() as db:
            db.execute("UPDATE reservations SET state='uncertain'")
        self.assert_polls_on_unrecovered()

    def test_changed_row_identity_is_not_started(self):
        self.started()
        row = self.driver.worker._row(self.job_id)
        with self.driver.worker._db() as db:
            db.execute('UPDATE executions SET identity=?', (json.dumps(dict(json.loads(row['identity']), reservation='other')),))
        self.assert_polls_on_unrecovered()

    def test_engine_outage_spends_nothing_and_polling_outlives_it(self):
        self.started()
        with self.proof_sees('unreachable'):
            self.assert_polls_on_unrecovered()
        self.assertNotIn('sourceRecovery', self.row()[1])

    def test_a_missing_task_is_not_started(self):
        self.started()
        with self.proof_sees('missing'):
            self.assert_polls_on_unrecovered()

    def proof_sees(self, observed):
        """Only the proof's read changes: the poll beside it still sees its task."""
        real = self.driver._source_started_proof
        worker = self.driver.worker

        def proof(job_id, data):
            with patch.object(worker, 'task_observation', return_value=(observed, None)):
                return real(job_id, data)
        return patch.object(self.driver, '_source_started_proof', side_effect=proof)

    def test_coordinator_outage_during_visibility_keeps_polling(self):
        self.started()
        self.down.update(('claim-state', 'recovery-state', 'recover'))
        self.assert_polls_on_unrecovered()
        self.down.clear()
        self.step('executing')
        self.assert_started_receipt()

    def test_operator_hold_is_never_cleared(self):
        self.started()
        self.execute("UPDATE jobs SET held_reason='operator_hold' WHERE id=?", self.job_id)
        for _ in range(3):
            self.assertEqual(self.driver.step()['state'], 'executing')
        self.assertEqual(self.receipts(), [])
        self.assertEqual(self.status(), ('held', 'operator_hold'))

    def test_updating_node_stops_visibility_and_not_polling(self):
        self.started()
        self.execute("UPDATE nodes SET state='updating',revision=revision+1 WHERE id=?", 'cluster')
        self.assert_polls_on_unrecovered()

    def test_draining_node_recovers_the_exact_claim(self):
        self.started()
        self.execute("UPDATE nodes SET state='draining',revision=revision+1 WHERE id=?", 'cluster')
        self.step('executing')
        self.assert_started_receipt()


class DeliveryAcrossRestartTests(ProcessingHarness):
    """A job that outlives a coordinator restart still reaches signed acceptance.

    Real delivery receipt, Queue completion, viewer transfer, signed V2 copy
    receipt, worker verification and observation. Only the engine and the Viewer
    signer are synthetic; the coordinator is reopened, not killed.
    """
    signer = SyntheticViewerCopyV2Signer()
    viewer_receipt_keys = signer.trusted_keys

    def real_driver(self):
        driver = super().real_driver()
        self.attach_sender(driver)
        return driver

    def attach_sender(self, driver):
        driver.engine_cleanup = driver.cleanup_executor = None
        driver.sender = ReferenceSender(driver.outbox, 'https://coordinator.invalid', bearer_token=WORKER_TOKEN,
            claim_binding_provider=driver._claim_binding, opener=self.deliver)

    def deliver(self, request, timeout=None):
        return _ControlOpener(self.control).open(request, timeout=timeout)

    def new_worker(self):
        super().new_worker()
        self.attach_sender(self.driver)

    def execute_until(self, stage, limit=8):
        for _ in range(limit):
            if self.row()[0] == stage:
                return
            result = self.driver.step()
            self.assertNotIn(result['state'], ('held',), result)
        self.assertEqual(self.row()[0], stage)

    def accept(self):
        """The Viewer's side: take the transfer, sign the copy, post the acceptance."""
        acceptance = self.store.ensure_viewer_transfer(self.job_id, accepted_at='2026-11-04T00:00:02.000Z')
        transfer = self.store.claim_viewer_transfer(now=self.now[0])
        private = self.store.get(self.job_id, private=True)
        expected = {**private['sourceAcceptanceBinding'],
            'resultManifestSha256': transfer['viewerResultManifest']['resultManifestSha256']}
        envelope = self.signer.envelope(expected, acceptance, transfer['viewerResultManifest'])
        self.store.accept(self.job_id, envelope['acceptance'], viewer_receipt=envelope['viewerReceipt'],
            trusted_viewer_receipt_keys=self.signer.trusted_keys)

    def assert_finished(self):
        self.assertEqual(self.transport.starts(), 1)
        job = self.store.get(self.job_id)
        self.assertEqual((job['status'], job['phase']), ('succeeded', 'completed'))
        self.driver._background()
        self.driver._background()
        self.assertTrue(self.row()[1].get('acceptanceObservationAck'))

    def finished_under_a_hold(self):
        self.to_execution_pending()
        self.step('executing')
        self.restart_coordinator()
        self.transport.tasks[self.job_id] = 40

    def test_control_no_restart_reaches_acceptance(self):
        self.to_execution_pending()
        self.step('executing')
        self.transport.tasks[self.job_id] = 40
        self.execute_until('awaiting_acceptance')
        self.accept()
        self.assert_finished()
        self.assertEqual(self.receipts(), [])

    def test_engine_finishing_under_a_restart_hold_reaches_acceptance(self):
        self.finished_under_a_hold()
        self.assertEqual(self.status(), ('held', 'coordinator_restart'))
        self.execute_until('awaiting_acceptance')
        self.assertEqual(self.status()[0], 'succeeded')
        self.accept()
        self.assert_finished()
        self.assertEqual(self.receipts(), [])

    def test_a_started_recovery_then_a_finish_reaches_acceptance(self):
        self.to_execution_pending()
        self.step('executing')
        self.restart_coordinator()
        self.step('executing')
        self.assertEqual(self.status(), ('running', None))
        self.transport.tasks[self.job_id] = 40
        self.execute_until('awaiting_acceptance')
        self.accept()
        self.assert_finished()
        self.assertEqual(len(self.receipts()), 1)

    def test_restart_between_sealing_and_sending(self):
        self.finished_under_a_hold()
        self.step_delivering()
        self.restart_coordinator()
        self.execute_until('awaiting_acceptance')
        self.accept()
        self.assert_finished()

    def step_delivering(self):
        self.assertEqual(self.driver.step()['state'], 'delivering')
        self.assertEqual(self.row()[0], 'delivering')

    def test_restart_and_worker_death_while_awaiting_acceptance(self):
        self.finished_under_a_hold()
        self.execute_until('awaiting_acceptance')
        self.restart_coordinator()
        self.new_worker()
        self.accept()
        self.restart_coordinator()
        self.assert_finished()


class WorkerDeathTests(ProcessingHarness):
    def recoverable(self):
        self.lost_processing_reply()
        self.restart_coordinator()

    def test_after_the_intent_and_before_its_post(self):
        self.recoverable()
        self.down.add('recover')
        self.assertEqual(self.driver.step()['state'], 'retry_wait')
        request = self.row()[1]['sourceRecovery']['pending']['request']
        self.new_worker()
        self.down.clear()
        self.step('processing_pending')
        self.assertEqual(self.receipts()[0]['requestId'], request['requestId'])
        self.run_to_engine()

    def test_after_the_post_commits_and_before_the_receipt_is_saved(self):
        self.recoverable()
        self.lose_reply.add('recover')
        self.assertEqual(self.driver.step()['state'], 'retry_wait')
        self.assertEqual(self.status(), ('running', None))
        self.new_worker()
        self.run_to_engine()
        self.assert_unstarted_receipt()
        self.assertEqual(self.row()[1]['sourceRecovery'], {'attempts': 0, 'pending': None})

    def test_in_execution_pending_with_the_intent_unresolved_settles_before_any_start(self):
        self.to_execution_pending()
        self.restart_coordinator()
        self.lose_reply.add('recover')
        self.assertEqual(self.driver.step()['state'], 'retry_wait')
        self.new_worker()
        self.step('execution_pending')
        self.assertEqual((self.row()[1]['sourceRecovery'], self.transport.starts()), ({'attempts': 0, 'pending': None}, 0))
        self.run_to_engine()

    def test_after_the_receipt_and_before_the_phase_retry(self):
        self.recoverable()
        self.step('processing_pending')
        self.new_worker()
        self.run_to_engine()

    def test_after_the_phase_retry_commits_and_before_its_row(self):
        self.recoverable()
        self.step('processing_pending')
        self.lose_reply.add('phase')
        self.assertEqual(self.driver.step()['state'], 'retry_wait')
        self.new_worker()
        self.run_to_engine()

    def test_after_execution_pending_and_before_the_claim(self):
        self.recoverable()
        self.step('processing_pending')
        self.step('execution_pending')
        self.new_worker()
        self.run_to_engine()

    def die_inside_start(self, name):
        """A real exception partway through start(), then a fresh worker."""
        self.to_execution_pending()
        self.transport.die_on = name
        self.driver.step()
        self.transport.die_on = None
        self.new_worker()
        return self.driver.worker._row(self.job_id)['phase']

    def assert_never_restarted(self, phase, starts):
        """Restart and step: ambiguous work is neither adopted nor started again."""
        self.restart_coordinator()
        for _ in range(4):
            self.assertNotEqual(self.driver.step().get('state'), 'processing_pending')
        self.assertEqual(self.transport.starts(), starts)
        self.assertEqual(self.driver.worker._row(self.job_id)['phase'], phase)
        self.assertEqual(self.receipts(), [])
        self.assertEqual(self.status(), ('held', 'coordinator_restart'))

    def test_during_initialize_is_never_started_again(self):
        self.assert_never_restarted(self.die_inside_start('initialize'), 1)

    def test_during_upload_is_never_started_again(self):
        self.assert_never_restarted(self.die_inside_start('upload'), 1)

    def test_during_commit_is_never_started_again(self):
        self.assert_never_restarted(self.die_inside_start('commit'), 1)

    def test_after_the_prepared_row_before_the_engine_is_held_with_no_start(self):
        self.to_execution_pending()
        with patch.object(self.ledger, 'set_engine_state', side_effect=OSError('synthetic death')):
            self.driver.step()
        self.new_worker()
        self.assertEqual(self.driver.worker._row(self.job_id)['phase'], 'prepared')
        self.assert_never_restarted('prepared', 0)

    def test_between_gate_and_row_is_an_occupied_gate_and_never_starts(self):
        self.to_execution_pending()
        real = NodeGate.bind

        def bound_then_dead(gate, *args, **kwargs):
            real(gate, *args, **kwargs)
            raise OSError('synthetic death before the execution row')
        with patch.object(NodeGate, 'bind', bound_then_dead):
            self.driver.step()
        self.new_worker()
        self.restart_coordinator()
        self.assert_stays_held('source_recovery_gate_occupied')


class CoordinatorDeathTests(ProcessingHarness):
    def test_before_commit(self):
        self.to_execution_pending()
        self.restart_coordinator()
        self.down.add('recover')
        self.assertEqual(self.driver.step()['state'], 'retry_wait')
        self.restart_coordinator()
        self.down.clear()
        self.step('execution_pending')
        self.assert_unstarted_receipt()
        self.assertEqual(self.row()[1]['sourceRecovery'], {'attempts': 0, 'pending': None})
        self.run_to_engine()

    def test_after_commit_before_reply_then_a_second_restart(self):
        self.to_execution_pending()
        self.restart_coordinator()
        self.lose_reply.add('recover')
        self.assertEqual(self.driver.step()['state'], 'retry_wait')
        self.restart_coordinator()
        self.assertEqual(self.status(), ('held', 'coordinator_restart'))
        self.step('execution_pending')
        self.assert_unstarted_receipt(2)
        self.run_to_engine()

    def test_second_restart_in_processing_pending(self):
        self.lost_processing_reply()
        self.restart_coordinator()
        self.step('processing_pending')
        self.restart_coordinator()
        self.step('processing_pending')
        self.assert_unstarted_receipt(2)
        self.run_to_engine()

    def test_second_restart_in_execution_pending(self):
        self.to_execution_pending()
        self.restart_coordinator()
        self.step('execution_pending')
        self.restart_coordinator()
        self.step('execution_pending')
        self.assert_unstarted_receipt(2)
        self.run_to_engine()

    def test_second_restart_while_the_fetching_phase_reply_was_lost(self):
        self.step('awaiting_grant')
        self.step('fetching_pending')
        self.lose_reply.add('phase')
        self.driver.step()
        self.restart_coordinator()
        self.step('fetching_pending')
        self.restart_coordinator()
        self.step('fetching_pending')
        self.assertEqual(len(self.receipts()), 2)
        self.run_to_engine()

    def test_restart_after_recovery_before_the_claim_read_stays_safe(self):
        self.to_execution_pending()
        self.restart_coordinator()
        self.step('execution_pending')
        self.restart_coordinator()
        self.down.add('claim-state')
        self.assertEqual(self.driver.step()['state'], 'retry_wait')
        self.assertEqual(self.transport.starts(), 0)
        self.down.clear()
        self.step('execution_pending')
        self.run_to_engine()


class EarlyRowTests(ProcessingHarness):
    def test_a_row_where_none_can_be_is_never_recovered_or_started(self):
        self.lost_processing_reply()
        self.restart_coordinator()
        with self.driver.worker._db() as db:
            db.execute("INSERT INTO executions(job,identity,receipt,phase) VALUES(?,'{}','r','prepared')", (self.job_id,))
        self.assert_stays_held('source_recovery_execution_exists')


class AmbiguousEvidenceTests(ProcessingHarness):
    def setUp(self):
        super().setUp()
        self.to_execution_pending()
        self.restart_coordinator()

    def test_control_clean_evidence_recovers(self):
        self.step('execution_pending')
        self.run_to_engine()

    def test_occupied_gate_with_no_row(self):
        with NodeGate('engine') as gate:
            gate.reserve('cluster', GATE_ORIGIN, self.queue.authority_id, 1, 'b' * 32)
        self.assert_stays_held('source_recovery_gate_occupied')

    def test_live_task_with_no_row_status_20_is_no_evidence(self):
        self.transport.tasks[self.job_id] = 20
        self.assert_stays_held('source_recovery_engine_task_present')

    def test_live_task_with_no_row_status_10(self):
        self.transport.tasks[self.job_id] = 10
        self.assert_stays_held('source_recovery_engine_task_present')

    def test_engine_outage_waits_and_spends_nothing(self):
        self.transport.down = True
        for _ in range(3):
            self.assertEqual(self.driver.step()['state'], 'retry_wait')
        self.assertEqual((self.receipts(), self.status()), ([], ('held', 'coordinator_restart')))
        self.assertNotIn('sourceRecovery', self.row()[1])
        self.transport.down = False
        self.step('execution_pending')
        self.run_to_engine()

    def change_ledger(self, statement):
        with self.driver.ledger._transaction() as db:
            db.execute(statement)

    def test_ledger_short_of_its_input_bytes_stays_held(self):
        self.change_ledger('UPDATE reservations SET allocated=allocated-1')
        self.assert_stays_held('source_recovery_ledger_changed')

    def test_ledger_not_reserved_stays_held(self):
        self.change_ledger("UPDATE reservations SET state='uncertain'")
        self.assert_stays_held('source_recovery_ledger_changed')

    def test_a_registered_result_stays_held(self):
        with patch.object(self.driver.ledger, 'result_registered', return_value=True):
            self.assert_stays_held('source_recovery_ledger_changed')

    def test_ledger_short_of_its_input_bytes(self):
        stage, data = self.row()
        data['claim']['submission']['manifest']['totalBytes'] += 1
        self.driver._save(self.job_id, stage, data)
        self.assertEqual(self.driver.step()['state'], 'held')
        self.assertEqual((self.receipts(), self.transport.starts()), ([], 0))

    def test_a_replayed_intent_is_checked_again_and_not_sent(self):
        self.down.add('recover')
        self.assertEqual(self.driver.step()['state'], 'retry_wait')
        self.down.clear()
        self.transport.tasks[self.job_id] = 20
        sent = len(self.requests)
        self.assertEqual(self.driver.step(), {'state': 'held', 'reason': 'source_recovery_engine_task_present'})
        self.assertNotIn(('POST', 'recover'), self.requests[sent:])
        self.assertEqual(self.status(), ('held', 'coordinator_restart'))

    def test_operator_hold_stays_held(self):
        self.execute("UPDATE jobs SET held_reason='operator_hold' WHERE id=?", self.job_id)
        for _ in range(3):
            self.assertEqual(self.driver.step()['state'], 'held')
        self.assertEqual((self.receipts(), self.transport.starts()), ([], 0))

    def test_exact_retries_of_a_refusal_spend_no_source_budget(self):
        spent = self.usage()
        self.transport.tasks[self.job_id] = 20
        for _ in range(3):
            self.driver.step()
        self.assertEqual(self.usage(), spent)

    def test_draining_node_starts_the_already_claimed_job(self):
        self.execute("UPDATE nodes SET state='draining',revision=revision+1 WHERE id=?", 'cluster')
        self.step('execution_pending')
        self.run_to_engine()

    def test_updating_held_or_failed_node_stays_held(self):
        for state in ('updating', 'held', 'failed'):
            self.execute('UPDATE nodes SET state=?,revision=revision+1 WHERE id=?', state, 'cluster')
            self.assertEqual(self.driver.step(), {'state': 'held', 'reason': 'source_recovery_ineligible'})
        self.assertEqual((self.receipts(), self.transport.starts()), ([], 0))

    def test_release_fence_stays_held(self):
        self.store.source_readers().fence(self.job_id, 1, 'recovery-release')
        self.assertEqual(self.driver.step(), {'state': 'held', 'reason': 'source_recovery_ineligible'})
        self.assertEqual((self.receipts(), self.transport.starts()), ([], 0))


class IrreversibleHoldTests(ProcessingHarness):
    def test_a_journaled_hold_is_never_retired_or_recovered(self):
        self.to_fetching()
        self.restart_coordinator()
        stage, data = self.row()
        data['hold'] = dict(holdId=str(fetch_tests.uuid.uuid4()), reasonCode='reservation_expired', reason='reservation_expired',
            sequence=0, observation=None, engineStatusCode=None, ack=None)
        self.driver._save(self.job_id, stage, data)
        self.driver._clear_transfer_watch(self.job_id, stage, self.row()[1])
        for _ in range(3):
            self.assertEqual(self.driver.step()['state'], 'held')
        self.assertEqual(self.row()[1]['hold']['reasonCode'], 'reservation_expired')
        self.assertNotIn('inputPaths', self.row()[1])
        self.assertEqual((self.receipts(), self.status()), ([], ('held', 'coordinator_restart')))


class PreflightTests(ProcessingHarness):
    """A restart before the first byte: same claim, same reservation, one start."""

    def assert_preflight_receipt(self, count=1, grant=False):
        receipts = self.receipts()
        self.assertEqual(len(receipts), count)
        self.assertEqual((receipts[-1]['schemaVersion'], receipts[-1]['executionPhase'], receipts[-1]['engineClass'],
            receipts[-1]['committedRanges'], receipts[-1]['grantSha256'] is not None), (3, 'awaiting_transfer', 'unstarted', 0, grant))
        self.assertEqual(receipts[-1]['transferRequestSha256'], protocol.digest(self.row()[1]['reservation']))

    def lost_reservation_reply(self):
        self.lose_reply.add('reservation')
        self.assertEqual(self.driver.step()['state'], 'retry_wait')
        self.assertEqual((self.row()[0], self.store.get(self.job_id)['phase']), ('reservation_pending', 'awaiting_transfer'))

    def test_reservation_pending_after_a_lost_reply(self):
        self.lost_reservation_reply()
        reservation = self.row()[1]['reservation']
        self.restart_coordinator()
        self.step('reservation_pending')
        self.assert_preflight_receipt()
        self.run_to_engine()
        self.assertEqual(self.row()[1]['reservation'], reservation)

    def test_awaiting_grant(self):
        self.step('awaiting_grant')
        self.restart_coordinator()
        self.step('awaiting_grant')
        self.assert_preflight_receipt()
        self.run_to_engine()

    def test_awaiting_grant_whose_grant_reply_was_lost_replays_the_same_grant(self):
        self.step('awaiting_grant')
        self.lose_reply.add('transfer-grant')
        self.assertEqual(self.driver.step()['state'], 'retry_wait')
        with self.queue._db() as db:
            minted = db.execute('SELECT digest FROM reference_transfer_grants').fetchone()[0]
        self.restart_coordinator()
        self.step('awaiting_grant')
        self.assert_preflight_receipt()
        self.step('fetching_pending')
        self.assertEqual(protocol.digest(self.row()[1]['grant']), minted)
        self.run_to_engine()

    def test_fetching_pending_with_awaiting_transfer_held(self):
        self.step('awaiting_grant')
        self.step('fetching_pending')
        self.restart_coordinator()
        self.step('fetching_pending')
        self.assert_preflight_receipt(grant=True)
        self.run_to_engine()

    def test_second_restart_in_each_stage(self):
        self.lost_reservation_reply()
        self.restart_coordinator()
        self.step('reservation_pending')
        self.restart_coordinator()
        self.step('reservation_pending')
        self.step('awaiting_grant')
        self.restart_coordinator()
        self.step('awaiting_grant')
        self.restart_coordinator()
        self.step('awaiting_grant')
        self.step('fetching_pending')
        self.restart_coordinator()
        self.step('fetching_pending')
        self.restart_coordinator()
        self.step('fetching_pending')
        self.assert_preflight_receipt(6, grant=True)
        self.assertEqual(self.row()[1]['sourceRecovery'], {'attempts': 0, 'pending': None})
        self.run_to_engine()

    def test_worker_death_after_the_intent_and_after_the_commit(self):
        self.step('awaiting_grant')
        self.restart_coordinator()
        self.down.add('recover')
        self.assertEqual(self.driver.step()['state'], 'retry_wait')
        request = self.row()[1]['sourceRecovery']['pending']['request']
        self.assertIsNone(request['grantSha256'])
        self.new_worker()
        self.down.clear()
        self.lose_reply.add('recover')
        self.assertEqual(self.driver.step()['state'], 'retry_wait')
        self.assertEqual(self.status(), ('running', None))
        self.new_worker()
        self.run_to_engine()
        self.assertEqual([item['requestId'] for item in self.receipts()], [request['requestId']])

    def test_no_reservation_stays_held_for_an_operator(self):
        self.down.add('reservation')
        self.assertEqual(self.driver.step()['state'], 'retry_wait')
        self.down.clear()
        self.assertEqual(self.store.get(self.job_id)['phase'], 'queued')
        self.restart_coordinator()
        self.assert_stays_held('source_recovery_claim_unreserved')

    def test_worker_ahead_in_fetching_with_awaiting_transfer_stays_held(self):
        self.to_fetching()
        self.execute("UPDATE reference_jobs SET phase='awaiting_transfer' WHERE job_id=?", self.job_id)
        self.restart_coordinator()
        for _ in range(3):
            self.assertEqual(self.driver.step(), {'state': 'held', 'reason': 'source_fetch_held'})
        self.assertEqual((self.receipts(), self.transport.starts()), ([], 0))

    def test_lapsed_reservation_stays_held(self):
        self.step('awaiting_grant')
        self.step('fetching_pending')
        self.now[0] = fetch_tests.stamp(self.row()[1]['reservation']['expiresAt']) + 1
        self.restart_coordinator()
        self.assertEqual(self.driver.step(), {'state': 'held', 'reason': 'reservation_expired'})
        self.assertEqual((self.receipts(), self.transport.starts()), ([], 0))

    def test_a_fence_before_the_transfer_stays_held(self):
        self.step('awaiting_grant')
        self.driver.ledger.begin_transfer(self.row()[1]['reservation']['reservationId'])
        self.restart_coordinator()
        self.assert_stays_held('source_recovery_ledger_changed')

    def test_input_paths_before_the_transfer_stay_held(self):
        self.step('awaiting_grant')
        stage, data = self.row()
        data['inputPaths'] = {'object': 'jobs/x/object.input'}
        self.driver._save(self.job_id, stage, data)
        self.restart_coordinator()
        self.assert_stays_held('source_recovery_ledger_changed')

    def test_occupied_gate_and_live_task_stay_held(self):
        self.step('awaiting_grant')
        self.restart_coordinator()
        self.transport.tasks[self.job_id] = 20
        self.assert_stays_held('source_recovery_engine_task_present')
        del self.transport.tasks[self.job_id]
        with NodeGate('engine') as gate:
            gate.reserve('cluster', GATE_ORIGIN, self.queue.authority_id, 1, 'b' * 32)
        self.assert_stays_held('source_recovery_gate_occupied')

    def closure(self, **changes):
        with self.queue._db() as db:
            return self.store._source_preflight_closure(db, self.job_id, **changes)

    def test_closure_control_and_each_refusal(self):
        self.step('awaiting_grant')
        self.assertIsNone(self.closure()['grantSha256'])
        with self.assertRaisesRegex(Conflict, 'reservation expired'):
            self.closure(clock=lambda: fetch_tests.stamp(self.row()[1]['reservation']['expiresAt']))
        intake_store._ensure_source_range_schema(self.queue)
        self.assertIsNone(self.closure()['grantSha256'])
        self.execute('INSERT INTO reference_source_range_usage VALUES(?,1,?,1,1,1)', self.job_id, 'r')
        with self.assertRaisesRegex(Conflict, 'already began'):
            self.closure()
        self.execute('DELETE FROM reference_source_range_usage WHERE job_id=?', self.job_id)
        self.store.source_readers().fence(self.job_id, 1, 'preflight-release')
        with self.assertRaises(Conflict):
            self.closure()

    def test_closure_refuses_a_missing_receipt(self):
        self.step('awaiting_grant')
        self.execute('DELETE FROM reference_source_object_receipts WHERE rowid=(SELECT MIN(rowid) FROM reference_source_object_receipts)')
        with self.assertRaisesRegex(Conflict, 'closure changed'):
            self.closure()

    def test_refused_closure_is_ineligible_and_held(self):
        self.step('awaiting_grant')
        self.restart_coordinator()
        self.execute('DELETE FROM reference_source_object_receipts WHERE rowid=(SELECT MIN(rowid) FROM reference_source_object_receipts)')
        for _ in range(2):
            self.assertEqual(self.driver.step(), {'state': 'held', 'reason': 'source_recovery_ineligible'})
        self.assertEqual((self.receipts(), self.transport.starts()), ([], 0))


class ProcessingWireTests(fetch_tests.RecoveryHarness):
    def complete(self, count=None):
        first = fetch_tests.validate_source_grant(self.first_grant, job=self.claim, clock=lambda: self.now[0])
        for index in range(len(first.objects) if count is None else count):
            self.fetch(first, index)
        self.store.update_phase(self.job_id, 'processing')
        self.restart_coordinator()

    def unstarted(self, **changes):
        return self.body(**{'schemaVersion': 3, 'executionPhase': 'processing', 'engineClass': 'unstarted',
            'committedRanges': self.state()['sourceTransfer']['requestsAdmitted'],
            'committedBytes': self.state()['sourceTransfer']['bytesAdmitted'], **changes})

    def assert_refused(self, body):
        before = self.rows()
        with self.assertRaises((Conflict, ValueError)):
            self.resume(body)
        self.assertEqual(self.rows(), before)
        self.assertEqual(self.status(), ('held', 'coordinator_restart'))

    def test_control_complete_fetch_is_eligible_on_schema_3(self):
        self.complete()
        state = self.state()
        self.assertEqual((state['schemaVersion'], state['eligible']), (3, True))
        self.assertEqual(self.resume(self.unstarted())['engineClass'], 'unstarted')
        self.assertEqual(self.status(), ('running', None))

    def test_incomplete_fetch_is_not_eligible(self):
        self.complete(1)
        self.assertFalse(self.state()['eligible'])
        self.assertIsNone(self.state()['sourceTransfer'])

    def test_a_retried_range_adds_no_row_so_retries_cannot_fake_completeness(self):
        first = fetch_tests.validate_source_grant(self.first_grant, job=self.claim, clock=lambda: self.now[0])
        for _ in range(3):
            self.read_range(first.token, first)
        requests, size, attempts = self.usage()
        self.assertEqual((requests, size, attempts), (1, first.objects[0].byte_size, 3))
        self.store.update_phase(self.job_id, 'processing')
        self.restart_coordinator()
        self.assertFalse(self.state()['eligible'])

    def test_every_range_served_reaches_both_limits_exactly(self):
        self.complete()
        source = self.state()['sourceTransfer']
        self.assertEqual(source['requestsAdmitted'], self.claim['transferRequest']['requestBudget'])
        self.assertEqual(source['bytesAdmitted'], self.claim['submission']['manifest']['totalBytes'])

    def test_schema_2_is_refused_in_processing(self):
        self.complete()
        body = self.unstarted()
        for key in ('executionPhase', 'engineClass'):
            body.pop(key)
        self.assert_refused(dict(body, schemaVersion=2))

    def test_schema_3_is_refused_in_fetching(self):
        self.restart_coordinator()
        self.assert_refused(self.body(schemaVersion=3, executionPhase='processing', engineClass='unstarted'))

    def test_a_null_grant_is_refused_outside_awaiting_transfer(self):
        self.complete()
        self.assert_refused(self.unstarted(grantSha256=None))

    def test_control_started_is_accepted_in_processing(self):
        self.complete()
        self.assertEqual(self.resume(self.unstarted(engineClass='started'))['engineClass'], 'started')

    def test_started_is_refused_before_the_transfer(self):
        with self.assertRaises(ValueError):
            ClaimRecoveryService._request(dict(self.unstarted_shape(), executionPhase='awaiting_transfer', engineClass='started'))

    def test_an_unknown_class_is_refused(self):
        self.complete()
        self.assert_refused(self.unstarted(engineClass='adopted'))

    def unstarted_shape(self):
        self.complete()
        return self.unstarted()

    def test_a_worker_short_of_the_served_ranges_is_refused(self):
        self.complete()
        self.assert_refused(self.unstarted(committedRanges=0, committedBytes=0))

    def test_a_treedar_job_is_not_eligible(self):
        self.complete()
        submission = self.store.get(self.job_id, private=True)['submission']
        row = {'input_json': protocol.canonical_json(dict(submission, pipeline=dict(submission['pipeline'],
            treedar=dict(submission['pipeline']['treedar'], requested=True)))).decode(),
            'transfer_json': protocol.canonical_json(self.claim['transferRequest']).decode()}
        source = self.state()['sourceTransfer']
        self.assertFalse(ClaimRecoveryService._source_fetch_complete(row, source))

    def test_a_failed_terminal_or_result_keeps_it_ineligible(self):
        self.complete()
        self.execute("UPDATE reference_jobs SET error='synthetic' WHERE job_id=?", self.job_id)
        self.assertFalse(self.state()['eligible'])

    def test_schema_2_wire_is_unchanged(self):
        # Digests pinned from 42f1163, before schema 3 existed.
        claim = dict(jobId='00000000-0000-4000-8000-000000000001', generation=1, environment='staging', nodeId='cluster',
            workerId='worker', workerInstanceId='instance', authorityId='00000000-0000-4000-8000-000000000002',
            claimRequestId='claim-1', claimTokenSha256='a' * 64, manifestSha256='b' * 64, pipelineDigest='c' * 64,
            reservationId='reservation-1', inputBytes=3)
        checked = ClaimRecoveryService._request(dict(schemaVersion=2, requestId='00000000-0000-4000-8000-000000000003',
            coordinatorEpoch='00000000-0000-4000-8000-000000000004', expectedNodeRevision=0, claimBinding=claim,
            transferRequestSha256='d' * 64, sourceClosureSha256='e' * 64, grantSha256='f' * 64, renewalSequence=0,
            committedRanges=1, committedBytes=3, workerEvidenceSha256='1' * 64))
        self.assertEqual(protocol.digest(checked), '79a62764a2efef0d804d08253693f7512bde376ce1d1dc1c5074f5ac67ed22ae')
        self.assertEqual(protocol.digest(ClaimRecoveryService._make_receipt(checked)), 'f548fcf9c0a996d8fe962be9255cd2c55c3cbbd536cd9367ae7a4d92dba09607')

    def test_recovery_adds_no_table(self):
        self.complete()
        with self.queue._db() as db:
            before = sorted(row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'"))
        self.resume(self.unstarted())
        with self.queue._db() as db:
            self.assertEqual(sorted(row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")), before)


if __name__ == '__main__':
    import unittest
    unittest.main()
