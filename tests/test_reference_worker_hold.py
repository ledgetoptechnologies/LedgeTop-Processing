"""Held work reaches the coordinator; real driver, worker, store and worker API."""
from contextlib import closing
import copy
import json
import sqlite3
import time
import unittest
import uuid

from processing import reference_driver as driver_module
from processing.node_gate import NodeGate
from processing.prototype import Queue
from processing.reference_control import ReferenceControlAPI
from processing.reference_driver import DriverWait
from processing.reference_engine_worker import WorkerHeld
from processing.reference_store import ReferenceStore
from processing.reference_worker_api import ReferenceWorkerAPI
from processing.viewer_service import EngineUnavailable
from tests import test_reference_driver as fixture

TOKEN, VIEWER = fixture.TOKEN, fixture.VIEWER

# getattr keeps this module loadable on commits that predate the constants, so
# each behaviour fails by itself there rather than as one import error.
MISSING_READS = getattr(driver_module, 'ENGINE_TASK_MISSING_READS', 10)
SUSPECT_SECONDS = getattr(driver_module, 'ENGINE_SUSPECT_SECONDS', 1800)
SCRATCH_SECONDS = getattr(driver_module, 'SCRATCH_SHORTAGE_SECONDS', 900)
OTHER = 'synthetic-other-worker-token'


class WorkerHoldTests(unittest.TestCase):
    tearDown = fixture.ReferenceDriverTests.tearDown
    make_fixture_driver = fixture.ReferenceDriverTests.make_driver
    sender_open = fixture.ReferenceDriverTests.sender_open
    download = fixture.ReferenceDriverTests.download
    submit = fixture.ReferenceDriverTests.submit
    supply_grants = fixture.ReferenceDriverTests.supply_grants
    advance = fixture.ReferenceDriverTests.advance

    def setUp(self):
        self.now = [time.time()]
        self.posts, self.lose_replies = [], 0
        fixture.ReferenceDriverTests.setUp(self)

    def make_driver(self, **kwargs):
        return self.make_fixture_driver(clock=lambda: self.now[0], **kwargs)

    def request(self, method, suffix, payload=None, *, claim_token=None):
        result = fixture.ReferenceDriverTests.request(self, method, suffix, payload, claim_token=claim_token)
        if suffix.endswith('/hold'):
            self.posts.append(copy.deepcopy(payload))
            if self.lose_replies:
                self.lose_replies -= 1
                raise DriverWait('coordinator_unavailable')
        return result

    def public(self, job):
        response = self.api.handle('GET', '/ltds/v1/jobs/' + job['jobId'], {'Authorization': 'Bearer ' + VIEWER},
            protected_transport=True)
        self.assertEqual(response.status, 200)
        return response.body

    def assert_public_held(self, job, code):
        record = self.public(job)
        self.assertEqual((record['status'], record['phase'], record['error']), ('held', 'held', code))
        self.assertEqual(record['queueReason'], code)
        # Held keeps the slot and never exposes a queue entry to another worker.
        self.assertIsNone(self.store.next_for_node('cluster'))
        with self.queue._db() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM jobs WHERE status='queued' AND id=?", (job['jobId'],)).fetchone()[0], 0)

    def assert_observing(self, job, code, observation=None):
        """Visible, but the job is exactly as it was: no hold, no error, retry intact."""
        record = self.public(job)
        self.assertEqual((record['status'], record['error'], record['queueReason']), ('running', None, code))
        self.assertEqual(record['holdObservation'], observation)
        with self.queue._db() as db:
            self.assertIsNone(db.execute('SELECT held_reason FROM jobs WHERE id=?', (job['jobId'],)).fetchone()[0])
        self.assertNotIn('hold', self.row(job)[1])

    def row(self, job):
        with self.driver._db() as db:
            row = db.execute('SELECT stage,data FROM driver_jobs WHERE job=?', (job['jobId'],)).fetchone()
        return None if row is None else (row['stage'], json.loads(row['data']))

    def steps(self, count):
        return [self.driver.step() for _ in range(count)]

    def engine_down(self):
        self.native.read.side_effect = EngineUnavailable('engine_read_unavailable')

    def engine_up(self):
        self.native.read.side_effect = self.transport.read

    def executing(self, *, code=20):
        self.transport.code = code
        job = self.submit()
        self.advance(job, 'executing')
        return job

    def fetching(self):
        job = self.submit()
        self.advance(job, 'fetching')
        return job

    def expire_reservation(self):
        with self.ledger._transaction() as db:
            db.execute('UPDATE reservations SET expires=1')

    def test_uncertain_engine_init_is_public_held_and_never_replayed(self):
        job = self.submit()
        self.transport.fail_initialize = True
        self.advance(job, 'execution_pending')
        self.steps(3)
        self.assert_public_held(job, 'engine_init_uncertain')
        self.assertEqual(sum(call[0] == 'initialize' for call in self.transport.calls), 1)

    def test_lost_task_is_observed_after_bounded_reads_never_held_and_a_good_read_resets(self):
        job = self.executing()
        saved = self.transport.tasks.pop(job['jobId'])
        results = self.steps(MISSING_READS - 1)
        self.assertEqual({item['state'] for item in results}, {'retry_wait'})
        self.assertNotIn('queueReason', self.public(job))
        self.transport.tasks[job['jobId']] = saved
        self.assertEqual(self.driver.step()['state'], 'executing')
        self.assertNotIn('suspect', self.row(job)[1])
        del self.transport.tasks[job['jobId']]
        self.steps(MISSING_READS - 1)
        self.assertNotIn('queueReason', self.public(job))
        self.assertEqual(self.driver.step()['state'], 'retry_wait')
        self.assert_observing(job, 'engine_task_missing', 'engine_task_missing')
        self.steps(MISSING_READS)
        self.now[0] += SUSPECT_SECONDS
        self.assertEqual(self.driver.step()['state'], 'retry_wait')
        self.assert_observing(job, 'engine_task_missing', 'engine_task_missing')
        self.transport.tasks[job['jobId']] = saved
        self.assertEqual(self.driver.step()['state'], 'executing')
        self.assertNotIn('queueReason', self.public(job))
        self.assertNotIn('watch', self.row(job)[1])

    def engine_replies(self, info, tasks):
        def read(route, **kwargs):
            value = tasks if route == '/task/list' else info
            if isinstance(value, Exception):
                raise value
            return value
        self.native.read.side_effect = read

    def test_absence_needs_a_not_found_read_and_a_well_formed_task_list_census(self):
        job = self.executing()
        identifier, down = job['jobId'], EngineUnavailable('engine_read_unavailable')
        for info in (None, down):
            self.engine_replies(info, [{'uuid': str(uuid.uuid4())}, ])
            self.assertEqual(self.worker.task_observation(identifier), ('missing', None))
            self.engine_replies(info, [])
            self.assertEqual(self.worker.task_observation(identifier), ('missing', None))
            for tasks in (down, {'tasks': []}, [{'uuid': identifier}], [{'id': identifier}], ['text'], None):
                self.engine_replies(info, tasks)
                self.assertEqual(self.worker.task_observation(identifier), ('unreachable', None), tasks)

    def test_malformed_200_is_neither_present_nor_absent_and_keeps_the_suspicion(self):
        job = self.executing()
        identifier = job['jobId']
        self.driver.step()
        for info in ({'uuid': identifier, 'status': {}}, {'uuid': identifier, 'status': {'code': 99}},
                     {'uuid': identifier, 'status': {'code': '40'}}, {'uuid': identifier, 'status': {'code': True}},
                     {'uuid': identifier, 'status': 40}, {'uuid': str(uuid.uuid4()), 'status': {'code': 40}}, {}, [], 'text'):
            self.engine_replies(info, [])
            self.assertEqual(self.worker.task_observation(identifier), ('unreachable', None), info)
            self.assertEqual(self.driver.step()['state'], 'retry_wait')
        self.assertEqual(self.row(job)[1]['suspect']['count'], 9)
        self.engine_replies({'uuid': identifier, 'status': {'code': 20}}, [])
        self.assertEqual(self.worker.task_observation(identifier)[0], 'present')

    def test_lost_node_is_observed_after_elapsed_time_not_on_short_outage(self):
        job = self.executing()
        self.engine_down()
        for _ in range(MISSING_READS + 2):
            self.assertEqual(self.driver.step()['state'], 'retry_wait')
        self.now[0] += SUSPECT_SECONDS - 1
        self.assertEqual(self.driver.step()['state'], 'retry_wait')
        self.assertNotIn('queueReason', self.public(job))
        self.engine_up()
        self.assertEqual(self.driver.step()['state'], 'executing')
        self.engine_down()
        self.driver.step()
        self.now[0] += SUSPECT_SECONDS - 1
        self.assertEqual(self.driver.step()['state'], 'retry_wait')
        self.now[0] += 1
        self.assertEqual(self.driver.step()['state'], 'retry_wait')
        self.assert_observing(job, 'node_lost', 'engine_unreachable')

    def long_outage(self, job):
        self.engine_down()
        self.driver.step()
        self.now[0] += SUSPECT_SECONDS
        self.driver.step()

    def assert_recovered(self, job):
        """Exact bound task finished: ordinary poll and outbox, nothing re-run."""
        self.assertEqual(self.row(job)[0], 'awaiting_acceptance')
        self.assertEqual(self.outbox_rows(), 1)
        self.assertNotIn('hold', self.row(job)[1])
        self.assertEqual(sum(call[0] == 'initialize' for call in self.transport.calls), 1)
        self.assertEqual(self.store.get(job['jobId'], private=True)['status'], 'succeeded')
        self.assertIsNotNone(self.public(job)['resultManifestSha256'])
        self.assertNotIn('queueReason', self.public(job))

    def test_task_finishing_during_a_long_outage_delivers_through_the_ordinary_outbox(self):
        job = self.executing()
        self.long_outage(job)
        self.assert_observing(job, 'node_lost', 'engine_unreachable')
        self.engine_up()
        self.transport.tasks[job['jobId']] = 40
        self.advance(job, 'awaiting_acceptance')
        self.assert_recovered(job)

    def test_outage_over_a_coordinator_restart_delivers_under_the_restart_authority(self):
        job = self.executing()
        self.restart_coordinator()
        self.long_outage(job)
        with self.queue._db() as db:
            held = db.execute('SELECT held_reason FROM jobs').fetchone()[0]
            error = db.execute('SELECT error FROM reference_jobs').fetchone()[0]
        self.assertEqual((held, error), ('coordinator_restart', None))
        record = self.public(job)
        self.assertEqual((record['status'], record['error'], record['queueReason']), ('held', 'coordinator_restart', 'node_lost'))
        self.engine_up()
        self.transport.tasks[job['jobId']] = 40
        self.advance(job, 'awaiting_acceptance')
        self.assert_recovered(job)

    def test_reappearing_task_with_another_identity_is_never_adopted(self):
        job = self.executing()
        self.long_outage(job)
        self.transport.tasks[job['jobId']] = 40
        def other(route, **kwargs):
            if route.startswith('/task/') and route != '/task/list':
                return {'uuid': str(uuid.uuid4()), 'status': {'code': 40}}
            return self.transport.read(route, **kwargs)
        self.native.read.side_effect = other
        self.assertEqual({item['state'] for item in self.steps(3)}, {'retry_wait'})
        self.assert_observing(job, 'node_lost', 'engine_unreachable')
        self.assertEqual(self.outbox_rows(), 0)
        self.assertFalse(any(call[0] == 'download' for call in self.transport.calls))

    def test_ambiguous_init_is_never_retried_or_adopted_when_a_finished_task_appears(self):
        job = self.submit()
        self.transport.fail_initialize = True
        self.advance(job, 'execution_pending')
        self.steps(3)
        self.assert_public_held(job, 'engine_init_uncertain')
        self.transport.fail_initialize = False
        self.transport.calls.clear()
        self.transport.tasks[job['jobId']] = 40
        results = self.steps(3)
        self.assertEqual((results[-1]['state'], results[-1]['observation'], results[-1]['engineStatusCode']),
            ('held', 'engine_task_present', 40))
        self.assertEqual({call[0] for call in self.transport.calls}, {'read'})
        self.assertEqual(self.outbox_rows(), 0)
        self.assertEqual(self.worker._row(job['jobId'])['phase'], 'initialize_pending')
        self.assert_public_held(job, 'engine_init_uncertain')

    def test_refused_start_is_observed_and_retried_as_before(self):
        job = self.submit()
        self.advance(job, 'execution_pending')
        real = self.worker.start
        def refuse(*args, **kwargs):
            raise WorkerHeld('job metadata integrity changed')
        self.worker.start = refuse
        self.assertEqual(self.driver.step()['state'], 'retry_wait')
        self.assert_observing(job, 'engine_start_refused')
        self.worker.start = real
        self.advance(job, 'executing')
        self.driver.step()
        self.assertNotIn('queueReason', self.public(job))

    def test_exhausted_fetch_budget_stays_retryable_and_visible(self):
        job = self.fetching()
        with self.driver._db() as db:
            row = json.loads(db.execute('SELECT data FROM driver_jobs').fetchone()[0])
            row['fetchAttempts'] = {obj['id']: 3 for obj in row['claim']['submission']['manifest']['objects']}
            db.execute('UPDATE driver_jobs SET data=?', (json.dumps(row),))
        result = self.driver.step()
        self.assertEqual((result['state'], result['observing']), ('held', 'source_transfer_unavailable'))
        self.assert_observing(job, 'source_transfer_unavailable')

    def test_refusal_message_without_a_lapsed_ledger_lease_is_not_a_hold(self):
        job = self.fetching()
        real = self.ledger.prepare_job_directory
        def refuse(*args):
            raise driver_module.PolicyConflict('allocation outside reservation')
        self.ledger.prepare_job_directory = refuse
        self.assertEqual(self.driver.step()['reason'], 'worker_reconciliation_required')
        self.assert_observing(job, 'worker_reconciliation_required')
        self.ledger.prepare_job_directory = real
        self.steps(3)
        self.assertNotIn('queueReason', self.public(job))

    def outbox_rows(self):
        with closing(sqlite3.connect(self.root / 'outbox.sqlite')) as db:
            return db.execute('SELECT COUNT(*) FROM completions').fetchone()[0]

    def test_declared_over_budget_archive_is_held_with_the_result_retained_on_the_node(self):
        job = self.executing(code=40)
        self.assert_over_budget(job, b'x' * (2 * 1024 * 1024))

    def test_streamed_over_budget_archive_is_held_not_retried_forever(self):
        job = self.executing(code=40)
        original = self.transport.download
        def undeclared(identifier):
            response = original(identifier)
            response.headers = {}
            return response
        self.native.download.side_effect = undeclared
        self.assert_over_budget(job, b'x' * (2 * 1024 * 1024))

    def assert_over_budget(self, job, archive):
        self.transport.archive = archive
        self.steps(3)
        self.assert_public_held(job, 'result_over_budget')
        self.assertEqual(self.outbox_rows(), 0)
        self.assertIn(job['jobId'], self.transport.tasks)
        self.assertFalse(any(call[0] in ('remove', 'cancel') for call in self.transport.calls))
        self.assertEqual(self.row(job)[0], 'executing')

    def test_job_cancelled_after_next_is_dropped_and_the_next_job_runs(self):
        first, second = self.submit(), self.submit()
        self.assertEqual(self.driver.step()['state'], 'claim_pending')
        self.store.update_phase(first['jobId'], 'cancelled')
        self.assertEqual(self.driver.step()['state'], 'claim_dropped')
        self.assertIsNone(self.row(first))
        self.advance(second, 'claimed')
        self.assertEqual(self.public(first)['status'], 'cancelled')
        self.assertEqual(self.public(second)['status'], 'running')

    def test_job_claimed_elsewhere_after_next_is_dropped_without_touching_that_claim(self):
        first = self.submit()
        self.assertEqual(self.driver.step()['state'], 'claim_pending')
        claim = self.store.claim(first['jobId'], node_id='cluster', worker_id='worker',
            worker_instance_id='instance', claim_request_id='other-request')
        self.assertEqual(self.driver.step()['state'], 'claim_dropped')
        self.assertEqual(self.store.get(first['jobId'], private=True)['claimToken'], claim['claimToken'])

    def test_lost_claim_reply_for_a_since_terminal_job_is_dropped(self):
        first, second = self.submit(), self.submit()
        self.assertEqual(self.driver.step()['state'], 'claim_pending')
        with self.driver._db() as db:
            request_id = db.execute('SELECT claim_request FROM driver_jobs').fetchone()[0]
        self.store.claim(first['jobId'], node_id='cluster', worker_id='worker',
            worker_instance_id='instance', claim_request_id=request_id)
        self.queue.operator(first['jobId'], 'hold', dict(note='synthetic'))
        self.queue.operator(first['jobId'], 'resolve', dict(note='synthetic', workerStopped=True))
        self.assertEqual(self.driver.step()['state'], 'claim_dropped')
        self.advance(second, 'claimed')

    def test_queued_job_that_is_briefly_unclaimable_keeps_its_row(self):
        first = self.submit()
        self.assertEqual(self.driver.step()['state'], 'claim_pending')
        self.store.update_fleet_snapshot({'cluster': dict(ready=False, taskQueueCount=0)})
        self.assertEqual(self.driver.step()['state'], 'claim_pending')
        self.assertEqual(self.row(first)[0], 'claim_pending')

    def test_scratch_shortage_waits_bounded_then_shows_and_a_pass_clears(self):
        self.ledger.probe = lambda root: 0
        job = self.submit()
        results = self.steps(4)
        self.assertEqual(results[-1], {'state': 'retry_wait', 'jobId': job['jobId'], 'reason': 'scratch_capacity_unavailable'})
        self.assertEqual(self.public(job)['status'], 'running')
        self.now[0] += SCRATCH_SECONDS
        self.assertEqual(self.driver.step()['state'], 'retry_wait')
        self.assert_observing(job, 'scratch_capacity_unavailable')
        self.ledger.probe = lambda root: 10**9
        self.advance(job, 'awaiting_grant')
        self.assertNotIn('queueReason', self.public(job))

    def test_scratch_recovering_inside_the_bound_continues_unreported(self):
        self.ledger.probe = lambda root: 0
        job = self.submit()
        self.steps(4)
        self.ledger.probe = lambda root: 10**9
        self.advance(job, 'awaiting_grant')
        self.assertNotIn('suspect', self.row(job)[1])
        self.assertEqual(self.public(job)['status'], 'running')

    def test_reservation_expiry_mid_first_object_is_public_held(self):
        job = self.fetching()
        self.expire_reservation()
        self.steps(2)
        self.assert_public_held(job, 'reservation_expired')

    def test_expired_lease_is_refused_before_the_gate_so_nothing_is_stranded(self):
        job = self.submit()
        self.advance(job, 'execution_pending')
        self.expire_reservation()
        self.steps(2)
        self.assert_public_held(job, 'reservation_expired')
        with NodeGate('engine') as gate:
            self.assertIsNone(gate.snapshot()['reservation'])
        with self.worker._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM executions').fetchone()[0], 0)
        self.assertFalse(any(call[0] == 'initialize' for call in self.transport.calls))

    def test_lost_reply_then_restart_replays_one_transition_and_one_ack(self):
        with self.queue._db() as db:
            db.execute('CREATE TABLE hold_audit (status TEXT)')
            db.execute("CREATE TRIGGER hold_audit_jobs AFTER UPDATE ON jobs WHEN NEW.status='held' "
                       'BEGIN INSERT INTO hold_audit VALUES(NEW.status); END')
        job = self.fetching()
        self.expire_reservation()
        self.lose_replies = 1
        self.assertEqual(self.driver.step()['state'], 'held')
        self.assertIsNone(self.row(job)[1]['hold']['ack'])
        self.assert_public_held(job, 'reservation_expired')
        self.driver = self.make_driver()
        self.assertEqual(self.driver.step()['holdReport'], 'acknowledged')
        self.assertEqual(self.driver.step()['holdReport'], 'acknowledged')
        self.assertEqual(len(self.posts), 2)
        self.assertEqual(self.posts[0], self.posts[1])
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM hold_audit').fetchone()[0], 1)

    def test_report_journaled_before_contact_survives_an_outage_and_restart(self):
        job = self.fetching()
        self.expire_reservation()
        real, self.driver._request = self.driver._request, lambda *args, **kwargs: (_ for _ in ()).throw(DriverWait('coordinator_unavailable'))
        self.assertEqual(self.driver.step()['state'], 'held')
        self.assertEqual(self.driver.step()['holdReport'], 'pending')
        self.assertEqual(self.public(job)['status'], 'running')
        hold_id = self.row(job)[1]['hold']['holdId']
        self.driver = self.make_driver()
        self.assertEqual(self.driver.step()['holdReport'], 'acknowledged')
        self.assertEqual(self.row(job)[1]['hold']['holdId'], hold_id)
        self.assert_public_held(job, 'reservation_expired')

    def held_request(self, job):
        self.driver.step()
        return copy.deepcopy(self.posts[-1])

    def post_hold(self, job, request, *, token=None, bearer=TOKEN, api=None):
        claim = self.store.get(job['jobId'], private=True)['claimToken']
        return (api or self.api).handle('POST', '/ltds/worker/v1/jobs/' + job['jobId'] + '/hold',
            {'Authorization': 'Bearer ' + bearer, 'X-Claim-Token': token or claim},
            json.dumps(request).encode(), protected_transport=True)

    def test_stale_claim_wrong_generation_and_wrong_worker_are_refused(self):
        job = self.fetching()
        self.expire_reservation()
        request = self.held_request(job)
        before = self.public(job)
        self.assertEqual(self.post_hold(job, request, token='synthetic-stale-claim-token').status, 409)
        self.assertEqual(self.post_hold(job, dict(request, generation=request['generation'] + 1)).status, 409)
        self.assertEqual(self.post_hold(job, dict(request, claimRequestId='driver-stale')).status, 409)
        self.assertEqual(self.post_hold(job, dict(request, holdId=str(uuid.uuid4()))).status, 409)
        for reason in ('node_lost', 'engine_init_uncertain', None):
            self.assertEqual(self.post_hold(job, dict(request, sequence=request['sequence'] + 1,
                reasonCode=reason)).status, 409)
        other = ReferenceWorkerAPI(self.store, None, bearer_workers={
            OTHER: dict(node_id='cluster', worker_id='other', worker_instance_id='other-instance')})
        self.assertEqual(self.post_hold(job, request, bearer=OTHER, api=other).status, 409)
        self.assertEqual(self.public(job), before)

    def test_wrong_node_is_refused(self):
        self.queue.close()
        self.queue = Queue(self.root / 'queue', reference_enabled=True, node_ids=('cluster', 'main'))
        self.store = ReferenceStore(self.queue)
        self.api = self.rebuild_api()
        job = self.fetching()
        self.expire_reservation()
        request = self.held_request(job)
        other = ReferenceWorkerAPI(self.store, None, bearer_workers={
            OTHER: dict(node_id='main', worker_id='worker', worker_instance_id='instance')})
        self.assertEqual(self.post_hold(job, request, bearer=OTHER, api=other).status, 409)

    def test_unknown_code_free_text_and_unrequested_fields_are_rejected_before_any_write(self):
        job = self.fetching()
        self.expire_reservation()
        request = self.held_request(job)
        for change in (dict(reasonCode='scratch full at /private/path'), dict(reasonCode='not_an_allowed_code'),
                       dict(observation='/private/path'), dict(engineStatusCode=99), dict(note='free text')):
            self.assertEqual(self.post_hold(job, dict(request, **change)).status, 400)

    def test_operator_hold_is_not_overwritten_and_the_worker_stays_held(self):
        job = self.fetching()
        self.queue.operator(job['jobId'], 'hold', dict(note='synthetic'))
        self.expire_reservation()
        self.driver.step()
        self.assertEqual(self.driver.step()['holdReport'], 'refused')
        with self.queue._db() as db:
            self.assertEqual(tuple(db.execute('SELECT held_reason,json_extract(result,"$.operatorAction") FROM jobs').fetchone()),
                ('operator', 'hold'))
            self.assertIsNone(db.execute('SELECT error FROM reference_jobs').fetchone()[0])
        self.assertEqual(self.public(job)['status'], 'held')

    def test_observation_never_touches_an_operator_hold(self):
        job = self.executing()
        self.queue.operator(job['jobId'], 'hold', dict(note='synthetic'))
        self.long_outage(job)
        with self.queue._db() as db:
            self.assertEqual(tuple(db.execute('SELECT status,held_reason FROM jobs').fetchone()), ('held', 'operator'))
            self.assertIsNone(db.execute('SELECT error FROM reference_jobs').fetchone()[0])
        self.assertNotIn('queueReason', self.public(job))

    def test_delayed_older_report_cannot_replace_a_newer_one(self):
        job = self.executing()
        self.long_outage(job)
        self.engine_up()
        del self.transport.tasks[job['jobId']]
        self.steps(MISSING_READS)
        self.assert_observing(job, 'engine_task_missing', 'engine_task_missing')
        first, second = self.posts[0], self.posts[-1]
        self.assertGreater(second['sequence'], first['sequence'])
        self.assertEqual(self.post_hold(job, first).status, 409)
        self.assertEqual(self.post_hold(job, second).status, 200)
        self.assert_observing(job, 'engine_task_missing', 'engine_task_missing')

    def restart_coordinator(self):
        self.queue.close()
        self.queue = Queue(self.root / 'queue', reference_enabled=True)
        self.store = ReferenceStore(self.queue)
        self.api = self.rebuild_api()

    def rebuild_api(self):
        return ReferenceControlAPI(self.store, viewer_bearers={VIEWER: 'staging'},
            worker_bearers={TOKEN: dict(node_id='cluster', worker_id='worker', worker_instance_id='instance')},
            capability_fingerprint=self.fixture['capabilities']['capabilityFingerprint'],
            options_fingerprint=self.fixture['capabilities']['engines']['odm']['optionsFingerprint'])

    def test_hold_survives_restart_and_a_report_never_replaces_a_restart_hold(self):
        job = self.fetching()
        self.restart_coordinator()
        self.assertEqual(self.public(job)['error'], 'coordinator_restart')
        self.expire_reservation()
        self.steps(2)
        with self.queue._db() as db:
            self.assertEqual(tuple(db.execute('SELECT status,held_reason FROM jobs').fetchone()), ('held', 'coordinator_restart'))
            self.assertIsNone(db.execute('SELECT error FROM reference_jobs').fetchone()[0])
        record = self.public(job)
        self.assertEqual((record['error'], record['queueReason']), ('coordinator_restart', 'reservation_expired'))
        self.restart_coordinator()
        self.assertEqual(self.public(job)['queueReason'], 'reservation_expired')

    def test_escalated_hold_survives_coordinator_restart(self):
        job = self.fetching()
        self.expire_reservation()
        self.steps(2)
        self.assert_public_held(job, 'reservation_expired')
        self.restart_coordinator()
        self.assert_public_held(job, 'reservation_expired')

    def uncertain_init(self):
        job = self.submit()
        self.transport.fail_initialize = True
        self.advance(job, 'execution_pending')
        self.steps(3)
        self.assert_public_held(job, 'engine_init_uncertain')
        return job

    def assert_fence_retained(self, job):
        result = self.driver.step()
        self.assertEqual((result['state'], result['reconciliation'], result['terminalStatus'], result['holdReport']),
            ('held', 'physical_reservation_retained', 'failed', 'acknowledged'))
        record = self.public(job)
        self.assertEqual((record['status'], record['queueReason']), ('failed', 'physical_reservation_retained'))
        with NodeGate('engine') as gate:
            self.assertEqual(gate.snapshot()['reservation']['job']['id'], job['jobId'])
        self.assertNotEqual(self.row(job)[0], 'completed')

    def test_operator_resolution_fails_the_job_without_requeue_or_deletion(self):
        job = self.uncertain_init()
        self.queue.operator(job['jobId'], 'resolve', dict(note='synthetic', workerStopped=True))
        record = self.public(job)
        self.assertEqual((record['status'], record['phase'], record['error']), ('failed', 'held', 'engine_init_uncertain'))
        self.assertIsNone(self.store.next_for_node('cluster'))
        response = self.api.handle('POST', '/ltds/worker/v1/claim', {'Authorization': 'Bearer ' + TOKEN},
            json.dumps(dict(jobId=job['jobId'], claimRequestId='driver-after-resolution')).encode(), protected_transport=True)
        self.assertEqual(response.body, dict(jobId=job['jobId'], status='failed', claimState='dropped'))

    def test_operator_terminal_keeps_a_live_fence_and_says_so_until_the_gate_is_resolved(self):
        job = self.uncertain_init()
        second = self.submit()
        self.queue.operator(job['jobId'], 'resolve', dict(note='synthetic', workerStopped=True))
        self.assert_fence_retained(job)
        self.assert_fence_retained(job)
        self.assertEqual(self.store.get(second['jobId'], private=True)['status'], 'queued')
        self.assertEqual(self.worker._row(job['jobId'])['phase'], 'initialize_pending')
        with NodeGate('engine') as gate:
            state = gate.snapshot()
            gate.resolve(state['reservation']['receiptId'], state['generation'], old_worker_stopped=True,
                coordinator_resolved=True, note='synthetic')
        self.worker.gate_generation += 1
        result = self.driver.step()
        self.assertEqual((result['state'], result['terminalStatus']), ('operator_terminal', 'failed'))
        self.assertEqual(self.row(job)[0], 'completed')
        self.assertEqual(self.worker._row(job['jobId'])['phase'], 'initialize_pending')
        self.transport.fail_initialize = False
        self.advance(second, 'claimed')
        self.assertEqual(self.public(second)['status'], 'running')
        self.assertEqual(self.public(job)['status'], 'failed')

    def test_operator_terminal_without_a_fence_frees_the_node(self):
        job, second = self.fetching(), self.submit()
        self.expire_reservation()
        self.steps(2)
        self.assert_public_held(job, 'reservation_expired')
        self.queue.operator(job['jobId'], 'resolve', dict(note='synthetic', workerStopped=True))
        self.assertEqual(self.driver.step()['state'], 'operator_terminal')
        self.advance(second, 'claimed')
        self.assertEqual(self.public(job)['status'], 'failed')

    def test_operator_ending_an_observed_job_is_noticed_and_the_fence_kept(self):
        job = self.executing()
        self.long_outage(job)
        self.queue.operator(job['jobId'], 'hold', dict(note='synthetic'))
        self.queue.operator(job['jobId'], 'resolve', dict(note='synthetic', workerStopped=True))
        self.assert_fence_retained(job)
        self.assertEqual(self.worker._row(job['jobId'])['phase'], 'processing')

    def test_terminal_word_with_another_binding_ends_nothing(self):
        job = self.fetching()
        self.expire_reservation()
        self.steps(2)
        self.queue.operator(job['jobId'], 'resolve', dict(note='synthetic', workerStopped=True))
        real = self.driver._request
        def tampered(method, suffix, payload=None, *, claim_token=None):
            status, value = real(method, suffix, payload, claim_token=claim_token)
            return status, dict(value, generation=value['generation'] + 1) if suffix.endswith('/claim-state') else value
        self.driver._request = tampered
        self.steps(2)
        self.assertEqual(self.row(job)[0], 'fetching')
        self.assertIn('hold', self.row(job)[1])


if __name__ == '__main__':
    unittest.main()
