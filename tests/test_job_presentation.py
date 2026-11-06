"""Start witness and public projection; real driver, worker, store and worker API.

Only the engine's edge is scripted. Status 20 alone, output alone and every
intent marker must fail to witness; a witness must change nothing but its own row.
"""
from contextlib import closing
import copy
import hashlib
import io
import json
import sqlite3
import time
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
import uuid

from processing import reference_presentation as presentation
from processing.maintenance import Conflict
from processing.node_gate import GateBusy, NodeGate
from processing.prototype import Queue
from processing.reference_control import ReferenceControlAPI
from processing.reference_driver import DriverHeld, DriverWait, ReferenceDriver
from processing.reference_engine_worker import ReferenceEngineWorker, WorkerHeld
from processing.reference_store import ReferenceStore
from processing.reference_worker_hold import make_request as make_hold, report as report_hold, withdraw_claim
from processing.viewer_service import EngineUnavailable
from tests import test_reference_driver as fixture

TOKEN, VIEWER = fixture.TOKEN, fixture.VIEWER
IDENTITY = dict(node_id='cluster', worker_id='worker', worker_instance_id='instance')
WORKER = {'Authorization': 'Bearer ' + TOKEN}


class Harness(unittest.TestCase):
    tearDown = fixture.ReferenceDriverTests.tearDown
    make_driver = fixture.ReferenceDriverTests.make_driver
    sender_open = fixture.ReferenceDriverTests.sender_open
    download = fixture.ReferenceDriverTests.download
    submit = fixture.ReferenceDriverTests.submit
    supply_grants = fixture.ReferenceDriverTests.supply_grants
    advance = fixture.ReferenceDriverTests.advance

    def setUp(self):
        self.output, self.posts, self.witness_error = ['first output line'], [], None
        fixture.ReferenceDriverTests.setUp(self)
        patched = patch.object(self.native, 'output_page', side_effect=lambda job, *, line, limit: (self.output, False))
        patched.start()
        self.patches.append(patched)

    def request(self, method, suffix, payload=None, *, claim_token=None):
        if suffix.endswith('/start-witness'):
            self.posts.append(copy.deepcopy(payload))
            if self.witness_error is not None:
                raise self.witness_error
        return fixture.ReferenceDriverTests.request(self, method, suffix, payload, claim_token=claim_token)

    def executing(self, code=20):
        self.transport.code = code
        job = self.submit()
        self.advance(job, 'executing')
        return job

    def row(self, job):
        with self.driver._db() as db:
            row = db.execute('SELECT stage,data FROM driver_jobs WHERE job=?', (job['jobId'],)).fetchone()
        return row['stage'], json.loads(row['data'])

    def latched(self, job=None):
        job = job or self.executing()
        self.assertEqual(self.driver.step()['state'], 'executing')
        return job, self.row(job)[1]['startWitness']['request']

    def token(self, job):
        return self.row(job)[1]['claim']['claimToken']

    def post(self, job, body, *, token=None):
        headers = dict(WORKER, **{'X-Claim-Token': token or self.token(job)})
        raw = body if type(body) is bytes else json.dumps(body).encode()
        return self.api.handle('POST', '/ltds/worker/v1/jobs/' + job['jobId'] + '/start-witness', headers, raw,
            protected_transport=True)

    def get(self, path):
        response = self.api.handle('GET', path, {'Authorization': 'Bearer ' + VIEWER}, protected_transport=True)
        self.assertEqual(response.status, 200)
        return response.body

    def public(self, job):
        return self.get('/ltds/v1/jobs/' + job['jobId'])

    def view(self, job):
        return self.public(job).get('presentation')

    def stored_rows(self, job):
        with self.queue._db() as db:
            return [dict(row) for row in db.execute('SELECT * FROM reference_outbox WHERE identity=?',
                (presentation.IDENTITY_PREFIX + job['jobId'],))]

    def stored(self, job):
        return json.loads(self.stored_rows(job)[0]['payload_json'])

    def execute(self, sql, *args):
        with closing(sqlite3.connect(self.queue.path)) as db, db:
            db.execute(sql, args)

    def restart_coordinator(self):
        self.queue.close()
        self.queue = Queue(self.root / 'queue', reference_enabled=True)
        self.store = ReferenceStore(self.queue)
        self.api = ReferenceControlAPI(self.store, viewer_bearers={VIEWER: 'staging'}, worker_bearers={TOKEN: IDENTITY},
            capability_fingerprint=self.fixture['capabilities']['capabilityFingerprint'],
            options_fingerprint=self.fixture['capabilities']['engines']['odm']['optionsFingerprint'])

    def snapshot(self):
        """Every durable thing except the one row a witness may add."""
        with closing(sqlite3.connect(self.queue.path)) as db:
            names = [row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
            tables = {name: db.execute('SELECT * FROM "' + name + '"').fetchall() for name in names if name != 'reference_outbox'}
        with closing(sqlite3.connect(self.ledger.path)) as db:
            ledger = list(db.iterdump())
        with closing(sqlite3.connect(self.worker.path)) as db:
            worker = list(db.iterdump())
        return tables, ledger, worker, (self.root / 'gate' / 'gate.json').read_bytes()


def resign(request, **changes):
    value = dict(request, **changes)
    value['witnessSha256'] = presentation.p.digest({key: value[key] for key in presentation.WITNESS_FIELDS})
    return value


class StartWitnessTests(Harness):
    """The worker's one sighting. A None is the only refusal the caller sees."""

    def scripted_info(self, job, replies):
        """Replace the task /info answers, one per read, and keep the rest of the engine."""
        real, replies = self.native.read.side_effect, list(replies)

        def read(route, **kwargs):
            if route == '/task/' + job['jobId'] + '/info':
                return replies.pop(0)
            return real(route, **kwargs)
        self.native.read.side_effect = read

    def witness(self, job):
        return self.worker.start_witness(job['jobId'])

    def test_control_status_20_with_output_witnesses_and_hashes_the_first_non_blank_line(self):
        job = self.executing()
        self.output = ['', '   ', 'real line', 'later']
        self.assertEqual(self.witness(job), dict(gateGeneration=1, gateReceiptId=self.worker._row(job['jobId'])['receipt'],
            nativeStatusCode=20, outputLineCount=4, outputPrefixSha256=hashlib.sha256(b'real line').hexdigest()))

    def test_status_20_with_empty_output_is_no_witness(self):
        job = self.executing()
        self.output = []
        self.assertIsNone(self.witness(job))

    def test_status_20_with_only_blank_lines_is_no_witness(self):
        job = self.executing()
        self.output = ['', '  ', '\t', '\r\n']
        self.assertIsNone(self.witness(job))

    def test_status_10_with_old_output_is_no_witness_before_and_after_a_worker_restart(self):
        job = self.executing(code=10)
        self.assertIsNone(self.witness(job))
        restarted = ReferenceEngineWorker(self.worker.path, ledger=self.ledger, outbox=self.outbox, engine=self.native,
            engine_id='engine', node_id='cluster', authority_id=self.queue.authority_id, coordinator_origin=self.gate_origin,
            gate_generation=1, worker_id='worker', engine_version='pinned-engine-1', engine_probe=lambda deadline: 'engine')
        self.assertIsNone(restarted.start_witness(job['jobId']))

    def test_status_must_hold_on_both_sides_of_the_output(self):
        for first, second in ((20, 10), (10, 20), (20, 30), (20, 40)):
            with self.subTest(first=first, second=second):
                job = self.executing()
                self.scripted_info(job, [{'uuid': job['jobId'], 'status': {'code': code}} for code in (first, second)])
                self.assertIsNone(self.witness(job))
                self.native.read.side_effect = self.transport.read
                self.transport.tasks[job['jobId']] = 40
                self.advance(job, 'awaiting_acceptance')

    def test_status_must_be_exactly_the_integer_20(self):
        for code in (True, 20.0, '20', None):
            with self.subTest(code=code):
                job = self.executing()
                self.scripted_info(job, [{'uuid': job['jobId'], 'status': {'code': code}}] * 2)
                self.assertIsNone(self.witness(job))
                self.native.read.side_effect = self.transport.read
                self.transport.tasks[job['jobId']] = 40
                self.advance(job, 'awaiting_acceptance')

    def test_wrong_task_uuid_is_no_witness(self):
        job = self.executing()
        self.scripted_info(job, [{'uuid': str(uuid.uuid4()), 'status': {'code': 20}}] * 2)
        self.assertIsNone(self.witness(job))

    def test_second_read_naming_another_task_is_no_witness(self):
        job = self.executing()
        self.scripted_info(job, [{'uuid': job['jobId'], 'status': {'code': 20}},
            {'uuid': str(uuid.uuid4()), 'status': {'code': 20}}])
        self.assertIsNone(self.witness(job))

    def test_gate_receipt_that_is_not_this_row_is_no_witness(self):
        job = self.executing()
        with self.worker._db() as db:
            db.execute('UPDATE executions SET receipt=? WHERE job=?', ('0' * 32, job['jobId']))
        self.assertIsNone(self.witness(job))

    def test_gate_with_a_completion_prepared_is_no_witness(self):
        job = self.executing()
        receipt = self.worker._row(job['jobId'])['receipt']
        with NodeGate('engine') as gate:
            gate.prepare_completion(receipt, dict(status='succeeded', result={'resultManifestSha256': 'a' * 64}))
        self.assertIsNone(self.witness(job))

    def test_gate_bound_to_another_job_is_no_witness(self):
        job = self.executing()
        path = self.root / 'gate' / 'gate.json'
        state = json.loads(path.read_text())
        state['reservation']['job']['id'] = str(uuid.uuid4())
        path.write_text(json.dumps(state))
        self.assertIsNone(self.witness(job))

    def test_gate_generation_that_changed_is_no_witness(self):
        job = self.executing()
        self.worker.gate_generation = 2
        self.assertIsNone(self.witness(job))

    def test_row_before_commit_is_no_witness_even_with_status_and_output(self):
        for phase in ('prepared', 'initialize_pending', 'upload_pending', 'archive_pending', 'delivered'):
            with self.subTest(phase=phase):
                job = self.executing()
                self.worker._phase(job['jobId'], phase)
                self.assertIsNone(self.witness(job))
                self.worker._phase(job['jobId'], 'processing')
                self.transport.tasks[job['jobId']] = 40
                self.advance(job, 'awaiting_acceptance')

    def test_commit_pending_row_with_both_proofs_witnesses(self):
        job = self.executing()
        self.worker._phase(job['jobId'], 'commit_pending')
        self.assertIsNotNone(self.witness(job))

    def test_engine_started_ledger_intent_alone_is_no_witness(self):
        job = self.executing(code=10)
        reservation = self.row(job)[1]['reservation']['reservationId']
        with self.ledger._transaction() as db:
            self.assertEqual(self.ledger._reservation(db, reservation)['state'], 'engine_started')
        self.output = []
        self.assertIsNone(self.witness(job))

    def test_processing_row_alone_is_no_witness(self):
        job = self.executing(code=10)
        self.assertEqual(self.worker._row(job['jobId'])['phase'], 'processing')
        self.output = []
        self.assertIsNone(self.witness(job))

    def test_processing_time_present_never_stands_in_for_status_or_output(self):
        job = self.executing()
        self.scripted_info(job, [{'uuid': job['jobId'], 'status': {'code': 10}, 'processingTime': 90000}] * 2)
        self.assertIsNone(self.witness(job))
        self.scripted_info(job, [{'uuid': job['jobId'], 'status': {'code': 20}, 'processingTime': 90000}] * 2)
        self.output = []
        self.assertIsNone(self.witness(job))

    def test_output_outside_its_bound_or_shape_is_no_witness(self):
        job = self.executing()
        for output in (['line'] * 9, 'line', None, [b'line'], ['line', 7], ['x' * 300000]):
            with self.subTest(output=str(output)[:20]):
                self.output = output
                self.assertIsNone(self.witness(job))

    def test_engine_identity_is_checked_after_the_reads_too(self):
        job = self.executing()
        identities = iter(['engine', 'other-engine'])
        self.worker.engine_probe = lambda deadline: next(identities)
        with self.assertRaises(WorkerHeld):
            self.witness(job)

    def test_engine_identity_is_checked_before_the_reads(self):
        job = self.executing()
        self.worker.engine_probe = lambda deadline: 'other-engine'
        before = len(self.transport.calls)
        with self.assertRaises(WorkerHeld):
            self.witness(job)
        self.assertEqual(len(self.transport.calls), before)


class DriverLatchTests(Harness):
    def test_latch_is_durable_before_any_post_and_the_next_step_sends_it(self):
        job, request = self.latched()
        self.assertEqual(self.posts, [])
        self.assertIsNone(self.row(job)[1]['startWitness']['ack'])
        self.assertEqual(self.stored_rows(job), [])
        self.assertEqual(self.driver.step()['state'], 'executing')
        self.assertEqual(self.posts, [request])
        self.assertEqual(self.row(job)[1]['startWitness']['ack']['requestSha256'], presentation.p.digest(request))
        self.assertEqual(len(self.stored_rows(job)), 1)

    def test_acknowledged_latch_is_never_sent_or_observed_again(self):
        job, _request = self.latched()
        self.driver.step()
        with patch.object(self.worker, 'start_witness') as watched:
            for _ in range(3):
                self.assertEqual(self.driver.step()['state'], 'executing')
        watched.assert_not_called()
        self.assertEqual(len(self.posts), 1)

    def test_status_10_with_output_never_latches_and_20_later_does(self):
        job = self.executing(code=10)
        for _ in range(3):
            self.assertEqual(self.driver.step()['state'], 'executing')
        self.assertNotIn('startWitness', self.row(job)[1])
        self.transport.tasks[job['jobId']] = 20
        self.driver.step()
        self.assertIn('startWitness', self.row(job)[1])

    def test_outage_keeps_the_exact_bytes_and_polling(self):
        job, request = self.latched()
        self.witness_error = DriverWait('coordinator_unavailable')
        for _ in range(3):
            self.assertEqual(self.driver.step()['state'], 'executing')
        self.witness_error = None
        self.driver.step()
        self.assertEqual(self.posts, [request] * 4)
        self.assertIsNotNone(self.row(job)[1]['startWitness']['ack'])

    def test_worker_restart_resends_the_journaled_bytes_and_mints_nothing(self):
        job, request = self.latched()
        self.driver = self.make_driver()
        self.driver.step()
        self.assertEqual(self.posts, [request])
        self.assertEqual(self.stored(job)['witnessId'], request['witnessId'])

    def test_rejection_stops_sending_keeps_the_latch_and_never_holds(self):
        job, _request = self.latched()
        self.witness_error = DriverHeld('control_request_rejected')
        results = [self.driver.step() for _ in range(3)]
        self.assertEqual({result['state'] for result in results}, {'executing'})
        stage, data = self.row(job)
        self.assertEqual(len(self.posts), 1)
        self.assertTrue(data['startWitness']['refused'])
        self.assertNotIn('hold', data)
        self.assertEqual(self.public(job)['status'], 'running')
        self.transport.tasks[job['jobId']] = 40
        self.advance(job, 'awaiting_acceptance')

    def test_real_request_404_from_an_old_coordinator_does_not_hold_the_job(self):
        job, _request = self.latched()
        served = self.driver._request

        def opener(request, timeout):
            raise HTTPError(request.full_url, 404, 'missing', {}, io.BytesIO())

        def route(method, suffix, payload=None, *, claim_token=None):
            if suffix.endswith('/start-witness'):
                return ReferenceDriver._request(self.driver, method, suffix, payload, claim_token=claim_token)
            return served(method, suffix, payload, claim_token=claim_token)
        self.driver._open, self.driver._request = opener, route
        result = self.driver.step()
        self.assertEqual(result['state'], 'executing')
        self.assertTrue(self.row(job)[1]['startWitness']['refused'])
        self.assertNotIn('hold', self.row(job)[1])

    def test_quota_refusal_does_not_stop_polling(self):
        job, _request = self.latched()
        self.store.max_metadata_bytes = 1
        self.assertEqual(self.driver.step()['state'], 'executing')
        self.store.max_metadata_bytes = 64 * 1024 * 1024
        self.assertTrue(self.row(job)[1]['startWitness']['refused'])
        self.assertEqual(self.stored_rows(job), [])
        self.transport.tasks[job['jobId']] = 40
        self.advance(job, 'awaiting_acceptance')

    def test_acknowledgement_for_something_else_stays_pending_and_never_holds(self):
        job, _request = self.latched()
        with patch('processing.reference_driver.validate_start_ack', side_effect=ValueError('binding')):
            self.assertEqual(self.driver.step()['state'], 'executing')
        self.assertIsNone(self.row(job)[1]['startWitness']['ack'])
        self.assertFalse(self.row(job)[1]['startWitness']['refused'])
        self.driver.step()
        self.assertIsNotNone(self.row(job)[1]['startWitness']['ack'])

    def test_no_observation_failure_reaches_the_step(self):
        job = self.executing()
        failures = (WorkerHeld('x'), GateBusy('x'), EngineUnavailable('x'), DriverHeld('x'), DriverWait('x'),
            OSError('x'), KeyError('x'), RuntimeError('x'))
        for failure in failures:
            with self.subTest(failure=type(failure).__name__), patch.object(self.worker, 'start_witness', side_effect=failure):
                self.assertEqual(self.driver.step(), {'state': 'executing', 'jobId': job['jobId']})
        self.assertNotIn('startWitness', self.row(job)[1])
        self.assertNotIn('hold', self.row(job)[1])
        self.assertNotIn('watch', self.row(job)[1])

    def test_journal_that_cannot_save_means_no_post(self):
        job = self.executing()
        real = self.driver._save
        with patch.object(self.driver, '_save', side_effect=sqlite3.OperationalError('locked')):
            self.assertEqual(self.driver.step()['state'], 'executing')
        self.assertNotIn('startWitness', self.row(job)[1])
        self.driver._save = real
        self.driver.step()
        self.assertEqual(self.posts, [])

    def test_held_row_still_flushes_and_stays_held(self):
        job, request = self.latched()
        stage, data = self.row(job)
        hold = dict(holdId=str(uuid.uuid4()), reasonCode='engine_init_uncertain', reason='engine_init_uncertain',
            sequence=0, observation=None, engineStatusCode=None, ack=None)
        self.driver._save(job['jobId'], stage, dict(data, hold=hold))
        result = self.driver.step()
        self.assertEqual(result['state'], 'held')
        self.assertEqual(self.posts, [request])
        data = self.row(job)[1]
        self.assertIsNotNone(data['startWitness']['ack'])
        self.assertIn('hold', data)
        self.assertEqual(self.public(job)['status'], 'held')
        self.assertIsNone(self.view(job)['state'])

    def test_other_engines_are_never_witnessed(self):
        job = self.executing()
        for kind in ('treedar', 'combined'):
            with self.subTest(kind=kind), patch.object(self.driver, '_kind', return_value=kind), \
                    patch.object(self.worker, 'start_witness') as watched:
                self.driver._latch_start(job['jobId'], 'executing', self.row(job)[1], 'request')
                watched.assert_not_called()

    def test_latch_is_no_input_to_recovery_proof_or_engine_watch(self):
        job, _request = self.latched()
        self.driver.step()
        stage, data = self.row(job)
        self.transport.tasks.pop(job['jobId'])
        self.assertEqual(self.driver.step()['state'], 'retry_wait')
        self.assertIn('suspect', self.row(job)[1])
        without = {key: value for key, value in self.row(job)[1].items() if key != 'startWitness'}
        self.assertEqual(without['claim'], data['claim'])
        self.assertEqual(without['reservation'], data['reservation'])

    def test_migration_census_ignores_the_latch(self):
        job, _request = self.latched()
        self.driver.step()
        self.transport.tasks[job['jobId']] = 40
        self.advance(job, 'awaiting_acceptance')
        physical = dict(nodeId='cluster', workerId='worker', engineId='engine', authorityId=self.queue.authority_id,
            gateGeneration=1, origin=self.gate_origin)
        stage, data = self.row(job)
        self.assertIn('startWitness', data)
        with_latch = ReferenceDriver.migration_census(self.driver.path, physical_binding=physical)
        del data['startWitness']
        self.driver._save(job['jobId'], stage, data)
        self.assertEqual(ReferenceDriver.migration_census(self.driver.path, physical_binding=physical), with_latch)


class CoordinatorReportTests(Harness):
    def accepted(self):
        job, request = self.latched()
        response = self.post(job, request)
        self.assertEqual(response.status, 200)
        return job, request, response.body

    def test_receipt_is_closed_and_validates_against_the_request(self):
        _job, request, ack = self.accepted()
        self.assertEqual(set(ack), set(presentation.ACK_FIELDS))
        self.assertEqual(presentation.validate_ack(ack, request), ack)
        self.assertEqual(ack['workerObservedAt'], request['observedAt'])

    def test_stored_row_is_one_acknowledged_row_of_the_new_kind_within_its_bound(self):
        job, request, _ack = self.accepted()
        rows = self.stored_rows(job)
        self.assertEqual(len(rows), 1)
        self.assertEqual((rows[0]['kind'], rows[0]['acknowledged'], rows[0]['attempts']), (presentation.KIND, 1, 0))
        self.assertEqual(set(json.loads(rows[0]['payload_json'])), set(presentation.RECORD_FIELDS))
        longest = dict(self.stored(job), claimRequestId='c' * 128, generation=2**53 - 1, gateGeneration=2**53 - 1)
        self.assertLessEqual(len(presentation.p.canonical_json(longest)), presentation.MAX_RECORD_BYTES)

    def test_exact_replay_returns_the_same_bytes_and_writes_nothing(self):
        job, request, ack = self.accepted()
        before = self.stored_rows(job)
        with patch.object(presentation, '_coordinator_now', return_value=4102444800.0):
            replay = self.post(job, request)
        self.assertEqual((replay.status, replay.body), (200, ack))
        self.assertEqual(self.stored_rows(job), before)

    def test_exact_replay_after_terminal_and_after_a_coordinator_restart(self):
        job, request, ack = self.accepted()
        self.transport.tasks[job['jobId']] = 40
        self.advance(job, 'awaiting_acceptance')
        self.assertEqual(self.post(job, request).body, ack)
        self.restart_coordinator()
        self.assertEqual(self.post(job, request).body, ack)

    def test_first_witness_wins_over_another_digest_or_identity(self):
        job, request, _ack = self.accepted()
        before = self.stored_rows(job)
        for changes in (dict(outputLineCount=2), dict(witnessId=str(uuid.uuid4())),
                dict(observedAt='2026-01-01T00:00:00.000Z'), dict(outputPrefixSha256='b' * 64)):
            with self.subTest(changes=changes):
                self.assertEqual(self.post(job, resign(request, **changes)).status, 409)
        self.assertEqual(self.stored_rows(job), before)

    def test_ownership_and_binding_are_checked_even_for_a_replay(self):
        job, request, _ack = self.accepted()
        self.assertEqual(self.post(job, request, token='another-claim-token').status, 409)
        for changes in (dict(claimRequestId='another-request'), dict(generation=2), dict(environment='production'),
                dict(coordinatorAuthorityId='another-authority'), dict(inputManifestSha256='c' * 64),
                dict(pipelineDigest='c' * 64), dict(claimTokenSha256='c' * 64)):
            with self.subTest(changes=changes):
                self.assertEqual(self.post(job, resign(request, **changes)).status, 409)

    def test_wrong_token_with_a_self_consistent_digest_is_refused(self):
        job, request = self.latched()
        forged = 'another-claim-token'
        body = resign(request, claimTokenSha256=hashlib.sha256(forged.encode()).hexdigest())
        self.assertEqual(self.post(job, body, token=forged).status, 409)
        self.assertEqual(self.stored_rows(job), [])

    def test_wrong_worker_is_refused(self):
        job, request = self.latched()
        with self.queue._db() as db:
            db.execute("UPDATE reference_claims SET instance_id='another-instance' WHERE job_id=?", (job['jobId'],))
        self.assertEqual(self.post(job, request).status, 409)
        self.assertEqual(self.stored_rows(job), [])

    def test_path_job_must_be_the_body_job(self):
        job, request = self.latched()
        other = dict(jobId=str(uuid.uuid4()))
        body = resign(request, jobId=other['jobId'], executionUuid=other['jobId'])
        self.assertEqual(self.post(job, body).status, 409)

    def test_no_claim_header_is_401_and_no_bearer_is_401(self):
        job, request = self.latched()
        path = '/ltds/worker/v1/jobs/' + job['jobId'] + '/start-witness'
        raw = json.dumps(request).encode()
        self.assertEqual(self.api.handle('POST', path, WORKER, raw, protected_transport=True).status, 401)
        self.assertEqual(self.api.handle('POST', path, {'X-Claim-Token': 'x'}, raw, protected_transport=True).status, 401)

    def test_protected_transport_is_required(self):
        job, request = self.latched()
        response = self.api.handle('POST', '/ltds/worker/v1/jobs/' + job['jobId'] + '/start-witness',
            dict(WORKER, **{'X-Claim-Token': self.token(job)}), json.dumps(request).encode())
        self.assertEqual(response.status, 503)

    def test_malformed_bodies_are_400_before_any_write(self):
        job, request = self.latched()
        bad = [dict(nativeStatusCode=10), dict(nativeStatusCode=True), dict(outputLineCount=0), dict(outputLineCount=9),
            dict(sequence=2), dict(sequence=0), dict(schemaVersion=2), dict(schemaVersion=True), dict(protocol='other'),
            dict(executionUuid=str(uuid.uuid4())), dict(witnessId='not-a-uuid'), dict(gateReceiptId='A' * 32),
            dict(gateReceiptId='a' * 31), dict(gateGeneration=0), dict(environment='other'),
            dict(outputPrefixSha256='short'), dict(observedAt='2026-11-05T10:00:00Z'),
            dict(observedAt='2026-11-05T10:00:00.000+00:00'), dict(observedAt='2026-13-05T10:00:00.000Z'),
            dict(observedAt='2026-02-30T10:00:00.000Z'), dict(observedAt=1)]
        for changes in bad:
            with self.subTest(changes=changes):
                self.assertEqual(self.post(job, resign(request, **changes)).status, 400)
        self.assertEqual(self.post(job, dict(request, witnessSha256='d' * 64)).status, 400)
        self.assertEqual(self.post(job, dict(request, extra=1)).status, 400)
        self.assertEqual(self.post(job, {k: v for k, v in request.items() if k != 'observedAt'}).status, 400)
        self.assertEqual(self.post(job, b'{').status, 400)
        self.assertEqual(self.post(job, json.dumps(request).encode() + b' ' * 2048).status, 400)
        self.assertEqual(self.stored_rows(job), [])

    def test_row_before_processing_is_refused(self):
        job, request = self.latched()
        with self.queue._db() as db:
            db.execute("UPDATE reference_jobs SET phase='fetching' WHERE job_id=?", (job['jobId'],))
        self.assertEqual(self.post(job, request).status, 409)
        self.assertEqual(self.stored_rows(job), [])

    def test_a_job_that_is_not_plain_odm_is_refused(self):
        job, request = self.latched()
        with self.queue._db() as db:
            raw = json.loads(db.execute('SELECT input_json FROM reference_jobs WHERE job_id=?', (job['jobId'],)).fetchone()[0])
            raw['pipeline']['treedar']['requested'] = True
            db.execute('UPDATE reference_jobs SET input_json=? WHERE job_id=?', (json.dumps(raw), job['jobId']))
        self.assertEqual(self.post(job, request).status, 409)
        self.assertEqual(self.stored_rows(job), [])

    def test_quota_is_called_before_the_insert_and_refuses_with_409(self):
        job, request = self.latched()
        self.store.max_metadata_bytes = 1
        self.assertEqual(self.post(job, request).status, 409)
        self.assertEqual(self.stored_rows(job), [])

    def test_freeze_is_503_with_no_write_and_reads_stay_open(self):
        job, request = self.latched()
        self.execute("UPDATE coordinator_freeze_state SET phase='held'")
        try:
            response = self.post(job, request)
            view = self.view(job)
        finally:
            self.execute("UPDATE coordinator_freeze_state SET phase='open'")
        self.assertEqual((response.status, response.body), (503, {'error': 'coordinator_maintenance'}))
        self.assertEqual(view['state'], 'preparing')
        self.assertEqual(self.stored_rows(job), [])
        self.assertEqual(self.post(job, request).status, 200)

    def test_first_report_on_a_held_job_is_recorded_and_lifts_nothing(self):
        job, request = self.latched()
        self.queue.operator(job['jobId'], 'hold', dict(note='synthetic hold'))
        before = self.snapshot()
        self.assertEqual(self.post(job, request).status, 200)
        self.assertEqual(self.snapshot(), before)
        record = self.public(job)
        self.assertEqual(record['status'], 'held')
        self.assertIsNone(record['presentation']['state'])

    def test_first_report_on_a_terminal_job_is_recorded_and_changes_no_status(self):
        job, request = self.latched()
        self.transport.tasks[job['jobId']] = 40
        with patch.object(self.driver, '_flush_start'):
            self.advance(job, 'awaiting_acceptance')
        before = self.snapshot()
        self.assertEqual(self.post(job, request).status, 200)
        self.assertEqual(self.snapshot(), before)
        self.assertIsNone(self.view(job)['state'])

    def test_report_leaves_every_other_table_ledger_worker_and_gate_byte_identical(self):
        job, request = self.latched()
        before = self.snapshot()
        self.assertEqual(self.post(job, request).status, 200)
        self.assertEqual(self.snapshot(), before)

    def test_public_time_is_coordinator_acceptance_and_worker_time_stays_attributed(self):
        job, request = self.latched()
        late = resign(request, observedAt='2026-11-05T09:00:00.000Z')
        # Accepted long after the worker saw it, so a min() of the two would show worker time.
        with patch.object(presentation, '_coordinator_now', return_value=1800000000.0):
            ack = self.post(job, late).body
        accepted = presentation.timestamp(1800000000.0)
        self.assertLess(late['observedAt'], accepted)
        self.assertEqual((ack['acceptedAt'], ack['workerObservedAt']), (accepted, late['observedAt']))
        self.assertEqual(self.view(job)['processingConfirmedAt'], accepted)
        self.assertEqual(self.stored(job)['workerObservedAt'], late['observedAt'])
        self.assertNotIn(late['observedAt'], json.dumps(self.public(job)))

    def test_worker_clock_is_never_compared_with_the_coordinators(self):
        for observed in ('2020-01-01T00:00:00.000Z', '2099-12-31T23:59:59.999Z'):
            with self.subTest(observed=observed):
                job, request = self.latched()
                body = resign(request, observedAt=observed)
                self.assertEqual(self.post(job, body).status, 200)
                self.assertEqual(self.stored(job)['workerObservedAt'], observed)
                self.assertEqual(self.view(job)['processingConfirmedAt'], self.stored(job)['acceptedAt'])
                self.transport.tasks[job['jobId']] = 40
                self.driver.step()
                self.advance(job, 'awaiting_acceptance')

    def test_coordinator_restart_keeps_both_times_and_held_dominates_first(self):
        job, request, ack = self.accepted()
        self.restart_coordinator()
        record = self.public(job)
        self.assertEqual((record['status'], record['error']), ('held', 'coordinator_restart'))
        self.assertEqual(record['presentation']['state'], None)
        self.assertEqual(record['presentation']['processingConfirmedAt'], ack['acceptedAt'])
        self.execute("UPDATE jobs SET status='running',held_reason=NULL WHERE id=?", job['jobId'])
        self.assertEqual(self.view(job), dict(schemaVersion=1, protocol='ltds-job-presentation-v1', state='processing',
            processingConfirmedAt=ack['acceptedAt']))
        self.assertEqual(self.post(job, request).body, ack)


class ProjectionTests(Harness):
    PUBLIC = dict(jobId=str(uuid.uuid4()), generation=1, status='running', phase='processing', error=None)
    RECORD = dict(witnessId=str(uuid.uuid4()), sequence=1, requestSha256='a' * 64, witnessSha256='b' * 64,
        executionUuid=PUBLIC['jobId'], generation=1, claimRequestId='driver-1', gateGeneration=1, gateReceiptId='c' * 32,
        workerObservedAt='2026-11-05T10:00:00.000Z', acceptedAt='2026-11-05T10:00:01.000Z')

    def project(self, *, witness=True, **changes):
        raw = json.dumps(self.RECORD) if witness else None
        return presentation.project(dict(self.PUBLIC, **changes), raw)

    def test_every_precedence_row_nulls_state_and_keeps_history(self):
        rows = [dict(status=status) for status in ('failed', 'cancelled', 'succeeded')]
        rows += [dict(status='held', phase='held', error=code) for code in ('coordinator_restart', 'source_receipt_held')]
        rows += [dict(status='held', phase='processing'), dict(status='held', phase='held', error='engine_init_uncertain')]
        rows += [dict(queueReason=reason) for reason in ('node_lost', 'engine_task_missing', 'source_receiving',
            'scratch_capacity_unavailable', 'worker_reconciliation_required')]
        rows += [dict(error='some_error')]
        rows += [dict(phase=phase) for phase in ('waiting_for_delivery', 'awaiting_acceptance', 'completed')]
        for row in rows:
            for witness in (True, False):
                with self.subTest(row=row, witness=witness):
                    view = self.project(witness=witness, **row)
                    self.assertIsNone(view['state'])
                    self.assertEqual(view['processingConfirmedAt'], self.RECORD['acceptedAt'] if witness else None)

    def test_derivation_table(self):
        table = (('queued', 'queued', False, 'queued'), ('queued', 'waiting_capacity', False, 'queued'),
            ('running', 'queued', False, 'preparing'), ('running', 'waiting_capacity', False, 'preparing'),
            ('running', 'awaiting_transfer', False, 'preparing'), ('running', 'fetching', False, 'preparing'),
            ('running', 'processing', False, 'preparing'), ('running', 'processing', True, 'processing'))
        for status, phase, witness, state in table:
            with self.subTest(status=status, phase=phase, witness=witness):
                self.assertEqual(self.project(witness=witness, status=status, phase=phase)['state'], state)

    def test_unknown_combinations_are_null_and_never_queued(self):
        for changes in (dict(status='queued', phase='processing'), dict(status='queued', phase='fetching'),
                dict(status='mystery'), dict(status='running', phase='mystery'), dict(status='queued', phase='mystery')):
            with self.subTest(changes=changes):
                self.assertIsNone(self.project(**changes)['state'])

    def test_exact_fields_and_no_invented_signals(self):
        self.assertEqual(self.project(), dict(schemaVersion=1, protocol='ltds-job-presentation-v1', state='processing',
            processingConfirmedAt=self.RECORD['acceptedAt']))

    def test_damaged_latch_omits_the_projection_and_never_claims_processing(self):
        damaged = ['{', 'null', '[]', '{}', json.dumps(dict(self.RECORD, extra=1)),
            json.dumps({k: v for k, v in self.RECORD.items() if k != 'acceptedAt'}),
            json.dumps(dict(self.RECORD, acceptedAt='yesterday')), json.dumps(dict(self.RECORD, acceptedAt=None)),
            json.dumps(dict(self.RECORD, workerObservedAt='2026-13-01T00:00:00.000Z')),
            json.dumps(dict(self.RECORD, executionUuid=str(uuid.uuid4()))), json.dumps(dict(self.RECORD, generation=2)),
            json.dumps(dict(self.RECORD, generation=True)), json.dumps(dict(self.RECORD, sequence=2)),
            json.dumps(dict(self.RECORD, requestSha256='short')), json.dumps(dict(self.RECORD, gateReceiptId='x')),
            json.dumps(dict(self.RECORD, witnessId='x')), json.dumps(dict(self.RECORD, claimRequestId='not a slug'))]
        for raw in damaged:
            with self.subTest(raw=raw[:40]):
                self.assertIsNone(presentation.project(dict(self.PUBLIC), raw))

    def test_damaged_stored_row_keeps_get_working_and_replay_refused(self):
        job, request = self.latched()
        self.driver.step()
        expected = {k: v for k, v in self.public(job).items() if k != 'presentation'}
        self.execute('UPDATE reference_outbox SET payload_json=? WHERE identity=?', '{',
            presentation.IDENTITY_PREFIX + job['jobId'])
        record = self.public(job)
        self.assertNotIn('presentation', record)
        self.assertEqual(record, expected)
        self.assertEqual(self.post(job, request).status, 409)
        self.assertEqual(self.stored_rows(job)[0]['payload_json'], '{')

    def test_row_of_another_kind_or_unacknowledged_is_no_latch(self):
        job, _request = self.latched()
        self.driver.step()
        identity = presentation.IDENTITY_PREFIX + job['jobId']
        self.execute('UPDATE reference_outbox SET kind=? WHERE identity=?', 'claim_refusal', identity)
        self.assertEqual(self.view(job)['state'], 'preparing')
        self.execute('UPDATE reference_outbox SET kind=?,acknowledged=0 WHERE identity=?', presentation.KIND, identity)
        self.assertEqual(self.view(job)['state'], 'preparing')

    def test_lifecycle_through_every_public_endpoint(self):
        self.transport.code = 20
        job = self.submit()
        self.assertEqual(self.view(job), dict(schemaVersion=1, protocol='ltds-job-presentation-v1', state='queued',
            processingConfirmedAt=None))
        seen = []
        for _ in range(60):
            self.supply_grants()
            self.driver.step()
            seen.append(self.view(job)['state'])
            if self.row(job)[0] == 'executing' and 'startWitness' in self.row(job)[1] and self.row(job)[1]['startWitness']['ack']:
                break
        self.assertEqual(seen[-1], 'processing')
        self.assertEqual(set(seen), {'queued', 'preparing', 'processing'})
        views = [self.get(path)['presentation'] for path in ('/ltds/v1/jobs/' + job['jobId'], '/ltds/v2/jobs/' + job['jobId'],
            '/ltds/v1/jobs/by-submission/' + job['attemptId'])]
        self.assertEqual(views, [views[0]] * 3)
        self.assertEqual(views[0]['state'], 'processing')

    def test_observing_report_hides_state_and_never_erases_the_latch(self):
        job, _request = self.latched()
        self.driver.step()
        confirmed = self.view(job)['processingConfirmedAt']
        stage, data = self.row(job)
        with self.driver._db() as db:
            request_id = db.execute('SELECT claim_request FROM driver_jobs WHERE job=?', (job['jobId'],)).fetchone()[0]

        def report(sequence, reason, observation):
            hold = dict(holdId=hold_id, sequence=sequence, reasonCode=reason, observation=observation, engineStatusCode=None)
            report_hold(self.store, job['jobId'], make_hold(data['claim'], authority_id=self.queue.authority_id,
                claim_request_id=request_id, hold=hold), identity=IDENTITY, claim_token=data['claim']['claimToken'])
        hold_id = str(uuid.uuid4())
        report(0, 'node_lost', 'engine_unreachable')
        record = self.public(job)
        self.assertEqual(record['queueReason'], 'node_lost')
        self.assertEqual(record['presentation'], dict(schemaVersion=1, protocol='ltds-job-presentation-v1', state=None,
            processingConfirmedAt=confirmed))
        report(1, None, None)
        self.assertEqual(self.view(job)['state'], 'processing')
        self.assertEqual(self.view(job)['processingConfirmedAt'], confirmed)

    def test_delivery_dominates_and_history_stays(self):
        job, _request = self.latched()
        self.driver.step()
        confirmed = self.view(job)['processingConfirmedAt']
        self.transport.tasks[job['jobId']] = 40
        self.advance(job, 'awaiting_acceptance')
        record = self.public(job)
        self.assertIn(record['phase'], ('waiting_for_delivery', 'awaiting_acceptance'))
        self.assertEqual(record['presentation']['state'], None)
        self.assertEqual(record['presentation']['processingConfirmedAt'], confirmed)

    def test_job_finishing_between_polls_never_shows_processing(self):
        job = self.executing(code=40)
        self.advance(job, 'awaiting_acceptance')
        self.assertEqual(self.view(job)['state'], None)
        self.assertIsNone(self.view(job)['processingConfirmedAt'])
        self.assertEqual(self.stored_rows(job), [])

    def test_terminal_failure_acceptance_is_unchanged_by_a_latch(self):
        job, _request = self.latched()
        self.driver.step()
        self.transport.tasks[job['jobId']] = 30
        self.advance(job, 'awaiting_failure_acceptance')
        record = self.public(job)
        self.assertEqual(record['status'], 'failed')
        self.assertIsNone(record['presentation']['state'])
        response = self.api.handle('GET', '/ltds/v1/jobs/' + job['jobId'] + '/terminal-outcome',
            {'Authorization': 'Bearer ' + VIEWER}, protected_transport=True)
        self.assertEqual(response.status, 200)
        self.assertNotIn('presentation', json.dumps(response.body))
        with self.queue._db() as db:
            result = db.execute('SELECT result FROM jobs WHERE id=?', (job['jobId'],)).fetchone()[0]
        self.assertNotIn('witness', result.lower())

    def test_public_record_is_exactly_the_old_record_plus_presentation(self):
        job, _request = self.latched()
        self.driver.step()
        with self.queue._db() as db:
            old = self.store._public(self.store._row(db, job['jobId']))
        record = self.public(job)
        self.assertEqual(set(record) - set(old), {'presentation'})
        self.assertEqual({key: record[key] for key in old}, old)

    def test_worker_replies_and_post_replies_carry_no_projection_and_do_not_change(self):
        job, request = self.latched()
        token = self.token(job)
        with self.driver._db() as db:
            claim_request = db.execute('SELECT claim_request FROM driver_jobs WHERE job=?', (job['jobId'],)).fetchone()[0]
        headers = dict(WORKER, **{'X-Claim-Token': token})
        root = '/ltds/worker/v1'

        def replies():
            items = [self.api.handle('POST', root + '/claim', WORKER, json.dumps(dict(jobId=job['jobId'],
                    claimRequestId=claim_request)).encode(), protected_transport=True),
                self.api.handle('POST', root + '/jobs/' + job['jobId'] + '/phase', WORKER,
                    json.dumps(dict(claimToken=token, phase='processing')).encode(), protected_transport=True),
                self.api.handle('GET', root + '/next', WORKER, protected_transport=True),
                self.api.handle('GET', root + '/jobs/' + job['jobId'] + '/claim-state', headers, protected_transport=True)]
            self.assertEqual([item.status for item in items], [200] * 4)
            return [presentation.p.canonical_json(item.body) for item in items]
        before = replies()
        self.assertEqual(self.post(job, request).status, 200)
        after = replies()
        self.assertEqual(after, before)
        self.assertFalse(any(b'presentation' in reply for reply in after))
        self.assertNotIn('presentation', self.row(job)[1]['claim'])
        self.assertEqual(self.store.get(job['jobId'], private=True).get('presentation'), None)


class OutboxConsumerTests(Harness):
    """Every in-process reader of reference_outbox, against the new kind."""

    def reported(self):
        job, _request = self.latched()
        self.driver.step()
        self.assertEqual(len(self.stored_rows(job)), 1)
        return job

    def test_quota_counts_exactly_the_payload(self):
        job, _request = self.latched()
        with self.queue._db() as db:
            before = self.store._metadata_used_bytes(db)
        self.driver.step()
        with self.queue._db() as db:
            after = self.store._metadata_used_bytes(db)
        self.assertEqual(after - before, len(self.stored_rows(job)[0]['payload_json']))

    def test_freeze_budget_sees_the_same_bytes_as_ordinary_usage(self):
        job, _request = self.latched()
        gate = self.queue._coordinator_freeze
        with self.queue._db() as db:
            before = gate._ordinary_usage(db)[0]
        self.driver.step()
        with self.queue._db() as db:
            after = gate._ordinary_usage(db)[0]
        self.assertEqual(after - before, len(self.stored_rows(job)[0]['payload_json']))

    def test_sender_queue_never_sees_it_and_retry_refuses_it(self):
        job = self.reported()
        self.assertEqual(self.store.pending_outbox(now=time.time() + 10**6), [])
        with self.assertRaises(Conflict):
            self.store.retry_outbox(self.stored_rows(job)[0]['identity'])

    def test_enqueue_cannot_address_this_identity(self):
        job = self.reported()
        with self.assertRaises(ValueError):
            self.store.enqueue_outbox('delivery_ack', self.stored_rows(job)[0]['identity'],
                dict(jobId=job['jobId'], generation=1, environment='staging', receiptId='r'))

    def test_claim_refusal_prune_leaves_it_and_refusal_lookup_ignores_it(self):
        job = self.reported()
        rows = self.stored_rows(job)
        waiting = self.submit()
        self.assertEqual(withdraw_claim(self.store, waiting['jobId'], 'refused-request', **IDENTITY), ('queued', 'refused'))
        self.assertEqual(self.stored_rows(job), rows)
        with self.queue._db() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM reference_outbox WHERE kind='claim_refusal'").fetchone()[0], 1)


if __name__ == '__main__':
    unittest.main()
