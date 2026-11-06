"""Worker-to-coordinator delivery across real process death: SIGKILL, then fresh processes.

Separate OS processes, the real ReferenceSender and WorkerOutbox, verified loopback
TLS, the real DeliveryReceiver and SQLite on both sides. A process dies inside a
named hook, so the point is exact and nothing races a timer. The test reads the
databases and never writes them. CLOCK_SKEW is a plain file that lapses a lease or
backoff without a wait.

The engine and the Viewer are synthetic. This test plays the Viewer: it reads the
result over TLS and posts one signed V2 copy receipt, and no result bytes move. Real
ScratchCleanupExecutor runs on every step with no grace, so the only thing holding a
result back is the verified copy, and every finished case proves the cleanup is live.
"""
import hashlib
import http.client
import json
import unittest
from urllib.request import HTTPSHandler, ProxyHandler, Request, build_opener
import uuid

from processing import reference_protocol as protocol
from processing.reference_tls import verified_client_context
from tests import test_reference_source_fetch_process_restart as restart
from tests.source_fetch_process_fixture import BACKGROUND_LOG, CLOCK_SKEW, ENGINE_SCRIPT, VIEWER_TOKEN
from tests.viewer_v2_receipt_signer import SyntheticViewerCopyV2Signer

# The bytes the intake fixture lands in the spool: the photo, the raw GCP file
# Viewer holds permanently and the attempt's derived GCP list.
SOURCE_BYTES = (b'abc', b'id,lat,lon\n1,2,3\n', b'1,2,3\n')
LEASE_LAPSED = 61   # past WorkerOutbox's 60-second sender lease
BACKOFF_LAPSED = 10  # past the first delivery and observation backoff
WEEK = 7 * 24 * 3600


class DeliveryProcessDeathTests(restart.ProcessHarness):
    def setUp(self):
        super().setUp()
        self.signer = SyntheticViewerCopyV2Signer()
        config = json.loads(self.config.read_text())
        config.update(viewerKeys=self.signer.trusted_keys, cleanupGrace=0)
        self.config.write_text(json.dumps(config))
        self.origin = 'https://127.0.0.1:' + str(config['port'])
        self.opener = build_opener(ProxyHandler({}), HTTPSHandler(context=verified_client_context(config['cert'])))
        self.sources = self.spool_files()
        self.assertLessEqual({hashlib.sha256(body).hexdigest() for body in SOURCE_BYTES}, set(self.sources.values()))

    # Durable facts, read only.

    def spool_files(self):
        spool = self.root / 'spool'
        return {str(path.relative_to(spool)): hashlib.sha256(path.read_bytes()).hexdigest()
                for path in spool.rglob('*') if path.is_file()}

    def identity(self):
        """Everything that must never change once a job has run: one of each, for good."""
        database, outbox = self.database, self.root / 'outbox.sqlite'
        facts = dict(job=self.read(database, 'SELECT id,request_id,claim_hash,node_id FROM jobs'),
            generation=self.read(database, 'SELECT job_id,generation,manifest_sha,pipeline_digest FROM reference_jobs'),
            claim=self.read(database, 'SELECT request_id,job_id,token FROM reference_claims'),
            completion=self.read(outbox, 'SELECT id,job,generation,payload,digest,local_metadata FROM completions'),
            reservation=self.read(self.root / 'ledger.sqlite', 'SELECT id FROM reservations'),
            execution=self.read(self.root / 'worker.sqlite', 'SELECT job,receipt,result,outbox_id FROM executions'))
        for name, rows in facts.items():
            self.assertEqual(len(rows), 1, name)
        self.assertEqual(facts['generation'][0][1], 1)
        self.assertEqual(facts['claim'][0][2], self.journal()[1]['claim']['claimToken'])
        return facts

    def delivery(self):
        """id, state, attempts, digest and receipt of the one outbox row."""
        rows = self.read(self.root / 'outbox.sqlite', 'SELECT id,state,attempts,digest,receipt FROM completions')
        self.assertEqual(len(rows), 1)
        return rows[0]

    def coordinator_receipts(self):
        return self.read(self.database, 'SELECT outbox_id,payload_sha,receipt_json FROM reference_worker_receipts')

    def job_state(self):
        return self.read(self.database, 'SELECT phase,result_json IS NOT NULL FROM reference_jobs')[0]

    def worker_phase(self):
        return self.read(self.root / 'worker.sqlite', 'SELECT phase FROM executions')[0][0]

    def result_object(self):
        rows = self.read(self.root / 'ledger.sqlite', 'SELECT path,sha,accepted,accepted_copy_verified,cleaned FROM objects')
        self.assertEqual(len(rows), 1)
        return rows[0]

    def background_log(self):
        path = self.root / BACKGROUND_LOG
        return path.read_text() if path.exists() else ''

    def background_checked(self):
        return self.read(self.root / 'driver-journal.sqlite', 'SELECT background_checked FROM driver_jobs')[0][0]

    def assert_background_ran(self, worker):
        """This step really reached cleanup, or a retained file would prove nothing."""
        checked = self.background_checked()
        self.assertIn(self.step(worker)['state'], ('idle', 'not_ready'))
        self.assertGreater(self.background_checked(), checked)
        self.assertEqual(self.background_log(), '')

    def assert_sources_retained(self):
        current = self.spool_files()
        self.assertEqual({path: current.get(path) for path in self.sources}, self.sources)

    def assert_retained(self, *, verified=False):
        """No cleanup began, the result is intact, and the worker's verification is as stated."""
        self.assertEqual(self.background_log(), '')
        path, sha, accepted, copy_verified, cleaned = self.result_object()
        self.assertEqual((accepted is not None, copy_verified, cleaned), (verified, int(verified), None))
        self.assertEqual(self.read(self.root / 'ledger.sqlite', 'SELECT COUNT(*) FROM cleanup_intents'), [(0,)])
        self.assertEqual(hashlib.sha256((self.root / 'scratch' / path).read_bytes()).hexdigest(), sha)
        self.assert_sources_retained()

    def assert_engine_ran_once(self):
        for call in ('initialize', 'commit', 'download'):
            self.assertEqual(self.engine_calls().count(call), 1, call)

    def assert_exact_acknowledgement(self, attempts):
        """The worker saved, byte for byte, the one receipt the coordinator first stored."""
        identifier, state, sent, digest, receipt = self.delivery()
        self.assertEqual((state, sent), ('acknowledged', attempts))
        self.assertEqual(self.coordinator_receipts(), [(identifier, digest, receipt)])
        self.assertEqual(self.job_state()[1], 1)

    def assert_finished(self, worker, before, *, attempts):
        """The worker verifies the signed copy; then, and only then, its result copy goes."""
        for _ in range(4):
            if self.journal()[0] == 'completed':
                break
            self.step(worker)
        stage, data = self.journal()
        self.assertEqual(stage, 'completed', self.background_log())
        self.assertEqual(self.background_log(), '')
        self.assertEqual(self.status(), ('succeeded', None))
        self.assertEqual(self.job_state(), ('completed', 1))
        self.assertEqual(self.worker_phase(), 'delivered')
        path, _sha, accepted, copy_verified, cleaned = self.result_object()
        self.assertEqual((accepted is not None, copy_verified, cleaned is not None), (True, 1, True))
        self.assertFalse((self.root / 'scratch' / path).exists())
        self.assert_sources_retained()
        self.assert_exact_acknowledgement(attempts)
        self.assertEqual(self.identity(), before)
        result = self.read(self.database, 'SELECT result_json FROM reference_jobs')[0][0]
        completion = json.loads(before['completion'][0][3])
        self.assertEqual(json.loads(result), completion['resultManifest'])
        self.assertEqual(json.loads(before['execution'][0][2]), completion['resultManifest'])
        self.assertEqual(self.read(self.database, 'SELECT COUNT(*) FROM reference_acceptances'), [(1,)])
        self.assertEqual(self.read(self.database, 'SELECT COUNT(*) FROM reference_viewer_receipts'), [(1,)])
        ack = self.read(self.database, 'SELECT ack_json FROM reference_worker_acceptance_observations')
        self.assertEqual([json.loads(row[0]) for row in ack], [data['acceptanceObservationAck']])
        self.assert_engine_ran_once()

    # Driving the processes.

    def lapse(self, seconds):
        (self.root / CLOCK_SKEW).write_text(str(seconds))

    def dies(self, child):
        self.assertIsNone(self.step(child))
        self.assertEqual(child.wait(restart.REAP_SECONDS), -9)

    def delivering(self, worker):
        self.fetched(worker)
        self.assert_started_once(worker)
        (self.root / ENGINE_SCRIPT).write_text(json.dumps({'code': 40}))
        self.assertEqual(self.step(worker)['state'], 'delivering')
        self.assertEqual(self.journal()[0], 'delivering')
        self.assertEqual(self.delivery()[1:3], ('pending', 0))
        self.assert_retained()

    def advance_to_acceptance(self, worker):
        self.assertEqual(self.step(worker), {'state': 'awaiting_acceptance', 'jobId': self.job_id})
        self.assertEqual(self.journal()[0], 'awaiting_acceptance')
        self.assertEqual(self.worker_phase(), 'delivered')

    def awaiting_copy(self, worker):
        self.delivering(worker)
        self.advance_to_acceptance(worker)
        self.assert_exact_acknowledgement(1)
        self.assertEqual(self.status(), ('succeeded', None))
        self.assertEqual(self.job_state(), ('awaiting_acceptance', 1))
        self.assert_retained()

    def killed_with_the_reply_unsaved(self):
        """Boundary 1: the coordinator holds its receipt; the worker holds only its attempt."""
        coordinator = self.spawn('coordinator')
        dying = self.spawn('worker', DELIVERY_DIE='before_outbox_ack')
        self.delivering(dying)
        self.dies(dying)
        self.assertEqual(self.status(), ('succeeded', None))
        self.assertEqual(self.job_state(), ('awaiting_acceptance', 1))
        stored = self.coordinator_receipts()
        identifier, state, attempts, digest, receipt = self.delivery()
        self.assertEqual((state, attempts, receipt), ('sending', 1, None))
        self.assertEqual([row[:2] for row in stored], [(identifier, digest)])
        self.assertEqual((self.journal()[0], self.worker_phase()), ('delivering', 'awaiting_delivery'))
        self.assert_retained()
        return coordinator, self.identity()

    # The Viewer's side.

    def viewer(self, method, suffix, body=None):
        headers = {'Authorization': 'Bearer ' + VIEWER_TOKEN, 'Accept': 'application/json'}
        if body is not None:
            headers['Content-Type'] = 'application/json'
        request = Request(self.origin + protocol.PREFIX + '/jobs/' + self.job_id + suffix, data=body, method=method, headers=headers)
        with self.opener.open(request, timeout=30) as response:
            return response.status, json.loads(response.read())

    def copy_request(self):
        """What Viewer keeps once it holds a verified copy: the exact bytes of every retry."""
        _status, result = self.viewer('GET', '/results')
        claim = self.journal()[1]['claim']
        acceptance = dict(schemaVersion=1, receiptId=str(uuid.uuid4()), storageReceiptId=str(uuid.uuid4()),
            jobId=self.job_id, generation=claim['generation'], environment=claim['environment'],
            inputManifestSha256=claim['manifestSha256'], pipelineDigest=claim['pipelineDigest'],
            resultManifestSha256=result['resultManifestSha256'],
            acceptedObjectIds=sorted(item['id'] for item in result['objects']),
            acceptedAt='2026-11-04T00:00:02.000Z', validationVersion=protocol.VALIDATION_VERSION)
        expected = {**claim['sourceAcceptanceBinding'], 'resultManifestSha256': result['resultManifestSha256']}
        return protocol.canonical_json(self.signer.envelope(expected, acceptance, result))

    def accept(self):
        status, reply = self.viewer('POST', '/acceptance', self.copy_request())
        self.assertEqual((status, reply['created'], reply['fullyAccepted']), (200, True, True))

    # Control.

    def test_control_no_death_reaches_verified_cleanup(self):
        self.spawn('coordinator')
        worker = self.spawn('worker')
        self.awaiting_copy(worker)
        before = self.identity()
        for _ in range(3):
            self.assert_background_ran(worker)
        self.assert_retained()
        self.accept()
        self.assert_finished(worker, before, attempts=1)
        self.assertEqual(self.receipts(), [])

    # Worker death around the coordinator's acceptance of the completion.

    def test_killed_before_the_post_resends_the_one_delivery_after_its_lease(self):
        self.spawn('coordinator')
        dying = self.spawn('worker', DELIVERY_DIE='before_results_post')
        self.delivering(dying)
        self.dies(dying)
        self.assertEqual(self.delivery()[1:3], ('sending', 1))
        self.assertEqual(self.coordinator_receipts(), [])
        self.assertEqual((self.status(), self.job_state()[1]), (('running', None), 0))
        before = self.identity()
        worker = self.spawn('worker')
        self.assertEqual(self.step(worker), {'state': 'delivering', 'jobId': self.job_id, 'deliveryState': 'idle'})
        self.lapse(LEASE_LAPSED)
        self.advance_to_acceptance(worker)
        self.accept()
        self.assert_finished(worker, before, attempts=2)

    def test_killed_after_coordinator_acceptance_replays_the_same_delivery(self):
        _coordinator, before = self.killed_with_the_reply_unsaved()
        stored = self.coordinator_receipts()
        worker = self.spawn('worker')
        self.assertEqual(self.step(worker), {'state': 'delivering', 'jobId': self.job_id, 'deliveryState': 'idle'})
        self.assertEqual(self.delivery()[1:3], ('sending', 1))
        self.lapse(LEASE_LAPSED)
        self.advance_to_acceptance(worker)
        self.assertEqual(self.coordinator_receipts(), stored)
        self.assert_exact_acknowledgement(2)
        self.assertEqual(self.identity(), before)
        self.assert_engine_ran_once()
        self.assert_retained()
        self.accept()
        self.assert_finished(worker, before, attempts=2)
        self.assertEqual(self.receipts(), [])

    def test_both_killed_after_coordinator_acceptance_replay_without_any_recovery(self):
        coordinator, before = self.killed_with_the_reply_unsaved()
        self.kill(coordinator)
        self.spawn('coordinator')
        self.assertEqual(self.status(), ('succeeded', None))
        worker = self.spawn('worker')
        self.lapse(LEASE_LAPSED)
        self.advance_to_acceptance(worker)
        self.accept()
        self.assert_finished(worker, before, attempts=2)
        self.assertEqual(self.receipts(), [])

    def test_viewer_accepting_while_the_worker_is_dead_leaves_the_replay_exact(self):
        _coordinator, before = self.killed_with_the_reply_unsaved()
        self.accept()
        self.assertEqual(self.job_state(), ('completed', 1))
        self.assert_retained()
        worker = self.spawn('worker')
        self.lapse(LEASE_LAPSED)
        self.advance_to_acceptance(worker)
        self.assert_exact_acknowledgement(2)
        self.assert_retained()
        self.assert_finished(worker, before, attempts=2)

    def test_killed_after_the_outbox_saved_the_reply_posts_nothing_again(self):
        self.spawn('coordinator')
        dying = self.spawn('worker', DELIVERY_DIE='after_outbox_ack')
        self.delivering(dying)
        self.dies(dying)
        self.assert_exact_acknowledgement(1)
        self.assertEqual((self.journal()[0], self.worker_phase()), ('delivering', 'awaiting_delivery'))
        before = self.identity()
        worker = self.spawn('worker')
        self.advance_to_acceptance(worker)
        self.assert_exact_acknowledgement(1)
        self.assert_retained()
        self.accept()
        self.assert_finished(worker, before, attempts=1)

    def test_killed_between_the_gate_ack_and_its_phase_settles_from_the_gate(self):
        self.spawn('coordinator')
        dying = self.spawn('worker', DELIVERY_DIE='before_delivered_phase')
        self.delivering(dying)
        self.dies(dying)
        self.assert_exact_acknowledgement(1)
        self.assertEqual((self.journal()[0], self.worker_phase()), ('delivering', 'awaiting_delivery'))
        before = self.identity()
        worker = self.spawn('worker')
        self.advance_to_acceptance(worker)
        self.accept()
        self.assert_finished(worker, before, attempts=1)

    # Coordinator death part way through accepting the completion.

    def test_coordinator_killed_between_seal_and_completion_finishes_on_replay(self):
        coordinator = self.spawn('coordinator', COORDINATOR_DIE='after_seal')
        worker = self.spawn('worker')
        self.delivering(worker)
        self.assertEqual(self.step(worker), {'state': 'delivering', 'jobId': self.job_id, 'deliveryState': 'retry_scheduled'})
        self.assertEqual(coordinator.wait(restart.REAP_SECONDS), -9)
        self.assertEqual(self.job_state(), ('waiting_for_delivery', 1))
        self.assertEqual(self.status(), ('running', None))
        self.assertEqual(self.coordinator_receipts(), [])
        self.assertEqual(self.delivery()[1:3], ('pending', 1))
        before = self.identity()
        self.spawn('coordinator')
        self.assertEqual(self.status(), ('held', 'coordinator_restart'))
        self.lapse(BACKOFF_LAPSED)
        self.advance_to_acceptance(worker)
        self.assertEqual(self.status(), ('succeeded', None))
        self.assert_retained()
        self.accept()
        self.assert_finished(worker, before, attempts=2)
        self.assertEqual(self.receipts(), [])

    # After delivery: Viewer unavailable, and a lost accepted-copy acknowledgement.

    def test_viewer_unavailable_across_both_deaths_cleans_nothing_however_long(self):
        coordinator = self.spawn('coordinator')
        worker = self.spawn('worker')
        self.awaiting_copy(worker)
        before = self.identity()
        self.kill(worker)
        self.kill(coordinator)
        self.spawn('coordinator')
        self.assertEqual(self.status(), ('succeeded', None))
        worker = self.spawn('worker')
        self.lapse(WEEK)
        for _ in range(3):
            self.assert_background_ran(worker)
        self.assertEqual(self.journal()[0], 'awaiting_acceptance')
        self.assertEqual(self.job_state(), ('awaiting_acceptance', 1))
        self.assert_retained()
        self.accept()
        self.assert_finished(worker, before, attempts=1)

    def test_coordinator_killed_after_committing_a_copy_replays_the_exact_acknowledgement(self):
        coordinator = self.spawn('coordinator', COORDINATOR_DIE='after_viewer_accept')
        worker = self.spawn('worker')
        self.awaiting_copy(worker)
        before = self.identity()
        request = self.copy_request()
        with self.assertRaises((OSError, http.client.HTTPException)):
            self.viewer('POST', '/acceptance', request)
        self.assertEqual(coordinator.wait(restart.REAP_SECONDS), -9)
        self.assertEqual(self.job_state(), ('completed', 1))
        stored = self.read(self.database, 'SELECT receipt_json FROM reference_viewer_receipts')
        self.assertEqual(len(stored), 1)
        self.assertEqual(self.step(worker)['state'], 'retry_wait')
        self.assert_retained()
        self.kill(worker)
        self.spawn('coordinator')
        self.assertEqual(self.status(), ('succeeded', None))
        status, reply = self.viewer('POST', '/acceptance', request)
        self.assertEqual((status, reply['created'], reply['fullyAccepted']), (200, False, True))
        self.assertEqual(reply['viewerReceipt'], json.loads(stored[0][0]))
        self.assertEqual(self.read(self.database, 'SELECT receipt_json FROM reference_viewer_receipts'), stored)
        self.assert_retained()
        self.assert_finished(self.spawn('worker'), before, attempts=1)

    def test_worker_killed_after_a_committed_observation_replays_its_one_intent(self):
        self.spawn('coordinator')
        dying = self.spawn('worker', DELIVERY_DIE='after_observation_post')
        self.awaiting_copy(dying)
        before = self.identity()
        self.accept()
        self.dies(dying)
        self.assert_retained(verified=True)
        stage, data = self.journal()
        intent = data['acceptanceObservationRequest']
        self.assertEqual((stage, data.get('acceptanceObservationAck')), ('awaiting_acceptance', None))
        query = 'SELECT observation_id,request_sha256,request_json,ack_json FROM reference_worker_acceptance_observations'
        observed = self.read(self.database, query)
        self.assertEqual(len(observed), 1)
        worker = self.spawn('worker')
        self.lapse(BACKOFF_LAPSED)
        self.assert_finished(worker, before, attempts=1)
        self.assertEqual(self.journal()[1]['acceptanceObservationRequest'], intent)
        self.assertEqual(self.read(self.database, query), observed)


if __name__ == '__main__':
    unittest.main()
