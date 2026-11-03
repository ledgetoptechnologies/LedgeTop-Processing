import copy
from contextlib import nullcontext
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
import uuid
from unittest.mock import Mock

from processing import reference_protocol as p
from processing.maintenance import Conflict
from processing.prototype import Queue, PROFILE as LEGACY_PROFILE
from processing.reference_store import ReferenceStore, PROFILE
from processing.reference_worker_api import ReferenceWorkerAPI, PREFIX
from processing.reference_driver import ReferenceDriver, DriverHeld
from processing.reference_engine_worker import WorkerHeld


class ViewerActivationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.queue = Queue(self.temp.name, node_ids=('cluster', 'node-2'))
        self.addCleanup(self.queue.close)
        self.queue.allowed_profiles.add(PROFILE)
        self.store = ReferenceStore(self.queue)
        self.fixture = json.loads((Path(__file__).resolve().parents[1] /
                                   'contracts/reference-v1.contract.json').read_text())

    def submit(self, environment='production'):
        raw = copy.deepcopy(self.fixture['submission'])
        raw['submissionId'] = str(uuid.uuid4())
        checked = p.validate_submission(raw, environment=environment,
            capability_fingerprint=raw['capabilityFingerprint'])
        job, _ = self.store.submit(checked)
        binding = dict(schemaVersion=1, protocol='ltds-viewer-activation-v1',
            activationId=str(uuid.uuid4()), principalId='viewer-prod', submissionId=raw['submissionId'],
            jobId=job['jobId'], generation=job['generation'], environment=environment,
            viewerTaskId=str(uuid.uuid4()), viewerAttemptId=str(uuid.uuid4()),
            requestDigest=checked['requestDigest'], manifestSha256=job['manifestSha256'],
            pipelineDigest=job['pipelineDigest'])
        return job, binding

    def claim(self, job, request='claim-one'):
        return self.store.claim(job['jobId'], node_id='cluster', worker_id='worker',
            worker_instance_id='instance', claim_request_id=request)

    def activate(self, binding):
        return self.store.activate_viewer_binding(binding, authenticated_principal='viewer-prod',
                                                  authenticated_environment=binding['environment'])

    def test_queued_job_never_discovered_or_claimed_before_activation(self):
        job, binding = self.submit()
        self.assertIsNone(self.store.next_for_node('cluster'))
        self.assertIsNone(self.claim(job))
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_claims').fetchone()[0], 0)
        acknowledgement = self.activate(binding)
        self.assertEqual(self.store.next_for_node('cluster')['jobId'], job['jobId'])
        first_claim = self.claim(job)
        self.assertEqual(first_claim['status'], 'running')
        self.assertEqual(self.activate(binding), acknowledgement)
        self.assertEqual(self.claim(job)['claimToken'], first_claim['claimToken'])

    def test_unactivated_production_head_blocks_activated_staging(self):
        prod, prod_binding = self.submit('production')
        staging, staging_binding = self.submit('staging')
        self.activate(staging_binding)
        self.assertIsNone(self.store.next_for_node('cluster'))
        self.assertIsNone(self.claim(staging, 'claim-staging'))
        self.assertIsNone(self.queue.claim({'workerId': 'legacy', 'nodeId': 'cluster', 'profiles': [LEGACY_PROFILE]}))
        self.activate(prod_binding)
        self.assertEqual(self.store.next_for_node('cluster')['jobId'], prod['jobId'])

    def test_worker_http_boundary_refuses_unactivated_job(self):
        job, binding = self.submit()
        token = 'synthetic-worker-token-0001'
        worker = ReferenceWorkerAPI(self.store, None, bearer_workers={token: dict(
            node_id='cluster', worker_id='worker', worker_instance_id='instance')})
        headers = {'Authorization': 'Bearer ' + token}
        next_reply = worker.handle('GET', PREFIX + '/next', headers, protected_transport=True)
        self.assertEqual(next_reply.status, 200)
        self.assertIsNone(next_reply.body['job'])
        payload = json.dumps({'jobId': job['jobId'], 'claimRequestId': 'direct-claim'}).encode()
        denied = worker.handle('POST', PREFIX + '/claim', headers, payload, protected_transport=True)
        self.assertEqual(denied.status, 409)
        self.activate(binding)
        allowed = worker.handle('POST', PREFIX + '/claim', headers, payload, protected_transport=True)
        self.assertEqual(allowed.status, 200)
        self.assertEqual(allowed.body['jobId'], job['jobId'])

    def test_replay_after_restart_exact_and_conflict_rejected(self):
        job, binding = self.submit()
        first = self.activate(binding)
        self.queue.close()
        self.queue = Queue(self.temp.name, node_ids=('cluster', 'node-2'))
        self.addCleanup(self.queue.close)
        self.queue.allowed_profiles.add(PROFILE)
        reopened = ReferenceStore(self.queue)
        self.assertEqual(first, reopened.activate_viewer_binding(binding, authenticated_principal='viewer-prod', authenticated_environment='production'))
        self.assertEqual(reopened.next_for_node('cluster')['jobId'], job['jobId'])
        for field, value in [('activationId', str(uuid.uuid4())), ('viewerTaskId', str(uuid.uuid4())),
                             ('viewerAttemptId', str(uuid.uuid4())), ('requestDigest', 'f' * 64),
                             ('environment', 'staging')]:
            with self.subTest(field=field), self.assertRaises(Conflict):
                reopened.activate_viewer_binding(dict(binding, **{field: value}), authenticated_principal='viewer-prod', authenticated_environment='production')
        with self.assertRaises(Conflict):
            reopened.activate_viewer_binding(binding, authenticated_principal='other-viewer', authenticated_environment='production')
        with self.assertRaises(Conflict):
            reopened.activate_viewer_binding(binding, authenticated_principal='viewer-prod', authenticated_environment='staging')

    def test_concurrent_exact_and_conflicting_first_writes(self):
        job, binding = self.submit()
        other = dict(binding, viewerAttemptId=str(uuid.uuid4()), activationId=str(uuid.uuid4()))
        def attempt(candidate):
            try:
                return self.activate(candidate)
            except Conflict:
                return None
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(attempt, [binding, binding, other, other]))
        wins = [r for r in results if r is not None]
        self.assertEqual(len(wins), 2)
        self.assertEqual(wins[0], wins[1])
        self.assertEqual(self.store.next_for_node('cluster')['jobId'], job['jobId'])

    def test_activation_and_claim_race_never_claims_without_binding(self):
        job, binding = self.submit()
        with ThreadPoolExecutor(max_workers=2) as pool:
            activation = pool.submit(self.activate, binding)
            claim = pool.submit(self.claim, job, 'race-claim')
            activation.result()
            result = claim.result()
        if result is None:
            result = self.claim(job, 'race-claim')
        self.assertEqual(result['status'], 'running')
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_viewer_activation WHERE job_id=?',
                                        (job['jobId'],)).fetchone()[0], 1)

    def test_queue_environment_mismatch_cannot_activate_or_claim(self):
        job, binding = self.submit('production')
        with self.queue._db() as db:
            db.execute("UPDATE jobs SET environment='staging' WHERE id=?", (job['jobId'],))
        with self.assertRaises(Conflict):
            self.activate(binding)
        with self.queue._db() as db:
            db.execute("UPDATE jobs SET environment='production' WHERE id=?", (job['jobId'],))
        self.activate(binding)
        with self.queue._db() as db:
            db.execute("UPDATE jobs SET environment='staging' WHERE id=?", (job['jobId'],))
        with self.assertRaises(Conflict):
            self.claim(job)

    def test_unbound_legacy_running_claim_cannot_replay_reserve_or_fetch(self):
        job, binding = self.submit()
        token = 'synthetic-claim-token'
        with self.queue._db() as db:
            db.execute("UPDATE jobs SET status='running',worker_id='worker',node_id='cluster',claim_hash=? WHERE id=?",
                       (hashlib.sha256(token.encode()).hexdigest(), job['jobId']))
            db.execute('UPDATE reference_jobs SET node_id=? WHERE job_id=?', ('cluster', job['jobId']))
            db.execute('INSERT INTO reference_claims(request_id,job_id,node_id,worker_id,instance_id,token) VALUES(?,?,?,?,?,?)',
                       ('legacy-claim', job['jobId'], 'cluster', 'worker', 'instance', token))
        self.assertIsNone(self.claim(job, 'legacy-claim'))
        with self.assertRaises(Conflict):
            self.activate(binding)
        with self.assertRaises(Conflict):
            self.store.claim_snapshot(job['jobId'], node_id='cluster', worker_id='worker',
                                      worker_instance_id='instance', claim_token=token)
        with self.assertRaises(Conflict):
            self.store.reserve(job['jobId'], self.fixture['transferRequest'], node_id='cluster')
        with self.assertRaises(Conflict):
            self.store.receive_transfer_grant(job['jobId'], {})
        with self.assertRaises(Conflict):
            self.store.private_transfer_grant(job['jobId'])
        worker = ReferenceWorkerAPI(self.store, None, bearer_workers={'x' * 20: dict(
            node_id='cluster', worker_id='worker', worker_instance_id='instance')})
        with self.assertRaises(Conflict):
            worker._owned_job(job['jobId'], dict(node_id='cluster', worker_id='worker',
                                               worker_instance_id='instance'), token)
        headers = {'Authorization': 'Bearer ' + 'x' * 20, 'X-Claim-Token': token}
        prefix = PREFIX + '/jobs/' + job['jobId']
        self.assertEqual(worker.handle('GET', prefix + '/claim-state', headers, protected_transport=True).status, 409)
        self.assertEqual(worker.handle('GET', prefix + '/transfer-grant', headers, protected_transport=True).status, 409)
        payload = json.dumps({'claimToken': token, 'transferRequest': self.fixture['transferRequest']}).encode()
        self.assertEqual(worker.handle('POST', prefix + '/reservation', headers, payload, protected_transport=True).status, 409)

    def test_mismatched_or_corrupt_binding_fails_closed(self):
        job, binding = self.submit()
        with self.assertRaises(Conflict):
            self.activate(dict(binding, manifestSha256='f' * 64))
        self.activate(binding)
        with self.queue._db() as db:
            db.execute("UPDATE reference_viewer_activation SET binding_sha256=? WHERE job_id=?", ('f' * 64, job['jobId']))
        with self.assertRaises(Conflict):
            self.store.next_for_node('cluster')
        with self.assertRaises(Conflict):
            self.claim(job)

    def test_activation_id_and_attempt_cannot_be_reused_for_another_job(self):
        _, first = self.submit()
        _, second = self.submit()
        self.activate(first)
        for conflicting in (dict(second, activationId=first['activationId']),
                            dict(second, viewerAttemptId=first['viewerAttemptId'])):
            with self.assertRaises(Conflict):
                self.activate(conflicting)

    def test_quota_refusal_does_not_create_activation(self):
        job, binding = self.submit()
        self.store.max_metadata_bytes = 1
        with self.assertRaises(Conflict):
            self.activate(binding)
        self.assertIsNone(self.claim(job))
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_viewer_activation').fetchone()[0], 0)

    def test_persisted_v1_grant_never_starts_fetch_or_new_execution(self):
        # The old grant format cannot bind activation/authority/epoch. This
        # directly exercises persisted driver stages without opening a broker.
        for stage, kind in [('awaiting_grant', 'odm'), ('fetching_pending', 'odm'),
                            ('fetching', 'odm'), ('processing_pending', 'odm'),
                            ('execution_pending', 'odm'), ('combined_prepared_held', 'combined')]:
            with self.subTest(stage=stage):
                row = {'job': str(uuid.uuid4()), 'claim_request': 'old-claim', 'stage': stage,
                       'data': json.dumps({'claim': {'claimToken': 'old-token'}})}
                db = Mock()
                db.execute.return_value.fetchone.return_value = row
                driver = object.__new__(ReferenceDriver)
                driver._db = lambda: nullcontext(db)
                driver._kind = lambda _job: kind
                driver._request = Mock()
                driver.broker = Mock()
                adapter = Mock()
                adapter._row.side_effect = WorkerHeld('no worker execution')
                driver._adapter = Mock(return_value=adapter)
                with self.assertRaisesRegex(DriverHeld, 'activation_bound_grant_unavailable'):
                    driver._step()
                driver._request.assert_not_called()
                driver.broker.download_object.assert_not_called()
                adapter.start.assert_not_called()

    def test_already_running_engine_can_still_be_observed(self):
        job_id = str(uuid.uuid4())
        row = {'job': job_id, 'claim_request': 'old-claim', 'stage': 'executing',
               'data': json.dumps({'claim': {'claimToken': 'old-token'}})}
        db = Mock()
        db.execute.return_value.fetchone.return_value = row
        driver = object.__new__(ReferenceDriver)
        driver._db = lambda: nullcontext(db)
        driver._kind = lambda _job: 'odm'
        driver._request = Mock()
        driver.worker = Mock()
        driver.worker.engine.read.return_value = {'uuid': job_id, 'status': {'code': 20}}
        adapter = Mock()
        adapter._row.return_value = {'phase': 'processing'}
        adapter.poll.return_value = None
        driver._adapter = Mock(return_value=adapter)
        self.assertEqual(driver._step(), {'state': 'executing', 'jobId': job_id})
        driver._request.assert_not_called()
        adapter.start.assert_not_called()
        adapter.poll.assert_called_once_with(job_id)


if __name__ == '__main__':
    unittest.main()
