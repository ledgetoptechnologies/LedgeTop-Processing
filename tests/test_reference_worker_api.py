import copy
import json
from pathlib import Path
import tempfile
import unittest
import uuid
from unittest.mock import patch

from processing.prototype import Queue
from processing.reference_delivery import DeliveryReceiver, BINDING
from processing.reference_protocol import digest, validate_submission
from processing.reference_store import ReferenceStore
from processing.reference_worker_api import ReferenceWorkerAPI, PREFIX
from processing.reference_failure import FailureOutbox, make_failure
from tests.reference_activation_fixture import activate_reference_job, prepare_reference_job


WORKER = "synthetic-worker-token-0001"
OTHER = "synthetic-other-worker-token-0002"
VIEWER = "synthetic-viewer-token-0003"


class ReferenceWorkerAPITests(unittest.TestCase):
    def setUp(self):
        # Only exercises post-activation mechanics with a synthetic future grant.
        self.ready_patch = patch.object(ReferenceStore, '_activation_bound_grant_ready', return_value=True)
        self.ready_patch.start()
        self.addCleanup(self.ready_patch.stop)
        self.directory = tempfile.TemporaryDirectory()
        self.queue = Queue(self.directory.name, reference_enabled=True)
        self.store = ReferenceStore(self.queue)
        self.fixture = json.loads((Path(__file__).resolve().parents[1] /
                                  "contracts/reference-v1.contract.json").read_text())
        raw = self.fixture["submission"]
        self.job = prepare_reference_job(self.store, validate_submission(raw, environment="staging",
            capability_fingerprint=raw["capabilityFingerprint"]))[0]
        activate_reference_job(self.store, self.job)
        self.identities = {
            WORKER: dict(node_id="cluster", worker_id="worker", worker_instance_id="instance-1"),
            OTHER: dict(node_id="cluster", worker_id="other-worker", worker_instance_id="instance-2")}
        self.api = ReferenceWorkerAPI(self.store, DeliveryReceiver(self.store), bearer_workers=self.identities)

    def tearDown(self):
        self.queue.close()
        self.directory.cleanup()

    def request(self, method, path, body=None, *, token=WORKER, claim_token=None, protected=True, headers=None):
        headers = dict(headers) if headers is not None else {"Authorization": "Bearer " + token}
        if claim_token is not None:
            headers["X-Claim-Token"] = claim_token
        return self.api.handle(method, PREFIX + path, headers,
            b"" if body is None else json.dumps(body).encode(), protected_transport=protected)

    def claim(self):
        response = self.request("POST", "/claim", {"jobId": self.job["jobId"], "claimRequestId": "claim-1"})
        self.assertEqual(response.status, 200)
        return response.body

    def processing_result(self):
        claim = self.claim()
        self.store.reserve(self.job["jobId"], self.fixture["transferRequest"], node_id="cluster")
        self.store.update_phase(self.job["jobId"], "fetching")
        self.store.update_phase(self.job["jobId"], "processing")
        result = copy.deepcopy(self.fixture["resultManifest"])
        result.update(jobId=self.job["jobId"], pipelineDigest=self.job["pipelineDigest"])
        result["objects"][0]["role"] = "odm_archive"
        result["resultManifestSha256"] = digest({k: v for k, v in result.items() if k != "resultManifestSha256"})
        payload = dict(jobId=self.job["jobId"], generation=1, environment="staging",
            inputManifestSha256=self.job["manifestSha256"], pipelineDigest=self.job["pipelineDigest"],
            resultManifestSha256=result["resultManifestSha256"], resultManifest=result, terminalState="succeeded")
        delivery = dict(outboxId=digest({k: payload[k] for k in BINDING}),
                        payloadSha256=digest(payload), payload=payload)
        return {"claimRequestId": "claim-1", "claimToken": claim["claimToken"], "delivery": delivery}

    def restart(self):
        self.queue.close()
        self.queue = Queue(self.directory.name, reference_enabled=True)
        self.store = ReferenceStore(self.queue)
        self.api = ReferenceWorkerAPI(self.store, DeliveryReceiver(self.store), bearer_workers=self.identities)

    def test_private_auth_transport_and_fixed_identity_claim_replay(self):
        self.assertEqual(self.request('GET','/authority').body, {'authorityId':self.queue.authority_id})
        self.assertEqual(self.request('GET','/authority',token=VIEWER).status,401)
        payload = {"jobId": self.job["jobId"], "claimRequestId": "claim-1"}
        self.assertEqual(self.request("POST", "/claim", payload, protected=False).status, 503)
        self.assertEqual(self.request("POST", "/claim", payload, token=VIEWER).status, 401)
        self.assertEqual(self.request("POST", "/claim", payload, headers={}).status, 401)
        claim = self.claim()
        self.assertIn("submission", claim)
        self.assertIn("claimToken", claim)
        self.assertEqual(self.request("POST", "/claim", payload).body["claimToken"], claim["claimToken"])
        self.assertEqual(self.request("POST", "/claim", payload, token=OTHER).status, 409)
        for mutation in ({"nodeId": "arbitrary"}, {"environment": "production"},
                         {"workerInstanceId": "spoofed"}, {"localPath": "C:/private/input"}):
            self.assertEqual(self.request("POST", "/claim", {**payload, **mutation}).status, 400)
        self.assertNotIn(WORKER, json.dumps(claim))

    def test_terminal_acceptance_relay_requires_exact_worker_claim(self):
        claim = self.claim()
        failure = make_failure(job_id=self.job['jobId'],generation=1,environment='staging',
            input_manifest_sha256=self.job['manifestSha256'],pipeline_digest=self.job['pipelineDigest'],
            terminal_state='failed',error_code='engine_failed')
        outbox = FailureOutbox(Path(self.directory.name)/'failure.sqlite')
        outbox.persist(failure,engine_terminal=True)
        delivery = outbox.claim_next()
        envelope = {key:delivery[key] for key in ('outboxId','payloadSha256','payload')}
        self.assertEqual(self.request('POST','/terminal-outcomes',dict(claimRequestId='claim-1',
            claimToken=claim['claimToken'],delivery=envelope)).status,200)
        route = '/jobs/'+self.job['jobId']+'/terminal-acceptance'
        self.assertEqual(self.request('GET',route,claim_token=claim['claimToken']).body,
                         {'receipt':None,'terminalAccepted':False})
        receipt = {key:failure[key] for key in ('jobId','generation','environment','inputManifestSha256','pipelineDigest','failureSha256')}
        receipt.update(schemaVersion=1,receiptId='terminal-ack',storageReceiptId='terminal-storage',
            acceptedAt='2026-10-27T00:00:00.000Z',validationVersion='ltds-reference-v1')
        self.api.terminal.accept(self.job['jobId'],receipt,'staging')
        self.assertEqual(self.request('GET',route,claim_token=claim['claimToken']).body,
                         {'receipt':receipt,'terminalAccepted':True})
        self.assertEqual(self.request('GET',route,claim_token=claim['claimToken'],token=OTHER).status,409)

    def test_owned_worker_reserves_and_advances_only_permitted_phases(self):
        ready = self.request('GET', '/next')
        self.assertEqual(ready.body['job']['jobId'], self.job['jobId'])
        claim = self.claim()
        self.assertEqual(self.request('GET', '/next').body, {'job': None, 'execution': None})
        path = '/jobs/' + self.job['jobId']
        body = dict(claimToken=claim['claimToken'], transferRequest=self.fixture['transferRequest'])
        self.assertEqual(self.request('POST', path + '/reservation', body, token=OTHER).status, 409)
        reserved = self.request('POST', path + '/reservation', body)
        self.assertEqual(reserved.status, 200)
        self.assertEqual(reserved.body['phase'], 'awaiting_transfer')
        for phase in ('fetching', 'processing'):
            response = self.request('POST', path + '/phase', dict(claimToken=claim['claimToken'], phase=phase))
            self.assertEqual(response.status, 200)
            self.assertEqual(response.body['phase'], phase)
        for phase in ('completed', 'cancelled', 'held'):
            self.assertEqual(self.request('POST', path + '/phase',
                dict(claimToken=claim['claimToken'], phase=phase)).status, 400)

    def test_next_execution_is_private_advice_and_preserves_public_job(self):
        before = self.store.get(self.job['jobId'])
        response = self.request('GET', '/next')
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body['job'], before)
        self.assertEqual(response.body['execution'], dict(kind='odm', **{key: before[key] for key in
            ('jobId', 'generation', 'environment', 'manifestSha256', 'pipelineDigest')}))
        self.assertEqual(self.store.get(self.job['jobId']), before)
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_claims').fetchone()[0], 0)
        serialized = json.dumps(response.body)
        for private in ('submission', 'claimToken', 'provenanceReceiptId', WORKER, VIEWER):
            self.assertNotIn(private, serialized)
        self.assertEqual(self.request('GET', '/next', token=VIEWER).status, 401)
        self.assertEqual(self.request('GET', '/next', protected=False).status, 503)

    def test_next_changed_head_requests_retry_instead_of_misbinding_hint(self):
        original = self.store.next_for_node
        def changed(node_id):
            job = original(node_id)
            with self.queue._db() as db:
                db.execute("UPDATE jobs SET status='cancelled' WHERE id=?", (job['jobId'],))
            return job
        with patch.object(self.store, 'next_for_node', changed):
            response = self.request('GET', '/next')
        self.assertEqual(response.status, 409)
        self.assertEqual(response.body, {'error': 'worker_conflict'})

    def pipeline_submission(self, kind):
        raw = copy.deepcopy(self.fixture['submission'])
        raw['submissionId'] = str(uuid.uuid4())
        if kind in ('treedar', 'combined'):
            obj = raw['manifest']['objects'][0]
            obj.update(kind='lidar', name='synthetic.las', mediaType='application/octet-stream')
            raw['manifest']['inputCounts'].update(images=0, lidarObjects=1)
            raw['pipeline']['treedar'].update(requested=True, inputObjectIds=[obj['id']],
                                              provenanceReceiptId='synthetic-proof')
            raw['pipeline']['odm']['requested'] = kind == 'combined'
            raw['manifest']['sha256'] = digest({k: v for k, v in raw['manifest'].items() if k != 'sha256'})
        return raw

    def test_next_kinds_follow_stored_pipeline_and_fixed_node_global_head(self):
        for kind in ('odm', 'treedar', 'combined'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                queue = Queue(directory, reference_enabled=True, node_ids=('cluster', 'main'))
                try:
                    store = ReferenceStore(queue)
                    raw = self.pipeline_submission(kind)
                    job = prepare_reference_job(store, validate_submission(raw, environment='staging',
                        capability_fingerprint=raw['capabilityFingerprint'], available_engines=('odm', 'treedar')))[0]
                    activate_reference_job(store, job)
                    api = ReferenceWorkerAPI(store, DeliveryReceiver(store), bearer_workers={
                        WORKER: dict(node_id='cluster', worker_id='worker', worker_instance_id='cluster-1'),
                        OTHER: dict(node_id='main', worker_id='worker', worker_instance_id='main-1')})
                    node_token = WORKER if kind == 'odm' else OTHER
                    def advice(token):
                        return api.handle('GET', PREFIX+'/next', {'Authorization':'Bearer '+token}, protected_transport=True)
                    response = advice(node_token)
                    self.assertEqual(response.status, 200)
                    self.assertEqual(response.body['execution']['kind'], kind)
                    self.assertEqual(response.body['job'], job)
                    other_token = OTHER if node_token == WORKER else WORKER
                    self.assertEqual(advice(other_token).body, {'job':None, 'execution':None})
                    # A production small job takes global priority over the
                    # earlier staging main-only job, without claiming either.
                    if kind != 'odm':
                        small = self.pipeline_submission('odm')
                        production = prepare_reference_job(store, validate_submission(small, environment='production',
                            capability_fingerprint=small['capabilityFingerprint']))[0]
                        activate_reference_job(store, production)
                        self.assertEqual(advice(WORKER).body['job'], production)
                        self.assertEqual(advice(OTHER).body, {'job':None, 'execution':None})
                        self.assertEqual(store.get(job['jobId'])['status'], 'queued')
                    with queue._db() as db:
                        self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_claims').fetchone()[0], 0)
                finally:
                    queue.close()

    def test_worker_can_relay_exact_partial_viewer_receipt_after_delivery(self):
        delivery = self.processing_result()
        self.assertEqual(self.request('POST', '/results', delivery).status, 200)
        result = delivery['delivery']['payload']['resultManifest']
        receipt = copy.deepcopy(self.fixture['acceptance'])
        receipt.update(jobId=self.job['jobId'], inputManifestSha256=self.job['manifestSha256'],
            pipelineDigest=self.job['pipelineDigest'], resultManifestSha256=result['resultManifestSha256'])
        self.store.accept(self.job['jobId'], receipt)
        route = '/jobs/' + self.job['jobId'] + '/acceptance'
        self.assertEqual(self.request('GET', route, claim_token='wrong').status, 409)
        self.assertEqual(self.request('GET', route, token=OTHER, claim_token=delivery['claimToken']).status, 409)
        response = self.request('GET', route, claim_token=delivery['claimToken'])
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body, {'receipts': [receipt], 'fullyAccepted': False})

    def test_grant_only_returns_for_exact_persisted_claim(self):
        claim = self.claim()
        path = "/jobs/" + self.job["jobId"] + "/transfer-grant"
        self.store.reserve(self.job["jobId"], self.fixture["transferRequest"], node_id="cluster")
        grant = dict(self.fixture["transferRequest"], jobId=self.job["jobId"], environment="staging",
                     objectIds=[obj["id"] for obj in self.fixture["submission"]["manifest"]["objects"]],
                     token="synthetic-grant-token")
        self.store.receive_transfer_grant(self.job["jobId"], grant)
        self.assertEqual(self.request("GET", path).status, 401)
        self.assertEqual(self.request("GET", path, claim_token="wrong").status, 409)
        self.assertEqual(self.request("GET", path, token=OTHER, claim_token=claim["claimToken"]).status, 409)
        returned = self.request("GET", path, claim_token=claim["claimToken"])
        self.assertEqual((returned.status, returned.body), (200, grant))
        self.restart()
        self.assertEqual(self.request("GET", path, claim_token=claim["claimToken"]).status, 409)

    def test_restart_worker_result_durable_metadata_receipt_and_exact_replay(self):
        body = self.processing_result()
        self.restart()
        replay = self.claim()
        self.assertEqual((replay["status"], replay["executionPhase"]), ("held", "processing"))
        self.assertEqual(replay["claimToken"], body["claimToken"])
        response = self.request("POST", "/results", body)
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body["type"], "durable-result-receipt")
        self.assertTrue(response.body["durable"])
        self.assertEqual(self.store.get(self.job["jobId"])["phase"], "awaiting_acceptance")
        self.assertEqual(self.queue.get(self.job["jobId"])["status"], "succeeded")
        self.restart()
        self.assertEqual(self.request("POST", "/results", body).body, response.body)
        for private in (body["claimToken"], WORKER, "local_objects", "owned_prefix", "C:/"):
            self.assertNotIn(private, json.dumps(response.body))

    def test_wrong_claim_and_caller_paths_cannot_release_worker_slot(self):
        body = self.processing_result()
        for mutation in ({"claimToken": "wrong"}, {"claimRequestId": "wrong-request"}):
            self.assertEqual(self.request("POST", "/results", {**body, **mutation}).status, 409)
        self.assertEqual(self.request("POST", "/results", body, token=OTHER).status, 409)
        modified = copy.deepcopy(body)
        modified["delivery"]["payload"]["localPath"] = "C:/private/result"
        self.assertEqual(self.request("POST", "/results", modified).status, 400)
        self.assertEqual(self.queue.get(self.job["jobId"])["status"], "running")

    def test_duplicate_auth_header_and_bad_claim_are_safe(self):
        response = self.request("POST", "/claim", {}, headers={
            "Authorization": "Bearer " + WORKER, "authorization": "Bearer " + OTHER})
        self.assertEqual(response.status, 401)
        response = self.request("POST", "/claim", {"jobId": "../secret", "claimRequestId": "claim"})
        self.assertEqual((response.status, response.body), (400, {"error": "invalid_request"}))


if __name__ == "__main__":
    unittest.main()
