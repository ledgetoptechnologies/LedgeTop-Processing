import copy
import json
from pathlib import Path
import tempfile
import unittest

from processing.prototype import Queue
from processing.reference_delivery import DeliveryReceiver, BINDING
from processing.reference_protocol import digest, validate_submission
from processing.reference_store import ReferenceStore
from processing.reference_worker_api import ReferenceWorkerAPI, PREFIX


WORKER = "synthetic-worker-token-0001"
OTHER = "synthetic-other-worker-token-0002"
VIEWER = "synthetic-viewer-token-0003"


class ReferenceWorkerAPITests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.queue = Queue(self.directory.name, reference_enabled=True)
        self.store = ReferenceStore(self.queue)
        self.fixture = json.loads((Path(__file__).resolve().parents[1] /
                                  "contracts/reference-v1.contract.json").read_text())
        raw = self.fixture["submission"]
        self.job = self.store.submit(validate_submission(raw, environment="staging",
            capability_fingerprint=raw["capabilityFingerprint"]))[0]
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

    def test_owned_worker_reserves_and_advances_only_permitted_phases(self):
        ready = self.request('GET', '/next')
        self.assertEqual(ready.body['job']['jobId'], self.job['jobId'])
        claim = self.claim()
        self.assertEqual(self.request('GET', '/next').body, {'job': None})
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
