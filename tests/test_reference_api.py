import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from processing import reference_protocol as p
from processing.prototype import Queue
from processing.reference_api import ReferenceAPI
from processing.reference_store import ReferenceStore, PROFILE
from tests.viewer_receipt_signer import SyntheticViewerSigner
from tests.reference_activation_fixture import prepare_reference_job, activate_reference_job


STAGING = "synthetic-staging-token-0001"
PRODUCTION = "synthetic-production-token-0002"


class ReferenceAPITests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.queue = Queue(self.directory.name)
        self.queue.allowed_profiles.add(PROFILE)
        self.store = ReferenceStore(self.queue)
        self.viewer_signer = SyntheticViewerSigner()
        self.fixture = json.loads((Path(__file__).resolve().parents[1] /
                                  "contracts/reference-v1.contract.json").read_text())
        self.api = self.make_api()

    def tearDown(self):
        self.queue.close()
        self.directory.cleanup()

    def make_api(self, **kwargs):
        viewer_keys = kwargs.pop("viewer_receipt_keys", self.viewer_signer.trusted_keys)
        return ReferenceAPI(kwargs.pop("store", self.store),
            bearer_tokens={STAGING: "staging", PRODUCTION: "production"},
            capability_fingerprint=self.fixture["capabilities"]["capabilityFingerprint"],
            options_fingerprint=self.fixture["capabilities"]["engines"]["odm"]["optionsFingerprint"],
            allowed_odm_options={"pc-quality", "projection"},
            viewer_receipt_keys=viewer_keys, **kwargs)

    def request(self, method, path, body=None, *, token=STAGING, protected=True, headers=None, api=None):
        return (api or self.api).handle(method, "/ltds/v1" + path,
            headers if headers is not None else {"Authorization": "Bearer " + token},
            b"" if body is None else json.dumps(body).encode(), protected_transport=protected)

    def legacy_job(self):
        # Seed a pre-existing legacy record directly; this is not a public admission.
        checked = p.validate_submission(self.fixture['submission'], environment='staging',
            capability_fingerprint=self.fixture['capabilities']['capabilityFingerprint'])
        return self.store.submit(checked)[0]

    def ready(self):
        checked = p.validate_submission(self.fixture['submission'], environment='staging',
            capability_fingerprint=self.fixture['capabilities']['capabilityFingerprint'])
        job, _ = prepare_reference_job(self.store, checked)
        activate_reference_job(self.store, job)
        # Only seed a historical completed job; real new execution remains held.
        ready_patch = patch.object(ReferenceStore, '_activation_bound_grant_ready', return_value=True)
        ready_patch.start()
        self.addCleanup(ready_patch.stop)
        binding = dict(node_id="cluster", worker_id="worker", worker_instance_id="instance", claim_request_id="claim-1")
        claim = self.store.claim(job["jobId"], **binding)
        self.store.reserve(job["jobId"], self.fixture["transferRequest"], node_id="cluster")
        self.store.update_phase(job["jobId"], "fetching")
        self.store.update_phase(job["jobId"], "processing")
        result = copy.deepcopy(self.fixture["resultManifest"])
        result.update(jobId=job["jobId"], pipelineDigest=job["pipelineDigest"])
        result["resultManifestSha256"] = p.digest({k: v for k, v in result.items() if k != "resultManifestSha256"})
        self.store.record_worker_result(job["jobId"], result, claim_token=claim["claimToken"], **binding)
        self.queue.complete(job["jobId"], {"claimToken": claim["claimToken"], "status": "succeeded", "result": {}})
        self.store.update_phase(job["jobId"], "awaiting_acceptance")
        # Convert the synthetic finished record to a pre-existing legacy record
        # so the real legacy result/receipt authorization path is exercised.
        with self.queue._db() as db:
            db.execute('DELETE FROM reference_viewer_activation WHERE job_id=?', (job['jobId'],))
            db.execute('DELETE FROM reference_viewer_submission_owners WHERE job_id=?', (job['jobId'],))
        return job, result, claim["claimToken"]

    def test_result_range_header_lookup_is_case_insensitive_and_rejects_duplicates(self):
        job, _result, _claim_token = self.ready()
        calls = []
        class Stream:
            status = 200
            headers = {"Content-Type": "application/zip", "Content-Length": "3"}
        class Proxy:
            def open(self, public, manifest, object_id, *, range_header=None):
                calls.append(range_header)
                return Stream()
        self.api.result_proxy = Proxy()
        route = "/ltds/v1/jobs/" + job["jobId"] + "/results/result-1"
        for name in ("Range", "range", "rAnGe"):
            response = self.api.handle("GET", route,
                {"Authorization": "Bearer " + STAGING, name: "bytes=0-9"},
                protected_transport=True)
            self.assertEqual(response.status, 200)
        self.assertEqual(calls, ["bytes=0-9"] * 3)
        duplicate = self.api.handle("GET", route,
            {"Authorization": "Bearer " + STAGING, "Range": "bytes=0-9",
             "range": "bytes=10-19"}, protected_transport=True)
        self.assertEqual((duplicate.status, duplicate.body), (400, {"error": "duplicate_header"}))
        self.assertEqual(calls, ["bytes=0-9"] * 3)

    def test_transport_auth_and_capabilities(self):
        self.assertEqual(self.request("GET", "/capabilities", protected=False).status, 503)
        spoofed = {"Authorization": "Bearer " + STAGING, "X-Forwarded-Proto": "https"}
        self.assertEqual(self.request("GET", "/capabilities", protected=False, headers=spoofed).status, 503)
        for headers in ({}, {"Authorization": "Bearer wrong"},
                        {"Authorization": "Bearer " + STAGING, "authorization": "Bearer " + PRODUCTION}):
            self.assertEqual(self.request("GET", "/capabilities", headers=headers).status, 401)
        response = self.request("GET", "/capabilities")
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body, self.fixture["capabilities"])
        self.assertEqual(response.headers["Cache-Control"], "no-store")

    def test_submission_idempotency_environment_scoping_and_no_private_data(self):
        job = self.legacy_job()
        replay = self.request("POST", "/jobs", self.fixture["submission"])
        self.assertEqual((replay.status, replay.body), (503, {'error': 'viewer_prepared_submission_required'}))
        path = "/jobs/" + job["jobId"]
        self.assertEqual(self.request("GET", path).body, job)
        for suffix, method in (("", "GET"), ("/results", "GET"), ("/acceptance", "POST")):
            response = self.request(method, path + suffix, {}, token=PRODUCTION)
            self.assertEqual((response.status, response.body), (404, {"error": "not_found"}))
        self.assertEqual(self.request("GET", "/jobs/unknown").status, 404)
        self.assertEqual(self.request("GET", path + "/results").status, 409)
        encoded = json.dumps(job)
        for private in ("claimToken", "submission", "input_json", STAGING, PRODUCTION):
            self.assertNotIn(private, encoded)

    def test_bounded_duplicate_decode_and_safe_errors(self):
        for body in (b'{"schemaVersion":1,"schemaVersion":1}', b"{" * 100,
                     b" " * (p.MAX_BODY_BYTES + 1)):
            response = self.api.handle("POST", "/ltds/v1/jobs", {"Authorization": "Bearer " + STAGING},
                                       body, protected_transport=True)
            self.assertEqual((response.status, response.body), (400, {"error": "invalid_request"}))
        payload = copy.deepcopy(self.fixture["submission"])
        payload["environment"] = "production"
        self.assertEqual(self.request("POST", "/jobs", payload).status, 400)

    def test_disabled_pipelines_stale_capabilities_and_options(self):
        for mutate in (lambda body: body["pipeline"]["gaussian_splat"].update(requested=True),
                       lambda body: body["pipeline"]["treedar"].update(requested=True),
                       lambda body: body.update(capabilityFingerprint="0" * 64),
                       lambda body: body["pipeline"]["odm"].update(options={"unknown-option": True}),
                       lambda body: body["pipeline"]["odm"].update(options={"projection": "https://secret/path"})):
            body = copy.deepcopy(self.fixture["submission"])
            mutate(body)
            self.assertEqual(self.request("POST", "/jobs", body).status, 400)
        self.assertEqual(self.store.list(), [])

    def test_admission_hold_precedes_quota_and_is_atomic(self):
        api = self.make_api(store=ReferenceStore(self.queue, max_metadata_bytes=1))
        response = self.request("POST", "/jobs", self.fixture["submission"], api=api)
        self.assertEqual((response.status, response.body), (503, {"error": "viewer_prepared_submission_required"}))
        self.assertEqual(self.queue.list(), [])

    def test_unexpected_internal_error_never_returns_exception_or_credentials(self):
        with patch.object(p, "validate_submission", side_effect=RuntimeError(STAGING + " C:/private/path")):
            response = self.request("POST", "/jobs", self.fixture["submission"])
        self.assertEqual((response.status, response.body), (500, {"error": "internal_error"}))
        self.assertNotIn(STAGING, json.dumps(response.body))

    def test_result_and_partial_acceptance_routes_and_worker_privacy(self):
        job, result, claim_token = self.ready()
        path = "/jobs/" + job["jobId"]
        self.assertEqual(self.request("GET", path + "/results").body, result)
        receipt = copy.deepcopy(self.fixture["acceptance"])
        receipt.update(jobId=job["jobId"], pipelineDigest=job["pipelineDigest"],
                       resultManifestSha256=result["resultManifestSha256"])
        accepted = self.request("POST", path + "/acceptance", receipt)
        self.assertEqual(accepted.status, 200)
        self.assertFalse(accepted.body["fullyAccepted"])
        self.assertFalse(self.request("POST", path + "/acceptance", receipt).body["created"])
        receipt["acceptedAt"] = "2026-10-28T12:00:00Z"
        self.assertEqual(self.request("POST", path + "/acceptance", receipt).status, 409)
        combined = json.dumps([self.request("GET", path).body, accepted.body,
                               self.request("GET", path + "/results").body])
        for secret in (claim_token, STAGING, PRODUCTION, "claimToken", "executionPhase", "submission"):
            self.assertNotIn(secret, combined)

    def test_signed_viewer_receipt_is_persisted_atomically_and_relayed_after_store_reopen(self):
        job, result, _claim_token = self.ready()
        acceptance = copy.deepcopy(self.fixture["acceptance"])
        acceptance.update(jobId=job["jobId"], pipelineDigest=job["pipelineDigest"],
            resultManifestSha256=result["resultManifestSha256"])
        signed = self.viewer_signer.receipt(acceptance, result)
        route = "/jobs/" + job["jobId"] + "/acceptance"
        response = self.request("POST", route, {"acceptance": acceptance, "viewerReceipt": signed})
        self.assertEqual(response.status, 200)
        self.assertFalse(response.body["fullyAccepted"])
        reopened = ReferenceStore(self.queue)
        relayed = reopened.acceptance_receipts(job["jobId"])
        self.assertEqual(relayed["receipts"], [{"acceptance": acceptance, "viewerReceipt": signed}])
        replay = self.request("POST", route, {"acceptance": acceptance, "viewerReceipt": signed})
        self.assertEqual(replay.status, 200)
        self.assertFalse(replay.body["created"])

    def test_signed_viewer_receipt_tamper_or_missing_key_fails_closed(self):
        job, result, _claim_token = self.ready()
        acceptance = copy.deepcopy(self.fixture["acceptance"])
        acceptance.update(jobId=job["jobId"], pipelineDigest=job["pipelineDigest"],
            resultManifestSha256=result["resultManifestSha256"])
        signed = self.viewer_signer.receipt(acceptance, result)
        signed["signature"] = ("A" if signed["signature"][0] != "A" else "B") + signed["signature"][1:]
        response = self.request("POST", "/jobs/" + job["jobId"] + "/acceptance",
            {"acceptance": acceptance, "viewerReceipt": signed})
        self.assertEqual(response.status, 409)
        self.assertEqual(self.store.acceptance_receipts(job["jobId"])["receipts"], [])
        unpinned = self.make_api(viewer_receipt_keys={})
        response = self.request("POST", "/jobs/" + job["jobId"] + "/acceptance",
            {"acceptance": acceptance, "viewerReceipt": self.viewer_signer.receipt(acceptance, result)}, api=unpinned)
        self.assertEqual(response.status, 409)
        self.assertEqual(self.store.acceptance_receipts(job["jobId"])["receipts"], [])

    def test_explicit_treedar_availability_does_not_prove_file_contents(self):
        api = self.make_api(treedar_available=True)
        self.assertTrue(self.request("GET", "/capabilities", api=api).body["engines"]["treedar"]["available"])
        body = copy.deepcopy(self.fixture["submission"])
        body["manifest"]["objects"][0].update(kind="lidar", name="cloud.las")
        body["manifest"]["inputCounts"].update(images=0, lidarObjects=1)
        body["manifest"]["sha256"] = p.manifest_sha256(body["manifest"])
        body["pipeline"]["treedar"].update(requested=True, inputObjectIds=["image-1"], provenanceReceiptId="provenance-1")
        self.assertEqual(self.request("POST", "/jobs", body).status, 400)
        self.assertEqual(self.request("POST", "/jobs", body, api=api).status, 503)


if __name__ == "__main__":
    unittest.main()
