import copy
import hashlib
import http.client
import json
from pathlib import Path
import tempfile
import threading
import unittest
import uuid
from unittest.mock import patch

from processing import reference_protocol as p
from processing.prototype import Queue
from processing.reference_api import ReferenceAPI
from processing.reference_store import ReferenceStore, PROFILE
from processing.reference_http import create_reference_http_server


STAGING = "synthetic-staging-token-0001"
PRODUCTION = "synthetic-production-token-0002"


class ReferenceAPITests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.queue = Queue(self.directory.name)
        self.queue.allowed_profiles.add(PROFILE)
        self.store = ReferenceStore(self.queue)
        self.fixture = json.loads((Path(__file__).resolve().parents[1] /
                                  "contracts/reference-v1.contract.json").read_text())
        self.api = self.make_api()

    def tearDown(self):
        self.queue.close()
        self.directory.cleanup()

    def make_api(self, **kwargs):
        return ReferenceAPI(kwargs.pop("store", self.store),
            bearer_tokens={STAGING: "staging", PRODUCTION: "production"},
            capability_fingerprint=self.fixture["capabilities"]["capabilityFingerprint"],
            options_fingerprint=self.fixture["capabilities"]["engines"]["odm"]["optionsFingerprint"],
            allowed_odm_options={"pc-quality", "projection"}, **kwargs)

    def request(self, method, path, body=None, *, token=STAGING, protected=True, headers=None, api=None):
        return (api or self.api).handle(method, "/ltds/v1" + path,
            headers if headers is not None else {"Authorization": "Bearer " + token},
            b"" if body is None else json.dumps(body).encode(), protected_transport=protected)

    def submit(self):
        response = self.request("POST", "/jobs", self.fixture["submission"])
        self.assertEqual(response.status, 201)
        return response.body

    def ready(self):
        job = self.submit()
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
        return job, result, claim["claimToken"]

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

    def test_pending_and_closed_drain_allow_only_exact_existing_claim_replay(self):
        from processing.admission_drain import AdmissionDrain
        self.queue.admission_drain=AdmissionDrain(self.queue)
        job=self.submit(); binding=dict(node_id='cluster',worker_id='worker',worker_instance_id='instance',claim_request_id='claim-1')
        original=self.store.claim(job['jobId'],**binding)
        gate=self.queue.admission_drain;gate._closing.set()
        try:
            self.assertEqual(self.store.claim(job['jobId'],**binding)['claimToken'],original['claimToken'])
            self.assertIsNone(self.store.claim(job['jobId'],**dict(binding,claim_request_id='new-claim')))
        finally:gate._closing.clear()
        gate.set_closed(True,request_id=str(uuid.uuid4()),expected_generation=0,expected_closed=False,expected_authority=self.queue.authority_id)
        self.assertEqual(self.store.claim(job['jobId'],**binding)['claimToken'],original['claimToken'])
        self.assertIsNone(self.store.claim(job['jobId'],**dict(binding,claim_request_id='new-claim')))

    def test_closed_replay_reads_job_and_claim_from_one_snapshot(self):
        from processing.admission_drain import AdmissionDrain
        with self.queue._db() as db:db.execute('PRAGMA journal_mode=WAL')
        self.queue.admission_drain=AdmissionDrain(self.queue)
        job=self.submit();binding=dict(node_id='cluster',worker_id='worker',worker_instance_id='instance',claim_request_id='claim-1')
        original=self.store.claim(job['jobId'],**binding);gate=self.queue.admission_drain;gate._closing.set()
        real=self.store._row;changed=[]
        def race(db,identifier):
            row=real(db,identifier)
            with self.queue._db() as writer:
                writer.execute('BEGIN IMMEDIATE');writer.execute('DELETE FROM reference_claims WHERE request_id=?',('claim-1',))
            changed.append(True)
            return row
        try:
            with patch.object(self.store,'_row',side_effect=race):
                replay=self.store.claim(job['jobId'],**binding)
            self.assertTrue(changed);self.assertEqual(replay['claimToken'],original['claimToken'])
        finally:gate._closing.clear()

    def test_submission_idempotency_environment_scoping_and_no_private_data(self):
        job = self.submit()
        replay = self.request("POST", "/jobs", self.fixture["submission"])
        self.assertEqual((replay.status, replay.body["jobId"]), (200, job["jobId"]))
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

    def test_quota_conflicts_are_safe_and_atomic(self):
        api = self.make_api(store=ReferenceStore(self.queue, max_metadata_bytes=1))
        response = self.request("POST", "/jobs", self.fixture["submission"], api=api)
        self.assertEqual((response.status, response.body), (409, {"error": "reference_conflict"}))
        self.assertEqual(self.queue.list(), [])

    def test_unexpected_internal_error_never_returns_exception_or_credentials(self):
        with patch.object(self.store, "submit", side_effect=RuntimeError(STAGING + " C:/private/path")):
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

    def _v1_acceptance_golden_replay(self, case_name, pinned_digest):
        data=(Path(__file__).resolve().parents[1]/
            'contracts/reference-v1-acceptance-sender.golden.json').read_bytes()
        self.assertEqual(hashlib.sha256(data).hexdigest(),'5c81c6973a1447efa64cd27dbac68393cea0395111fa087c5bd3c451b18dca41')
        fixture=json.loads(data);receipt=copy.deepcopy(fixture['acceptance']);result=fixture['resultManifest']
        if case_name=='bridge_shape':
            receipt.update(receiptId=fixture['bridgeKeyCandidate']['receiptId'],
                storageReceiptId=fixture['bridgeKeyCandidate']['storageReceiptId'])
        else:
            self.assertEqual(p.canonical_json(receipt).decode('utf8'),fixture['canonicalAcceptanceUtf8'])
            self.assertEqual(p.digest(receipt),fixture['acceptanceSha256'])
        self.assertEqual(receipt['acceptedAt'],'2026-10-29T12:00:00.000Z')
        self.assertEqual(p.digest(receipt),pinned_digest)
        if case_name=='bridge_shape':
            obj=result['objects'][0];binding={key:receipt[key] for key in ('jobId','generation','environment','inputManifestSha256','pipelineDigest','resultManifestSha256')}
            binding.update(objectId=obj['id'],objectSha256=obj['sha256'],objectBytes=obj['byteSize'])
            self.assertEqual(receipt['storageReceiptId'],p.digest(binding))
            self.assertEqual(p.digest(receipt),fixture['bridgeKeyCandidate']['canonicalAcceptanceSha256'])
        else:
            self.assertEqual(receipt['storageReceiptId'],'33333333-3333-4333-8333-333333333333')
        with patch('processing.reference_store.uuid.uuid4',return_value=uuid.UUID(receipt['jobId'])):
            job=self.submit()
        self.assertEqual(job['pipelineDigest'],receipt['pipelineDigest'])
        self.assertEqual(job['manifestSha256'],receipt['inputManifestSha256'])
        binding=dict(node_id='cluster',worker_id='worker',worker_instance_id='instance',claim_request_id='claim-1')
        claim=self.store.claim(job['jobId'],**binding)
        self.store.reserve(job['jobId'],self.fixture['transferRequest'],node_id='cluster')
        self.store.update_phase(job['jobId'],'fetching');self.store.update_phase(job['jobId'],'processing')
        self.store.record_worker_result(job['jobId'],result,claim_token=claim['claimToken'],**binding)
        self.queue.complete(job['jobId'],{'claimToken':claim['claimToken'],'status':'succeeded','result':{}})
        self.store.update_phase(job['jobId'],'awaiting_acceptance')
        path='/ltds/v1/jobs/'+job['jobId']+'/acceptance'
        def loopback_post(value):
            server=create_reference_http_server(self.api,request_timeout=2)
            thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':0.02})
            thread.start()
            try:
                connection=http.client.HTTPConnection(*server.server_address,timeout=3)
                try:
                    connection.request('POST',path,body=json.dumps(value).encode('utf8'),
                        headers={'Authorization':'Bearer '+STAGING,'Content-Type':'application/json'})
                    response=connection.getresponse()
                    return response.status,json.loads(response.read())
                finally:connection.close()
            finally:
                server.shutdown();server.server_close();thread.join(timeout=3)
                self.assertFalse(thread.is_alive(),'owned loopback listener did not stop')
        first_status,first=loopback_post(receipt)
        self.assertEqual(first_status,200)
        self.assertTrue(first['created']);self.assertTrue(first['fullyAccepted'])
        self.assertEqual(first['receiptId'],receipt['receiptId'])
        self.assertEqual(first['receiptSha256'],pinned_digest)
        # Ignore the committed response and reopen coordinator state; this is
        # not a simulated network disconnect or machine power loss.
        self.queue.close()
        self.queue=Queue(self.directory.name);self.queue.allowed_profiles.add(PROFILE)
        self.store=ReferenceStore(self.queue);self.api=self.make_api()
        replay_status,replay=loopback_post(receipt)
        self.assertEqual(replay_status,200)
        self.assertFalse(replay['created']);self.assertTrue(replay['fullyAccepted'])
        self.assertEqual(replay['receiptId'],first['receiptId'])
        self.assertEqual(replay['receiptSha256'],first['receiptSha256'])
        self.assertEqual(replay['acceptedObjectIds'],receipt['acceptedObjectIds'])
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_acceptances').fetchone()[0],1)
        changed=dict(receipt,acceptedAt='2026-10-29T12:00:00.001Z')
        self.assertEqual(loopback_post(changed)[0],409)

    def test_v1_api_client_receipt_http_reopen_replay(self):
        self._v1_acceptance_golden_replay('api_client','5f2056f679638551e0f00034485bb2600f9d12c4cb5049ff7ea5e081e440b504')

    def test_v1_bridge_shaped_receipt_http_reopen_replay(self):
        self._v1_acceptance_golden_replay('bridge_shape','7f5f61e0b9e1349fed53ba3aa667b42b031fd39e05108e4efce7dab37f61a55f')

    def test_explicit_treedar_availability_does_not_prove_file_contents(self):
        api = self.make_api(treedar_available=True)
        self.assertTrue(self.request("GET", "/capabilities", api=api).body["engines"]["treedar"]["available"])
        body = copy.deepcopy(self.fixture["submission"])
        body["manifest"]["objects"][0].update(kind="lidar", name="cloud.las")
        body["manifest"]["inputCounts"].update(images=0, lidarObjects=1)
        body["manifest"]["sha256"] = p.manifest_sha256(body["manifest"])
        body["pipeline"]["treedar"].update(requested=True, inputObjectIds=["image-1"], provenanceReceiptId="provenance-1")
        self.assertEqual(self.request("POST", "/jobs", body).status, 400)
        rejected=self.request("POST", "/jobs", body, api=api)
        self.assertEqual((rejected.status,rejected.body),
                         (409,{'error': 'treedar_execution_shape_unsupported'}))
        self.assertEqual(self.store.list(), [])
        body['pipeline']['odm']['requested'] = False
        self.assertEqual(self.request("POST", "/jobs", body, api=api).status, 201)

    def test_existing_combined_exact_replay_reconciles_without_new_job(self):
        body=copy.deepcopy(self.fixture['submission'])
        body['manifest']['objects'][0].update(kind='lidar',name='cloud.las')
        body['manifest']['inputCounts'].update(images=0,lidarObjects=1)
        body['manifest']['sha256']=p.manifest_sha256(body['manifest'])
        body['pipeline']['treedar'].update(requested=True,inputObjectIds=['image-1'],provenanceReceiptId='provenance-1')
        validated=p.validate_submission(body,environment='staging',capability_fingerprint=body['capabilityFingerprint'],
            available_engines=('odm','treedar'),allowed_odm_options=set(body['pipeline']['odm']['options']))
        original,_=self.store.submit(validated)
        api=self.make_api(treedar_available=True)
        replay=self.request('POST','/jobs',body,api=api)
        self.assertEqual((replay.status,replay.body['jobId']),(200,original['jobId']))
        changed=copy.deepcopy(body);changed['pipeline']['treedar']['provenanceReceiptId']='different-id'
        conflict=self.request('POST','/jobs',changed,api=api)
        self.assertEqual((conflict.status,conflict.body),(409,{'error':'treedar_execution_shape_unsupported'}))
        cross=self.request('POST','/jobs',body,api=api,token=PRODUCTION)
        self.assertEqual((cross.status,cross.body),(409,{'error':'treedar_execution_shape_unsupported'}))
        self.assertEqual(len(self.store.list()),1)

    def test_treedar_rejects_multiple_or_mixed_inputs_before_persisting(self):
        api = self.make_api(treedar_available=True)
        for second_kind in ('lidar', 'image'):
            with self.subTest(second_kind=second_kind):
                body = copy.deepcopy(self.fixture['submission'])
                first = body['manifest']['objects'][0]
                first.update(kind='lidar', name='cloud.las')
                second = dict(first, id='input-two', name='second.laz' if second_kind=='lidar' else 'second.jpg', kind=second_kind)
                body['manifest']['objects'].append(second)
                body['manifest']['objectCount'] = 2
                body['manifest']['totalBytes'] = first['byteSize'] + second['byteSize']
                body['manifest']['inputCounts'].update(images=int(second_kind=='image'), lidarObjects=1+int(second_kind=='lidar'))
                body['manifest']['sha256'] = p.manifest_sha256(body['manifest'])
                body['pipeline']['odm']['requested'] = False
                body['pipeline']['treedar'].update(requested=True, inputObjectIds=['image-1'], provenanceReceiptId='provenance-1')
                response=self.request('POST','/jobs',body,api=api)
                self.assertEqual((response.status,response.body),(409,{'error':'treedar_execution_shape_unsupported'}))
                self.assertEqual(self.store.list(), [])


if __name__ == "__main__":
    unittest.main()
