import copy
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import tempfile
import unittest
import uuid

from processing import reference_protocol as p
from processing.prototype import Queue, PROFILE as LEGACY_PROFILE
from processing.maintenance import Conflict
from processing.reference_store import ReferenceStore, PROFILE
from processing.source_range_transfer import CHUNK_BYTES, source_version_for_manifest


class ReferenceStoreTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.queue = Queue(self.directory.name, node_ids=("cluster", "node-2"))
        self.queue.allowed_profiles.add(PROFILE)
        self.store = ReferenceStore(self.queue)
        self.fixture = json.loads((Path(__file__).resolve().parents[1] /
                                  "contracts/reference-v1.contract.json").read_text())
        self.submission = p.validate_submission(self.fixture["submission"], environment="staging",
            capability_fingerprint=self.fixture["capabilities"]["capabilityFingerprint"])

    def tearDown(self):
        self.queue.close()
        self.directory.cleanup()

    def new_submission(self):
        raw = copy.deepcopy(self.fixture["submission"])
        raw["submissionId"] = str(uuid.uuid4())
        return p.validate_submission(raw, environment="staging",
            capability_fingerprint=raw["capabilityFingerprint"])

    def reserve(self, job):
        self.store.claim(job["jobId"], node_id="cluster", worker_id="worker",
                         worker_instance_id="instance-1", claim_request_id="claim-" + job["jobId"])
        transfer = copy.deepcopy(self.fixture["transferRequest"])
        transfer.update(manifestSha256=job["manifestSha256"], generation=job["generation"],
                        byteBudget=self.submission["manifest"]["totalBytes"])
        return self.store.reserve(job["jobId"], transfer, node_id="cluster")

    def result_ready(self):
        job, _ = self.store.submit(self.submission)
        self.reserve(job)
        self.store.update_phase(job["jobId"], "fetching")
        self.store.update_phase(job["jobId"], "processing")
        result = copy.deepcopy(self.fixture["resultManifest"])
        result.update(jobId=job["jobId"], pipelineDigest=job["pipelineDigest"])
        result["resultManifestSha256"] = p.digest({k: v for k, v in result.items() if k != "resultManifestSha256"})
        self.store.set_result_manifest(job["jobId"], result)
        self.store.update_phase(job["jobId"], "awaiting_acceptance")
        return job, result

    def receipt(self, job, result):
        receipt = copy.deepcopy(self.fixture["acceptance"])
        receipt.update(jobId=job["jobId"], pipelineDigest=job["pipelineDigest"],
                       inputManifestSha256=job["manifestSha256"],
                       resultManifestSha256=result["resultManifestSha256"])
        return receipt

    def test_retry_transfer_budget_has_explicit_three_copy_ceiling(self):
        job, _ = self.store.submit(self.submission)
        self.store.claim(job['jobId'], node_id='cluster', worker_id='worker',
                         worker_instance_id='instance', claim_request_id='claim-budget')
        transfer = copy.deepcopy(self.fixture['transferRequest'])
        for budget in (2, 10):
            transfer['byteBudget'] = budget
            with self.assertRaises(Conflict):
                self.store.reserve(job['jobId'], transfer, node_id='cluster')
        transfer['byteBudget'] = 9
        self.assertEqual(self.store.reserve(job['jobId'], transfer, node_id='cluster')['phase'], 'awaiting_transfer')

    def test_wire_expiry_requires_viewer_compatible_utc_milliseconds(self):
        job, _ = self.store.submit(self.submission)
        self.store.claim(job['jobId'], node_id='cluster', worker_id='worker',
            worker_instance_id='instance', claim_request_id='claim-expiry')
        for value in ('2030-01-01T00:00:00.123456Z', '2030-01-01T00:00:00Z', '2030-01-01T00:00:00.000+00:00'):
            transfer = {**self.fixture['transferRequest'], 'expiresAt': value}
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.store.reserve(job['jobId'], transfer, node_id='cluster')
        self.assertEqual(self.store.reserve(job['jobId'], self.fixture['transferRequest'], node_id='cluster')['phase'], 'awaiting_transfer')

    def test_source_range_v2_reservation_is_bound_and_durable_in_the_shared_queue(self):
        submission = copy.deepcopy(self.fixture['submission'])
        submission['submissionId'] = str(uuid.uuid4())
        submission['manifest']['id'] = 'dataset-fixture-store'
        submission['manifest']['sha256'] = p.manifest_sha256(submission['manifest'])
        submission = p.validate_submission(submission, environment='staging',
            capability_fingerprint=submission['capabilityFingerprint'])
        job, _ = self.store.submit(submission)
        claim = self.store.claim(job['jobId'], node_id='cluster', worker_id='worker',
            worker_instance_id='instance-v2', claim_request_id='claim-v2-' + job['jobId'])
        manifest = claim['submission']['manifest']
        chunks = sum((item['byteSize'] + CHUNK_BYTES - 1) // CHUNK_BYTES for item in manifest['objects'])
        transfer = {
            'protocol': 'source-range-v2', 'reservationId': 'reservation-v2',
            'generation': claim['generation'], 'expiresAt': '2030-01-01T00:00:00.000Z',
            'manifestSha256': manifest['sha256'], 'sourceVersion': source_version_for_manifest(manifest),
            'objectIds': [item['id'] for item in manifest['objects']], 'chunkBytes': CHUNK_BYTES,
            'requestBudget': chunks, 'byteBudget': manifest['totalBytes'],
            'nonce': 'nonce-v2', 'attemptBudget': chunks * 3,
        }
        reserved = self.store.reserve(job['jobId'], transfer, node_id='cluster')
        self.assertEqual(reserved['phase'], 'awaiting_transfer')
        self.assertEqual(reserved['transferRequest'], transfer)
        self.assertEqual(self.store.reserve(job['jobId'], transfer, node_id='cluster'), reserved)

        grant = {**transfer, 'expiresAt': '2029-01-01T00:00:00.000Z',
            'jobId': job['jobId'], 'environment': 'staging', 'token': 'A' * 43}
        receipt = self.store.receive_transfer_grant(job['jobId'], grant)
        self.assertTrue(receipt['accepted'])
        public = self.store.get(job['jobId'])
        self.assertNotIn('token', json.dumps(public))
        self.assertEqual(self.store.private_transfer_grant(job['jobId']), grant)

    def test_source_range_v2_reservation_rejects_source_identity_and_unbounded_budgets(self):
        submission = copy.deepcopy(self.fixture['submission'])
        submission['submissionId'] = str(uuid.uuid4())
        submission['manifest']['id'] = 'dataset-fixture-store'
        submission['manifest']['sha256'] = p.manifest_sha256(submission['manifest'])
        submission = p.validate_submission(submission, environment='staging',
            capability_fingerprint=submission['capabilityFingerprint'])
        job, _ = self.store.submit(submission)
        claim = self.store.claim(job['jobId'], node_id='cluster', worker_id='worker',
            worker_instance_id='instance-v2', claim_request_id='claim-v2-' + job['jobId'])
        manifest = claim['submission']['manifest']
        chunks = sum((item['byteSize'] + CHUNK_BYTES - 1) // CHUNK_BYTES for item in manifest['objects'])
        base = {'protocol': 'source-range-v2', 'reservationId': 'reservation-v2',
            'generation': claim['generation'], 'expiresAt': '2030-01-01T00:00:00.000Z',
            'manifestSha256': manifest['sha256'], 'sourceVersion': source_version_for_manifest(manifest),
            'objectIds': [item['id'] for item in manifest['objects']], 'chunkBytes': CHUNK_BYTES,
            'requestBudget': chunks, 'byteBudget': manifest['totalBytes'],
            'nonce': 'nonce-v2', 'attemptBudget': chunks * 3}
        for mutation in (
            {'sourceVersion': 'dataset:wrong:' + manifest['sha256']},
            {'objectIds': ['unbound-object']},
            {'requestBudget': chunks + 1},
            {'attemptBudget': chunks * 4},
            {'expiresAt': '2020-01-01T00:00:00.000Z'},
        ):
            with self.subTest(mutation=mutation), self.assertRaises(Conflict):
                self.store.reserve(job['jobId'], {**base, **mutation}, node_id='cluster')

    def test_disabled_profile_fails_closed(self):
        self.queue.allowed_profiles.remove(PROFILE)
        with self.assertRaises(ValueError):
            ReferenceStore(self.queue)

    def test_atomic_concurrent_idempotency_and_shared_queue(self):
        with ThreadPoolExecutor(max_workers=8) as pool:
            answers = list(pool.map(lambda _: self.store.submit(self.submission), range(8)))
        self.assertEqual(sum(created for _, created in answers), 1)
        self.assertEqual(len({job["jobId"] for job, _ in answers}), 1)
        queue_jobs = self.queue.list()
        self.assertEqual(len(queue_jobs), 1)
        self.assertEqual(queue_jobs[0]["profile"], PROFILE)

    def test_submission_conflicts_and_unvalidated_digests(self):
        self.store.submit(self.submission)
        spoofed = copy.deepcopy(self.submission)
        spoofed["environment"] = "production"
        with self.assertRaises(Conflict):
            self.store.submit(spoofed)
        for mutation in ({"requestDigest": "0" * 64}, {"credential": "secret"},
                         {"manifestSha256": "0" * 64}):
            bad = copy.deepcopy(self.submission)
            bad.update(mutation)
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                self.store.submit(bad)

    def test_pending_quota_is_not_historical_count(self):
        store = ReferenceStore(self.queue, max_queued_jobs=1)
        job, _ = store.submit(self.submission)
        other = self.new_submission()
        with self.assertRaises(Conflict):
            store.submit(other)
        store.update_phase(job["jobId"], "cancelled")
        store.submit(other)
        self.assertEqual(len(self.queue.list()), 2)

    def test_fifty_gigabyte_ten_thousand_photo_manifest_routes_to_main(self):
        raw = copy.deepcopy(self.fixture["submission"])
        raw["submissionId"] = str(uuid.uuid4())
        manifest = raw["manifest"]
        manifest["objects"] = [
            {"id": f"image-{index:05d}", "name": f"image-{index:05d}.jpg",
             "byteSize": 5_000_000, "sha256": "a" * 64,
             "kind": "image", "mediaType": "image/jpeg"}
            for index in range(10_000)
        ]
        manifest.update(totalBytes=50_000_000_000, objectCount=10_000,
                        inputCounts={"images": 10_000, "lidarObjects": 0, "points": 0})
        manifest["sha256"] = p.manifest_sha256(manifest)
        checked = p.validate_submission(raw, environment="staging",
            capability_fingerprint=raw["capabilityFingerprint"])
        self.assertEqual(checked["manifest"]["totalBytes"], 50_000_000_000)
        job, created = self.store.submit(checked)
        self.assertTrue(created)
        self.assertEqual(self.store._required_node(checked), "node-2")
        self.assertIsNone(self.store.next_for_node("cluster"))
        self.assertEqual(self.store.next_for_node("node-2")["jobId"], job["jobId"])

    def test_metadata_quota_rolls_back_both_tables(self):
        store = ReferenceStore(self.queue, max_metadata_bytes=1)
        with self.assertRaises(Conflict):
            store.submit(self.submission)
        self.assertEqual(self.queue.list(), [])
        self.assertEqual(store.list(), [])

    def test_reservation_identity_transition_and_public_redaction(self):
        job, _ = self.store.submit(self.submission)
        with self.assertRaises(Conflict):
            self.store.update_phase(job["jobId"], "processing")
        with self.assertRaises(Conflict):
            self.store.update_phase(job["jobId"], "awaiting_transfer")
        reserved = self.reserve(job)
        self.assertEqual(reserved["phase"], "awaiting_transfer")
        self.assertNotIn("submission", self.store.get(job["jobId"]))
        self.assertEqual(self.store.get(job["jobId"], private=True)["submission"], self.submission)
        self.assertEqual(self.reserve(job), reserved)
        with self.assertRaises(ValueError):
            self.store.update_phase(job["jobId"], "held", error="https://secret/path")
        with self.assertRaises(Conflict):
            self.store.get("unknown")

    def test_global_engine_lock_includes_legacy_treedar(self):
        job, _ = self.store.submit(self.submission)
        self.reserve(job)
        legacy, _ = self.queue.submit({"requestId": "legacy", "environment": "staging", "profile": LEGACY_PROFILE})
        claim = self.store.get(job["jobId"], private=True)
        self.assertEqual(claim["jobId"], job["jobId"])
        self.assertIsNone(self.queue.claim({"workerId": "legacy", "profiles": [LEGACY_PROFILE]}))
        self.store.update_phase(job["jobId"], "held", error="worker_uncertain")
        with self.assertRaises(Conflict):
            self.store.update_phase(job["jobId"], "cancelled")
        self.assertIsNone(self.queue.claim({"workerId": "legacy", "profiles": [LEGACY_PROFILE]}))
        self.assertEqual(self.queue.get(legacy["id"])["status"], "queued")

    def test_restart_preserves_identity_and_holds_uncertain_engine(self):
        job, _ = self.store.submit(self.submission)
        self.reserve(job)
        self.queue.claim({"workerId": "worker", "profiles": [PROFILE]})
        self.queue.close()
        self.queue = Queue(self.directory.name, node_ids=("cluster", "node-2"))
        self.queue.allowed_profiles.add(PROFILE)
        self.store = ReferenceStore(self.queue)
        self.assertEqual(self.store.get(job["jobId"])["phase"], "held")
        self.assertFalse(self.store.submit(self.submission)[1])
        self.assertIsNone(self.queue.claim({"workerId": "worker", "profiles": [PROFILE]}))

    def test_acceptance_subsets_and_changed_receipt(self):
        job, result = self.result_ready()
        receipt = self.receipt(job, result)
        accepted = self.store.accept(job["jobId"], receipt)
        self.assertFalse(accepted["fullyAccepted"])
        self.assertFalse(self.store.accept(job["jobId"], receipt)["created"])
        changed = copy.deepcopy(receipt)
        changed["acceptedAt"] = "2026-10-28T12:00:00Z"
        with self.assertRaises(Conflict):
            self.store.accept(job["jobId"], changed)
        changed["receiptId"] = "receipt-final"
        changed['storageReceiptId'] = 'storage-final'
        changed["acceptedObjectIds"] = [obj["id"] for obj in result["objects"]]
        accepted = self.store.accept(job["jobId"], changed)
        self.assertTrue(accepted["fullyAccepted"])
        self.assertEqual(accepted["job"]["phase"], "completed")
        # Acceptance is metadata; it never releases the shared worker lock.
        self.assertEqual(accepted["job"]["status"], "running")

    def test_claim_replay_atomicity_binding_and_restart(self):
        job, _ = self.store.submit(self.submission)
        kwargs = dict(node_id="cluster", worker_id="worker", worker_instance_id="instance-1", claim_request_id="claim-1")
        with ThreadPoolExecutor(max_workers=8) as pool:
            claims = list(pool.map(lambda _: self.store.claim(job["jobId"], **kwargs), range(8)))
        token = claims[0]["claimToken"]
        self.assertEqual({claim["claimToken"] for claim in claims}, {token})
        self.assertNotIn("claimToken", self.store.get(job["jobId"]))
        for changed in ({"worker_instance_id": "instance-2"}, {"worker_id": "other"}, {"node_id": "node-2"}):
            with self.subTest(changed=changed), self.assertRaises(Conflict):
                self.store.claim(job["jobId"], **{**kwargs, **changed})
        self.queue.close()
        self.queue = Queue(self.directory.name, node_ids=("cluster", "node-2"))
        self.queue.allowed_profiles.add(PROFILE)
        self.store = ReferenceStore(self.queue)
        replay = self.store.claim(job["jobId"], **kwargs)
        self.assertEqual(replay["claimToken"], token)
        self.assertEqual((replay["status"], replay["phase"]), ("held", "held"))
        self.queue.complete(job["jobId"], {"claimToken": token, "status": "succeeded", "result": {}})
        self.assertEqual(self.store.claim(job["jobId"], **kwargs)["status"], "succeeded")

    def test_claim_transaction_rolls_back_queue_when_envelope_insert_fails(self):
        job, _ = self.store.submit(self.submission)
        with self.queue._db() as db:
            db.execute("CREATE TRIGGER reject_claim BEFORE INSERT ON reference_claims BEGIN SELECT RAISE(ABORT,'test crash'); END")
        with self.assertRaises(Exception):
            self.store.claim(job["jobId"], node_id="cluster", worker_id="worker",
                             worker_instance_id="instance", claim_request_id="claim-1")
        self.assertEqual(self.queue.get(job["jobId"])["status"], "queued")
        self.assertIsNone(self.store.get(job["jobId"], private=True)["claimToken"])

    def test_unclaimed_execution_and_reservation_rejected(self):
        job, _ = self.store.submit(self.submission)
        with self.assertRaises(Conflict):
            self.store.reserve(job["jobId"], self.fixture["transferRequest"], node_id="cluster")
        with self.assertRaises(Conflict):
            self.store.update_phase(job["jobId"], "fetching")

    def test_claim_respects_shared_priority_and_node_maintenance(self):
        job, _ = self.store.submit(self.submission)
        self.queue.submit({"requestId": "priority", "environment": "production", "profile": LEGACY_PROFILE})
        kwargs = dict(node_id="cluster", worker_id="worker", worker_instance_id="instance", claim_request_id="claim-1")
        self.assertIsNone(self.store.claim(job["jobId"], **kwargs))
        with self.queue._db() as db:
            db.execute("UPDATE jobs SET status='succeeded' WHERE profile=?", (LEGACY_PROFILE,))
            db.execute("UPDATE nodes SET state='held' WHERE id='cluster'")
        self.assertIsNone(self.store.claim(job["jobId"], **kwargs))
        with self.queue._db() as db:
            db.execute("UPDATE nodes SET state='active' WHERE id='cluster'")
        self.assertEqual(self.store.claim(job["jobId"], **kwargs)["jobId"], job["jobId"])

    def test_surviving_worker_terminal_result_recovers_without_engine_replay(self):
        job, _ = self.store.submit(self.submission)
        self.reserve(job)
        self.store.update_phase(job["jobId"], "fetching")
        self.store.update_phase(job["jobId"], "processing")
        token = self.store.get(job["jobId"], private=True)["claimToken"]
        result = copy.deepcopy(self.fixture["resultManifest"])
        result.update(jobId=job["jobId"], pipelineDigest=job["pipelineDigest"])
        result["resultManifestSha256"] = p.digest({k: v for k, v in result.items() if k != "resultManifestSha256"})
        self.queue.close()
        self.queue = Queue(self.directory.name, node_ids=("cluster", "node-2"))
        self.queue.allowed_profiles.add(PROFILE)
        self.store = ReferenceStore(self.queue)
        recovered = self.store.get(job["jobId"], private=True)
        self.assertEqual((recovered["phase"], recovered["executionPhase"]), ("held", "processing"))
        with self.assertRaises(Conflict):
            self.store.set_result_manifest(job["jobId"], result)
        kwargs = dict(claim_token=token, node_id="cluster", worker_id="worker",
                      worker_instance_id="instance-1", claim_request_id="claim-" + job["jobId"])
        with self.assertRaises(Conflict):
            self.store.record_worker_result(job["jobId"], result, **{**kwargs, "claim_token": "wrong"})
        self.store.record_worker_result(job["jobId"], result, **kwargs)
        self.queue.complete(job["jobId"], {"claimToken": token, "status": "succeeded", "result": {}})
        execution_done = self.store.get(job["jobId"])
        self.assertEqual((execution_done["status"], execution_done["phase"]), ("succeeded", "waiting_for_delivery"))
        self.store.update_phase(job["jobId"], "awaiting_acceptance")
        receipt = self.receipt(job, result)
        receipt["acceptedObjectIds"] = [obj["id"] for obj in result["objects"]]
        self.assertTrue(self.store.accept(job["jobId"], receipt)["fullyAccepted"])

    def test_operator_hold_blocks_worker_result_recovery(self):
        job, _ = self.result_ready()
        private = self.store.get(job["jobId"], private=True)
        self.store.update_phase(job["jobId"], "held", error="operator_uncertain")
        with self.assertRaises(Conflict):
            self.store.record_worker_result(job["jobId"], private["resultManifest"],
                claim_token=private["claimToken"], node_id="cluster", worker_id="worker",
                worker_instance_id="instance-1", claim_request_id="claim-" + job["jobId"])

    def test_stale_result_receipt_and_sealed_result(self):
        job, result = self.result_ready()
        receipt = self.receipt(job, result)
        receipt["environment"] = "production"
        with self.assertRaises(ValueError):
            self.store.accept(job["jobId"], receipt)
        result["objects"][0]["byteSize"] += 1
        result["resultManifestSha256"] = p.digest({k: v for k, v in result.items() if k != "resultManifestSha256"})
        with self.assertRaises(Conflict):
            self.store.set_result_manifest(job["jobId"], result)

    def test_outbox_durable_idempotency_backoff_and_no_secrets(self):
        job, _ = self.store.submit(self.submission)
        payload = {"jobId": job["jobId"], "generation": 1, "environment": "staging", "receiptId": "receipt-1"}
        self.assertTrue(self.store.enqueue_outbox("delivery_ack", "ack-1", payload))
        self.assertFalse(self.store.enqueue_outbox("delivery_ack", "ack-1", payload))
        self.assertEqual(len(self.store.pending_outbox(now=100)), 1)
        self.assertEqual(self.store.retry_outbox("ack-1", now=100), 1)
        self.assertEqual(self.store.pending_outbox(now=100), [])
        self.assertEqual(len(self.store.pending_outbox(now=101)), 1)
        self.store = ReferenceStore(self.queue)
        self.assertEqual(len(self.store.pending_outbox(now=101)), 1)
        self.store.acknowledge_outbox("ack-1")
        self.assertEqual(self.store.pending_outbox(), [])
        payload["token"] = "secret"
        with self.assertRaises(ValueError):
            self.store.enqueue_outbox("delivery_ack", "ack-2", payload)


if __name__ == "__main__":
    unittest.main()
