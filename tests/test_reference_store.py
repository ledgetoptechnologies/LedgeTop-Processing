import copy
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
import uuid

from processing import reference_protocol as p
from processing.prototype import Queue, PROFILE as LEGACY_PROFILE, VIEWER_PROFILE
from processing.config import ProcessingNode
from processing.maintenance import Conflict
from processing.reference_store import ReferenceStore, PROFILE
from processing.viewer_service import ViewerService
from processing.viewer_tasks import TaskStore


class ReferenceStoreTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.queue = Queue(self.directory.name, node_ids=("cluster", "node-2"))
        self.queue.allowed_profiles.add(PROFILE)
        self.queue.allowed_profiles.add(VIEWER_PROFILE)
        self.store = ReferenceStore(self.queue)
        self.viewer_store = TaskStore(self.queue, Path(self.directory.name) / "viewer-inputs")
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

    def submission_with_images(self, count, environment="staging"):
        raw = copy.deepcopy(self.fixture["submission"])
        raw["submissionId"] = str(uuid.uuid4())
        original = raw["manifest"]["objects"][0]
        raw["manifest"]["objects"] = []
        for index in range(count):
            item = dict(original, id=f"image-{index + 1}", name=f"image-{index + 1:04}.jpg")
            raw["manifest"]["objects"].append(item)
        raw["manifest"].update(objectCount=count, totalBytes=count * original["byteSize"],
                                inputCounts={"images": count, "lidarObjects": 0, "points": 0})
        raw["manifest"]["sha256"] = p.manifest_sha256(raw["manifest"])
        return p.validate_submission(raw, environment=environment,
            capability_fingerprint=raw["capabilityFingerprint"])

    def viewer_job(self, count, environment="staging"):
        identifier = str(uuid.uuid4())
        self.viewer_store.initialize(identifier, "Synthetic Viewer task", [], None, environment)
        for index in range(count):
            data = ("photo-%d" % index).encode()
            self.viewer_store.upload(identifier, "photo-%04d.jpg" % index,
                                     io.BytesIO(data), len(data))
        self.viewer_store.commit(identifier)
        return self.viewer_store.task(identifier)

    def fleet(self, cluster_images=1):
        self.store.update_fleet_snapshot({
            "cluster": dict(ready=True, maxImages=cluster_images, maxInputBytes=None, taskQueueCount=0),
            "node-2": dict(ready=True, maxImages=None, maxInputBytes=None, taskQueueCount=0),
        })

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

    def test_earlier_eligible_viewer_job_blocks_reference_claim_on_same_node(self):
        self.fleet()
        viewer = self.viewer_job(1)
        reference, _ = self.store.submit(self.submission_with_images(1))
        self.assertIsNone(self.store.next_for_node("cluster"))
        self.assertIsNone(self.store.claim(reference["jobId"], node_id="cluster", worker_id="reference-worker",
            worker_instance_id="reference-instance", claim_request_id="reference-before-viewer"))
        claimed = self.queue.claim({"workerId": "viewer-dispatch", "nodeId": "cluster",
            "profiles": [VIEWER_PROFILE], "expectedJobId": viewer["job_id"]})
        self.assertEqual(claimed["id"], viewer["job_id"])

    def test_oversized_viewer_job_does_not_block_small_reference_on_cluster(self):
        self.fleet(cluster_images=1)
        viewer = self.viewer_job(3)
        reference, _ = self.store.submit(self.submission_with_images(1))
        self.assertEqual(self.store.next_for_node("cluster")["jobId"], reference["jobId"])
        self.assertIsNone(self.store.next_for_node("node-2"))
        reference_claim = self.store.claim(reference["jobId"], node_id="cluster", worker_id="reference-worker",
            worker_instance_id="reference-instance", claim_request_id="reference-cluster-small")
        self.assertIsNotNone(reference_claim)
        viewer_claim = self.queue.claim({"workerId": "viewer-dispatch", "nodeId": "node-2",
            "profiles": [VIEWER_PROFILE], "expectedJobId": viewer["job_id"]})
        self.assertIsNotNone(viewer_claim)

    def test_viewer_dispatch_refreshes_shared_size_snapshot_before_atomic_claim(self):
        viewer = self.viewer_job(3)
        reference, _ = self.store.submit(self.submission_with_images(1))

        class Engine:
            def initialize(self, task):
                self.task = task

            def upload(self, identifier, item):
                self.asserted = Path(item["path"]).is_file()

            def action(self, action, identifier):
                return {"success": True}

        engines = {node: Engine() for node in ("cluster", "node-2")}
        config = SimpleNamespace(
            processing_nodes=(ProcessingNode("cluster", "http://cluster.example", max_input_bytes=1000),
                              ProcessingNode("node-2", "http://node-2.example")),
            prototype_state_dir=self.directory.name, local_max_photos=1,
            local_max_input_bytes=1000, viewer_max_input_bytes=1000)
        def probe(node_id, _url, _token):
            return dict(ready=True, maxImages=1 if node_id == "cluster" else None,
                        taskQueueCount=0)
        service = ViewerService(self.queue, config, store=self.viewer_store,
                                engines=engines, probe=probe)
        catalog = {"ready": True, "commonOptions": [], "nodes": [
            dict(role="cluster", available=True, options=[]),
            dict(role="node-2", available=True, options=[])]}
        service._catalog = lambda: catalog

        service.tick()
        task = self.viewer_store.task(viewer["uuid"])
        self.assertEqual(task["node_id"], "node-2")
        self.assertEqual(task["job"]["status"], "running")
        self.assertEqual(self.store.next_for_node("cluster")["jobId"], reference["jobId"])

    def test_production_reference_job_preempts_staging_viewer_queue_order(self):
        self.fleet()
        viewer = self.viewer_job(1, environment="staging")
        reference, _ = self.store.submit(self.submission_with_images(1, environment="production"))
        self.assertEqual(self.store.next_for_node("cluster")["jobId"], reference["jobId"])
        self.assertIsNone(self.queue.claim({"workerId": "viewer-dispatch", "nodeId": "cluster",
            "profiles": [VIEWER_PROFILE], "expectedJobId": viewer["job_id"]}))
        claimed = self.store.claim(reference["jobId"], node_id="cluster", worker_id="reference-worker",
            worker_instance_id="reference-instance", claim_request_id="production-priority-claim")
        self.assertIsNotNone(claimed)

    def test_concurrent_viewer_and_reference_claims_cannot_bypass_shared_head(self):
        self.fleet()
        reference, _ = self.store.submit(self.submission_with_images(1))
        viewer = self.viewer_job(1)

        def claim_reference():
            return self.store.claim(reference["jobId"], node_id="cluster", worker_id="reference-worker",
                worker_instance_id="reference-instance", claim_request_id="race-reference-claim")

        def claim_viewer():
            return self.queue.claim({"workerId": "viewer-dispatch", "nodeId": "cluster",
                "profiles": [VIEWER_PROFILE], "expectedJobId": viewer["job_id"]})

        with ThreadPoolExecutor(max_workers=2) as pool:
            reference_claim, viewer_claim = list(pool.map(lambda fn: fn(), (claim_reference, claim_viewer)))
        self.assertIsNotNone(reference_claim)
        self.assertIsNone(viewer_claim)
        self.assertEqual(self.queue.get(reference["jobId"])["nodeId"], "cluster")
        self.assertEqual(self.queue.get(viewer["job_id"])["status"], "queued")

    def test_mismatched_viewer_task_environment_blocks_cross_profile_bypass(self):
        self.fleet()
        viewer = self.viewer_job(1, environment="staging")
        reference, _ = self.store.submit(self.submission_with_images(1))
        with self.queue._db() as db:
            db.execute("UPDATE viewer_tasks SET environment='production' WHERE uuid=?", (viewer["uuid"],))
        self.assertIsNone(self.store.next_for_node("cluster"))
        self.assertIsNone(self.store.claim(reference["jobId"], node_id="cluster", worker_id="reference-worker",
            worker_instance_id="reference-instance", claim_request_id="reference-after-invalid-viewer"))
        self.assertIsNone(self.queue.claim({"workerId": "viewer-dispatch", "nodeId": "cluster",
            "profiles": [VIEWER_PROFILE], "expectedJobId": viewer["job_id"]}))

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

    def test_reference_workers_claim_one_job_per_node_in_queue_order(self):
        store = ReferenceStore(self.queue, local_max_photos=1,
            local_max_input_bytes=20 * 1024**3, main_node_id="node-2")
        small, _ = store.submit(self.new_submission())
        large_raw = copy.deepcopy(self.fixture["submission"])
        large_raw["submissionId"] = str(uuid.uuid4())
        first = copy.deepcopy(large_raw["manifest"]["objects"][0])
        first.update(id="image-2", name="image-2.jpg")
        large_raw["manifest"]["objects"].append(first)
        large_raw["manifest"]["objectCount"] = 2
        large_raw["manifest"]["totalBytes"] *= 2
        large_raw["manifest"]["inputCounts"]["images"] = 2
        large_raw["manifest"]["sha256"] = p.manifest_sha256(large_raw["manifest"])
        large_submission = p.validate_submission(large_raw, environment="staging",
            capability_fingerprint=large_raw["capabilityFingerprint"])
        large, _ = store.submit(large_submission)

        self.assertEqual(store.next_for_node("cluster")["jobId"], small["jobId"])
        store.claim(small["jobId"], node_id="cluster", worker_id="cluster-worker",
            worker_instance_id="cluster-instance", claim_request_id="cluster-claim")
        # A running cluster job must not suppress the queue head that only the
        # main node can accept. Each physical node has its own active slot.
        self.assertEqual(store.next_for_node("node-2")["jobId"], large["jobId"])
        self.assertIsNotNone(store.claim(large["jobId"], node_id="node-2",
            worker_id="main-worker", worker_instance_id="main-instance",
            claim_request_id="main-claim"))

        later_small, _ = store.submit(self.new_submission())
        self.assertIsNone(store.claim(later_small["jobId"], node_id="cluster",
            worker_id="cluster-worker", worker_instance_id="cluster-instance",
            claim_request_id="cluster-second-claim"))

    def test_ordered_live_capacity_routes_jobs_across_three_nodes_and_keeps_claim_atomic(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        queue = Queue(directory.name, node_ids=("cluster", "middle", "main"),
                      treedar_node_ids=("main",), reference_enabled=True)
        self.addCleanup(queue.close)
        store = ReferenceStore(queue, local_max_photos=1,
            local_max_input_bytes=20 * 1024**3, main_node_id="main",
            node_limits={"middle": {"maxImages": 4}})
        capacities = {
            "cluster": {"ready": True, "maxImages": 1, "maxInputBytes": None, "taskQueueCount": 0},
            "middle": {"ready": True, "maxImages": 4, "maxInputBytes": None, "taskQueueCount": 0},
            "main": {"ready": True, "maxImages": None, "maxInputBytes": None, "taskQueueCount": 0},
        }
        node_options = [dict(role=node, available=True, options=[]) for node in queue.node_ids]
        store.update_fleet_snapshot(capacities, node_options)
        large, _ = store.submit(self.submission_with_images(3))
        small, _ = store.submit(self.submission_with_images(1))
        self.assertEqual(store.next_for_node("cluster")["jobId"], small["jobId"])
        self.assertEqual(store.next_for_node("middle")["jobId"], large["jobId"])
        self.assertIsNone(store.next_for_node("main"))

        # A simultaneous claimant on a later capable node cannot steal a job
        # while an earlier eligible node remains idle.
        with ThreadPoolExecutor(max_workers=2) as pool:
            answers = list(pool.map(lambda node: store.claim(small["jobId"], node_id=node,
                worker_id=node + "-worker", worker_instance_id=node + "-instance",
                claim_request_id=node + "-claim"), ("cluster", "middle")))
        claims = [answer for answer in answers if answer is not None]
        self.assertEqual(len(claims), 1)
        self.assertEqual(claims[0]["nodeId"], "cluster")

        # The intermediate node takes the larger job it fits, preserving FIFO
        # for that node. Once cluster and middle are occupied, small work uses
        # the next idle capable node in the configured order.
        self.assertEqual(store.next_for_node("middle")["jobId"], large["jobId"])
        store.claim(large["jobId"], node_id="middle", worker_id="middle-worker",
            worker_instance_id="middle-instance", claim_request_id="middle-claim")
        later, _ = store.submit(self.submission_with_images(1))
        self.assertEqual(store.next_for_node("main")["jobId"], later["jobId"])

    def test_live_unavailable_or_busy_first_node_falls_through_without_violating_size(self):
        self.store.local_max_photos = 1
        job, _ = self.store.submit(self.submission_with_images(1))
        self.store.update_fleet_snapshot({
            "cluster": {"ready": False, "maxImages": None, "maxInputBytes": None, "taskQueueCount": 0},
            "node-2": {"ready": True, "maxImages": 10, "maxInputBytes": None, "taskQueueCount": 0},
        }, [dict(role="cluster", available=False, options=[]),
            dict(role="node-2", available=True, options=[])])
        self.assertEqual(self.store.next_for_node("node-2")["jobId"], job["jobId"])
        self.assertIsNone(self.store.next_for_node("cluster"))

    def test_preferred_node_in_maintenance_falls_through_to_next_eligible_node(self):
        job, _ = self.store.submit(self.submission_with_images(1))
        stamp = datetime.now(timezone.utc).isoformat()
        self.queue.schedule_node('cluster', {'expectedRevision': 0, 'drainAt': stamp,
            'notBefore': stamp, 'afterJobId': None, 'targetImage': 'sha256:' + 'a' * 64})
        self.assertEqual(self.queue.get_node('cluster')['state'], 'draining')
        self.assertIsNone(self.store.next_for_node('cluster'))
        self.assertEqual(self.store.next_for_node('node-2')['jobId'], job['jobId'])
        claim = self.store.claim(job['jobId'], node_id='node-2', worker_id='fallback-worker',
            worker_instance_id='fallback-instance', claim_request_id='maintenance-fallback-claim')
        self.assertEqual(claim['nodeId'], 'node-2')

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

    def test_legacy_treedar_can_run_on_another_idle_node(self):
        job, _ = self.store.submit(self.submission)
        self.reserve(job)
        legacy, _ = self.queue.submit({"requestId": "legacy", "environment": "staging", "profile": LEGACY_PROFILE})
        claim = self.store.get(job["jobId"], private=True)
        self.assertEqual(claim["jobId"], job["jobId"])
        other_node_claim = self.queue.claim({"workerId": "legacy", "profiles": [LEGACY_PROFILE], "nodeId": "node-2"})
        self.assertEqual(other_node_claim["nodeId"], "node-2")
        # Per-node serialization remains strict even while another node runs.
        self.assertIsNone(self.queue.claim({"workerId": "legacy-2", "profiles": [LEGACY_PROFILE], "nodeId": "cluster"}))
        self.store.update_phase(job["jobId"], "held", error="worker_uncertain")
        with self.assertRaises(Conflict):
            self.store.update_phase(job["jobId"], "cancelled")
        self.assertEqual(self.queue.get(legacy["id"])["status"], "running")

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
