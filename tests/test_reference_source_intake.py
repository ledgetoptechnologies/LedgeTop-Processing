import copy
import base64
import hashlib
import json
import io
import http.client
import os
from pathlib import Path
import ssl
import tempfile
import unittest
import threading
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from processing import reference_protocol as p
from processing import viewer_source_attestation as attest
from processing.reference_source_intake_canonical import pipeline_digest
from processing.reference_source_intake import validate_source_intake
from processing.prototype import Queue
from processing.reference_api import ReferenceAPI
from processing.reference_control import ReferenceControlAPI
from processing.reference_http import create_reference_http_server
from processing.reference_store import PROFILE, ReferenceStore
from processing.source_spool import SourceSpool
from processing.maintenance import Conflict


FIXTURES = Path(__file__).resolve().parents[1] / "contracts" / "reference-v1-fixtures.json"
INTAKE_VECTORS = Path(__file__).resolve().parents[1] / "contracts" / "reference-source-intake-v1-vectors.json"
ATTEMPT = "00000000-0000-4000-8000-000000000001"
DATASET = "00000000-0000-4000-8000-000000000002"
IMAGE_ID = "00000000-0000-4000-8000-000000000010"
GCP_ID = "00000000-0000-4000-8000-000000000020"
TASK = "00000000-0000-4000-8000-000000000030"
AUTHORITY = "00000000-0000-4000-8000-000000000040"


def sha(value):
    return hashlib.sha256(value).hexdigest()


class ReferenceSourceIntakeTests(unittest.TestCase):
    def setUp(self):
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
        from cryptography.hazmat.primitives import serialization
        self.signing_key = Ed25519PrivateKey.generate()
        self.trusted_keys = [{"environment":"staging","issuerId":"viewer-test","keyId":"source-test",
            "principalId":"viewer-staging","publicKeyPem":self.signing_key.public_key().public_bytes(
                serialization.Encoding.PEM,serialization.PublicFormat.SubjectPublicKeyInfo).decode("ascii")}]
        fixtures = json.loads(FIXTURES.read_text(encoding="utf-8"))
        image_bytes = b"abc"
        gcp_bytes = b"id,lat,lon\n1,2,3\n"
        derived_bytes = b"1,2,3\n"
        self.payload = {
            "schemaVersion": 2,
            "submissionId": ATTEMPT,
            "providerTaskId": "00000000-0000-4000-8000-000000000003",
            "viewerTaskId": TASK,
            "sourceManifest": {
                "schemaVersion": 1,
                "id": DATASET,
                "datasetManifestSha256": "d" * 64,
                "sha256": "0" * 64,
                "totalBytes": len(image_bytes) + len(gcp_bytes),
                "objectCount": 2,
                "objects": [
                    {"id": IMAGE_ID, "name": "Survey_001.JPG", "byteSize": len(image_bytes),
                     "sha256": sha(image_bytes), "sourceType": "raw_input",
                     "retentionClass": "release_after_acceptance", "retentionHoldId": None},
                    {"id": GCP_ID, "name": "Survey_GCP.csv", "byteSize": len(gcp_bytes),
                     "sha256": sha(gcp_bytes), "sourceType": "gcp_source_csv",
                     "retentionClass": "permanent_viewer_hold", "retentionHoldId": GCP_ID},
                ],
            },
            "derivedManifest": {
                "schemaVersion": 1,
                "attemptId": ATTEMPT,
                "sha256": "0" * 64,
                "objects": [{
                    "id": "gcp_" + ATTEMPT,
                    "name": "gcp_list.txt",
                    "byteSize": len(derived_bytes),
                    "sha256": sha(derived_bytes),
                    "sourceType": "attempt_derived_gcp_list",
                    "retentionClass": "release_after_acceptance",
                    "retentionHoldId": None,
                    "snapshotAttemptId": ATTEMPT,
                    "snapshotContentSha256": sha(derived_bytes),
                    "snapshotEvidenceSha256": "e" * 64,
                }],
            },
            "manifest": copy.deepcopy(fixtures["submission"]["manifest"]),
            "pipeline": copy.deepcopy(fixtures["submission"]["pipeline"]),
            "capabilityFingerprint": "a" * 64,
        }
        self.payload["manifest"].update(id="manifest-v2-synthetic", totalBytes=0,
                                        objectCount=2, sha256="0" * 64)
        self.payload["manifest"]["objects"][0].update(id=IMAGE_ID, name="Survey_001.JPG", byteSize=len(image_bytes),
                                                        sha256=sha(image_bytes))
        self.payload["manifest"]["objects"].append({
            "id": "gcp_" + ATTEMPT, "name": "gcp_list.txt", "byteSize": len(derived_bytes),
            "sha256": sha(derived_bytes), "kind": "auxiliary", "mediaType": "text/plain",
        })
        self.payload["manifest"]["totalBytes"] = len(image_bytes) + len(derived_bytes)
        self.rehash()

    def _make_reserved_source_job(self, directory, spool, claim_suffix, *, source_range_v2=False):
        worker_token = "synthetic-worker-token-012345"
        queue = Queue(directory)
        queue.allowed_profiles.add(PROFILE)
        self.authority_id = queue.authority_id
        self.payload["pipeline"]["odm"]["options"] = {"numeric-probe": 1e-7}
        store = ReferenceStore(queue)
        control = ReferenceControlAPI(store, viewer_bearers={"synthetic-staging-token": "staging"},
            worker_bearers={worker_token: dict(node_id="cluster", worker_id="worker",
                worker_instance_id="instance")}, capability_fingerprint="a" * 64,
            options_fingerprint=self.payload["pipeline"]["odm"]["optionsFingerprint"],
            allowed_odm_options={"numeric-probe"}, source_spool=spool,
            source_attestation_keys=self.trusted_keys, authority_id=queue.authority_id)
        signed = self.validate(allowed_odm_options={"numeric-probe"})
        request = copy.deepcopy(self.payload)
        request["sourceAttestation"] = signed["sourceAttestation"]
        viewer_headers = {"Authorization": "Bearer synthetic-staging-token"}
        posted = control.handle("POST", "/ltds/v2/jobs", viewer_headers,
            p.canonical_json(request), protected_transport=True)
        self.assertEqual(posted.status, 201, posted.body)
        job = posted.body
        bodies = {IMAGE_ID: b"abc", GCP_ID: b"id,lat,lon\n1,2,3\n", "gcp_" + ATTEMPT: b"1,2,3\n"}
        all_objects = self.payload["sourceManifest"]["objects"] + self.payload["derivedManifest"]["objects"]
        for object_id, body in bodies.items():
            item = next(value for value in all_objects if value["id"] == object_id)
            received = control.viewer.stream_source_object(
                f"/ltds/v2/jobs/{job['jobId']}/source/{object_id}?generation=1",
                {**viewer_headers, "Content-SHA256": item["sha256"]}, io.BytesIO(body), len(body),
                protected_transport=True)
            self.assertEqual(received.status, 201, received.body)
        completion = {"generation": 1, "sourceManifestSha256": self.payload["sourceManifest"]["sha256"],
            "derivedManifestSha256": self.payload["derivedManifest"]["sha256"],
            "pipelineManifestSha256": self.payload["manifest"]["sha256"]}
        completed = control.handle("POST", f"/ltds/v2/jobs/{job['jobId']}/source/complete",
            viewer_headers, p.canonical_json(completion), protected_transport=True)
        self.assertEqual(completed.status, 201, completed.body)
        claim = control.handle("POST", "/ltds/worker/v1/claim", {"Authorization": "Bearer " + worker_token},
            p.canonical_json({"jobId": job["jobId"], "claimRequestId": "source-claim-" + claim_suffix}),
            protected_transport=True)
        self.assertEqual(claim.status, 200, claim.body)
        claim_token = claim.body["claimToken"]
        expires = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(
            timespec="milliseconds").replace("+00:00", "Z")
        reservation = {"reservationId": "source-reservation-" + claim_suffix,
            "generation": claim.body["generation"], "expiresAt": expires,
            "manifestSha256": job["manifestSha256"], "byteBudget": self.payload["manifest"]["totalBytes"],
            "nonce": "source-nonce-" + claim_suffix}
        if source_range_v2:
            from processing.source_range_transfer import build_source_transfer_request
            reservation = build_source_transfer_request(store.get(job['jobId'],private=True),
                reservation_id=reservation["reservationId"], expires_at=expires,
                nonce=reservation["nonce"])
        reserved = control.handle("POST", f"/ltds/worker/v1/jobs/{job['jobId']}/reservation",
            {"Authorization": "Bearer " + worker_token},
            p.canonical_json({"claimToken": claim_token, "transferRequest": reservation}),
            protected_transport=True)
        self.assertEqual(reserved.status, 200, reserved.body)
        return queue, store, control, job, claim_token

    def test_source_grant_hash_does_not_hold_coordinator_lock_and_stale_claim_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "spool"
            root.mkdir(mode=0o700)
            marker = root / ".ltds-source-spool-id"
            marker.write_text("a" * 64, encoding="ascii")
            if os.name != "nt": marker.chmod(0o600)
            mount_patch = patch("processing.source_spool.os.path.ismount", return_value=True)
            mount_patch.start()
            try:
                spool = SourceSpool(root, mount_id="a" * 64, safety_floor_bytes=0,
                    max_object_bytes=1024, chunk_bytes=3)
                queue, store, _control, job, _claim_token = self._make_reserved_source_job(
                    directory, spool, "blocked")
                for invalid_clock in (float("nan"), float("inf"), float("-inf")):
                    with self.assertRaisesRegex(Conflict, "invalid source grant clock"):
                        store.mint_source_transfer_grant(job["jobId"], source_spool=spool,
                            clock=lambda value=invalid_clock: value)
                entered = threading.Event()
                release = threading.Event()
                write_done = threading.Event()
                outcome = {}
                original = spool.verify_object

                def blocked_verify(**kwargs):
                    entered.set()
                    if not release.wait(3):
                        raise AssertionError("test did not release the blocked verifier")
                    return original(**kwargs)

                def mint():
                    try:
                        outcome["grant"] = store.mint_source_transfer_grant(job["jobId"], source_spool=spool)
                    except Exception as error:
                        outcome["error"] = error

                def unrelated_write():
                    try:
                        with queue._db() as db:
                            db.execute("CREATE TABLE IF NOT EXISTS source_grant_lock_probe (value INTEGER)")
                            db.execute("INSERT INTO source_grant_lock_probe(value) VALUES(1)")
                    except Exception as error:
                        outcome["write_error"] = error
                    finally:
                        write_done.set()

                with patch.object(spool, "verify_object", side_effect=blocked_verify):
                    mint_thread = threading.Thread(target=mint, name="grant-hash-blocked")
                    mint_thread.start()
                    try:
                        self.assertTrue(entered.wait(2), "grant verification did not reach the blocked NAS read")
                        write_thread = threading.Thread(target=unrelated_write, name="unrelated-coordinator-write")
                        write_thread.start()
                        self.assertTrue(write_done.wait(1), "coordinator write blocked behind NAS verification")
                        write_thread.join(2)
                        self.assertFalse(write_thread.is_alive(), "unrelated write did not finish")
                        store.update_phase(job["jobId"], "held", error="test_claim_changed")
                    finally:
                        release.set()
                        mint_thread.join(4)
                    self.assertFalse(mint_thread.is_alive(), "grant mint thread leaked")
                self.assertIsInstance(outcome.get("error"), Conflict)
                with queue._db() as db:
                    self.assertEqual(db.execute("SELECT COUNT(*) FROM reference_transfer_grants").fetchone()[0], 0)
                    self.assertEqual(db.execute("SELECT COUNT(*) FROM reference_source_grant_tokens").fetchone()[0], 0)
            finally:
                mount_patch.stop()
                if 'queue' in locals(): queue.close()

    def test_concurrent_identical_source_grant_mints_reconcile_one_persisted_grant(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "spool"
            root.mkdir(mode=0o700)
            marker = root / ".ltds-source-spool-id"
            marker.write_text("a" * 64, encoding="ascii")
            if os.name != "nt": marker.chmod(0o600)
            mount_patch = patch("processing.source_spool.os.path.ismount", return_value=True)
            mount_patch.start()
            queue = None
            try:
                spool = SourceSpool(root, mount_id="a" * 64, safety_floor_bytes=0,
                    max_object_bytes=1024, chunk_bytes=3)
                queue, store, _control, job, _claim_token = self._make_reserved_source_job(
                    directory, spool, "parallel")
                rendezvous = threading.Barrier(2)
                original = spool.verify_object
                outcomes = []
                errors = []

                def paired_verify(**kwargs):
                    if kwargs["object_id"] == IMAGE_ID:
                        rendezvous.wait(timeout=3)
                    return original(**kwargs)

                def mint():
                    try:
                        outcomes.append(store.mint_source_transfer_grant(job["jobId"], source_spool=spool))
                    except Exception as error:
                        errors.append(error)

                with patch.object(spool, "verify_object", side_effect=paired_verify):
                    threads = [threading.Thread(target=mint, name=f"parallel-grant-{index}") for index in range(2)]
                    for thread in threads: thread.start()
                    for thread in threads: thread.join(8)
                self.assertTrue(all(not thread.is_alive() for thread in threads), "grant mint thread leaked")
                self.assertEqual(errors, [])
                self.assertEqual(len(outcomes), 2)
                self.assertEqual(outcomes[0], outcomes[1])
                with queue._db() as db:
                    self.assertEqual(db.execute("SELECT COUNT(*) FROM reference_transfer_grants").fetchone()[0], 1)
                    self.assertEqual(db.execute("SELECT COUNT(*) FROM reference_source_grant_tokens").fetchone()[0], 1)
            finally:
                mount_patch.stop()
                if queue is not None: queue.close()

    def test_orphan_grant_token_history_blocks_mint_before_and_after_verification(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "spool"
            root.mkdir(mode=0o700)
            marker = root / ".ltds-source-spool-id"
            marker.write_text("a" * 64, encoding="ascii")
            if os.name != "nt": marker.chmod(0o600)
            mount_patch = patch("processing.source_spool.os.path.ismount", return_value=True)
            mount_patch.start()
            queue = None
            try:
                spool = SourceSpool(root, mount_id="a" * 64, safety_floor_bytes=0,
                    max_object_bytes=1024, chunk_bytes=3)
                queue, store, _control, job, _claim_token = self._make_reserved_source_job(
                    directory, spool, "orphan")
                with queue._db() as db:
                    db.execute('''INSERT INTO reference_source_grant_tokens(token_sha256,job_id,generation)
                        VALUES(?,?,1)''', ("f" * 64, job["jobId"]))
                with self.assertRaisesRegex(Conflict, "orphan source grant token history"):
                    store.mint_source_transfer_grant(job["jobId"], source_spool=spool)
                with queue._db() as db:
                    self.assertEqual(db.execute("SELECT COUNT(*) FROM reference_source_grant_tokens").fetchone()[0], 1)
                    self.assertEqual(db.execute("SELECT COUNT(*) FROM reference_transfer_grants").fetchone()[0], 0)
                    db.execute("DELETE FROM reference_source_grant_tokens WHERE token_sha256=?", ("f" * 64,))

                original = spool.verify_object
                injected = False

                def inject_orphan_while_verifying(**kwargs):
                    nonlocal injected
                    if not injected:
                        injected = True
                        with queue._db() as db:
                            db.execute('''INSERT INTO reference_source_grant_tokens(token_sha256,job_id,generation)
                                VALUES(?,?,1)''', ("e" * 64, job["jobId"]))
                    return original(**kwargs)

                with patch.object(spool, "verify_object", side_effect=inject_orphan_while_verifying):
                    with self.assertRaisesRegex(Conflict, "orphan source grant token history"):
                        store.mint_source_transfer_grant(job["jobId"], source_spool=spool)
                with queue._db() as db:
                    self.assertEqual(db.execute("SELECT COUNT(*) FROM reference_source_grant_tokens").fetchone()[0], 1)
                    self.assertEqual(db.execute("SELECT token_sha256 FROM reference_source_grant_tokens").fetchone()[0], "e" * 64)
                    self.assertEqual(db.execute("SELECT COUNT(*) FROM reference_transfer_grants").fetchone()[0], 0)
            finally:
                mount_patch.stop()
                if queue is not None: queue.close()
    def rehash(self):
        for key in ("sourceManifest", "derivedManifest"):
            manifest = self.payload[key]
            manifest["sha256"] = p.digest({k: v for k, v in manifest.items() if k != "sha256"})
        self.payload["manifest"]["sha256"] = p.manifest_sha256(self.payload["manifest"])

    def validate(self, payload=None, **kwargs):
        payload = copy.deepcopy(payload or self.payload)
        proof = {"schemaVersion":2,"protocol":attest.PROTOCOL,"issuerId":"viewer-test","keyId":"source-test",
            "principalId":"viewer-staging","authorityId":getattr(self,"authority_id",AUTHORITY),"environment":"staging",
            "submissionId":payload["submissionId"],"viewerAttemptId":payload["submissionId"],
            "viewerTaskId":payload["viewerTaskId"],"providerTaskId":payload["providerTaskId"],
            "activationId":"00000000-0000-4000-8000-000000000050",
            "preparedEnvelopeSha256":"1"*64,"sourceSnapshotSha256":"2"*64,
            "sourceManifestSha256":payload["sourceManifest"]["sha256"],
            "derivedManifestSha256":payload["derivedManifest"]["sha256"],
            "pipelineManifestSha256":payload["manifest"]["sha256"],
            "pipelineDigest":pipeline_digest(payload["pipeline"])}
        message=attest.DOMAIN+p.canonical_json(proof)
        signature=self.signing_key.sign(message)
        payload["sourceAttestation"]={"proof":proof,
            "signature":base64.urlsafe_b64encode(signature).decode("ascii").rstrip("="),
            "attestationSha256":hashlib.sha256(message).hexdigest()}
        options = dict(capability_fingerprint="a" * 64,
                       options_fingerprint=self.payload["pipeline"]["odm"]["optionsFingerprint"],
                       allowed_odm_options=set(), trusted_source_keys=self.trusted_keys,
                       authority_id=getattr(self,"authority_id",AUTHORITY))
        options.update(kwargs)
        return validate_source_intake(payload, environment="staging",
            **options)

    def test_valid_raw_and_derived_closures_bind_to_existing_pipeline_manifest(self):
        result = self.validate()
        self.assertEqual(result["environment"], "staging")
        self.assertEqual(result["submissionId"], ATTEMPT)
        self.assertEqual(result["sourceManifestSha256"], self.payload["sourceManifest"]["sha256"])
        self.assertEqual(result["derivedManifestSha256"], self.payload["derivedManifest"]["sha256"])
        self.assertEqual(result["manifestSha256"], self.payload["manifest"]["sha256"])
        self.assertEqual(result["pipelineDigest"], pipeline_digest(self.payload["pipeline"]))
        self.assertEqual(len(result["sourceManifest"]["objects"]), 2)
        self.assertTrue(self.payload["manifest"]["objects"][-1]["id"].startswith("gcp_"))
        result["sourceManifest"]["objects"].clear()
        self.assertEqual(len(self.payload["sourceManifest"]["objects"]), 2)

    def _large_dji_payload(self, count):
        payload = copy.deepcopy(self.payload)
        source_objects = []
        pipeline_objects = []
        base = 0x10000000000040008000000000000000
        for index in range(count):
            object_id = str(__import__('uuid').UUID(int=base + index))
            name = f"DJI_{index:05d}_{'X' * 230}.JPG"
            source_objects.append({"id": object_id, "name": name, "byteSize": 1,
                "sha256": "a" * 64, "sourceType": "raw_input",
                "retentionClass": "release_after_acceptance", "retentionHoldId": None})
            pipeline_objects.append({"id": object_id, "name": name, "byteSize": 1,
                "sha256": "a" * 64, "kind": "image", "mediaType": "image/jpeg"})
        payload["sourceManifest"].update(totalBytes=count, objectCount=count, objects=source_objects)
        payload["manifest"].update(id="large-dji-synthetic", revision=1, totalBytes=count,
            objectCount=count, inputCounts={"images": count, "lidarObjects": 0, "points": 0},
            objects=pipeline_objects)
        payload["derivedManifest"]["objects"] = []
        self.payload = payload
        self.rehash()
        return copy.deepcopy(self.payload)

    def test_v2_large_signed_dji_metadata_admits_without_processing(self):
        from processing.reference_limits import V2_SOURCE_INTAKE_BYTES
        from processing.reference_source_intake import validate_source_intake
        payload = self._large_dji_payload(10_000)
        normalized = self.validate(payload)
        request = copy.deepcopy(payload)
        request["sourceAttestation"] = normalized["sourceAttestation"]
        body = p.canonical_json(request)
        self.assertGreater(len(body), p.MAX_BODY_BYTES)
        self.assertLessEqual(len(body), V2_SOURCE_INTAKE_BYTES)
        pipeline_v1 = {key: payload[key] for key in
                       ("schemaVersion", "submissionId", "manifest", "pipeline", "capabilityFingerprint")}
        self.assertGreater(len(p.canonical_json(pipeline_v1)), p.MAX_BODY_BYTES)
        with self.assertRaisesRegex(p.ProtocolError, "configured metadata limit"):
            p.validate_submission(pipeline_v1, environment="staging",
                capability_fingerprint="a" * 64, allowed_odm_options=set())

        class Spool:
            safety_floor_bytes = 0
            max_object_bytes = 10_000_000
            chunk_bytes = 1_000_000
            def available_bytes(self):
                return 1_000_000_000
            def check_capacity(self, required_bytes, *, reserved_bytes=0):
                self.asserted = (required_bytes, reserved_bytes)
        spool = Spool()
        with tempfile.TemporaryDirectory() as directory:
            queue = Queue(directory)
            queue.allowed_profiles.add(PROFILE)
            try:
                store = ReferenceStore(queue)
                api = ReferenceAPI(store, bearer_tokens={"synthetic-staging-token": "staging"},
                    capability_fingerprint="a" * 64,
                    options_fingerprint=payload["pipeline"]["odm"]["optionsFingerprint"],
                    source_spool=spool, source_attestation_keys=self.trusted_keys,
                    authority_id=AUTHORITY)
                response = api.handle("POST", "/ltds/v2/jobs",
                    {"Authorization": "Bearer synthetic-staging-token"}, body,
                    protected_transport=True)
                self.assertEqual(response.status, 201, response.body)
                self.assertEqual(response.body["phase"], "source_receiving")
                self.assertEqual(response.body["sourceState"], "receiving")
                self.assertEqual(normalized["sourceManifest"]["objectCount"], 10_000)
                self.assertEqual(len(store.list()), 1)
                capabilities = api.handle("GET", "/ltds/v2/capabilities",
                    {"Authorization": "Bearer synthetic-staging-token"}, protected_transport=True)
                self.assertEqual(capabilities.body["sourceIntake"]["maxMetadataBytes"],
                                 V2_SOURCE_INTAKE_BYTES)
                self.assertEqual(capabilities.body["sourceIntake"]["maxManifestBytes"],
                                 V2_SOURCE_INTAKE_BYTES)
                oversized = api.handle("POST", "/ltds/v2/jobs",
                    {"Authorization": "Bearer synthetic-staging-token"},
                    b" " * (V2_SOURCE_INTAKE_BYTES + 1), protected_transport=True)
                self.assertEqual((oversized.status, oversized.body),
                                 (400, {"error": "invalid_request"}))
            finally:
                queue.close()
        # The same valid serialized envelope is accepted by the bounded V2 validator.
        self.assertEqual(validate_source_intake(body, environment="staging",
            capability_fingerprint="a" * 64,
            options_fingerprint=payload["pipeline"]["odm"]["optionsFingerprint"],
            allowed_odm_options=set(), trusted_source_keys=self.trusted_keys,
            authority_id=AUTHORITY)["sourceManifest"]["objectCount"], 10_000)

    def test_v2_object_count_and_v2_metadata_size_are_bounded(self):
        from processing.reference_limits import V2_SOURCE_INTAKE_BYTES
        oversized_count = self._large_dji_payload(10_001)
        with self.assertRaisesRegex(p.ProtocolError, "invalid source object count"):
            self.validate(oversized_count)
        with self.assertRaises(p.ProtocolError):
            p.decode_submission(b" " * (V2_SOURCE_INTAKE_BYTES + 1),
                                max_bytes=V2_SOURCE_INTAKE_BYTES)

    def test_v1_decoder_remains_four_mib_and_v2_keeps_strict_json(self):
        from processing.reference_limits import V2_SOURCE_INTAKE_BYTES
        with self.assertRaisesRegex(p.ProtocolError, "body exceeds configured"):
            p.decode_submission(b" " * (p.MAX_BODY_BYTES + 1))
        for body in (b'{"x":1,"x":2}', b'{"x":NaN}'):
            with self.assertRaises(p.ProtocolError):
                p.decode_submission(body, max_bytes=V2_SOURCE_INTAKE_BYTES)

    def test_empty_derived_manifest_is_a_valid_attempt_bound_closure(self):
        self.payload["derivedManifest"]["objects"] = []
        self.payload["manifest"]["objects"].pop()
        self.payload["manifest"]["objectCount"] = 1
        self.payload["manifest"]["totalBytes"] = 3
        self.rehash()
        result = self.validate()
        self.assertEqual(result["derivedManifestSha256"], self.payload["derivedManifest"]["sha256"])

    def test_cross_language_canonical_manifest_vectors(self):
        vector = json.loads(INTAKE_VECTORS.read_text(encoding="utf-8"))
        source = vector["sourceManifest"]
        derived = vector["derivedManifest"]
        self.assertEqual(p.digest({k: v for k, v in source.items() if k != "sha256"}),
                         vector["sourceManifestSha256"])
        self.assertEqual(p.digest({k: v for k, v in derived.items() if k != "sha256"}),
                         vector["derivedManifestSha256"])
        self.assertEqual(p.digest(vector["pipeline"]), vector["pipelineDigest"])

    def test_attempt_snapshot_cannot_be_replayed_under_another_attempt(self):
        self.payload["submissionId"] = "00000000-0000-4000-8000-000000000004"
        self.payload["providerTaskId"] = "00000000-0000-4000-8000-000000000005"
        with self.assertRaisesRegex(p.ProtocolError, "attempt mismatch"):
            self.validate()

    def test_source_manifest_rejects_wrong_digest_count_order_or_total(self):
        cases = []
        item = copy.deepcopy(self.payload)
        item["sourceManifest"]["sha256"] = "f" * 64
        cases.append(item)
        item = copy.deepcopy(self.payload)
        item["sourceManifest"]["objectCount"] += 1
        item["sourceManifest"]["sha256"] = p.digest({k: v for k, v in item["sourceManifest"].items() if k != "sha256"})
        cases.append(item)
        item = copy.deepcopy(self.payload)
        item["sourceManifest"]["objects"].reverse()
        item["sourceManifest"]["sha256"] = p.digest({k: v for k, v in item["sourceManifest"].items() if k != "sha256"})
        cases.append(item)
        item = copy.deepcopy(self.payload)
        item["sourceManifest"]["totalBytes"] += 1
        item["sourceManifest"]["sha256"] = p.digest({k: v for k, v in item["sourceManifest"].items() if k != "sha256"})
        cases.append(item)
        for candidate in cases:
            with self.subTest(candidate=candidate["sourceManifest"]), self.assertRaises(p.ProtocolError):
                self.validate(candidate)

    def test_permanent_hold_must_be_exact_file_id_and_gcp_csv_is_held(self):
        for updates in (
            {"retentionHoldId": "different-file"},
            {"retentionClass": "release_after_acceptance", "retentionHoldId": None},
            {"name": "Survey_GCP.txt"},
        ):
            candidate = copy.deepcopy(self.payload)
            candidate["sourceManifest"]["objects"][1].update(updates)
            candidate["sourceManifest"]["sha256"] = p.digest({k: v for k, v in candidate["sourceManifest"].items() if k != "sha256"})
            with self.subTest(updates=updates), self.assertRaises(p.ProtocolError):
                self.validate(candidate)

    def test_source_names_allow_unicode_but_never_path_syntax(self):
        candidate = copy.deepcopy(self.payload)
        candidate["sourceManifest"]["objects"][1]["name"] = "Terreno_área.csv"
        candidate["sourceManifest"]["sha256"] = p.digest({k: v for k, v in candidate["sourceManifest"].items() if k != "sha256"})
        self.assertEqual(self.validate(candidate)["sourceManifest"]["objects"][1]["name"], "Terreno_área.csv")
        for unsafe in ("../raw.csv", "folder/raw.csv", "C:\\raw.csv", "bad\nname.csv"):
            candidate["sourceManifest"]["objects"][1]["name"] = unsafe
            candidate["sourceManifest"]["sha256"] = p.digest({k: v for k, v in candidate["sourceManifest"].items() if k != "sha256"})
            with self.subTest(name=unsafe), self.assertRaises(p.ProtocolError):
                self.validate(candidate)

    def test_derived_file_content_snapshot_and_identity_are_bound(self):
        changes = (
            ("id", "gcp_00000000-0000-4000-8000-000000000099"),
            ("snapshotAttemptId", "00000000-0000-4000-8000-000000000099"),
            ("snapshotContentSha256", "f" * 64),
            ("snapshotEvidenceSha256", "not-a-hash"),
        )
        for field, value in changes:
            candidate = copy.deepcopy(self.payload)
            candidate["derivedManifest"]["objects"][0][field] = value
            candidate["derivedManifest"]["sha256"] = p.digest({k: v for k, v in candidate["derivedManifest"].items() if k != "sha256"})
            with self.subTest(field=field), self.assertRaises(p.ProtocolError):
                self.validate(candidate)

    def test_pipeline_must_be_an_exact_subset_and_never_send_raw_gcp_csv(self):
        candidate = copy.deepcopy(self.payload)
        candidate["manifest"]["objects"][0]["sha256"] = "f" * 64
        candidate["manifest"]["sha256"] = p.manifest_sha256(candidate["manifest"])
        with self.assertRaisesRegex(p.ProtocolError, "differs from retained input"):
            self.validate(candidate)

        candidate = copy.deepcopy(self.payload)
        raw_csv = next(item for item in candidate["sourceManifest"]["objects"] if item["sourceType"] == "gcp_source_csv")
        candidate["manifest"]["objects"].append({
            "id": raw_csv["id"], "name": raw_csv["name"], "byteSize": raw_csv["byteSize"],
            "sha256": raw_csv["sha256"], "kind": "auxiliary", "mediaType": "text/csv",
        })
        candidate["manifest"]["objectCount"] += 1
        candidate["manifest"]["totalBytes"] += raw_csv["byteSize"]
        candidate["manifest"]["sha256"] = p.manifest_sha256(candidate["manifest"])
        with self.assertRaisesRegex(p.ProtocolError, "retained but not sent"):
            self.validate(candidate)

    def test_unknown_fields_and_capability_drift_fail_closed(self):
        candidate = copy.deepcopy(self.payload)
        candidate["sourceManifest"]["unexpected"] = True
        with self.assertRaises(p.ProtocolError):
            self.validate(candidate)
        with self.assertRaisesRegex(p.ProtocolError, "stale capability"):
            self.validate(capability_fingerprint="f" * 64)

    def test_authenticated_post_streamed_objects_completion_and_exact_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)/"spool";root.mkdir(mode=0o700)
            marker=root/".ltds-source-spool-id";marker.write_text("a"*64,encoding="ascii")
            if os.name!="nt":marker.chmod(0o600)
            self.mount_patch=patch("processing.source_spool.os.path.ismount",return_value=True)
            self.mount_patch.start()
            try:
                spool=SourceSpool(root,mount_id="a"*64,safety_floor_bytes=0,max_object_bytes=1024,chunk_bytes=3)
                queue=Queue(directory);queue.allowed_profiles.add(PROFILE)
                try:
                    self.authority_id=queue.authority_id
                    self.payload["pipeline"]["odm"]["options"]={"numeric-probe":1e-7}
                    api=ReferenceAPI(ReferenceStore(queue),bearer_tokens={"synthetic-staging-token":"staging"},
                        capability_fingerprint="a"*64,options_fingerprint=self.payload["pipeline"]["odm"]["optionsFingerprint"],
                        allowed_odm_options={"numeric-probe"},source_spool=spool,
                        source_attestation_keys=self.trusted_keys,authority_id=queue.authority_id)
                    signed_payload=self.validate(allowed_odm_options={"numeric-probe"})
                    request=copy.deepcopy(self.payload)
                    request["sourceAttestation"]=signed_payload["sourceAttestation"]
                    response=api.handle("POST","/ltds/v2/jobs",{"Authorization":"Bearer synthetic-staging-token"},
                        p.canonical_json(request),protected_transport=True)
                    self.assertEqual(response.status,201,response.body)
                    job=response.body
                    self.assertEqual((job["sourceIntakeVersion"],job["phase"],job["sourceState"],job["derivedState"]),
                        (2,"source_receiving","receiving","receiving"))
                    with self.assertRaises(Conflict):
                        api.store.claim(job["jobId"],node_id="cluster",worker_id="worker",
                            worker_instance_id="instance",claim_request_id="before-receipts")
                    body_map={IMAGE_ID:b"abc",GCP_ID:b"id,lat,lon\n1,2,3\n","gcp_"+ATTEMPT:b"1,2,3\n"}
                    for object_id,body in body_map.items():
                        item=next(obj for obj in self.payload["sourceManifest"]["objects"]+
                            self.payload["derivedManifest"]["objects"] if obj["id"]==object_id)
                        receipt=api.stream_source_object(f"/ltds/v2/jobs/{job['jobId']}/source/{object_id}?generation=1",
                            {"Authorization":"Bearer synthetic-staging-token","Content-SHA256":item["sha256"]},
                            io.BytesIO(body),len(body),protected_transport=True)
                        self.assertEqual(receipt.status,201,receipt.body)
                    completion={"generation":1,"sourceManifestSha256":self.payload["sourceManifest"]["sha256"],
                        "derivedManifestSha256":self.payload["derivedManifest"]["sha256"],
                        "pipelineManifestSha256":self.payload["manifest"]["sha256"]}
                    completed=api.handle("POST",f"/ltds/v2/jobs/{job['jobId']}/source/complete",
                        {"Authorization":"Bearer synthetic-staging-token"},p.canonical_json(completion),protected_transport=True)
                    self.assertEqual(completed.status,201,completed.body)
                    self.assertEqual((completed.body["phase"],completed.body["sourceState"],completed.body["derivedState"]),
                        ("queued","retained","retained"))
                    self.assertEqual(len(completed.body["sourceObjectReceipts"]),2)
                    self.assertEqual(len(completed.body["derivedObjectReceipts"]),1)
                    claim=api.store.claim(job["jobId"],node_id="cluster",worker_id="worker",
                        worker_instance_id="instance",claim_request_id="after-receipts")
                    self.assertEqual((claim["sourceIntakeVersion"],claim["submission"]["sourceIntakeVersion"]),(2,2))
                    binding = claim['sourceAcceptanceBinding']
                    self.assertEqual(binding['viewerTaskId'], self.payload['viewerTaskId'])
                    self.assertEqual(binding['coordinatorAuthorityId'], queue.authority_id)
                    self.assertEqual(binding['sourceReceiptSha256'], completed.body['sourceReceiptSha256'])
                    self.assertEqual(binding['preparedEnvelopeSha256'], request['sourceAttestation']['proof']['preparedEnvelopeSha256'])
                    parent_claim=api.store.claim_snapshot(job["jobId"],node_id="cluster",worker_id="worker",
                        worker_instance_id="instance",claim_token=claim["claimToken"])
                    self.assertEqual(parent_claim["sourceIntakeVersion"],2)
                    completion_ack=copy.deepcopy(completed.body)
                    expires=(datetime.now(timezone.utc)+timedelta(minutes=5)).isoformat(
                        timespec="milliseconds").replace("+00:00","Z")
                    transfer={"reservationId":"source-completion-replay-reservation",
                        "generation":claim["generation"],"expiresAt":expires,
                        "manifestSha256":job["manifestSha256"],
                        "byteBudget":self.payload["manifest"]["totalBytes"],"nonce":"completion-replay-nonce"}
                    api.store.reserve(job["jobId"],transfer,node_id="cluster")
                    api.store.mint_source_transfer_grant(job["jobId"],source_spool=spool)
                    api.store.update_phase(job["jobId"],"fetching")
                    api.store.update_phase(job["jobId"],"processing")
                    completion_retry=api.handle("POST",f"/ltds/v2/jobs/{job['jobId']}/source/complete",
                        {"Authorization":"Bearer synthetic-staging-token"},p.canonical_json(completion),
                        protected_transport=True)
                    self.assertEqual((completion_retry.status,completion_retry.body),(200,completion_ack))
                    self.assertEqual(p.canonical_json(completion_retry.body),p.canonical_json(completion_ack))
                    live=api.handle("GET",f"/ltds/v2/jobs/{job['jobId']}",
                        {"Authorization":"Bearer synthetic-staging-token"},protected_transport=True)
                    self.assertEqual((live.body["status"],live.body["phase"]),("running","processing"))
                    replay=api.handle("POST","/ltds/v2/jobs",{"Authorization":"Bearer synthetic-staging-token"},
                        p.canonical_json(request),protected_transport=True)
                    self.assertEqual((replay.status,replay.body["jobId"]),(200,job["jobId"]))
                    self.assertEqual(replay.body["sourceReceipt"],completed.body["sourceReceipt"])
                    queue.close()
                    queue=Queue(directory);queue.allowed_profiles.add(PROFILE)
                    reopened=ReferenceStore(queue)
                    self.assertEqual(reopened.reconcile_source_spool(spool),{"jobs":1,"adopted":0,"held":0})
                    stored=reopened.get(job["jobId"],private=True)
                    self.assertEqual(stored["submission"]["sourceIntakeVersion"],2)
                    self.assertEqual(stored['sourceAcceptanceBinding'], binding)
                    recovered_claim=reopened.claim_snapshot(job["jobId"],node_id="cluster",worker_id="worker",
                        worker_instance_id="instance",claim_token=claim["claimToken"])
                    self.assertEqual(recovered_claim["sourceIntakeVersion"],2)
                    replay_api=ReferenceAPI(reopened,bearer_tokens={"synthetic-staging-token":"staging"},
                        capability_fingerprint="a"*64,options_fingerprint=self.payload["pipeline"]["odm"]["optionsFingerprint"],
                        allowed_odm_options={"numeric-probe"},source_spool=spool,
                        source_attestation_keys=self.trusted_keys,authority_id=queue.authority_id)
                    completion_replay=replay_api.handle("POST",f"/ltds/v2/jobs/{job['jobId']}/source/complete",
                        {"Authorization":"Bearer synthetic-staging-token"},p.canonical_json(completion),
                        protected_transport=True)
                    self.assertEqual((completion_replay.status,completion_replay.body),(200,completion_ack))
                    self.assertEqual(p.canonical_json(completion_replay.body),p.canonical_json(completion_ack))
                    after_restart=replay_api.handle("GET",f"/ltds/v2/jobs/{job['jobId']}",
                        {"Authorization":"Bearer synthetic-staging-token"},protected_transport=True)
                    self.assertEqual((after_restart.body["status"],after_restart.body["phase"]),
                        ("held","held"))
                    recovered=replay_api.handle("POST","/ltds/v2/jobs",{"Authorization":"Bearer synthetic-staging-token"},
                        p.canonical_json(request),protected_transport=True)
                    self.assertEqual((recovered.status,recovered.body["sourceReceipt"]),
                        (200,completed.body["sourceReceipt"]))
                    changed=copy.deepcopy(self.payload);changed["pipeline"]["odm"]["options"]["numeric-probe"]=2e-7
                    changed_signature=self.validate(changed,allowed_odm_options={"numeric-probe"})
                    changed["sourceAttestation"]=changed_signature["sourceAttestation"]
                    conflict=replay_api.handle("POST","/ltds/v2/jobs",{"Authorization":"Bearer synthetic-staging-token"},
                        p.canonical_json(changed),protected_transport=True)
                    self.assertEqual(conflict.status,409)
                finally:
                    if queue is not None:queue.close()
            finally:
                self.mount_patch.stop()


    def test_source_broker_grant_download_is_claim_scoped_and_receipt_verified(self):
        from processing.reference_transfer import OBJECT_PREFIX
        worker_token="synthetic-worker-token-012345"
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)/"spool";root.mkdir(mode=0o700)
            marker=root/".ltds-source-spool-id";marker.write_text("a"*64,encoding="ascii")
            if os.name!="nt":marker.chmod(0o600)
            mount_patch=patch("processing.source_spool.os.path.ismount",return_value=True);mount_patch.start()
            try:
                spool=SourceSpool(root,mount_id="a"*64,safety_floor_bytes=0,max_object_bytes=1024,chunk_bytes=3)
                queue=Queue(directory);queue.allowed_profiles.add(PROFILE)
                try:
                    self.authority_id=queue.authority_id
                    self.payload["pipeline"]["odm"]["options"]={"numeric-probe":1e-7}
                    store=ReferenceStore(queue)
                    control=ReferenceControlAPI(store,viewer_bearers={"synthetic-staging-token":"staging"},
                        worker_bearers={worker_token:dict(node_id="cluster",worker_id="worker",
                            worker_instance_id="instance")},capability_fingerprint="a"*64,
                        options_fingerprint=self.payload["pipeline"]["odm"]["optionsFingerprint"],
                        allowed_odm_options={"numeric-probe"},source_spool=spool,
                        source_attestation_keys=self.trusted_keys,authority_id=queue.authority_id)
                    signed=self.validate(allowed_odm_options={"numeric-probe"})
                    request=copy.deepcopy(self.payload);request["sourceAttestation"]=signed["sourceAttestation"]
                    viewer_headers={"Authorization":"Bearer synthetic-staging-token"}
                    posted=control.handle("POST","/ltds/v2/jobs",viewer_headers,p.canonical_json(request),
                        protected_transport=True)
                    self.assertEqual(posted.status,201,posted.body);job=posted.body
                    bodies={IMAGE_ID:b"abc",GCP_ID:b"id,lat,lon\n1,2,3\n","gcp_"+ATTEMPT:b"1,2,3\n"}
                    all_objects=self.payload["sourceManifest"]["objects"]+self.payload["derivedManifest"]["objects"]
                    for object_id,body in bodies.items():
                        item=next(value for value in all_objects if value["id"]==object_id)
                        received=control.viewer.stream_source_object(
                            f"/ltds/v2/jobs/{job['jobId']}/source/{object_id}?generation=1",
                            {**viewer_headers,"Content-SHA256":item["sha256"]},io.BytesIO(body),len(body),
                            protected_transport=True)
                        self.assertEqual(received.status,201,received.body)
                    completion={"generation":1,"sourceManifestSha256":self.payload["sourceManifest"]["sha256"],
                        "derivedManifestSha256":self.payload["derivedManifest"]["sha256"],
                        "pipelineManifestSha256":self.payload["manifest"]["sha256"]}
                    completed=control.handle("POST",f"/ltds/v2/jobs/{job['jobId']}/source/complete",
                        viewer_headers,p.canonical_json(completion),protected_transport=True)
                    self.assertEqual(completed.status,201,completed.body)
                    claim=control.handle("POST","/ltds/worker/v1/claim",{"Authorization":"Bearer "+worker_token},
                        p.canonical_json({"jobId":job["jobId"],"claimRequestId":"source-claim-1"}),
                        protected_transport=True)
                    self.assertEqual(claim.status,200,claim.body)
                    claim_token=claim.body["claimToken"]
                    expires=(datetime.now(timezone.utc)+timedelta(minutes=5)).isoformat(timespec="milliseconds").replace("+00:00","Z")
                    reservation={"reservationId":"source-reservation-1","generation":claim.body["generation"],
                        "expiresAt":expires,"manifestSha256":job["manifestSha256"],
                        "byteBudget":self.payload["manifest"]["totalBytes"],"nonce":"source-nonce-1"}
                    reserved=control.handle("POST",f"/ltds/worker/v1/jobs/{job['jobId']}/reservation",
                        {"Authorization":"Bearer "+worker_token},p.canonical_json({"claimToken":claim_token,
                            "transferRequest":reservation}),protected_transport=True)
                    self.assertEqual(reserved.status,200,reserved.body)
                    expiry_time=datetime.fromisoformat(expires.replace("Z","+00:00")).timestamp()
                    wall_time=datetime.now(timezone.utc).timestamp()
                    rollback_clock=iter([wall_time,wall_time-1])
                    with self.assertRaisesRegex(Conflict,"source grant clock moved backwards"):
                        store.mint_source_transfer_grant(job["jobId"],source_spool=spool,
                            clock=lambda:next(rollback_clock))
                    self.assertIsNone(store.private_transfer_grant(job["jobId"]))
                    grant_clock=iter([datetime.now(timezone.utc).timestamp(),expiry_time+1])
                    with self.assertRaisesRegex(Conflict,"expired during source verification"):
                        store.mint_source_transfer_grant(job["jobId"],source_spool=spool,
                            clock=lambda:next(grant_clock))
                    self.assertIsNone(store.private_transfer_grant(job["jobId"]))
                    granted=control.handle("GET",f"/ltds/worker/v1/jobs/{job['jobId']}/transfer-grant",
                        {"Authorization":"Bearer "+worker_token,"X-Claim-Token":claim_token},
                        protected_transport=True)
                    self.assertEqual(granted.status,200,granted.body)
                    grant=granted.body
                    self.assertEqual(set(grant["objectIds"]),{IMAGE_ID,"gcp_"+ATTEMPT})
                    phase=control.handle("POST",f"/ltds/worker/v1/jobs/{job['jobId']}/phase",
                        {"Authorization":"Bearer "+worker_token},p.canonical_json({"claimToken":claim_token,
                            "phase":"fetching"}),protected_transport=True)
                    self.assertEqual(phase.status,200,phase.body)
                    from cryptography import x509
                    from cryptography.hazmat.primitives import hashes,serialization
                    from cryptography.hazmat.primitives.asymmetric import rsa
                    from cryptography.x509.oid import NameOID
                    import ipaddress
                    tls_key=rsa.generate_private_key(public_exponent=65537,key_size=2048)
                    subject=x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,"127.0.0.1")])
                    now=datetime.now(timezone.utc).replace(tzinfo=None)
                    certificate=(x509.CertificateBuilder().subject_name(subject).issuer_name(subject)
                        .public_key(tls_key.public_key()).serial_number(x509.random_serial_number())
                        .not_valid_before(now-timedelta(minutes=1)).not_valid_after(now+timedelta(minutes=10))
                        .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]),
                            critical=False).sign(tls_key,hashes.SHA256()))
                    cert_path=Path(directory)/"broker-cert.pem";key_path=Path(directory)/"broker-key.pem"
                    cert_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
                    key_path.write_bytes(tls_key.private_bytes(serialization.Encoding.PEM,
                        serialization.PrivateFormat.TraditionalOpenSSL,serialization.NoEncryption()))
                    server_tls=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);server_tls.load_cert_chain(cert_path,key_path)
                    client_tls=ssl.create_default_context(cafile=str(cert_path))
                    server=create_reference_http_server(control,ssl_context=server_tls,request_timeout=2)
                    thread=threading.Thread(target=server.serve_forever,kwargs={"poll_interval":0.02},daemon=True)
                    thread.start()
                    try:
                        def get(path,token):
                            connection=http.client.HTTPSConnection(*server.server_address,timeout=3,context=client_tls)
                            connection.request("GET",path,headers={"Authorization":"Bearer "+token,
                                "Accept-Encoding":"identity"})
                            response=connection.getresponse();data=response.read();status=response.status
                            headers=dict(response.getheaders());connection.close()
                            return status,data,headers
                        status,data,headers=get(OBJECT_PREFIX+IMAGE_ID,grant["token"])
                        self.assertEqual((status,data),(200,b"abc"))
                        self.assertEqual(headers["Content-Length"],"3")
                        self.assertEqual(headers["Content-Type"],"application/octet-stream")
                        from processing.reference_transfer import BrokerClient,validate_grant
                        private_job=store.get(job["jobId"],private=True)
                        verified_grant=validate_grant(grant,job=private_job)
                        worker_scratch=Path(directory)/"worker-scratch";worker_scratch.mkdir(mode=0o700)
                        downloaded=BrokerClient(f"https://127.0.0.1:{server.server_address[1]}",
                            ssl_context=client_tls).download_object(verified_grant,IMAGE_ID,
                                worker_scratch/"image.input")
                        self.assertEqual(downloaded.read_bytes(),b"abc")
                        self.assertEqual(get(OBJECT_PREFIX+IMAGE_ID,"x"*48)[0],409)
                        self.assertEqual(get(OBJECT_PREFIX+GCP_ID,grant["token"])[0],409)
                        expired=datetime.fromisoformat(grant["expiresAt"].replace("Z","+00:00")).timestamp()+1
                        with patch("processing.reference_source_intake_store.time.time",return_value=expired):
                            self.assertEqual(get(OBJECT_PREFIX+IMAGE_ID,grant["token"])[0],409)
                    finally:
                        server.shutdown();server.server_close();thread.join(2)
                finally:
                    queue.close()
            finally:
                mount_patch.stop()


if __name__ == "__main__":
    unittest.main()
