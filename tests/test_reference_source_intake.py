import copy
import base64
import hashlib
import json
import io
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from processing import reference_protocol as p
from processing import viewer_source_attestation as attest
from processing.reference_source_intake_canonical import pipeline_digest
from processing.reference_source_intake import validate_source_intake
from processing.prototype import Queue
from processing.reference_api import ReferenceAPI
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
                    parent_claim=api.store.claim_snapshot(job["jobId"],node_id="cluster",worker_id="worker",
                        worker_instance_id="instance",claim_token=claim["claimToken"])
                    self.assertEqual(parent_claim["sourceIntakeVersion"],2)
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
                    recovered_claim=reopened.claim_snapshot(job["jobId"],node_id="cluster",worker_id="worker",
                        worker_instance_id="instance",claim_token=claim["claimToken"])
                    self.assertEqual(recovered_claim["sourceIntakeVersion"],2)
                    replay_api=ReferenceAPI(reopened,bearer_tokens={"synthetic-staging-token":"staging"},
                        capability_fingerprint="a"*64,options_fingerprint=self.payload["pipeline"]["odm"]["optionsFingerprint"],
                        allowed_odm_options={"numeric-probe"},source_spool=spool,
                        source_attestation_keys=self.trusted_keys,authority_id=queue.authority_id)
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


if __name__ == "__main__":
    unittest.main()
