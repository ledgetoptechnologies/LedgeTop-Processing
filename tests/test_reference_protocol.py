import copy
import json
import unittest
from pathlib import Path

from processing import reference_protocol as p


FIXTURES = Path(__file__).resolve().parents[1] / "contracts" / "reference-v1-fixtures.json"


class ReferenceProtocolTests(unittest.TestCase):
    def setUp(self):
        self.fixtures = json.loads(FIXTURES.read_text(encoding="utf-8"))
        self.payload = copy.deepcopy(self.fixtures["submission"])

    def validate(self, payload=None, **kwargs):
        return p.validate_submission(payload or self.payload, environment="staging",
                                     capability_fingerprint="a" * 64, **kwargs)

    def rehash(self):
        self.payload["manifest"]["sha256"] = p.manifest_sha256(self.payload["manifest"])

    def test_canonical_integer_fixture_and_detached_result(self):
        self.assertEqual(p.manifest_sha256(self.payload["manifest"]), self.fixtures["manifestSha256"])
        self.assertEqual(p.canonical_json({k: v for k, v in self.payload["manifest"].items()
                                         if k != "sha256"}).decode(), self.fixtures["canonicalManifest"])
        result = self.validate()
        self.assertEqual(result["requestDigest"], self.fixtures["requestDigest"])
        result["manifest"]["objects"].clear()
        self.assertEqual(len(self.payload["manifest"]["objects"]), 1)

    def test_unknown_environment_and_duplicate_wire_fields(self):
        self.payload["environment"] = "production"
        with self.assertRaises(p.ProtocolError):
            self.validate()
        with self.assertRaises(p.ProtocolError):
            p.decode_submission('{"schemaVersion":1,"schemaVersion":1}')
        with self.assertRaises(p.ProtocolError):
            p.decode_submission('{"n":Infinity}')

    def test_invalid_byte_counts(self):
        for value in (True, 0, -1, 2**53, float("inf"), 1.5):
            with self.subTest(value=value), self.assertRaises(p.ProtocolError):
                obj = copy.deepcopy(self.payload)
                obj["manifest"]["objects"][0]["byteSize"] = value
                self.validate(obj)

    def test_paths_duplicates_and_total(self):
        for name in ("../a.jpg", "C:\\a.jpg", "a/b.jpg", "CON.jpg", "a.jpg."):
            self.payload["manifest"]["objects"][0]["name"] = name
            self.rehash()
            with self.subTest(name=name), self.assertRaises(p.ProtocolError):
                self.validate()
        self.setUp()
        twin = copy.deepcopy(self.payload["manifest"]["objects"][0])
        twin.update(id="00000000-0000-4000-8000-000000000004", name="PHOTO.JPG")
        self.payload["manifest"]["objects"].append(twin)
        self.payload["manifest"]["objectCount"] = 2
        self.payload["manifest"]["inputCounts"]["images"] = 2
        self.payload["manifest"]["totalBytes"] *= 2
        self.rehash()
        with self.assertRaises(p.ProtocolError):
            self.validate()

    def test_tree_admission_requires_available_lidar_and_provenance(self):
        tree = self.payload["pipeline"]["treedar"]
        tree.update(requested=True, inputObjectIds=[self.payload["manifest"]["objects"][0]["id"]],
                    provenanceReceiptId="00000000-0000-4000-8000-000000000005")
        with self.assertRaisesRegex(p.ProtocolError, "unavailable"):
            self.validate()
        with self.assertRaisesRegex(p.ProtocolError, "LAS/LAZ"):
            self.validate(available_engines=("odm", "treedar"))
        self.payload["manifest"]["objects"][0].update(kind="lidar", name="cloud.las")
        self.payload["manifest"]["inputCounts"].update(images=0, lidarObjects=1)
        self.rehash()
        self.validate(available_engines=("odm", "treedar"))
        tree["provenanceReceiptId"] = None
        with self.assertRaises(p.ProtocolError):
            self.validate(available_engines=("odm", "treedar"))

    def test_gaussian_rejected_before_manifest_validation(self):
        self.payload["pipeline"]["gaussian_splat"]["requested"] = True
        self.payload["manifest"] = {}
        with self.assertRaisesRegex(p.ProtocolError, "Gaussian"):
            self.validate()

    def test_dynamic_options_fail_closed_and_fingerprints(self):
        self.payload["pipeline"]["odm"]["options"] = {"pc-quality": "high"}
        with self.assertRaises(p.ProtocolError):
            self.validate()
        self.validate(allowed_odm_options={"pc-quality"})
        with self.assertRaises(p.ProtocolError):
            self.validate(allowed_odm_options={"pc-quality"}, options_fingerprint="f" * 64)

    def test_odm_paths_rejected_but_proj_and_epsg_allowed(self):
        for value in ("https://example.org/x", "file:///x", "C:\\data\\a", "/tmp/data",
                      "../data", "a/../data", "--file=/tmp/a"):
            self.payload["pipeline"]["odm"]["options"] = {"projection": value}
            with self.subTest(value=value), self.assertRaises(p.ProtocolError):
                self.validate(allowed_odm_options={"projection"})
        for value in ("EPSG:32616", "+proj=utm +zone=16 +datum=WGS84 +units=m +no_defs"):
            self.payload["pipeline"]["odm"]["options"] = {"projection": value}
            self.validate(allowed_odm_options={"projection"})

    def test_declared_counts_safe_ascii_ids_and_names(self):
        for change in ({"objectCount": True}, {"objectCount": 2},
                       {"inputCounts": {"images": 0, "lidarObjects": 0, "points": 0}},
                       {"inputCounts": {"images": 1, "lidarObjects": 0, "points": 2**53}},
                       {"id": "../manifest"}):
            self.setUp()
            self.payload["manifest"].update(change)
            self.rehash()
            with self.subTest(change=change), self.assertRaises(p.ProtocolError):
                self.validate()
        self.setUp()
        self.payload["manifest"]["objects"][0]["name"] = "caf\u00e9.jpg"
        self.rehash()
        with self.assertRaises(p.ProtocolError):
            self.validate()

    def test_zero_result_report_allowed_geometry_rejected(self):
        for role, allowed in (("report", True), ("pointcloud", False)):
            result = copy.deepcopy(self.fixtures["resultManifest"])
            result["objects"][0].update(role=role, byteSize=0)
            result["resultManifestSha256"] = p.digest({k: v for k, v in result.items()
                                                       if k != "resultManifestSha256"})
            kwargs = dict(job_id=result["jobId"], generation=1, environment="staging",
                          pipeline_digest=self.fixtures["pipelineDigest"])
            if allowed:
                p.validate_result_manifest(result, **kwargs)
            else:
                with self.assertRaises(p.ProtocolError):
                    p.validate_result_manifest(result, **kwargs)

    def test_engine_versions_require_nonspace_printable_ascii(self):
        for version, allowed in (("sha256:" + "a" * 64, True), ("3.5.2", True),
                                 ("version with space", False), ("v\u00e9rsion", False),
                                 ("v\t1", False), ("x" * 129, False)):
            result = copy.deepcopy(self.fixtures["resultManifest"])
            result["engineVersions"] = {"odm": version}
            result["resultManifestSha256"] = p.digest({k: v for k, v in result.items()
                                                       if k != "resultManifestSha256"})
            kwargs = dict(job_id=result["jobId"], generation=1, environment="staging",
                          pipeline_digest=self.fixtures["pipelineDigest"])
            with self.subTest(version=version):
                if allowed:
                    p.validate_result_manifest(result, **kwargs)
                else:
                    with self.assertRaises(p.ProtocolError):
                        p.validate_result_manifest(result, **kwargs)

    def test_body_and_object_limits_and_total_mismatch(self):
        with self.assertRaises(p.ProtocolError):
            p.decode_submission(" " * (p.MAX_BODY_BYTES + 1))
        self.payload["manifest"]["totalBytes"] = 1
        self.rehash()
        with self.assertRaisesRegex(p.ProtocolError, "totalBytes"):
            self.validate()
        self.payload["manifest"]["objects"] *= p.MAX_OBJECTS + 1
        with self.assertRaisesRegex(p.ProtocolError, "object count"):
            self.validate()

    def test_result_exact_identity_and_digest(self):
        result = copy.deepcopy(self.fixtures["resultManifest"])
        with self.assertRaisesRegex(p.ProtocolError, "identity"):
            p.validate_result_manifest(result, job_id=result["jobId"], generation=2,
                environment="staging", pipeline_digest=self.fixtures["pipelineDigest"])
        result["objects"][0]["byteSize"] += 1
        with self.assertRaisesRegex(p.ProtocolError, "digest"):
            p.validate_result_manifest(result, job_id=result["jobId"], generation=1,
                environment="staging", pipeline_digest=self.fixtures["pipelineDigest"])

    def test_result_and_partial_acceptance(self):
        result = p.validate_result_manifest(self.fixtures["resultManifest"],
            job_id=self.fixtures["resultManifest"]["jobId"], generation=1,
            environment="staging", pipeline_digest=self.fixtures["pipelineDigest"])
        receipt = copy.deepcopy(self.fixtures["acceptance"])
        p.validate_acceptance(receipt, result_manifest=result,
                              input_manifest_sha256=self.fixtures["manifestSha256"])
        for key, value in (("generation", 2), ("environment", "production"),
                           ("pipelineDigest", "0" * 64), ("resultManifestSha256", "0" * 64),
                           ("inputManifestSha256", "0" * 64),
                           ("acceptedObjectIds", receipt["acceptedObjectIds"] * 2)):
            changed = copy.deepcopy(receipt)
            changed[key] = value
            with self.subTest(key=key), self.assertRaises(p.ProtocolError):
                p.validate_acceptance(changed, result_manifest=result,
                                      input_manifest_sha256=self.fixtures["manifestSha256"])
        self.assertEqual(p.receipt_tombstone_digest(receipt), self.fixtures["receiptTombstoneDigest"])

    def test_submission_id_conflicts_require_stateful_caller(self):
        first = self.validate()
        self.payload["pipeline"]["odm"]["options"] = {"pc-quality": "high"}
        second = self.validate(allowed_odm_options={"pc-quality"})
        self.assertEqual(first["submissionId"], second["submissionId"])
        self.assertNotEqual(first["requestDigest"], second["requestDigest"])


if __name__ == "__main__":
    unittest.main()
