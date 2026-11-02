"""Synthetic failure/recovery tests for the Processing -> Viewer transfer leg."""
import copy
import hashlib
import hmac
import io
import json
import urllib.parse
import unittest

from processing import reference_protocol as protocol
from processing.viewer_acceptance_transfer import (
    ViewerAcceptanceTransfer, ViewerTransferUnavailable, _service_auth_headers,
)
from tests.viewer_receipt_signer import SyntheticViewerSigner


SECRET = "synthetic-viewer-service-key-material"


class FakeResponse:
    def __init__(self, url, status, body):
        self.status, self._url, self._body = status, url, body
        self.headers = {"Content-Type": "application/json", "Content-Length": str(len(body)),
                        "Content-Encoding": "identity"}

    def geturl(self): return self._url
    def read(self, size=-1):
        if size < 0: size = len(self._body)
        result, self._body = self._body[:size], self._body[size:]
        return result
    def close(self): pass


class FakeViewer:
    def __init__(self, signer):
        self.signer = signer
        self.payload = None
        self.buffers = {}
        self.accepted = None
        self.calls = []
        self.fail_after_first_chunk = False
        self.fail_after_commit = False

    @staticmethod
    def _json(status, value):
        return status, json.dumps(value, separators=(",", ":")).encode()

    def __call__(self, request, timeout):
        method, url = request.get_method(), request.full_url
        parsed = urllib.parse.urlsplit(url)
        path = parsed.path + (("?" + parsed.query) if parsed.query else "")
        body = request.data or b""
        headers = request.header_items()
        h = {key.lower(): value for key, value in headers}
        stamp, nonce = h["x-ltds-timestamp"], h["x-ltds-nonce"]
        digest = hashlib.sha256(body).hexdigest()
        canonical = "\n".join(("ltds-viewer-service-v1", method, path, stamp, nonce, digest)).encode()
        expected_signature = __import__("base64").urlsafe_b64encode(
            hmac.new(SECRET.encode(), canonical, hashlib.sha256).digest()).decode().rstrip("=")
        assert h["x-ltds-key-id"] == "synthetic-service"
        assert h["x-ltds-content-sha256"] == digest
        assert h["x-ltds-signature"] == expected_signature
        self.calls.append((method, path))
        base = "/api/v1/reference/v1/acceptances/receipt-test"

        def response(status, value):
            code, data = self._json(status, value)
            return FakeResponse(url, code, data)

        if method == "GET" and path == base:
            if self.accepted:
                return response(200, self.accepted)
            if not self.payload:
                raise __import__("urllib.error", fromlist=["HTTPError"]).HTTPError(
                    url, 404, "not found", {}, None)
            return response(202, self._transfer())
        if method == "PUT" and path == base:
            self.payload = json.loads(body)
            self.buffers = {item["id"]: bytearray() for item in self.payload["resultManifest"]["objects"]}
            return response(202, self._transfer())
        if method == "PUT" and path.startswith(base + "/objects/"):
            object_id = path[len(base + "/objects/"):].split("?", 1)[0]
            offset = int(urllib.parse.parse_qs(parsed.query)["offset"][0])
            buffer = self.buffers[object_id]
            if len(buffer) != offset:
                return response(409, {"code": "TRANSFER_OFFSET_MISMATCH"})
            buffer.extend(body)
            obj = next(item for item in self.payload["resultManifest"]["objects"] if item["id"] == object_id)
            complete = len(buffer) == obj["byteSize"]
            if self.fail_after_first_chunk:
                self.fail_after_first_chunk = False
                raise OSError("lost chunk response")
            return response(200, {"receivedBytes": len(buffer), "complete": complete,
                                  "state": "object_complete" if complete else "receiving"})
        if method == "POST" and path == base + "/commit":
            if any(len(self.buffers[item["id"]]) != item["byteSize"]
                   for item in self.payload["resultManifest"]["objects"]):
                return response(409, {"code": "TRANSFER_INCOMPLETE"})
            acceptance = self.payload["acceptance"]
            signed = self.signer.receipt(acceptance, self.payload["resultManifest"])
            self.accepted = {"receiptId": acceptance["receiptId"], "state": "accepted",
                "acceptedObjectIds": acceptance["acceptedObjectIds"], "fullyAccepted": True,
                "acceptance": acceptance, "viewerReceipt": signed}
            if self.fail_after_commit:
                self.fail_after_commit = False
                raise OSError("lost commit response")
            return response(201, self.accepted)
        raise AssertionError((method, path))

    def _transfer(self):
        expected = self.payload["expected"]
        objects = []
        for obj in self.payload["resultManifest"]["objects"]:
            count = len(self.buffers[obj["id"]])
            complete = obj["byteSize"] > 0 and count == obj["byteSize"]
            objects.append({"objectId": obj["id"], "expectedBytes": obj["byteSize"],
                "expectedSha256": obj["sha256"], "receivedBytes": count,
                "state": "complete" if complete else ("receiving" if count else "pending")})
        return {"receiptId": self.payload["acceptance"]["receiptId"], "state": "receiving",
            **{key: expected[key] for key in ("jobId", "generation", "environment",
               "inputManifestSha256", "resultManifestSha256")},
            "acceptedObjectIds": self.payload["acceptance"]["acceptedObjectIds"], "objects": objects}


class FakeResultProxy:
    def __init__(self, objects): self.objects = objects; self.ranges = []
    def open(self, job, manifest, object_id, *, range_header=None):
        self.ranges.append(range_header)
        if range_header is None: return io.BytesIO(self.objects[object_id])
        match = range_header.removeprefix("bytes=").split("-")
        start, end = map(int, match)
        return io.BytesIO(self.objects[object_id][start:end + 1])


class ViewerAcceptanceTransferTests(unittest.TestCase):
    def setUp(self):
        self.signer = SyntheticViewerSigner()
        self.objects = {"result-test": b"synthetic-result-bytes", "empty-test": b""}
        self.input_manifest = copy.deepcopy(json.loads((__import__("pathlib").Path(__file__).resolve().parents[1]
            / "contracts/reference-v1.contract.json").read_text())["submission"]["manifest"])
        self.result = {
            "schemaVersion": 1, "jobId": "job-test", "generation": 1, "environment": "staging",
            "pipelineDigest": "c" * 64, "engineVersions": {"odm": "synthetic-1"},
            "objects": [
                {"id": "result-test", "role": "report", "byteSize": len(self.objects["result-test"]),
                 "sha256": hashlib.sha256(self.objects["result-test"]).hexdigest()},
                {"id": "empty-test", "role": "report", "byteSize": 0,
                 "sha256": hashlib.sha256(b"").hexdigest()},
            ],
        }
        self.result["resultManifestSha256"] = protocol.digest(self.result)
        self.job = {
            "jobId": self.result["jobId"], "generation": self.result["generation"],
            "environment": self.result["environment"], "manifestSha256": self.input_manifest["sha256"],
            "pipelineDigest": self.result["pipelineDigest"], "nodeId": "cluster",
            "submission": {"manifest": self.input_manifest}, "resultManifest": self.result,
            "resultManifestSha256": self.result["resultManifestSha256"],
        }
        self.acceptance = {
            "schemaVersion": 1, "receiptId": "receipt-test", "storageReceiptId": "storage-test",
            "jobId": self.job["jobId"], "generation": self.job["generation"],
            "environment": self.job["environment"], "inputManifestSha256": self.job["manifestSha256"],
            "pipelineDigest": self.job["pipelineDigest"], "resultManifestSha256": self.result["resultManifestSha256"],
            "acceptedObjectIds": [obj["id"] for obj in self.result["objects"]],
            "acceptedAt": "2026-11-02T12:00:00.000Z", "validationVersion": protocol.VALIDATION_VERSION,
        }
        self.viewer = FakeViewer(self.signer)
        self.proxy = FakeResultProxy(self.objects)
        self.client = ViewerAcceptanceTransfer("https://viewer.synthetic", key_id="synthetic-service",
            secret=SECRET, trusted_receipt_keys=self.signer.trusted_keys, result_proxy=self.proxy,
            opener=self.viewer, chunk_bytes=2)

    def test_service_auth_matches_viewer_node_golden_vector(self):
        headers = _service_auth_headers(SECRET.encode(), "synthetic-service", "PUT",
            "/api/v1/reference/v1/acceptances/receipt-test?offset=7",
            b'{"contract":"viewer-transfer-v1"}', timestamp=1793620800,
            nonce="nonce_20261102_0000000000000000")
        self.assertEqual(headers, {
            "X-LTDS-Key-Id": "synthetic-service",
            "X-LTDS-Timestamp": "1793620800",
            "X-LTDS-Nonce": "nonce_20261102_0000000000000000",
            "X-LTDS-Content-SHA256": "b3ead6fac9d0b0c1ffef75e74fa1654b81a756d571479b6c8a885483fa8138d8",
            "X-LTDS-Signature": "OGS3kafRYDOPXsKT37fHiozTEH3eR3ijz_HH6GNOzcU",
        })

    def test_full_transfer_is_chunked_and_returns_verified_receipt_pair(self):
        pair = self.client.transfer(self.job, self.acceptance)
        self.assertEqual(pair["acceptance"], self.acceptance)
        self.assertTrue(self.viewer.accepted)
        self.assertEqual({key: bytes(value) for key, value in self.viewer.buffers.items()}, self.objects)
        self.assertGreater(len([call for call in self.viewer.calls if "/objects/" in call[1]]), len(self.result["objects"]))
        self.assertIn(("PUT", "/api/v1/reference/v1/acceptances/receipt-test/objects/empty-test?offset=0"), self.viewer.calls)

    def test_lost_chunk_response_resumes_from_viewer_byte_offset(self):
        self.viewer.fail_after_first_chunk = True
        with self.assertRaises(ViewerTransferUnavailable): self.client.transfer(self.job, self.acceptance)
        first_calls = len([call for call in self.viewer.calls if "/objects/" in call[1]])
        pair = self.client.transfer(self.job, self.acceptance)
        self.assertEqual(pair["acceptance"], self.acceptance)
        self.assertEqual({key: bytes(value) for key, value in self.viewer.buffers.items()}, self.objects)
        self.assertGreater(len([call for call in self.viewer.calls if "/objects/" in call[1]]), first_calls)

    def test_lost_commit_response_recovers_accepted_pair_without_reupload(self):
        self.viewer.fail_after_commit = True
        with self.assertRaises(ViewerTransferUnavailable): self.client.transfer(self.job, self.acceptance)
        upload_calls = len([call for call in self.viewer.calls if "/objects/" in call[1]])
        pair = self.client.transfer(self.job, self.acceptance)
        self.assertEqual(pair["acceptance"], self.acceptance)
        self.assertEqual(len([call for call in self.viewer.calls if "/objects/" in call[1]]), upload_calls)

    def test_unpinned_viewer_receipt_is_rejected_after_commit(self):
        with self.assertRaises(ValueError):
            ViewerAcceptanceTransfer("https://viewer.synthetic", key_id="synthetic-service",
                secret=SECRET, trusted_receipt_keys={"synthetic-viewer-key": {
                    "issuerId": "synthetic-viewer-issuer", "publicKeyPem": "not-a-public-key"}},
                result_proxy=self.proxy, opener=self.viewer, chunk_bytes=2)

    def test_tree_dar_result_is_held_without_any_viewer_request(self):
        self.job["resultManifest"]["engineVersions"] = {"treedar": "test"}
        self.job["resultManifest"]["resultManifestSha256"] = protocol.digest(
            {k: v for k, v in self.job["resultManifest"].items() if k != "resultManifestSha256"})
        self.job["resultManifestSha256"] = self.job["resultManifest"]["resultManifestSha256"]
        self.acceptance["resultManifestSha256"] = self.job["resultManifestSha256"]
        with self.assertRaisesRegex(ViewerTransferUnavailable, "viewer_engine_not_supported"):
            self.client.transfer(self.job, self.acceptance)
        self.assertEqual(self.viewer.calls, [])


if __name__ == "__main__": unittest.main()
