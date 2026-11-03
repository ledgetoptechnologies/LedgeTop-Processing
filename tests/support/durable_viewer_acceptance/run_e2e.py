import copy, hashlib, io, json, os, ssl, sys, urllib.request
from pathlib import Path
from datetime import datetime, timezone

temp = Path(os.environ['LT_TEST_DIR']).resolve()
sys.path.insert(0, os.environ['LTDS_PROCESSING_REPO'])
from processing import reference_protocol as protocol
from processing.viewer_acceptance_transfer import ViewerAcceptanceTransfer, ViewerTransferUnavailable

origin = os.environ['LT_TEST_ORIGIN']
ctx = ssl.create_default_context(cafile=str(temp / "ca.pem"))
with urllib.request.urlopen(origin + "/_test/receipt-public-key", context=ctx, timeout=10) as response:
    key_data = json.loads(response.read())
trusted = {key_data["keyId"]: {"issuerId": key_data["issuerId"], "publicKeyPem": key_data["publicKeyPem"]}}

contract = json.loads((Path(os.environ['LTDS_PROCESSING_REPO']) / "contracts/reference-v1.contract.json").read_text(encoding="utf-8"))
input_manifest = copy.deepcopy(contract["submission"]["manifest"])
objects = {"result-test": b"synthetic-result-bytes-for-resume", "empty-test": b""}
result_manifest = {
    "schemaVersion": 1, "jobId": "job-e2e-synthetic", "generation": 1, "environment": "staging",
    "pipelineDigest": "c" * 64, "engineVersions": {"odm": "synthetic-e2e"},
    "objects": [
        {"id": key, "role": "report", "byteSize": len(data), "sha256": hashlib.sha256(data).hexdigest()}
        for key, data in objects.items()
    ],
}
result_manifest["resultManifestSha256"] = protocol.digest(result_manifest)
job = {
    "jobId": result_manifest["jobId"], "generation": result_manifest["generation"],
    "environment": result_manifest["environment"], "manifestSha256": input_manifest["sha256"],
    "pipelineDigest": result_manifest["pipelineDigest"], "nodeId": "synthetic-node",
    "submission": {"manifest": input_manifest}, "resultManifest": result_manifest,
    "resultManifestSha256": result_manifest["resultManifestSha256"],
}
acceptance = {
    "schemaVersion": 1, "receiptId": "receipt-e2e-synthetic", "storageReceiptId": "storage-e2e-synthetic",
    "jobId": job["jobId"], "generation": job["generation"], "environment": job["environment"],
    "inputManifestSha256": job["manifestSha256"], "pipelineDigest": job["pipelineDigest"],
    "resultManifestSha256": result_manifest["resultManifestSha256"],
    "acceptedObjectIds": [item["id"] for item in result_manifest["objects"]],
    "acceptedAt": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
    "validationVersion": protocol.VALIDATION_VERSION,
}
class Proxy:
    def open(self, _job, _manifest, object_id, *, range_header=None):
        data = objects[object_id]
        if range_header is None:
            return io.BytesIO(data)
        start, end = map(int, range_header.removeprefix("bytes=").split("-"))
        return io.BytesIO(data[start:end + 1])

client = ViewerAcceptanceTransfer(origin, key_id="synthetic-service",
    secret="synthetic-viewer-service-key-material", trusted_receipt_keys=trusted,
    result_proxy=Proxy(), ssl_context=ctx, chunk_bytes=7, timeout=10)

try:
    client.transfer(job, acceptance)
    raise AssertionError("expected first chunk response to be lost")
except ViewerTransferUnavailable as exc:
    first = str(exc)

try:
    client.transfer(job, acceptance)
    raise AssertionError("expected commit response to be lost")
except ViewerTransferUnavailable as exc:
    second = str(exc)

with urllib.request.urlopen(origin + "/_test/stats", context=ctx, timeout=10) as response:
    before_recovery = json.loads(response.read())
pair = client.transfer(job, acceptance)
with urllib.request.urlopen(origin + "/_test/stats", context=ctx, timeout=10) as response:
    after_recovery = json.loads(response.read())
assert pair["acceptance"] == acceptance, "recovered acceptance differs"
assert pair["viewerReceipt"]["jobId"] == job["jobId"], "signed receipt binding differs"
assert before_recovery["chunkResponsesDropped"] == 1, before_recovery
assert before_recovery["commitResponsesDropped"] == 1, before_recovery
assert before_recovery["commits"] == 1, before_recovery
assert before_recovery["chunksAcknowledged"] > 1, before_recovery
assert after_recovery == before_recovery, "status recovery unexpectedly uploaded or committed again"
print(json.dumps({"result": "PASS", "first_failure": first, "second_failure": second,
    "chunks": before_recovery["chunksAcknowledged"], "commits": before_recovery["commits"],
    "recovery_without_reupload": True, "receipt_verified": True}, sort_keys=True))

