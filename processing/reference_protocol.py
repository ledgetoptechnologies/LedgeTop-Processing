"""Pure draft validators for NAS reference protocol v1; no transfer or job state.

LiDAR admission checks metadata only. A worker must independently validate file
contents, CRS, units and usable ground returns before running TreeDar.
"""
from __future__ import annotations

import copy
import hashlib
import json
import math
import re
import unicodedata
from datetime import datetime
from uuid import UUID

PREFIX = "/ltds/v1"
MAX_BODY_BYTES = 4 * 1024 * 1024
MAX_OBJECTS = 10_000
MAX_SAFE_INTEGER = 2**53 - 1
VALIDATION_VERSION = "ltds-reference-v1"
TREEDAR_ADMISSION_VERSION = "treedar-lidar-v1"
_HEX = re.compile(r"[0-9a-f]{64}\Z")


class ProtocolError(ValueError):
    """A request or receipt violates the reference protocol contract."""


def _require(condition, message):
    if not condition:
        raise ProtocolError(message)


def _closed(value, fields, label):
    _require(type(value) is dict, f"{label} must be an object")
    _require(set(value) == set(fields), f"{label} has missing or unknown fields")


def _uuid(value, label):
    _require(type(value) is str, f"{label} must be a UUID")
    try:
        parsed = UUID(value)
    except (ValueError, AttributeError):
        raise ProtocolError(f"{label} must be a UUID") from None
    _require(str(parsed) == value, f"{label} must be a lowercase canonical UUID")
    return value


def _integer(value, label, *, allow_zero=False):
    _require(type(value) is int and (0 if allow_zero else 1) <= value <= MAX_SAFE_INTEGER,
             f"{label} must be a {'nonnegative' if allow_zero else 'positive'} safe integer")


def _slug(value, label):
    _require(type(value) is str and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}", value),
             f"{label} must be a safe ASCII identifier")


def _hash(value, label):
    _require(type(value) is str and _HEX.fullmatch(value) is not None,
             f"{label} must be lowercase SHA-256 hex")


def _string(value, label, maximum=255):
    _require(type(value) is str and 0 < len(value) <= maximum
             and not any(unicodedata.category(c).startswith("C") for c in value),
             f"{label} must be bounded printable text")


def _environment(value):
    _require(value in ("production", "staging"), "invalid trusted environment")


def _basename(value):
    _string(value, "name")
    _require(value.isascii(), "v1 name must be ASCII")
    _require(value not in (".", "..") and not any(c in value for c in '/\\:<>"|?*')
             and value[-1] not in " ." and value[0] != " ", "unsafe basename")
    _require(value.split(".")[0].upper() not in
             {"CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(1, 10)),
              *(f"LPT{i}" for i in range(1, 10))}, "reserved basename")


def canonical_json(value):
    """UTF-8, sorted keys, compact separators, literal Unicode, no NaN/Infinity.

    Manifest fields contain only strings, integers and lists/objects. Its hashes
    are cross-language stable without floating-point serialization assumptions.
    General request and pipeline digests are server-owned, not client hash inputs.
    """
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"),
                          ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError, UnicodeError, RecursionError):
        raise ProtocolError("body must be finite UTF-8 JSON") from None


def digest(value):
    return hashlib.sha256(canonical_json(value)).hexdigest()


def manifest_sha256(manifest):
    return digest({k: v for k, v in manifest.items() if k != "sha256"})


def decode_submission(body, *, max_bytes=MAX_BODY_BYTES):
    """Decode bounded wire JSON while rejecting duplicate object keys."""
    _require(isinstance(body, (str, bytes)), "body must be JSON text")
    from .reference_limits import V2_SOURCE_INTAKE_BYTES
    _require(type(max_bytes) is int and 0 < max_bytes <= V2_SOURCE_INTAKE_BYTES,
             "invalid bounded decoder limit")
    try:
        encoded = body.encode("utf-8") if isinstance(body, str) else body
        _require(len(encoded) <= max_bytes, "body exceeds configured metadata limit")

        def pairs(items):
            result = {}
            for key, value in items:
                _require(key not in result, "duplicate JSON key")
                result[key] = value
            return result

        return json.loads(encoded.decode("utf-8"), object_pairs_hook=pairs,
                          parse_constant=lambda _: (_ for _ in ()).throw(
                              ProtocolError("nonfinite JSON number")))
    except (UnicodeError, json.JSONDecodeError, RecursionError):
        raise ProtocolError("invalid JSON body") from None


def validate_submission(payload, *, environment, capability_fingerprint,
                        available_engines=("odm",), options_fingerprint=None,
                        allowed_odm_options=None, max_body_bytes=MAX_BODY_BYTES):
    """Validate metadata and return a detached normalized record.

    Caller must enforce submission-ID idempotency against durable state and
    supply capabilities/environment from trusted authentication/configuration.
    """
    from .reference_limits import V2_SOURCE_INTAKE_BYTES
    _require(type(max_body_bytes) is int and 0 < max_body_bytes <= V2_SOURCE_INTAKE_BYTES,
             "invalid bounded submission limit")
    if isinstance(payload, (str, bytes)):
        payload = decode_submission(payload, max_bytes=max_body_bytes)
    _environment(environment)
    _require(len(canonical_json(payload)) <= max_body_bytes, "body exceeds configured metadata limit")
    _closed(payload, ("schemaVersion", "submissionId", "manifest", "pipeline",
                      "capabilityFingerprint"), "submission")
    _require(type(payload["schemaVersion"]) is int and payload["schemaVersion"] == 1,
             "unsupported schemaVersion")
    _uuid(payload["submissionId"], "submissionId")
    _hash(payload["capabilityFingerprint"], "capabilityFingerprint")
    _require(payload["capabilityFingerprint"] == capability_fingerprint,
             "stale capability fingerprint")
    pipeline = payload["pipeline"]
    _closed(pipeline, ("odm", "treedar", "gaussian_splat"), "pipeline")
    _closed(pipeline["gaussian_splat"], ("requested",), "gaussian_splat")
    _require(pipeline["gaussian_splat"]["requested"] is False,
             "Gaussian splatting is disabled")
    manifest = payload["manifest"]
    _closed(manifest, ("id", "revision", "sha256", "totalBytes", "objectCount", "inputCounts", "objects"), "manifest")
    _slug(manifest["id"], "manifest.id")
    _integer(manifest["revision"], "manifest.revision")
    _integer(manifest["totalBytes"], "manifest.totalBytes")
    _hash(manifest["sha256"], "manifest.sha256")
    objects = manifest["objects"]
    _require(type(objects) is list and 0 < len(objects) <= MAX_OBJECTS, "invalid object count")
    _integer(manifest["objectCount"], "objectCount")
    _require(manifest["objectCount"] == len(objects), "objectCount mismatch")
    counts = manifest["inputCounts"]
    _closed(counts, ("images", "lidarObjects", "points"), "inputCounts")
    for key, value in counts.items():
        _integer(value, key, allow_zero=True)
    ids, names = {}, set()
    for obj in objects:
        _closed(obj, ("id", "name", "byteSize", "sha256", "kind", "mediaType"), "object")
        _slug(obj["id"], "object.id")
        _basename(obj["name"])
        _require(obj["id"] not in ids and obj["name"].casefold() not in names,
                 "duplicate object ID or basename")
        ids[obj["id"]] = obj
        names.add(obj["name"].casefold())
        _integer(obj["byteSize"], "object.byteSize")
        _hash(obj["sha256"], "object.sha256")
        _require(obj["kind"] in ("image", "lidar", "auxiliary"), "invalid object kind")
        _string(obj["mediaType"], "mediaType", 127)
    _require(sum(obj["byteSize"] for obj in objects) == manifest["totalBytes"], "totalBytes mismatch")
    _require(counts["images"] == sum(obj["kind"] == "image" for obj in objects)
             and counts["lidarObjects"] == sum(obj["kind"] == "lidar" for obj in objects),
             "inputCounts mismatch")
    _require(manifest_sha256(manifest) == manifest["sha256"], "manifest digest mismatch")
    odm = pipeline["odm"]
    _closed(odm, ("requested", "options", "optionsFingerprint"), "odm")
    _require(type(odm["requested"]) is bool and type(odm["options"]) is dict, "invalid ODM request")
    _hash(odm["optionsFingerprint"], "optionsFingerprint")
    if options_fingerprint is not None:
        _require(odm["optionsFingerprint"] == options_fingerprint, "stale options fingerprint")
    for key, value in odm["options"].items():
        _require(type(key) is str and re.fullmatch(r"[a-z][a-z0-9-]{0,79}", key), "invalid ODM option name")
        _require(allowed_odm_options is not None and key in allowed_odm_options,
                 "unsupported ODM option")
        _require(value is None or type(value) in (str, bool, int, float), "ODM option must be a JSON scalar")
        if type(value) is str:
            _require(not re.search(r"[A-Za-z][A-Za-z0-9+.-]*://|(?:^|[\s=])[A-Za-z]:[\\/]|(?:^|[\s=])[\\/]|(?:^|[\\/])\.\.(?:[\\/]|$)", value)
                     and "\\" not in value,
                     "ODM option cannot contain URL, absolute path or traversal")
        if type(value) in (int, float):
            _require(abs(value) <= MAX_SAFE_INTEGER and math.isfinite(value), "invalid ODM numeric option")
    treedar = pipeline["treedar"]
    _closed(treedar, ("requested", "inputObjectIds", "provenanceReceiptId"), "treedar")
    _require(type(treedar["requested"]) is bool and type(treedar["inputObjectIds"]) is list,
             "invalid TreeDar request")
    input_ids = treedar["inputObjectIds"]
    for item in input_ids:
        _slug(item, "TreeDar input ID")
    _require(len(set(input_ids)) == len(input_ids), "duplicate TreeDar input ID")
    if treedar["provenanceReceiptId"] is not None:
        _slug(treedar["provenanceReceiptId"], "provenanceReceiptId")
    if treedar["requested"]:
        _require("treedar" in available_engines, "TreeDar unavailable")
        _require(input_ids and treedar["provenanceReceiptId"] is not None, "TreeDar requires provenance and inputs")
        for item in input_ids:
            _require(item in ids, "unknown TreeDar input")
            obj = ids[item]
            _require(obj["kind"] == "lidar" and
                     obj["name"].lower().endswith((".las", ".laz")),
                     "TreeDar requires explicit LAS/LAZ input")
    else:
        _require(not input_ids and treedar["provenanceReceiptId"] is None, "inactive TreeDar contains inputs")
    _require(not odm["requested"] or "odm" in available_engines, "ODM unavailable")
    _require(odm["requested"] or treedar["requested"], "no engine requested")
    result = copy.deepcopy(payload)
    result.update(environment=environment, requestDigest=digest(payload),
                  manifestSha256=manifest["sha256"], pipelineDigest=digest(pipeline))
    return result


def validate_result_manifest(payload, *, job_id, generation, environment, pipeline_digest):
    _closed(payload, ("schemaVersion", "jobId", "generation", "environment", "resultManifestSha256",
                      "engineVersions", "objects", "pipelineDigest"), "result manifest")
    _environment(environment)
    _require(type(payload["schemaVersion"]) is int and payload["schemaVersion"] == 1,
             "unsupported result schemaVersion")
    _slug(payload["jobId"], "jobId")
    _integer(payload["generation"], "generation")
    _hash(payload["pipelineDigest"], "pipelineDigest")
    _require((payload["jobId"], payload["generation"], payload["environment"], payload["pipelineDigest"])
             == (job_id, generation, environment, pipeline_digest), "result identity mismatch")
    _require(type(payload["engineVersions"]) is dict and payload["engineVersions"], "missing engine versions")
    for key, value in payload["engineVersions"].items():
        _require(key in ("odm", "treedar"), "invalid result engine")
        _require(type(value) is str and 1 <= len(value) <= 128
                 and all(0x21 <= ord(char) <= 0x7e for char in value),
                 "engine version must be bounded non-space printable ASCII")
    _require(type(payload["objects"]) is list and 0 < len(payload["objects"]) <= MAX_OBJECTS,
             "invalid result object count")
    ids = set()
    for obj in payload["objects"]:
        _closed(obj, ("id", "role", "byteSize", "sha256"), "result object")
        _slug(obj["id"], "result object ID")
        _require(obj["id"] not in ids, "duplicate result object ID")
        ids.add(obj["id"])
        _string(obj["role"], "role", 80)
        _integer(obj["byteSize"], "byteSize", allow_zero=obj["role"] not in
                 ("orthophoto", "pointcloud", "mesh", "dsm", "dtm", "dem"))
        _hash(obj["sha256"], "sha256")
    _hash(payload["resultManifestSha256"], "resultManifestSha256")
    expected = digest({k: v for k, v in payload.items() if k != "resultManifestSha256"})
    _require(payload["resultManifestSha256"] == expected, "result manifest digest mismatch")
    return copy.deepcopy(payload)


def validate_acceptance(payload, *, result_manifest, input_manifest_sha256):
    """Validate a receipt against an already validated exact result manifest."""
    _closed(payload, ("schemaVersion", "receiptId", "storageReceiptId", "jobId", "generation",
                      "environment", "inputManifestSha256", "pipelineDigest", "resultManifestSha256",
                      "acceptedObjectIds", "acceptedAt", "validationVersion"), "acceptance")
    _require(type(payload["schemaVersion"]) is int and payload["schemaVersion"] == 1, "unsupported schemaVersion")
    for key in ("receiptId", "storageReceiptId", "jobId"):
        _slug(payload[key], key)
    _integer(payload["generation"], "generation")
    for key in ("inputManifestSha256", "pipelineDigest", "resultManifestSha256"):
        _hash(payload[key], key)
    for key in ("jobId", "generation", "environment", "pipelineDigest", "resultManifestSha256"):
        _require(payload[key] == result_manifest[key], f"acceptance {key} mismatch")
    _require(payload["inputManifestSha256"] == input_manifest_sha256, "acceptance input digest mismatch")
    _require(payload["validationVersion"] == VALIDATION_VERSION, "unsupported validation version")
    accepted = payload["acceptedObjectIds"]
    _require(type(accepted) is list and len(accepted) <= MAX_OBJECTS, "invalid accepted objects")
    for item in accepted:
        _slug(item, "accepted object ID")
    _require(len(set(accepted)) == len(accepted) and set(accepted) <=
             {obj["id"] for obj in result_manifest["objects"]}, "invalid accepted subset")
    _string(payload["acceptedAt"], "acceptedAt", 40)
    try:
        parsed = datetime.fromisoformat(payload["acceptedAt"].replace("Z", "+00:00"))
        _require(parsed.utcoffset() is not None, "acceptedAt requires timezone")
    except ValueError:
        raise ProtocolError("invalid acceptedAt") from None
    return copy.deepcopy(payload)


def receipt_tombstone_digest(validated_receipt):
    """Stable content identity only; durable replay/cleanup state is caller-owned."""
    return digest(validated_receipt)
