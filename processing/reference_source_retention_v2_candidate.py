"""Fixture-only verifier for the proposed Viewer source-retention-v2 wire form.

This module verifies a signed claim against caller-supplied trust material. It
does not install a key registry, establish rollback-resistant lineage, prove a
physical NAS pin, or authorize release/cleanup. No production code imports it.
"""
from __future__ import annotations

import base64
import copy
import hashlib
import re
from datetime import datetime

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from . import reference_protocol as p

MAX_TXG = 18_446_744_073_709_551_615
RECEIPT_DOMAIN = b"LTDS-SOURCE-RETENTION-V2\0"
CLOSURE_DOMAIN = b"LTDS-SOURCE-CLOSURE-V2\0"
PAYLOAD_FIELDS = (
    "schemaVersion", "receiptType", "receiptId", "storageReceiptId",
    "operationId", "issuerId", "keyId", "keyEpoch", "ownerInstanceId",
    "ledgerEpoch", "jobId", "generation", "environment",
    "inputManifestSha256", "pipelineDigest", "datasetId", "snapshotId",
    "snapshotCreateTxg", "holdId", "objects", "sourceClosureSha256",
    "retainedAt", "validationVersion", "retentionGuarantee",
)
ENVELOPE_FIELDS = ("payload", "signatureAlgorithm", "signature")
SOURCE_INTENT_FIELDS = (
    "operationId", "jobId", "generation", "environment", "manifestSha256",
    "objects",
)
OBJECT_FIELDS = ("id", "byteSize", "sha256")
MANIFEST_FIELDS = (
    "id", "revision", "sha256", "totalBytes", "objectCount", "inputCounts",
    "objects",
)
MANIFEST_OBJECT_FIELDS = (
    "id", "name", "byteSize", "sha256", "kind", "mediaType",
)
COUNT_FIELDS = ("images", "lidarObjects", "points")
_SLUG = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z")
_HASH = re.compile(r"[a-f0-9]{64}\Z")
_DATASET_SEGMENT = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}\Z")
_TXG = re.compile(r"[1-9][0-9]{0,19}\Z")
_B64URL = re.compile(r"[A-Za-z0-9_-]{86}\Z")


class SourceRetentionV2CandidateError(p.ProtocolError):
    """The fixture-only candidate receipt or its supplied trust material failed."""


def _need(condition: bool, message: str) -> None:
    if not condition:
        raise SourceRetentionV2CandidateError(message)


def _closed(value, fields, label: str) -> None:
    _need(type(value) is dict and set(value) == set(fields), f"invalid {label}")


def _slug(value, label: str) -> None:
    _need(type(value) is str and _SLUG.fullmatch(value) is not None, f"invalid {label}")


def _hash(value, label: str) -> None:
    _need(type(value) is str and _HASH.fullmatch(value) is not None, f"invalid {label}")


def _timestamp(value: str) -> datetime:
    _need(type(value) is str and re.fullmatch(
        r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z", value
    ) is not None, "invalid retainedAt")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        raise SourceRetentionV2CandidateError("invalid retainedAt") from None
    _need(parsed.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z" == value,
          "noncanonical retainedAt")
    return parsed


def _dataset(value: str) -> None:
    _need(type(value) is str and 3 <= len(value.encode("ascii", "ignore")) <= 512
          and value.isascii() and not value.startswith("/") and not value.endswith("/"),
          "invalid datasetId")
    segments = value.split("/")
    _need(all(s not in ("", ".", "..") and _DATASET_SEGMENT.fullmatch(s) for s in segments),
          "invalid datasetId")


def _manifest(manifest: dict) -> dict:
    _closed(manifest, MANIFEST_FIELDS, "input manifest")
    _slug(manifest["id"], "manifest id")
    p._integer(manifest["revision"], "manifest revision")
    _hash(manifest["sha256"], "manifest digest")
    p._integer(manifest["totalBytes"], "manifest total bytes", allow_zero=True)
    p._integer(manifest["objectCount"], "manifest object count")
    _closed(manifest["inputCounts"], COUNT_FIELDS, "manifest counts")
    for name, count in manifest["inputCounts"].items():
        p._integer(count, name, allow_zero=True)
    objects = manifest["objects"]
    _need(type(objects) is list and 0 < len(objects) <= p.MAX_OBJECTS
          and manifest["objectCount"] == len(objects), "invalid manifest object count")
    names, ids, total = set(), set(), 0
    images = lidar = 0
    media_by_kind = {
        "image": {"image/jpeg", "image/png", "image/tiff"},
        "lidar": {"application/vnd.las", "application/vnd.laz"},
        "auxiliary": {"application/octet-stream", "application/json", "text/plain", "text/csv"},
    }
    for obj in objects:
        _closed(obj, MANIFEST_OBJECT_FIELDS, "manifest object")
        _slug(obj["id"], "manifest object id")
        p._basename(obj["name"])
        _need(not obj["name"].startswith(".") and "." in obj["name"]
              and not obj["name"].endswith("."), "invalid manifest object extension")
        p._integer(obj["byteSize"], "manifest object byte size")
        _hash(obj["sha256"], "manifest object digest")
        _need(obj["kind"] in ("image", "lidar", "auxiliary"), "invalid object kind")
        _need(type(obj["mediaType"]) is str and obj["mediaType"] in media_by_kind[obj["kind"]],
              "invalid manifest object media type")
        _need(obj["id"] not in ids and obj["name"].casefold() not in names,
              "duplicate manifest object")
        ids.add(obj["id"]); names.add(obj["name"].casefold())
        total += obj["byteSize"]
        _need(total <= p.MAX_SAFE_INTEGER, "manifest byte total exceeds safe integer")
        images += obj["kind"] == "image"
        lidar += obj["kind"] == "lidar"
    _need(total == manifest["totalBytes"]
          and manifest["inputCounts"]["images"] == images
          and manifest["inputCounts"]["lidarObjects"] == lidar
          and (lidar > 0 or manifest["inputCounts"]["points"] == 0),
          "manifest counts or total mismatch")
    _need(p.manifest_sha256(manifest) == manifest["sha256"], "manifest digest mismatch")
    return manifest


def _closure(objects) -> list[dict]:
    _need(type(objects) is list and 0 < len(objects) <= p.MAX_OBJECTS,
          "invalid retained object closure")
    checked, ids = [], set()
    total = 0
    for item in objects:
        _closed(item, OBJECT_FIELDS, "retained object")
        _slug(item["id"], "retained object id")
        p._integer(item["byteSize"], "retained object byte size")
        _hash(item["sha256"], "retained object digest")
        _need(item["id"] not in ids, "duplicate retained object id")
        ids.add(item["id"])
        total += item["byteSize"]
        _need(total <= p.MAX_SAFE_INTEGER, "retained bytes exceed safe integer")
        checked.append(dict(item))
    _need([item["id"] for item in checked] == sorted(ids, key=lambda x: x.encode("ascii")),
          "retained objects must be sorted by ASCII ID")
    return checked


def _source_intent(payload: dict, objects: list[dict]) -> dict:
    return dict(operationId=payload["operationId"], jobId=payload["jobId"],
                generation=payload["generation"], environment=payload["environment"],
                manifestSha256=payload["inputManifestSha256"], objects=objects)


def validate_source_retention_v2_candidate(
    envelope,
    *,
    job_id: str,
    generation: int,
    environment: str,
    input_manifest: dict,
    pipeline_digest: str,
    trusted_key: dict | None,
    trusted_now: str,
    verify_lineage,
    scope: str,
) -> dict:
    """Validate a frozen *fixture-only* v2 candidate receipt.

    `trusted_key`, `trusted_now`, and `verify_lineage` are explicitly supplied
    authorities; this function installs or persists none of them. Passing
    these values does not prove NAS custody. `scope` must be the literal
    `synthetic-only` so no production caller can treat this as an enabled
    storage owner.
    """
    _need(scope == "synthetic-only", "candidate scope required")
    if isinstance(envelope, (str, bytes)):
        envelope = p.decode_submission(envelope)
    p._require(len(p.canonical_json(envelope)) <= p.MAX_BODY_BYTES,
               "source retention envelope exceeds body limit")
    _closed(envelope, ENVELOPE_FIELDS, "source retention envelope")
    _need(envelope["signatureAlgorithm"] == "Ed25519", "unsupported signature algorithm")
    payload = envelope["payload"]
    _closed(payload, PAYLOAD_FIELDS, "source retention v2 payload")
    _need(type(payload["schemaVersion"]) is int and payload["schemaVersion"] == 2,
          "unsupported source retention schema")
    _need(payload["receiptType"] == "source_retention_v2"
          and payload["validationVersion"] == "candidate-v2"
          and payload["retentionGuarantee"] == "until_explicit_release_ack_v2",
          "unsupported source retention family")
    for field in ("receiptId", "storageReceiptId", "operationId", "issuerId", "keyId",
                  "ownerInstanceId", "ledgerEpoch", "jobId"):
        _slug(payload[field], field)
    role_ids = (payload["receiptId"], payload["storageReceiptId"], payload["operationId"])
    _need(len(set(role_ids)) == len(role_ids), "receipt role IDs must be distinct")
    for field in ("generation", "keyEpoch"):
        p._integer(payload[field], field)
    _need(type(payload["environment"]) is str
          and payload["environment"] in ("staging", "production"), "invalid environment")
    _hash(payload["inputManifestSha256"], "input manifest digest")
    _hash(payload["pipelineDigest"], "pipeline digest")
    _hash(payload["sourceClosureSha256"], "source closure digest")
    _slug(job_id, "expected job id")
    p._integer(generation, "expected generation")
    p._environment(environment)
    _hash(pipeline_digest, "expected pipeline digest")
    _need((payload["jobId"], payload["generation"], payload["environment"],
           payload["pipelineDigest"]) == (job_id, generation, environment, pipeline_digest),
          "source retention identity mismatch")
    manifest = _manifest(input_manifest)
    _need(payload["inputManifestSha256"] == manifest["sha256"],
          "source retention manifest mismatch")
    objects = _closure(payload["objects"])
    expected = sorted((dict(id=o["id"], byteSize=o["byteSize"], sha256=o["sha256"])
                       for o in manifest["objects"]), key=lambda x: x["id"].encode("ascii"))
    _need(objects == expected, "source retention closure mismatch")
    closure_message = CLOSURE_DOMAIN + p.canonical_json(objects)
    _need(hashlib.sha256(closure_message).hexdigest() == payload["sourceClosureSha256"],
          "source closure digest mismatch")
    _dataset(payload["datasetId"])
    txg = payload["snapshotCreateTxg"]
    _need(type(txg) is str and _TXG.fullmatch(txg) is not None
          and int(txg) <= MAX_TXG, "invalid snapshotCreateTxg")
    _need(payload["holdId"] == "truenas", "unsupported hold id")
    intent = _source_intent(payload, objects)
    snapshot_name = "ltds-" + hashlib.sha256(p.canonical_json(intent)).hexdigest()[:32]
    _need(payload["snapshotId"] == f"{payload['datasetId']}@{snapshot_name}",
          "snapshot identity mismatch")
    retained_at = _timestamp(payload["retainedAt"])
    now = _timestamp(trusted_now)
    _need(retained_at <= now, "retention time is in the future")
    _closed(trusted_key, ("issuerId", "keyId", "keyEpoch", "ownerInstanceId",
                          "environment", "algorithm", "datasetId", "revoked",
                          "notBefore", "notAfter", "publicKeySpkiDer"), "trusted key record")
    for field in ("issuerId", "keyId", "ownerInstanceId", "environment"):
        _need(type(trusted_key) is dict and trusted_key.get(field) == payload[field],
              "trusted key binding unavailable")
    _need(trusted_key.get("keyEpoch") == payload["keyEpoch"]
          and trusted_key.get("algorithm") == "Ed25519"
          and trusted_key.get("datasetId") == payload["datasetId"]
          and trusted_key.get("revoked") is False,
          "trusted key record is not bound to this receipt")
    _need(type(trusted_key.get("notBefore")) is str
          and type(trusted_key.get("notAfter")) is str,
          "trusted key validity window unavailable")
    key_not_before = _timestamp(trusted_key["notBefore"])
    key_not_after = _timestamp(trusted_key["notAfter"])
    _need(key_not_before <= retained_at <= key_not_after,
          "receipt was not signed during key validity")
    _need(key_not_before <= now <= key_not_after,
          "trusted key is not currently valid")
    signature = envelope["signature"]
    _need(type(signature) is str and _B64URL.fullmatch(signature) is not None,
          "invalid Ed25519 signature encoding")
    signature_bytes = base64.urlsafe_b64decode(signature + "==")
    _need(base64.urlsafe_b64encode(signature_bytes).decode("ascii").rstrip("=") == signature
          and len(signature_bytes) == 64, "invalid Ed25519 signature")
    key_der = trusted_key.get("publicKeySpkiDer")
    _need(type(key_der) is bytes, "trusted public key unavailable")
    try:
        public_key = serialization.load_der_public_key(key_der)
    except (TypeError, ValueError):
        raise SourceRetentionV2CandidateError("invalid trusted public key") from None
    _need(isinstance(public_key, Ed25519PublicKey), "trusted key is not Ed25519")
    signed_message = RECEIPT_DOMAIN + p.canonical_json(payload)
    try:
        public_key.verify(signature_bytes, signed_message)
    except InvalidSignature:
        raise SourceRetentionV2CandidateError("source retention signature mismatch") from None
    _need(callable(verify_lineage)
          and verify_lineage(payload["ownerInstanceId"], payload["ledgerEpoch"]) is True,
          "trusted rollback-resistant lineage unavailable")
    return copy.deepcopy(envelope)
