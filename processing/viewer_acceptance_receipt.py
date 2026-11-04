"""Verify Viewer-issued accepted-copy receipts against pinned Ed25519 keys.

This module only verifies a receipt. It does not authorize deleting source or
worker data; callers must also enforce the job's exact accepted-object closure
and their separate cleanup policy.
"""
from __future__ import annotations

import base64
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
import re

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from . import reference_protocol as protocol
from .viewer_result_manifest import validate_viewer_result_manifest

PROTOCOL = "ltds-viewer-accepted-copy-v1"
SIGNATURE_DOMAIN = b"LTDS-VIEWER-ACCEPTED-COPY-ACK-V1\0"
RECEIPT_FIELDS = {
    "protocol", "schemaVersion", "issuerId", "keyId", "receiptId",
    "storageReceiptId", "jobId", "generation", "environment",
    "inputManifestSha256", "pipelineDigest", "resultManifestSha256",
    "acceptedObjectIds", "objects", "acceptedAt", "signature",
}
EXPECTED_FIELDS = {
    "jobId", "generation", "environment", "inputManifestSha256",
    "pipelineDigest", "resultManifestSha256",
}
_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z")
_HASH = re.compile(r"[a-f0-9]{64}\Z")
_TIME = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z\Z")


class ViewerReceiptRejected(ValueError):
    """A signed Viewer acceptance receipt is untrusted or not exactly bound."""


@dataclass(frozen=True)
class VerifiedViewerReceipt:
    receipt: dict
    key_id: str
    issuer_id: str


def validate_trusted_viewer_receipt_keys(value: object) -> dict:
    """Validate and detach a bounded public-key pin registry."""
    _need(type(value) is dict and len(value) <= 32, "invalid_trusted_receipt_keys")
    clean = {}
    for key_id, record in value.items():
        _identifier(key_id, "invalid_trusted_key_id")
        _need(type(record) is dict and set(record) == {"issuerId", "publicKeyPem"},
              "invalid_trusted_key_record")
        _identifier(record["issuerId"], "invalid_trusted_issuer_id")
        pem = record["publicKeyPem"]
        _need(type(pem) is str and 1 <= len(pem) <= 8192, "invalid_trusted_public_key")
        _load_key(pem)
        clean[key_id] = {"issuerId": record["issuerId"], "publicKeyPem": pem}
    return clean


def _need(condition: bool, code: str) -> None:
    if not condition:
        raise ViewerReceiptRejected(code)


def _identifier(value: object, code: str) -> None:
    _need(type(value) is str and _ID.fullmatch(value) is not None, code)


def _hash(value: object, code: str) -> None:
    _need(type(value) is str and _HASH.fullmatch(value) is not None, code)


def _canonical_signature(value: object) -> bytes:
    _need(type(value) is str and re.fullmatch(r"[A-Za-z0-9_-]{86}", value) is not None,
          "invalid_signature_encoding")
    try:
        raw = base64.urlsafe_b64decode(value + "==")
    except (ValueError, TypeError):
        raise ViewerReceiptRejected("invalid_signature_encoding") from None
    _need(len(raw) == 64 and base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=") == value,
          "invalid_signature_encoding")
    return raw


def _load_key(value: object) -> Ed25519PublicKey:
    try:
        if isinstance(value, str):
            key = serialization.load_pem_public_key(value.encode("ascii"))
        elif isinstance(value, bytes):
            key = serialization.load_pem_public_key(value)
        else:
            raise TypeError
    except (ValueError, TypeError, UnicodeError):
        raise ViewerReceiptRejected("invalid_pinned_public_key") from None
    _need(isinstance(key, Ed25519PublicKey), "invalid_pinned_public_key")
    return key


def verify_viewer_acceptance_receipt(receipt: object, *, trusted_keys: dict,
                                    expected: dict, result_manifest: dict,
                                    accepted_object_ids: list[str]) -> VerifiedViewerReceipt:
    """Verify signature and exact job/object bindings using an external key pin.

    ``trusted_keys`` maps each permitted key ID to ``{"issuerId", "publicKeyPem"}``.
    Expected values and object IDs must come from the authenticated durable job
    state, not fields copied from the receipt itself.
    """
    _need(type(receipt) is dict and set(receipt) == RECEIPT_FIELDS, "invalid_receipt_fields")
    _need(receipt["protocol"] == PROTOCOL and type(receipt["schemaVersion"]) is int
          and receipt["schemaVersion"] == 1, "unsupported_receipt_version")
    for field in ("issuerId", "keyId", "receiptId", "storageReceiptId", "jobId"):
        _identifier(receipt[field], f"invalid_{field}")
    _need(type(receipt["generation"]) is int and receipt["generation"] > 0,
          "invalid_generation")
    _need(receipt["environment"] in ("production", "staging"), "invalid_environment")
    for field in ("inputManifestSha256", "pipelineDigest", "resultManifestSha256"):
        _hash(receipt[field], f"invalid_{field}")
    _need(type(receipt["acceptedAt"]) is str and _TIME.fullmatch(receipt["acceptedAt"]) is not None,
          "invalid_accepted_at")
    try:
        datetime.fromisoformat(receipt["acceptedAt"].replace("Z", "+00:00"))
    except ValueError:
        raise ViewerReceiptRejected("invalid_accepted_at") from None

    _need(type(expected) is dict and set(expected) == EXPECTED_FIELDS,
          "invalid_expected_binding")
    _need(type(accepted_object_ids) is list and accepted_object_ids
          and all(type(item) is str and _ID.fullmatch(item) for item in accepted_object_ids)
          and len(set(accepted_object_ids)) == len(accepted_object_ids),
          "invalid_expected_objects")
    try:
        validated_manifest = validate_viewer_result_manifest(
            result_manifest,
            job_id=expected["jobId"], generation=expected["generation"],
            environment=expected["environment"], pipeline_digest=expected["pipelineDigest"],
        )
    except (ValueError, KeyError, TypeError):
        raise ViewerReceiptRejected("invalid_expected_result_manifest") from None
    object_map = {item["id"]: item for item in validated_manifest["objects"]}
    _need(set(accepted_object_ids) <= set(object_map), "unexpected_expected_object")
    if validated_manifest["schemaVersion"] == 2:
        _need(accepted_object_ids == [item["id"] for item in validated_manifest["objects"]],
              "incomplete_or_reordered_v2_acceptance")
    expected_sorted_ids = sorted(accepted_object_ids)
    expected_objects = [
        {"id": object_id, "byteSize": object_map[object_id]["byteSize"],
         "sha256": object_map[object_id]["sha256"]}
        for object_id in expected_sorted_ids
    ]
    actual_binding = {field: receipt[field] for field in EXPECTED_FIELDS}
    _need(actual_binding == expected, "receipt_job_binding_mismatch")
    _need(receipt["acceptedObjectIds"] == expected_sorted_ids
          and receipt["objects"] == expected_objects, "receipt_object_binding_mismatch")
    _need(type(trusted_keys) is dict and receipt["keyId"] in trusted_keys,
          "untrusted_receipt_key")
    key_record = trusted_keys[receipt["keyId"]]
    _need(type(key_record) is dict and set(key_record) == {"issuerId", "publicKeyPem"}
          and key_record["issuerId"] == receipt["issuerId"], "untrusted_receipt_issuer")
    public_key = _load_key(key_record["publicKeyPem"])
    signature = _canonical_signature(receipt["signature"])
    signed = {key: value for key, value in receipt.items() if key != "signature"}
    try:
        public_key.verify(signature, SIGNATURE_DOMAIN + protocol.canonical_json(signed))
    except InvalidSignature:
        raise ViewerReceiptRejected("invalid_receipt_signature") from None
    return VerifiedViewerReceipt(deepcopy(receipt), receipt["keyId"], receipt["issuerId"])
