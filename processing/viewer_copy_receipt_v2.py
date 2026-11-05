"""Verify Viewer accepted-copy V2 receipts against pinned Ed25519 keys.

The caller must build ``expected`` from authenticated durable job, attempt,
source-intake, and acceptance state. This verifier authenticates the receipt;
it does not itself provide durable replay protection or authorize cleanup.
"""
from __future__ import annotations

import hashlib
import re
from copy import deepcopy
from datetime import datetime

from cryptography.exceptions import InvalidSignature

from . import reference_protocol as protocol
from .viewer_acceptance_receipt import (
    ViewerReceiptRejected,
    VerifiedViewerReceipt,
    _canonical_signature,
    _hash,
    _identifier,
    _load_key,
    validate_trusted_viewer_receipt_keys,
)
from .viewer_result_manifest import validate_viewer_acceptance, validate_viewer_result_manifest


PROTOCOL = "ltds-viewer-accepted-copy-v2"
SIGNATURE_DOMAIN = b"LTDS-VIEWER-ACCEPTED-COPY-ACK-V2\0"
ACCEPTANCE_DOMAIN = b"LTDS-OUTER-ACCEPTANCE-V1\0"
BINDING_FIELDS = (
    "coordinatorAuthorityId", "viewerPrincipalId", "submissionId", "activationId",
    "viewerTaskId", "viewerAttemptId", "providerTaskId", "preparedEnvelopeSha256",
    "jobId", "generation", "environment", "inputManifestSha256", "pipelineDigest",
    "resultManifestSha256", "sourceManifestSha256", "derivedManifestSha256",
    "sourceReceiptSha256", "derivedReceiptSha256",
)
RECEIPT_FIELDS = frozenset((
    "protocol", "schemaVersion", "issuerId", "keyId", "receiptId", "storageReceiptId",
    "acceptanceReceiptId", "acceptanceStorageReceiptId", "acceptancePayloadSha256",
    *BINDING_FIELDS, "acceptedObjectIds", "objects", "acceptedAt", "signature",
))
_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\Z")
_TIME = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z\Z")


def _need(condition: bool, code: str) -> None:
    if not condition:
        raise ViewerReceiptRejected(code)


def _ascii_tree(value: object, depth: int = 0) -> bool:
    if depth > 24:
        return False
    if value is None or type(value) is bool:
        return True
    if type(value) is str:
        return all(" " <= char <= "~" for char in value)
    if type(value) is int:
        return 0 <= value <= 9007199254740991
    if type(value) is list:
        return len(value) <= protocol.MAX_OBJECTS and all(_ascii_tree(item, depth + 1) for item in value)
    if type(value) is dict:
        return all(type(key) is str and key and all(" " <= char <= "~" for char in key)
                   and _ascii_tree(item, depth + 1) for key, item in value.items())
    return False


def _validate_binding(binding: object, code: str) -> dict:
    _need(type(binding) is dict and set(binding) == set(BINDING_FIELDS), code)
    _need(_ascii_tree(binding), code)
    for field in ("coordinatorAuthorityId", "viewerPrincipalId", "providerTaskId", "jobId"):
        _identifier(binding[field], code)
    for field in ("submissionId", "activationId", "viewerTaskId", "viewerAttemptId"):
        _need(type(binding[field]) is str and _UUID.fullmatch(binding[field]) is not None, code)
    _need(binding["providerTaskId"] == binding["jobId"]
          and binding["submissionId"] == binding["viewerAttemptId"], code)
    _need(type(binding["generation"]) is int and binding["generation"] > 0, code)
    _need(binding["environment"] in ("production", "staging"), code)
    for field in ("preparedEnvelopeSha256", "inputManifestSha256", "pipelineDigest",
                  "resultManifestSha256", "sourceManifestSha256", "derivedManifestSha256",
                  "sourceReceiptSha256", "derivedReceiptSha256"):
        _hash(binding[field], code)
    return binding


def _valid_time(value: object) -> bool:
    if type(value) is not str or _TIME.fullmatch(value) is None:
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.isoformat(timespec="milliseconds").replace("+00:00", "Z") == value


def verify_viewer_copy_receipt_v2(receipt: object, *, trusted_keys: dict,
                                 expected: dict, result_manifest: dict,
                                 acceptance: dict) -> VerifiedViewerReceipt:
    """Authenticate and exactly bind a Viewer V2 accepted-copy receipt.

    ``expected`` is the closed set of V2 binding fields and must be derived by
    the caller from trusted persisted state. The outer acceptance is validated
    independently and its exact canonical digest and two receipt IDs are bound
    into the signed inner receipt.
    """
    _need(type(receipt) is dict and set(receipt) == RECEIPT_FIELDS, "invalid_receipt_fields")
    _need(receipt["protocol"] == PROTOCOL and type(receipt["schemaVersion"]) is int
          and receipt["schemaVersion"] == 2, "unsupported_receipt_version")
    for field in ("issuerId", "keyId", "receiptId", "storageReceiptId",
                  "acceptanceReceiptId", "acceptanceStorageReceiptId"):
        _identifier(receipt[field], f"invalid_{field}")
    _need(type(receipt["generation"]) is int and receipt["generation"] > 0,
          "invalid_generation")
    _need(receipt["environment"] in ("production", "staging"), "invalid_environment")
    for field in ("preparedEnvelopeSha256", "inputManifestSha256", "pipelineDigest",
                  "resultManifestSha256", "sourceManifestSha256", "derivedManifestSha256",
                  "sourceReceiptSha256", "derivedReceiptSha256", "acceptancePayloadSha256"):
        _hash(receipt[field], f"invalid_{field}")
    _need(_valid_time(receipt["acceptedAt"]), "invalid_accepted_at")
    _need(_ascii_tree(receipt), "invalid_receipt_values")

    checked_expected = _validate_binding(expected, "invalid_expected_binding")
    actual_binding = {field: receipt[field] for field in BINDING_FIELDS}
    _need(actual_binding == checked_expected, "receipt_job_binding_mismatch")

    try:
        manifest = validate_viewer_result_manifest(result_manifest,
            job_id=checked_expected["jobId"], generation=checked_expected["generation"],
            environment=checked_expected["environment"], pipeline_digest=checked_expected["pipelineDigest"])
        _need(manifest["resultManifestSha256"] == checked_expected["resultManifestSha256"],
              "invalid_expected_result_manifest")
        checked_acceptance = validate_viewer_acceptance(acceptance, result_manifest=manifest,
            input_manifest_sha256=checked_expected["inputManifestSha256"])
    except (TypeError, ValueError, KeyError):
        raise ViewerReceiptRejected("invalid_expected_result_or_acceptance") from None
    _need(_ascii_tree(checked_acceptance), "invalid_acceptance_values")
    object_ids = [item["id"] for item in manifest["objects"]]
    _need(bool(object_ids) and len(set(object_ids)) == len(object_ids), "invalid_result_closure")
    expected_objects = [{"id": item["id"], "byteSize": item["byteSize"], "sha256": item["sha256"]}
                        for item in manifest["objects"]]
    _need(receipt["acceptedObjectIds"] == object_ids
          and receipt["objects"] == expected_objects, "receipt_object_binding_mismatch")
    _need(checked_acceptance["acceptedObjectIds"] == object_ids,
          "acceptance_object_closure_mismatch")
    _need(receipt["acceptanceReceiptId"] == checked_acceptance["receiptId"]
          and receipt["acceptanceStorageReceiptId"] == checked_acceptance["storageReceiptId"],
          "receipt_acceptance_identity_mismatch")
    ids = (receipt["receiptId"], receipt["storageReceiptId"],
           checked_acceptance["receiptId"], checked_acceptance["storageReceiptId"])
    _need(len(set(ids)) == len(ids), "receipt_identity_replay")
    expected_acceptance_sha = hashlib.sha256(
        ACCEPTANCE_DOMAIN + protocol.canonical_json(checked_acceptance)).hexdigest()
    _need(receipt["acceptancePayloadSha256"] == expected_acceptance_sha,
          "receipt_acceptance_digest_mismatch")

    try:
        pins = validate_trusted_viewer_receipt_keys(trusted_keys)
    except (TypeError, ValueError):
        raise ViewerReceiptRejected("invalid_trusted_receipt_keys") from None
    _need(receipt["keyId"] in pins, "untrusted_receipt_key")
    pin = pins[receipt["keyId"]]
    _need(pin["issuerId"] == receipt["issuerId"], "untrusted_receipt_issuer")
    public_key = _load_key(pin["publicKeyPem"])
    signature = _canonical_signature(receipt["signature"])
    body = {key: value for key, value in receipt.items() if key != "signature"}
    try:
        public_key.verify(signature, SIGNATURE_DOMAIN + protocol.canonical_json(body))
    except InvalidSignature:
        raise ViewerReceiptRejected("invalid_receipt_signature") from None
    return VerifiedViewerReceipt(deepcopy(receipt), receipt["keyId"], receipt["issuerId"])
