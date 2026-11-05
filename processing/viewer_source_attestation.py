"""Verify source-intake proofs using externally pinned Viewer signing keys.

A verified proof binds admission metadata, not physical NAS custody or cleanup.
The caller must supply expected values from its authenticated queue/intake state.
"""
from __future__ import annotations

import base64
from copy import deepcopy
from dataclasses import dataclass
import hashlib
import re
from types import MappingProxyType

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from . import reference_protocol as p

PROTOCOL = "ltds-preclaim-source-attestation-v2"
DOMAIN = (PROTOCOL + "\0").encode("ascii")
MAX_PROOF_BYTES = 8192
PROOF_FIELDS = (
    "schemaVersion", "protocol", "issuerId", "keyId", "authorityId", "principalId",
    "environment", "submissionId", "viewerTaskId", "providerTaskId", "viewerAttemptId", "activationId",
    "preparedEnvelopeSha256", "sourceSnapshotSha256", "sourceManifestSha256",
    "derivedManifestSha256", "pipelineManifestSha256", "pipelineDigest",
)


class SourceAttestationError(ValueError):
    """Malformed, untrusted or incorrectly bound source proof."""


@dataclass(frozen=True)
class ValidatedSourceKeys:
    records: object


def validate_trusted_keys(records):
    """Parse a bounded explicit environment/issuer/key/principal pin registry."""
    if type(records) is not list or not 1 <= len(records) <= 32:
        raise SourceAttestationError("invalid source attestation trust registry")
    registry = {}
    try:
        for record in records:
            p._closed(record, ("environment", "issuerId", "keyId", "principalId", "publicKeyPem"), "trusted source key")
            p._environment(record["environment"])
            for field in ("issuerId", "keyId", "principalId"):
                p._slug(record[field], field)
            identity = (record["environment"], record["issuerId"], record["keyId"])
            p._require(identity not in registry, "duplicate source signer identity")
            pem = record["publicKeyPem"]
            p._require(type(pem) is str and 1 <= len(pem) <= 8192 and pem.isascii()
                       and all(c in "\r\n" or 0x20 <= ord(c) <= 0x7e for c in pem)
                       and "\r" not in pem.replace("\r\n", ""), "invalid source public key")
            key = serialization.load_pem_public_key(pem.encode("ascii"))
            p._require(isinstance(key, Ed25519PublicKey), "source key must be Ed25519")
            registry[identity] = (record["principalId"], key)
    except (ValueError, TypeError, UnicodeError):
        raise SourceAttestationError("invalid source attestation trust registry") from None
    return ValidatedSourceKeys(MappingProxyType(registry))


def proof_digest(proof):
    return hashlib.sha256(DOMAIN + p.canonical_json(proof)).hexdigest()


def validate_and_verify(attestation, *, trusted_keys, authority_id, environment,
                        submission_id, provider_task_id, attempt_id, viewer_task_id,
                        source_manifest_sha256, derived_manifest_sha256,
                        pipeline_manifest_sha256, pipeline_digest):
    """Verify the closed proof, signature and every externally expected binding.

    Persisted exact retries are intentionally not expired by wall-clock time.
    Revoked/unconfigured keys fail verification; this method grants no source
    release, engine execution or deletion authority.
    """
    try:
        p._closed(attestation, ("proof", "signature", "attestationSha256"), "source attestation")
        proof = attestation["proof"]
        p._closed(proof, PROOF_FIELDS, "source attestation proof")
        p._require(type(proof["schemaVersion"]) is int and proof["schemaVersion"] == 2
                   and proof["protocol"] == PROTOCOL, "invalid source proof protocol")
        for field in ("issuerId", "keyId", "authorityId", "principalId"):
            p._slug(proof[field], field)
        for field in ("submissionId", "viewerTaskId", "providerTaskId", "viewerAttemptId", "activationId"):
            p._uuid(proof[field], field)
        p._environment(proof["environment"])
        for field in ("preparedEnvelopeSha256", "sourceSnapshotSha256", "sourceManifestSha256",
                      "derivedManifestSha256", "pipelineManifestSha256", "pipelineDigest"):
            p._hash(proof[field], field)
        expected = {
            "authorityId": authority_id, "environment": environment,
            "submissionId": submission_id, "providerTaskId": provider_task_id,
            "viewerAttemptId": attempt_id, "viewerTaskId": viewer_task_id,
            "sourceManifestSha256": source_manifest_sha256,
            "derivedManifestSha256": derived_manifest_sha256,
            "pipelineManifestSha256": pipeline_manifest_sha256, "pipelineDigest": pipeline_digest,
        }
        p._require(all(proof[k] == v for k, v in expected.items()), "source proof intake binding mismatch")
        raw = p.canonical_json(proof)
        p._require(len(raw) <= MAX_PROOF_BYTES, "source proof too large")
        message = DOMAIN + raw
        p._hash(attestation["attestationSha256"], "source attestation digest")
        p._require(attestation["attestationSha256"] == hashlib.sha256(message).hexdigest(),
                   "source proof digest mismatch")
        encoded = attestation["signature"]
        p._require(type(encoded) is str and re.fullmatch(r"[A-Za-z0-9_-]{86}", encoded) is not None,
                   "invalid source signature encoding")
        signature = base64.urlsafe_b64decode(encoded + "==")
        p._require(len(signature) == 64 and base64.urlsafe_b64encode(signature).decode("ascii").rstrip("=") == encoded,
                   "noncanonical source signature encoding")
        registry = trusted_keys if isinstance(trusted_keys, ValidatedSourceKeys) else validate_trusted_keys(trusted_keys)
        trust = registry.records.get((environment, proof["issuerId"], proof["keyId"]))
        p._require(trust is not None and trust[0] == proof["principalId"], "untrusted source signer")
        trust[1].verify(signature, message)
        return deepcopy(attestation)
    except (ValueError, TypeError, KeyError, AttributeError, InvalidSignature):
        raise SourceAttestationError("source attestation rejected") from None
