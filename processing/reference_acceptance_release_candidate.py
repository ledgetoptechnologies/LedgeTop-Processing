"""Inactive cross-service acceptance/release binding checks.

These checks are synthetic contract work only. They do not authenticate the
transport, prove NAS custody or physical deletion, establish a filesystem
writer fence, persist replay state, or authorize cleanup in the runtime.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import re

from . import reference_v2_candidate as v2

AUTHORIZATION_DOMAIN = b"LTDS-SOURCE-RELEASE-AUTHORIZATION-V1\0"
SPOOL_CLOSURE_DOMAIN = b"LTDS-PROCESSING-SPOOL-CLOSURE-V1\0"
CUSTODY_RECEIPT_DOMAIN = b"LTDS-PROCESSING-SPOOL-CUSTODY-RECEIPT-V1\0"
SPOOL_CUSTODY_FIELDS = {
    "schemaVersion", "receiptType", "receiptId", "issuerId", "storageReceiptId", "binding",
    "spoolRootId", "spoolRootGeneration", "sourceClosureSha256", "objects",
    "writerFenceReceiptId", "capacityReservationReceiptId", "reservedBytes", "retainedAt",
    "retentionGuarantee",
}
AUTH_FIELDS = {
    "schemaVersion", "receiptType", "authorizationId", "operationId", "issuerId",
    "binding", "sourceCustodyReceiptId", "sourceCustodyReceiptSha256", "storageReceiptId",
    "spoolRootId", "spoolRootGeneration", "sourceClosureSha256", "objects",
    "terminalReceiptId", "terminalReceiptSha256", "acceptedCopyAckIds", "policySha256",
    "authorizedAt", "expiresAt",
}
ACK_FIELDS = {
    "schemaVersion", "receiptType", "ackId", "operationId", "authorizationId",
    "authorizationSha256", "issuerId", "binding", "sourceCustodyReceiptId",
    "sourceCustodyReceiptSha256", "storageReceiptId", "spoolRootId",
    "spoolRootGeneration", "sourceClosureSha256", "deletedObjects", "deletedAt", "acknowledgedAt",
}
BINDING_FIELDS = {"jobId", "generation", "environment", "inputManifestSha256", "pipelineDigest"}
OBJECT_FIELDS = {"objectId", "physicalObjectId", "objectGeneration", "byteSize", "sha256", "custodyReceiptId"}
DELETED_FIELDS = OBJECT_FIELDS | {"deletionReceiptId"}
_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z")
_HASH = re.compile(r"[a-f0-9]{64}\Z")
_TIME = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z\Z")


class CandidateRejected(ValueError):
    """A synthetic cross-service record or binding is not exact."""


@dataclass(frozen=True)
class CandidateBindingCheck:
    record_type: str
    operation_id: str
    receipt_ids: tuple[str, ...]
    scope: str = "synthetic structural and cross-record binding only; not authority"

    def __bool__(self):
        raise TypeError("candidate binding checks are not authority")


def canonical_digest(value: dict, domain: bytes) -> str:
    if type(domain) is not bytes or not domain.endswith(b"\0"):
        raise CandidateRejected("invalid hash domain")
    return hashlib.sha256(domain + v2.canonical_metadata(value)).hexdigest()


def spool_closure_digest(objects: list[dict]) -> str:
    """Hash the exact ASCII-object-ID ordered physical spool inventory."""
    _objects(objects, OBJECT_FIELDS)
    return canonical_digest(objects, SPOOL_CLOSURE_DOMAIN)


def custody_receipt_digest(receipt: dict) -> str:
    return canonical_digest(receipt, CUSTODY_RECEIPT_DOMAIN)


def validate_spool_custody_receipt(receipt_raw, *, authenticated_peer, expected_binding,
                                   expected_spool_root_id, expected_spool_root_generation,
                                   expected_source_objects, expected_custody_receipt_sha256,
                                   expected_writer_fence_receipt_id,
                                   expected_capacity_reservation_receipt_id,
                                   expected_reserved_bytes, now):
    """Bind a candidate custody receipt to authenticated external observations.

    Passing this checker does not prove that the caller's transport principal,
    fence, capacity reservation or physical file observations are trustworthy.
    """
    receipt = _decode(receipt_raw)
    _record(receipt, SPOOL_CUSTODY_FIELDS, "processing_spool_custody")
    _need(authenticated_peer == {"role": "sourceStorageExecutor", "principal": receipt["issuerId"]},
          "authenticated storage role mismatch")
    _need(receipt["binding"] == expected_binding, "stale or foreign source binding")
    _need(receipt["spoolRootId"] == expected_spool_root_id
          and receipt["spoolRootGeneration"] == expected_spool_root_generation,
          "stale spool root identity")
    _objects(receipt["objects"], OBJECT_FIELDS)
    _need(receipt["objects"] == expected_source_objects
          and receipt["sourceClosureSha256"] == spool_closure_digest(receipt["objects"]),
          "source custody inventory mismatch")
    _need(_hash(expected_custody_receipt_sha256)
          and custody_receipt_digest(receipt) == expected_custody_receipt_sha256,
          "source custody receipt digest mismatch")
    for key in ("receiptId", "issuerId", "storageReceiptId", "spoolRootId",
                "writerFenceReceiptId", "capacityReservationReceiptId", "retentionGuarantee"):
        _need(_id(receipt[key]), "invalid custody identity")
    _need(receipt["writerFenceReceiptId"] == expected_writer_fence_receipt_id
          and receipt["capacityReservationReceiptId"] == expected_capacity_reservation_receipt_id,
          "unverified storage reservation or writer fence")
    _need(type(receipt["spoolRootGeneration"]) is int
          and 0 < receipt["spoolRootGeneration"] <= 2**53 - 1,
          "invalid spool root generation")
    _need(type(receipt["reservedBytes"]) is int and receipt["reservedBytes"] == expected_reserved_bytes
          and 0 < receipt["reservedBytes"] <= 2**53 - 1
          and sum(item["byteSize"] for item in receipt["objects"]) <= receipt["reservedBytes"],
          "invalid reserved capacity")
    retained_at, current = map(_timestamp, (receipt["retainedAt"], now))
    _need(retained_at <= current, "source custody receipt is from the future")
    _need(receipt["retentionGuarantee"] == "until_explicit_source_release_ack_v1",
          "unsupported source retention guarantee")
    return CandidateBindingCheck(
        "ProcessingSpoolCustodyReceipt", receipt["receiptId"],
        (receipt["storageReceiptId"], receipt["writerFenceReceiptId"],
         receipt["capacityReservationReceiptId"]))


def _need(condition: bool, message: str):
    if not condition:
        raise CandidateRejected(message)


def _decode(raw):
    try:
        value = v2._decode(raw)
    except v2.CandidateError as exc:
        raise CandidateRejected("invalid bounded strict JSON") from exc
    _need(type(value) is dict, "record must be a JSON object")
    return value


def _id(value):
    return type(value) is str and _ID.fullmatch(value) is not None


def _hash(value):
    return type(value) is str and _HASH.fullmatch(value) is not None


def _timestamp(value):
    _need(type(value) is str and _TIME.fullmatch(value) is not None, "invalid timestamp")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        raise CandidateRejected("invalid timestamp") from None
    _need(parsed.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z" == value,
          "noncanonical timestamp")
    return parsed


def _objects(objects, fields):
    _need(type(objects) is list and 0 < len(objects) <= 10000, "invalid object inventory")
    identifiers = []
    aliases = {"objectId": set(), "physicalObjectId": set(), "custodyReceiptId": set()}
    for item in objects:
        _need(type(item) is dict and set(item) == fields, "invalid object record")
        for key in aliases:
            _need(_id(item[key]), "invalid object identity")
            _need(item[key] not in aliases[key], "duplicate object identity")
            aliases[key].add(item[key])
        _need(type(item["objectGeneration"]) is int and 0 < item["objectGeneration"] <= 2**53 - 1,
              "invalid object generation")
        _need(type(item["byteSize"]) is int and 0 < item["byteSize"] <= 2**53 - 1,
              "invalid object byte size")
        _need(_hash(item["sha256"]), "invalid object digest")
        identifiers.append(item["objectId"])
        if "deletionReceiptId" in fields:
            _need(_id(item["deletionReceiptId"]), "invalid deletion receipt")
    _need(identifiers == sorted(identifiers, key=lambda value: value.encode("ascii")),
          "object inventory must use ASCII identity order")
    if "deletionReceiptId" in fields:
        deletion_ids = [item["deletionReceiptId"] for item in objects]
        _need(len(deletion_ids) == len(set(deletion_ids)), "duplicate deletion receipt")


def _binding(value):
    _need(type(value) is dict and set(value) == BINDING_FIELDS, "invalid job binding")
    _need(_id(value["jobId"]) and type(value["generation"]) is int
          and 0 < value["generation"] <= 2**53 - 1, "invalid job identity")
    _need(value["environment"] in ("production", "staging"), "invalid environment")
    _need(_hash(value["inputManifestSha256"]) and _hash(value["pipelineDigest"]),
          "invalid job digest")


def _record(value, fields, record_type):
    _need(type(value) is dict and set(value) == fields, "invalid closed record")
    _need(type(value["schemaVersion"]) is int and value["schemaVersion"] == 1
          and value["receiptType"] == record_type, "unsupported record type/version")
    _binding(value["binding"])


def validate_result_acceptance(acceptance_raw, ack_raw, *, authenticated_issuer_id,
                               authenticated_role, expected_binding,
                               expected_result_set_id, expected_result_manifest_sha256,
                               expected_artifacts):
    """Bind the proposed Viewer ack to the trusted expected result context.

    ``authenticated_issuer_id`` and ``authenticated_role`` must come from the
    transport identity/role map, never from the payload. This still does not
    independently prove the issuer stored the bytes or durably committed replay.
    """
    acceptance = v2.validate_candidate(acceptance_raw, record_type="PartialAcceptance").value
    ack_record = v2.validate_accepted_copy_ack(acceptance_raw, ack_raw)
    ack = ack_record.value
    _need(authenticated_role == "resultAcceptanceIssuer" and _id(authenticated_issuer_id)
          and ack["issuerId"] == authenticated_issuer_id,
          "unauthenticated acceptance issuer")
    _need(acceptance["binding"] == expected_binding and ack["binding"] == expected_binding,
          "stale or foreign job binding")
    _need(acceptance["resultSetId"] == expected_result_set_id
          and acceptance["resultManifestSha256"] == expected_result_manifest_sha256,
          "foreign result identity")
    _need(type(expected_artifacts) is list and expected_artifacts,
          "expected immutable result inventory required")
    projection = ("id", "sha256", "byteSize", "stageId", "workerId", "reservationId",
                 "producerCustodyReceiptId")
    _need(all(type(item) is dict and set(item) == set(projection) for item in expected_artifacts),
          "invalid expected result inventory")
    _need([ {key: item[key] for key in projection} for item in acceptance["acceptedArtifacts"] ]
          == expected_artifacts, "accepted result inventory differs from producer manifest")
    for key in ("physicalReceiptId", "validationReceiptId", "producerCustodyReceiptId"):
        values = [item[key] for item in acceptance["acceptedArtifacts"]]
        _need(len(values) == len(set(values)), "duplicate receipt identity")
    return CandidateBindingCheck("AcceptedCopyAck", ack["ackOperationId"],
                                 (ack["acceptanceReceiptId"], ack["storageReceiptId"]))


def validate_source_release_pair(authorization_raw, acknowledgement_raw, *,
                                 authorization_peer, acknowledgement_peer,
                                 expected_binding, expected_custody_receipt_id,
                                 expected_custody_receipt_sha256, expected_storage_receipt_id,
                                 expected_spool_root_id, expected_spool_root_generation,
                                 expected_source_objects, expected_source_closure_sha256,
                                 expected_terminal_receipt_id, expected_terminal_receipt_sha256,
                                 expected_accepted_copy_ack_ids, expected_policy_sha256,
                                 now):
    """Check exact candidate authorization/ack bindings and external identity.

    The storage service must perform and durably journal the actual fenced delete;
    this validator only checks the returned record against trusted call context.
    This validator rejects expired authorizations. Exact post-expiry response
    replay belongs in an authenticated lookup of the immutable storage ledger
    keyed by issuer plus operation and complete ACK digest; it must not re-run a
    physical deletion under this authorization.
    """
    authorization = _decode(authorization_raw)
    acknowledgement = _decode(acknowledgement_raw)
    _record(authorization, AUTH_FIELDS, "source_release_authorization")
    _record(acknowledgement, ACK_FIELDS, "source_release_ack")
    auth, ack = authorization, acknowledgement
    _need(authorization_peer == {"role": "sourceReleaseAuthorizer", "principal": auth["issuerId"]}
          and acknowledgement_peer == {"role": "sourceStorageExecutor", "principal": ack["issuerId"]},
          "authenticated service role mismatch")
    _need(authorization_peer["principal"] != acknowledgement_peer["principal"],
          "source release roles require distinct service principals")
    _need(auth["binding"] == expected_binding and ack["binding"] == expected_binding,
          "stale or foreign job binding")
    expected_auth = {
        "sourceCustodyReceiptId": expected_custody_receipt_id,
        "sourceCustodyReceiptSha256": expected_custody_receipt_sha256,
        "storageReceiptId": expected_storage_receipt_id,
        "spoolRootId": expected_spool_root_id,
        "spoolRootGeneration": expected_spool_root_generation,
        "sourceClosureSha256": expected_source_closure_sha256,
        "terminalReceiptId": expected_terminal_receipt_id,
        "terminalReceiptSha256": expected_terminal_receipt_sha256,
        "acceptedCopyAckIds": expected_accepted_copy_ack_ids,
        "policySha256": expected_policy_sha256,
    }
    for key, expected in expected_auth.items():
        _need(auth[key] == expected, "authorization differs from trusted state")
    _objects(auth["objects"], OBJECT_FIELDS)
    _need(auth["objects"] == expected_source_objects
          and spool_closure_digest(auth["objects"]) == auth["sourceClosureSha256"],
          "source inventory differs from custody")
    ack_ids = auth["acceptedCopyAckIds"]
    _need(type(ack_ids) is list and len(ack_ids) <= 10000
          and all(_id(value) for value in ack_ids)
          and ack_ids == sorted(set(ack_ids), key=lambda value: value.encode("ascii")),
          "invalid accepted copy receipt inventory")
    for key in ("authorizationId", "operationId", "issuerId", "sourceCustodyReceiptId",
                "storageReceiptId", "spoolRootId", "terminalReceiptId"):
        _need(_id(auth[key]), "invalid authorization identity")
    for key in ("sourceCustodyReceiptSha256", "sourceClosureSha256", "terminalReceiptSha256",
                "policySha256"):
        _need(_hash(auth[key]), "invalid authorization digest")
    _need(type(auth["spoolRootGeneration"]) is int and auth["spoolRootGeneration"] == expected_spool_root_generation
          and 0 < auth["spoolRootGeneration"] <= 2**53 - 1,
          "stale spool root generation")
    authorized_at, expires_at, current = map(_timestamp, (auth["authorizedAt"], auth["expiresAt"], now))
    deleted_at, acknowledged_at = map(_timestamp, (ack["deletedAt"], ack["acknowledgedAt"]))
    _need(authorized_at <= current and authorized_at < expires_at,
          "authorization time invalid")
    _need(current <= expires_at, "expired authorization")
    _need(deleted_at >= authorized_at and deleted_at <= acknowledged_at
          and acknowledged_at <= current and acknowledged_at <= expires_at,
          "deletion or durable ACK time invalid")
    _need(ack["operationId"] == auth["operationId"]
          and ack["authorizationId"] == auth["authorizationId"]
          and ack["authorizationSha256"] == canonical_digest(auth, AUTHORIZATION_DOMAIN),
          "authorization replay binding mismatch")
    for key in ("sourceCustodyReceiptId", "sourceCustodyReceiptSha256", "storageReceiptId",
                "spoolRootId", "spoolRootGeneration", "sourceClosureSha256"):
        _need(ack[key] == auth[key], "release ACK identity mismatch")
    _objects(ack["deletedObjects"], DELETED_FIELDS)
    _need([{key: item[key] for key in OBJECT_FIELDS} for item in ack["deletedObjects"]]
          == auth["objects"], "deleted inventory differs from authorization")
    for key in ("ackId", "issuerId"):
        _need(_id(ack[key]), "invalid ACK identity")
    _need(_hash(ack["authorizationSha256"]), "invalid authorization digest")
    return CandidateBindingCheck("SourceReleaseAck", ack["operationId"],
                                 (ack["ackId"], ack["authorizationId"]))
