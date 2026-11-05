"""Pure validator for Viewer source-intake v2 requests.

This module validates identities and manifest closure only. It does not write
files, issue custody receipts, reserve NAS bytes, or authorize a job claim.
Those actions require the separately qualified durable spool implementation.
"""
from __future__ import annotations

import copy
import re

from . import reference_protocol as p

SOURCE_PREFIX = "/ltds/v2"


SOURCE_MANIFEST_FIELDS = (
    "schemaVersion", "id", "datasetManifestSha256", "sha256",
    "totalBytes", "objectCount", "objects",
)
SOURCE_OBJECT_FIELDS = (
    "id", "name", "byteSize", "sha256", "sourceType", "retentionClass",
    "retentionHoldId",
)
DERIVED_MANIFEST_FIELDS = ("schemaVersion", "attemptId", "sha256", "objects")
DERIVED_OBJECT_FIELDS = (
    "id", "name", "byteSize", "sha256", "sourceType", "retentionClass",
    "retentionHoldId", "snapshotAttemptId", "snapshotContentSha256",
    "snapshotEvidenceSha256",
)
INTAKE_FIELDS = (
    "schemaVersion", "submissionId", "providerTaskId", "sourceManifest",
    "viewerTaskId", "derivedManifest", "manifest", "pipeline", "capabilityFingerprint",
    "sourceAttestation",
)
MAX_SOURCE_BYTES = p.MAX_SAFE_INTEGER


def _ordered_ids(objects, label):
    ids = [item["id"] for item in objects]
    p._require(ids == sorted(ids, key=lambda value: value.encode("ascii")),
               f"{label} objects must be sorted by ASCII ID")
    p._require(len(ids) == len(set(ids)), f"duplicate {label} object ID")
    return ids


def _source_basename(value):
    """Validate descriptive source filename without the engine's ASCII limit."""
    p._string(value, "source object name")
    p._require(value not in (".", "..")
               and not any(char in value for char in '/\\:<>' + '"|?*')
               and value[-1] not in " ." and value[0] != " ",
               "unsafe source object name")
    p._require(value.split(".")[0].upper() not in
               {"CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(1, 10)),
                *(f"LPT{i}" for i in range(1, 10))},
               "reserved source object name")


def _validate_source_manifest(value):
    p._closed(value, SOURCE_MANIFEST_FIELDS, "source manifest")
    p._require(type(value["schemaVersion"]) is int and value["schemaVersion"] == 1,
               "unsupported source manifest version")
    p._uuid(value["id"], "source dataset ID")
    p._hash(value["datasetManifestSha256"], "dataset manifest digest")
    p._hash(value["sha256"], "source manifest digest")
    p._integer(value["totalBytes"], "source total bytes")
    p._integer(value["objectCount"], "source object count")
    objects = value["objects"]
    p._require(type(objects) is list and 0 < len(objects) <= p.MAX_OBJECTS,
               "invalid source object count")
    p._require(value["objectCount"] == len(objects), "source objectCount mismatch")
    total = 0
    for obj in objects:
        p._closed(obj, SOURCE_OBJECT_FIELDS, "source object")
        p._uuid(obj["id"], "dataset file ID")
        _source_basename(obj["name"])
        p._integer(obj["byteSize"], "source object byte size", allow_zero=True)
        p._hash(obj["sha256"], "source object digest")
        p._require(obj["sourceType"] in ("raw_input", "gcp_source_csv"),
                   "invalid source type")
        p._require(obj["retentionClass"] in
                   ("release_after_acceptance", "permanent_viewer_hold"),
                   "invalid source retention class")
        hold_id = obj["retentionHoldId"]
        if obj["retentionClass"] == "permanent_viewer_hold":
            p._require(hold_id == obj["id"], "permanent hold must bind exact dataset file ID")
        else:
            p._require(hold_id is None, "release-after-acceptance object cannot carry a hold")
        if obj["sourceType"] == "gcp_source_csv":
            p._require(obj["name"].lower().endswith(".csv"), "GCP source must be CSV")
            p._require(obj["retentionClass"] == "permanent_viewer_hold",
                       "raw GCP source requires permanent Viewer hold")
        total += obj["byteSize"]
        p._require(total <= MAX_SOURCE_BYTES, "source byte total exceeds safe integer")
    _ordered_ids(objects, "source")
    p._require(total == value["totalBytes"], "source totalBytes mismatch")
    expected = p.digest({key: item for key, item in value.items() if key != "sha256"})
    p._require(value["sha256"] == expected, "source manifest digest mismatch")
    return {item["id"]: item for item in objects}


def _validate_derived_manifest(value, attempt_id):
    p._closed(value, DERIVED_MANIFEST_FIELDS, "derived manifest")
    p._require(type(value["schemaVersion"]) is int and value["schemaVersion"] == 1,
               "unsupported derived manifest version")
    p._require(value["attemptId"] == attempt_id,
               "derived manifest attempt mismatch")
    p._hash(value["sha256"], "derived manifest digest")
    objects = value["objects"]
    p._require(type(objects) is list and len(objects) <= 1,
               "derived manifest supports at most one generated GCP list")
    by_id = {}
    for obj in objects:
        p._closed(obj, DERIVED_OBJECT_FIELDS, "derived input object")
        expected_id = "gcp_" + attempt_id
        p._require(obj["id"] == expected_id, "invalid attempt-derived object ID")
        p._basename(obj["name"])
        p._require(obj["name"] == "gcp_list.txt", "unsupported derived object name")
        p._integer(obj["byteSize"], "derived object byte size", allow_zero=True)
        for field in ("sha256", "snapshotContentSha256", "snapshotEvidenceSha256"):
            p._hash(obj[field], field)
        p._require(obj["snapshotAttemptId"] == attempt_id,
                   "derived object GCP snapshot attempt mismatch")
        p._require(obj["sha256"] == obj["snapshotContentSha256"],
                   "derived object does not match frozen GCP content")
        p._require(obj["sourceType"] == "attempt_derived_gcp_list",
                   "unsupported derived source type")
        p._require(obj["retentionClass"] == "release_after_acceptance"
                   and obj["retentionHoldId"] is None,
                   "derived GCP list must have attempt-scoped retention")
        by_id[obj["id"]] = obj
    _ordered_ids(objects, "derived")
    expected = p.digest({key: item for key, item in value.items() if key != "sha256"})
    p._require(value["sha256"] == expected, "derived manifest digest mismatch")
    return by_id


def validate_source_intake(payload, *, environment, capability_fingerprint,
                           options_fingerprint=None, allowed_odm_options=None,
                           available_engines=("odm", "treedar"),
                           trusted_source_keys=None, authority_id=None):
    """Validate a v2 Viewer submission and return a detached normalized copy.

    The caller must durably persist every identity/digest before acknowledging
    the POST and must prevent claims until source and derived receipt custody is
    separately established by the configured storage owner.
    """
    if isinstance(payload, (str, bytes)):
        payload = p.decode_submission(payload)
    p._require(len(p.canonical_json(payload)) <= p.MAX_BODY_BYTES,
               "source-intake body exceeds 4 MiB")
    p._require(type(payload) is dict and set(payload) == set(INTAKE_FIELDS),
               "source-intake request has missing or unknown fields")
    p._require(type(payload["schemaVersion"]) is int and payload["schemaVersion"] == 2,
               "unsupported source-intake schemaVersion")
    p._uuid(payload["submissionId"], "submissionId")
    provider_task_id = payload["providerTaskId"]
    p._require(provider_task_id is None or type(provider_task_id) is str,
               "invalid providerTaskId")
    if provider_task_id is not None:
        p._uuid(provider_task_id, "providerTaskId")
    viewer_task_id = payload["viewerTaskId"]
    p._uuid(viewer_task_id, "viewerTaskId")
    p._environment(environment)

    source_objects = _validate_source_manifest(payload["sourceManifest"])
    derived_objects = _validate_derived_manifest(payload["derivedManifest"], payload["submissionId"])
    p._require(not (set(source_objects) & set(derived_objects)),
               "raw and derived object IDs overlap")

    v1_payload = {
        "schemaVersion": 1,
        "submissionId": payload["submissionId"],
        "manifest": payload["manifest"],
        "pipeline": payload["pipeline"],
        "capabilityFingerprint": payload["capabilityFingerprint"],
    }
    # providerTaskId is a V2 admission identity, validated above and bound by
    # the complete V2 request digest below. Keep the V1 validator's closed
    # wire format unchanged while validating the shared pipeline contents.
    checked = p.validate_submission(v1_payload, environment=environment,
        capability_fingerprint=capability_fingerprint,
        options_fingerprint=options_fingerprint,
        allowed_odm_options=allowed_odm_options,
        available_engines=available_engines)
    from .reference_source_intake_canonical import pipeline_digest as v2_pipeline_digest
    checked["pipelineDigest"] = v2_pipeline_digest(checked["pipeline"])

    # The Viewer signs its server-owned prepared attempt and source snapshot.
    # Verify this against the authenticated bearer environment and configured
    # queue authority before recording any claimable job metadata.
    if trusted_source_keys is None or authority_id is None:
        raise p.ProtocolError("source attestation verification is not configured")
    p._uuid(authority_id, "queue authority ID")
    from .viewer_source_attestation import validate_and_verify
    attestation = validate_and_verify(payload["sourceAttestation"],
        trusted_keys=trusted_source_keys, authority_id=authority_id,
        environment=environment, submission_id=payload["submissionId"],
        provider_task_id=provider_task_id, attempt_id=payload["submissionId"],
        viewer_task_id=viewer_task_id,
        source_manifest_sha256=payload["sourceManifest"]["sha256"],
        derived_manifest_sha256=payload["derivedManifest"]["sha256"],
        pipeline_manifest_sha256=checked["manifestSha256"],
        pipeline_digest=checked["pipelineDigest"])

    pipeline_objects = checked["manifest"]["objects"]
    for item in pipeline_objects:
        source = source_objects.get(item["id"])
        derived = derived_objects.get(item["id"])
        p._require((source is None) != (derived is None),
                   "pipeline object must map to exactly one retained input")
        retained = source if source is not None else derived
        p._require((item["name"], item["byteSize"], item["sha256"]) ==
                   (retained["name"], retained["byteSize"], retained["sha256"]),
                   "pipeline object differs from retained input")
        p._require(not (source is not None and source["sourceType"] == "gcp_source_csv"),
                   "raw GCP source CSV is retained but not sent to the engine")

    result = copy.deepcopy(payload)
    result.update(environment=environment,
                  requestDigest=p.digest(payload),
                  manifestSha256=checked["manifestSha256"],
                  sourceManifestSha256=payload["sourceManifest"]["sha256"],
                  derivedManifestSha256=payload["derivedManifest"]["sha256"],
                  pipelineDigest=checked["pipelineDigest"],
                  sourceAttestation=attestation,
                  viewerTaskId=viewer_task_id)
    return result
