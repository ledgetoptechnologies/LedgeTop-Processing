"""Versioned wire manifests for the private Viewer accepted-copy API.

Processing's durable worker receipt remains reference-protocol v1. TreeDar
artifacts use a distinct Viewer wire manifest so the ODM-only Viewer v1 route
stays unchanged while preserving the complete sequential ODM→TreeDar closure.
"""
from copy import deepcopy

from . import reference_protocol as protocol

TREEDAR_PROTOCOL = "ltds-reference-result-v2"
TREEDAR_ROLES = frozenset((
    "ground_model", "canopy_height", "canopy_height_unfiltered", "crown_labels",
    "area_labels", "orthophoto_point_colour", "crowns", "tree_table", "tree_features",
    "tree_areas", "lasting_tree_ids", "area_types_template", "labels",
    "tree_type_predictions", "quality_report", "run_summary", "classifier_report",
    "preview", "area_map_preview", "report", "final_test_predictions", "engine_manifest",
))
TREEDAR_REQUIRED_ROLES = frozenset((
    "ground_model", "canopy_height", "canopy_height_unfiltered", "crown_labels",
    "crowns", "tree_table", "tree_features", "run_summary", "report", "preview",
    "area_map_preview", "engine_manifest",
))
V2_FIELDS = frozenset((
    "protocol", "schemaVersion", "jobId", "generation", "environment",
    "pipelineDigest", "engineVersions", "objects", "resultManifestSha256",
))


class ViewerManifestRejected(ValueError):
    pass


def _need(condition, reason):
    if not condition:
        raise ViewerManifestRejected(reason)


def _validate_v2_roles(roles, engines):
    for role in roles:
        _need(role in TREEDAR_ROLES or ("odm" in engines and role == "odm_archive"),
              "viewer_v2_role_unsupported")
    counts = {role: roles.count(role) for role in set(roles)}
    _need(all(counts.get(role) == 1 for role in TREEDAR_REQUIRED_ROLES),
          "viewer_v2_required_role_missing")
    _need(counts.get("report", 0) in ((1, 2) if "odm" in engines else (1,)),
          "viewer_v2_report_closure_invalid")
    _need(all(count <= (2 if role == "report" else 1) for role, count in counts.items()),
          "viewer_v2_role_multiplicity_invalid")
    _need(counts.get("odm_archive", 0) == (1 if "odm" in engines else 0),
          "viewer_v2_odm_archive_closure_invalid")


def derive_viewer_result_manifest(result_manifest, *, job_id, generation, environment,
                                  pipeline_digest):
    """Convert one sealed internal result into its exact Viewer wire schema."""
    try:
        source = protocol.validate_result_manifest(result_manifest, job_id=job_id,
            generation=generation, environment=environment, pipeline_digest=pipeline_digest)
    except (TypeError, ValueError):
        raise ViewerManifestRejected("invalid_processing_result_manifest") from None
    engines = set(source["engineVersions"])
    if engines == {"odm"}:
        return source
    _need(engines in ({"treedar"}, {"odm", "treedar"}), "unsupported_viewer_engine_set")
    objects = source["objects"]
    _need(0 < len(objects) <= 100, "viewer_v2_object_count_invalid")
    roles = [item["role"] for item in objects]
    _validate_v2_roles(roles, engines)
    payload = {
        "protocol": TREEDAR_PROTOCOL,
        "schemaVersion": 2,
        "jobId": source["jobId"],
        "generation": source["generation"],
        "environment": source["environment"],
        "pipelineDigest": source["pipelineDigest"],
        "engineVersions": deepcopy(source["engineVersions"]),
        "objects": deepcopy(objects),
    }
    payload["resultManifestSha256"] = protocol.digest(payload)
    return validate_viewer_result_manifest(payload, job_id=job_id, generation=generation,
        environment=environment, pipeline_digest=pipeline_digest)


def validate_viewer_result_manifest(payload, *, job_id, generation, environment,
                                    pipeline_digest):
    """Validate either unchanged ODM v1 or the private TreeDar v2 wire schema."""
    if type(payload) is not dict:
        raise ViewerManifestRejected("invalid_viewer_result_manifest")
    if payload.get("schemaVersion") == 1:
        try:
            source = protocol.validate_result_manifest(payload, job_id=job_id,
                generation=generation, environment=environment, pipeline_digest=pipeline_digest)
        except (TypeError, ValueError):
            raise ViewerManifestRejected("invalid_viewer_result_manifest") from None
        _need(set(source["engineVersions"]) == {"odm"}, "viewer_v1_is_odm_only")
        _need(all(item["role"] in {"pointcloud", "mesh", "orthophoto", "dem", "report", "tree_inventory", "odm_archive"}
                  for item in source["objects"]), "viewer_v1_role_unsupported")
        return source
    _need(set(payload) == V2_FIELDS and payload.get("protocol") == TREEDAR_PROTOCOL
          and type(payload.get("schemaVersion")) is int and payload["schemaVersion"] == 2,
          "invalid_viewer_v2_fields")
    # Reuse the internal identity, scalar, and closed-object validation while
    # computing the v2 digest over the Viewer-specific protocol discriminator.
    base = {key: value for key, value in payload.items() if key not in ("protocol", "resultManifestSha256")}
    base["schemaVersion"] = 1
    base["resultManifestSha256"] = protocol.digest({key: value for key, value in base.items()
        if key != "resultManifestSha256"})
    try:
        checked = protocol.validate_result_manifest(base, job_id=job_id, generation=generation,
            environment=environment, pipeline_digest=pipeline_digest)
    except (TypeError, ValueError):
        raise ViewerManifestRejected("invalid_viewer_v2_identity_or_objects") from None
    engines = set(checked["engineVersions"])
    _need(engines in ({"treedar"}, {"odm", "treedar"}), "unsupported_viewer_engine_set")
    objects = checked["objects"]
    _need(0 < len(objects) <= 100, "viewer_v2_object_count_invalid")
    _need(all(type(item["byteSize"]) is int and item["byteSize"] > 0 for item in objects),
          "viewer_v2_object_size_invalid")
    roles = [item["role"] for item in objects]
    _validate_v2_roles(roles, engines)
    _need((checked["jobId"], checked["generation"], checked["environment"], checked["pipelineDigest"])
          == (job_id, generation, environment, pipeline_digest), "viewer_v2_job_binding_mismatch")
    expected_digest = protocol.digest({key: value for key, value in payload.items()
        if key != "resultManifestSha256"})
    _need(payload["resultManifestSha256"] == expected_digest, "viewer_v2_digest_mismatch")
    return deepcopy(payload)


def validate_viewer_acceptance(payload, *, result_manifest, input_manifest_sha256):
    """Require the durable Viewer acceptance to cover the exact ordered closure."""
    manifest = validate_viewer_result_manifest(result_manifest,
        job_id=result_manifest.get("jobId"), generation=result_manifest.get("generation"),
        environment=result_manifest.get("environment"), pipeline_digest=result_manifest.get("pipelineDigest"))
    try:
        checked = protocol.validate_acceptance(payload, result_manifest=manifest,
            input_manifest_sha256=input_manifest_sha256)
    except (TypeError, ValueError):
        raise ViewerManifestRejected("invalid_viewer_acceptance") from None
    expected_ids = [item["id"] for item in manifest["objects"]]
    if manifest["schemaVersion"] == 2:
        _need(checked["acceptedObjectIds"] == expected_ids, "viewer_acceptance_not_exact_ordered_closure")
    return checked
