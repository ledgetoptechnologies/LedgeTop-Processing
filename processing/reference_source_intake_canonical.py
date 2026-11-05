"""V2-only pipeline digest matching the Viewer prepared-envelope canonicalizer.

V1 digest/history deliberately retains its original serialization. Callers must
validate the complete pipeline and option policy before invoking this helper.
"""
import hashlib

import rfc8785

from . import reference_protocol as p
from .reference_protocol import ProtocolError


def pipeline_digest(pipeline):
    if type(pipeline) is not dict:
        raise ProtocolError("V2 pipeline must be an object")
    try:
        return hashlib.sha256(rfc8785.dumps(pipeline)).hexdigest()
    except (rfc8785.CanonicalizationError, TypeError, ValueError):
        raise ProtocolError("V2 pipeline cannot be canonicalized") from None


def _source_version(value):
    if type(value) is not dict:
        raise ProtocolError("bound job/submission required")
    if "sourceIntakeVersion" not in value:
        return 1
    version = value["sourceIntakeVersion"]
    if type(version) is not int or version != 2:
        raise ProtocolError("unsupported source intake digest version")
    return version


def bind_validated_submission(checked, original):
    """Apply the persisted V2 digest marker after ordinary pipeline validation.

    The caller must still compare the entire returned value with original;
    this does not accept a digest supplied by the unvalidated submission.
    """
    if _source_version(original) == 2:
        return {**checked, "sourceIntakeVersion": 2,
                "pipelineDigest": pipeline_digest(checked["pipeline"])}
    return checked


def pipeline_digest_for_job(job):
    """Require matching envelope/submission versions; never try both hashes."""
    version = _source_version(job)
    submission = job["submission"]
    if _source_version(submission) != version:
        raise ProtocolError("source intake digest version binding mismatch")
    return (pipeline_digest(submission["pipeline"]) if version == 2
            else p.digest(submission["pipeline"]))
