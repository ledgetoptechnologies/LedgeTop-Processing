"""V2-only pipeline digest matching the Viewer prepared-envelope canonicalizer.

V1 digest/history deliberately retains its original serialization. Callers must
validate the complete pipeline and option policy before invoking this helper.
"""
import hashlib

import rfc8785

from .reference_protocol import ProtocolError


def pipeline_digest(pipeline):
    if type(pipeline) is not dict:
        raise ProtocolError("V2 pipeline must be an object")
    try:
        return hashlib.sha256(rfc8785.dumps(pipeline)).hexdigest()
    except (rfc8785.CanonicalizationError, TypeError, ValueError):
        raise ProtocolError("V2 pipeline cannot be canonicalized") from None
