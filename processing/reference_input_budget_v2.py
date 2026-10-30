"""Provisional input-v2 budget calculation; not a grant or runtime protocol.

The caller must authenticate and verify the manifest binding separately. This
planner does not reserve broker capacity, reconcile charges or renew grants.
"""
from datetime import datetime
from . import reference_protocol as p

MAX_OBJECTS = 32_768
MAX_METADATA_BYTES = 16 * 1024 * 1024
MAX_RANGE_BYTES = 16 * 1024 * 1024


def plan_input_budget(proposal):
    p._closed(proposal, ('protocolVersion', 'jobId', 'environment', 'reservationId',
                         'generation', 'manifestSha256', 'capabilityFingerprint',
                         'expiresAt', 'objects', 'rangeBytes', 'retryRequests',
                         'retryBytes'), 'budget proposal')
    p._require(type(proposal['protocolVersion']) is int and proposal['protocolVersion'] == 2,
               'unsupported proposal version')
    for field in ('jobId', 'reservationId'):
        p._slug(proposal[field], field)
    p._environment(proposal['environment'])
    p._integer(proposal['generation'], 'generation')
    for field in ('manifestSha256', 'capabilityFingerprint'):
        p._hash(proposal[field], field)
    expiry = proposal['expiresAt']
    p._require(type(expiry) is str and len(expiry) <= 40 and expiry.endswith('Z'), 'invalid expiry')
    try:
        datetime.fromisoformat(expiry[:-1] + '+00:00')
    except (ValueError, OverflowError):
        raise p.ProtocolError('invalid expiry') from None
    p._integer(proposal['rangeBytes'], 'range bytes')
    p._require(proposal['rangeBytes'] <= MAX_RANGE_BYTES, 'range ceiling exceeded')
    for field in ('retryRequests', 'retryBytes'):
        p._integer(proposal[field], field, allow_zero=True)
    objects = proposal['objects']
    p._require(type(objects) is list and 0 < len(objects) <= MAX_OBJECTS, 'invalid object count')
    seen = set()
    total_bytes = total_requests = 0
    for item in objects:
        p._closed(item, ('id', 'byteSize', 'sha256'), 'budget object')
        p._slug(item['id'], 'object id')
        p._integer(item['byteSize'], 'object bytes', allow_zero=True)
        p._hash(item['sha256'], 'object hash')
        p._require(item['id'] not in seen, 'duplicate object id')
        seen.add(item['id'])
        total_bytes += item['byteSize']
        # An empty object still requires a request.
        total_requests += max(1, (item['byteSize'] + proposal['rangeBytes'] - 1) // proposal['rangeBytes'])
        p._require(total_bytes <= p.MAX_SAFE_INTEGER and total_requests <= p.MAX_SAFE_INTEGER,
                   'aggregate exceeds safe integer')
    byte_budget = total_bytes + proposal['retryBytes']
    request_budget = total_requests + proposal['retryRequests']
    p._require(byte_budget <= p.MAX_SAFE_INTEGER and request_budget <= p.MAX_SAFE_INTEGER,
               'budget exceeds safe integer')
    encoded = p.canonical_json(proposal)
    p._require(len(encoded) <= MAX_METADATA_BYTES, 'metadata ceiling exceeded')
    return {'protocolVersion': 2, 'proposalSha256': p.digest(proposal),
            'requiredRequests': total_requests, 'requestBudget': request_budget,
            'byteBudget': byte_budget}
