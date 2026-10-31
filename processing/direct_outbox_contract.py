"""Draft offline identity contract; intentionally not an execution or admission gate.

Validation proves only message shape and digest binding. It does not authenticate
a sender, reserve disk, own an engine, create a durable record or authorize any
job, cleanup, result transfer, endpoint, or live deployment.
"""
from . import reference_protocol as p

FIELDS = ('schemaVersion', 'authorityId', 'gateGeneration', 'gateReservationId',
          'nodeId', 'workerId', 'engineId', 'environment', 'viewerTaskId',
          'viewerGeneration', 'jobId', 'engineTaskId', 'inputSetSha256',
          'outputBudgetBytes')


def validate_binding(binding):
    """Return a canonical hash of an exact proposed enrollment identity.

    Every field comes from authenticated, independently verified durable state
    in a future integration. Caller-supplied values alone have no authority.
    """
    p._closed(binding, FIELDS, 'direct outbox binding')
    p._require(type(binding['schemaVersion']) is int and binding['schemaVersion'] == 1,
               'unsupported direct outbox schema')
    for field in ('authorityId', 'viewerTaskId', 'jobId', 'engineTaskId'):
        p._uuid(binding[field], field)
    for field in ('gateGeneration', 'viewerGeneration', 'outputBudgetBytes'):
        p._integer(binding[field], field)
    for field in ('gateReservationId', 'nodeId', 'workerId', 'engineId'):
        p._slug(binding[field], field)
    p._environment(binding['environment'])
    p._hash(binding['inputSetSha256'], 'inputSetSha256')
    return p.digest(binding)


def require_same_binding(expected, candidate):
    """Reject an identity change; never interpret a matching hash as a receipt."""
    expected_digest = validate_binding(expected)
    candidate_digest = validate_binding(candidate)
    p._require(expected_digest == candidate_digest, 'direct outbox binding changed')
    return expected_digest
