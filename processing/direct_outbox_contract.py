"""Draft offline identity contract; intentionally not an execution or admission gate.

Validation proves only message shape and digest binding. It does not authenticate
a sender, reserve disk, own an engine, create a durable record or authorize any
job, cleanup, result transfer, endpoint, or live deployment.
"""
import hashlib
import re

from . import reference_protocol as p
from . import node_gate
from .viewer_tasks import canonical as viewer_canonical

FIELDS = ('schemaVersion', 'authorityId', 'coordinatorOrigin', 'gateGeneration', 'gateReservationId',
          'nodeId', 'workerId', 'engineId', 'environment', 'viewerTaskId',
          'viewerGeneration', 'jobId', 'engineTaskId', 'inputSetSha256',
          'pipelineSha256', 'outputBudgetBytes')


def direct_pipeline_digest(options, outputs):
    """Hash direct Viewer settings using its existing canonical JSON spelling.

    The arrays retain stored order, which may be significant to the engine.
    This is a proposed identity calculation, not proof these settings were used.
    """
    p._require(isinstance(options,list) and len(options)<=1000 and all(
        isinstance(item,dict) and set(item)=={'name','value'} and isinstance(item['name'],str)
        and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,119}',item['name']) for item in options),
        'invalid direct options')
    p._require(len({item['name'] for item in options})==len(options),'duplicate direct options')
    p._require(outputs is None or (isinstance(outputs,list) and len(outputs)<=1000 and all(
        isinstance(value,str) and len(value)<=256 for value in outputs)), 'invalid direct outputs')
    try:
        options_text=viewer_canonical(options);outputs_text=viewer_canonical(outputs)
        p._require(len(options_text.encode())+len(outputs_text.encode())<=65536,'direct settings too large')
        encoded=viewer_canonical({'options':options,'outputs':outputs}).encode('utf-8')
    except (TypeError,ValueError,UnicodeError,RecursionError):
        raise p.ProtocolError('invalid direct settings') from None
    return hashlib.sha256(encoded).hexdigest()


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
    try:
        actual=node_gate._binding(binding['nodeId'],binding['coordinatorOrigin'],binding['authorityId'])
    except node_gate.GateError:
        raise p.ProtocolError('invalid node gate binding') from None
    p._require(actual['coordinatorOrigin']==binding['coordinatorOrigin'],'noncanonical coordinator origin')
    p._require(isinstance(binding['gateReservationId'],str) and node_gate.RECEIPT.fullmatch(binding['gateReservationId']),
               'invalid gate reservation identity')
    p._require(isinstance(binding['workerId'],str) and node_gate.SLUG.fullmatch(binding['workerId']),
               'invalid worker identity')
    p._require(isinstance(binding['engineId'],str) and node_gate.ENGINE.fullmatch(binding['engineId']),
               'invalid engine identity')
    p._environment(binding['environment'])
    p._hash(binding['inputSetSha256'], 'inputSetSha256')
    p._hash(binding['pipelineSha256'], 'pipelineSha256')
    return p.digest(binding)


def require_same_binding(expected, candidate):
    """Reject an identity change; never interpret a matching hash as a receipt."""
    expected_digest = validate_binding(expected)
    candidate_digest = validate_binding(candidate)
    p._require(expected_digest == candidate_digest, 'direct outbox binding changed')
    return expected_digest
