"""Experimental operation-scoped MAC; not a current-authority proof."""
import hashlib
import hmac
import re

from . import reference_protocol as protocol
from .node_gate import GateError
from .node_job_claim import verify_job_claim, _now

_FIELDS = {'schema', 'jobClaimSha256', 'operationId', 'kind', 'requestSha256', 'expiresAt'}
_HASH = re.compile(r'[0-9a-f]{64}\Z')


def verify_operation_claim(envelope, key, *, node, authority, gate_generation, task, origin,
                           operation_id, kind, request_digest):
    if type(envelope) is not dict or set(envelope) != {'jobClaim', 'operation', 'mac'}:
        raise GateError('operation claim required')
    job = verify_job_claim(envelope['jobClaim'], key, node=node, authority=authority,
                           gate_generation=gate_generation, task=task, origin=origin, admission=False)
    op, mac = envelope['operation'], envelope['mac']
    now = _now()
    if (type(op) is not dict or set(op) != _FIELDS or op['schema'] != 'ledgetop-node-operation-claim-v1'
            or op['jobClaimSha256'] != protocol.digest(job)
            or (op['operationId'], op['kind'], op['requestSha256']) != (operation_id, kind, request_digest)
            or type(op['expiresAt']) is not int or not now < op['expiresAt'] <= now + 900
            or type(mac) is not str or not _HASH.fullmatch(mac)):
        raise GateError('invalid operation claim binding')
    expected = hmac.new(key, b'ledgetop-node-operation-claim-v1\x00' + protocol.canonical_json(op),
                        hashlib.sha256).hexdigest()
    if not hmac.compare_digest(mac, expected):
        raise GateError('invalid operation claim authentication')
    return job
