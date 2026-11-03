"""Held-only signed job binding experiment; no authority handover or activation.

The signing key is independent from the node transport bearer. A valid MAC only
proves possession of that key, not that a coordinator lease is current. This
module never authorizes recovery, replay or result deletion.
"""
import hashlib
import hmac
import re
import time
import uuid

from . import reference_protocol as protocol
from .node_gate import GateError

_FIELDS = {'schema', 'viewerJobId', 'viewerGeneration', 'engineTaskId', 'nodeId',
           'authorityId', 'gateGeneration', 'coordinatorOrigin', 'environment', 'requestSha256', 'nonce', 'expiresAt'}
_HASH = re.compile(r'[0-9a-f]{64}\Z')


def _uuid(value):
    try:
        return type(value) is str and str(uuid.UUID(value)) == value
    except (ValueError, TypeError, AttributeError):
        return False


def verify_job_claim(envelope, key, *, node, authority, gate_generation, task, origin, now=None, admission=True):
    if not isinstance(key, bytes) or len(key) < 32 or len(key) > 4096:
        raise GateError('job claim verification unavailable')
    if type(envelope) is not dict or set(envelope) != {'claim', 'mac'}:
        raise GateError('invalid job claim')
    claim, mac = envelope['claim'], envelope['mac']
    if (type(claim) is not dict or set(claim) != _FIELDS or type(mac) is not str or not _HASH.fullmatch(mac)
            or claim['schema'] != 'ledgetop-node-job-claim-v1'
            or claim['environment'] not in ('production', 'staging')
            or claim['coordinatorOrigin'] != origin
            or not _uuid(claim['viewerJobId']) or not _uuid(claim['engineTaskId'])
            or not _uuid(claim['authorityId'])
            or type(claim['viewerGeneration']) is not int or claim['viewerGeneration'] < 1
            or type(claim['gateGeneration']) is not int or claim['gateGeneration'] < 1
            or type(claim['expiresAt']) is not int or claim['expiresAt'] < 1
            or type(claim['nonce']) is not str or not _HASH.fullmatch(claim['nonce'])
            or type(claim['requestSha256']) is not str or not _HASH.fullmatch(claim['requestSha256'])
            or (claim['nodeId'], claim['authorityId'], claim['gateGeneration'], claim['engineTaskId'])
                != (node, authority, gate_generation, task)):
        raise GateError('invalid job claim binding')
    expected = hmac.new(key, b'ledgetop-node-job-claim-v1\x00' + protocol.canonical_json(claim), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(mac, expected):
        raise GateError('invalid job claim authentication')
    if admission:
        current = int(time.time()) if now is None else now
        if type(current) is not int or not current < claim['expiresAt'] <= current + 900:
            raise GateError('job claim expired or too long')
    return dict(claim)
