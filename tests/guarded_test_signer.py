"""Synthetic operation signer shared by paired guarded-transport fixtures."""
import hashlib
import hmac
import time

from processing import reference_protocol as protocol
from processing.viewer_tasks import TaskStore


def operation_signer(proxy, key, envelope):
    def sign(kind, request):
        name_key = hashlib.sha256(TaskStore._filename(request['name']).encode('ascii')).hexdigest() if kind == 'upload' else None
        op_id = proxy._op(request['task'], kind, name_key) if name_key else proxy._op(request['task'], kind)
        op = dict(schema='ledgetop-node-operation-claim-v1', jobClaimSha256=protocol.digest(envelope['claim']),
                  operationId=op_id, kind=kind, requestSha256=protocol.digest(request),
                  expiresAt=int(time.time()) + 300)
        mac = hmac.new(key, b'ledgetop-node-operation-claim-v1\x00' + protocol.canonical_json(op), hashlib.sha256).hexdigest()
        return {'jobClaim': envelope, 'operation': op, 'mac': mac}
    return sign
