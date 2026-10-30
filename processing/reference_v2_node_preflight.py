"""Disabled v2 local-input physical-node gate prerequisite; no engine start.

All engine clients must cooperate with the same NodeGate; direct ingress, old
workers using another gate and mount/state replacement are outside this fence.
The caller persists a unique unpredictable 32-byte operation key and authenticated binding before
calling and owns the worker/file fence until eventual engine submission.
"""
import hashlib
import hmac
from . import reference_protocol as p
from .node_gate import NodeGate, GateError
from .reference_v2_local_input import verify_complete_input_set


def operation_receipt(*, operation_key, binding, reservation_id, engine_id, node_id, origin, authority_id, generation, worker_id):
    """Stable operation-bound receipt; caller durably stores its random key.

    This is an identity commitment, not authorization or proof of key custody.
    """
    if type(operation_key) is not bytes or len(operation_key)!=32:
        raise GateError('32-byte durable unique operation key required')
    identity=dict(binding=binding,reservationId=reservation_id,engineId=engine_id,nodeId=node_id,
                  coordinatorOrigin=origin,authorityId=authority_id,gateGeneration=generation,workerId=worker_id)
    return hmac.new(operation_key,b'ledgetop-v2-node-hold\0'+p.canonical_json(identity),hashlib.sha256).hexdigest()[:32]


def retain_node_for_verified_input(*, fence, journal, ledger, reservation_id,
        binding, manifest_bytes, engine_id, node_id, origin, authority_id,
        generation, worker_id, operation_key):
    """Reserve/bind NodeGate before verification; retain on any ambiguity.

    Exact receipt replay can bind a reservation left empty by a crash. The
    caller must durably own the operation key and fence all prior users of it. This
    function never starts an engine, clears the node gate or the scratch hold,
    and its point-in-time observation is not authority to start one.
    """
    receipt_id=operation_receipt(operation_key=operation_key,binding=binding,reservation_id=reservation_id,
        engine_id=engine_id,node_id=node_id,origin=origin,authority_id=authority_id,generation=generation,worker_id=worker_id)
    fence.check()
    with NodeGate(engine_id) as gate:
        state=gate.snapshot()
        if (state['nodeId'],state['coordinatorOrigin'],state['authorityId'],state['generation'])!=(node_id,origin,authority_id,generation):
            raise GateError('node ownership changed')
        last_ack=state.get('lastAcknowledgment') or {}
        last_resolution=(state.get('lastResolution') or {}).get('reservation') or {}
        if last_ack.get('receiptId')==receipt_id or last_resolution.get('receiptId')==receipt_id:
            raise GateError('previously resolved receipt cannot be reused')
        owner=state['reservation']
        if owner is None:
            gate.reserve(node_id,origin,authority_id,generation,receipt_id)
        elif owner['receiptId']!=receipt_id or owner['completionDigest'] is not None:
            raise GateError('node has another or completed owner')
        gate.bind(receipt_id,dict(id=binding['jobId'],workerId=worker_id,nodeId=node_id,authorityId=authority_id))
        fence.check()
        result=verify_complete_input_set(fence=fence,journal=journal,ledger=ledger,reservation_id=reservation_id,binding=binding,manifest_bytes=manifest_bytes)
        fence.check()
        return {'state':'node_and_input_held','receiptId':receipt_id,'objectCount':result['objectCount']}
