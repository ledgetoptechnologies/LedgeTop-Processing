"""Disabled v2 local-input physical-node gate prerequisite; no engine start.

All engine clients must cooperate with the same NodeGate; direct ingress, old
workers using another gate and mount/state replacement are outside this fence.
The caller persists an unpredictable receipt and authenticated binding before
calling and owns the worker/file fence until eventual engine submission.
"""
from .node_gate import NodeGate, GateError
from .reference_v2_local_input import verify_complete_input_set


def retain_node_for_verified_input(*, fence, journal, ledger, reservation_id,
        binding, manifest_bytes, engine_id, node_id, origin, authority_id,
        generation, worker_id, receipt_id):
    """Reserve/bind NodeGate before verification; retain on any ambiguity.

    Exact receipt replay can bind a reservation left empty by a crash. The
    caller must durably own that receipt and fence all prior users of it. This
    function never starts an engine, clears the node gate or the scratch hold,
    and its point-in-time observation is not authority to start one.
    """
    fence.check()
    with NodeGate(engine_id) as gate:
        state=gate.snapshot()
        if (state['nodeId'],state['coordinatorOrigin'],state['authorityId'],state['generation'])!=(node_id,origin,authority_id,generation):
            raise GateError('node ownership changed')
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
