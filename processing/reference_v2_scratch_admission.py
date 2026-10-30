"""Experimental metadata-bound scratch reservation, not a worker or API.

The trusted caller must own/fence the worker and coordinator. This does not
authorize transfer, claim an execution slot, start an engine, or release data.
Cross-database partial outcomes are deliberately retained for reconciliation.
"""
from . import reference_protocol as p
from .reference_input_budget_v2 import decode_canonical_input_v2
from .reference_v2_metadata_ledger import V2MetadataLedger
from .scratch_policy import ScratchLedger, PolicyConflict


class ScratchAdmissionHeld(ValueError):
    pass


def reserve_v2_scratch(metadata, scratch, *, job_id, principal, claim_token,
                       worker_identity, worker_ready, output_budget, scratch_ttl, clock, enabled=False):
    """Reserve input and output capacity only for the exact claimed v2 job.

    `worker_identity` is trusted locally configured identity, never user input.
    A successful result is not a grant, engine-start authority or receipt of
    any physical files. The scratch TTL is a fixed retry-stable local policy;
    it does not extend the metadata reservation or a broker credential.
    """
    if enabled is not True:
        raise ScratchAdmissionHeld('v2 scratch admission disabled')
    if type(metadata) is not V2MetadataLedger or type(scratch) is not ScratchLedger:
        raise ScratchAdmissionHeld('actual metadata and scratch ledgers required')
    if worker_ready is not True:
        raise ScratchAdmissionHeld('worker readiness unavailable')
    p._closed(worker_identity, ('nodeId', 'workerId', 'instanceId'), 'worker identity')
    for key in worker_identity: p._slug(worker_identity[key], key)
    p._integer(output_budget, 'output budget', allow_zero=True)
    p._integer(scratch_ttl, 'scratch TTL')
    if scratch_ttl > 86400:
        raise ScratchAdmissionHeld('scratch TTL exceeds bound')
    snapshot = metadata.snapshot_bytes(job_id, principal=principal, claim_token=claim_token, clock=clock)
    identity = snapshot['claimIdentity']
    if any(identity[key] != value for key,value in worker_identity.items()):
        raise ScratchAdmissionHeld('worker identity differs')
    manifest = decode_canonical_input_v2(snapshot['manifestBytes'])
    reservation = decode_canonical_input_v2(snapshot['reservationBytes'])
    if (reservation['jobId'] != job_id or reservation['manifestSha256'] != manifest['sha256']
            or snapshot['manifestSha256'] != manifest['sha256']):
        raise ScratchAdmissionHeld('metadata binding differs')
    try:
        held = scratch.reserve(job_id=job_id, generation=reservation['generation'],
            reservation_id=reservation['reservationId'], manifest_sha256=manifest['sha256'],
            input_bytes=manifest['totalBytes'], output_budget=output_budget,
            environment=reservation['environment'], ttl_seconds=scratch_ttl, worker_ready=worker_ready)
    except (PolicyConflict, ValueError):
        raise ScratchAdmissionHeld('scratch reservation unavailable') from None
    if (held['state'] != 'reserved' or held['allocated'] != 0
            or held['remaining'] != manifest['totalBytes'] + output_budget):
        raise ScratchAdmissionHeld('scratch reservation requires reconciliation')
    # A crash, expiry or changed epoch between the two independent commits is
    # uncertain: never release capacity or authorize work on that path.
    again = metadata.snapshot_bytes(job_id, principal=principal, claim_token=claim_token, clock=clock)
    if again != snapshot:
        raise ScratchAdmissionHeld('metadata changed after reservation')
    return {'jobId':job_id, 'reservationId':reservation['reservationId'],
            'manifestSha256':manifest['sha256'], 'inputBytes':manifest['totalBytes'],
            'outputBudget':output_budget, 'nodeId':worker_identity['nodeId']}
