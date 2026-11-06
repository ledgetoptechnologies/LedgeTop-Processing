"""Pre-enroll one signed Viewer inventory into the real worker gate/scratch.

This function is not a listener. The caller must authenticate the coordinator
principal and supply only locally configured trust, engine and mount settings.
An interrupted preparation retains the physical gate and never frees inputs.
"""
from . import direct_v2_inventory as inventory
from . import direct_worker_scratch_v2 as scratch
from . import direct_worker_readiness_journal_v2 as readiness
from .node_gate import NodeGate
from .scratch_policy import PolicyConflict
from .source_spool import SourceSpool
from . import reference_protocol as p


class WorkerPreparationUncertain(RuntimeError):
    """A node or scratch hold may exist and must not be cleared blindly."""


def prepare_signed_job(ledger, *, payload, item, principal, worker, engine_id,
                       coordinator_origin, gate_generation, source_verification,
                       mount_path, mount_id, marker_name, challenge_id,
                       issuer_id, key_id, private_key, validity_seconds=30):
    """Verify source first; bind physical node, reserve scratch and sign readiness."""
    if type(source_verification) is not dict:
        raise ValueError('protected source verification settings required')
    p._slug(principal,'coordinator principal');p._uuid(challenge_id,'challenge')
    p._slug(issuer_id,'issuer');p._slug(key_id,'key');p._integer(gate_generation,'gate generation')
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    if not isinstance(private_key,Ed25519PrivateKey) or type(validity_seconds) is not int or not 1<=validity_seconds<=60:
        raise ValueError('protected readiness signer required')
    # Viewer principal is authenticated by the independently pinned source
    # signature; coordinator principal is separate and comes from mTLS.
    checked=inventory.prepare(payload,**source_verification)
    normalized,digest,scratch_id=scratch.identity(item,expected_worker=worker)
    reservation={key:normalized[key] for key in ('reservationId','jobId','attemptId','generation',
        'authorityId','environment','nodeId','workerId','workerInstanceId')}
    keys=('authorityId','principalId','environment','submissionId','viewerTaskId','providerTaskId',
        'sourceManifestSha256','derivedManifestSha256','pipelineManifestSha256','pipelineDigest',
        'attestationSha256','sourceSnapshotSha256','preparedEnvelopeSha256','activationId')
    expected={key:checked[key] for key in keys}
    index=inventory.prepare_worker_index(checked,expected_binding=expected,
        reservation_binding=reservation,**source_verification)
    if (any(normalized[key]!=checked[key] for key in ('sourceManifestSha256','derivedManifestSha256',
            'pipelineManifestSha256','inventorySha256'))
            or normalized['objectCount']!=sum(len(checked[key]['objects']) for key in ('sourceManifest','derivedManifest'))
            or normalized['inputBytes']!=sum(obj['byteSize'] for key in ('sourceManifest','derivedManifest') for obj in checked[key]['objects'])):
        raise PolicyConflict('signed worker inventory binding changed')
    # Inspect the actual mount/marker and private worker root before taking a
    # durable node hold; recheck the same identities after taking the gate.
    preflight=SourceSpool(ledger.root,mount_path=mount_path,mount_id=mount_id,marker_name=marker_name,
                          safety_floor_bytes=0,require_mount=True)
    receipt=digest[:32]
    # Once gate inspection begins, even an I/O failure may conceal an earlier
    # committed attempt. Never infer a free node from an unreadable gate.
    may_be_held=True
    try:
        with NodeGate(engine_id) as gate:
            preflight._assert_mount()
            state=gate.snapshot()
            if (state['nodeId']!=normalized['nodeId'] or state['coordinatorOrigin']!=coordinator_origin
                    or state['authorityId']!=normalized['authorityId'] or state['generation']!=gate_generation):
                raise PolicyConflict('node authority or generation changed')
            job=dict(id=normalized['jobId'],workerId=normalized['workerId'],
                     nodeId=normalized['nodeId'],authorityId=normalized['authorityId'])
            if state['reservation'] is None:
                may_be_held=True
                gate.reserve_direct_bound(normalized['nodeId'],coordinator_origin,normalized['authorityId'],
                                          gate_generation,receipt,job,digest)
            else:
                held=state['reservation']
                if (held['receiptId']!=receipt or held['job']!=job or held['completionDigest'] is not None):
                    raise PolicyConflict('node has another or uncertain reservation')
                may_be_held=True
                expected_hold=dict(receiptId=receipt,generation=gate_generation,scratchBindingSha256=digest)
                if state['schema']!=2 or state['directHold']!=expected_hold:
                    raise PolicyConflict('direct node hold requires reconciliation')
            scratch.prepare(ledger,normalized,expected_worker=worker,worker_ready=True)
            directory=ledger.prepare_job_directory(scratch_id)/'direct-source'
            directory.mkdir(mode=0o700,exist_ok=True)
            spool=SourceSpool(directory,mount_path=mount_path,mount_id=mount_id,marker_name=marker_name,
                              safety_floor_bytes=0,require_mount=True)
            preflight._assert_mount();spool._assert_mount()
        proof=readiness.issue(ledger,spool,normalized,principal=principal,expected_worker=worker,
            engine_id=engine_id,coordinator_origin=coordinator_origin,gate_generation=gate_generation,
            challenge_id=challenge_id,validity_seconds=validity_seconds,issuer_id=issuer_id,
            key_id=key_id,private_key=private_key)
        return index,spool,proof
    except Exception:
        if may_be_held:
            raise WorkerPreparationUncertain('worker preparation requires reconciliation') from None
        raise
