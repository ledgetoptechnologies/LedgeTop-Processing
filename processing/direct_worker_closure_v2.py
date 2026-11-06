"""V2 local all-source observation; never Viewer custody or execution."""
import os
from pathlib import Path

from .direct_v2_inventory import VerifiedWorkerInventory
from .direct_worker_closure import _persist_receipt
from .direct_worker_receiver import check_scope
from .direct_worker_scratch_v2 import require_fenced, settings
from .node_gate import NodeGate
from .scratch_policy import PolicyConflict


def verify_closure(ledger, spool, inventory, item, *, expected_binding, reservation_binding,
                   verification, expected_worker, engine_id, coordinator_origin, gate_generation,
                   worker_signer=None):
    """Reverify signed inventory and every file, keeping the node held afterward."""
    if not isinstance(inventory, VerifiedWorkerInventory) or type(verification) is not dict:
        raise PolicyConflict('verified V2 inventory required')
    item, digest, scratch_id = require_fenced(ledger, item, expected_worker=expected_worker)
    checked = inventory.revalidate_for_closure(expected_binding=expected_binding,
        reservation_binding=reservation_binding, **verification)
    if (any(item[key] != reservation_binding[key] for key in reservation_binding)
            or any(item[key] != checked[key] for key in ('sourceManifestSha256',
                'derivedManifestSha256', 'pipelineManifestSha256', 'inventorySha256'))):
        raise PolicyConflict('V2 closure inventory binding changed')
    ids = inventory.object_ids(reservation_binding=reservation_binding)
    if (len(ids) != item['objectCount'] or item['inputBytes'] != sum(
            obj['byteSize'] for key in ('sourceManifest','derivedManifest')
            for obj in checked[key]['objects'])):
        raise PolicyConflict('V2 closure object count changed')
    result = dict(protocol='ltds-direct-local-closure-v2', reservationId=item['reservationId'],
        scratchBindingSha256=digest, jobId=item['jobId'], attemptId=item['attemptId'],
        generation=item['generation'], authorityId=item['authorityId'], nodeId=item['nodeId'],
        workerId=item['workerId'], workerInstanceId=item['workerInstanceId'],
        sourceManifestSha256=item['sourceManifestSha256'], derivedManifestSha256=item['derivedManifestSha256'],
        pipelineManifestSha256=item['pipelineManifestSha256'], inventorySha256=item['inventorySha256'],
        sourceObjectCount=len(checked['sourceManifest']['objects']),
        derivedObjectCount=len(checked['derivedManifest']['objects']), localClosureComplete=True,
        sourceCustodyAccepted=False, transferComplete=False, processingAuthorized=False)
    with NodeGate(engine_id) as gate:
        check_scope(ledger, spool, item, digest, scratch_id, gate,
                    coordinator_origin=coordinator_origin, gate_generation=gate_generation)
        gate._arm_direct_hold(digest[:32], digest)
        receipt_parent = spool.root.parent
        if receipt_parent != spool.mount_path and spool.mount_path not in receipt_parent.parents:
            raise PolicyConflict('V2 closure receipt outside verified mount')
        spool._assert_mount()
        if receipt_parent.stat().st_dev != spool._mount_identity[0]:
            raise PolicyConflict('V2 closure receipt mount changed')
        for object_id in ids:
            try: (spool.root / spool.storage_key(item['jobId'], item['generation'], object_id)).lstat()
            except FileNotFoundError: raise PolicyConflict('V2 closure incomplete') from None
        pin = ledger.begin_direct_writer(scratch_id, settings(item, scratch_id))
        spool._assert_mount()
        for object_id in ids:
            obj = inventory.object(object_id, reservation_binding=reservation_binding)
            spool.verify_object(job_id=item['jobId'], generation=item['generation'], object_id=object_id,
                                byte_size=obj['byteSize'], sha256=obj['sha256'])
        directory = spool.root / item['jobId'] / str(item['generation'])
        names = {Path(spool.storage_key(item['jobId'], item['generation'], object_id)).name for object_id in ids}
        with os.scandir(directory) as entries:
            found = set()
            for entry in entries:
                if len(found) >= len(names): raise PolicyConflict('V2 closure unexpected file')
                found.add(entry.name)
        if found != names: raise PolicyConflict('V2 closure incomplete')
        spool._fsync_directory(directory)
        spool._assert_mount()
        _persist_receipt(receipt_parent, result, name='direct-v2-source-closure.json')
        signed = None
        if worker_signer is not None:
            from .direct_worker_receipt_v2 import WorkerClosureSignerV2, verify
            if not isinstance(worker_signer, WorkerClosureSignerV2):
                raise PolicyConflict('pinned V2 worker signer required')
            signed = worker_signer.issue(result)
            verify(signed, expected_closure=result, issuer_id=worker_signer.issuer_id,
                   key_id=worker_signer.key_id, public_key=worker_signer._key.public_key())
            _persist_receipt(receipt_parent, signed, name='direct-v2-worker-signed-closure.json')
        spool._assert_mount()
        require_fenced(ledger, item, expected_worker=expected_worker)
        ledger._retire_direct_writer(scratch_id, pin)
    return signed if signed is not None else result
