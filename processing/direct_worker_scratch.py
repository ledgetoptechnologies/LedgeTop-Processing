"""Unwired scratch-capacity prototype for future direct source delivery.

This component receives *already authenticated and pinned* worker bindings from
its caller. It neither authenticates a network request nor reserves NodeGate,
accepts files, authorizes transfer/processing, or issues a custody receipt.
It deliberately cannot release a fenced reservation. Do not expose it via an
API until those boundaries and disk/accounting integration are qualified.
"""
import hashlib
import json

from . import reference_protocol as p
from .scratch_policy import PolicyConflict


FIELDS = frozenset(('reservationId', 'jobId', 'attemptId', 'generation', 'environment',
                    'authorityId', 'nodeId', 'workerId', 'workerInstanceId', 'manifestSha256', 'derivedManifestSha256',
                    'inputBytes', 'outputBudget', 'ttlSeconds'))
DOMAIN = b'LTDS-DIRECT-WORKER-SCRATCH-BINDING-V1\0'


def binding(value, *, expected_worker):
    if type(value) is not dict or set(value) != FIELDS:
        raise ValueError('closed direct scratch binding required')
    for name in ('reservationId', 'jobId', 'attemptId', 'authorityId'):
        p._uuid(value[name], name)
    for name in ('nodeId', 'workerId', 'workerInstanceId'):
        p._slug(value[name], name)
    p._hash(value['manifestSha256'], 'manifest digest')
    p._hash(value['derivedManifestSha256'], 'derived manifest digest')
    p._environment(value['environment'])
    for name in ('generation', 'ttlSeconds'):
        p._integer(value[name], name)
    for name in ('inputBytes', 'outputBudget'):
        p._integer(value[name], name, allow_zero=True)
    if (type(expected_worker) is not dict or set(expected_worker) !=
            {'authorityId', 'nodeId', 'workerId', 'workerInstanceId'}
            or any(value[key] != expected_worker[key] for key in expected_worker)):
        raise PolicyConflict('direct scratch worker binding changed')
    # Fixed schema, ASCII IDs and exact canonical bytes make retry identity
    # deterministic without persisting credentials or source file names.
    return dict(value)


def scratch_identity(value, *, expected_worker):
    item = binding(value, expected_worker=expected_worker)
    sha = hashlib.sha256(DOMAIN + p.canonical_json(item)).hexdigest()
    return item, sha, 'direct-' + sha


def scratch_settings(item, scratch_id):
    return dict(job_id=item['jobId'], generation=item['generation'], reservation_id=scratch_id,
        manifest_sha256=item['manifestSha256'], input_bytes=item['inputBytes'],
        output_budget=item['outputBudget'], environment=item['environment'], ttl_seconds=item['ttlSeconds'])


def require_fenced(ledger, value, *, expected_worker):
    """Check the exact persisted scratch binding; never create or renew it."""
    item, sha, scratch_id = scratch_identity(value, expected_worker=expected_worker)
    expected = scratch_settings(item, scratch_id)
    with ledger._transaction() as db:
        row = ledger._reservation(db, scratch_id)
        if (row['state'] != 'reserved' or row['allocated'] != 0
                or json.loads(row['settings']) != expected
                or not db.execute('SELECT 1 FROM transfer_fences WHERE reservation=?', (scratch_id,)).fetchone()):
            raise PolicyConflict('direct scratch fence changed')
    return item, sha, scratch_id


def prepare(ledger, value, *, expected_worker, worker_ready):
    """Fence capacity; a result is NOT a source-custody acknowledgement.

    Resume between reserve and fence only for the exact input; after fencing,
    ScratchLedger holds the full remaining budget regardless of time. A crash
    after the fence but before returning can safely repeat this exact call.
    """
    item, sha, scratch_id = scratch_identity(value, expected_worker=expected_worker)
    row = ledger.reserve(job_id=item['jobId'], generation=item['generation'],
        reservation_id=scratch_id, manifest_sha256=item['manifestSha256'],
        input_bytes=item['inputBytes'], output_budget=item['outputBudget'],
        environment=item['environment'], ttl_seconds=item['ttlSeconds'], worker_ready=worker_ready)
    if row['state'] != 'reserved' or row['allocated'] != 0:
        raise PolicyConflict('direct scratch no longer unstarted')
    ledger.begin_transfer(scratch_id)
    require_fenced(ledger, item, expected_worker=expected_worker)
    return dict(protocol='ltds-direct-scratch-observation-v1', bindingSha256=sha,
        reservationId=item['reservationId'], scratchReservationId=scratch_id,
        state='scratch_fenced', transferAuthorized=False, sourceCustodyAccepted=False)
