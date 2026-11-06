"""Distinct V2 scratch binding; not a transport or custody acknowledgement."""
import hashlib
import json

from . import reference_protocol as p
from .scratch_policy import PolicyConflict


DOMAIN = b'LTDS-DIRECT-WORKER-SCRATCH-BINDING-V2\0'
FIELDS = frozenset(('reservationId', 'jobId', 'attemptId', 'generation', 'environment',
    'authorityId', 'nodeId', 'workerId', 'workerInstanceId', 'sourceManifestSha256',
    'derivedManifestSha256', 'pipelineManifestSha256', 'inventorySha256', 'inputBytes',
    'objectCount', 'overheadBytesPerObject', 'outputBudget', 'ttlSeconds'))
MIN_OBJECT_OVERHEAD = 65536


def binding(value, *, expected_worker):
    p._closed(value, FIELDS, 'V2 direct scratch binding')
    for key in ('reservationId', 'jobId', 'attemptId', 'authorityId'):
        p._uuid(value[key], key)
    for key in ('nodeId', 'workerId', 'workerInstanceId'):
        p._slug(value[key], key)
    for key in ('sourceManifestSha256', 'derivedManifestSha256', 'pipelineManifestSha256', 'inventorySha256'):
        p._hash(value[key], key)
    p._environment(value['environment'])
    for key in ('generation', 'ttlSeconds'):
        p._integer(value[key], key)
    p._integer(value['objectCount'], 'object count')
    p._integer(value['overheadBytesPerObject'], 'object overhead')
    if value['overheadBytesPerObject'] < MIN_OBJECT_OVERHEAD:
        raise ValueError('V2 per-object disk overhead reservation too small')
    for key in ('inputBytes', 'outputBudget'):
        p._integer(value[key], key, allow_zero=True)
    p._integer(value['inputBytes'] + value['objectCount'] * value['overheadBytesPerObject'],
               'V2 reserved input bytes')
    if (type(expected_worker) is not dict or set(expected_worker) !=
            {'authorityId', 'nodeId', 'workerId', 'workerInstanceId'}
            or any(value[key] != expected_worker[key] for key in expected_worker)):
        raise PolicyConflict('V2 scratch worker binding changed')
    return dict(value)


def identity(value, *, expected_worker):
    item = binding(value, expected_worker=expected_worker)
    digest = hashlib.sha256(DOMAIN + p.canonical_json(item)).hexdigest()
    return item, digest, 'direct-' + digest


def settings(item, scratch_id):
    return dict(job_id=item['jobId'], generation=item['generation'], reservation_id=scratch_id,
        manifest_sha256=item['inventorySha256'], input_bytes=(item['inputBytes'] +
            item['objectCount'] * item['overheadBytesPerObject']),
        output_budget=item['outputBudget'], environment=item['environment'], ttl_seconds=item['ttlSeconds'])


def require_fenced(ledger, value, *, expected_worker):
    item, digest, scratch_id = identity(value, expected_worker=expected_worker)
    with ledger._transaction() as db:
        row = ledger._reservation(db, scratch_id)
        if (row['state'] != 'reserved' or row['allocated'] != 0
                or json.loads(row['settings']) != settings(item, scratch_id)
                or not db.execute('SELECT 1 FROM transfer_fences WHERE reservation=?', (scratch_id,)).fetchone()):
            raise PolicyConflict('V2 scratch fence changed')
    return item, digest, scratch_id


def prepare(ledger, value, *, expected_worker, worker_ready):
    """Reserve/fence exact V2 bytes; caller must separately prove node readiness."""
    item, digest, scratch_id = identity(value, expected_worker=expected_worker)
    row = ledger.reserve(job_id=item['jobId'], generation=item['generation'], reservation_id=scratch_id,
        manifest_sha256=item['inventorySha256'], input_bytes=(item['inputBytes'] +
            item['objectCount'] * item['overheadBytesPerObject']),
        output_budget=item['outputBudget'], environment=item['environment'],
        ttl_seconds=item['ttlSeconds'], worker_ready=worker_ready)
    if row['state'] != 'reserved' or row['allocated'] != 0:
        raise PolicyConflict('V2 scratch no longer unstarted')
    ledger.begin_transfer(scratch_id)
    require_fenced(ledger, item, expected_worker=expected_worker)
    return dict(protocol='ltds-direct-scratch-observation-v2', bindingSha256=digest,
        reservationId=item['reservationId'], scratchReservationId=scratch_id,
        state='scratch_fenced', transferAuthorized=False, sourceCustodyAccepted=False)
