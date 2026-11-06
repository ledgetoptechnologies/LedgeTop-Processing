"""Unwired local object receiver; no authenticated HTTP or source-complete ACK.

Requires a pre-existing durable scratch fence, an exact immutable input
manifest and a separately provisioned physical NodeGate. The caller must prove
the upstream binding and provision the private mounted SourceSpool; neither a
coordinator nor Viewer is authorized by constructing this object.
"""
from pathlib import Path

from . import reference_protocol as p
from .direct_worker_scratch import require_fenced, scratch_settings
from .node_gate import NodeGate, _binding as gate_binding
from .scratch_policy import PolicyConflict


def _object(manifest, item, object_id):
    p._closed(manifest, ('id','revision','sha256','totalBytes','objectCount','inputCounts','objects'), 'manifest')
    p._slug(manifest['id'], 'manifest id'); p._integer(manifest['revision'], 'revision')
    p._integer(manifest['totalBytes'], 'totalBytes'); p._integer(manifest['objectCount'], 'objectCount')
    if (manifest['sha256'] != item['manifestSha256'] or p.manifest_sha256(manifest) != manifest['sha256']
            or type(manifest['objects']) is not list or not 0 < len(manifest['objects']) <= p.MAX_OBJECTS
            or len(manifest['objects']) != manifest['objectCount'] or manifest['totalBytes'] != item['inputBytes']):
        raise PolicyConflict('direct manifest binding changed')
    p._closed(manifest['inputCounts'], ('images','lidarObjects','points'), 'inputCounts')
    for key in manifest['inputCounts']: p._integer(manifest['inputCounts'][key], key, allow_zero=True)
    objects = {}; names = set()
    for obj in manifest['objects']:
        p._closed(obj, ('id','name','byteSize','sha256','kind','mediaType'), 'object')
        p._slug(obj['id'], 'object id'); p._basename(obj['name']); p._hash(obj['sha256'], 'object hash')
        p._integer(obj['byteSize'], 'object size'); p._string(obj['mediaType'], 'media type', 127)
        if obj['id'] in objects or obj['name'].casefold() in names or obj['kind'] not in ('image','lidar','auxiliary'):
            raise PolicyConflict('direct manifest object invalid')
        objects[obj['id']] = obj; names.add(obj['name'].casefold())
    counts = manifest['inputCounts']
    if (sum(obj['byteSize'] for obj in objects.values()) != manifest['totalBytes']
            or counts['images'] != sum(obj['kind'] == 'image' for obj in objects.values())
            or counts['lidarObjects'] != sum(obj['kind'] == 'lidar' for obj in objects.values())
            or object_id not in objects):
        raise PolicyConflict('direct manifest inventory changed')
    return objects[object_id]


def receive(ledger, spool, manifest, item, *, expected_worker, engine_id,
            coordinator_origin, gate_generation, object_id, reader):
    """Durably publish one object; no complete-source or Viewer ACK is issued."""
    item, sha, scratch_id = require_fenced(ledger, item, expected_worker=expected_worker)
    obj = _object(manifest, item, object_id)
    expected_root = Path(ledger.root) / 'jobs' / scratch_id / 'direct-source'
    if (spool.root != expected_root or not spool.require_mount
            or spool.root.stat().st_dev != Path(ledger.root).stat().st_dev):
        raise PolicyConflict('direct scratch mount binding changed')
    # The gate receipt derives from the full immutable scratch binding. A gate
    # with an unbound/different job, generation or coordinator cannot write.
    expected_receipt = sha[:32]
    expected_job = dict(id=item['jobId'], workerId=item['workerId'],
                        nodeId=item['nodeId'], authorityId=item['authorityId'])
    expected_gate = gate_binding(item['nodeId'], coordinator_origin, item['authorityId'])
    if type(gate_generation) is not int or gate_generation < 1:
        raise ValueError('positive gate generation required')
    if not callable(getattr(reader, 'close', None)) or getattr(reader, 'closed', None) is not False:
        raise ValueError('owned open source reader required')
    with NodeGate(engine_id) as gate:
        state = gate.snapshot(); reservation = state['reservation']
        if (state['generation'] != gate_generation
                or any(state[key] != value for key, value in expected_gate.items())
                or reservation is None or reservation['receiptId'] != expected_receipt
                or reservation['job'] != expected_job or reservation['completionDigest'] is not None):
            raise PolicyConflict('direct node gate binding changed')
        pin = ledger.begin_direct_writer(scratch_id, scratch_settings(item, scratch_id))
        # On any exception, including a close failure or process death, retain
        # the pin. Inspection/recovery must prove the reader has retired.
        receipt, replay = spool.receive_object(job_id=item['jobId'], generation=item['generation'],
            object_id=obj['id'], byte_size=obj['byteSize'], sha256=obj['sha256'], reader=reader)
        reader.close()
        if getattr(reader, 'closed', None) is not True:
            raise PolicyConflict('direct source reader retirement uncertain')
        require_fenced(ledger, item, expected_worker=expected_worker)
        ledger._retire_direct_writer(scratch_id, pin)
    return dict(protocol='ltds-direct-local-object-v1', reservationId=item['reservationId'],
        scratchBindingSha256=sha, jobId=item['jobId'], attemptId=item['attemptId'],
        generation=item['generation'], nodeId=item['nodeId'], objectId=obj['id'],
        byteSize=obj['byteSize'], sha256=obj['sha256'], localObjectDurable=True,
        sourceCustodyAccepted=False, transferComplete=False, replayed=replay)
