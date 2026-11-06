"""Unwired direct-V2 reservation schema and logical integrity census.

This validates durable rows; it does not authenticate keys, mint custody,
recover held reservations, or fence older binaries. Call under the existing
coordinator transaction and freeze ownership.
"""
import json
from . import reference_protocol as p
from .maintenance import Conflict

TABLE='direct_v2_reservations'
FENCE_TABLE='legacy_fence_requirement'
FENCE_SCHEMA="CREATE TABLE legacy_fence_requirement (version INTEGER NOT NULL CHECK(version=2))"
SCHEMA='''CREATE TABLE direct_v2_reservations (
                reservation_id TEXT PRIMARY KEY,job_id TEXT UNIQUE NOT NULL,node_id TEXT UNIQUE NOT NULL,
                principal TEXT NOT NULL,request_digest TEXT NOT NULL,item_json BLOB NOT NULL,
                proof_json BLOB NOT NULL,challenge_id TEXT NOT NULL,state TEXT NOT NULL)'''

def _normalized(sql):return ' '.join(sql.split()).replace(' IF NOT EXISTS ',' ')

def marker_version(db):
    columns={row['name'] for row in db.execute('PRAGMA table_info(coordinator_identity)')}
    if 'direct_v2_reservation_version' not in columns:return 0
    row=db.execute('SELECT direct_v2_reservation_version FROM coordinator_identity WHERE singleton=1').fetchone()
    if row is None or type(row[0]) is not int or row[0] not in (0,1):
        raise Conflict('direct V2 reservation marker requires inspection')
    return row[0]

def verify_schema(db):
    row=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?",(TABLE,)).fetchone()
    if row is None or type(row['sql']) is not str or _normalized(row['sql'])!=_normalized(SCHEMA):
        raise Conflict('direct V2 reservation schema requires inspection')

def ensure_schema(db):
    if not db.in_transaction:raise Conflict('direct V2 schema transaction required')
    columns={row['name'] for row in db.execute('PRAGMA table_info(coordinator_identity)')}
    if 'direct_v2_reservation_version' not in columns:
        db.execute('ALTER TABLE coordinator_identity ADD COLUMN direct_v2_reservation_version INTEGER NOT NULL DEFAULT 0')
    version=marker_version(db)
    table_exists=db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(TABLE,)).fetchone() is not None
    fence_exists=db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(FENCE_TABLE,)).fetchone() is not None
    if table_exists:
        verify_schema(db)
    if table_exists != fence_exists or (version==1)!=table_exists:
        raise Conflict('direct V2 reservation fence mismatch')
    if not table_exists:
        db.execute(FENCE_SCHEMA)
        db.execute('INSERT INTO legacy_fence_requirement(version) VALUES(2)')
        db.execute(SCHEMA)
        db.execute('UPDATE coordinator_identity SET direct_v2_reservation_version=1 WHERE singleton=1 AND direct_v2_reservation_version=0')
    verify_schema(db)
    verify_fence(db)

def verify_fence(db):
    row=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?",(FENCE_TABLE,)).fetchone()
    if row is None or type(row['sql']) is not str or _normalized(row['sql'])!=_normalized(FENCE_SCHEMA):
        raise Conflict('direct V2 reservation fence requires inspection')
    values=db.execute('SELECT version FROM legacy_fence_requirement LIMIT 2').fetchall()
    if len(values)!=1 or type(values[0][0]) is not int or values[0][0]!=2:
        raise Conflict('direct V2 reservation fence requires inspection')

def metadata_bytes(db):
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(TABLE,)).fetchone() is None:
        if marker_version(db)==1 or db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(FENCE_TABLE,)).fetchone():
            raise Conflict('direct V2 reservation fence mismatch')
        return 0
    verify_schema(db)
    verify_fence(db)
    if marker_version(db)!=1:raise Conflict('direct V2 reservation fence mismatch')
    if db.execute('''SELECT 1 FROM direct_v2_reservations WHERE
        typeof(reservation_id)!='text' OR typeof(job_id)!='text' OR typeof(node_id)!='text'
        OR typeof(principal)!='text' OR typeof(request_digest)!='text' OR typeof(challenge_id)!='text'
        OR typeof(state)!='text' OR state!='receiving' OR typeof(item_json)!='blob'
        OR typeof(proof_json)!='blob' OR length(item_json)>8192 OR length(proof_json)>8192 LIMIT 1''').fetchone():
        raise Conflict('direct V2 reservation census requires inspection')
    return db.execute('''SELECT COALESCE(SUM(length(CAST(reservation_id AS BLOB))+length(CAST(job_id AS BLOB))+
        length(CAST(node_id AS BLOB))+length(CAST(principal AS BLOB))+length(CAST(request_digest AS BLOB))+
        length(item_json)+length(proof_json)+length(CAST(challenge_id AS BLOB))+length(CAST(state AS BLOB))),0)
        FROM direct_v2_reservations''').fetchone()[0]

def validate_receiving(db,reservation_id):
    """Validate durable identity and queue state before replay, not proof auth."""
    from .direct_worker_scratch_v2 import identity
    from .direct_worker_fence_proof_v2 import _body, PROTOCOL
    from .source_intake_mode import require
    verify_schema(db)
    row=db.execute('''SELECT d.*,j.status,j.held_reason,j.node_id AS queue_node,
        r.node_id AS reference_node,r.generation AS current_generation,r.environment AS current_environment,
        r.manifest_sha AS reference_manifest,s.request_digest AS source_digest,s.source_state,s.derived_state,
        s.source_manifest_sha,s.derived_manifest_sha,s.pipeline_manifest_sha,s.source_attestation_json,
        (SELECT authority_id FROM coordinator_identity WHERE singleton=1) AS current_authority
        FROM direct_v2_reservations d
        LEFT JOIN jobs j ON j.id=d.job_id LEFT JOIN reference_jobs r ON r.job_id=d.job_id
        LEFT JOIN reference_source_intakes s ON s.job_id=d.job_id WHERE d.reservation_id=?''',
        (reservation_id,)).fetchone()
    if row is None:raise Conflict('direct V2 reservation missing')
    try:
        if (type(row['item_json']) is not bytes or type(row['proof_json']) is not bytes
                or len(row['item_json'])>8192 or len(row['proof_json'])>8192):raise ValueError()
        item=json.loads(row['item_json']);proof=json.loads(row['proof_json'])
        if p.canonical_json(item)!=row['item_json'] or p.canonical_json(proof)!=row['proof_json']:raise ValueError()
        worker={key:item[key] for key in ('authorityId','nodeId','workerId','workerInstanceId')}
        item,digest,_=identity(item,expected_worker=worker)
        if type(proof) is not dict or set(proof)!={'protocol','issuerId','keyId','body','signature'}:raise ValueError()
        if proof['protocol']!=PROTOCOL or type(proof['signature']) is not str or len(proof['signature'])!=86:raise ValueError()
        p._slug(proof['issuerId'],'issuer');p._slug(proof['keyId'],'key')
        body=_body(proof['body'])
        attestation=json.loads(row['source_attestation_json'])
        p._slug(row['principal'],'principal');p._hash(row['request_digest'],'request digest')
        p._uuid(row['challenge_id'],'challenge')
        if (type(attestation) is not dict or type(attestation.get('proof')) is not dict
                or attestation['proof'].get('principalId')!=row['principal']
                or row['source_state']!='receiving' or row['derived_state']!='receiving'
                or row['state']!='receiving' or row['status']!='held' or row['held_reason']!='direct_v2_receiving'
                or row['queue_node']!=row['node_id'] or row['reference_node']!=row['node_id']
                or row['source_digest']!=row['request_digest'] or row['current_generation']!=item['generation']
                or row['current_environment']!=item['environment'] or row['reservation_id']!=item['reservationId']
                or row['job_id']!=item['jobId'] or row['node_id']!=item['nodeId']
                or row['current_authority']!=item['authorityId']
                or row['source_manifest_sha']!=item['sourceManifestSha256']
                or row['derived_manifest_sha']!=item['derivedManifestSha256']
                or row['pipeline_manifest_sha']!=item['pipelineManifestSha256']
                or row['reference_manifest']!=item['pipelineManifestSha256']
                or attestation['proof'].get('sourceManifestSha256')!=item['sourceManifestSha256']
                or attestation['proof'].get('derivedManifestSha256')!=item['derivedManifestSha256']
                or attestation['proof'].get('pipelineManifestSha256')!=item['pipelineManifestSha256']
                or body['scratchBindingSha256']!=digest or body['challengeId']!=row['challenge_id']):raise ValueError()
        require(db,row['job_id'],'direct-v2')
        return dict(row)
    except (ValueError,TypeError,KeyError,AttributeError):
        raise Conflict('direct V2 reservation requires inspection') from None
