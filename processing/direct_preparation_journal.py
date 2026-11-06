"""Coordinator write-ahead preparation intents; not custody or execution.

Pending and uncertain attempts are retained until an independently qualified
reconciler proves the exact worker hold is safe to resolve. No automatic retry.
"""
import json
from . import reference_protocol as p
from .maintenance import Conflict

SCHEMA = '''CREATE TABLE direct_preparation_intents (
 reservation_id TEXT NOT NULL PRIMARY KEY, job_id TEXT NOT NULL UNIQUE,
 node_id TEXT NOT NULL UNIQUE, principal TEXT NOT NULL, request_digest TEXT NOT NULL,
 item_json BLOB NOT NULL, challenge_id TEXT NOT NULL, gate_generation INTEGER NOT NULL,
 state TEXT NOT NULL, proof_json BLOB)'''

def _normalize(value):return ' '.join(value.split())

def ensure_schema(db):
    if not db.in_transaction:raise Conflict('preparation intent requires transaction')
    columns={r['name'] for r in db.execute('PRAGMA table_info(coordinator_identity)')}
    if 'direct_preparation_version' not in columns:
        db.execute('ALTER TABLE coordinator_identity ADD COLUMN direct_preparation_version INTEGER NOT NULL DEFAULT 0')
    version=db.execute('SELECT direct_preparation_version FROM coordinator_identity WHERE singleton=1').fetchone()[0]
    exists=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='direct_preparation_intents'").fetchone()
    if type(version) is not int or version not in (0,1) or bool(exists)!=(version==1):
        raise Conflict('preparation intent schema requires inspection')
    if not exists:
        db.execute(SCHEMA)
        db.execute('UPDATE coordinator_identity SET direct_preparation_version=1 WHERE singleton=1')
    verify_schema(db)

def verify_schema(db):
    row=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='direct_preparation_intents'").fetchone()
    if row is None or _normalize(row['sql'])!=_normalize(SCHEMA):raise Conflict('preparation intent schema changed')

def metadata_bytes(db):
    columns={r['name'] for r in db.execute('PRAGMA table_info(coordinator_identity)')}
    version=0 if 'direct_preparation_version' not in columns else db.execute(
        'SELECT direct_preparation_version FROM coordinator_identity WHERE singleton=1').fetchone()[0]
    exists=db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='direct_preparation_intents'").fetchone()
    if version==0 and not exists:return 0
    if type(version) is not int or version!=1 or not exists:raise Conflict('preparation intent marker changed')
    verify_schema(db)
    if db.execute('''SELECT 1 FROM direct_preparation_intents WHERE state NOT IN ('pending','proof_observed')
        OR typeof(item_json)!='blob' OR length(item_json)>8192
        OR (state='proof_observed')!=(proof_json IS NOT NULL)
        OR (proof_json IS NOT NULL AND (typeof(proof_json)!='blob' OR length(proof_json)>8192)) LIMIT 1''').fetchone():
        raise Conflict('preparation intent census invalid')
    if db.execute('''SELECT 1 FROM direct_preparation_intents d LEFT JOIN jobs j ON j.id=d.job_id
        LEFT JOIN reference_jobs r ON r.job_id=d.job_id LEFT JOIN reference_source_intakes s ON s.job_id=d.job_id
        LEFT JOIN reference_source_intake_modes m ON m.job_id=d.job_id WHERE j.id IS NULL OR r.job_id IS NULL
        OR s.job_id IS NULL OR m.mode IS NULL OR m.mode!='direct-v2' OR j.status!='held'
        OR j.held_reason IS NULL OR j.held_reason NOT IN ('direct_preparing','direct_v2_receiving') OR j.node_id IS NOT d.node_id
        OR r.node_id IS NOT d.node_id OR s.request_digest IS NOT d.request_digest LIMIT 1''').fetchone():
        raise Conflict('preparation intent binding changed')
    return db.execute('SELECT COUNT(*)*32768 FROM direct_preparation_intents').fetchone()[0]

def prepare_once(reservations, client, *, payload, item, principal, challenge_id, gate_generation,
                 capability_fingerprint,options_fingerprint,allowed_odm_options):
    """Fence selected queue node, journal exact intent, then call enrolled mTLS worker."""
    from .direct_v2_inventory import prepare
    from .direct_worker_scratch_v2 import identity
    from .source_intake_mode import require
    from .direct_preparation_client import DirectPreparationClient
    from cryptography.hazmat.primitives import serialization
    if type(client) is not DirectPreparationClient:raise ValueError('protected preparation client required')
    p._slug(principal,'principal');p._uuid(challenge_id,'challenge');p._integer(gate_generation,'gate generation')
    worker=client._worker;item,_,_=identity(item,expected_worker=worker)
    pin=reservations._workers.get(item['nodeId'])
    raw=lambda key:key.public_bytes(serialization.Encoding.Raw,serialization.PublicFormat.Raw)
    if (pin is None or pin[0]!=worker or (pin[1],pin[2])!=(client._issuer,client._key)
            or raw(pin[3])!=raw(client._public)):
        raise Conflict('worker pin changed')
    checked=prepare(payload,authenticated_principal=principal,environment=item['environment'],
        capability_fingerprint=capability_fingerprint,options_fingerprint=options_fingerprint,
        allowed_odm_options=allowed_odm_options,trusted_source_keys=reservations.store._source_routing_keys,
        authority_id=reservations.queue.authority_id)
    if (item['jobId']!=checked['providerTaskId'] or item['attemptId']!=checked['submissionId']
            or any(item[k]!=checked[k] for k in ('sourceManifestSha256','derivedManifestSha256','pipelineManifestSha256','inventorySha256'))
            or item['objectCount']!=sum(len(checked[k]['objects']) for k in ('sourceManifest','derivedManifest'))
            or item['inputBytes']!=sum(o['byteSize'] for k in ('sourceManifest','derivedManifest') for o in checked[k]['objects'])):
        raise Conflict('preparation inventory changed')
    item_wire=p.canonical_json(item);digest=p.digest(payload)
    if len(item_wire)>8192:raise ValueError('preparation item too large')
    try:
        with reservations.queue.coordinator_operation('source-io-admission'),reservations.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');ensure_schema(db);require(db,item['jobId'],'direct-v2')
            row=reservations.store._row(db,item['jobId'])
            if (row['source_request_digest']!=digest or row['generation']!=item['generation']
                    or row['environment']!=item['environment'] or row['status']!='held'
                    or row['held_reason']!='direct_admission_pending' or row['node_id'] is not None):
                raise Conflict('direct pending job changed')
            old=db.execute('SELECT 1 FROM direct_preparation_intents WHERE reservation_id=? OR job_id=? OR node_id=?',
                           (item['reservationId'],item['jobId'],item['nodeId'])).fetchone()
            if old:raise Conflict('preparation already attempted; reconciliation required')
            candidate=reservations.store._candidate_any_row(db,item['nodeId'])
            if candidate is None or candidate['id']!=item['jobId']:raise Conflict('node not eligible')
            reservations.store._quota(db,32768)
            db.execute('INSERT INTO direct_preparation_intents VALUES(?,?,?,?,?,?,?,?,?,NULL)',
                       (item['reservationId'],item['jobId'],item['nodeId'],principal,digest,item_wire,challenge_id,gate_generation,'pending'))
            if db.execute("UPDATE jobs SET node_id=?,held_reason='direct_preparing' WHERE id=? AND status='held' AND held_reason='direct_admission_pending'",
                          (item['nodeId'],item['jobId'])).rowcount!=1:raise Conflict('direct job changed')
            db.execute('UPDATE reference_jobs SET node_id=? WHERE job_id=?',(item['nodeId'],item['jobId']))
    except Exception:
        raise Conflict('preparation intent requires reconciliation') from None
    # Commit may have succeeded on an exception; no caller may infer no effect.
    try:
        proof=client.prepare(source=payload,item=item,challenge_id=challenge_id,gate_generation=gate_generation)
        proof_wire=p.canonical_json(proof)
        if len(proof_wire)>8192:raise ValueError('proof too large')
        with reservations.queue.coordinator_operation('source-io-admission'),reservations.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');verify_schema(db)
            changed=db.execute("UPDATE direct_preparation_intents SET state='proof_observed',proof_json=? WHERE reservation_id=? AND job_id=? AND node_id=? AND state='pending' AND item_json=? AND challenge_id=?",
                (proof_wire,item['reservationId'],item['jobId'],item['nodeId'],item_wire,challenge_id)).rowcount
            if changed!=1:raise Conflict('preparation intent changed')
        return {'reservationId':item['reservationId'],'state':'proof_observed',
                'sourceCustodyAccepted':False,'processingAuthorized':False}
    except Exception:
        raise Conflict('worker preparation uncertain; reconciliation required') from None
