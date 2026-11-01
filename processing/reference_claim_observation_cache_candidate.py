"""Disabled new-state-only claim observation cache; never grants execution.

SQLite triggers invalidate verified submission metadata whenever its source row
changes. This cooperative candidate does not fence database restore, old binaries,
local administrators or a detached coordinator.
"""
import hashlib
import hmac
import json
from . import reference_protocol as p
from .maintenance import Conflict

QUALIFIED=False
DDL_TABLE='CREATE TABLE reference_claim_observation_cache (job_id TEXT PRIMARY KEY, epoch TEXT NOT NULL, proof TEXT NOT NULL)'
DDL_UPDATE="CREATE TRIGGER reference_claim_cache_update AFTER UPDATE OF input_json,manifest_sha,pipeline_digest,environment,generation ON reference_jobs BEGIN DELETE FROM reference_claim_observation_cache WHERE job_id=OLD.job_id; END"
DDL_DELETE="CREATE TRIGGER reference_claim_cache_delete AFTER DELETE ON reference_jobs BEGIN DELETE FROM reference_claim_observation_cache WHERE job_id=OLD.job_id; END"
DDL_INSERT="CREATE TRIGGER reference_claim_cache_insert AFTER INSERT ON reference_jobs BEGIN DELETE FROM reference_claim_observation_cache WHERE job_id=NEW.job_id; END"
SCHEMA=(('table','reference_claim_observation_cache',DDL_TABLE),('trigger','reference_claim_cache_update',DDL_UPDATE),('trigger','reference_claim_cache_delete',DDL_DELETE),('trigger','reference_claim_cache_insert',DDL_INSERT))


def install(db, *, enabled=False):
    if QUALIFIED is not True or enabled is not True:raise Conflict('claim observation cache not qualified')
    if db.execute('SELECT 1 FROM reference_jobs LIMIT 1').fetchone():raise Conflict('claim observation cache requires new empty state')
    for _,_,ddl in SCHEMA:db.execute(ddl)


def attest(db):
    for kind,name,ddl in SCHEMA:
        row=db.execute('SELECT sql FROM sqlite_master WHERE type=? AND name=?',(kind,name)).fetchone()
        if row is None or row[0]!=ddl:raise Conflict('claim observation cache schema changed')


def snapshot(store, db, job_id, *, node_id, worker_id, worker_instance_id, claim_token):
    """Read live claim/status; use cached metadata only under same DB transaction."""
    from .reference_store import PHASES, PROFILE, _reason
    if QUALIFIED is not True:raise Conflict('claim observation cache not qualified')
    attest(db)
    cached=db.execute('SELECT epoch,proof FROM reference_claim_observation_cache WHERE job_id=?',(job_id,)).fetchone()
    if cached is None or cached['epoch']!=store.queue.startup_id:
        proof=store._claim_snapshot_uncached(db,job_id,node_id=node_id,worker_id=worker_id,worker_instance_id=worker_instance_id,claim_token=claim_token)
        encoded=p.canonical_json(proof).decode()
        db.execute('INSERT OR REPLACE INTO reference_claim_observation_cache VALUES(?,?,?)',(job_id,store.queue.startup_id,encoded))
        return proof
    try:
        proof=json.loads(cached['proof'])
        if p.canonical_json(proof).decode()!=cached['proof']:raise ValueError()
        store.queue._known_node(node_id);p._uuid(job_id,'jobId');p._slug(worker_id,'worker ID');p._slug(worker_instance_id,'worker instance ID')
        if type(claim_token) is not str or not claim_token.isascii() or not 1<=len(claim_token)<=128:raise ValueError()
        token_sha=hashlib.sha256(claim_token.encode()).hexdigest()
        row=db.execute('''SELECT r.generation,r.environment,r.node_id,r.manifest_sha,r.pipeline_digest,r.phase,
            j.status AS queue_status,j.environment AS queue_environment,j.node_id AS queue_node,
            j.worker_id AS queue_worker,j.claim_hash AS queue_claim_hash,j.held_reason,
            c.request_id AS claim_request,c.node_id AS claim_node,c.worker_id AS claim_worker,
            c.instance_id AS claim_instance,c.token AS claim_token
            FROM reference_jobs r JOIN jobs j ON j.id=r.job_id
            JOIN reference_claims c ON c.job_id=r.job_id WHERE r.job_id=? AND j.profile=?''',(job_id,PROFILE)).fetchone()
        authority=db.execute('SELECT authority_id FROM coordinator_identity WHERE singleton=1').fetchone()
        if (row is None or authority is None or authority[0]!=store.queue.authority_id
                or (row['node_id'],row['queue_node'],row['claim_node'])!=(node_id,)*3
                or (row['queue_worker'],row['claim_worker'])!=(worker_id,)*2 or row['claim_instance']!=worker_instance_id
                or type(row['claim_token']) is not str or not row['claim_token'].isascii()
                or not hmac.compare_digest(row['claim_token'],claim_token)
                or type(row['queue_claim_hash']) is not str or not row['queue_claim_hash'].isascii()
                or not hmac.compare_digest(row['queue_claim_hash'],token_sha)
                or row['environment']!=row['queue_environment']):raise ValueError()
        expected=dict(authorityId=store.queue.authority_id,jobId=job_id,generation=row['generation'],environment=row['environment'],
            nodeId=node_id,workerId=worker_id,workerInstanceId=worker_instance_id,claimRequestId=row['claim_request'],
            claimTokenSha256=token_sha,manifestSha256=row['manifest_sha'],pipelineDigest=row['pipeline_digest'])
        if any(proof.get(k)!=v for k,v in expected.items()):raise ValueError()
        if row['phase'] not in PHASES or row['queue_status'] not in ('running','held','succeeded','failed','cancelled'):raise ValueError()
        _reason(row['held_reason'])
        if row['queue_status']!='held' and row['held_reason'] is not None:raise ValueError()
        return dict(proof,status=row['queue_status'],executionPhase=row['phase'],heldReason=row['held_reason'])
    except (KeyError,TypeError,ValueError):raise Conflict('persisted claim binding unavailable') from None
