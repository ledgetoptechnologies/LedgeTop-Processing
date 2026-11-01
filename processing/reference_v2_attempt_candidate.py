"""Disabled local attempt identity experiment; no signed reservation intent."""
import json
import uuid
from . import reference_protocol as p
from .reference_store import ReferenceStore

QUALIFIED=False
DDL=('CREATE TABLE v2_attempts (request_id TEXT PRIMARY KEY, job_id TEXT NOT NULL UNIQUE, '
     'attempt_id TEXT NOT NULL UNIQUE, claim_sha TEXT NOT NULL, identity TEXT NOT NULL)')
TRIGGERS={
 'v2_attempt_update':"CREATE TRIGGER v2_attempt_update BEFORE UPDATE ON v2_attempts BEGIN SELECT RAISE(ABORT,'v2 attempt immutable'); END",
 'v2_attempt_delete':"CREATE TRIGGER v2_attempt_delete BEFORE DELETE ON v2_attempts BEGIN SELECT RAISE(ABORT,'v2 attempt immutable'); END",
 'v2_attempt_replace':"CREATE TRIGGER v2_attempt_replace BEFORE INSERT ON v2_attempts WHEN EXISTS (SELECT 1 FROM v2_attempts WHERE request_id=NEW.request_id OR job_id=NEW.job_id OR attempt_id=NEW.attempt_id) OR (SELECT COUNT(*) FROM pragma_index_list('v2_attempts') WHERE \"unique\"=1)!=3 OR EXISTS (SELECT 1 FROM pragma_index_list('v2_attempts') WHERE \"unique\"=1 AND name NOT IN ('sqlite_autoindex_v2_attempts_1','sqlite_autoindex_v2_attempts_2','sqlite_autoindex_v2_attempts_3')) BEGIN SELECT RAISE(ABORT,'v2 attempt immutable'); END"}
class AttemptHeld(ValueError):pass

def attest(db):
    schema=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='v2_attempts'").fetchone()
    if schema is None or schema[0]!=DDL:raise AttemptHeld('attempt schema changed')
    for name,sql in TRIGGERS.items():
        actual=db.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?",(name,)).fetchone()
        if actual is None or actual[0]!=sql:raise AttemptHeld('attempt guard changed')
    seen=set()
    for row in db.execute('PRAGMA index_list(v2_attempts)'):
        if row[2]:
            name=row[1]
            if row[4] or not isinstance(name,str) or not name.replace('_','').isalnum():raise AttemptHeld('attempt index changed')
            columns=tuple(item[2] for item in db.execute('PRAGMA index_info("'+name+'")'))
            if None in columns or columns in seen:raise AttemptHeld('attempt index changed')
            seen.add(columns)
    if seen!={('request_id',),('job_id',),('attempt_id',)}:raise AttemptHeld('attempt index changed')

def install_offline(store,*,enabled=False):
    if QUALIFIED is not True or enabled is not True or type(store) is not ReferenceStore:raise AttemptHeld('unqualified')
    with store.queue._db() as db:
        db.execute('BEGIN IMMEDIATE')
        if db.execute("SELECT 1 FROM jobs WHERE status IN ('running','held') LIMIT 1").fetchone():raise AttemptHeld('offline queue required')
        db.execute(DDL)
        for sql in TRIGGERS.values():db.execute(sql)

def create(store,*,request_id,claim):
    """Stable local attempt for an existing claim; never an authority grant."""
    if QUALIFIED is not True or type(store) is not ReferenceStore or type(claim) is not dict or set(claim)!={'jobId','nodeId','workerId','workerInstanceId','claimToken'}:raise AttemptHeld('unqualified')
    p._slug(request_id,'attempt request')
    with store.queue._db() as db:
        db.execute('BEGIN IMMEDIATE')
        attest(db)
        snapshot=store._claim_snapshot_uncached(db,claim['jobId'],node_id=claim['nodeId'],worker_id=claim['workerId'],worker_instance_id=claim['workerInstanceId'],claim_token=claim['claimToken'])
        if snapshot['status']!='running':raise AttemptHeld('current running claim required')
        identity=dict(schemaVersion=1,scope='local_attempt_not_authority',**{k:snapshot[k] for k in ('authorityId','jobId','generation','environment','nodeId','workerId','workerInstanceId','claimRequestId','manifestSha256','pipelineDigest')})
        encoded=p.canonical_json(identity).decode();row=db.execute('SELECT request_id,attempt_id,claim_sha,identity FROM v2_attempts WHERE job_id=?',(claim['jobId'],)).fetchone()
        if row is None:
            if snapshot['executionPhase'] not in ('queued','waiting_capacity'):raise AttemptHeld('unreserved claim required')
            attempt=str(uuid.uuid4())
            db.execute('INSERT INTO v2_attempts VALUES(?,?,?,?,?)',(request_id,claim['jobId'],attempt,snapshot['claimTokenSha256'],encoded))
        else:
            if (row['request_id'],row['claim_sha'],row['identity'])!=(request_id,snapshot['claimTokenSha256'],encoded):raise AttemptHeld('attempt binding changed')
            attempt=row['attempt_id'];p._uuid(attempt,'stored attempt')
        return dict(attemptId=attempt,identity=json.loads(encoded),authorization='unqualified')
