"""Disabled coordinator-owned intent pin; not Viewer event authentication."""
import hashlib
import json
import re
from datetime import datetime
from . import reference_protocol as p
from .reference_store import ReferenceStore
from . import reference_v2_envelope_candidate as envelope
from . import reference_v2_attempt_candidate as attempts

QUALIFIED=False
DDL=('CREATE TABLE v2_event_pins (operation TEXT PRIMARY KEY, job_id TEXT NOT NULL UNIQUE, '
     'claim_sha TEXT NOT NULL, claim_binding TEXT NOT NULL, event_bytes BLOB NOT NULL, '
     'event_sha TEXT NOT NULL, local_identity TEXT NOT NULL, local_identity_sha TEXT NOT NULL, state TEXT NOT NULL CHECK(state=\'held\'))')
LOCAL_IDENTITY_DOMAIN=b'ltds-v2-local-claim-observation-experiment-v1\0'
TRIGGERS={
 'v2_pin_record_update':"CREATE TRIGGER v2_pin_record_update BEFORE UPDATE ON v2_event_pins BEGIN SELECT RAISE(ABORT,'v2 event pin immutable'); END",
 'v2_pin_record_delete':"CREATE TRIGGER v2_pin_record_delete BEFORE DELETE ON v2_event_pins BEGIN SELECT RAISE(ABORT,'v2 event pin immutable'); END",
 'v2_pin_record_replace':"CREATE TRIGGER v2_pin_record_replace BEFORE INSERT ON v2_event_pins WHEN EXISTS (SELECT 1 FROM v2_event_pins WHERE operation=NEW.operation OR job_id=NEW.job_id) BEGIN SELECT RAISE(ABORT,'v2 event pin immutable'); END",
 'v2_pin_job_insert':"CREATE TRIGGER v2_pin_job_insert BEFORE INSERT ON jobs WHEN EXISTS (SELECT 1 FROM v2_event_pins p JOIN jobs j ON j.id=p.job_id WHERE p.job_id=NEW.id OR j.seq=NEW.seq OR j.request_id=NEW.request_id) BEGIN SELECT RAISE(ABORT,'v2 event pin held'); END",
 'v2_pin_job_update':"CREATE TRIGGER v2_pin_job_update BEFORE UPDATE ON jobs WHEN EXISTS (SELECT 1 FROM v2_event_pins WHERE job_id=OLD.id OR job_id=NEW.id) BEGIN SELECT RAISE(ABORT,'v2 event pin held'); END",
 'v2_pin_job_delete':"CREATE TRIGGER v2_pin_job_delete BEFORE DELETE ON jobs WHEN EXISTS (SELECT 1 FROM v2_event_pins WHERE job_id=OLD.id) BEGIN SELECT RAISE(ABORT,'v2 event pin held'); END",
 'v2_pin_reference_update':"CREATE TRIGGER v2_pin_reference_update BEFORE UPDATE ON reference_jobs WHEN EXISTS (SELECT 1 FROM v2_event_pins WHERE job_id=OLD.job_id OR job_id=NEW.job_id) BEGIN SELECT RAISE(ABORT,'v2 event pin held'); END",
 'v2_pin_reference_insert':"CREATE TRIGGER v2_pin_reference_insert BEFORE INSERT ON reference_jobs WHEN EXISTS (SELECT 1 FROM v2_event_pins p JOIN reference_jobs j ON j.job_id=p.job_id WHERE p.job_id=NEW.job_id OR j.submission_id=NEW.submission_id) BEGIN SELECT RAISE(ABORT,'v2 event pin held'); END",
 'v2_pin_reference_delete':"CREATE TRIGGER v2_pin_reference_delete BEFORE DELETE ON reference_jobs WHEN EXISTS (SELECT 1 FROM v2_event_pins WHERE job_id=OLD.job_id) BEGIN SELECT RAISE(ABORT,'v2 event pin held'); END",
 'v2_pin_claim_update':"CREATE TRIGGER v2_pin_claim_update BEFORE UPDATE ON reference_claims WHEN EXISTS (SELECT 1 FROM v2_event_pins WHERE job_id=OLD.job_id OR job_id=NEW.job_id) BEGIN SELECT RAISE(ABORT,'v2 event pin held'); END",
 'v2_pin_claim_delete':"CREATE TRIGGER v2_pin_claim_delete BEFORE DELETE ON reference_claims WHEN EXISTS (SELECT 1 FROM v2_event_pins WHERE job_id=OLD.job_id) BEGIN SELECT RAISE(ABORT,'v2 event pin held'); END",
 'v2_pin_claim_insert':"CREATE TRIGGER v2_pin_claim_insert BEFORE INSERT ON reference_claims WHEN EXISTS (SELECT 1 FROM v2_event_pins p JOIN reference_claims c ON c.job_id=p.job_id WHERE p.job_id=NEW.job_id OR c.request_id=NEW.request_id) BEGIN SELECT RAISE(ABORT,'v2 event pin held'); END",
}
class PinHeld(ValueError):pass

REFERENCE_DDL={
 'reference_jobs':'''CREATE TABLE reference_jobs (
                  job_id TEXT PRIMARY KEY REFERENCES jobs(id), submission_id TEXT UNIQUE NOT NULL,
                  request_digest TEXT NOT NULL, environment TEXT NOT NULL, input_json TEXT NOT NULL,
                  manifest_sha TEXT NOT NULL, pipeline_digest TEXT NOT NULL, generation INTEGER NOT NULL DEFAULT 1,
                  phase TEXT NOT NULL, node_id TEXT, transfer_json TEXT, result_json TEXT, error TEXT,
                  created_at TEXT DEFAULT CURRENT_TIMESTAMP, updated_at TEXT DEFAULT CURRENT_TIMESTAMP)''',
 'reference_claims':'''CREATE TABLE reference_claims (
                  request_id TEXT PRIMARY KEY, job_id TEXT UNIQUE NOT NULL REFERENCES reference_jobs(job_id),
                  node_id TEXT NOT NULL, worker_id TEXT NOT NULL, instance_id TEXT NOT NULL,
                  token TEXT NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP)'''}

def _unique_schema(db):
    from .transfer_fence import JOBS_DDL
    for table,sql in dict(REFERENCE_DDL,jobs=JOBS_DDL).items():
        actual=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?",(table,)).fetchone()
        if actual is None or actual[0]!=sql:raise PinHeld('source schema changed')
    expected={'jobs':{('id',),('request_id',)},'reference_jobs':{('job_id',),('submission_id',)},'reference_claims':{('request_id',),('job_id',)}}
    for table,keys in expected.items():
        indexes=db.execute('PRAGMA index_list('+table+')').fetchall()
        observed=set()
        for row in indexes:
            if row[2]:
                if row[4]:raise PinHeld('partial unique index unsupported')
                name=row[1]
                if not isinstance(name,str) or not name.replace('_','').isalnum():raise PinHeld('invalid index name')
                columns=tuple(v[2] for v in db.execute('PRAGMA index_info("'+name+'")'))
                if None in columns or columns in observed:raise PinHeld('unexpected unique index')
                observed.add(columns)
        if observed!=keys:raise PinHeld('source unique schema changed')

def install_offline(store,*,enabled=False):
    if QUALIFIED is not True or enabled is not True or type(store) is not ReferenceStore:raise PinHeld('unqualified')
    with store.queue._db() as db:
        db.execute('BEGIN IMMEDIATE')
        if db.execute("SELECT 1 FROM jobs WHERE status IN ('running','held') LIMIT 1").fetchone():raise PinHeld('offline queue required')
        _unique_schema(db)
        db.execute(DDL)
        for sql in TRIGGERS.values():db.execute(sql)

def attest(db):
    _unique_schema(db)
    row=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='v2_event_pins'").fetchone()
    if row is None or row[0]!=DDL:raise PinHeld('pin schema missing')
    for name,sql in TRIGGERS.items():
        row=db.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?",(name,)).fetchone()
        if row is None or row[0]!=sql:raise PinHeld('pin trigger missing')

def pin(store,*,claim,event_raw):
    """Persist an authenticated existing claim's held intent, not event authority."""
    if QUALIFIED is not True or type(store) is not ReferenceStore or type(claim) is not dict or set(claim)!={'jobId','nodeId','workerId','workerInstanceId','claimToken'}:raise PinHeld('unqualified claim')
    if type(event_raw) is not bytes or not 0<len(event_raw)<=4096:raise PinHeld('invalid event')
    try:
        event=json.loads(event_raw.decode('ascii'))
        if (type(event) is not dict or set(event)!={'domain','ledger','epoch','operation','previousRevision','previousDigest','nextRevision','nextDigest'}
                or envelope._canonical(event)!=event_raw):raise ValueError()
        for field in ('domain','ledger','epoch','operation'):
            if type(event[field]) is not str or not envelope.ID.fullmatch(event[field]):raise ValueError()
        for field in ('previousDigest','nextDigest'):
            if type(event[field]) is not str or not envelope.HEX.fullmatch(event[field]):raise ValueError()
        if type(event['previousRevision']) is not int or type(event['nextRevision']) is not int or not 0<=event['previousRevision']<2**53-1 or event['nextRevision']!=event['previousRevision']+1:raise ValueError()
    except Exception:raise PinHeld('invalid event') from None
    with store.queue._db() as db:
        db.execute('BEGIN IMMEDIATE');attest(db)
        snapshot=store._claim_snapshot_uncached(db,claim['jobId'],node_id=claim['nodeId'],worker_id=claim['workerId'],worker_instance_id=claim['workerInstanceId'],claim_token=claim['claimToken'])
        if snapshot['status']!='running' or snapshot['executionPhase'] not in ('fetching','processing'):raise PinHeld('running claim required')
        try:attempts.attest(db)
        except attempts.AttemptHeld:raise PinHeld('attempt guard missing') from None
        attempt=db.execute('SELECT request_id,attempt_id,claim_sha,identity FROM v2_attempts WHERE job_id=?',(claim['jobId'],)).fetchone()
        expected_attempt=dict(schemaVersion=1,scope='local_attempt_not_authority',**{k:snapshot[k] for k in ('authorityId','jobId','generation','environment','nodeId','workerId','workerInstanceId','claimRequestId','manifestSha256','pipelineDigest')})
        if attempt is None or attempt['claim_sha']!=snapshot['claimTokenSha256'] or attempt['identity']!=p.canonical_json(expected_attempt).decode():raise PinHeld('attempt binding changed')
        p._slug(attempt['request_id'],'attempt request');p._uuid(attempt['attempt_id'],'attempt')
        raw_transfer=db.execute('SELECT transfer_json,input_json FROM reference_jobs WHERE job_id=?',(claim['jobId'],)).fetchone()
        try:
            transfer=json.loads(raw_transfer[0]);submission=json.loads(raw_transfer[1])
            p._closed(transfer,('reservationId','generation','expiresAt','manifestSha256','byteBudget','nonce'),'transfer request')
            for field in ('reservationId','nonce'):p._slug(transfer[field],field)
            p._integer(transfer['generation'],'generation');p._integer(transfer['byteBudget'],'byteBudget');p._hash(transfer['manifestSha256'],'manifest')
            expiry=transfer['expiresAt']
            if type(expiry) is not str or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z',expiry):raise ValueError()
            datetime.fromisoformat(expiry.replace('Z','+00:00'))
            total=submission['manifest']['totalBytes'];p._integer(total,'total')
            if ((transfer['generation'],transfer['manifestSha256'])!=(snapshot['generation'],snapshot['manifestSha256'])
                    or not total<=transfer['byteBudget']<=min(p.MAX_SAFE_INTEGER,3*total)):raise ValueError()
            reservation=transfer['reservationId']
        except Exception:raise PinHeld('reserved claim required') from None
        local_identity=dict(schemaVersion=1,scope='local_observation_not_authority',reservationId=reservation,attemptId=attempt['attempt_id'],
            **{k:snapshot[k] for k in ('authorityId','jobId','generation','environment','nodeId','workerId','workerInstanceId','claimRequestId','manifestSha256','pipelineDigest')})
        local_bytes=p.canonical_json(local_identity);local_sha=hashlib.sha256(LOCAL_IDENTITY_DOMAIN+local_bytes).hexdigest()
        binding=p.canonical_json(snapshot).decode('utf-8');digest=hashlib.sha256(event_raw).hexdigest()
        prior=db.execute('SELECT job_id,claim_sha,claim_binding,event_bytes,event_sha,local_identity,local_identity_sha,state FROM v2_event_pins WHERE operation=?',(event['operation'],)).fetchone()
        expected=(claim['jobId'],snapshot['claimTokenSha256'],binding,event_raw,digest,local_bytes.decode(),local_sha,'held')
        if prior is None:
            if db.execute('SELECT 1 FROM v2_event_pins WHERE job_id=?',(claim['jobId'],)).fetchone():raise PinHeld('parent already pinned')
            db.execute('INSERT INTO v2_event_pins VALUES(?,?,?,?,?,?,?,?,?)',(event['operation'],*expected))
        elif tuple(prior)!=expected:raise PinHeld('conflicting pinned event')
        return dict(operation=event['operation'],eventSha256=digest,localIdentitySha256=local_sha,state='held',authorization='unqualified')
