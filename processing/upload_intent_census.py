"""Inactive opt-in upload intent census; existing ambiguous history is held."""
import hashlib
import json
import re
import sqlite3

MARKER = "CREATE TABLE upload_census_required (singleton INTEGER PRIMARY KEY CHECK(singleton=1), required INTEGER NOT NULL CHECK(required=1))"
DDL = "CREATE TABLE upload_intent_kinds (operation_id TEXT PRIMARY KEY, job_id TEXT NOT NULL, grant_hash TEXT NOT NULL, kind TEXT NOT NULL)"
TRIGGERS = {
 'upload_intent_kind_update': "CREATE TRIGGER upload_intent_kind_update BEFORE UPDATE ON upload_intent_kinds BEGIN SELECT RAISE(ABORT,'upload intent kind immutable'); END",
 'upload_intent_kind_delete': "CREATE TRIGGER upload_intent_kind_delete BEFORE DELETE ON upload_intent_kinds BEGIN SELECT RAISE(ABORT,'upload intent kind immutable'); END",
}
# Explicit availability limit; larger histories remain held until an offline
# compaction or indexed-summary migration is qualified.
MAX_CENSUS_ROWS = 12000
class CensusError(ValueError): pass

def _bound(db, *, need_headroom=False):
    for table in ('broker_intents','upload_intent_kinds','broker_sender_evidence','broker_node_receipts'):
        rows=db.execute('SELECT 1 FROM '+table+' LIMIT ?', (MAX_CENSUS_ROWS+1,)).fetchall()
        if len(rows)>MAX_CENSUS_ROWS or (need_headroom and len(rows)>=MAX_CENSUS_ROWS):
            raise CensusError('upload census capacity exceeded')

def is_enabled(db):
    rows=db.execute("SELECT name FROM sqlite_master WHERE name IN ('upload_census_required','upload_intent_kinds')").fetchall()
    return bool(rows)

def attest(db):
    from .durable_upload_evidence import attest as attest_evidence
    attest_evidence(db)
    for name,sql in [('upload_census_required',MARKER),('upload_intent_kinds',DDL),*TRIGGERS.items()]:
        row=db.execute("SELECT sql FROM sqlite_master WHERE name=?",(name,)).fetchone()
        if row is None or row[0]!=sql:raise CensusError('upload census schema unavailable')
    if db.execute('SELECT COUNT(*) FROM upload_census_required WHERE singleton=1 AND required=1').fetchone()[0]!=1:
        raise CensusError('upload census marker unavailable')

def _valid_upload_grant(grant, row):
    from .node_mutation_broker import _UPLOAD_FIELDS, _uuid, _json
    from .engine_ingress_guard import MAX_STREAM_BYTES
    from .viewer_tasks import TaskStore
    try:TaskStore._filename(grant.get('fileName'))
    except (AttributeError,ValueError,TypeError):return False
    integers={'gateGeneration','taskGeneration','fileBytes'}
    return (type(grant) is dict and set(grant)==_UPLOAD_FIELDS
            and all(type(grant[k]) is str and 1<=len(grant[k])<=256 for k in _UPLOAD_FIELDS-integers)
            and grant['operation']=='upload' and grant['operationId']==row['operation_id'] and grant['jobId']==row['job_id']
            and all(_uuid(grant[k]) for k in ('authorityId','coordinatorEpoch','operationId','jobId','engineTaskId'))
            and grant['nodeId'] in ('cluster','main') and grant['environment'] in ('production','staging')
            and type(grant['gateGeneration']) is int and grant['gateGeneration']>0
            and type(grant['taskGeneration']) is int and grant['taskGeneration']>0
            and type(grant['fileBytes']) is int and 0<grant['fileBytes']<=MAX_STREAM_BYTES-4096
            and grant['contentType']=='application/octet-stream'
            and bool(re.fullmatch('[A-Za-z0-9:._-]{1,255}',grant['engineId']))
            and bool(re.fullmatch('[a-f0-9]{64}',grant['bodySha256']))
            and hashlib.sha256(_json(grant)).hexdigest()==row['grant_hash'])

def _bad_completed(db):
    from .durable_upload_evidence import verify_existing_node_receipt
    from .node_mutation_broker import _json
    bad=[]
    rows=db.execute("SELECT i.operation_id,i.job_id,i.grant_hash,k.job_id AS kj,k.grant_hash AS kh,k.kind FROM broker_intents i LEFT JOIN upload_intent_kinds k ON k.operation_id=i.operation_id WHERE i.state='complete'").fetchall()
    for row in rows:
        kind=row['kind']
        if (row['kj'],row['kh'])!=(row['job_id'],row['grant_hash']) or kind not in ('init','upload_local','upload_remote'):
            bad.append(row['operation_id']);continue
        if kind=='upload_remote':
            try:
                found=db.execute('SELECT substr(receipt_json,1,10001) FROM broker_node_receipts WHERE operation_id=?',(row['operation_id'],)).fetchone()
                if found is None or type(found[0]) is not str or len(found[0])>10000:raise ValueError()
                grant=json.loads(found[0])['grant']
                if not _valid_upload_grant(grant,row):raise ValueError()
                verify_existing_node_receipt(db,grant)
            except (ValueError,TypeError,KeyError,IndexError):bad.append(row['operation_id'])
    return bad

def incremental_enabled(db):
    from .incremental_upload_census import is_enabled
    return is_enabled(db)

def clear(db, *, required=False, require_incremental=False):
    if not is_enabled(db):return not (required or require_incremental)
    try:
        if incremental_enabled(db):
            from .incremental_upload_census import attest as attest_incremental
            attest_incremental(db);return True
        if require_incremental:return False
        attest(db);_bound(db)
        if db.execute('SELECT 1 FROM upload_intent_kinds k LEFT JOIN broker_intents i ON i.operation_id=k.operation_id WHERE i.operation_id IS NULL LIMIT 1').fetchone():return False
        return not _bad_completed(db)
    except (ValueError,TypeError,KeyError,IndexError,sqlite3.Error):return False

def startup_census(db, *, required=False, require_incremental=False):
    if not is_enabled(db):
        if required or require_incremental:raise CensusError('required upload census unavailable')
        return
    if incremental_enabled(db):
        from .incremental_upload_census import attest as attest_incremental
        attest_incremental(db);return
    if require_incremental:raise CensusError('required incremental census unavailable')
    attest(db);_bound(db)
    for operation_id in _bad_completed(db):
        db.execute("UPDATE broker_intents SET state='receipt_uncertain' WHERE operation_id=? AND state='complete'",(operation_id,))

def register(db, grant, digest, *, remote, new=False):
    if not is_enabled(db):return
    attest(db)
    kind='init' if grant['operation']=='init' else ('upload_remote' if remote else 'upload_local')
    row=db.execute('SELECT job_id,grant_hash,kind FROM upload_intent_kinds WHERE operation_id=?',(grant['operationId'],)).fetchone()
    expected=(grant['jobId'],digest,kind)
    if row is None:
        if not new:raise CensusError('unclassified intent requires inspection')
        if not incremental_enabled(db):_bound(db,need_headroom=True)
        db.execute('INSERT INTO upload_intent_kinds VALUES(?,?,?,?)',(grant['operationId'],*expected))
    elif tuple(row)!=expected:raise CensusError('upload intent classification changed')

def install_offline(db, *, enabled=False):
    if enabled is not True or db.in_transaction:raise CensusError('offline migration required')
    db.execute('BEGIN IMMEDIATE')
    try:
        from .durable_upload_evidence import attest as attest_evidence
        attest_evidence(db)
        if db.execute("SELECT 1 FROM jobs WHERE status IN ('running','held') LIMIT 1").fetchone():raise CensusError('idle coordinator required')
        if is_enabled(db):raise CensusError('existing census requires inspection')
        # Bound all legacy source tables before creating or changing anything.
        for table in ('broker_intents','broker_sender_evidence','broker_node_receipts'):
            if len(db.execute('SELECT 1 FROM '+table+' LIMIT ?', (MAX_CENSUS_ROWS+1,)).fetchall())>MAX_CENSUS_ROWS:
                raise CensusError('upload census capacity exceeded')
        db.execute(MARKER);db.execute('INSERT INTO upload_census_required VALUES(1,1)');db.execute(DDL)
        for sql in TRIGGERS.values():db.execute(sql)
        # Exact existing coordinator receipt+sender proof can identify a remote
        # upload. Everything else is ambiguous in the old journal.
        from .durable_upload_evidence import verify_existing_node_receipt
        from .node_mutation_broker import _json
        for row in db.execute('SELECT operation_id,job_id,grant_hash,state FROM broker_intents').fetchall():
            kind='legacy_unknown'
            try:
                receipt=db.execute('SELECT substr(receipt_json,1,10001) FROM broker_node_receipts WHERE operation_id=?',(row['operation_id'],)).fetchone()
                if receipt is None or type(receipt[0]) is not str or len(receipt[0])>10000:raise ValueError()
                grant=json.loads(receipt[0])['grant']
                if not _valid_upload_grant(grant,row):raise ValueError()
                verify_existing_node_receipt(db,grant);kind='upload_remote'
            except (ValueError,TypeError,KeyError,IndexError):pass
            db.execute('INSERT INTO upload_intent_kinds VALUES(?,?,?,?)',(row['operation_id'],row['job_id'],row['grant_hash'],kind))
            if kind=='legacy_unknown' and row['state']=='complete':
                db.execute("UPDATE broker_intents SET state='receipt_uncertain' WHERE operation_id=? AND state='complete'",(row['operation_id'],))
        attest(db);db.commit()
    except BaseException:
        db.rollback();raise
