"""Inactive offline-audited, trigger-enforced upload intent invariant."""
import hashlib
import json
import re
import sqlite3

MARKER="CREATE TABLE upload_incremental_required (singleton INTEGER PRIMARY KEY CHECK(singleton=1), required INTEGER NOT NULL CHECK(required=1))"
INDEX="CREATE INDEX upload_census_unresolved ON broker_intents(operation_id) WHERE state IS NOT 'complete'"
_VALID = "EXISTS (SELECT 1 FROM upload_intent_kinds k WHERE k.operation_id=NEW.operation_id AND k.job_id=NEW.job_id AND k.grant_hash=NEW.grant_hash AND upload_intent_valid(NEW.operation_id,NEW.job_id,NEW.grant_hash,k.kind)=1 AND (k.kind IN ('init','upload_local') OR (k.kind='upload_remote' AND EXISTS (SELECT 1 FROM broker_node_receipts r JOIN broker_sender_evidence e ON e.operation_id=r.operation_id WHERE r.operation_id=NEW.operation_id AND upload_receipt_valid(NEW.operation_id,NEW.job_id,NEW.grant_hash,r.job_id,r.epoch,r.grant_hash,r.receipt_json,r.receipt_sha256,e.job_id,e.epoch,e.grant_hash)=1))))"
TRIGGERS={
 'upload_guard_kind_insert': "CREATE TRIGGER upload_guard_kind_insert BEFORE INSERT ON upload_intent_kinds WHEN upload_census_writer()!=1 OR upload_intent_valid(NEW.operation_id,NEW.job_id,NEW.grant_hash,NEW.kind)!=1 OR NEW.kind NOT IN ('init','upload_local','upload_remote') OR EXISTS (SELECT 1 FROM upload_intent_kinds WHERE operation_id=NEW.operation_id) OR NOT EXISTS (SELECT 1 FROM broker_intents WHERE operation_id=NEW.operation_id AND job_id=NEW.job_id AND grant_hash=NEW.grant_hash AND state='started') BEGIN SELECT RAISE(ABORT,'unqualified upload classification'); END",
 'upload_guard_intent_insert': "CREATE TRIGGER upload_guard_intent_insert BEFORE INSERT ON broker_intents WHEN upload_census_writer()!=1 OR upload_intent_valid(NEW.operation_id,NEW.job_id,NEW.grant_hash,'init')!=1 OR (EXISTS (SELECT 1 FROM upload_intent_kinds WHERE operation_id=NEW.operation_id AND (job_id IS NOT NEW.job_id OR grant_hash IS NOT NEW.grant_hash))) OR (NEW.state IS 'complete' AND NOT ("+_VALID+")) BEGIN SELECT RAISE(ABORT,'unqualified upload intent'); END",
 'upload_guard_intent_update': "CREATE TRIGGER upload_guard_intent_update BEFORE UPDATE ON broker_intents WHEN upload_census_writer()!=1 OR upload_intent_valid(NEW.operation_id,NEW.job_id,NEW.grant_hash,'init')!=1 OR OLD.operation_id IS NOT NEW.operation_id OR OLD.job_id IS NOT NEW.job_id OR OLD.grant_hash IS NOT NEW.grant_hash OR (NEW.state IS 'complete' AND NOT ("+_VALID+")) BEGIN SELECT RAISE(ABORT,'unqualified upload intent'); END",
 'upload_guard_intent_delete': "CREATE TRIGGER upload_guard_intent_delete BEFORE DELETE ON broker_intents BEGIN SELECT RAISE(ABORT,'upload intent immutable'); END",
 'upload_guard_evidence_insert': "CREATE TRIGGER upload_guard_evidence_insert BEFORE INSERT ON broker_sender_evidence WHEN upload_census_writer()!=1 OR EXISTS (SELECT 1 FROM broker_sender_evidence WHERE operation_id=NEW.operation_id) OR NOT EXISTS (SELECT 1 FROM broker_intents i JOIN upload_intent_kinds k ON k.operation_id=i.operation_id WHERE i.operation_id=NEW.operation_id AND i.job_id=NEW.job_id AND i.grant_hash=NEW.grant_hash AND i.state='started' AND k.kind='upload_remote') BEGIN SELECT RAISE(ABORT,'unqualified sender evidence'); END",
 'upload_guard_receipt_insert': "CREATE TRIGGER upload_guard_receipt_insert BEFORE INSERT ON broker_node_receipts WHEN upload_census_writer()!=1 OR EXISTS (SELECT 1 FROM broker_node_receipts WHERE operation_id=NEW.operation_id) OR NOT EXISTS (SELECT 1 FROM broker_intents i JOIN upload_intent_kinds k ON k.operation_id=i.operation_id WHERE i.operation_id=NEW.operation_id AND i.job_id=NEW.job_id AND i.grant_hash=NEW.grant_hash AND i.state='started' AND k.kind='upload_remote') BEGIN SELECT RAISE(ABORT,'unqualified node receipt'); END",
}
class IncrementalError(ValueError):pass

def receipt_valid(operation_id,job_id,grant_hash,rjob,repoch,rhash,receipt,rsha,ejob,eepoch,ehash):
    try:
        from .upload_intent_census import _valid_upload_grant
        from .node_mutation_broker import _upload_receipt
        if type(receipt) is not str or len(receipt)>10000:return 0
        payload=json.loads(receipt);grant=payload['grant']
        row={'operation_id':operation_id,'job_id':job_id,'grant_hash':grant_hash}
        if not _valid_upload_grant(grant,row):return 0
        canonical,digest=_upload_receipt(grant,payload['reply'],grant_hash)
        return int(canonical==receipt and digest==rsha and (rjob,repoch,rhash)==(job_id,grant['coordinatorEpoch'],grant_hash)
                   and (ejob,eepoch,ehash)==(job_id,grant['coordinatorEpoch'],grant_hash))
    except (ValueError,TypeError,KeyError,IndexError,OverflowError):return 0

def intent_valid(operation_id,job_id,grant_hash,kind):
    from .node_mutation_broker import _uuid
    return int(_uuid(operation_id) and _uuid(job_id) and type(grant_hash) is str
               and bool(re.fullmatch('[a-f0-9]{64}',grant_hash))
               and kind in ('init','upload_local','upload_remote'))

def configure(db):
    db.create_function('upload_census_writer',0,lambda:1)
    db.create_function('upload_intent_valid',4,intent_valid)
    db.create_function('upload_receipt_valid',11,receipt_valid)

def is_enabled(db):
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE name='upload_incremental_required'").fetchone())

def attest(db):
    from .upload_intent_census import attest as attest_census
    attest_census(db)
    for name,sql in [('upload_incremental_required',MARKER),('upload_census_unresolved',INDEX),*TRIGGERS.items()]:
        row=db.execute('SELECT sql FROM sqlite_master WHERE name=?',(name,)).fetchone()
        if row is None or row[0]!=sql:raise IncrementalError('incremental census schema unavailable')
    if db.execute('SELECT COUNT(*) FROM upload_incremental_required WHERE singleton=1 AND required=1').fetchone()[0]!=1:
        raise IncrementalError('incremental census marker unavailable')

def install_offline(db, *, enabled=False):
    if enabled is not True or db.in_transaction:raise IncrementalError('offline migration required')
    db.execute('BEGIN IMMEDIATE')
    try:
        from .upload_intent_census import attest as attest_census, _valid_upload_grant
        from .durable_upload_evidence import verify_existing_node_receipt
        attest_census(db)
        if is_enabled(db):raise IncrementalError('incremental census already exists')
        if db.execute("SELECT 1 FROM jobs WHERE status IN ('running','held') LIMIT 1").fetchone():raise IncrementalError('idle coordinator required')
        if db.execute("SELECT 1 FROM broker_intents WHERE operation_id IS NULL OR operation_id='' LIMIT 1").fetchone():raise IncrementalError('invalid historical intent identity')
        if db.execute('SELECT 1 FROM upload_intent_kinds k LEFT JOIN broker_intents i ON i.operation_id=k.operation_id WHERE i.operation_id IS NULL LIMIT 1').fetchone():
            raise IncrementalError('orphan classifications require inspection')
        db.execute(INDEX)
        # Chunked offline audit; never infer an unknown prior completion is safe.
        last=''
        while True:
            rows=db.execute("SELECT i.operation_id,i.job_id,i.grant_hash,k.job_id AS kj,k.grant_hash AS kh,k.kind FROM broker_intents i LEFT JOIN upload_intent_kinds k ON k.operation_id=i.operation_id WHERE i.state='complete' AND i.operation_id>? ORDER BY i.operation_id LIMIT 500",(last,)).fetchall()
            if not rows:break
            for row in rows:
                bad=(row['kj'],row['kh'])!=(row['job_id'],row['grant_hash']) or not intent_valid(row['operation_id'],row['job_id'],row['grant_hash'],row['kind'])
                if not bad and row['kind']=='upload_remote':
                    try:
                        found=db.execute('SELECT substr(receipt_json,1,10001) FROM broker_node_receipts WHERE operation_id=?',(row['operation_id'],)).fetchone()
                        if found is None or type(found[0]) is not str or len(found[0])>10000:raise ValueError()
                        grant=json.loads(found[0])['grant']
                        if not _valid_upload_grant(grant,row):raise ValueError()
                        verify_existing_node_receipt(db,grant)
                    except (ValueError,TypeError,KeyError,IndexError):bad=True
                if bad:db.execute("UPDATE broker_intents SET state='receipt_uncertain' WHERE operation_id=? AND state='complete'",(row['operation_id'],))
            last=rows[-1]['operation_id']
        db.execute(MARKER);db.execute('INSERT INTO upload_incremental_required VALUES(1,1)')
        for sql in TRIGGERS.values():db.execute(sql)
        attest(db);db.commit()
    except BaseException:
        db.rollback();raise
