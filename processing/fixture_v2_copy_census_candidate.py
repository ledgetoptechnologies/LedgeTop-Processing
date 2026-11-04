"""Bounded structural census for the unmounted v2 copy experiment.

This is not signature verification or migration/writer fencing. It fails
closed on unknown rows and is only relevant if the fixture table exists.
"""
import hashlib
from . import reference_protocol as p

MAX_ROWS=10000
MAX_BYTES=64*1024*1024
SCHEMA="""CREATE TABLE fixture_v3_copy_v2 (job_id TEXT PRIMARY KEY, raw BLOB NOT NULL,
 digest TEXT NOT NULL, receipt_id TEXT UNIQUE NOT NULL, storage_id TEXT UNIQUE NOT NULL,
 outer_id TEXT UNIQUE NOT NULL, outer_storage_id TEXT UNIQUE NOT NULL)"""

def _checked_schema(db):
    objects=db.execute("SELECT type,sql,name FROM sqlite_master WHERE name=? COLLATE NOCASE",('fixture_v3_copy_v2',)).fetchall()
    if not objects:return False
    if len(objects)!=1 or objects[0][0]!='table' or objects[0][2]!='fixture_v3_copy_v2' or type(objects[0][1]) is not str or ' '.join(objects[0][1].split())!=' '.join(SCHEMA.split()):
        raise ValueError('fixture schema differs')
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' AND tbl_name='fixture_v3_copy_v2' LIMIT 1").fetchone():
        raise ValueError('fixture trigger differs')
    indexes=db.execute('PRAGMA index_list(fixture_v3_copy_v2)').fetchall()
    expected={'job_id':'pk','receipt_id':'u','storage_id':'u','outer_id':'u','outer_storage_id':'u'}
    if len(indexes)!=len(expected):raise ValueError('fixture indexes differ')
    seen={}
    for index in indexes:
        if index[2]!=1 or index[4]!=0 or index[3] not in ('pk','u'):raise ValueError('fixture index differs')
        columns=db.execute('PRAGMA index_info("'+index[1].replace('"','""')+'")').fetchall()
        if len(columns)!=1 or columns[0][2] not in expected or columns[0][2] in seen:raise ValueError('fixture index differs')
        seen[columns[0][2]]=index[3]
    if seen!=expected:raise ValueError('fixture indexes differ')
    return True

def census(db, *, exclude_job=None):
    if not _checked_schema(db):return set(),0
    if not db.in_transaction:raise ValueError('transaction required')
    count=db.execute('SELECT COUNT(*) FROM (SELECT 1 FROM fixture_v3_copy_v2 LIMIT ?)',(MAX_ROWS+1,)).fetchone()[0]
    if count>MAX_ROWS:raise ValueError('fixture row limit')
    columns=('job_id','digest','receipt_id','storage_id','outer_id','outer_storage_id')
    invalid="typeof(raw)!='blob' OR length(raw) NOT BETWEEN 1 AND ? OR " + ' OR '.join("typeof(%s)!='text' OR length(%s) NOT BETWEEN 1 AND 128 OR instr(%s,char(0))!=0"%(x,x,x) for x in columns)
    if db.execute('SELECT 1 FROM fixture_v3_copy_v2 WHERE '+invalid+' LIMIT 1',(p.MAX_BODY_BYTES,)).fetchone():raise ValueError('invalid fixture row')
    total=0; identities=set(); seen=set()
    for row in db.execute('SELECT job_id,raw,digest,receipt_id,storage_id,outer_id,outer_storage_id FROM fixture_v3_copy_v2'):
        job,raw,digest,*ids=tuple(row)
        total+=len(raw)+sum(len(x.encode('utf-8')) for x in (job,digest,*ids))+128
        if total>MAX_BYTES:raise ValueError('fixture byte limit')
        p._slug(job,'job');p._hash(digest,'digest')
        for value in ids:p._slug(value,'receipt id')
        if len(set(ids))!=4 or set(ids)&seen:raise ValueError('duplicate fixture ids')
        envelope=p.decode_submission(raw);p._closed(envelope,('acceptance','viewerReceipt'),'copy')
        receipt=envelope['viewerReceipt'];acceptance=envelope['acceptance']
        if (hashlib.sha256(raw).hexdigest()!=digest or ids != [receipt['receiptId'],receipt['storageReceiptId'],
              acceptance['receiptId'],acceptance['storageReceiptId']] or receipt['jobId']!=job):raise ValueError('fixture proof changed')
        seen.update(ids)
        if job!=exclude_job:identities.update(ids)
    return identities,total
