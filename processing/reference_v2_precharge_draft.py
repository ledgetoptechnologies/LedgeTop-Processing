"""Inactive single-transfer v2 precharge prototype; no HTTP or authorization.

Caller must authenticate source/worker/grant, pin database inode and durable
transfer epoch, fence older processes, prevent database restore or path
replacement, and separately enforce stream concurrency. No renewal exists.
A successful return is accounting evidence, never an authorization to stream.
"""
import hashlib,json,os,sqlite3,stat
from datetime import datetime,timezone
from pathlib import Path
from . import reference_protocol as p
from .reference_input_grant_v2_draft import verify_candidate_grant
from .reference_manifest_v2 import decode_manifest_v2

SCHEMA=("CREATE TABLE transfer (id INTEGER PRIMARY KEY CHECK(id=1), grant BLOB NOT NULL, manifest BLOB NOT NULL, grant_sha TEXT NOT NULL, clock_ms INTEGER NOT NULL, requests INTEGER NOT NULL, bytes INTEGER NOT NULL)",
 "CREATE TABLE charges (request_id TEXT PRIMARY KEY, object_id TEXT NOT NULL, start INTEGER NOT NULL, end INTEGER NOT NULL, bytes INTEGER NOT NULL)")

def _schema_template():
 db=sqlite3.connect(':memory:')
 try:
  for q in SCHEMA:db.execute(q)
  return tuple(db.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name'))
 finally:db.close()
EXPECTED=_schema_template()

class PrechargeHeld(ValueError):pass

def _now_ms(clock):
 now=clock()
 if type(now) is not int or not 0<=now<=p.MAX_SAFE_INTEGER:raise PrechargeHeld('invalid clock')
 return now

def _utc_ms(value):
 dt=datetime.fromisoformat(value[:-1]+'+00:00');delta=dt-datetime(1970,1,1,tzinfo=timezone.utc)
 return delta.days*86400000+delta.seconds*1000+delta.microseconds//1000

class PrechargeLedger:
 def __init__(self,path,*,inode,epoch,clock):
  self.path=Path(path).absolute();self.inode=inode;self.epoch=epoch;self.clock=clock
  p._slug(epoch,'transfer epoch');self._check_path()
  db=self._db()
  try:self._validate(db)
  finally:db.close()
 def _check_path(self):
  if self.path.resolve(strict=True)!=self.path or any(v.is_symlink() for v in (self.path,*self.path.parents)):raise PrechargeHeld('ledger path changed')
  st=self.path.stat()
  if not stat.S_ISREG(st.st_mode) or (st.st_dev,st.st_ino)!=self.inode or st.st_nlink!=1 or (os.name=='posix' and (st.st_uid!=os.geteuid() or stat.S_IMODE(st.st_mode)!=0o600)):raise PrechargeHeld('ledger identity invalid')
 def _db(self):
  self._check_path();db=sqlite3.connect(self.path.as_uri()+'?mode=rw',uri=True,timeout=10,isolation_level=None)
  db.execute('PRAGMA synchronous=FULL');return db
 def _validate(self,db):
  schema=tuple(db.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name'))
  if schema!=EXPECTED or db.execute('SELECT 1 FROM sqlite_temp_master LIMIT 1').fetchone():raise PrechargeHeld('ledger schema differs')
  if db.execute("SELECT COUNT(*) FROM transfer").fetchone()[0]!=1 or db.execute("SELECT 1 FROM transfer WHERE length(grant)>16384 OR length(manifest)>16777216 LIMIT 1").fetchone():raise PrechargeHeld('bounded transfer required')
  if db.execute("SELECT 1 FROM charges WHERE typeof(request_id)!='text' OR length(request_id)>128 OR typeof(object_id)!='text' OR length(object_id)>128 OR typeof(start)!='integer' OR typeof(end)!='integer' OR typeof(bytes)!='integer' OR start<0 OR end<start OR bytes!=end-start LIMIT 1").fetchone():raise PrechargeHeld('invalid charge row')
  rows=db.execute('SELECT grant,manifest,grant_sha,clock_ms,requests,bytes FROM transfer WHERE id=1').fetchall()
  if len(rows)!=1:raise PrechargeHeld('transfer binding unavailable')
  raw,manifest_raw,digest,high,requests,charged=rows[0]
  try:
   grant=verify_candidate_grant(raw);manifest=decode_manifest_v2(manifest_raw)
   if grant['sha256']!=digest or grant['payload']['transferEpoch']!=self.epoch or grant['payload']['manifestSha256']!=manifest['sha256']:raise ValueError()
   if any(type(v) is not int or v<0 for v in (high,requests,charged)):raise ValueError()
   count,total=db.execute('SELECT COUNT(*),COALESCE(SUM(bytes),0) FROM charges').fetchone()
   if count!=requests or total!=charged or count>100000 or requests>grant['payload']['totalRequestBudget'] or charged>grant['payload']['totalByteBudget']:raise ValueError()
   objects={x['id']:x for x in manifest['objects']}
   for request_id,object_id,start,end,length in db.execute('SELECT request_id,object_id,start,end,bytes FROM charges'):
    p._slug(request_id,'request id');p._slug(object_id,'object id');obj=objects.get(object_id)
    if obj is None or length!=end-start or not ((obj['byteSize']==0 and start==end==0) or (0<=start<end<=obj['byteSize'] and end-start<=grant['payload']['rangeBytes'])):raise ValueError()
  except (ValueError,TypeError,OverflowError):raise PrechargeHeld('transfer accounting invalid') from None
  return grant['payload'],manifest,high,requests,charged
 def precharge(self,*,request_id,object_id,start,end):
  p._slug(request_id,'request id');p._slug(object_id,'object id')
  p._integer(start,'range start',allow_zero=True);p._integer(end,'range end',allow_zero=True)
  db=self._db()
  try:
   # Persist clock independently, so a denial or later rollback cannot revive an
   # expiry already observed by this database. This is not a rollback-proof clock.
   db.execute('BEGIN IMMEDIATE');grant,manifest,high,_,_=self._validate(db)
   now=_now_ms(self.clock);db.execute('UPDATE transfer SET clock_ms=? WHERE id=1',(max(now,high),));self._check_path();db.execute('COMMIT')
   db.execute('BEGIN IMMEDIATE');grant,manifest,high,requests,charged=self._validate(db)
   high=max(high,_now_ms(self.clock));db.execute('UPDATE transfer SET clock_ms=? WHERE id=1',(high,))
   if high>=_utc_ms(grant['expiresAt']):
    self._check_path();db.execute('COMMIT');raise PrechargeHeld('grant expired')
   obj=next((x for x in manifest['objects'] if x['id']==object_id),None)
   if obj is None:raise PrechargeHeld('object outside manifest')
   if not ((obj['byteSize']==0 and start==end==0) or (0<=start<end<=obj['byteSize'] and end-start<=grant['rangeBytes'])):raise PrechargeHeld('invalid range')
   prior=db.execute('SELECT object_id,start,end,bytes FROM charges WHERE request_id=?',(request_id,)).fetchone()
   if prior is not None:
    if prior!=(object_id,start,end,end-start):raise PrechargeHeld('request identity changed')
    db.execute('COMMIT');return {'state':'replay_no_body'}
   length=end-start
   if requests>=grant['totalRequestBudget'] or charged+length>grant['totalByteBudget']:raise PrechargeHeld('budget exhausted')
   db.execute('SAVEPOINT charge_attempt')
   db.execute('INSERT INTO charges VALUES(?,?,?,?,?)',(request_id,object_id,start,end,length))
   db.execute('UPDATE transfer SET requests=requests+1,bytes=bytes+? WHERE id=1',(length,))
   final=max(high,_now_ms(self.clock))
   if final>=_utc_ms(grant['expiresAt']):
    db.execute('ROLLBACK TO charge_attempt');db.execute('RELEASE charge_attempt')
    db.execute('UPDATE transfer SET clock_ms=? WHERE id=1',(final,));self._check_path();db.execute('COMMIT');raise PrechargeHeld('grant expired')
   db.execute('RELEASE charge_attempt');db.execute('UPDATE transfer SET clock_ms=? WHERE id=1',(final,))
   self._check_path();db.execute('COMMIT')
   return {'state':'charged_no_authority','requestId':request_id,'bytes':length}
  except Exception:
   if db.in_transaction:db.execute('ROLLBACK')
   raise
  finally:db.close()
