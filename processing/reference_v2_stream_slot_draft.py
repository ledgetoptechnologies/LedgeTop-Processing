"""Disabled single in-flight stream slot experiment; never a stream grant.

The trusted supervisor must fence all noncooperating workers/hosts, prevent
SQLite and lock-path replacement or restore, and verify source streams are
closed before finish. A cooperative file lock cannot prove those properties.
The caller must separately authenticate the grant and enforce expiry at stream
admission. Pending state survives worker death and is never auto-cleared.
"""
import hashlib,json,os,sqlite3,stat
from pathlib import Path
from . import reference_protocol as p
from .reference_worker_fence_v2 import WorkerFence
from .reference_v2_indexed_precharge_draft import IndexedPrecharge
from .reference_input_grant_v2_draft import DOMAIN

DDL="CREATE TABLE stream_slot (id INTEGER PRIMARY KEY CHECK(id=1), epoch TEXT NOT NULL, phase TEXT NOT NULL, identity TEXT)"

class SlotHeld(ValueError):pass

def _expected():
 db=sqlite3.connect(':memory:')
 try:db.execute(DDL);return tuple(db.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name'))
 finally:db.close()
SCHEMA=_expected()

class StreamSlot:
 def __init__(self,*,path,inode,epoch,lock_directory):
  self.path=Path(path).absolute();self.inode=inode;self.epoch=epoch;self.fence=None;self.db=None;self.pid=os.getpid();p._slug(epoch,'epoch')
  try:
   self.fence=WorkerFence(lock_directory);self._check_path()
   self.db=sqlite3.connect(self.path.as_uri()+'?mode=rw',uri=True,isolation_level=None,timeout=10)
   self.db.execute('PRAGMA synchronous=FULL');self._validate()
  except Exception:self.close();raise
 def _check_path(self):
  self.fence.check()
  try:
   if self.path.resolve(strict=True)!=self.path or any(x.is_symlink() for x in (self.path,*self.path.parents)):raise SlotHeld('slot path changed')
   st=self.path.stat()
   if not stat.S_ISREG(st.st_mode) or (st.st_dev,st.st_ino)!=self.inode or st.st_nlink!=1 or (os.name=='posix' and (st.st_uid!=os.geteuid() or stat.S_IMODE(st.st_mode)!=0o600)):raise SlotHeld('slot identity changed')
  except OSError:raise SlotHeld('slot path unavailable') from None
 def _validate(self):
  if self.pid!=os.getpid() or self.db is None:raise SlotHeld('slot owner changed')
  self._check_path()
  if tuple(self.db.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name'))!=SCHEMA or self.db.execute('SELECT 1 FROM sqlite_temp_master LIMIT 1').fetchone():raise SlotHeld('slot schema changed')
  rows=self.db.execute('SELECT id,epoch,phase,identity FROM stream_slot').fetchall()
  if len(rows)!=1 or rows[0][0]!=1 or rows[0][1]!=self.epoch or rows[0][2] not in ('idle','opening','charged','closed'):raise SlotHeld('slot state changed')
  if rows[0][2]=='idle' and rows[0][3] is not None:raise SlotHeld('slot idle identity invalid')
  if rows[0][2]!='idle' and (type(rows[0][3]) is not str or len(rows[0][3].encode())>2048):raise SlotHeld('slot identity invalid')
  return rows[0]
 def _change(self,expected,phase,identity):
  self.db.execute('BEGIN IMMEDIATE')
  try:
   row=self._validate()
   if row[2]!=expected:raise SlotHeld('slot requires reconciliation')
   if expected in ('opening','charged') and row[3]!=identity:raise SlotHeld('slot identity changed')
   self.db.execute('UPDATE stream_slot SET phase=?,identity=? WHERE id=1',(phase,identity));self._check_path();self.db.execute('COMMIT')
  except Exception:
   if self.db.in_transaction:self.db.execute('ROLLBACK')
   raise
 def begin(self,ledger,*,request_id,object_id,start,end):
  if type(ledger) is not IndexedPrecharge or ledger.epoch!=self.epoch:raise SlotHeld('exact transfer ledger required')
  p._slug(request_id,'request id');p._slug(object_id,'object id');p._integer(start,'start',allow_zero=True);p._integer(end,'end',allow_zero=True)
  db=ledger._conn
  if db is None or db.in_transaction or ledger._pid!=os.getpid():raise SlotHeld('idle owned ledger required')
  try:
   db.execute('BEGIN IMMEDIATE');grant,_,_,_,_=ledger._refresh();ledger._check_path();db.execute('COMMIT')
  except Exception:
   if db.in_transaction:db.execute('ROLLBACK')
   raise
  identity=p.canonical_json(dict(epoch=self.epoch,grantSha256=hashlib.sha256(DOMAIN+p.canonical_json(grant)).hexdigest(),jobId=grant['jobId'],
   generation=grant['generation'],environment=grant['environment'],sourceId=grant['sourceId'],manifestSha256=grant['manifestSha256'],
   requestId=request_id,objectId=object_id,start=start,end=end)).decode()
  self.db.execute('BEGIN IMMEDIATE')
  try:
   row=self._validate()
   if row[2] not in ('idle','closed'):raise SlotHeld('slot requires reconciliation')
   self.db.execute("UPDATE stream_slot SET phase='opening',identity=? WHERE id=1",(identity,));self._check_path();self.db.execute('COMMIT')
  except Exception:
   if self.db.in_transaction:self.db.execute('ROLLBACK')
   raise
  # No bytes can be sent here. Any exception leaves a durable opening hold.
  charge=ledger.precharge(request_id=request_id,object_id=object_id,start=start,end=end)
  if charge.get('state')!='charged_no_authority':raise SlotHeld('replay cannot open stream')
  self._change('opening','charged',identity)
  return identity
 def finish(self,identity,*,stream_closed):
  if stream_closed is not True:raise SlotHeld('stream closure attestation required')
  # Assertion belongs to caller; this is not an independent process proof.
  self._change('charged','closed',identity)
 def state(self):return self._validate()[2]
 def close(self):
  if self.db is not None:self.db.close();self.db=None
  if self.fence is not None:self.fence.close();self.fence=None
