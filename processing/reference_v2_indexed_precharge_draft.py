"""Inactive process-owned indexed precharge experiment; not authorization.

External supervisor must prevent database/file replacement and restore, direct
writes through this private connection, native/user code tampering and old
uncooperative writers. Different cooperating processes may transact on the same
SQLite DB; their commits trigger full revalidation before this process continues.
Every operation holds BEGIN IMMEDIATE while checking data_version. Same-process
connection writes, restored snapshots and underlying disk corruption are not
proved by data_version. No HTTP, streaming or renewal is provided.
"""
import os
from . import reference_protocol as p
from .reference_v2_precharge_draft import PrechargeLedger,PrechargeHeld,MAX_LOCAL_CHARGES,_now_ms,_utc_ms

class IndexedPrecharge(PrechargeLedger):
 def __init__(self,path,*,inode,epoch,clock):
  super().__init__(path,inode=inode,epoch=epoch,clock=clock)
  self._conn=super()._db();self._pid=os.getpid();self._version=None;self._cache=None
  try:
   self._conn.execute('BEGIN IMMEDIATE');self._refresh();self._conn.execute('COMMIT')
  except Exception:
   self._conn.close();self._conn=None;raise

 def close(self):
  if self._conn is not None:self._conn.close();self._conn=None

 def _refresh(self):
  db=self._conn
  if self._pid!=os.getpid() or db is None:raise PrechargeHeld('connection owner changed')
  if not db.in_transaction:raise PrechargeHeld('locked snapshot required')
  self._check_path();version=db.execute('PRAGMA data_version').fetchone()[0]
  if self._version!=version:
   grant,manifest,high,requests,charged=self._validate(db)
   self._cache=(grant,{o['id']:o for o in manifest['objects']},high,requests,charged)
   self._version=version
  return self._cache

 def precharge(self,*,request_id,object_id,start,end):
  p._slug(request_id,'request id');p._slug(object_id,'object id')
  p._integer(start,'range start',allow_zero=True);p._integer(end,'range end',allow_zero=True)
  db=self._conn
  if db is None or self._pid!=os.getpid() or db.in_transaction:raise PrechargeHeld('idle owned connection required')
  try:
   db.execute('BEGIN IMMEDIATE');grant,objects,high,requests,charged=self._refresh()
   high=max(high,_now_ms(self.clock));db.execute('UPDATE transfer SET clock_ms=? WHERE id=1',(high,))
   self._check_path();db.execute('COMMIT');self._cache=(grant,objects,high,requests,charged)
   db.execute('BEGIN IMMEDIATE');grant,objects,high,requests,charged=self._refresh()
   high=max(high,_now_ms(self.clock));db.execute('UPDATE transfer SET clock_ms=? WHERE id=1',(high,))
   def deny(reason):
    self._check_path();db.execute('COMMIT');self._cache=(grant,objects,high,requests,charged);raise PrechargeHeld(reason)
   if high>=_utc_ms(grant['expiresAt']):deny('grant expired')
   obj=objects.get(object_id)
   if obj is None:deny('object outside manifest')
   if not ((obj['byteSize']==0 and start==end==0) or (0<=start<end<=obj['byteSize'] and end-start<=grant['rangeBytes'])):deny('invalid range')
   prior=db.execute('SELECT object_id,start,end,bytes FROM charges WHERE request_id=?',(request_id,)).fetchone()
   if prior is not None:
    if prior!=(object_id,start,end,end-start):deny('request identity changed')
    self._check_path();db.execute('COMMIT');self._cache=(grant,objects,high,requests,charged);return {'state':'replay_no_body'}
   length=end-start
   if requests>=MAX_LOCAL_CHARGES:deny('local charge record limit reached')
   if requests>=grant['totalRequestBudget'] or charged+length>grant['totalByteBudget']:deny('budget exhausted')
   db.execute('SAVEPOINT charge_attempt');db.execute('INSERT INTO charges VALUES(?,?,?,?,?)',(request_id,object_id,start,end,length))
   db.execute('UPDATE transfer SET requests=requests+1,bytes=bytes+? WHERE id=1',(length,))
   final=max(high,_now_ms(self.clock))
   if final>=_utc_ms(grant['expiresAt']):
    db.execute('ROLLBACK TO charge_attempt');db.execute('RELEASE charge_attempt')
    db.execute('UPDATE transfer SET clock_ms=? WHERE id=1',(final,));self._check_path();db.execute('COMMIT')
    self._cache=(grant,objects,final,requests,charged);raise PrechargeHeld('grant expired')
   db.execute('RELEASE charge_attempt');db.execute('UPDATE transfer SET clock_ms=? WHERE id=1',(final,))
   self._check_path();db.execute('COMMIT');self._cache=(grant,objects,final,requests+1,charged+length)
   return {'state':'charged_no_authority','requestId':request_id,'bytes':length}
  except Exception:
   if db.in_transaction:db.execute('ROLLBACK')
   # Revalidate after uncertain commit or exception; do not trust a stale cache.
   self._version=None
   raise
