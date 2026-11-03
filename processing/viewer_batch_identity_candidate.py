"""Inactive principal-scoped fresh reservation candidate; no network listener."""
import hmac
import re
from .maintenance import Conflict
from .viewer_tasks import RESERVATION_LIMIT

OWNER_DDL="CREATE TABLE held_viewer_owner_v1 (task_id TEXT PRIMARY KEY, principal TEXT NOT NULL, environment TEXT NOT NULL, authority TEXT NOT NULL)"
class Identity:
 def __init__(self,store,credentials,legacy_tokens,authority):
  if type(credentials) is not list or not credentials or type(legacy_tokens) is not list or type(authority) is not str or authority!=store.queue.authority_id: raise ValueError('invalid identity configuration')
  self.store=store;self.authority=authority;self.credentials=[]
  seen=list(legacy_tokens)
  for x in credentials:
   if type(x) is not dict or set(x)!={'token','principalId','environment'}:raise ValueError('invalid credential')
   token,principal,environment=x['token'],x['principalId'],x['environment']
   if type(token) is not str or not 16<=len(token)<=4096 or not token.isascii() or not all(33<=ord(c)<=126 for c in token) or type(principal) is not str or not re.fullmatch(r'[A-Za-z0-9_-]{1,80}',principal) or environment not in ('production','staging') or any(hmac.compare_digest(token,old) for old in seen):raise ValueError('invalid credential')
   seen.append(token);self.credentials.append((token,principal,environment))
 def authenticate(self,token,protected_transport):
  if protected_transport is not True or type(token) is not str or not token.isascii():raise Conflict('identity unavailable')
  result=None
  for expected,principal,environment in self.credentials:
   if hmac.compare_digest(token,expected):result=(principal,environment)
  if result is None:raise Conflict('identity unavailable')
  return result
 def initialize(self):
  with self.store.queue._db() as db:
   db.execute('BEGIN IMMEDIATE');row=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='held_viewer_owner_v1'").fetchone()
   if row is None:db.execute(OWNER_DDL)
   elif row['sql']!=OWNER_DDL:raise Conflict('owner schema incompatible')
 def reserve_fresh(self,task,capability,token,protected_transport):
  principal,environment=self.authenticate(token,protected_transport)
  digest=self.store._capability_hash(task,environment,capability)
  with self.store._lock,self.store.queue._db() as db:
   db.execute('BEGIN IMMEDIATE')
   a=db.execute('SELECT authority_id FROM coordinator_identity WHERE singleton=1').fetchone()
   if a is None or a[0]!=self.authority or self.store.queue.authority_id!=self.authority:raise Conflict('authority unavailable')
   old=db.execute('SELECT * FROM held_viewer_owner_v1 WHERE task_id=?',(task,)).fetchone()
   if old:
    if (old['principal'],old['environment'],old['authority'])!=(principal,environment,self.authority):raise Conflict('identity unavailable')
    self.store._reservation(db,task,environment,capability);return
   if db.execute('SELECT 1 FROM viewer_reservations WHERE uuid=?',(task,)).fetchone() or db.execute('SELECT 1 FROM viewer_tasks WHERE uuid=?',(task,)).fetchone():raise Conflict('legacy reservation unavailable')
   if db.execute('SELECT COUNT(*) FROM viewer_reservations').fetchone()[0]>=RESERVATION_LIMIT:raise Conflict('reservation quota')
   db.execute("INSERT INTO viewer_reservations(uuid,environment,capability_hash,state) VALUES (?,?,?,'reserved')",(task,environment,digest))
   db.execute('INSERT INTO held_viewer_owner_v1 VALUES(?,?,?,?)',(task,principal,environment,self.authority))

 def prepare(self,task,capability,token,protected_transport,generation,batch_id,files):
  from .viewer_batch_intent_candidate import prepare
  principal,environment=self.authenticate(token,protected_transport)
  return prepare(self.store,task,environment,capability,generation,batch_id,files,identity=(principal,self.authority))
 def upload(self,task,capability,token,protected_transport,generation,batch_id,name,stream,length):
  from .viewer_batch_intent_candidate import upload_held
  principal,environment=self.authenticate(token,protected_transport)
  return upload_held(self.store,task,environment,capability,generation,batch_id,name,stream,length,identity=(principal,self.authority))

def check_owner(db,queue,task,environment,identity):
 principal,authority=identity
 owner=db.execute('SELECT principal,environment,authority FROM held_viewer_owner_v1 WHERE task_id=?',(task,)).fetchone()
 actual=db.execute('SELECT authority_id FROM coordinator_identity WHERE singleton=1').fetchone()
 if owner is None or actual is None or tuple(owner)!=(principal,environment,authority) or actual[0]!=authority or queue.authority_id!=authority:raise Conflict('identity unavailable')
