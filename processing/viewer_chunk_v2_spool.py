"""Fixture-only v2 coordinator slot and journal; not a transport or receipt."""
import hashlib,json,os,secrets,sqlite3,threading
from . import reference_protocol as p
from .direct_output_slot import open_slot,_open_slot
from .direct_slot_capture import _exists
from .viewer_archive_spool import ViewerArchiveSpool,SpoolHeld
from .viewer_chunk_spool import SyntheticChunkSpool
from .viewer_chunk_v2_geometry import geometry,verify_geometry_schema
from .viewer_chunk_v2_journal import SyntheticV2ChunkJournal

DDL="CREATE TABLE viewer_chunk_v2_slots (job_id TEXT PRIMARY KEY, binding TEXT NOT NULL, slot_key TEXT NOT NULL, slot_identity TEXT, state TEXT NOT NULL CHECK(state IN ('creating','ready','sealed')))"

class SyntheticV2Spool:
 def __init__(self,spool,*,synthetic_fixture=False):
  if synthetic_fixture is not True or type(spool) is not ViewerArchiveSpool:raise SpoolHeld('synthetic_fixture_required')
  self.spool=spool;self._pid=os.getpid();self._lock=threading.Lock();self._active={}
 def _binding(self,db,job,identity,token):
  saved,_=self.spool.claims._authorize_inventory(db,job,identity,token)
  metadata=self.spool._metadata(db,job,identity,token)
  if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='viewer_chunk_v2_geometry'").fetchone():raise SpoolHeld('v2_geometry_missing')
  verify_geometry_schema(db)
  row=db.execute('SELECT binding FROM viewer_chunk_v2_geometry WHERE job_id=?',(job,)).fetchone()
  if not row:raise SpoolHeld('v2_geometry_missing')
  record=p.decode_submission(row['binding'].encode());p._closed(record,('kind','jobId','claimSha256','metadataSha256','geometry'),'v2 binding')
  if (record['kind']!='synthetic-v2-geometry-held' or record['jobId']!=job or record['claimSha256']!=p.digest(saved)
      or record['metadataSha256']!=p.digest(metadata) or p.canonical_json(record).decode()!=row['binding']):raise SpoolHeld('v2_binding_changed')
  g=record['geometry'];p._closed(g,('version','archiveBytes','chunkBytes','chunkCount'),'v2 geometry')
  if geometry(g['archiveBytes'],g['chunkBytes'],g['version'])!=g or g['archiveBytes']!=metadata['archiveBytes']:raise SpoolHeld('v2_geometry_changed')
  binding=dict(jobId=job,taskId=metadata['viewerTaskId'],generation=metadata['viewerGeneration'],environment=metadata['environment'],archiveBytes=g['archiveBytes'],archiveSha256=metadata['archiveSha256'],version=2,chunkBytes=g['chunkBytes'],chunkCount=g['chunkCount'])
  return binding,p.canonical_json(binding).decode(),p.digest(dict(kind='synthetic-v2-slot',binding=binding))
 @staticmethod
 def _schema(db,*,create=False):
  rows=db.execute("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE tbl_name='viewer_chunk_v2_slots' ORDER BY type,name").fetchall()
  if not rows:
   if not create:raise SpoolHeld('v2_slot_schema_missing')
   db.execute(DDL)
  expected=sqlite3.connect(':memory:')
  try:
   expected.execute(DDL)
   query="SELECT type,name,tbl_name,sql FROM sqlite_master WHERE tbl_name='viewer_chunk_v2_slots' ORDER BY type,name"
   if [tuple(x) for x in db.execute(query)]!=[tuple(x) for x in expected.execute(query)]:raise SpoolHeld('v2_slot_schema_changed')
  finally:expected.close()
 def prepare(self,job,identity,token):
  with self.spool._admit():
   with self.spool.claims.queue._db() as db:
    db.execute('BEGIN IMMEDIATE');binding,encoded,key=self._binding(db,job,identity,token);SyntheticChunkSpool._admit_operation(db,job)
    phase=db.execute('SELECT phase FROM viewer_chunk_v2_geometry WHERE job_id=?',(job,)).fetchone()['phase']
    self._schema(db,create=phase=='negotiated');row=db.execute('SELECT * FROM viewer_chunk_v2_slots WHERE job_id=?',(job,)).fetchone()
    if row:
     if row['binding']!=encoded or row['slot_key']!=key or row['state'] not in ('ready','sealed') or phase!=row['state'] or not row['slot_identity']:raise SpoolHeld('v2_slot_requires_inspection')
     existing=True
    else:
     if phase!='negotiated':raise SpoolHeld('v2_slot_requires_inspection')
     if db.execute("UPDATE viewer_chunk_v2_geometry SET phase='creating' WHERE job_id=? AND phase='negotiated'",(job,)).rowcount!=1:raise SpoolHeld('v2_slot_changed')
     db.execute("INSERT INTO viewer_chunk_v2_slots VALUES(?,?,?,NULL,'creating')",(job,encoded,key));existing=False
   if existing:
    slot,journal,_,_,state=self._open(job,identity,token)
    try:
     if state=='sealed':journal.finish(binding['chunkCount'])
     return dict(state=state)
    finally:slot.close()
   slot=open_slot(self.spool.root,self.spool.root_identity,key,binding['archiveBytes'],max_budget=self.spool.max_bytes,create=True)
   try:
    SyntheticV2ChunkJournal(slot,binding,legacy_state='new',synthetic_fixture=True,allow_create=True)
    actual=self.spool._slot_identity(slot)
    with self.spool.claims.queue._db() as db:
     db.execute('BEGIN IMMEDIATE');_,current,current_key=self._binding(db,job,identity,token);SyntheticChunkSpool._admit_operation(db,job);self._schema(db)
     if current!=encoded or current_key!=key or db.execute("UPDATE viewer_chunk_v2_slots SET state='ready',slot_identity=? WHERE job_id=? AND state='creating' AND binding=? AND slot_key=?",(actual,job,encoded,key)).rowcount!=1:raise SpoolHeld('v2_slot_changed')
     if db.execute("UPDATE viewer_chunk_v2_geometry SET phase='ready' WHERE job_id=? AND phase='creating'",(job,)).rowcount!=1:raise SpoolHeld('v2_slot_changed')
    return dict(state='ready')
   finally:slot.close()
 def _open(self,job,identity,token,*,admitted=False):
  with self.spool.claims.queue._db() as db:
   db.execute('BEGIN IMMEDIATE');binding,encoded,key=self._binding(db,job,identity,token)
   if not admitted:SyntheticChunkSpool._admit_operation(db,job)
   self._schema(db)
   row=db.execute('SELECT * FROM viewer_chunk_v2_slots WHERE job_id=?',(job,)).fetchone()
   phase=db.execute('SELECT phase FROM viewer_chunk_v2_geometry WHERE job_id=?',(job,)).fetchone()['phase']
   if not row or row['binding']!=encoded or row['slot_key']!=key or row['state'] not in ('ready','sealed') or phase!=row['state'] or not row['slot_identity']:raise SpoolHeld('v2_slot_requires_inspection')
   recorded=row['slot_identity'];state=row['state']
  slot=_open_slot(self.spool.root,self.spool.root_identity,key,binding['archiveBytes'],max_budget=self.spool.max_bytes,allow_chunk_journal=True)
  try:
   if self.spool._slot_identity(slot)!=recorded or not _exists(slot._root,slot._name+'.chunks-v2.header'):raise SpoolHeld('v2_slot_changed')
   return slot,SyntheticV2ChunkJournal(slot,binding,legacy_state='new',synthetic_fixture=True),encoded,key,state
  except BaseException:slot.close();raise
 def _spec(self,binding,index,offset,length,sha):
  p._hash(sha,'chunk sha256')
  if (type(index) is not int or not 0<=index<binding['chunkCount'] or type(offset) is not int
      or offset!=index*binding['chunkBytes'] or type(length) is not int
      or length!=min(binding['chunkBytes'],binding['archiveBytes']-offset)):
   raise SpoolHeld('v2_chunk_geometry_changed')
  return p.canonical_json(dict(version=2,index=index,offset=offset,length=length,sha256=sha)).decode()
 def admit(self,job,identity,token,index,offset,length,sha):
  operation='v2-'+str(index)
  with self.spool.claims.queue._db() as db:
   db.execute('BEGIN IMMEDIATE');binding,encoded,key=self._binding(db,job,identity,token);SyntheticChunkSpool._admit_operation(db,job);self._schema(db)
   row=db.execute('SELECT * FROM viewer_chunk_v2_slots WHERE job_id=?',(job,)).fetchone()
   if not row or row['state']!='ready' or row['binding']!=encoded or row['slot_key']!=key:raise SpoolHeld('v2_slot_requires_inspection')
   spec=self._spec(binding,index,offset,length,sha)
   old=db.execute('SELECT specification,state FROM viewer_worker_chunk_permits WHERE job_id=? AND operation_id=?',(job,operation)).fetchone()
   if old and (old['specification'],old['state'])!=(spec,'finished'):raise SpoolHeld('v2_permit_requires_inspection')
   if not old:db.execute("INSERT INTO viewer_worker_chunk_permits VALUES(?,?,?,'active')",(job,operation,spec))
  if old:
   slot,journal,_,_,_=self._open(job,identity,token)
   try:journal.verify_chunk(index,offset,length,sha)
   finally:slot.close()
   return {'state':'finished'}
  nonce=secrets.token_hex(16)
  with self._lock:self._active[(job,operation)]=nonce
  return {'state':'active','permit':nonce}
 def status(self,job,identity,token,index,offset,length,sha):
  operation='v2-'+str(index)
  with self.spool.claims.queue._db() as db:
   db.execute('BEGIN');binding,_,_=self._binding(db,job,identity,token);self._schema(db);spec=self._spec(binding,index,offset,length,sha)
   row=db.execute('SELECT specification,state FROM viewer_worker_chunk_permits WHERE job_id=? AND operation_id=?',(job,operation)).fetchone()
   if not row:
    if db.execute("SELECT 1 FROM viewer_worker_chunk_permits WHERE job_id=? AND state='active'",(job,)).fetchone():raise SpoolHeld('v2_permit_requires_inspection')
    return {'state':'missing'}
   if (row['specification'],row['state'])!=(spec,'finished'):raise SpoolHeld('v2_permit_requires_inspection')
  with self.spool._admit():
   slot,journal,_,_,_=self._open(job,identity,token)
   try:journal.verify_chunk(index,offset,length,sha);return {'state':'finished'}
   finally:slot.close()
 def abandon(self,job,index,nonce):
  with self._lock:
   key=(job,'v2-'+str(index));current=self._active.get(key)
   if current and isinstance(nonce,str) and secrets.compare_digest(current,nonce):del self._active[key]
 def write_admitted(self,job,identity,token,index,offset,data,nonce):
  operation='v2-'+str(index)
  if os.getpid()!=self._pid or type(data) is not bytes or not 1<=len(data)<=1024*1024:raise SpoolHeld('v2_permit_requires_inspection')
  with self._lock:
   current=self._active.get((job,operation))
   if not current or not isinstance(nonce,str) or not secrets.compare_digest(current,nonce):raise SpoolHeld('v2_permit_requires_inspection')
   del self._active[(job,operation)]
  with self.spool._admit():
   slot,journal,encoded,key,state=self._open(job,identity,token,admitted=True)
   try:
    sha=hashlib.sha256(data).hexdigest();spec=self._spec(journal.binding,index,offset,len(data),sha)
    with self.spool.claims.queue._db() as db:
     db.execute('BEGIN IMMEDIATE');_,current_binding,current_key=self._binding(db,job,identity,token)
     permit=db.execute('SELECT specification,state FROM viewer_worker_chunk_permits WHERE job_id=? AND operation_id=?',(job,operation)).fetchone()
     if current_binding!=encoded or current_key!=key or state!='ready' or not permit or (permit['specification'],permit['state'])!=(spec,'active'):raise SpoolHeld('v2_permit_changed')
    receipt=journal.write(index,offset,data)
    with self.spool.claims.queue._db() as db:
     db.execute('BEGIN IMMEDIATE');_,current_binding,current_key=self._binding(db,job,identity,token)
     if current_binding!=encoded or current_key!=key or db.execute("UPDATE viewer_worker_chunk_permits SET state='finished' WHERE job_id=? AND operation_id=? AND state='active' AND specification=?",(job,operation,spec)).rowcount!=1:raise SpoolHeld('v2_permit_changed')
    return receipt
   finally:slot.close()
 def write(self,job,identity,token,index,offset,data):
  with self.spool._admit():
   slot,journal,_,_,state=self._open(job,identity,token)
   try:
    if state!='ready':raise SpoolHeld('v2_slot_sealed')
    return journal.write(index,offset,data)
   finally:slot.close()
 def finish(self,job,identity,token,count):
  with self.spool._admit():
   slot,journal,encoded,key,state=self._open(job,identity,token)
   try:
    seal=journal.finish(count)
    with self.spool.claims.queue._db() as db:
     db.execute('BEGIN IMMEDIATE');_,current,current_key=self._binding(db,job,identity,token);SyntheticChunkSpool._admit_operation(db,job);self._schema(db)
     if current!=encoded or current_key!=key:raise SpoolHeld('v2_binding_changed')
     row=db.execute('SELECT * FROM viewer_chunk_v2_slots WHERE job_id=?',(job,)).fetchone()
     if not row or row['slot_identity']!=self.spool._slot_identity(slot) or row['state'] not in ('ready','sealed'):raise SpoolHeld('v2_slot_changed')
     if row['state']=='ready':
      if db.execute("UPDATE viewer_chunk_v2_slots SET state='sealed' WHERE job_id=? AND state='ready'",(job,)).rowcount!=1:raise SpoolHeld('v2_slot_changed')
      if db.execute("UPDATE viewer_chunk_v2_geometry SET phase='sealed' WHERE job_id=? AND phase='ready'",(job,)).rowcount!=1:raise SpoolHeld('v2_slot_changed')
    return dict(state='sealed_no_receipt',seal=seal)
   finally:slot.close()
