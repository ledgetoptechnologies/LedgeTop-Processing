"""Fixture-only v2 coordinator slot and journal; not a transport or receipt."""
import json,sqlite3
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
  self.spool=spool
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
 def _open(self,job,identity,token):
  with self.spool.claims.queue._db() as db:
   db.execute('BEGIN IMMEDIATE');binding,encoded,key=self._binding(db,job,identity,token);SyntheticChunkSpool._admit_operation(db,job);self._schema(db)
   row=db.execute('SELECT * FROM viewer_chunk_v2_slots WHERE job_id=?',(job,)).fetchone()
   phase=db.execute('SELECT phase FROM viewer_chunk_v2_geometry WHERE job_id=?',(job,)).fetchone()['phase']
   if not row or row['binding']!=encoded or row['slot_key']!=key or row['state'] not in ('ready','sealed') or phase!=row['state'] or not row['slot_identity']:raise SpoolHeld('v2_slot_requires_inspection')
   recorded=row['slot_identity'];state=row['state']
  slot=_open_slot(self.spool.root,self.spool.root_identity,key,binding['archiveBytes'],max_budget=self.spool.max_bytes,allow_chunk_journal=True)
  try:
   if self.spool._slot_identity(slot)!=recorded or not _exists(slot._root,slot._name+'.chunks-v2.header'):raise SpoolHeld('v2_slot_changed')
   return slot,SyntheticV2ChunkJournal(slot,binding,legacy_state='new',synthetic_fixture=True),encoded,key,state
  except BaseException:slot.close();raise
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
