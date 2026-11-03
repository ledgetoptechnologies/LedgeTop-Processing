"""Inactive transactional staging candidate; never use with a live DB or route."""
import hashlib
import io
import json
import sqlite3
from . import reference_protocol as p
from .viewer_import_confirmation_store import _preflight_stored_json
from .viewer_paged_frame_candidate import VERSION as SOURCE_VERSION,DOMAIN as SOURCE_DOMAIN,MAX_FRAME_BYTES,MAX_TOTAL_BYTES,MAX_FRAMES,_pairs
from .viewer_selected_frames_candidate import VERSION as SELECTED_VERSION,DOMAIN as SELECTED_DOMAIN,verify,binding_digest
from .large_paged_manifest_candidate import validate as validate_pages

class ReceiverHeld(ValueError):pass

def _durability(db):
 mode=db.execute('PRAGMA journal_mode').fetchone()[0].lower()
 sync=db.execute('PRAGMA synchronous').fetchone()[0]
 attached=db.execute('PRAGMA database_list').fetchall()
 if mode not in ('delete','truncate','persist','wal') or type(sync) is not int or sync<2 or len(attached)!=1 or not attached[0][2]:raise ValueError()

def _preflight_binding(binding):
 p._closed(binding,('datasetId','datasetManifestSha256','principalId','environment','authorityId','viewerTaskId','viewerAttemptId','pipelineDigest','revocationEpoch','sourcePageHashes','sourceCount','selectedPageHashes','selectedCount','inputManifestSha256','processingClosureSha256'),'binding')
 for key in ('datasetId','principalId','authorityId'):p._slug(binding[key],key)
 for key in ('datasetManifestSha256','pipelineDigest','inputManifestSha256','processingClosureSha256'):p._hash(binding[key],key)
 p._environment(binding['environment']);p._uuid(binding['viewerTaskId'],'task');p._uuid(binding['viewerAttemptId'],'attempt');p._integer(binding['revocationEpoch'],'epoch',allow_zero=True)
 for key in ('sourceCount','selectedCount'):
  p._integer(binding[key],key)
  if binding[key]>20000:raise ValueError()
 for key in ('sourcePageHashes','selectedPageHashes'):
  if type(binding[key]) is not list or not 0<len(binding[key])<=MAX_FRAMES:raise ValueError()
  for value in binding[key]:p._hash(value,key)

def initialize(db):
 """Initialize a dedicated synthetic database; ownership/auth are caller concerns."""
 if db.in_transaction:raise ReceiverHeld('dedicated_database_required')
 began_transaction=False
 try:
  db.execute('PRAGMA foreign_keys=ON')
  db.execute('PRAGMA synchronous=FULL')
  _durability(db)
  db.execute('BEGIN IMMEDIATE');began_transaction=True
  db.execute('CREATE TABLE IF NOT EXISTS candidate_frame_receipts(stage_id TEXT PRIMARY KEY,binding_pin TEXT NOT NULL,binding_json TEXT NOT NULL,closure_json TEXT NOT NULL)')
  db.execute('CREATE TABLE IF NOT EXISTS candidate_frame_pages(stage_id TEXT NOT NULL REFERENCES candidate_frame_receipts(stage_id),kind TEXT NOT NULL,ordinal INTEGER NOT NULL,wire BLOB NOT NULL,PRIMARY KEY(stage_id,kind,ordinal))')
  db.commit()
 except (ValueError,sqlite3.Error):
  if began_transaction and db.in_transaction:db.rollback()
  raise ReceiverHeld('durable_database_required') from None

class _Rows(io.RawIOBase):
 def __init__(self,cursor):self.cursor=cursor;self.pending=b''
 def readable(self):return True
 def readinto(self,target):
  if not self.pending:
   row=self.cursor.fetchone()
   if row is None:return 0
   self.pending=bytes(row[0])+b'\n'
  n=min(len(target),len(self.pending));target[:n]=self.pending[:n];self.pending=self.pending[n:];return n

def _bounded_frames(db,stage_id,kind,stream,pins,version,domain,fault):
 if type(stream) not in (io.BytesIO,io.BufferedReader) or not stream.readable():raise ValueError()
 if type(pins) is not list or not 0<len(pins)<=MAX_FRAMES:raise ValueError()
 total=0
 for index,pin in enumerate(pins):
  p._hash(pin,'page pin');line=stream.readline(MAX_FRAME_BYTES+2)
  if type(line) is not bytes or not line.endswith(b'\n') or len(line)>MAX_FRAME_BYTES+1:raise ValueError()
  raw=line[:-1];total+=len(line)
  if total>MAX_TOTAL_BYTES:raise ValueError()
  _preflight_stored_json(raw)
  obj=json.loads(raw.decode('utf8'),object_pairs_hook=_pairs,parse_constant=lambda _:(_ for _ in ()).throw(ValueError()))
  p._closed(obj,('version','index','records'),'frame')
  if obj['version']!=version or type(obj['index']) is not int or obj['index']!=index or hashlib.sha256(domain+raw).hexdigest()!=pin or p.canonical_json(obj)!=raw:raise ValueError()
  db.execute('INSERT INTO candidate_frame_pages VALUES(?,?,?,?)',(stage_id,kind,index,raw))
  if fault:fault(kind,index)
 if stream.read(1)!=b'':raise ValueError()

def _preflight_stored_pages(db,stage_id):
 for kind in ('source','selected','processing'):
  row=db.execute("SELECT COUNT(*),COALESCE(SUM(length(wire)),0),COALESCE(MAX(length(wire)),0),SUM(CASE WHEN typeof(wire)!='blob' THEN 1 ELSE 0 END) FROM candidate_frame_pages WHERE stage_id=? AND kind=?",(stage_id,kind)).fetchone()
  if not 0<row[0]<=MAX_FRAMES or row[1]>MAX_TOTAL_BYTES or row[2]>MAX_FRAME_BYTES or row[3]:raise ValueError()
 if db.execute("SELECT 1 FROM candidate_frame_pages WHERE stage_id=? AND kind NOT IN ('source','selected','processing') LIMIT 1",(stage_id,)).fetchone():raise ValueError()

def _validated(db,stage_id,binding,pin,closure,processing_pages):
 def source(kind):
  cursor=db.execute('SELECT wire FROM candidate_frame_pages WHERE stage_id=? AND kind=? ORDER BY ordinal',(stage_id,kind))
  return io.BufferedReader(_Rows(cursor))
 with source('source') as left,source('selected') as right:
  return verify(left,right,source_hashes=binding['sourcePageHashes'],source_count=binding['sourceCount'],selected_hashes=binding['selectedPageHashes'],input_manifest_sha256=binding['inputManifestSha256'],processing_closure=closure,processing_pages=processing_pages,closure_sha256=binding['processingClosureSha256'],trusted_binding=binding,expected_binding_sha256=pin)

def stage(db,stage_id,*,trusted_binding,expected_binding_sha256,source_stream,selected_stream,processing_closure,processing_pages,_fault=None):
 """Transactional candidate only; trusted_binding has NO authentication here."""
 if db.in_transaction:raise ReceiverHeld('staging_unavailable')
 began_transaction=False
 try:
  _durability(db);_preflight_binding(trusted_binding)
  p._slug(stage_id,'stage ID');p._hash(expected_binding_sha256,'binding pin')
  # Snapshot/pin are application-owned and immutable; wire decoder not supplied.
  if binding_digest(trusted_binding)!=expected_binding_sha256:raise ValueError()
  validate_pages(processing_closure,processing_pages,expected_closure_sha256=trusted_binding['processingClosureSha256'])
  binding_wire=p.canonical_json(trusted_binding);closure_wire=p.canonical_json(processing_closure)
  if len(binding_wire)>65536 or len(closure_wire)>65536:raise ValueError()
  db.execute('BEGIN IMMEDIATE');began_transaction=True
  old=db.execute("SELECT CASE WHEN typeof(binding_pin)='text' AND instr(binding_pin,char(0))=0 AND length(binding_pin)=64 THEN binding_pin END, CASE WHEN typeof(binding_json)='text' AND instr(binding_json,char(0))=0 AND length(binding_json)<=65536 THEN binding_json END, CASE WHEN typeof(closure_json)='text' AND instr(closure_json,char(0))=0 AND length(closure_json)<=65536 THEN closure_json END FROM candidate_frame_receipts WHERE stage_id=?",(stage_id,)).fetchone()
  if old:
   _preflight_stored_pages(db,stage_id)
   if tuple(old)!=(expected_binding_sha256,binding_wire.decode(),closure_wire.decode()):raise ValueError()
   # Persisted processing pages are reloaded and revalidated on every replay.
   processing_pages=[]
   for row in db.execute("SELECT wire FROM candidate_frame_pages WHERE stage_id=? AND kind='processing' ORDER BY ordinal",(stage_id,)):
    raw=bytes(row[0])
    if not 0<len(raw)<=MAX_FRAME_BYTES:raise ValueError()
    _preflight_stored_json(raw);processing_pages.append(json.loads(raw.decode('utf8'),object_pairs_hook=_pairs))
   if not 0<len(processing_pages)<=MAX_FRAMES:raise ValueError()
   result=_validated(db,stage_id,trusted_binding,expected_binding_sha256,processing_closure,processing_pages)
   db.commit();return dict(result,created=False)
  db.execute('INSERT INTO candidate_frame_receipts VALUES(?,?,?,?)',(stage_id,expected_binding_sha256,binding_wire.decode(),closure_wire.decode()))
  for index,page in enumerate(processing_pages):
   raw=p.canonical_json(page)
   if len(raw)>MAX_FRAME_BYTES:raise ValueError()
   db.execute('INSERT INTO candidate_frame_pages VALUES(?,?,?,?)',(stage_id,'processing',index,raw))
  _bounded_frames(db,stage_id,'source',source_stream,trusted_binding['sourcePageHashes'],SOURCE_VERSION,SOURCE_DOMAIN,_fault)
  _bounded_frames(db,stage_id,'selected',selected_stream,trusted_binding['selectedPageHashes'],SELECTED_VERSION,SELECTED_DOMAIN,_fault)
  _preflight_stored_pages(db,stage_id)
  result=_validated(db,stage_id,trusted_binding,expected_binding_sha256,processing_closure,processing_pages)
  db.commit();return dict(result,created=True)
 except (sqlite3.Error,OSError,ValueError,TypeError,KeyError,UnicodeError,OverflowError,AttributeError):
  if began_transaction and db.in_transaction:db.rollback()
  raise ReceiverHeld('staging_unavailable') from None
