"""Inactive synthetic evidence staging; separate DB, no authenticated loader."""
import hashlib
import io
import json
import sqlite3
from . import reference_protocol as p
from .gcp_evidence_stream_candidate import verify,EvidenceHeld,DOMAIN,MAX_LINE,MAX_PAGE,MAX_PAGES,MAX_STREAM
from .paged_frame_receiver_candidate import _Rows,_durability

class EvidenceStageHeld(ValueError):pass

def initialize(db):
 if db.in_transaction:raise EvidenceStageHeld('owned_transaction_required')
 began=False
 try:
  db.execute('PRAGMA foreign_keys=ON');db.execute('PRAGMA synchronous=FULL');_durability(db)
  db.execute('BEGIN IMMEDIATE');began=True
  db.execute('CREATE TABLE IF NOT EXISTS candidate_evidence(stage_id TEXT PRIMARY KEY,metadata TEXT NOT NULL)')
  db.execute('CREATE TABLE IF NOT EXISTS candidate_evidence_pages(stage_id TEXT NOT NULL REFERENCES candidate_evidence(stage_id),ordinal INTEGER NOT NULL,wire BLOB NOT NULL,PRIMARY KEY(stage_id,ordinal))')
  db.commit()
 except (ValueError,sqlite3.Error):
  if began and db.in_transaction:db.rollback()
  raise EvidenceStageHeld('evidence_stage_unavailable') from None

def _preflight_metadata(meta):
 p._closed(meta,('statementSha256','attemptId','evidenceSha256','pageHashes','pageBytes','decodedBytes'),'evidence metadata')
 p._hash(meta['statementSha256'],'statement hash');p._uuid(meta['attemptId'],'attempt');p._hash(meta['evidenceSha256'],'evidence hash');p._integer(meta['decodedBytes'],'decoded bytes')
 hashes,sizes=meta['pageHashes'],meta['pageBytes']
 if type(hashes) is not list or type(sizes) is not list or not 2<=len(hashes)<=MAX_PAGES or len(hashes)!=len(sizes):raise ValueError()
 total=0
 for h,n in zip(hashes,sizes):
  p._hash(h,'page hash');p._integer(n,'page bytes')
  if n>MAX_PAGE:raise ValueError()
  total+=n+1
 if total>MAX_STREAM or meta['decodedBytes']>16*1024*1024:raise ValueError()

def _verify_stored(db,stage_id,meta):
 count,total,maximum,bad=db.execute("SELECT COUNT(*),COALESCE(SUM(length(wire)),0),COALESCE(MAX(length(wire)),0),SUM(CASE WHEN typeof(wire)!='blob' THEN 1 ELSE 0 END) FROM candidate_evidence_pages WHERE stage_id=?",(stage_id,)).fetchone()
 if count!=len(meta['pageHashes']) or total+count>MAX_STREAM or maximum>MAX_PAGE or bad:raise ValueError()
 cursor=db.execute('SELECT wire FROM candidate_evidence_pages WHERE stage_id=? ORDER BY ordinal',(stage_id,))
 with io.BufferedReader(_Rows(cursor)) as stream:
  return verify(stream,attempt_id=meta['attemptId'],evidence_sha256=meta['evidenceSha256'],page_hashes=meta['pageHashes'],page_bytes=meta['pageBytes'],decoded_bytes=meta['decodedBytes'])

def stage(db,stage_id,*,metadata,stream,_fault=None):
 """Dedicated DB test candidate; metadata must be independently trusted."""
 if db.in_transaction:raise EvidenceStageHeld('owned_transaction_required')
 began=False
 try:
  _durability(db);p._slug(stage_id,'stage');_preflight_metadata(metadata)
  wire=p.canonical_json(metadata)
  if len(wire)>65536:raise ValueError()
  db.execute('BEGIN IMMEDIATE');began=True
  old=db.execute("SELECT CASE WHEN typeof(metadata)='text' AND instr(metadata,char(0))=0 AND length(metadata)<=65536 THEN metadata END FROM candidate_evidence WHERE stage_id=?",(stage_id,)).fetchone()
  if old is not None:
   if old[0]!=wire.decode():raise ValueError()
   result=_verify_stored(db,stage_id,metadata);db.commit();return dict(result,created=False)
  if type(stream) not in (io.BytesIO,io.BufferedReader) or not stream.readable():raise ValueError()
  db.execute('INSERT INTO candidate_evidence VALUES(?,?)',(stage_id,wire.decode()))
  for index,(expected_hash,expected_size) in enumerate(zip(metadata['pageHashes'],metadata['pageBytes'])):
   line=stream.readline(MAX_LINE+1)
   if type(line) is not bytes or not line.endswith(b'\n') or len(line)>MAX_LINE:raise ValueError()
   raw=line[:-1]
   if len(raw)!=expected_size or hashlib.sha256(DOMAIN+raw).hexdigest()!=expected_hash:raise ValueError()
   db.execute('INSERT INTO candidate_evidence_pages VALUES(?,?,?)',(stage_id,index,raw))
   if _fault:_fault(index)
  if stream.read(1)!=b'':raise ValueError()
  result=_verify_stored(db,stage_id,metadata);db.commit();return dict(result,created=True)
 except (ValueError,TypeError,KeyError,sqlite3.Error,OSError,OverflowError,UnicodeError):
  if began and db.in_transaction:db.rollback()
  raise EvidenceStageHeld('evidence_stage_unavailable') from None
