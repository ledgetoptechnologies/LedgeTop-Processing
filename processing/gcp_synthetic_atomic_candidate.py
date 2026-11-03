"""Synthetic-only same-transaction GCP experiment; NEVER an authority boundary.

Input is already allocated/bounded test data and trust is fixture-injected. Not
mounted or called by coordinator journal; no execution, cleanup, or broker use.
"""
import base64,hashlib,io,json,sqlite3
from . import reference_protocol as p
from . import viewer_paged_frame_candidate as source
from . import viewer_selected_frames_candidate as selected
from . import gcp_evidence_stream_candidate as evidence
from . import gcp_file_stream_candidate as file
from .gcp_cumulative_quota_candidate import validate_wire_quota
from .gcp_semantic_bridge_candidate import inspect
from .source_inventory_candidate import _parse
from .viewer_import_confirmation_store import _preflight_stored_json

class SyntheticHeld(ValueError):pass

def initialize(db):
 if db.in_transaction:raise SyntheticHeld('transaction active')
 db.execute('CREATE TABLE synthetic_gcp_bundle (id TEXT PRIMARY KEY, statement BLOB NOT NULL, source BLOB NOT NULL, selection BLOB NOT NULL, evidence BLOB, file BLOB)')

def _pages(wire, hashes, sizes, domain):
 if type(wire) is not bytes or type(hashes) is not list or type(sizes) is not list or len(hashes)!=len(sizes):raise ValueError()
 lines=wire.splitlines(keepends=True)
 if len(lines)!=len(hashes):raise ValueError()
 records=[]
 for i,(line,h,n) in enumerate(zip(lines,hashes,sizes)):
  if not line.endswith(b'\n') or type(n) is not int or len(line)!=n+1 or hashlib.sha256(domain+line[:-1]).hexdigest()!=h:raise ValueError()
  _preflight_stored_json(line[:-1]);obj=json.loads(line[:-1]);
  if p.canonical_json(obj)!=line[:-1] or type(obj.get('index')) is not int or obj['index']!=i:raise ValueError()
  records.extend(obj['records'])
 return records

def _decoded(wire):
 rows=wire.splitlines();return b''.join(base64.urlsafe_b64decode((obj['data']+'='*((-len(obj['data']))%4)).encode()) for obj in (json.loads(x) for x in rows[:-1]))

def stage_owned(db,*,bundle_id,statement,streams,trusted_images,trusted_correspondences,trusted_binding,expected_binding_sha256,processing_closure,processing_pages,_fault=None):
 """Caller owns transaction and MUST rollback on any error; not production code."""
 if not db.in_transaction:raise SyntheticHeld('owned transaction required')
 try:
  p._closed(statement,('version','attemptId','sourceCount','sourcePageHashes','sourcePageBytes','selectedPageHashes','selectedPageBytes','gcpEvidenceVersion','gcpEvidencePageHashes','gcpEvidencePageBytes','gcpEvidencePageCount','gcpEvidenceByteCount','gcpEvidenceSha256','gcpFileVersion','gcpFilePageHashes','gcpFilePageBytes','gcpFilePageCount','gcpFileByteCount','gcpFileSha256'),'statement')
  if statement['version']!='synthetic-gcp-combined-v1':raise ValueError()
  p._slug(bundle_id,'bundle');p._uuid(statement['attemptId'],'attempt')
  p._closed(streams,('source','selection','evidence','file'),'streams')
  src,sel,ev,fp=(streams[k] for k in ('source','selection','evidence','file'))
  if type(src) is not bytes or type(sel) is not bytes or len(src)>12*1024*1024 or len(sel)>12*1024*1024:raise ValueError()
  if type(ev) is not bytes or type(fp) is not bytes or len(ev)>24*1024*1024 or len(fp)>24*1024*1024:raise ValueError()
  if statement['gcpEvidenceVersion']!=evidence.VERSION or statement['gcpFileVersion']!=file.VERSION:raise ValueError()
  for prefix in ('gcpEvidence','gcpFile'):
   if type(statement[prefix+'PageCount']) is not int or statement[prefix+'PageCount']!=len(statement[prefix+'PageHashes']):raise ValueError()
  validate_wire_quota(source_bytes=len(src),selected_bytes=len(sel),evidence_page_bytes=statement['gcpEvidencePageBytes'],file_page_bytes=statement['gcpFilePageBytes'])
  raw=p.canonical_json(statement)
  if len(raw)>65536:raise ValueError()
  if db.execute('SELECT 1 FROM synthetic_gcp_bundle WHERE id=?',(bundle_id,)).fetchone():raise ValueError('replay must be handled by journal')
  db.execute('INSERT INTO synthetic_gcp_bundle VALUES(?,?,?,?,NULL,NULL)',(bundle_id,raw,src,sel))
  if _fault:_fault('source')
  source.verify_dataset_count_v2(io.BytesIO(src),expected_page_hashes=statement['sourcePageHashes'],expected_dataset_items=statement['sourceCount'])
  sources=_pages(src,statement['sourcePageHashes'],statement['sourcePageBytes'],source.DOMAIN)
  selections=_pages(sel,statement['selectedPageHashes'],statement['selectedPageBytes'],selected.DOMAIN)
  # Independent trusted fixture controls selection, input hash, and closure;
  # production has no such loader yet. All remain within the caller transaction.
  if (trusted_binding['viewerAttemptId']!=statement['attemptId'] or trusted_binding['sourceCount']!=statement['sourceCount']
      or trusted_binding['sourcePageHashes']!=statement['sourcePageHashes'] or trusted_binding['selectedPageHashes']!=statement['selectedPageHashes']):raise ValueError()
  selected.verify_dataset_count_v2(io.BytesIO(src),io.BytesIO(sel),source_hashes=statement['sourcePageHashes'],source_count=statement['sourceCount'],selected_hashes=statement['selectedPageHashes'],input_manifest_sha256=trusted_binding['inputManifestSha256'],processing_closure=processing_closure,processing_pages=processing_pages,closure_sha256=trusted_binding['processingClosureSha256'],trusted_binding=trusted_binding,expected_binding_sha256=expected_binding_sha256)
  if _fault:_fault('selection')
  evidence.verify(io.BytesIO(ev),attempt_id=statement['attemptId'],evidence_sha256=statement['gcpEvidenceSha256'],page_hashes=statement['gcpEvidencePageHashes'],page_bytes=statement['gcpEvidencePageBytes'],decoded_bytes=statement['gcpEvidenceByteCount'])
  db.execute('UPDATE synthetic_gcp_bundle SET evidence=? WHERE id=?',(ev,bundle_id))
  if _fault:_fault('evidence')
  file.verify(io.BytesIO(fp),attempt_id=statement['attemptId'],evidence_sha256=statement['gcpEvidenceSha256'],file_sha256=statement['gcpFileSha256'],page_hashes=statement['gcpFilePageHashes'],page_bytes=statement['gcpFilePageBytes'],decoded_bytes=statement['gcpFileByteCount'])
  db.execute('UPDATE synthetic_gcp_bundle SET file=? WHERE id=?',(fp,bundle_id))
  if _fault:_fault('file')
  evbytes=_decoded(ev);fbytes=_decoded(fp);parsed=_parse(evbytes)
  inspect(attempt_id=statement['attemptId'],evidence=parsed,evidence_bytes=evbytes,evidence_sha256=statement['gcpEvidenceSha256'],generated_bytes=fbytes,source_records=sources,selected_records=selections,trusted_images=trusted_images,trusted_correspondences=trusted_correspondences)
  if _fault:_fault('semantic')
  return {'state':'held_synthetic_only'}
 except (ValueError,TypeError,KeyError,sqlite3.Error,UnicodeError,AttributeError):raise SyntheticHeld('synthetic_bundle_unavailable') from None
