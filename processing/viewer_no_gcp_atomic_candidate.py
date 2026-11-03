"""Synthetic no-GCP single-DB prototype; no auth or execution authority."""
import io,json,sqlite3
from . import reference_protocol as p
from . import viewer_dual_digest_binding_candidate as binding
from . import paged_frame_receiver_candidate as frames
from .viewer_import_confirmation_store import _preflight_stored_json
from .viewer_paged_frame_candidate import _pairs
from .paged_snapshot_journal_candidate import _check_frame_schema, FRAME_MARKER_DDL, FRAME_RECEIPTS_DDL, FRAME_PAGES_DDL

class NoGcpHeld(ValueError):pass

def _history(db):
 # Refuse oversized histories before joins; each bounded census scans at most
 # its cap plus one and no client row payloads are materialized.
 for table,cap in (('synthetic_viewer_dataset_transport_v1',10000),('candidate_frame_receipts',10000),('candidate_frame_pages',100000)):
  if db.execute('SELECT count(*) FROM (SELECT 1 FROM '+table+' LIMIT ?)',(cap+1,)).fetchone()[0]>cap:raise ValueError('candidate history too large')
 # Existence probes use primary-key joins and materialize at most one row.
 for query in (
  "SELECT 1 FROM candidate_frame_pages p LEFT JOIN candidate_frame_receipts r ON r.stage_id=p.stage_id WHERE r.stage_id IS NULL LIMIT 1",
  "SELECT 1 FROM candidate_frame_receipts r LEFT JOIN synthetic_viewer_dataset_transport_v1 b ON b.job_id=r.stage_id WHERE b.job_id IS NULL LIMIT 1",
  "SELECT 1 FROM synthetic_viewer_dataset_transport_v1 b LEFT JOIN candidate_frame_receipts r ON r.stage_id=b.job_id WHERE r.stage_id IS NULL LIMIT 1",
  "SELECT 1 FROM candidate_frame_receipts r WHERE NOT EXISTS (SELECT 1 FROM candidate_frame_pages p WHERE p.stage_id=r.stage_id AND p.kind='source') OR NOT EXISTS (SELECT 1 FROM candidate_frame_pages p WHERE p.stage_id=r.stage_id AND p.kind='selected') OR NOT EXISTS (SELECT 1 FROM candidate_frame_pages p WHERE p.stage_id=r.stage_id AND p.kind='processing') LIMIT 1"):
  if db.execute(query).fetchone():raise ValueError('unmatched candidate history')

def initialize(db):
 """Create both synthetic namespaces together or reject partial history."""
 if db.in_transaction:raise NoGcpHeld('synthetic_schema_unavailable')
 try:
  db.execute('PRAGMA foreign_keys=ON');db.execute('BEGIN IMMEDIATE')
  names={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE name IN ('synthetic_viewer_dataset_transport_v1','synthetic_viewer_dataset_transport_marker_v1','paged_snapshot_frame_marker_v1','candidate_frame_receipts','candidate_frame_pages')")}
  if not names:
   for ddl in (binding.DDL,binding.MARKER,FRAME_MARKER_DDL,FRAME_RECEIPTS_DDL,FRAME_PAGES_DDL):db.execute(ddl)
   db.execute('INSERT INTO synthetic_viewer_dataset_transport_marker_v1 VALUES(1,1)');db.execute('INSERT INTO paged_snapshot_frame_marker_v1 VALUES(1,1)')
  elif len(names)!=5:raise ValueError('partial candidate history')
  binding._schema(db);_check_frame_schema(db);_history(db);db.commit()
 except (ValueError,sqlite3.Error):
  if db.in_transaction:db.rollback()
  raise NoGcpHeld('synthetic_schema_unavailable') from None

def stage(store,submission_id,statement,*,principal,environment,configured_authority,synthetic_viewer_provenance,synthetic_bundle,_fault=None):
 try:
  p._closed(synthetic_bundle,('binding','bindingSha256','source','selection','processingClosure','processingPages'),'synthetic bundle')
  if any(statement[k] is not None for k in binding.PROVENANCE_FIELDS if k.startswith(('gcpEvidence','gcpFile'))):raise ValueError()
  src,sel=synthetic_bundle['source'],synthetic_bundle['selection']
  if type(src) is not bytes or type(sel) is not bytes or not 0<len(src)<=12*1024*1024 or not 0<len(sel)<=12*1024*1024:raise ValueError()
  trusted=synthetic_bundle['binding'];expected=dict(datasetId=statement['viewerDatasetId'],datasetManifestSha256=statement['viewerDatasetManifestSha256'],principalId=principal,environment=environment,authorityId=configured_authority,viewerTaskId=statement['viewerTaskId'],viewerAttemptId=statement['viewerAttemptId'],pipelineDigest=statement['pipelineDigest'],sourcePageHashes=statement['sourcePageHashes'],sourceCount=statement['sourceCount'],selectedPageHashes=statement['selectedPageHashes'],selectedCount=statement['selectedCount'],inputManifestSha256=statement['viewerInputManifestSha256'])
  if type(trusted) is not dict or any(trusted.get(k)!=v for k,v in expected.items()):raise ValueError()
  # Bounded prescan before any frame array allocation or database writes.
  scan=io.BytesIO(sel);index=0
  while True:
   line=scan.readline(frames.MAX_FRAME_BYTES+2)
   if not line:break
   if index>=frames.MAX_FRAMES or len(line)>frames.MAX_FRAME_BYTES+1 or not line.endswith(b'\n'):raise ValueError()
   raw=line[:-1];_preflight_stored_json(raw)
   f=json.loads(raw,object_pairs_hook=_pairs,parse_constant=lambda _:(_ for _ in ()).throw(ValueError()))
   p._closed(f,('version','index','records'),'selected frame')
   if f['version']!=frames.SELECTED_VERSION or type(f['index']) is not int or f['index']!=index or type(f['records']) is not list or not 0<len(f['records'])<=512 or p.canonical_json(f)!=raw:raise ValueError()
   for obj in f['records']:
    if type(obj) is not dict or obj.get('type')!='dataset_file' or obj.get('pageObjectId')!=obj.get('datasetFileId'):raise ValueError()
   index+=1
  if not index:raise ValueError()
  with store.queue._db() as db:
   db.execute('PRAGMA foreign_keys=ON')
   if db.execute('PRAGMA foreign_keys').fetchone()[0]!=1:raise ValueError()
   db.execute('BEGIN IMMEDIATE');_check_frame_schema(db)
   fk=[tuple(r) for r in db.execute('PRAGMA foreign_key_list(candidate_frame_pages)')]
   if fk!=[(0,0,'candidate_frame_receipts','stage_id','stage_id','NO ACTION','NO ACTION','NONE')]:raise ValueError()
   if list(db.execute('PRAGMA foreign_key_list(candidate_frame_receipts)')) or list(db.execute('PRAGMA foreign_key_list(paged_snapshot_frame_marker_v1)')) or list(db.execute('PRAGMA index_list(paged_snapshot_frame_marker_v1)')):raise ValueError()
   binding._schema(db);_history(db)
   result=binding._stage_owned(db,store,submission_id,statement,principal=principal,environment=environment,configured_authority=configured_authority,synthetic_viewer_provenance=synthetic_viewer_provenance)
   if result is None:return None
   if _fault:_fault('binding')
   row=store._row(db,statement['jobId']);transport=json.loads(row['input_json'])['manifest']['objects']
   objects=[o for page in synthetic_bundle['processingPages'] for o in page['body']['objects']]
   if transport!=objects:raise ValueError()
   stage_id=statement['jobId']
   if not result['created']:
    # SQL aggregate preflight precedes any BLOB materialization; replay then
    # compares one bounded row at a time rather than joining the stored corpus.
    frames._preflight_stored_pages(db,stage_id)
    for kind,raw in (('source',src),('selected',sel)):
     stream=io.BytesIO(raw)
     for r in db.execute('SELECT wire FROM candidate_frame_pages WHERE stage_id=? AND kind=? ORDER BY ordinal',(stage_id,kind)):
      value=r[0]
      if type(value) is not bytes or stream.readline(frames.MAX_FRAME_BYTES+2)!=value+b'\n':raise ValueError()
     if stream.read(1)!=b'':raise ValueError()
   def on_page(kind,index):
    if _fault:_fault(kind)
   r=frames._stage_owned(db,stage_id,trusted_binding=trusted,expected_binding_sha256=synthetic_bundle['bindingSha256'],source_stream=io.BytesIO(src),selected_stream=io.BytesIO(sel),processing_closure=synthetic_bundle['processingClosure'],processing_pages=synthetic_bundle['processingPages'],_fault=on_page)
   if _fault:_fault('verified')
   for kind,prefix in (('source','source'),('selected','selected')):
    sizes=[r[0] for r in db.execute('SELECT length(wire) FROM candidate_frame_pages WHERE stage_id=? AND kind=? ORDER BY ordinal',(stage_id,kind))]
    if sizes!=statement[prefix+'PageBytes']:raise ValueError()
   return {'state':'held_synthetic_no_gcp','created':result['created']}
 except (ValueError,TypeError,KeyError,sqlite3.Error,UnicodeError,RuntimeError,AttributeError):raise NoGcpHeld('synthetic_no_gcp_unavailable') from None
