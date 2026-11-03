"""Unmounted synthetic single-DB binding+GCP bundle; not signed or authorized."""
import io,json,sqlite3
from . import reference_protocol as p
from . import viewer_dual_digest_binding_candidate as binding
from . import gcp_synthetic_atomic_candidate as gcp
from .viewer_import_confirmation_store import _preflight_stored_json
from .viewer_paged_frame_candidate import _pairs

TABLE='CREATE TABLE synthetic_gcp_bundle (id TEXT PRIMARY KEY, statement BLOB NOT NULL, source BLOB NOT NULL, selection BLOB NOT NULL, evidence BLOB, file BLOB)'
MARKER='CREATE TABLE synthetic_dual_gcp_marker (singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL CHECK(version=1))'
class AtomicHeld(ValueError):pass


def _strict_records(raw):
 if type(raw) is not bytes or not 0<len(raw)<=12*1024*1024:raise ValueError()
 stream=io.BytesIO(raw)
 for index in range(65):
  line=stream.readline(262146)
  if not line:return
  if index>=64 or not line.endswith(b'\n') or len(line)>262145:raise ValueError()
  payload=line[:-1];_preflight_stored_json(payload)
  frame=json.loads(payload.decode('utf8'),object_pairs_hook=_pairs,parse_constant=lambda _:(_ for _ in ()).throw(ValueError()))
  p._closed(frame,('version','index','records'),'frame')
  if type(frame['index']) is not int or frame['index']!=index or type(frame['records']) is not list or not 0<len(frame['records'])<=512 or p.canonical_json(frame)!=payload:raise ValueError()
  yield from frame['records']


def _schema(db):
 for name,ddl in (('synthetic_gcp_bundle',TABLE),('synthetic_dual_gcp_marker',MARKER)):
  row=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?",(name,)).fetchone()
  if row is None or row[0]!=ddl or db.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' AND tbl_name=? LIMIT 1",(name,)).fetchone():raise ValueError()
 if [tuple(x) for x in db.execute('SELECT singleton,version FROM synthetic_dual_gcp_marker LIMIT 2')]!=[(1,1)]:raise ValueError()
 indices=db.execute('PRAGMA index_list(synthetic_gcp_bundle)').fetchall()
 if len(indices)!=1 or indices[0][2]!=1 or indices[0][3]!='pk':raise ValueError()


def initialize(db):
 if db.in_transaction:raise AtomicHeld('schema_unavailable')
 db.execute('BEGIN IMMEDIATE')
 try:
  names={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE name IN ('synthetic_gcp_bundle','synthetic_dual_gcp_marker')")}
  if not names:
   if db.execute("SELECT 1 FROM sqlite_master WHERE name='synthetic_viewer_dataset_transport_v1'").fetchone() and db.execute('SELECT 1 FROM synthetic_viewer_dataset_transport_v1 LIMIT 1').fetchone():raise ValueError()
   db.execute(TABLE);db.execute(MARKER);db.execute('INSERT INTO synthetic_dual_gcp_marker VALUES(1,1)')
  elif len(names)!=2:raise ValueError()
  _schema(db);db.commit()
 except (ValueError,sqlite3.Error):db.rollback();raise AtomicHeld('schema_unavailable') from None


def stage(store,submission_id,statement,*,principal,environment,configured_authority,synthetic_viewer_provenance,synthetic_bundle,_fault=None):
 """All trust is injected synthetic test data; return held, never dispatch."""
 try:
  p._closed(synthetic_bundle,('statement','streams','trusted_images','trusted_correspondences','trusted_binding','expected_binding_sha256','processing_closure','processing_pages'),'synthetic bundle')
  spec=synthetic_bundle['statement'];p._closed(spec,('version','attemptId','sourceCount','sourcePageHashes','sourcePageBytes','selectedPageHashes','selectedPageBytes','gcpEvidenceVersion','gcpEvidencePageHashes','gcpEvidencePageBytes','gcpEvidencePageCount','gcpEvidenceByteCount','gcpEvidenceSha256','gcpFileVersion','gcpFilePageHashes','gcpFilePageBytes','gcpFilePageCount','gcpFileByteCount','gcpFileSha256'),'synthetic statement')
  for key in ('sourceCount','sourcePageHashes','sourcePageBytes','selectedPageHashes','selectedPageBytes','gcpEvidenceVersion','gcpEvidencePageHashes','gcpEvidencePageBytes','gcpEvidencePageCount','gcpEvidenceByteCount','gcpEvidenceSha256','gcpFileVersion','gcpFilePageHashes','gcpFilePageBytes','gcpFilePageCount','gcpFileByteCount','gcpFileSha256'):
   if spec[key]!=statement[key]:raise ValueError()
  if spec['attemptId']!=statement['viewerAttemptId']:raise ValueError()
  streams=synthetic_bundle['streams'];p._closed(streams,('source','selection','evidence','file'),'streams')
  if type(streams['selection']) is not bytes or not 0<len(streams['selection'])<=12*1024*1024:raise ValueError()
  if type(streams['source']) is not bytes or not 0<len(streams['source'])<=12*1024*1024:raise ValueError()
  generated_id='gcp_'+statement['viewerAttemptId']
  for record in _strict_records(streams['source']):
   if type(record) is not dict:raise ValueError()
   if record.get('type')=='dataset_file' and record.get('datasetFileId')==generated_id:raise ValueError('generated ID collides with dataset')
  seen=set()
  for record in _strict_records(streams['selection']):
   if record['type']=='dataset_file':expected_id=record['datasetFileId']
   elif record['type']=='attempt_generated_gcp':
    expected_id=generated_id
    if record['attemptId']!=statement['viewerAttemptId'] or record['mediaType']!='text/plain':raise ValueError()
   else:raise ValueError()
   if record['pageObjectId']!=expected_id or expected_id in seen:raise ValueError('Viewer object mapping mismatch')
   seen.add(expected_id)
  trusted=synthetic_bundle['trusted_binding']
  expected=dict(datasetId=statement['viewerDatasetId'],datasetManifestSha256=statement['viewerDatasetManifestSha256'],principalId=principal,environment=environment,authorityId=configured_authority,viewerTaskId=statement['viewerTaskId'],viewerAttemptId=statement['viewerAttemptId'],pipelineDigest=statement['pipelineDigest'],sourcePageHashes=statement['sourcePageHashes'],sourceCount=statement['sourceCount'],selectedPageHashes=statement['selectedPageHashes'],selectedCount=statement['selectedCount'],inputManifestSha256=statement['viewerInputManifestSha256'])
  if type(trusted) is not dict or any(trusted.get(k)!=v for k,v in expected.items()):raise ValueError()
  with store.queue._db() as db:
   db.execute('BEGIN IMMEDIATE');_schema(db)
   result=binding._stage_owned(db,store,submission_id,statement,principal=principal,environment=environment,configured_authority=configured_authority,synthetic_viewer_provenance=synthetic_viewer_provenance)
   if result is None:return None
   row=store._row(db,statement['jobId']);transport=json.loads(row['input_json'])['manifest']['objects']
   selected_objects=[obj for page in synthetic_bundle['processing_pages'] for obj in page['body']['objects']]
   if transport!=selected_objects:raise ValueError('selected source does not match persisted transport')
   if _fault:_fault('binding')
   job=statement['jobId']
   if not result['created']:
    old=db.execute("SELECT CASE WHEN typeof(statement)='blob' AND length(statement)<=65536 THEN statement END,CASE WHEN typeof(source)='blob' AND length(source)<=12582912 THEN source END,CASE WHEN typeof(selection)='blob' AND length(selection)<=12582912 THEN selection END,CASE WHEN typeof(evidence)='blob' AND length(evidence)<=25165824 THEN evidence END,CASE WHEN typeof(file)='blob' AND length(file)<=25165824 THEN file END FROM synthetic_gcp_bundle WHERE id=?",(job,)).fetchone()
    if old is None or any(v is None for v in old) or tuple(old)!=(p.canonical_json(spec),streams['source'],streams['selection'],streams['evidence'],streams['file']):raise ValueError('replay differs')
   gcp.stage_owned(db,bundle_id=job,statement=spec,streams=streams,trusted_images=synthetic_bundle['trusted_images'],trusted_correspondences=synthetic_bundle['trusted_correspondences'],trusted_binding=trusted,expected_binding_sha256=synthetic_bundle['expected_binding_sha256'],processing_closure=synthetic_bundle['processing_closure'],processing_pages=synthetic_bundle['processing_pages'],_fault=_fault,_verify_only=not result['created'])
   return {'state':'held_synthetic_binding_and_gcp','created':result['created'],'bindingSha256':result['bindingSha256']}
 except (ValueError,TypeError,KeyError,sqlite3.Error,UnicodeError,RuntimeError,AttributeError):raise AtomicHeld('synthetic_atomic_unavailable') from None
