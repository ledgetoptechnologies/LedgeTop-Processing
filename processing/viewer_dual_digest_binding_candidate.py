"""Unmounted synthetic v1 dual-identity binding; never admission authority."""
import hashlib,json,sqlite3
from contextlib import nullcontext
from . import reference_protocol as p
from . import prepared_envelope_jcs_candidate as jcs

VERSION='ltds-viewer-dataset-transport-binding-v1'
DOMAIN=b'ltds-viewer-dataset-transport-binding-v1\0'
DDL='CREATE TABLE synthetic_viewer_dataset_transport_v1 (job_id TEXT PRIMARY KEY, binding_sha256 TEXT NOT NULL, binding_json BLOB NOT NULL)'
MARKER='CREATE TABLE synthetic_viewer_dataset_transport_marker_v1 (singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL CHECK(version=1))'
PROVENANCE_FIELDS=('viewerDatasetId','viewerDatasetManifestSha256','viewerTaskId','viewerAttemptId','viewerInputManifestSha256','sourcePageHashes','sourcePageBytes','sourceCount','selectedPageHashes','selectedPageBytes','selectedCount','gcpEvidenceVersion','gcpEvidencePageHashes','gcpEvidencePageBytes','gcpEvidencePageCount','gcpEvidenceByteCount','gcpEvidenceSha256','gcpFileVersion','gcpFilePageHashes','gcpFilePageBytes','gcpFilePageCount','gcpFileByteCount','gcpFileSha256')
FIELDS=('version','schemaVersion','principalId','environment','coordinatorAuthorityId','submissionId','jobId','generation','activationId','viewerTaskId','viewerAttemptId','preparedEnvelopeSha256','viewerDatasetId','viewerDatasetManifestSha256','viewerInputManifestSha256','transportManifestSha256','pipelineDigest')+tuple(k for k in PROVENANCE_FIELDS if k not in ('viewerDatasetId','viewerDatasetManifestSha256','viewerTaskId','viewerAttemptId','viewerInputManifestSha256'))

class BindingHeld(ValueError):pass


def _schema(db):
 for name,ddl in (('synthetic_viewer_dataset_transport_v1',DDL),('synthetic_viewer_dataset_transport_marker_v1',MARKER)):
  row=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?",(name,)).fetchone()
  if row is None or row[0]!=ddl or db.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' AND tbl_name=? LIMIT 1",(name,)).fetchone():raise ValueError()
 if [tuple(r) for r in db.execute('SELECT singleton,version FROM synthetic_viewer_dataset_transport_marker_v1 LIMIT 2')]!=[(1,1)]:raise ValueError()
 indexes=db.execute('PRAGMA index_list(synthetic_viewer_dataset_transport_v1)').fetchall()
 if len(indexes)!=1 or indexes[0][2]!=1 or indexes[0][3]!='pk':raise ValueError()


def initialize(db):
 if db.in_transaction:raise BindingHeld('binding_schema_unavailable')
 db.execute('BEGIN IMMEDIATE')
 try:
  names={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE name IN ('synthetic_viewer_dataset_transport_v1','synthetic_viewer_dataset_transport_marker_v1')")}
  if not names:db.execute(DDL);db.execute(MARKER);db.execute('INSERT INTO synthetic_viewer_dataset_transport_marker_v1 VALUES(1,1)')
  elif len(names)!=2:raise ValueError()
  _schema(db);db.commit()
 except (ValueError,sqlite3.Error):
  db.rollback();raise BindingHeld('binding_schema_unavailable') from None


def _stage(store,submission_id,statement,*,principal,environment,configured_authority,synthetic_viewer_provenance,_fault=None,_owned_db=None):
 """Only test-injected provenance; writes held metadata in one coordinator tx.

 Independent authenticated provenance does not yet exist. This function has no
 route and its returned state must never authorize work, pages or cleanup.
 """
 try:
  p._closed(statement,FIELDS,'dual identity binding')
  if statement['version']!=VERSION or type(statement['schemaVersion']) is not int or statement['schemaVersion']!=1:raise ValueError()
  p._closed(synthetic_viewer_provenance,PROVENANCE_FIELDS,'synthetic Viewer provenance')
  for k in ('viewerDatasetManifestSha256','viewerInputManifestSha256','transportManifestSha256','preparedEnvelopeSha256','pipelineDigest'):p._hash(statement[k],k)
  for k in ('coordinatorAuthorityId','submissionId','viewerTaskId','viewerAttemptId','activationId'):p._uuid(statement[k],k)
  for prefix in ('source','selected'):
   hashes,sizes,count=statement[prefix+'PageHashes'],statement[prefix+'PageBytes'],statement[prefix+'Count']
   if type(hashes) is not list or type(sizes) is not list or not 0<len(hashes)<=64 or len(hashes)!=len(sizes):raise ValueError()
   p._integer(count,'frame count')
   if count>20000:raise ValueError()
   for value in hashes:p._hash(value,'page hash')
   for value in sizes:
    p._integer(value,'page bytes')
    if value>262144:raise ValueError()
   if sum(sizes)+len(sizes)>12*1024*1024:raise ValueError()
  gcp=[statement[k] for k in PROVENANCE_FIELDS if k.startswith(('gcpEvidence','gcpFile'))]
  if any(x is None for x in gcp) and any(x is not None for x in gcp):raise ValueError()
  if gcp[0] is not None:
   for prefix,version in (('gcpEvidence','ltds-viewer-gcp-evidence-chunks-v1'),('gcpFile','ltds-viewer-generated-gcp-file-chunks-v1')):
    if statement[prefix+'Version']!=version:raise ValueError()
    hashes,sizes,count=statement[prefix+'PageHashes'],statement[prefix+'PageBytes'],statement[prefix+'PageCount']
    if type(hashes) is not list or type(sizes) is not list or type(count) is not int or not 2<=count<=512 or len(hashes)!=count or len(sizes)!=count:raise ValueError()
    p._integer(statement[prefix+'ByteCount'],'GCP bytes');p._hash(statement[prefix+'Sha256'],'GCP hash')
    if statement[prefix+'ByteCount']>16*1024*1024:raise ValueError()
    for value in hashes:p._hash(value,'page hash')
    for value in sizes:
     p._integer(value,'page bytes')
     if value>65535:raise ValueError()
    if sum(sizes)+len(sizes)>24*1024*1024:raise ValueError()
  p._slug(statement['viewerDatasetId'],'viewer dataset');p._slug(principal,'principal');p._environment(environment)
  if (statement['principalId'],statement['environment'],statement['coordinatorAuthorityId'],statement['submissionId'])!=(principal,environment,configured_authority,submission_id):raise ValueError()
  if any(statement[k]!=v for k,v in synthetic_viewer_provenance.items()):raise ValueError()
  wire=p.canonical_json(statement);digest=hashlib.sha256(DOMAIN+wire).hexdigest()
  if len(wire)>65536:raise ValueError()
  with (store.queue._db() if _owned_db is None else nullcontext(_owned_db)) as db:
   if _owned_db is None:db.execute('BEGIN IMMEDIATE')
   elif not db.in_transaction:raise ValueError('owned transaction required')
   _schema(db);store._check_jcs_capability(db);store._require_viewer_authority(db,configured_authority)
   found=store._scoped_prepared_job_id(db,submission_id,principal,environment,jcs.PROTOCOL)
   if found is None:return None
   row=store._row(db,found['job_id']);owner=store._submission_owner(db,row)
   env=db.execute('SELECT * FROM reference_viewer_envelopes_v2 WHERE job_id=?',(row['job_id'],)).fetchone()
   policy=db.execute('SELECT * FROM reference_viewer_v3_policy WHERE job_id=?',(row['job_id'],)).fetchone()
   if owner is None or env is None or policy is None or (owner['principal_id'],owner['environment'],owner['protocol'])!=(principal,environment,jcs.PROTOCOL):raise ValueError()
   pol=json.loads(policy['policy_json'])
   if p.canonical_json(pol).decode()!=policy['policy_json'] or hashlib.sha256(policy['policy_json'].encode()).hexdigest()!=policy['policy_sha256']:raise ValueError()
   _,envelope_sha,normalized=jcs.validate(env['envelope_json'].encode(),trusted_policy=pol,environment=environment)
   if envelope_sha!=env['envelope_sha256'] or p.canonical_json(normalized).decode()!=row['input_json']:raise ValueError()
   if (statement['jobId'],statement['generation'],statement['activationId'],statement['viewerTaskId'],statement['viewerAttemptId'],statement['preparedEnvelopeSha256'],statement['transportManifestSha256'],statement['pipelineDigest'])!=(row['job_id'],row['generation'],env['activation_id'],env['viewer_task_id'],env['viewer_attempt_id'],envelope_sha,normalized['manifestSha256'],normalized['pipelineDigest']):raise ValueError()
   old=db.execute("SELECT binding_sha256,CASE WHEN typeof(binding_json)='blob' AND length(binding_json)<=65536 THEN binding_json END FROM synthetic_viewer_dataset_transport_v1 WHERE job_id=?",(row['job_id'],)).fetchone()
   if old is not None:
    if tuple(old)!=(digest,wire):raise ValueError()
    return {'state':'held_synthetic_binding_only','created':False,'bindingSha256':digest}
   db.execute('INSERT INTO synthetic_viewer_dataset_transport_v1 VALUES(?,?,?)',(row['job_id'],digest,wire))
   if _fault:_fault()
   return {'state':'held_synthetic_binding_only','created':True,'bindingSha256':digest}
 except (ValueError,TypeError,KeyError,sqlite3.Error,UnicodeError,RuntimeError,AttributeError):raise BindingHeld('binding_unavailable') from None


def stage(store,submission_id,statement,**kwargs):
 return _stage(store,submission_id,statement,**kwargs)

def _stage_owned(db,store,submission_id,statement,**kwargs):
 if not db.in_transaction:raise BindingHeld("owned_transaction_required")
 return _stage(store,submission_id,statement,_owned_db=db,**kwargs)
