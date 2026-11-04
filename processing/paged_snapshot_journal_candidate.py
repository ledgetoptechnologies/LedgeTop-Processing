"""Inactive synthetic signed-snapshot inspection. Never authorizes staging or fetching."""
import base64
import hashlib
import hmac
import json
import re
import sqlite3
from datetime import datetime,timezone
from types import MappingProxyType
from cryptography.exceptions import InvalidSignature,UnsupportedAlgorithm
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from . import reference_protocol as p
from .maintenance import Conflict
from . import prepared_envelope_jcs_candidate as jcs
from .viewer_import_confirmation_store import _preflight_stored_json

PROTOCOL='ltds-viewer-paged-source-snapshot-v1'
DOMAIN=b'LTDS-VIEWER-PAGED-SOURCE-SNAPSHOT-V1\0'
PURPOSE='paged-source-snapshot'
MAX_BODY=65536
MAX_TTL_SECONDS=900
FIELDS=('protocol','schemaVersion','issuerId','keyId','snapshotId','principalId','environment','coordinatorAuthorityId','jobId','generation','submissionId','activationId','viewerTaskId','viewerAttemptId','preparedEnvelopeSha256','datasetId','datasetManifestSha256','pipelineDigest','sourcePageHashes','sourcePageBytes','sourceCount','selectedPageHashes','selectedPageBytes','selectedCount','inputManifestSha256','processingClosureSha256','gcpEvidenceVersion','gcpEvidencePageHashes','gcpEvidencePageBytes','gcpEvidencePageCount','gcpEvidenceByteCount','gcpEvidenceSha256','issuedAt','expiresAt','signature')

class SnapshotHeld(ValueError):pass

def _pairs(items):
 result={}
 for key,value in items:
  if key in result:raise ValueError()
  result[key]=value
 return result

def pin_registry(registry):
 """Operator-owned config; no registry fields may come from a request."""
 try:
  if type(registry) is not list or not 0<len(registry)<=32:raise ValueError()
  out={};owners={}
  for entry in registry:
   p._closed(entry,('coordinatorAuthorityId','principalId','environment','issuerId','keyId','purpose','publicKeyPem'),'key')
   p._uuid(entry['coordinatorAuthorityId'],'key authority')
   for key in ('principalId','issuerId','keyId'):p._slug(entry[key],key)
   p._environment(entry['environment'])
   if entry['purpose']!=PURPOSE or type(entry['publicKeyPem']) is not str or len(entry['publicKeyPem'])>8192:raise ValueError()
   key=serialization.load_pem_public_key(entry['publicKeyPem'].encode('ascii'))
   if not isinstance(key,Ed25519PublicKey):raise ValueError()
   raw=key.public_bytes(serialization.Encoding.Raw,serialization.PublicFormat.Raw);scope=(entry['coordinatorAuthorityId'],entry['principalId'],entry['environment']);identity=(*scope,entry['issuerId'],entry['keyId'])
   if identity in out or raw in owners:raise ValueError()
   owners[raw]=scope;out[identity]=key
  return out
 except (ValueError,TypeError,KeyError,UnicodeError,UnsupportedAlgorithm):raise SnapshotHeld('invalid_key_registry') from None

class ServerSnapshotTrust:
 """Independently configured server policy; never construct from request data."""
 def __init__(self,*,authority_id,key_entries,clock_provider=None,max_future_skew_ms=60000):
  p._uuid(authority_id,'configured authority')
  if type(max_future_skew_ms) is not int or not 0<=max_future_skew_ms<=60000:raise ValueError('invalid clock skew')
  for provider in (clock_provider,):
   if provider is not None and not callable(provider):raise ValueError('invalid provider')
  registry=pin_registry(key_entries)
  if any(key[0]!=authority_id for key in registry):raise ValueError('key authority differs')
  self.authority_id=authority_id;self.registry=MappingProxyType(registry);self.clock_provider=clock_provider
  self.max_future_skew_ms=max_future_skew_ms

DDL="CREATE TABLE paged_snapshot_journal_v1 (issuer_id TEXT NOT NULL, key_id TEXT NOT NULL, snapshot_id TEXT NOT NULL, statement_sha256 TEXT NOT NULL, signed_bytes BLOB NOT NULL, job_id TEXT NOT NULL, principal_id TEXT NOT NULL, environment TEXT NOT NULL, authority_id TEXT NOT NULL, PRIMARY KEY(issuer_id,snapshot_id))"
MARKER_DDL="CREATE TABLE paged_snapshot_journal_marker_v1 (singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL CHECK(version=1))"

def _check_schema(db):
 row=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='paged_snapshot_journal_v1'").fetchone()
 if row is None or row[0]!=DDL:raise SnapshotHeld('snapshot_journal_schema_unavailable')
 marker=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='paged_snapshot_journal_marker_v1'").fetchone()
 if marker is None or marker[0]!=MARKER_DDL or [tuple(r) for r in db.execute('SELECT singleton,version FROM paged_snapshot_journal_marker_v1 LIMIT 2')]!=[(1,1)]:raise SnapshotHeld('snapshot_journal_schema_unavailable')
 indexes=db.execute("PRAGMA index_list('paged_snapshot_journal_v1')").fetchall()
 if len(indexes)!=1 or indexes[0][2]!=1 or indexes[0][3]!='pk':raise SnapshotHeld('snapshot_journal_schema_unavailable')

def initialize(db):
 """Explicit candidate setup, never a live migration."""
 if db.in_transaction:raise SnapshotHeld('snapshot_journal_schema_unavailable')
 db.execute('BEGIN IMMEDIATE')
 try:
  names={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE name IN ('paged_snapshot_journal_v1','paged_snapshot_journal_marker_v1')")}
  if not names:
   db.execute(DDL);db.execute(MARKER_DDL);db.execute('INSERT INTO paged_snapshot_journal_marker_v1 VALUES(1,1)')
  elif len(names)!=2:raise SnapshotHeld('snapshot_journal_schema_unavailable')
  _check_schema(db);db.commit()
 except BaseException:
  db.rollback();raise

def inspect(store,submission_id,raw,*,principal,environment,trust,stage_bundle=None,_fault=None):
 """Durably journal held metadata. `trust` must be provisioned independently.

 Principal and environment must come from authenticated server context.
 Provider seams are server configuration only, never request/body parameters.
 No result authorizes ingestion, dispatch, fetch or cleanup.
 """
 try:
  if type(trust) is not ServerSnapshotTrust:raise ValueError()
  expected_authority_id=trust.authority_id
  p._uuid(submission_id,'submission');p._slug(principal,'principal');p._environment(environment)
  if type(raw) is not bytes or not 0<len(raw)<=MAX_BODY:raise ValueError()
  _preflight_stored_json(raw)
  statement=json.loads(raw.decode('utf8'),object_pairs_hook=_pairs,parse_constant=lambda _:(_ for _ in ()).throw(ValueError()))
  p._closed(statement,FIELDS,'snapshot')
  if statement['protocol']!=PROTOCOL or type(statement['schemaVersion']) is not int or statement['schemaVersion']!=1:raise ValueError()
  for k in ('issuerId','keyId','snapshotId','principalId','jobId','datasetId'):p._slug(statement[k],k)
  p._environment(statement['environment'])
  for k in ('coordinatorAuthorityId','submissionId','activationId','viewerTaskId','viewerAttemptId'):p._uuid(statement[k],k)
  for k in ('preparedEnvelopeSha256','datasetManifestSha256','pipelineDigest','inputManifestSha256','processingClosureSha256'):p._hash(statement[k],k)
  evidence_keys=('gcpEvidenceVersion','gcpEvidencePageHashes','gcpEvidencePageBytes','gcpEvidencePageCount','gcpEvidenceByteCount','gcpEvidenceSha256')
  if any(statement[k] is not None for k in evidence_keys):
   if any(statement[k] is None for k in evidence_keys) or statement['gcpEvidenceVersion']!='ltds-viewer-gcp-evidence-chunks-v1':raise ValueError()
   hashes=statement['gcpEvidencePageHashes'];sizes=statement['gcpEvidencePageBytes'];count=statement['gcpEvidencePageCount'];size=statement['gcpEvidenceByteCount']
   if type(hashes) is not list or type(sizes) is not list or type(count) is not int or not 2<=count<=512 or len(hashes)!=count or len(sizes)!=count:raise ValueError()
   p._integer(size,'evidence byte count')
   if not count-1<=size<=min(16*1024*1024,(count-1)*46080):raise ValueError()
   for value in hashes:p._hash(value,'evidence page hash')
   for value in sizes:
    p._integer(value,'evidence page bytes')
    if value>65535:raise ValueError()
   if sum(sizes)+count>24*1024*1024:raise ValueError()
   p._hash(statement['gcpEvidenceSha256'],'GCP evidence')
  for k in ('generation','sourceCount','selectedCount'):p._integer(statement[k],k)
  if statement['sourceCount']>20000 or statement['selectedCount']>20000:raise ValueError()
  for k in ('sourcePageHashes','selectedPageHashes'):
   if type(statement[k]) is not list or not 0<len(statement[k])<=64:raise ValueError()
   for value in statement[k]:p._hash(value,k)
  for prefix in ('source','selected'):
   sizes=statement[prefix+'PageBytes'];hashes=statement[prefix+'PageHashes']
   if type(sizes) is not list or len(sizes)!=len(hashes) or not len(hashes)<=statement[prefix+'Count']<=len(hashes)*512:raise ValueError()
   for size in sizes:
    p._integer(size,'page bytes')
    if size>256*1024:raise ValueError()
   if sum(sizes)+len(sizes)>12*1024*1024:raise ValueError()
  times=[]
  for key in ('issuedAt','expiresAt'):
   value=statement[key]
   if type(value) is not str or not re.fullmatch(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z',value):raise ValueError()
   times.append(datetime.strptime(value,'%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc))
  if not 0<(times[1]-times[0]).total_seconds()<=MAX_TTL_SECONDS:raise ValueError()
  sig=statement['signature']
  if type(sig) is not str or len(sig)!=86:raise ValueError()
  signature=base64.urlsafe_b64decode(sig+'==')
  if len(signature)!=64 or base64.urlsafe_b64encode(signature).decode().rstrip('=')!=sig:raise ValueError()
  key=trust.registry.get((expected_authority_id,principal,environment,statement['issuerId'],statement['keyId']))
  if key is None:raise ValueError()
  unsigned={k:v for k,v in statement.items() if k!='signature'};wire=p.canonical_json(unsigned)
  key.verify(signature,DOMAIN+wire)
  if (statement['principalId'],statement['environment'],statement['coordinatorAuthorityId'],statement['submissionId'])!=(principal,environment,expected_authority_id,submission_id):raise ValueError()
  if p.canonical_json(statement)!=raw:raise ValueError('canonical signed bytes required')
  statement_hash=hashlib.sha256(DOMAIN+wire).hexdigest()
  with store.queue._db() as db:
   db.execute('BEGIN IMMEDIATE');_check_schema(db);store._check_jcs_capability(db);store._require_viewer_authority(db,expected_authority_id)
   found=store._scoped_prepared_job_id(db,submission_id,principal,environment,jcs.PROTOCOL)
   if found is None:return None
   row=store._row(db,found['job_id']);owner=store._submission_owner(db,row)
   if owner is None or (owner['principal_id'],owner['environment'],owner['protocol'])!=(principal,environment,jcs.PROTOCOL):return None
   v2names={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('held_source_bridge_v2','held_source_bridge_blobs_v2','held_source_bridge_marker_v2','held_source_protocol_policy_v2')")}
   if v2names:
    from .held_source_bridge_journal_candidate import _schema as v2_schema
    v2_schema(db)
    if db.execute('SELECT 1 FROM held_source_protocol_policy_v2 WHERE job_id=?',(row['job_id'],)).fetchone():raise ValueError('v2 protocol required')
   envelope=db.execute('SELECT * FROM reference_viewer_envelopes_v2 WHERE job_id=?',(row['job_id'],)).fetchone()
   policy=db.execute('SELECT * FROM reference_viewer_v3_policy WHERE job_id=?',(row['job_id'],)).fetchone()
   if envelope is None or policy is None:raise ValueError()
   pol=json.loads(policy['policy_json'])
   if p.canonical_json(pol).decode()!=policy['policy_json'] or hashlib.sha256(policy['policy_json'].encode()).hexdigest()!=policy['policy_sha256']:raise ValueError()
   canonical,digest,normalized=jcs.validate(envelope['envelope_json'].encode(),trusted_policy=pol,environment=environment)
   if digest!=envelope['envelope_sha256'] or p.canonical_json(normalized).decode()!=row['input_json']:raise ValueError()
   expected=(row['job_id'],row['generation'],envelope['activation_id'],envelope['viewer_task_id'],envelope['viewer_attempt_id'],digest,normalized['manifest']['id'],normalized['manifestSha256'],normalized['pipelineDigest'])
   actual=tuple(statement[k] for k in ('jobId','generation','activationId','viewerTaskId','viewerAttemptId','preparedEnvelopeSha256','datasetId','datasetManifestSha256','pipelineDigest'))
   if actual!=expected:raise ValueError()
   if trust.clock_provider is None:return {'state':'held','reason':'trusted_clock_unavailable'}
   try:now_ms=trust.clock_provider()
   except Exception:return {'state':'held','reason':'trusted_clock_unavailable'}
   if type(now_ms) is not int or not 0<=now_ms<=p.MAX_SAFE_INTEGER:return {'state':'held','reason':'trusted_clock_unavailable'}
   epoch0=datetime(1970,1,1,tzinfo=timezone.utc)
   def millis(value):
    delta=value-epoch0
    return (delta.days*86400+delta.seconds)*1000+delta.microseconds//1000
   stored=db.execute("SELECT statement_sha256,CASE WHEN typeof(signed_bytes)='blob' AND length(signed_bytes)<=? THEN signed_bytes END,job_id,principal_id,environment,authority_id,key_id FROM paged_snapshot_journal_v1 WHERE issuer_id=? AND snapshot_id=?",(MAX_BODY,statement['issuerId'],statement['snapshotId'])).fetchone()
   if stored is not None:
    if tuple(stored)!=(statement_hash,raw,row['job_id'],principal,environment,expected_authority_id,statement['keyId']):raise ValueError('snapshot identity conflict')
   else:
    if now_ms+trust.max_future_skew_ms<millis(times[0]) or now_ms>=millis(times[1]):raise ValueError()
    db.execute('INSERT INTO paged_snapshot_journal_v1 VALUES(?,?,?,?,?,?,?,?,?)',(statement['issuerId'],statement['keyId'],statement['snapshotId'],statement_hash,raw,row['job_id'],principal,environment,expected_authority_id))
   if _fault:_fault('journal')
   if type(stage_bundle) is not dict:raise ValueError('transactional staging unavailable')
   p._closed(stage_bundle,('binding','bindingSha256','sourceStream','selectedStream','processingClosure','processingPages','evidenceStream'),'staging')
   binding=stage_bundle['binding']
   expected_binding=dict(datasetId=statement['datasetId'],datasetManifestSha256=statement['datasetManifestSha256'],principalId=principal,environment=environment,authorityId=expected_authority_id,viewerTaskId=statement['viewerTaskId'],viewerAttemptId=statement['viewerAttemptId'],pipelineDigest=statement['pipelineDigest'],sourcePageHashes=statement['sourcePageHashes'],sourceCount=statement['sourceCount'],selectedPageHashes=statement['selectedPageHashes'],selectedCount=statement['selectedCount'],inputManifestSha256=statement['inputManifestSha256'],processingClosureSha256=statement['processingClosureSha256'])
   if type(binding) is not dict or any(binding.get(k)!=v for k,v in expected_binding.items()):raise ValueError('binding mismatch')
   from . import paged_frame_receiver_candidate as frames
   stage_id=hashlib.sha256((statement['issuerId']+'\0'+statement['snapshotId']).encode()).hexdigest()
   if stored is not None:
    # A status-only replay may omit streams; supplied streams must match exactly.
    for kind,stream in (('source',stage_bundle['sourceStream']),('selected',stage_bundle['selectedStream'])):
     if not hasattr(stream,'readline'):raise ValueError('invalid replay stream')
     first=stream.readline(262146)
     if first:
      rows=db.execute('SELECT wire FROM candidate_frame_pages WHERE stage_id=? AND kind=? ORDER BY ordinal',(stage_id,kind))
      line=first
      for item in rows:
       if line!=bytes(item[0])+b'\n':raise ValueError('changed replay stream')
       line=stream.readline(262146)
      if line:raise ValueError('extra replay data')
   frames._stage_owned(db,stage_id,trusted_binding=binding,expected_binding_sha256=stage_bundle['bindingSha256'],source_stream=stage_bundle['sourceStream'],selected_stream=stage_bundle['selectedStream'],processing_closure=stage_bundle['processingClosure'],processing_pages=stage_bundle['processingPages'])
   for kind,prefix in (('source','source'),('selected','selected')):
    sizes=[r[0] for r in db.execute('SELECT length(wire) FROM candidate_frame_pages WHERE stage_id=? AND kind=? ORDER BY ordinal',(stage_id,kind))]
    if sizes!=statement[prefix+'PageBytes']:raise ValueError('signed page sizes differ')
   if _fault:_fault('frames')
   generated=[]
   for frame in db.execute("SELECT wire FROM candidate_frame_pages WHERE stage_id=? AND kind='source' ORDER BY ordinal",(stage_id,)):
    generated.extend(record for record in json.loads(bytes(frame[0]).decode('utf8'))['records'] if record['type']=='attempt_generated_gcp')
   if generated or statement['gcpEvidenceSha256'] is not None:
    # The current evidence stream doesn't contain the generated GCP bytes.
    # Do not commit a detached proof or silently omit required GCP semantics.
    raise ValueError('generated GCP content binding unavailable')
   if stage_bundle['evidenceStream'] is not None:raise ValueError('unexpected evidence')
   if _fault:_fault('evidence')
   return {'state':'held','reason':'snapshot_replay_status_only' if stored is not None else 'signed_pages_staged_only','statementSha256':statement_hash}

 except (ValueError,TypeError,KeyError,UnicodeError,OverflowError,InvalidSignature,Conflict,sqlite3.Error,OSError,RuntimeError,AttributeError):raise SnapshotHeld('snapshot_inspection_unavailable') from None
