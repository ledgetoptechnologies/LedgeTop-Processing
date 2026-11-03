"""Inactive synthetic signed-snapshot inspection. Never authorizes staging or fetching."""
import base64
import hashlib
import hmac
import json
import re
import sqlite3
from datetime import datetime,timezone
from cryptography.exceptions import InvalidSignature
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
MAX_TTL_SECONDS=3600
FIELDS=('protocol','schemaVersion','issuerId','keyId','snapshotId','principalId','environment','coordinatorAuthorityId','jobId','generation','submissionId','activationId','viewerTaskId','viewerAttemptId','preparedEnvelopeSha256','datasetId','datasetManifestSha256','pipelineDigest','sourcePageHashes','sourcePageBytes','sourceCount','selectedPageHashes','selectedPageBytes','selectedCount','inputManifestSha256','processingClosureSha256','gcpEvidenceSha256','sourceRevision','revocationEpoch','issuedAt','expiresAt','signature')

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
   p._closed(entry,('principalId','environment','issuerId','keyId','purpose','publicKeyPem'),'key')
   for key in ('principalId','issuerId','keyId'):p._slug(entry[key],key)
   p._environment(entry['environment'])
   if entry['purpose']!=PURPOSE or type(entry['publicKeyPem']) is not str or len(entry['publicKeyPem'])>8192:raise ValueError()
   key=serialization.load_pem_public_key(entry['publicKeyPem'].encode('ascii'))
   if not isinstance(key,Ed25519PublicKey):raise ValueError()
   raw=key.public_bytes(serialization.Encoding.Raw,serialization.PublicFormat.Raw);scope=(entry['principalId'],entry['environment']);identity=(*scope,entry['issuerId'],entry['keyId'])
   if identity in out or (raw in owners and owners[raw]!=scope):raise ValueError()
   owners[raw]=scope;out[identity]=key
  return out
 except (ValueError,TypeError,KeyError,UnicodeError):raise SnapshotHeld('invalid_key_registry') from None

def inspect(store,submission_id,raw,*,principal,environment,expected_authority_id,key_registry,epoch_provider=None,snapshot_pin_provider=None):
 """Read-only synthetic inspection; providers are independent server-owned seams.

 `principal`/`environment` must come from authentication, registry and provider
 configuration must be independently managed. No real provider exists here.
 No result of this function authorizes ingestion, dispatch, fetch or cleanup.
 """
 try:
  p._uuid(submission_id,'submission');p._slug(principal,'principal');p._environment(environment);p._uuid(expected_authority_id,'authority')
  if type(raw) is not bytes or not 0<len(raw)<=MAX_BODY:raise ValueError()
  _preflight_stored_json(raw)
  statement=json.loads(raw.decode('utf8'),object_pairs_hook=_pairs,parse_constant=lambda _:(_ for _ in ()).throw(ValueError()))
  p._closed(statement,FIELDS,'snapshot')
  if statement['protocol']!=PROTOCOL or type(statement['schemaVersion']) is not int or statement['schemaVersion']!=1:raise ValueError()
  for k in ('issuerId','keyId','snapshotId','principalId','jobId','datasetId','sourceRevision'):p._slug(statement[k],k)
  p._environment(statement['environment'])
  for k in ('coordinatorAuthorityId','submissionId','activationId','viewerTaskId','viewerAttemptId'):p._uuid(statement[k],k)
  for k in ('preparedEnvelopeSha256','datasetManifestSha256','pipelineDigest','inputManifestSha256','processingClosureSha256'):p._hash(statement[k],k)
  if statement['gcpEvidenceSha256'] is not None:p._hash(statement['gcpEvidenceSha256'],'GCP evidence')
  for k in ('generation','sourceCount','selectedCount'):p._integer(statement[k],k)
  p._integer(statement['revocationEpoch'],'epoch',allow_zero=True)
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
  key=key_registry.get((principal,environment,statement['issuerId'],statement['keyId']))
  if key is None:raise ValueError()
  unsigned={k:v for k,v in statement.items() if k!='signature'};wire=p.canonical_json(unsigned)
  key.verify(signature,DOMAIN+wire)
  if (statement['principalId'],statement['environment'],statement['coordinatorAuthorityId'],statement['submissionId'])!=(principal,environment,expected_authority_id,submission_id):raise ValueError()
  statement_hash=hashlib.sha256(DOMAIN+wire).hexdigest()
  with store.queue._db() as db:
   db.execute('PRAGMA query_only=ON');db.execute('BEGIN');store._check_jcs_capability(db);store._require_viewer_authority(db,expected_authority_id)
   found=store._scoped_prepared_job_id(db,submission_id,principal,environment,jcs.PROTOCOL)
   if found is None:return None
   row=store._row(db,found['job_id']);owner=store._submission_owner(db,row)
   if owner is None or (owner['principal_id'],owner['environment'],owner['protocol'])!=(principal,environment,jcs.PROTOCOL):return None
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
   if epoch_provider is None or snapshot_pin_provider is None:return {'state':'held','reason':'trusted_snapshot_or_epoch_unavailable'}
   epoch=epoch_provider(db,principal,environment,expected_authority_id)
   pin=snapshot_pin_provider(db,row['job_id'],statement['snapshotId'])
   if type(epoch) is not int or epoch!=statement['revocationEpoch'] or type(pin) is not str or not hmac.compare_digest(pin,statement_hash):raise ValueError()
   return {'state':'held','reason':'atomic_staging_unavailable','statementSha256':statement_hash}
 except (ValueError,TypeError,KeyError,UnicodeError,OverflowError,InvalidSignature,Conflict,sqlite3.Error,OSError,RuntimeError,AttributeError):raise SnapshotHeld('snapshot_inspection_unavailable') from None
