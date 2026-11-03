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
 def __init__(self,*,authority_id,key_entries,clock_provider=None,epoch_provider=None,snapshot_pin_provider=None,max_future_skew_ms=60000):
  p._uuid(authority_id,'configured authority')
  if type(max_future_skew_ms) is not int or not 0<=max_future_skew_ms<=300000:raise ValueError('invalid clock skew')
  for provider in (clock_provider,epoch_provider,snapshot_pin_provider):
   if provider is not None and not callable(provider):raise ValueError('invalid provider')
  registry=pin_registry(key_entries)
  if any(key[0]!=authority_id for key in registry):raise ValueError('key authority differs')
  self.authority_id=authority_id;self.registry=MappingProxyType(registry);self.clock_provider=clock_provider
  self.epoch_provider=epoch_provider;self.snapshot_pin_provider=snapshot_pin_provider;self.max_future_skew_ms=max_future_skew_ms

def inspect(store,submission_id,raw,*,principal,environment,trust):
 """Read-only held inspection. `trust` must be provisioned independently.

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
  key=trust.registry.get((expected_authority_id,principal,environment,statement['issuerId'],statement['keyId']))
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
   if trust.clock_provider is None:return {'state':'held','reason':'trusted_clock_unavailable'}
   try:now_ms=trust.clock_provider()
   except Exception:return {'state':'held','reason':'trusted_clock_unavailable'}
   if type(now_ms) is not int or not 0<=now_ms<=p.MAX_SAFE_INTEGER:return {'state':'held','reason':'trusted_clock_unavailable'}
   epoch0=datetime(1970,1,1,tzinfo=timezone.utc)
   def millis(value):
    delta=value-epoch0
    return (delta.days*86400+delta.seconds)*1000+delta.microseconds//1000
   if now_ms+trust.max_future_skew_ms<millis(times[0]) or now_ms>=millis(times[1]):raise ValueError()
   if trust.epoch_provider is None or trust.snapshot_pin_provider is None:return {'state':'held','reason':'trusted_snapshot_or_epoch_unavailable'}
   # Callbacks receive only immutable identifiers, never the SQLite handle.
   # They are trusted in-process code, not a sandbox or a same-transaction loader.
   context=MappingProxyType(dict(principalId=principal,environment=environment,authorityId=expected_authority_id,jobId=row['job_id'],snapshotId=statement['snapshotId']))
   epoch=trust.epoch_provider(context)
   pin=trust.snapshot_pin_provider(context)
   if type(epoch) is not int or epoch!=statement['revocationEpoch'] or type(pin) is not str or not hmac.compare_digest(pin,statement_hash):raise ValueError()
   return {'state':'held','reason':'atomic_staging_unavailable','statementSha256':statement_hash}
 except (ValueError,TypeError,KeyError,UnicodeError,OverflowError,InvalidSignature,Conflict,sqlite3.Error,OSError,RuntimeError,AttributeError):raise SnapshotHeld('snapshot_inspection_unavailable') from None
