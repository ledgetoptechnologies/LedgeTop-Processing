import base64
import copy
import hashlib
import json
import tempfile
import unittest
import uuid
from pathlib import Path
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from processing.prototype import Queue
from processing.reference_store import ReferenceStore,PROFILE
from processing import reference_protocol as p
from processing.prepared_envelope_jcs_candidate import PROTOCOL as PREPARED
from processing.paged_snapshot_signature_candidate import PROTOCOL,DOMAIN,pin_registry,inspect,SnapshotHeld,ServerSnapshotTrust

class PagedSnapshotSignatureTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory(prefix='signed-snapshot-');self.addCleanup(self.temp.cleanup)
  self.queue=Queue(self.temp.name);self.queue.allowed_profiles.add(PROFILE);self.addCleanup(self.queue.close)
  self.store=ReferenceStore(self.queue,jcs_runtime_enabled=True)
  fixture=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())
  self.policy=dict(capabilityFingerprint=fixture['capabilities']['capabilityFingerprint'],optionsFingerprint=fixture['capabilities']['engines']['odm']['optionsFingerprint'],availableEngines=['odm'],optionDefinitions={})
  self.env=dict(schemaVersion=3,protocol=PREPARED,submission=copy.deepcopy(fixture['submission']),viewerTaskId=str(uuid.uuid4()),viewerAttemptId=str(uuid.uuid4()),activationId=str(uuid.uuid4()))
  self.job,_=self.store.prepare_viewer_envelope_jcs_v3(json.dumps(self.env).encode(),authenticated_principal='viewer-a',authenticated_environment='staging',trusted_policy=self.policy,expected_authority_id=self.queue.authority_id)
  found=self.store.find_viewer_envelope_v2(self.env['submission']['submissionId'],authenticated_principal='viewer-a',authenticated_environment='staging',expected_protocol=PREPARED,expected_authority_id=self.queue.authority_id)
  self.private=Ed25519PrivateKey.generate();pem=self.private.public_key().public_bytes(serialization.Encoding.PEM,serialization.PublicFormat.SubjectPublicKeyInfo).decode()
  self.config=[dict(coordinatorAuthorityId=self.queue.authority_id,principalId='viewer-a',environment='staging',issuerId='issuer-1',keyId='key-1',purpose='paged-source-snapshot',publicKeyPem=pem)];self.registry=pin_registry(self.config)
  self.statement=dict(protocol=PROTOCOL,schemaVersion=1,issuerId='issuer-1',keyId='key-1',snapshotId='snapshot-1',principalId='viewer-a',environment='staging',coordinatorAuthorityId=self.queue.authority_id,jobId=self.job['jobId'],generation=self.job['generation'],submissionId=self.env['submission']['submissionId'],activationId=self.env['activationId'],viewerTaskId=self.env['viewerTaskId'],viewerAttemptId=self.env['viewerAttemptId'],preparedEnvelopeSha256=found['envelopeSha256'],datasetId=self.env['submission']['manifest']['id'],datasetManifestSha256=self.env['submission']['manifest']['sha256'],pipelineDigest=p.digest(self.env['submission']['pipeline']),sourcePageHashes=['1'*64],sourcePageBytes=[100],sourceCount=1,selectedPageHashes=['2'*64],selectedPageBytes=[120],selectedCount=1,inputManifestSha256='3'*64,processingClosureSha256='4'*64,gcpEvidenceVersion=None,gcpEvidencePageHashes=None,gcpEvidencePageBytes=None,gcpEvidencePageCount=None,gcpEvidenceByteCount=None,gcpEvidenceSha256=None,issuedAt='2030-01-01T00:00:00.000Z',expiresAt='2030-01-01T00:15:00.000Z')
  self.pin=self.statement_hash(self.statement)
 def statement_hash(self,statement):return hashlib.sha256(DOMAIN+p.canonical_json({k:v for k,v in statement.items() if k!='signature'})).hexdigest()
 def sign(self,value=None,key=None):
  value=copy.deepcopy(self.statement if value is None else value);sig=(key or self.private).sign(DOMAIN+p.canonical_json(value));value['signature']=base64.urlsafe_b64encode(sig).decode().rstrip('=');return p.canonical_json(value)
 def call(self,raw=None,**kwargs):
  principal=kwargs.pop('principal','viewer-a');environment=kwargs.pop('environment','staging')
  authority=kwargs.pop('expected_authority_id',self.queue.authority_id);entries=kwargs.pop('key_entries',self.config)
  clock=kwargs.pop('clock_provider',lambda:1893456450000);snapshot=kwargs.pop('snapshot_pin_provider',lambda *_:self.pin)
  self.assertFalse(kwargs)
  trust=ServerSnapshotTrust(authority_id=authority,key_entries=entries,clock_provider=clock,snapshot_pin_provider=snapshot)
  return inspect(self.store,self.env['submission']['submissionId'],self.sign() if raw is None else raw,principal=principal,environment=environment,trust=trust)
 def test_valid_exact_replay_and_reopen_are_held_without_writes(self):
  before=hashlib.sha256(Path(self.queue.path).read_bytes()).hexdigest();first=self.call();second=self.call();self.assertEqual(first,second);self.assertEqual(first['reason'],'independent_epoch_and_atomic_staging_unavailable')
  self.store=ReferenceStore(self.queue,jcs_runtime_enabled=True);self.assertEqual(self.call(),first);self.assertEqual(hashlib.sha256(Path(self.queue.path).read_bytes()).hexdigest(),before)
 def test_wire_rejects_unprovisioned_revision_and_epoch_fields(self):
  for key,value in [('sourceRevision','unprovisioned'),('revocationEpoch',3)]:
   changed=copy.deepcopy(self.statement);changed[key]=value
   with self.subTest(key=key),self.assertRaises(SnapshotHeld):self.call(raw=self.sign(changed))
 def test_gcp_digest_is_only_an_unverified_statement(self):
  changed=copy.deepcopy(self.statement);changed.update(gcpEvidenceVersion='ltds-viewer-gcp-evidence-chunks-v1',gcpEvidencePageHashes=['e'*64,'f'*64],gcpEvidencePageBytes=[100,100],gcpEvidencePageCount=2,gcpEvidenceByteCount=40,gcpEvidenceSha256='d'*64);pin=self.statement_hash(changed)
  result=self.call(raw=self.sign(changed),snapshot_pin_provider=lambda *_:pin)
  self.assertEqual(result['state'],'held')
  self.assertEqual(result['reason'],'independent_epoch_and_atomic_staging_unavailable')
 def test_evidence_fields_are_all_null_or_consistent_bounded_bundle(self):
  partial=copy.deepcopy(self.statement);partial['gcpEvidenceSha256']='d'*64
  with self.assertRaises(SnapshotHeld):self.call(raw=self.sign(partial))
  base=copy.deepcopy(self.statement);base.update(gcpEvidenceVersion='ltds-viewer-gcp-evidence-chunks-v1',gcpEvidencePageHashes=['e'*64,'f'*64],gcpEvidencePageBytes=[100,100],gcpEvidencePageCount=2,gcpEvidenceByteCount=40,gcpEvidenceSha256='d'*64)
  for key,value in [('gcpEvidencePageCount',1),('gcpEvidenceByteCount',46081),('gcpEvidencePageBytes',[65536,100]),('gcpEvidenceVersion','unknown')]:
   bad=copy.deepcopy(base);bad[key]=value
   with self.subTest(key=key),self.assertRaises(SnapshotHeld):self.call(raw=self.sign(bad))
  large=copy.deepcopy(base);large.update(gcpEvidencePageHashes=['e'*64]*512,gcpEvidencePageBytes=[100]*512,gcpEvidencePageCount=512,gcpEvidenceByteCount=512)
  raw=self.sign(large);self.assertLessEqual(len(raw),65536);pin=self.statement_hash(large)
  self.assertEqual(self.call(raw=raw,snapshot_pin_provider=lambda *_:pin)['reason'],'independent_epoch_and_atomic_staging_unavailable')
 def test_missing_independent_sources_never_authorizes(self):
  self.assertEqual(self.call(snapshot_pin_provider=None)['reason'],'trusted_snapshot_unavailable')
 def test_wrong_signature_owner_environment_or_snapshot_hold(self):
  changed=copy.deepcopy(self.statement);changed['sourceCount']=2
  unknown=copy.deepcopy(self.statement);unknown['keyId']='unknown-key'
  cases=[dict(raw=self.sign(key=Ed25519PrivateKey.generate())),dict(raw=self.sign(unknown)),dict(raw=self.sign(changed)),dict(principal='viewer-b'),dict(environment='production'),dict(snapshot_pin_provider=lambda *_:'f'*64)]
  for args in cases:
   with self.subTest(args=list(args)),self.assertRaises(SnapshotHeld):self.call(**args)
 def test_wrong_prepared_attempt_or_authority_holds_even_when_signed_and_pinned(self):
  for key,value in [('viewerAttemptId',str(uuid.uuid4())),('coordinatorAuthorityId',str(uuid.uuid4())),('datasetManifestSha256','f'*64)]:
   changed=copy.deepcopy(self.statement);changed[key]=value;pin=self.statement_hash(changed)
   with self.subTest(key=key),self.assertRaises(SnapshotHeld):self.call(raw=self.sign(changed),snapshot_pin_provider=lambda *_:pin)
 def test_wrong_configured_authority_cannot_inspect_or_write(self):
  wrong=str(uuid.uuid4());changed=copy.deepcopy(self.statement);changed['coordinatorAuthorityId']=wrong;pin=self.statement_hash(changed)
  before=hashlib.sha256(Path(self.queue.path).read_bytes()).hexdigest()
  with self.assertRaises(SnapshotHeld):self.call(raw=self.sign(changed),expected_authority_id=wrong,key_entries=[dict(self.config[0],coordinatorAuthorityId=wrong)],snapshot_pin_provider=lambda *_:pin)
  self.assertEqual(hashlib.sha256(Path(self.queue.path).read_bytes()).hexdigest(),before)
 def test_missing_or_invalid_server_clock_holds(self):
  self.assertEqual(self.call(clock_provider=None)['reason'],'trusted_clock_unavailable')
  self.assertEqual(self.call(clock_provider=lambda:True)['reason'],'trusted_clock_unavailable')
  def unavailable():raise RuntimeError('clock unavailable')
  self.assertEqual(self.call(clock_provider=unavailable)['reason'],'trusted_clock_unavailable')
 def test_expired_future_and_skew_boundaries(self):
  for now in (1893456900000,1893455939999):
   with self.subTest(now=now),self.assertRaises(SnapshotHeld):self.call(clock_provider=lambda:now)
  self.assertEqual(self.call(clock_provider=lambda:1893455940000)['reason'],'independent_epoch_and_atomic_staging_unavailable')
  self.assertEqual(self.call(clock_provider=lambda:1893456899999)['reason'],'independent_epoch_and_atomic_staging_unavailable')
 def test_key_entitlement_must_match_configured_authority(self):
  wrong=str(uuid.uuid4())
  with self.assertRaises(ValueError):ServerSnapshotTrust(authority_id=self.queue.authority_id,key_entries=[dict(self.config[0],coordinatorAuthorityId=wrong)])
  with self.assertRaises(SnapshotHeld):pin_registry(self.config+[dict(self.config[0],coordinatorAuthorityId=wrong)])
 def test_rotation_accepts_new_scope_and_rejects_retired_key(self):
  newer=Ed25519PrivateKey.generate();pem=newer.public_key().public_bytes(serialization.Encoding.PEM,serialization.PublicFormat.SubjectPublicKeyInfo).decode()
  new_entry=dict(self.config[0],keyId='key-2',publicKeyPem=pem)
  rotated=copy.deepcopy(self.statement);rotated['keyId']='key-2';pin=self.statement_hash(rotated)
  self.assertEqual(self.call(raw=self.sign(rotated,key=newer),key_entries=self.config+[new_entry],snapshot_pin_provider=lambda *_:pin)['reason'],'independent_epoch_and_atomic_staging_unavailable')
  with self.assertRaises(SnapshotHeld):self.call(key_entries=[new_entry])
 def test_provider_cannot_write_through_read_only_inspection(self):
  before=hashlib.sha256(Path(self.queue.path).read_bytes()).hexdigest()
  def malicious(db,*_):
   db.execute('CREATE TABLE forbidden_write(id INTEGER)')
   return 3
  with self.assertRaises(SnapshotHeld):self.call(snapshot_pin_provider=malicious)
  self.assertEqual(hashlib.sha256(Path(self.queue.path).read_bytes()).hexdigest(),before)
 def test_provider_cannot_reset_read_only_or_escape_transaction(self):
  before=hashlib.sha256(Path(self.queue.path).read_bytes()).hexdigest()
  for provider in ('snapshot_pin_provider',):
   for sql in ('PRAGMA query_only=OFF','CREATE TABLE forbidden_write(id INTEGER)','DELETE FROM reference_jobs','COMMIT'):
    observed=[]
    def attempt(context):
     observed.append(type(context).__name__)
     return context.execute(sql)
    with self.subTest(provider=provider,sql=sql),self.assertRaises(SnapshotHeld):self.call(**{provider:attempt})
    self.assertEqual(observed,['mappingproxy'])
    self.assertEqual(hashlib.sha256(Path(self.queue.path).read_bytes()).hexdigest(),before)
  def mutate(context):context['jobId']='changed';return 3
  with self.assertRaises(SnapshotHeld):self.call(snapshot_pin_provider=mutate)
  self.assertEqual(hashlib.sha256(Path(self.queue.path).read_bytes()).hexdigest(),before)
 def test_same_key_cannot_be_aliased_within_scope(self):
  for changes in (dict(keyId='another-key'),dict(issuerId='another-issuer')):
   with self.subTest(changes=changes),self.assertRaises(SnapshotHeld):pin_registry(self.config+[dict(self.config[0],**changes)])
 def test_ttl_and_page_count_constraints_hold(self):
  for key,value in [('expiresAt','2030-01-01T02:00:00.000Z'),('sourcePageBytes',[262145]),('selectedCount',513)]:
   changed=copy.deepcopy(self.statement);changed[key]=value;pin=self.statement_hash(changed)
   with self.subTest(key=key),self.assertRaises(SnapshotHeld):self.call(raw=self.sign(changed),snapshot_pin_provider=lambda *_:pin)
 def test_provisional_ttl_is_at_most_fifteen_minutes(self):
  self.assertEqual(self.call()['reason'],'independent_epoch_and_atomic_staging_unavailable')
  longer=copy.deepcopy(self.statement);longer['expiresAt']='2030-01-01T00:15:00.001Z';pin=self.statement_hash(longer)
  with self.assertRaises(SnapshotHeld):self.call(raw=self.sign(longer),snapshot_pin_provider=lambda *_:pin)
 def test_signed_page_bytes_exclude_lf_total_includes_lf(self):
  valid=copy.deepcopy(self.statement);valid['sourcePageBytes']=[262144];pin=self.statement_hash(valid)
  self.assertEqual(self.call(raw=self.sign(valid),snapshot_pin_provider=lambda *_:pin)['reason'],'independent_epoch_and_atomic_staging_unavailable')
  oversized=copy.deepcopy(self.statement);oversized['sourcePageHashes']=['1'*64]*48;oversized['sourcePageBytes']=[262144]*48;oversized['sourceCount']=48
  pin=self.statement_hash(oversized)
  with self.assertRaises(SnapshotHeld):self.call(raw=self.sign(oversized),snapshot_pin_provider=lambda *_:pin)
 def test_duplicate_scope_keys_are_not_cross_environment_shared(self):
  another=dict(self.config[0],environment='production')
  with self.assertRaises(SnapshotHeld):pin_registry(self.config+[another])

if __name__=='__main__':unittest.main()
