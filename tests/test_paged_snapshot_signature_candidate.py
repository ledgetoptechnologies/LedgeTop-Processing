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
from processing.paged_snapshot_signature_candidate import PROTOCOL,DOMAIN,pin_registry,inspect,SnapshotHeld

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
  self.config=[dict(principalId='viewer-a',environment='staging',issuerId='issuer-1',keyId='key-1',purpose='paged-source-snapshot',publicKeyPem=pem)];self.registry=pin_registry(self.config)
  self.statement=dict(protocol=PROTOCOL,schemaVersion=1,issuerId='issuer-1',keyId='key-1',snapshotId='snapshot-1',principalId='viewer-a',environment='staging',coordinatorAuthorityId=self.queue.authority_id,jobId=self.job['jobId'],generation=self.job['generation'],submissionId=self.env['submission']['submissionId'],activationId=self.env['activationId'],viewerTaskId=self.env['viewerTaskId'],viewerAttemptId=self.env['viewerAttemptId'],preparedEnvelopeSha256=found['envelopeSha256'],datasetId=self.env['submission']['manifest']['id'],datasetManifestSha256=self.env['submission']['manifest']['sha256'],pipelineDigest=p.digest(self.env['submission']['pipeline']),sourcePageHashes=['1'*64],sourcePageBytes=[100],sourceCount=1,selectedPageHashes=['2'*64],selectedPageBytes=[120],selectedCount=1,inputManifestSha256='3'*64,processingClosureSha256='4'*64,gcpEvidenceSha256=None,sourceRevision='revision-1',revocationEpoch=3,issuedAt='2030-01-01T00:00:00.000Z',expiresAt='2030-01-01T01:00:00.000Z')
  self.pin=self.statement_hash(self.statement)
 def statement_hash(self,statement):return hashlib.sha256(DOMAIN+p.canonical_json({k:v for k,v in statement.items() if k!='signature'})).hexdigest()
 def sign(self,value=None,key=None):
  value=copy.deepcopy(self.statement if value is None else value);sig=(key or self.private).sign(DOMAIN+p.canonical_json(value));value['signature']=base64.urlsafe_b64encode(sig).decode().rstrip('=');return p.canonical_json(value)
 def call(self,raw=None,**kwargs):
  args=dict(principal='viewer-a',environment='staging',expected_authority_id=self.queue.authority_id,key_registry=self.registry,epoch_provider=lambda *_:3,snapshot_pin_provider=lambda *_:self.pin);args.update(kwargs)
  return inspect(self.store,self.env['submission']['submissionId'],self.sign() if raw is None else raw,**args)
 def test_valid_exact_replay_and_reopen_are_held_without_writes(self):
  before=hashlib.sha256(Path(self.queue.path).read_bytes()).hexdigest();first=self.call();second=self.call();self.assertEqual(first,second);self.assertEqual(first['reason'],'atomic_staging_unavailable')
  self.store=ReferenceStore(self.queue,jcs_runtime_enabled=True);self.assertEqual(self.call(),first);self.assertEqual(hashlib.sha256(Path(self.queue.path).read_bytes()).hexdigest(),before)
 def test_missing_independent_sources_never_authorizes(self):
  self.assertEqual(self.call(epoch_provider=None)['reason'],'trusted_snapshot_or_epoch_unavailable')
  self.assertEqual(self.call(snapshot_pin_provider=None)['reason'],'trusted_snapshot_or_epoch_unavailable')
 def test_wrong_signature_owner_environment_snapshot_or_epoch_hold(self):
  changed=copy.deepcopy(self.statement);changed['sourceCount']=2
  unknown=copy.deepcopy(self.statement);unknown['keyId']='unknown-key'
  cases=[dict(raw=self.sign(key=Ed25519PrivateKey.generate())),dict(raw=self.sign(unknown)),dict(raw=self.sign(changed)),dict(principal='viewer-b'),dict(environment='production'),dict(epoch_provider=lambda *_:4),dict(snapshot_pin_provider=lambda *_:'f'*64)]
  for args in cases:
   with self.subTest(args=list(args)),self.assertRaises(SnapshotHeld):self.call(**args)
 def test_wrong_prepared_attempt_or_authority_holds_even_when_signed_and_pinned(self):
  for key,value in [('viewerAttemptId',str(uuid.uuid4())),('coordinatorAuthorityId',str(uuid.uuid4())),('datasetManifestSha256','f'*64)]:
   changed=copy.deepcopy(self.statement);changed[key]=value;pin=self.statement_hash(changed)
   with self.subTest(key=key),self.assertRaises(SnapshotHeld):self.call(raw=self.sign(changed),snapshot_pin_provider=lambda *_:pin)
 def test_wrong_configured_authority_cannot_inspect_or_write(self):
  wrong=str(uuid.uuid4());changed=copy.deepcopy(self.statement);changed['coordinatorAuthorityId']=wrong;pin=self.statement_hash(changed)
  before=hashlib.sha256(Path(self.queue.path).read_bytes()).hexdigest()
  with self.assertRaises(SnapshotHeld):self.call(raw=self.sign(changed),expected_authority_id=wrong,snapshot_pin_provider=lambda *_:pin)
  self.assertEqual(hashlib.sha256(Path(self.queue.path).read_bytes()).hexdigest(),before)
 def test_provider_cannot_write_through_read_only_inspection(self):
  before=hashlib.sha256(Path(self.queue.path).read_bytes()).hexdigest()
  def malicious(db,*_):
   db.execute('CREATE TABLE forbidden_write(id INTEGER)')
   return 3
  with self.assertRaises(SnapshotHeld):self.call(epoch_provider=malicious)
  self.assertEqual(hashlib.sha256(Path(self.queue.path).read_bytes()).hexdigest(),before)
 def test_ttl_and_page_count_constraints_hold(self):
  for key,value in [('expiresAt','2030-01-01T02:00:00.000Z'),('sourcePageBytes',[262145]),('selectedCount',513),('sourceRevision','')]:
   changed=copy.deepcopy(self.statement);changed[key]=value;pin=self.statement_hash(changed)
   with self.subTest(key=key),self.assertRaises(SnapshotHeld):self.call(raw=self.sign(changed),snapshot_pin_provider=lambda *_:pin)
 def test_signed_page_bytes_exclude_lf_total_includes_lf(self):
  valid=copy.deepcopy(self.statement);valid['sourcePageBytes']=[262144];pin=self.statement_hash(valid)
  self.assertEqual(self.call(raw=self.sign(valid),snapshot_pin_provider=lambda *_:pin)['reason'],'atomic_staging_unavailable')
  oversized=copy.deepcopy(self.statement);oversized['sourcePageHashes']=['1'*64]*48;oversized['sourcePageBytes']=[262144]*48;oversized['sourceCount']=48
  pin=self.statement_hash(oversized)
  with self.assertRaises(SnapshotHeld):self.call(raw=self.sign(oversized),snapshot_pin_provider=lambda *_:pin)
 def test_duplicate_scope_keys_are_not_cross_environment_shared(self):
  another=dict(self.config[0],environment='production')
  with self.assertRaises(SnapshotHeld):pin_registry(self.config+[another])

if __name__=='__main__':unittest.main()
