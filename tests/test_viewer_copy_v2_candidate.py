"""Fixture-only tests: no route, database, storage or auth is exercised."""
import base64
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import unittest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from processing import reference_protocol as p
from processing import viewer_copy_v2_candidate as c

VECTOR=Path(__file__).parent/'fixtures/accepted_copy_v2_proposal.json'

class ViewerCopyV2CandidateTests(unittest.TestCase):
    def setUp(self):
        vector=json.loads(VECTOR.read_text(encoding='utf8'))
        self.r=vector['receipt']; self.a=vector['acceptance']
        self.key=Ed25519PrivateKey.from_private_bytes(bytes(range(32))) # public fixture key
        self.pins={self.r['keyId']:{'issuerId':self.r['issuerId'],'publicKeyPem':vector['publicKeyPem'],
            'coordinatorAuthorityId':self.r['coordinatorAuthorityId'],'viewerPrincipalId':self.r['viewerPrincipalId'],'environment':self.r['environment']}}
        self.manifest={'schemaVersion':1,'jobId':self.r['jobId'],'generation':1,'environment':'staging',
            'pipelineDigest':self.r['pipelineDigest'],'engineVersions':{'odm':'synthetic'},
            'objects':[{'id':'object-b','role':'report','byteSize':0,'sha256':hashlib.sha256(b'').hexdigest()},
                       {'id':'object-a','role':'pointcloud','byteSize':3,'sha256':hashlib.sha256(b'abc').hexdigest()}]}
        self.manifest['resultManifestSha256']=p.digest(self.manifest)
        self.r['resultManifestSha256']=self.manifest['resultManifestSha256']
        self.a['resultManifestSha256']=self.manifest['resultManifestSha256']
        self.expected={k:self.r[k] for k in c.BINDINGS}
        self.rebind()

    def rebind(self):
        self.r['acceptancePayloadSha256']=hashlib.sha256(c.ACCEPTANCE_DOMAIN+p.canonical_json(self.a)).hexdigest()
        body={k:v for k,v in self.r.items() if k!='signature'}
        self.r['signature']=base64.urlsafe_b64encode(self.key.sign(c.DOMAIN+p.canonical_json(body))).decode().rstrip('=')

    def verify(self):
        return c._verify_v2_for_fixtures(p.canonical_json({'acceptance':self.a,'viewerReceipt':self.r}),expected=self.expected,result_manifest=self.manifest,trusted_keys=self.pins)

    def test_valid_full_closure_and_public_api_disabled(self):
        self.assertEqual(self.verify()['viewerAttemptId'],self.expected['viewerAttemptId'])
        with self.assertRaises(c.CandidateUnavailable):c.verify_copy_v2(b'{}')

    def test_valid_signature_with_wrong_scope_rejected(self):
        for field in ('coordinatorAuthorityId','viewerPrincipalId','preparedEnvelopeSha256','viewerAttemptId'):
            with self.subTest(field=field):
                saved=self.r[field]
                self.r[field]='e'*64 if field.endswith('Sha256') else ('99999999-9999-4999-8999-999999999999' if field.endswith('Id') and field=='viewerAttemptId' else 'other')
                self.rebind()
                with self.assertRaises(c.CandidateRejected):self.verify()
                self.r[field]=saved

    def test_outer_substitution_and_reordered_or_partial_closure_rejected(self):
        self.a['acceptedAt']='2026-11-04T00:00:01.000Z'
        with self.assertRaises(c.CandidateRejected):self.verify() # no new signed digest
        self.rebind();self.a['acceptedObjectIds'].reverse();self.rebind()
        with self.assertRaises(c.CandidateRejected):self.verify()
        self.a['acceptedObjectIds']=['object-b'];self.r['acceptedObjectIds']=['object-b'];self.r['objects']=self.r['objects'][:1];self.rebind()
        with self.assertRaises(c.CandidateRejected):self.verify()

    def test_duplicate_keys_unknown_fields_and_malformed_signature_rejected(self):
        raw=p.canonical_json({'acceptance':self.a,'viewerReceipt':self.r})
        duplicate=raw.replace(b'"protocol":',b'"protocol":"duplicate","protocol":',1)
        with self.assertRaises(c.CandidateRejected):c._verify_v2_for_fixtures(duplicate,expected=self.expected,result_manifest=self.manifest,trusted_keys=self.pins)
        self.r['unexpected']=True;self.rebind()
        with self.assertRaises(c.CandidateRejected):self.verify()
        del self.r['unexpected'];self.rebind();self.r['signature']+='='
        with self.assertRaises(c.CandidateRejected):self.verify()

    def test_wrong_key_and_mismatched_result_rejected(self):
        other=Ed25519PrivateKey.from_private_bytes(bytes(range(1,33))).public_key().public_bytes(serialization.Encoding.PEM,serialization.PublicFormat.SubjectPublicKeyInfo).decode()
        self.pins[self.r['keyId']]['publicKeyPem']=other
        with self.assertRaises(c.CandidateRejected):self.verify()
        self.pins[self.r['keyId']]['publicKeyPem']=self.key.public_key().public_bytes(serialization.Encoding.PEM,serialization.PublicFormat.SubjectPublicKeyInfo).decode()
        self.manifest['objects'][0]['sha256']='f'*64
        with self.assertRaises(c.CandidateRejected):self.verify()

    def test_signed_boolean_is_not_a_zero_byte_size(self):
        self.r['objects'][0]['byteSize']=False
        self.rebind()
        with self.assertRaises(c.CandidateRejected):self.verify()

    def test_key_scope_and_cross_identity_reuse_rejected(self):
        key=self.r['keyId']; saved=self.pins[key]['viewerPrincipalId']
        self.pins[key]['viewerPrincipalId']='other'
        with self.assertRaises(c.CandidateRejected):self.verify()
        self.pins[key]['viewerPrincipalId']=saved
        self.pins['other-key']=deepcopy(self.pins[key]);self.pins['other-key']['environment']='production'
        with self.assertRaises(c.CandidateRejected):self.verify()
        self.pins['other-key']['publicKeyPem']=Ed25519PrivateKey.from_private_bytes(bytes(range(1,33))).public_key().public_bytes(serialization.Encoding.PEM,serialization.PublicFormat.SubjectPublicKeyInfo).decode()
        with self.assertRaises(c.CandidateRejected):self.verify() # issuer reused across scopes

    def test_signed_outer_timestamp_requires_utc_milliseconds(self):
        for timestamp in ('2026-11-04T00:00:01Z','2026-11-04T00:00:01.000+00:00','2026-11-04T00:00:01.0000Z'):
            with self.subTest(timestamp=timestamp):
                self.a['acceptedAt']=timestamp;self.rebind()
                with self.assertRaises(c.CandidateRejected):self.verify()
