"""Synthetic candidate vectors only; does not establish actual custody."""
import copy
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from processing import direct_coordinator_receipt_candidate as c


# Public RFC 8032 test seed, not a credential and never used outside fixtures.
TEST_SEED=bytes.fromhex('9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60')


def body():
    return dict(receiptId='00000000-0000-4000-8000-000000000001',
        operationId='00000000-0000-4000-8000-000000000002',issuedAt='2026-11-06T12:00:00.000Z',
        coordinatorAuthorityId='00000000-0000-4000-8000-000000000003',viewerPrincipalId='viewer-test',
        viewerDestinationId='destination-test',submissionId='00000000-0000-4000-8000-000000000004',
        viewerTaskId='00000000-0000-4000-8000-000000000005',viewerAttemptId='00000000-0000-4000-8000-000000000006',
        jobId='00000000-0000-4000-8000-000000000007',generation=1,environment='production',
        intakeProtocol='ltds-direct-intake-v1',mode='direct',modeRevision=1,
        submissionIntentSha256='1'*64,sourceSnapshotSha256='2'*64,originalPathInventorySha256='3'*64,
        sourceManifestSha256='4'*64,derivedManifestSha256='5'*64,engineMappingSha256='6'*64,
        workerClosureReceiptSha256='7'*64,sourceObjects=[dict(id='image-1',byteSize=4,sha256='8'*64)],
        derivedObjects=[dict(id='gcp-1',byteSize=5,sha256='9'*64)],retentionGuarantee=c.GUARANTEE)


class CandidateReceiptTests(unittest.TestCase):
    def setUp(self):
        self.body=body();self.key=Ed25519PrivateKey.from_private_bytes(TEST_SEED)
        self.receipt=c.issue(self.body,issuer_id='coordinator-test',key_id='test-key',private_key=self.key)
    def verify(self,value=None,expected=None):
        return c.verify(value or self.receipt,expected_body=expected or self.body,
                        issuer_id='coordinator-test',key_id='test-key',public_key=self.key.public_key())
    def test_exact_signature_and_binding(self):self.assertEqual(self.verify(),self.receipt)
    def test_fixed_public_cross_language_vector(self):
        vector=json.loads((Path(__file__).resolve().parents[1]/'contracts/direct-coordinator-source-receipt-v1.vector.json').read_text())
        self.assertEqual(vector['receipt'],self.receipt)
        self.assertEqual(vector['domainHex'],c.DOMAIN.hex())
        self.assertEqual(vector['unsignedCanonicalUtf8'],c.rfc8785.dumps({k:v for k,v in self.receipt.items() if k!='signature'}).decode())
    def test_reject_changed_identity_or_type(self):
        for field,value in (('generation',True),('generation',1.0),('modeRevision',0),('environment','other'),
                            ('jobId','bad'),('derivedManifestSha256','A'*64),('retentionGuarantee','none')):
            with self.subTest(field=field,value=value):
                changed=copy.deepcopy(self.body);changed[field]=value
                with self.assertRaises(c.CoordinatorReceiptRejected):self.verify(expected=changed)
    def test_sorted_disjoint_and_exact_object_set(self):
        for objects in ([dict(id='z',byteSize=1,sha256='a'*64),dict(id='a',byteSize=1,sha256='b'*64)],
                        [dict(id='gcp-1',byteSize=1,sha256='a'*64)]):
            changed=copy.deepcopy(self.body);changed['sourceObjects']=objects
            with self.assertRaises(c.CoordinatorReceiptRejected):c.validate_body(changed)
    def test_ten_thousand_photos_plus_derived_fit_candidate_budget(self):
        value=copy.deepcopy(self.body)
        value['sourceObjects']=[dict(id=f'image-{i:05}',byteSize=5_000_000,sha256='a'*64) for i in range(10000)]
        raw=c.rfc8785.dumps(c.validate_body(value))
        self.assertLess(len(raw),c.MAX_BODY_BYTES);self.assertEqual(len(value['sourceObjects']),10000)
        self.assertGreaterEqual(c.MAX_OBJECTS,10001)
    def test_durable_exact_replay_and_conflicting_intent_hold(self):
        with tempfile.TemporaryDirectory() as root:
            path=Path(root)/'candidate.sqlite'
            with closing(sqlite3.connect(path)) as db:
                db.execute('PRAGMA synchronous=FULL')
                first=c.persist_candidate(db,self.receipt,expected_body=self.body,issuer_id='coordinator-test',key_id='test-key',public_key=self.key.public_key())
            with closing(sqlite3.connect(path)) as db:
                db.execute('PRAGMA synchronous=FULL')
                self.assertEqual(c.persist_candidate(db,self.receipt,expected_body=self.body,issuer_id='coordinator-test',key_id='test-key',public_key=self.key.public_key()),first)
                changed=copy.deepcopy(self.body);changed['submissionIntentSha256']='a'*64
                another=c.issue(changed,issuer_id='coordinator-test',key_id='test-key',private_key=self.key)
                with self.assertRaisesRegex(c.CoordinatorReceiptRejected,'persisted receipt conflict'):
                    c.persist_candidate(db,another,expected_body=changed,issuer_id='coordinator-test',key_id='test-key',public_key=self.key.public_key())
                self.assertEqual(db.execute('SELECT receipt FROM direct_source_receipt_candidate').fetchone()[0],first)


if __name__=='__main__':unittest.main()
