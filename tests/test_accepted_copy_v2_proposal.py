"""Synthetic contract vectors only; deliberately not an admission verifier."""
import base64
import hashlib
import json
import os
from pathlib import Path
import subprocess
import unittest
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from processing.reference_protocol import canonical_json

ROOT=Path(__file__).resolve().parents[1]
VECTOR=ROOT/'tests/fixtures/accepted_copy_v2_proposal.json'
DOMAIN=b'LTDS-VIEWER-ACCEPTED-COPY-ACK-V2\0'

class AcceptedCopyV2ProposalVectors(unittest.TestCase):
    def setUp(self):
        self.vector=json.loads(VECTOR.read_text(encoding='utf8'))
        self.receipt=self.vector['receipt']
        self.body={k:v for k,v in self.receipt.items() if k!='signature'}
        self.signature=base64.urlsafe_b64decode(self.receipt['signature']+'==')
        self.key=serialization.load_pem_public_key(self.vector['publicKeyPem'].encode())

    def test_python_signature_and_node_canonicalization(self):
        canonical=canonical_json(self.body)
        self.assertEqual(hashlib.sha256(canonical).hexdigest(),self.vector['bodyCanonicalSha256'])
        self.key.verify(self.signature,DOMAIN+canonical)
        viewer=os.environ.get('VIEWER_SOURCE')
        if not viewer:self.skipTest('VIEWER_SOURCE required for Node cross-language check')
        result=subprocess.run(['node',str(ROOT/'tests/fixtures/verify_accepted_copy_v2_proposal.js'),str(VECTOR)],
                              env=os.environ.copy(),capture_output=True,text=True,timeout=10,check=True)
        self.assertEqual(json.loads(result.stdout),{'verified':True,'bodyCanonicalSha256':self.vector['bodyCanonicalSha256']})

    def test_scope_swaps_and_wrong_domain_break_signature(self):
        for field in ('coordinatorAuthorityId','viewerPrincipalId','submissionId','activationId',
                      'viewerTaskId','viewerAttemptId','preparedEnvelopeSha256',
                      'acceptanceReceiptId','acceptanceStorageReceiptId','acceptancePayloadSha256','resultManifestSha256'):
            with self.subTest(field=field):
                changed={**self.body,field:'changed'}
                with self.assertRaises(InvalidSignature):self.key.verify(self.signature,DOMAIN+canonical_json(changed))
        with self.assertRaises(InvalidSignature):
            self.key.verify(self.signature,b'LTDS-VIEWER-ACCEPTED-COPY-ACK-V1\0'+canonical_json(self.body))

    def test_synthetic_vector_preserves_nonlexical_acceptance_order(self):
        objects=self.body['objects']
        self.assertEqual(self.body['acceptedObjectIds'],[v['id'] for v in objects])
        self.assertEqual(self.body['acceptedObjectIds'],self.vector['acceptance']['acceptedObjectIds'])
        self.assertNotEqual(self.body['acceptedObjectIds'],sorted(self.body['acceptedObjectIds']))
        self.assertEqual(len(set(self.body['acceptedObjectIds'])),len(objects))
        self.assertEqual(len({self.body[x] for x in ('receiptId','storageReceiptId','acceptanceReceiptId','acceptanceStorageReceiptId')}),4)
        self.assertEqual(len(self.signature),64)
        self.assertEqual(base64.urlsafe_b64encode(self.signature).decode().rstrip('='),self.receipt['signature'])

    def test_outer_mutation_with_same_ids_changes_signed_digest(self):
        acceptance=self.vector['acceptance']
        def digest(value):return hashlib.sha256(b'LTDS-OUTER-ACCEPTANCE-V1\0'+canonical_json(value)).hexdigest()
        self.assertEqual(digest(acceptance),self.body['acceptancePayloadSha256'])
        changed={**acceptance,'acceptedAt':'2026-11-04T00:00:01.000Z'}
        self.assertEqual(changed['receiptId'],acceptance['receiptId'])
        self.assertNotEqual(digest(changed),self.body['acceptancePayloadSha256'])

    def test_valid_signature_is_not_independent_scope_authority(self):
        # This only illustrates a required semantic comparison, not a validator.
        expected={'coordinatorAuthorityId':'coord-test','viewerPrincipalId':'viewer-test',
                  'submissionId':'55555555-5555-4555-8555-555555555555'}
        self.assertTrue(all(self.body[k]==v for k,v in expected.items()))
        changed={**self.body,'viewerPrincipalId':'different-principal'}
        attacker=Ed25519PrivateKey.from_private_bytes(bytes(range(32))) # public test key only
        signature=attacker.sign(DOMAIN+canonical_json(changed))
        self.key.verify(signature,DOMAIN+canonical_json(changed))
        self.assertFalse(all(changed[k]==v for k,v in expected.items()))

    def test_documented_negative_mutations_invalidate_original_signature(self):
        # Re-signed malicious cases additionally require independent semantic
        # scope/closure checks; this test does not implement such a verifier.
        for case in self.vector['negativeCases']:
            with self.subTest(case=case['case']):
                changed={**self.body,case['field']:case['value']}
                with self.assertRaises(InvalidSignature):
                    self.key.verify(self.signature,DOMAIN+canonical_json(changed))
