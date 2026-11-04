"""Offline signer/verifier experiment only, with no route or real data."""
import base64
from copy import deepcopy
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
import uuid
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from processing import reference_protocol as p
from processing import viewer_copy_journal_candidate as j
from processing import viewer_copy_v2_candidate as v

VIEWER_REV='3c7f727c9408238742b5fa422136658133cb3f83'
PATHS=('apps/viewer/server/referenceProtocol.js','apps/viewer/server/referenceReceiptV2Candidate.js',
       'apps/viewer/test/helpers/accepted-copy-v2-candidate-fixture.js')


class CrossLanguageNegativeTests(unittest.TestCase):
    def test_signed_receipt_replay_and_negative_cases(self):
        source=os.environ.get('VIEWER_SOURCE')
        if not source:self.skipTest('explicit VIEWER_SOURCE required')
        root=Path(source).resolve()
        def run(args):
            return subprocess.run(args,cwd=root,stdout=subprocess.PIPE,stderr=subprocess.PIPE,
                text=True,timeout=10,check=True).stdout.strip()
        self.assertEqual(run(['git','rev-parse','HEAD']),VIEWER_REV)
        self.assertEqual(run(['git','status','--porcelain','--untracked-files=all','--',*PATHS]),'')
        for path in PATHS:run(['git','ls-files','--error-unmatch','--',path])
        fixture=json.loads(run(['node',PATHS[2]]))
        raw=base64.b64decode(fixture['rawBase64'],validate=True)
        kwargs=dict(expected=fixture['expected'],result_manifest=fixture['resultManifest'],trusted_keys=fixture['trustedKeys'])
        v._verify_v2_for_fixtures(raw,**kwargs)
        altered=json.loads(raw);altered['viewerReceipt']['viewerPrincipalId']='different'
        with self.assertRaises(v.CandidateRejected):v._verify_v2_for_fixtures(p.canonical_json(altered),**kwargs)
        altered=json.loads(raw);altered['acceptance']['acceptedAt']='2026-11-04T00:00:02.000Z'
        with self.assertRaises(v.CandidateRejected):v._verify_v2_for_fixtures(p.canonical_json(altered),**kwargs)
        for field in ('providerTaskId','sourceManifestSha256','derivedManifestSha256',
                      'sourceReceiptSha256','derivedReceiptSha256'):
            changed=json.loads(raw);changed['viewerReceipt'][field]='1'*64
            with self.subTest(field=field), self.assertRaises(v.CandidateRejected):
                v._verify_v2_for_fixtures(p.canonical_json(changed),**kwargs)
        bad=deepcopy(kwargs)
        bad['trusted_keys'][fixture['viewerReceipt']['keyId']]['publicKeyPem']=Ed25519PrivateKey.from_private_bytes(bytes(range(1,33))).public_key().public_bytes(serialization.Encoding.PEM,serialization.PublicFormat.SubjectPublicKeyInfo).decode()
        with self.assertRaises(v.CandidateRejected):v._verify_v2_for_fixtures(raw,**bad)
        with tempfile.TemporaryDirectory(prefix='ltds-copy-journal-') as directory:
            root=Path(directory);owner=uuid.uuid4().hex
            (root/'.fixture-owner').write_text(owner,encoding='ascii')
            db=j._open_fixture(root/'journal.sqlite',owner)
            try:
                self.assertEqual(j._journal_fixture(db,raw,**kwargs),'stored')
                self.assertEqual(j._journal_fixture(db,raw,**kwargs),'replay')
                with self.assertRaises(j.JournalConflict):j._journal_fixture(db,b' '+raw,**kwargs)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM fixture_copies').fetchone()[0],1)
            finally:db.close()
