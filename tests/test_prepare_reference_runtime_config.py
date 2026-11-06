"""Only newly created synthetic private inputs; no installed credentials or NAS."""
import copy
import hashlib
import json
import os
from pathlib import Path
import stat
import unittest
import uuid
from unittest.mock import patch

from cryptography.hazmat.primitives import serialization as s
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from ops import prepare_reference_runtime_config as prep
from tests import test_prepare_reference_worker_config as fixtures


@unittest.skipUnless(os.name == 'posix' and os.geteuid() != 0, 'owner-private Linux fixture')
class RuntimePreparationTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.PrivateWorkerPreparationTests(); self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        f = self.fixture; self.out = f.root/'runtime-output'
        manifest = f.file('manifest.json', prep.canonical(f.m))
        gate = f.file('copied-gate.json', prep.canonical(f.gate))
        self.keys = {}
        for role in ('source', 'snapshot', 'copy'):
            key = Ed25519PrivateKey.generate()
            self.keys[role] = dict(issuerId=role+'-issuer',keyId=role+'-key',privateKey=f.file(role+'.pem',
                key.private_bytes(s.Encoding.PEM,s.PrivateFormat.PKCS8,s.NoEncryption())))
        self.spec = dict(schemaVersion=1,protocol=prep.PROTOCOL,preparationId=str(uuid.uuid4()),authorityId=f.authority,
            listener=dict(host='0.0.0.0',port=8443,certificate='/run/processing-tls/coordinator.crt',
                privateKey='/run/processing-tls/coordinator.key',clientCA='/run/processing-tls/ca.crt'),
            coordinatorOrigin='https://192.0.2.9:18443',nodes=[dict(manifest=manifest,gate=gate,
                endpoint='http://127.0.0.1:13004',workerId='original-worker',instanceId='original-instance',
                resultOrigin='https://192.0.2.8:18445',bearer=f.file('claim-token',b'c'*40),
                resultBearer=f.file('result-token',b'r'*40),outputBudget=123456)],
            environments=dict(production=dict(providerId=str(uuid.uuid4()),principalId='viewer-source',
                bearer=f.file('viewer-token',b'v'*40),viewerCaPath='/run/processing-ca/ca.crt',**self.keys)),
            forbiddenCredentials=[f.file('legacy-token',b'l'*40)],streamBudget=dict(maxConcurrent=2,maxObjectBytes=1000000),
            sourceSpool=dict(root='/spool/objects',mountPath='/spool',mountId='a'*64,markerName='.spool-id',
                safetyFloorBytes=100,maxObjectBytes=1000000,chunkBytes=1024),outputDirectory=str(self.out))

    def test_private_outputs_preserve_bindings_and_separate_signing_roles(self):
        before=(self.fixture.root/'runtime.json').read_bytes()
        receipt=prep.prepare(self.spec)
        boot=json.loads((self.out/'bootstrap.json').read_bytes()); final=json.loads((self.out/'final.json').read_bytes())
        viewer=json.loads((self.out/'viewer-production.json').read_bytes())
        self.assertEqual(boot['schemaVersion'],2);self.assertEqual(final['schemaVersion'],6)
        self.assertNotIn('sourceRelease',final);self.assertEqual(final['viewerAcceptance'],{})
        self.assertEqual(boot['recoveryBindings'],final['recoveryBindings'])
        self.assertEqual(final['recoveryBindings']['main']['gateGeneration'],7)
        self.assertNotIn('PRIVATE_KEY',str(final));self.assertNotIn('PRIVATE KEY',str(final))
        self.assertEqual(viewer['VIEWER_DEPLOYMENT_ENVIRONMENT'],'production')
        self.assertEqual(viewer['VIEWER_SOURCE_RELEASE_ENABLED'],'false')
        self.assertFalse(receipt['runtimePreflightProven']);self.assertEqual(stat.S_IMODE(self.out.stat().st_mode),0o700)
        for path in self.out.iterdir():self.assertEqual(stat.S_IMODE(path.stat().st_mode),0o600)
        for name,pin in receipt['files'].items():
            self.assertEqual(hashlib.sha256((self.out/name).read_bytes()).hexdigest(),pin['sha256'])
        for role in self.keys.values():self.assertNotIn(prep.pinned(role['privateKey']).decode(),str(receipt))
        self.assertEqual(before,(self.fixture.root/'runtime.json').read_bytes())

    def test_reused_signing_key_refuses_before_any_output(self):
        self.spec['environments']['production']['copy']['privateKey']=self.keys['source']['privateKey']
        with self.assertRaisesRegex(Exception,'distinct_environment_signing_roles'):prep.prepare(self.spec)
        self.assertFalse(self.out.exists())

    def test_authority_and_original_gate_drift_refuse(self):
        self.spec['authorityId']=str(uuid.uuid4())
        with self.assertRaisesRegex(Exception,'physical_binding'):prep.prepare(self.spec)
        self.assertFalse(self.out.exists())

    def test_key_hash_drift_refuses(self):
        artifact=self.keys['source']['privateKey'];Path(artifact['path']).write_bytes(b'changed')
        with self.assertRaises(Exception):prep.prepare(self.spec)
        self.assertFalse(self.out.exists())

    def test_symlink_key_refuses(self):
        artifact=self.keys['source']['privateKey']; original=Path(artifact['path'])
        link=original.with_suffix('.link');link.symlink_to(original);artifact['path']=str(link)
        with self.assertRaises(Exception):prep.prepare(self.spec)
        self.assertFalse(self.out.exists())

    def test_hardlinked_key_refuses(self):
        original=Path(self.keys['source']['privateKey']['path']);os.link(original,original.with_suffix('.link'))
        with self.assertRaises(Exception):prep.prepare(self.spec)
        self.assertFalse(self.out.exists())

    def test_private_key_permissions_refuse(self):
        Path(self.keys['source']['privateKey']['path']).chmod(0o644)
        with self.assertRaises(Exception):prep.prepare(self.spec)
        self.assertFalse(self.out.exists())

    def test_token_collision_refuses(self):
        self.spec['environments']['production']['bearer']=self.spec['nodes'][0]['bearer']
        with self.assertRaisesRegex(Exception,'disjoint_private_roles'):prep.prepare(self.spec)
        self.assertFalse(self.out.exists())

    def test_unknown_environment_or_field_refuses(self):
        self.spec['environments']['development']=self.spec['environments'].pop('production')
        with self.assertRaises(Exception):prep.prepare(self.spec)
        self.assertFalse(self.out.exists())

    def test_unknown_spec_field_refuses_before_output(self):
        self.spec['sourceRelease']={'enabled':True}
        with self.assertRaises(Exception):prep.prepare(self.spec)
        self.assertFalse(self.out.exists())

    def test_gate_hash_matches_but_identity_differs_refuses(self):
        gate=copy.deepcopy(self.fixture.gate);gate['engineId']='wrong-daemon'
        self.spec['nodes'][0]['gate']=self.fixture.file('wrong-gate',prep.canonical(gate))
        # Even a separately repinned manifest cannot silently change physical identity.
        m=copy.deepcopy(self.fixture.m);m['gate']['sha256']=self.spec['nodes'][0]['gate']['sha256']
        self.spec['nodes'][0]['manifest']=self.fixture.file('repinned-manifest',prep.canonical(m))
        with self.assertRaisesRegex(Exception,'physical_binding'):prep.prepare(self.spec)
        self.assertFalse(self.out.exists())

    def test_partial_fsync_failure_retains_and_refuses_retry(self):
        with patch.object(prep.os,'fsync',side_effect=OSError('synthetic fsync failure')):
            with self.assertRaises(OSError):prep.prepare(self.spec)
        self.assertTrue(self.out.is_dir()); retained={p.name:p.read_bytes() for p in self.out.iterdir()}
        with self.assertRaisesRegex(Exception,'new_private_output'):prep.prepare(self.spec)
        self.assertEqual(retained,{p.name:p.read_bytes() for p in self.out.iterdir()})

    def test_existing_output_never_overwritten(self):
        self.out.mkdir(mode=0o700);(self.out/'sentinel').write_bytes(b'original')
        with self.assertRaisesRegex(Exception,'new_private_output'):prep.prepare(self.spec)
        self.assertEqual((self.out/'sentinel').read_bytes(),b'original')

    def test_non_ed25519_private_key_refuses(self):
        self.spec['environments']['production']['source']['privateKey']=self.fixture.spec['tls']['privateKey']
        with self.assertRaisesRegex(Exception,'ed25519_private_signer'):prep.prepare(self.spec)
        self.assertFalse(self.out.exists())
