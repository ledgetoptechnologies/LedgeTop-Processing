"""Synthetic public test root and in-memory inputs; no live trust anchor."""
import base64
import json
from pathlib import Path
import unittest
from unittest.mock import patch
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding,PublicFormat
from processing import reference_v2_envelope_candidate as env,reference_v2_registry_candidate as reg

FIX=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v2-envelope-experiment.json').read_text())

class RegistryTests(unittest.TestCase):
    def setUp(self):
        self.root=Ed25519PrivateKey.from_private_bytes(bytes.fromhex(FIX['testSeedHex']))
        self.public=bytes.fromhex(FIX['publicKeyRawHex'])
        self.value=dict(version=1,epoch='epoch-1',revision=2,issuedAt=1000,expiresAt=1100,keys=[dict(
            issuer='coordinator',keyId='test-key-1',purpose='grant',audience='viewer',operation='admit',publicKey=base64.urlsafe_b64encode(self.public).decode().rstrip('='),notBefore=900,notAfter=1200,revoked=False)])
        self.roles=(('coordinator','grant','viewer','admit'),)
        self.flag=patch.object(reg,'QUALIFIED',True);self.flag.start();self.addCleanup(self.flag.stop)
    def signed(self,value):
        raw=env._canonical(value);sig=base64.urlsafe_b64encode(self.root.sign(reg.DOMAIN+raw)).decode().rstrip('=');return raw,sig
    def load(self,value,**kwargs):
        raw,sig=self.signed(value);return reg.load_signed(raw,sig,root_public_key=self.public,expected_epoch='epoch-1',minimum_revision=2,now=1050,role_policy=self.roles,**kwargs)
    def test_signed_registry_and_revocation(self):
        result=self.load(self.value);self.assertFalse(result.entries[('coordinator','test-key-1','grant')].revoked)
        self.assertEqual((result.entries[('coordinator','test-key-1','grant')].not_before,result.entries[('coordinator','test-key-1','grant')].not_after),(1000,1100))
        vector=FIX['vectors'][0];wire=vector['canonical'].encode('ascii');value=json.loads(wire);payload=FIX['canonicalPayload'].encode('ascii')
        expected={k:value[k] for k in ('kind','issuer','audience','operation','epoch','requestHash','deliveryId','nonce','actor','payloadSha256')}
        with patch.object(env,'QUALIFIED',True):
            self.assertEqual(env.verify(wire,vector['signatureBase64url'],payload_raw=payload,expected_payload=json.loads(payload),registry=result,expected=expected,now=1050)['issuer'],'coordinator')
            with self.assertRaises(env.EnvelopeHeld):env.verify(wire,vector['signatureBase64url'],payload_raw=payload,expected_payload=json.loads(payload),registry=result,expected=expected,now=1100)
        changed=json.loads(json.dumps(self.value));changed['revision']=3;changed['keys'][0]['revoked']=True
        self.assertTrue(self.load(changed).entries[('coordinator','test-key-1','grant')].revoked)
    def test_stale_expired_wrong_root_and_malformed_hold(self):
        raw,sig=self.signed(self.value)
        for kwargs in (dict(root_public_key=bytes(32),expected_epoch='epoch-1',minimum_revision=2,now=1050),
                       dict(root_public_key=self.public,expected_epoch='epoch-2',minimum_revision=2,now=1050),
                       dict(root_public_key=self.public,expected_epoch='epoch-1',minimum_revision=3,now=1050),
                       dict(root_public_key=self.public,expected_epoch='epoch-1',minimum_revision=2,now=1100)):
            with self.assertRaises(reg.RegistryHeld):reg.load_signed(raw,sig,role_policy=self.roles,**kwargs)
        with self.assertRaises(reg.RegistryHeld):reg.load_signed(raw+b' ',sig,root_public_key=self.public,expected_epoch='epoch-1',minimum_revision=2,now=1050,role_policy=self.roles)
        duplicate=json.loads(json.dumps(self.value));duplicate['keys'].append(duplicate['keys'][0])
        with self.assertRaises(reg.RegistryHeld):self.load(duplicate)
        for field in ('issuer','purpose','audience','operation'):
            altered=json.loads(json.dumps(self.value));altered['keys'][0][field]='wrong'
            with self.subTest(field=field),self.assertRaises(reg.RegistryHeld):self.load(altered)
        alias=json.loads(json.dumps(self.value));alias['keys'].append(dict(alias['keys'][0],keyId='alias'))
        with self.assertRaises(reg.RegistryHeld):self.load(alias)
    def test_disabled_by_default(self):
        with patch.object(reg,'QUALIFIED',False),self.assertRaises(reg.RegistryHeld):self.load(self.value)
    def test_public_registry_interop_vector(self):
        vector=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v2-registry-experiment.json').read_text())
        root=Ed25519PrivateKey.from_private_bytes(bytes.fromhex(vector['testRootSeedHex']))
        self.assertEqual(root.public_key().public_bytes(Encoding.DER,PublicFormat.SubjectPublicKeyInfo).hex(),vector['rootPublicKeySpkiDerHex'])
        raw=vector['canonical'].encode('ascii')
        self.assertEqual(base64.urlsafe_b64encode(root.sign(reg.DOMAIN+raw)).decode().rstrip('='),vector['signatureBase64url'])
        public=root.public_key().public_bytes(Encoding.Raw,PublicFormat.Raw)
        self.assertNotEqual(public,self.public)
        keys=reg.load_signed(raw,vector['signatureBase64url'],root_public_key=public,expected_epoch='epoch-1',minimum_revision=2,now=1050,role_policy=self.roles)
        self.assertEqual(keys.entries[('coordinator','test-key-1','grant')].public_key,self.public)
