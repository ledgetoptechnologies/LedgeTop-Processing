"""Deterministic public test keys only. No network or runtime authority."""
from dataclasses import replace
import base64
import json
from pathlib import Path
import unittest
from unittest.mock import patch
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding,PublicFormat
from processing import reference_v2_envelope_candidate as m

FIX=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v2-envelope-experiment.json').read_text())

class EnvelopeTests(unittest.TestCase):
    def setUp(self):
        self.flag=patch.object(m,'QUALIFIED',True);self.flag.start();self.addCleanup(self.flag.stop)
        self.public=bytes.fromhex(FIX['publicKeyRawHex'])
    def verify(self,raw,signature,**kwargs):
        payload=FIX['canonicalPayload'].encode('ascii')
        return m.verify(raw,signature,payload_raw=payload,expected_payload=json.loads(payload),**kwargs)
    def registry(self,keys):
        return m.Registry(keys,role_policy=(('coordinator','grant','viewer','admit'),('viewer','observation','processing','reconcile')))
    def parts(self,index=0):
        v=FIX['vectors'][index];value=json.loads(v['canonical']);raw=v['canonical'].encode('ascii')
        expected={k:value[k] for k in ('kind','issuer','audience','operation','epoch','requestHash','deliveryId','nonce','actor','payloadSha256')}
        key=m.Key(value['issuer'],value['keyId'],value['kind'],self.public,900,1200,False,value['audience'],value['operation'])
        return v,raw,expected,key
    def test_both_fixed_public_vectors(self):
        private=Ed25519PrivateKey.from_private_bytes(bytes.fromhex(FIX['testSeedHex']))
        self.assertEqual(private.public_key().public_bytes(Encoding.DER,PublicFormat.SubjectPublicKeyInfo).hex(),FIX['publicKeySpkiDerHex'])
        for index in (0,1):
            v,raw,expected,key=self.parts(index)
            self.assertEqual(base64.urlsafe_b64encode(private.sign(m.DOMAINS[v['kind']]+raw)).decode().rstrip('='),v['signatureBase64url'])
            self.assertEqual(self.verify(raw,v['signatureBase64url'],registry=self.registry([key]),expected=expected,now=1050),json.loads(raw))
    def test_scope_expiry_revocation_rotation_and_malformed_signatures_hold(self):
        v,raw,expected,key=self.parts();reg=self.registry([key])
        for field in expected:
            altered=dict(expected);altered[field]='different'
            with self.subTest(field=field),self.assertRaises(m.EnvelopeHeld):self.verify(raw,v['signatureBase64url'],registry=reg,expected=altered,now=1050)
        for now in (999,1100):
            with self.assertRaises(m.EnvelopeHeld):self.verify(raw,v['signatureBase64url'],registry=reg,expected=expected,now=now)
        for changed in (replace(key,revoked=True),replace(key,key_id='other'),replace(key,purpose='observation'),replace(key,not_after=1049)):
            with self.assertRaises(m.EnvelopeHeld):self.verify(raw,v['signatureBase64url'],registry=self.registry([changed]),expected=expected,now=1050)
        for signature in ('',v['signatureBase64url']+'=',v['signatureBase64url'][:-1]+'B'):
            with self.assertRaises(m.EnvelopeHeld):self.verify(raw,signature,registry=reg,expected=expected,now=1050)
        for bad in (raw+b' ',raw.replace(b'"version":1',b'"version":1,"version":1'),raw.replace(b'"version":1',b'"version":1.0'),raw.replace(b'worker-1',b'worker-2')):
            with self.assertRaises(m.EnvelopeHeld):self.verify(bad,v['signatureBase64url'],registry=reg,expected=expected,now=1050)
        new_public=Ed25519PrivateKey.from_private_bytes(bytes([42])*32).public_key().public_bytes(Encoding.Raw,PublicFormat.Raw)
        rotated=replace(key,key_id='new-key',public_key=new_public)
        self.assertEqual(self.verify(raw,v['signatureBase64url'],registry=self.registry([key,rotated]),expected=expected,now=1050)['keyId'],key.key_id)
        with self.assertRaises(m.EnvelopeHeld):self.registry([key,key])
        with self.assertRaises(m.EnvelopeHeld):self.registry([key,replace(key,key_id='alias')])
        for bad in (replace(key,audience='other'),replace(key,operation='other'),replace(key,issuer='viewer'),replace(key,purpose='observation')):
            with self.assertRaises(m.EnvelopeHeld):self.registry([bad])
    def test_disabled_by_default(self):
        v,raw,expected,key=self.parts()
        with patch.object(m,'QUALIFIED',False),self.assertRaises(m.EnvelopeHeld):self.verify(raw,v['signatureBase64url'],registry=self.registry([key]),expected=expected,now=1050)
    def test_exact_payload_bytes_and_expected_binding_required(self):
        v,raw,expected,key=self.parts();payload=FIX['canonicalPayload'].encode('ascii');scope=json.loads(payload)
        for changed in (payload+b' ',payload.replace(b'job-1',b'job-2'),payload.replace(b'"generation":1',b'"generation":1,"generation":1')):
            with self.assertRaises(m.EnvelopeHeld):m.verify(raw,v['signatureBase64url'],payload_raw=changed,expected_payload=scope,registry=self.registry([key]),expected=expected,now=1050)
        for field in scope:
            altered=dict(scope);altered[field]='wrong'
            with self.subTest(field=field),self.assertRaises(m.EnvelopeHeld):m.verify(raw,v['signatureBase64url'],payload_raw=payload,expected_payload=altered,registry=self.registry([key]),expected=expected,now=1050)


