"""Synthetic registry; no credentials, service or transport is exercised."""
from copy import deepcopy
import unittest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from processing import viewer_service_identity_candidate as c

AUTH='11111111-1111-4111-8111-111111111111'
OTHER_AUTH='22222222-2222-4222-8222-222222222222'

def pem(seed):
    return Ed25519PrivateKey.from_private_bytes(bytes([seed])*32).public_key().public_bytes(
        serialization.Encoding.PEM,serialization.PublicFormat.SubjectPublicKeyInfo).decode('ascii')

class IdentityFixtureTests(unittest.TestCase):
    def setUp(self):
        self.item={'owner':'owner-a','environment':'production','authority':AUTH,'service':'viewer-service-a',
                   'keys':[{'keyId':'key-old','issuerId':'issuer-a','publicKeyPem':pem(1),'revoked':False},
                           {'keyId':'key-new','issuerId':'issuer-a','publicKeyPem':pem(2),'revoked':False}]}
        self.args={'owner':'owner-a','environment':'production','authority':AUTH,'service':'viewer-service-a',
                   'key_id':'key-old','issuer_id':'issuer-a'}

    def test_public_factory_disabled(self):
        with self.assertRaises(c.IdentityUnavailable):c.create_registry([self.item])

    def test_overlap_and_revocation_no_fallback(self):
        registry=c._registry_for_fixtures([self.item])
        self.assertEqual(c._resolve_for_fixtures(registry,**self.args)['keyId'],'key-old')
        self.assertEqual(c._resolve_for_fixtures(registry,**{**self.args,'key_id':'key-new'})['keyId'],'key-new')
        revoked=deepcopy(self.item);revoked['keys'][0]['revoked']=True
        registry=c._registry_for_fixtures([revoked])
        with self.assertRaises(c.IdentityRejected):c._resolve_for_fixtures(registry,**self.args)
        self.assertEqual(c._resolve_for_fixtures(registry,**{**self.args,'key_id':'key-new'})['keyId'],'key-new')

    def test_every_coordinate_is_required(self):
        registry=c._registry_for_fixtures([self.item])
        for field,value in [('owner','owner-b'),('environment','staging'),('authority',OTHER_AUTH),
                            ('service','other-service'),('key_id','missing'),('issuer_id','other')]:
            with self.subTest(field=field),self.assertRaises(c.IdentityRejected):
                c._resolve_for_fixtures(registry,**{**self.args,field:value})

    def test_key_issuer_reuse_and_duplicate_ids_rejected_across_scope(self):
        for field,value in [('owner','owner-b'),('environment','staging'),('authority',OTHER_AUTH)]:
            other=deepcopy(self.item);other[field]=value
            other['keys']=[{'keyId':'different-key','issuerId':'different-issuer','publicKeyPem':pem(1),'revoked':True}]
            with self.subTest(field=field,case='same-key'),self.assertRaises(c.IdentityRejected):c._registry_for_fixtures([self.item,other])
            other['keys'][0]['publicKeyPem']=pem(3);other['keys'][0]['issuerId']='issuer-a'
            with self.subTest(field=field,case='same-issuer'),self.assertRaises(c.IdentityRejected):c._registry_for_fixtures([self.item,other])
            other['keys'][0]['issuerId']='different-issuer';other['keys'][0]['keyId']='key-old'
            with self.subTest(field=field,case='same-id'),self.assertRaises(c.IdentityRejected):c._registry_for_fixtures([self.item,other])

    def test_closed_schema_and_detached_results(self):
        for mutate in ('extra','bad_revoked','duplicate_scope'):
            config=[deepcopy(self.item)]
            if mutate=='extra':config[0]['extra']=1
            elif mutate=='bad_revoked':config[0]['keys'][0]['revoked']=0
            else:config.append(deepcopy(config[0]))
            with self.subTest(mutate=mutate),self.assertRaises(c.IdentityRejected):c._registry_for_fixtures(config)
        registry=c._registry_for_fixtures([self.item]);key=c._resolve_for_fixtures(registry,**self.args)
        key['revoked']=True;self.assertFalse(c._resolve_for_fixtures(registry,**self.args)['revoked'])
