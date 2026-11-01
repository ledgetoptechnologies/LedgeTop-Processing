"""Synthetic signed pairs only; no trusted epoch, reservation or network."""
import base64,copy,hashlib,json,unittest
from pathlib import Path
from dataclasses import replace
from unittest.mock import patch
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding,PublicFormat
from processing import reference_v2_envelope_candidate as e,reference_v2_reservation_candidate as r

class ReservationTests(unittest.TestCase):
    def setUp(self):
        flag=patch.object(r,'QUALIFIED',True);flag.start();self.addCleanup(flag.stop)
        self.coord=Ed25519PrivateKey.from_private_bytes(bytes([41])*32);self.viewer=Ed25519PrivateKey.from_private_bytes(bytes([42])*32)
        public=lambda k:k.public_key().public_bytes(Encoding.Raw,PublicFormat.Raw)
        self.keys=[e.Key('coordinator','coord-key','grant',public(self.coord),900,1200,False,'viewer-deploy','reserve-source'),e.Key('viewer','viewer-key','observation',public(self.viewer),900,1200,False,'coord-deploy','reserve-source-committed')]
        self.roles=(('coordinator','grant','viewer-deploy','reserve-source'),('viewer','observation','coord-deploy','reserve-source-committed'))
        ident=lambda b:base64.urlsafe_b64encode(bytes([b])*32).decode().rstrip('=')
        shared=dict(environment='staging',trustDomain='test',coordinatorDeploymentId='coord-deploy',viewerDeploymentId='viewer-deploy',viewerLedgerId='ledger-1',viewerLedgerEpoch='ledger-epoch-1',intentId=ident(1),idempotencyId=ident(2),jobId='00000000-0000-4000-8000-000000000001',attemptId='00000000-0000-4000-8000-000000000002',generation=1,inputManifestSha256='a'*64,objectCount=1,totalSourceBytes=3,byteBudget=3,policyId='policy-1')
        self.intent=dict(version=1,kind='reservation-intent',issuer='coordinator',keyId='coord-key',audience='viewer-deploy',operation='reserve-source',issuedAt=1000,expiresAt=1100,coordinatorAuthorityId='authority-1',coordinatorEpoch='epoch-1',reservationRequestId=ident(3),claimBindingSha256='b'*64,**shared)
        self.receipt=dict(version=1,kind='reservation-receipt',issuer='viewer',keyId='viewer-key',audience='coord-deploy',operation='reserve-source-committed',issuedAt=1000,expiresAt=1100,receiptId=ident(4),reservationId=ident(5),sourceRevisionId='source-1',intentDigest='',**shared)
    def pair(self,intent=None,receipt=None):
        intent=copy.deepcopy(intent or self.intent);receipt=copy.deepcopy(receipt or self.receipt);raw=e._canonical(intent)
        receipt['intentDigest']=hashlib.sha256(r.INTENT_DIGEST_DOMAIN+raw).hexdigest();receipt_raw=e._canonical(receipt)
        sign=lambda key,domain,data:base64.urlsafe_b64encode(key.sign(domain+data)).decode().rstrip('=')
        return raw,sign(self.coord,r.INTENT_DOMAIN,raw),receipt_raw,sign(self.viewer,r.RECEIPT_DOMAIN,receipt_raw),intent,receipt
    def verify(self,pair,*,intent=None,receipt=None,keys=None,now=1050):
        return r.verify_pair(*pair[:4],registry=e.Registry(keys or self.keys,role_policy=self.roles),expected_intent=intent or pair[4],expected_receipt=receipt or pair[5],now=now)
    def test_real_distinct_signatures_and_exact_pair(self):
        pair=self.pair();self.assertEqual(self.verify(pair)['scope'],'verified_bytes_not_authority')
        vector=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v2-reservation-pair-experiment.json').read_text())
        self.assertEqual((pair[0].decode(),pair[1],pair[2].decode(),pair[3]),(vector['intentCanonical'],vector['intentSignature'],vector['receiptCanonical'],vector['receiptSignature']))
        self.assertEqual(self.coord.public_key().public_bytes(Encoding.DER,PublicFormat.SubjectPublicKeyInfo).hex(),vector['coordinatorSpkiDerHex'])
        self.assertEqual(self.viewer.public_key().public_bytes(Encoding.DER,PublicFormat.SubjectPublicKeyInfo).hex(),vector['viewerSpkiDerHex'])
        self.assertNotEqual(vector['coordinatorSpkiDerHex'],vector['viewerSpkiDerHex'])
    def test_changed_binding_role_expiry_and_revocation_hold(self):
        pair=self.pair()
        for field in pair[4]:
            changed=dict(pair[4]);changed[field]='wrong'
            with self.subTest(field=field),self.assertRaises(r.ReservationHeld):self.verify(pair,intent=changed)
        for field in pair[5]:
            changed=dict(pair[5]);changed[field]='wrong'
            with self.subTest(field=field),self.assertRaises(r.ReservationHeld):self.verify(pair,receipt=changed)
        with self.assertRaises(r.ReservationHeld):self.verify(pair,now=1100)
        with self.assertRaises(r.ReservationHeld):self.verify(pair,keys=[self.keys[0],replace(self.keys[1],revoked=True)])
        for index in (0,2):
            changed=list(pair);changed[index]+=b' '
            with self.assertRaises(r.ReservationHeld):self.verify(changed)
        changed=list(pair);changed[1],changed[3]=changed[3],changed[1]
        with self.assertRaises(r.ReservationHeld):self.verify(changed)
        for field in r.SHARED:
            changed=copy.deepcopy(self.receipt);changed[field]=('production' if field=='environment' else 2 if field in ('generation','objectCount','totalSourceBytes','byteBudget') else 'different')
            with self.subTest(shared=field),self.assertRaises(r.ReservationHeld):self.verify(self.pair(receipt=changed))
    def test_disabled(self):
        with patch.object(r,'QUALIFIED',False),self.assertRaises(r.ReservationHeld):self.verify(self.pair())
    def test_provisional_nonempty_budget_and_time_boundaries(self):
        for field in ('objectCount','totalSourceBytes','byteBudget'):
            intent=copy.deepcopy(self.intent);receipt=copy.deepcopy(self.receipt);intent[field]=0;receipt[field]=0
            with self.subTest(field=field),self.assertRaises(r.ReservationHeld):self.verify(self.pair(intent,receipt))
        intent=copy.deepcopy(self.intent);receipt=copy.deepcopy(self.receipt);intent['byteBudget']=2;receipt['byteBudget']=2
        with self.assertRaises(r.ReservationHeld):self.verify(self.pair(intent,receipt))
        intent=copy.deepcopy(self.intent);receipt=copy.deepcopy(self.receipt);intent['expiresAt']=1301;receipt['expiresAt']=1301
        keys=[replace(k,not_after=1400) for k in self.keys]
        with self.assertRaises(r.ReservationHeld):self.verify(self.pair(intent,receipt),keys=keys)
        intent['expiresAt']=1300;receipt['expiresAt']=1300
        self.assertEqual(self.verify(self.pair(intent,receipt),keys=keys)['scope'],'verified_bytes_not_authority')
