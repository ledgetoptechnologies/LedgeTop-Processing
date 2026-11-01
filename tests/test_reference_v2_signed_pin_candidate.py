"""Synthetic crypto + real coordinator transaction; no current authority."""
import base64,copy,hashlib,json,unittest
from dataclasses import replace
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding,PublicFormat
from cryptography.hazmat.primitives.serialization import load_der_public_key
from unittest.mock import patch
from pathlib import Path
from processing import reference_protocol as p,reference_v2_event_pin_candidate as pin,reference_v2_envelope_candidate as env,reference_v2_reservation_candidate as signed,reference_v2_event_transition_candidate as transition,reference_transfer_v2_contract_candidate as request_contract
from tests import test_reference_v2_event_pin_candidate as pin_fixture,test_reference_v2_reservation_candidate as pair_fixture

class SignedPinTests(unittest.TestCase):
    def setUp(self):
        self.f=pin_fixture.EventPinTests();self.f.setUp();self.addCleanup(self.f.doCleanups)
        self.s=pair_fixture.ReservationTests();self.s.setUp();self.addCleanup(self.s.doCleanups)
        self.event_key=Ed25519PrivateKey.from_private_bytes(bytes([43])*32)
        public=self.event_key.public_key().public_bytes(Encoding.Raw,PublicFormat.Raw)
        self.event_registry_key=env.Key('viewer','viewer-event-key','observation',public,900,1200,False,'coord-deploy','ledger-event')
        self.s.roles=self.s.roles+(('viewer','observation','coord-deploy','ledger-event'),)
        for module,name in ((pin,'SIGNED_QUALIFIED'),(signed,'QUALIFIED'),(transition,'QUALIFIED'),(request_contract,'QUALIFIED')):
            flag=patch.object(module,name,True);flag.start();self.addCleanup(flag.stop)
        reservation=base64.urlsafe_b64encode(bytes([5])*32).decode().rstrip('=')
        with self.f.fixture.queue._db() as db:
            row=db.execute('SELECT transfer_json FROM reference_jobs WHERE job_id=?',(self.f.claim['jobId'],)).fetchone()
            transfer=json.loads(row[0]);transfer['reservationId']=reservation
            db.execute('UPDATE reference_jobs SET transfer_json=? WHERE job_id=?',(json.dumps(transfer),self.f.claim['jobId']))
        for body in (self.s.intent,self.s.receipt):
            body.update(jobId=self.f.claim['jobId'],attemptId=self.f.attempt['attemptId'],generation=1,environment='staging',inputManifestSha256=self.f.fixture.submission['manifestSha256'],byteBudget=transfer['byteBudget'])
        self.s.intent['coordinatorAuthorityId']=self.f.fixture.queue.authority_id;self.s.intent['coordinatorEpoch']='00000000-0000-4000-8000-000000000002';self.s.receipt['reservationId']=reservation
        with self.f.fixture.queue._db() as db:
            identity=json.loads(db.execute('SELECT identity FROM v2_attempts WHERE job_id=?',(self.f.claim['jobId'],)).fetchone()[0])
        binding=dict(identity=identity,attemptId=self.f.attempt['attemptId'],authorityEpoch=self.s.intent['coordinatorEpoch'])
        self.s.intent['claimBindingSha256']=hashlib.sha256(pin.CLAIM_BINDING_DOMAIN+p.canonical_json(binding)).hexdigest()
    def payload(self,intent=None,receipt=None,keys=None):
        pair=self.s.pair(intent,receipt)
        body=json.loads(json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v2-transition-experiment.json').read_text())['canonicalBody'])
        obj=self.f.fixture.submission['manifest']['objects'][0];i,r=pair[4:]
        body.update(version=3,operationId=self.f.event['operation'],ledgerId=r['viewerLedgerId'],ledgerEpoch=r['viewerLedgerEpoch'],previousRevision=self.f.event['previousRevision'],nextRevision=self.f.event['nextRevision'],jobId=i['jobId'],generation=i['generation'],environment=i['environment'],workerInstanceId=self.f.claim['workerInstanceId'],attemptId=i['attemptId'],inputManifestSha256=i['inputManifestSha256'],claimLineageSha256=i['claimBindingSha256'],authorityEpoch=i['coordinatorEpoch'],trustDomain=i['trustDomain'],receiptId=r['receiptId'],reservationId=r['reservationId'],intentDigest=r['intentDigest'],receiptBodyDigest=hashlib.sha256(transition.RECEIPT_DIGEST_DOMAIN+pair[2]).hexdigest(),objectId=obj['id'],objectSha256=obj['sha256'],objectLength=obj['byteSize'],chargedBytes=obj['byteSize'])
        vector=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-transfer-v2-hash-vector.json').read_text());admission=json.loads(vector['canonicalAdmission']);b=admission['binding']
        b.update(jobId=i['jobId'],attemptId=i['attemptId'],generation=i['generation'],environment=i['environment'],manifestSha256=i['inputManifestSha256'],reservationId=r['reservationId'],workerInstanceId=self.f.claim['workerInstanceId'],claimLineageSha256=i['claimBindingSha256'],authorityId=i['coordinatorAuthorityId'],epoch=i['coordinatorEpoch'],objectId=obj['id'],objectLength=obj['byteSize'],objectSha256=obj['sha256'],range=body['range'])
        admission_raw=p.canonical_json(admission);_,request_sha=request_contract.parse_admission(admission_raw)
        body.update(requestId=admission['requestId'],deliveryId=admission['deliveryId'],requestSha256=request_sha)
        value=dict(intentRaw=pair[0],intentSignature=pair[1],receiptRaw=pair[2],receiptSignature=pair[3],expectedIntent=i,expectedReceipt=r,registry=env.Registry((keys or self.s.keys)+[self.event_registry_key],role_policy=self.s.roles),eventBody=p.canonical_json(body),eventEnvelopeRaw=b'',eventSignature='',admissionRaw=admission_raw,policyDomain='test',now=1050)
        return self.resign(value)
    def resign(self,value):
        body=json.loads(value['eventBody']);event=dict(self.f.event,ledger=value['expectedReceipt']['viewerLedgerId'],epoch=value['expectedReceipt']['viewerLedgerEpoch'],nextDigest=transition.calculate_receipt_bound(self.f.event['previousDigest'],value['eventBody']))
        nonce=base64.urlsafe_b64encode(bytes([8])*32).decode().rstrip('=')
        wire=dict(version=1,issuer='viewer',keyId='viewer-event-key',audience='coord-deploy',operation='ledger-event',issuedAt=1000,expiresAt=1100,nonce=nonce,domain=event['domain'],ledger=event['ledger'],epoch=event['epoch'],eventOperation=event['operation'],previousRevision=event['previousRevision'],previousDigest=event['previousDigest'],nextRevision=event['nextRevision'],nextDigest=event['nextDigest'],bodySha256=hashlib.sha256(value['eventBody']).hexdigest(),bodyBytes=len(value['eventBody']),requestId=body['requestId'],requestSha256=body['requestSha256'],deliveryId=body['deliveryId'],claimBindingSha256=body['claimLineageSha256'],receiptBodyDigest=body['receiptBodyDigest'])
        value['eventEnvelopeRaw']=p.canonical_json(wire);value['eventSignature']=base64.urlsafe_b64encode(self.event_key.sign(pin.EVENT_SIGNATURE_DOMAIN+value['eventEnvelopeRaw'])).decode().rstrip('=')
        return value
    def invoke(self,value):
        event=dict(self.f.event,ledger=value['expectedReceipt']['viewerLedgerId'],epoch=value['expectedReceipt']['viewerLedgerEpoch'],nextDigest=transition.calculate_receipt_bound(self.f.event['previousDigest'],value['eventBody']))
        return pin.pin(self.f.fixture.store,claim=self.f.claim,event_raw=p.canonical_json(event),_signed_pair=value)
    def test_signed_pair_compared_to_actual_claim_attempt_and_reservation(self):
        for field in ('jobId','attemptId','inputManifestSha256','coordinatorAuthorityId','byteBudget','objectCount','totalSourceBytes','claimBindingSha256'):
            intent=copy.deepcopy(self.s.intent);receipt=copy.deepcopy(self.s.receipt)
            value=2 if field=='objectCount' else 2 if field=='totalSourceBytes' else 4 if field=='byteBudget' else '00000000-0000-4000-8000-000000000099' if field in ('jobId','attemptId','coordinatorAuthorityId') else 'f'*64
            intent[field]=value
            if field in receipt:receipt[field]=value
            verified=[];original=signed.verify_pair
            def capture(*args,**kwargs):
                result=original(*args,**kwargs);verified.append(result);return result
            with self.subTest(field=field),patch.object(signed,'verify_pair',side_effect=capture):
                with self.assertRaisesRegex(pin.PinHeld,'signed reservation mismatch'):self.invoke(self.payload(intent,receipt))
                self.assertEqual(len(verified),1)
        bad=copy.deepcopy(self.s.receipt);bad['reservationId']=base64.urlsafe_b64encode(bytes([6])*32).decode().rstrip('=')
        with self.assertRaisesRegex(pin.PinHeld,'signed reservation mismatch'):self.invoke(self.payload(receipt=bad))
        with self.assertRaisesRegex(pin.PinHeld,'signed reservation mismatch'):self.invoke(self.payload(keys=[self.s.keys[0],replace(self.s.keys[1],revoked=True)]))
        for field in ('ledgerId','ledgerEpoch','workerInstanceId','receiptBodyDigest','intentDigest'):
            value=self.payload();body=json.loads(value['eventBody']);body[field]='f'*64 if field.endswith('Digest') else 'wrong';value['eventBody']=p.canonical_json(body);self.resign(value)
            with self.subTest(event_field=field),self.assertRaisesRegex(pin.PinHeld,'signed reservation mismatch'):self.invoke(value)
        invalid=self.payload();invalid['eventSignature']=invalid['intentSignature']
        with self.assertRaisesRegex(pin.PinHeld,'signed reservation mismatch'):self.invoke(invalid)
        invalid=self.payload();invalid['policyDomain']='wrong'
        with self.assertRaisesRegex(pin.PinHeld,'signed reservation mismatch'):self.invoke(invalid)
        for field in ('domain','ledger','epoch','previousDigest','nextDigest','bodySha256','requestSha256','receiptBodyDigest','audience'):
            value=self.payload();wire=json.loads(value['eventEnvelopeRaw']);wire[field]='f'*64 if field.endswith('Digest') or field.endswith('Sha256') else 'wrong';value['eventEnvelopeRaw']=p.canonical_json(wire)
            value['eventSignature']=base64.urlsafe_b64encode(self.event_key.sign(pin.EVENT_SIGNATURE_DOMAIN+value['eventEnvelopeRaw'])).decode().rstrip('=')
            with self.subTest(envelope_field=field),self.assertRaisesRegex(pin.PinHeld,'signed reservation mismatch'):self.invoke(value)
        value=self.payload();changed=dict(self.f.event,ledger=value['expectedReceipt']['viewerLedgerId'],epoch=value['expectedReceipt']['viewerLedgerEpoch'],previousDigest='c'*64)
        changed['nextDigest']=transition.calculate_receipt_bound(changed['previousDigest'],value['eventBody'])
        with self.assertRaisesRegex(pin.PinHeld,'signed reservation mismatch'):
            pin.pin(self.f.fixture.store,claim=self.f.claim,event_raw=p.canonical_json(changed),_signed_pair=value)
        result=self.invoke(self.payload());self.assertEqual(result['authorization'],'unqualified')
        self.assertEqual(self.invoke(self.payload()),result)
    def test_false_signed_gate_blocks(self):
        with patch.object(pin,'SIGNED_QUALIFIED',False),self.assertRaises(pin.PinHeld):self.invoke(self.payload())
    def test_public_full_vector_signatures_and_commitments(self):
        value=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v2-signed-event-experiment.json').read_text())
        for name,signature,key,domain in (('intentCanonical','intentSignature','coordinatorSpkiDerHex',signed.INTENT_DOMAIN),('receiptCanonical','receiptSignature','viewerReceiptSpkiDerHex',signed.RECEIPT_DOMAIN),('eventEnvelopeCanonical','eventSignature','viewerEventSpkiDerHex',pin.EVENT_SIGNATURE_DOMAIN)):
            raw=value[name].encode('ascii');sig=env._bytes64(value[signature],64)
            load_der_public_key(bytes.fromhex(value[key])).verify(sig,domain+raw)
        body=value['eventBodyCanonical'].encode('ascii');receipt=value['receiptCanonical'].encode('ascii');envelope=json.loads(value['eventEnvelopeCanonical']);event=json.loads(body)
        self.assertEqual((envelope['bodySha256'],envelope['bodyBytes']),(hashlib.sha256(body).hexdigest(),len(body)))
        self.assertEqual(envelope['nextDigest'],transition.calculate_receipt_bound(envelope['previousDigest'],body))
        self.assertEqual(event['receiptBodyDigest'],hashlib.sha256(transition.RECEIPT_DIGEST_DOMAIN+receipt).hexdigest())
        self.assertEqual(event['intentDigest'],hashlib.sha256(signed.INTENT_DIGEST_DOMAIN+value['intentCanonical'].encode('ascii')).hexdigest())
        admission,request_sha=request_contract.parse_admission(value['admissionCanonical'].encode('ascii'))
        self.assertEqual((event['requestId'],event['deliveryId'],event['requestSha256']),(admission['requestId'],admission['deliveryId'],request_sha))
