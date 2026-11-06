"""Disposable local gate/pin tests, not a live coordinator handoff."""
import copy
import io
import unittest
import uuid
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from processing import direct_worker_fence_proof as proof
from processing.direct_limits import PROFILE
from processing.scratch_policy import PolicyConflict
from processing import direct_worker_receiver as receiver
from tests import test_direct_worker_receiver as fixture

class FenceProofTests(unittest.TestCase):
    def setUp(self):
        self.f=fixture.DirectWorkerReceiverTests();self.f.with_derived=True;self.f.limit_profile=PROFILE
        self.f.setUp();self.addCleanup(self.f.doCleanups)
        self.key=Ed25519PrivateKey.generate();self.challenge=str(uuid.uuid4())
    def issue(self):
        f=self.f
        return proof.issue_fenced(f.ledger,f.spool,f.item,expected_worker=f.worker,engine_id=f.engine,
            coordinator_origin=f.origin,gate_generation=1,challenge_id=self.challenge,issued_at=100,expires_at=130,
            issuer_id='worker',key_id='key',private_key=self.key)
    def verify(self,value,**kwargs):
        args=dict(expected_item=self.f.item,expected_worker=self.f.worker,challenge_id=self.challenge,
                  gate_generation=1,issuer_id='worker',key_id='key',public_key=self.key.public_key(),now=101)
        args.update(kwargs);return proof.verify(value,**args)
    def test_signed_fence_retains_pin_and_does_not_ack_custody(self):
        signed=self.issue();body=self.verify(signed)
        self.assertFalse(body['sourceCustodyAccepted']);self.assertFalse(body['transferAuthorized'])
        self.assertFalse(body['processingAuthorized'])
        self.f.ledger=self.f.open_ledger()
        with self.assertRaises(PolicyConflict):
            self.f.ledger.settle_unstarted(self.f.observation['scratchReservationId'],transfer_ended=True)
        with self.assertRaises(PolicyConflict):self.issue()
    def test_wrong_binding_or_expired_proof_is_rejected(self):
        signed=self.issue()
        for kwargs in (dict(now=130),dict(now=99),dict(now=True),dict(challenge_id=str(uuid.uuid4())),
                       dict(gate_generation=2),dict(expected_item=dict(self.f.item,derivedManifestSha256='c'*64)),
                       dict(public_key=Ed25519PrivateKey.generate().public_key())):
            with self.subTest(kwargs=kwargs),self.assertRaises((proof.FenceProofRejected,ValueError)):
                self.verify(signed,**kwargs)
        changed=copy.deepcopy(signed);changed['body']['pinId']='a'*32
        with self.assertRaises(proof.FenceProofRejected):self.verify(changed)
    def test_invalid_bounds_never_take_pin(self):
        f=self.f
        with self.assertRaises(proof.FenceProofRejected):
            proof.issue_fenced(f.ledger,f.spool,f.item,expected_worker=f.worker,engine_id=f.engine,
                coordinator_origin=f.origin,gate_generation=1,challenge_id=self.challenge,issued_at=100,expires_at=161,
                issuer_id='worker',key_id='key',private_key=self.key)
        self.assertTrue(self.verify(self.issue()))
    def test_exact_pin_handover_to_real_reader_without_unfenced_gap(self):
        f=self.f;signed=self.issue();pin=self.verify(signed)['pinId']
        args=dict(expected_worker=f.worker,engine_id=f.engine,coordinator_origin=f.origin,
                  gate_generation=1,object_id='object-1',derived_ids=f.derived_ids)
        with self.assertRaises(PolicyConflict):
            receiver.receive(f.ledger,f.spool,f.manifest,f.item,reader=io.BytesIO(f.data),_readiness_pin='f'*32,**args)
        with self.assertRaises(PolicyConflict):
            f.ledger.settle_unstarted(f.observation['scratchReservationId'],transfer_ended=True)
        result=receiver.receive(f.ledger,f.spool,f.manifest,f.item,reader=io.BytesIO(f.data),_readiness_pin=pin,**args)
        self.assertTrue(result['localObjectDurable']);self.assertFalse(result['sourceCustodyAccepted'])
        with self.assertRaises(PolicyConflict):
            receiver.receive(f.ledger,f.spool,f.manifest,f.item,reader=io.BytesIO(f.data),_readiness_pin=pin,**args)
    def test_lost_outcome_after_pin_swap_keeps_replacement_held(self):
        f=self.f;signed=self.issue();pin=self.verify(signed)['pinId']
        with self.assertRaises(Exception):
            receiver.receive(f.ledger,f.spool,f.manifest,f.item,expected_worker=f.worker,engine_id=f.engine,
                coordinator_origin=f.origin,gate_generation=1,object_id='object-1',derived_ids=f.derived_ids,
                reader=io.BytesIO(b'truncated'),_readiness_pin=pin)
        f.ledger=f.open_ledger()
        with self.assertRaises(PolicyConflict):
            f.ledger.settle_unstarted(f.observation['scratchReservationId'],transfer_ended=True)

if __name__=='__main__':unittest.main()
