"""Disposable local gate/pin tests, not a live coordinator handoff."""
import copy
import io
import json
import importlib.util
import os
import threading
import unittest
import uuid
from unittest.mock import patch
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from processing import direct_worker_fence_proof as proof
from processing.direct_limits import PROFILE
from processing.scratch_policy import PolicyConflict
from processing import direct_worker_receiver as receiver
from processing.node_gate import NodeGate, GateBusy, GateError
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
        with NodeGate(f.engine) as gate:
            with self.assertRaisesRegex(GateError,'direct transfer hold'):
                gate.resolve(f.observation['bindingSha256'][:32],1,old_worker_stopped=True,
                    coordinator_resolved=True,note='synthetic')

    def test_operator_resolve_cannot_race_active_first_writer(self):
        f=self.f;signed=self.issue();pin=self.verify(signed)['pinId']
        started=threading.Event();proceed=threading.Event();errors=[]
        class BlockingReader(io.BytesIO):
            def read(self,n=-1):
                started.set()
                if not proceed.wait(3):raise TimeoutError('test wait expired')
                return super().read(n)
        def run():
            try:
                receiver.receive(f.ledger,f.spool,f.manifest,f.item,expected_worker=f.worker,engine_id=f.engine,
                    coordinator_origin=f.origin,gate_generation=1,object_id='object-1',derived_ids=f.derived_ids,
                    reader=BlockingReader(f.data),_readiness_pin=pin)
            except BaseException as e:errors.append(e)
        thread=threading.Thread(target=run);thread.start()
        try:
            self.assertTrue(started.wait(3))
            with self.assertRaises(GateBusy):
                with NodeGate(f.engine):pass
        finally:
            proceed.set();thread.join(3)
        self.assertFalse(thread.is_alive());self.assertEqual(errors,[])
        with NodeGate(f.engine) as gate:
            with self.assertRaisesRegex(GateError,'direct transfer hold'):
                gate.resolve(f.observation['bindingSha256'][:32],1,old_worker_stopped=True,
                    coordinator_resolved=True,note='synthetic')

    def test_failure_after_gate_latch_before_pin_still_blocks_resolve(self):
        f=self.f
        with patch.object(f.ledger,'begin_direct_writer',side_effect=RuntimeError('synthetic crash')):
            with self.assertRaises(RuntimeError):self.issue()
        with NodeGate(f.engine) as gate:
            with self.assertRaisesRegex(GateError,'direct transfer hold'):
                gate.resolve(f.observation['bindingSha256'][:32],1,old_worker_stopped=True,
                    coordinator_resolved=True,note='synthetic')

    def test_corrupt_direct_hold_still_blocks_gate_mutation(self):
        f=self.f;self.issue()
        path=f.root/'gate'/'gate.json';state=json.loads(path.read_text());state['directHold']['generation']=True
        path.write_text(json.dumps(state))
        with NodeGate(f.engine) as gate:
            with self.assertRaisesRegex(GateError,'inspection'):
                gate.resolve(f.observation['bindingSha256'][:32],1,old_worker_stopped=True,
                    coordinator_resolved=True,note='synthetic')

    def test_old_gate_schema_reader_cannot_accept_direct_hold(self):
        f=self.f;self.issue()
        state=json.loads((f.root/'gate'/'gate.json').read_text())
        self.assertEqual(state['schema'],2)
        # The actual prior implementation's _load requires schema == 1.
        self.assertNotEqual(state['schema'],1)

    @unittest.skipUnless(os.environ.get('LTDS_OLD_GATE_SOURCE'),'old gate source fixture not supplied')
    def test_actual_old_gate_executable_rejects_version_two(self):
        f=self.f;self.issue()
        spec=importlib.util.spec_from_file_location('old_node_gate_fixture',os.environ['LTDS_OLD_GATE_SOURCE'])
        old=importlib.util.module_from_spec(spec);spec.loader.exec_module(old)
        with patch.object(old,'canonical_gate_path',return_value=f.root/'gate'):
            with old.NodeGate(f.engine) as gate:
                with self.assertRaises(old.GateError):gate.snapshot()

if __name__=='__main__':unittest.main()
