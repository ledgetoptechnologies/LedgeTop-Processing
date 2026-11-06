"""Synthetic private boundary; real local files/gate, no listener or Viewer ACK."""
import copy
import io
import json
import unittest
from unittest.mock import patch
from pathlib import Path
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from processing.direct_worker_boundary import DirectBoundaryDenied, DirectWorkerBoundary
from processing.direct_worker_receipt import ReceiptRejected, WorkerClosureSigner, verify
from processing.scratch_policy import PolicyConflict
from tests import test_direct_worker_receiver as receiver_fixture
from processing import direct_worker_closure


class DirectWorkerBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.f=receiver_fixture.DirectWorkerReceiverTests();self.f.with_derived=True;self.f.setUp();self.addCleanup(self.f.doCleanups)
        self.key=Ed25519PrivateKey.generate()
        self.signer=WorkerClosureSigner(issuer_id='worker-issuer',key_id='worker-key',
                                       identity=self.f.worker,private_key=self.key)
        self.bearer='synthetic-coordinator-token-0123456789'
        self.headers={'Authorization':'Bearer '+self.bearer,'X-Coordinator-Authority':self.f.worker['authorityId']}
        self.boundary=self.open_boundary()

    def open_boundary(self):
        f=self.f
        return DirectWorkerBoundary(ledger=f.ledger,spool=f.spool,manifest=f.manifest,item=f.item,
            derived_ids=f.derived_ids,expected_worker=f.worker,engine_id=f.engine,
            coordinator_origin=f.origin,gate_generation=1,bearer=self.bearer,signer=self.signer)

    def put(self,object_id,data,headers=None,protected=True):
        obj=next(x for x in self.f.manifest['objects'] if x['id']==object_id)
        return self.boundary.put(self.headers if headers is None else headers,protected_transport=protected,
            object_id=object_id,content_length=obj['byteSize'],content_sha256=obj['sha256'],reader=io.BytesIO(data))

    def upload(self):
        self.put('object-1',self.f.data);self.put('derived-1',self.f.derived_data)

    def complete(self):return self.boundary.complete(self.headers,protected_transport=True)

    def test_wrong_credentials_authority_and_transport_never_read_or_write(self):
        for headers,protected in (({'Authorization':'Bearer wrong','X-Coordinator-Authority':self.f.worker['authorityId']},True),
            ({**self.headers,'X-Coordinator-Authority':str(__import__('uuid').uuid4())},True),
            ({**self.headers,'authorization':'Bearer '+self.bearer},True),(self.headers,False)):
            with self.subTest(headers=headers,protected=protected),self.assertRaises(DirectBoundaryDenied):
                self.put('object-1',self.f.data,headers,protected)
        self.assertFalse((self.f.spool.root/self.f.item['jobId']).exists())

    def test_profile_runs_local_boundary_and_signed_closure(self):
        from processing.direct_limits import PROFILE
        f=receiver_fixture.DirectWorkerReceiverTests();f.with_derived=True;f.limit_profile=PROFILE
        f.setUp();self.addCleanup(f.doCleanups)
        signer=WorkerClosureSigner(issuer_id='worker-issuer',key_id='worker-key',identity=f.worker,private_key=self.key)
        boundary=DirectWorkerBoundary(ledger=f.ledger,spool=f.spool,manifest=f.manifest,item=f.item,
            derived_ids=f.derived_ids,expected_worker=f.worker,engine_id=f.engine,
            coordinator_origin=f.origin,gate_generation=1,bearer=self.bearer,signer=signer)
        headers={'Authorization':'Bearer '+self.bearer,'X-Coordinator-Authority':f.worker['authorityId']}
        for obj,data in ((f.manifest['objects'][0],f.data),(f.manifest['objects'][1],f.derived_data)):
            observation=boundary.put(headers,protected_transport=True,object_id=obj['id'],
                content_length=obj['byteSize'],content_sha256=obj['sha256'],reader=io.BytesIO(data))
            self.assertFalse(observation['sourceCustodyAccepted'])
        signed=boundary.complete(headers,protected_transport=True)
        self.assertEqual(signed['closure']['limitProfile'],PROFILE)
        self.assertFalse(signed['closure']['sourceCustodyAccepted'])
        self.assertFalse(signed['closure']['processingAuthorized'])

    def test_prepared_manifest_reused_and_original_mutation_cannot_change_binding(self):
        from processing import direct_worker_receiver
        original=copy.deepcopy(self.f.manifest['objects'][0])
        self.f.manifest['objects'][0]['sha256']='f'*64
        with patch.object(direct_worker_receiver,'_object',side_effect=AssertionError('unexpected rescan')):
            observation=self.boundary.put(self.headers,protected_transport=True,object_id='object-1',
                content_length=original['byteSize'],content_sha256=original['sha256'],reader=io.BytesIO(self.f.data))
            self.assertTrue(observation['localObjectDurable'])
        self.boundary.item['generation']+=1
        with self.assertRaises(PolicyConflict):self.put('derived-1',self.f.derived_data)

    def test_exact_signed_receipt_is_durable_and_replays_after_restart(self):
        self.upload();first=self.complete()
        self.assertFalse(first['closure']['sourceCustodyAccepted']);self.assertFalse(first['closure']['processingAuthorized'])
        receipt_path=self.f.spool.root.parent/'direct-worker-signed-closure.json'
        before=receipt_path.read_bytes();self.assertEqual(json.loads(before),first)
        self.f.now=1000;self.f.ledger=self.f.open_ledger();self.f.spool=self.f.open_spool();self.boundary=self.open_boundary()
        self.assertEqual(self.complete(),first);self.assertEqual(receipt_path.read_bytes(),before)
        self.assertEqual(verify(first,expected_closure=first['closure'],issuer_id='worker-issuer',
                                key_id='worker-key',public_key=self.key.public_key()),first)

    def test_wrong_worker_key_or_changed_binding_never_verifies(self):
        self.upload();signed=self.complete()
        with self.assertRaises(ReceiptRejected):verify(signed,expected_closure=signed['closure'],
            issuer_id='worker-issuer',key_id='worker-key',public_key=Ed25519PrivateKey.generate().public_key())
        changed=copy.deepcopy(signed['closure']);changed['attemptId']=str(__import__('uuid').uuid4())
        with self.assertRaises(ReceiptRejected):verify(signed,expected_closure=changed,
            issuer_id='worker-issuer',key_id='worker-key',public_key=self.key.public_key())
        wrong=dict(self.f.worker,workerInstanceId='wrong-instance')
        self.signer.identity=wrong
        with self.assertRaises(DirectBoundaryDenied):self.open_boundary()

    def test_interrupted_source_leaves_no_signed_receipt_and_holds(self):
        with self.assertRaises(Exception):self.put('object-1',b'short')
        self.assertFalse((self.f.spool.root.parent/'direct-worker-signed-closure.json').exists())
        self.f.ledger=self.f.open_ledger();self.boundary=self.open_boundary()
        with self.assertRaises(PolicyConflict):self.put('object-1',self.f.data)

    def test_incomplete_closure_never_signs_or_acknowledges(self):
        self.put('object-1',self.f.data)
        with self.assertRaises(PolicyConflict):self.complete()
        self.assertFalse((self.f.spool.root.parent/'direct-worker-signed-closure.json').exists())
        self.put('derived-1',self.f.derived_data)
        self.assertFalse(self.complete()['closure']['sourceCustodyAccepted'])

    def test_lost_reply_after_signed_file_holds_pin_until_inspection(self):
        self.upload();original=direct_worker_closure._persist_receipt
        def interrupt(parent,expected,**kwargs):
            original(parent,expected,**kwargs)
            if kwargs.get('name')=='direct-worker-signed-closure.json':raise OSError('synthetic lost outcome')
        with patch.object(direct_worker_closure,'_persist_receipt',side_effect=interrupt):
            with self.assertRaises(OSError):self.complete()
        self.assertTrue((self.f.spool.root.parent/'direct-worker-signed-closure.json').exists())
        self.f.ledger=self.f.open_ledger();self.f.spool=self.f.open_spool();self.boundary=self.open_boundary()
        with self.assertRaisesRegex(PolicyConflict,'active or uncertain'):self.complete()

    def test_changed_signing_key_cannot_replace_persisted_receipt(self):
        self.upload();self.complete()
        receipt_path=self.f.spool.root.parent/'direct-worker-signed-closure.json';before=receipt_path.read_bytes()
        self.signer=WorkerClosureSigner(issuer_id='worker-issuer',key_id='worker-key',identity=self.f.worker,
                                        private_key=Ed25519PrivateKey.generate())
        self.boundary=self.open_boundary()
        with self.assertRaisesRegex(PolicyConflict,'receipt changed'):self.complete()
        self.assertEqual(receipt_path.read_bytes(),before)

    def test_exact_closure_types_and_scalar_bounds_fail_closed(self):
        self.upload();signed=self.complete();good=signed['closure']
        invalid=(('generation',True),('generation',1.0),('generation',0),('generation',2**53),
            ('sourceObjectCount',True),('sourceObjectCount',1.0),('sourceObjectCount',0),
            ('derivedObjectCount',False),('derivedObjectCount',1.0),('derivedObjectCount',-1),
            ('derivedObjectCount',10001),('jobId','not-a-uuid'),('authorityId','NOT-UUID'),
            ('workerId','bad/worker'),('manifestSha256','A'*64),('environment','other'),
            ('localClosureComplete',1),('sourceCustodyAccepted',0),('processingAuthorized',0))
        for field,value in invalid:
            with self.subTest(field=field,value=value):
                bad=copy.deepcopy(good);bad[field]=value
                with self.assertRaises(ReceiptRejected):self.signer.issue(bad)
                with self.assertRaises(ReceiptRejected):verify(signed,expected_closure=bad,
                    issuer_id='worker-issuer',key_id='worker-key',public_key=self.key.public_key())
                altered=copy.deepcopy(signed);altered['closure'][field]=value
                with self.assertRaises(ReceiptRejected):verify(altered,expected_closure=good,
                    issuer_id='worker-issuer',key_id='worker-key',public_key=self.key.public_key())


if __name__=='__main__':unittest.main()
