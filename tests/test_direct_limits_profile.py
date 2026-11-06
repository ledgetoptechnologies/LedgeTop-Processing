"""Prototype profile tests; no installed transport or large-data acceptance."""
import copy
import unittest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from processing import direct_limits as limits, direct_worker_receiver as receiver
from processing import direct_worker_scratch as scratch, direct_worker_receipt as receipt
from processing import reference_protocol as p
from processing.scratch_policy import PolicyConflict

ID='00000000-0000-4000-8000-000000000001'


class DirectProfileTests(unittest.TestCase):
    def test_real_receiver_large_manifest_and_old_profile_refusal(self):
        objects=[dict(id=f'photo-{n:05}',name=f'{n:05}.jpg',byteSize=1,sha256='a'*64,
                      kind='image',mediaType='image/jpeg') for n in range(10000)]
        objects.append(dict(id='derived-1',name='gcp.txt',byteSize=1,sha256='b'*64,
                            kind='auxiliary',mediaType='text/plain'))
        manifest=dict(id='dataset',revision=1,totalBytes=len(objects),objectCount=len(objects),
                      inputCounts=dict(images=10000,lidarObjects=0,points=0),objects=objects)
        manifest['sha256']=p.manifest_sha256(manifest)
        item=dict(manifestSha256=manifest['sha256'],inputBytes=len(objects),limitProfile=limits.PROFILE)
        self.assertEqual(receiver._object(manifest,item,'derived-1',['derived-1'])['id'],'derived-1')
        with self.assertRaises(PolicyConflict):
            receiver._object(manifest,{k:v for k,v in item.items() if k!='limitProfile'},'derived-1',['derived-1'])
        with self.assertRaises(ValueError):limits.counts(limits.PROFILE,10001,1)
        with self.assertRaises(ValueError):limits.counts(limits.PROFILE,10000,10001)
        self.assertEqual(p.MAX_OBJECTS,10000)
        self.assertEqual(p.MAX_BODY_BYTES,4*1024*1024)

    def test_profile_is_bound_to_scratch_and_signed_worker_closure(self):
        worker=dict(authorityId=ID,nodeId='node',workerId='worker',workerInstanceId='instance')
        item=dict(reservationId=ID,jobId=ID,attemptId=ID,generation=1,environment='staging',
                  manifestSha256='a'*64,derivedManifestSha256='b'*64,inputBytes=1,outputBudget=1,
                  ttlSeconds=1,**worker)
        legacy=scratch.scratch_identity(item,expected_worker=worker)[1]
        item['limitProfile']=limits.PROFILE
        self.assertNotEqual(legacy,scratch.scratch_identity(item,expected_worker=worker)[1])
        closure=dict(protocol='ltds-direct-local-closure-v1',reservationId=ID,scratchBindingSha256='a'*64,
            jobId=ID,attemptId=ID,generation=1,environment='staging',manifestSha256='a'*64,
            derivedManifestSha256='b'*64,sourceObjectCount=10000,derivedObjectCount=1,
            localClosureComplete=True,sourceCustodyAccepted=False,processingAuthorized=False,
            limitProfile=limits.PROFILE,**worker)
        key=Ed25519PrivateKey.generate()
        signer=receipt.WorkerClosureSigner(issuer_id='test',key_id='test',identity=worker,private_key=key)
        signed=signer.issue(closure)
        self.assertEqual(receipt.verify(signed,expected_closure=closure,issuer_id='test',key_id='test',public_key=key.public_key()),signed)
        changed=copy.deepcopy(closure);del changed['limitProfile']
        with self.assertRaises(receipt.ReceiptRejected):receipt.validate_closure(changed)

    def test_unknown_profile_and_overlarge_metadata_rejected(self):
        with self.assertRaises(ValueError):limits.counts('unknown',1,0)
        with self.assertRaises(ValueError):limits.manifest(limits.PROFILE,{'padding':'x'*(limits.MAX_MANIFEST_BYTES+1)},1,0)

if __name__=='__main__':unittest.main()
