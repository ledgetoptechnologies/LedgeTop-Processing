import copy
from dataclasses import asdict, replace
from datetime import datetime,timezone
import hashlib
import io
import json
from pathlib import Path
import ssl
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from processing import reference_protocol as p
from processing.reference_transfer import BrokerClient,validate_grant
from processing.reference_provenance import LidarProvenanceClient,ProvenanceUnavailable


class Reply(io.BytesIO):
    def __init__(self,body,url,headers=None,status=200):
        super().__init__(body)
        self.status,self.url=status,url
        self.headers={'Content-Type':'application/json','Content-Length':str(len(body))} if headers is None else headers
    def geturl(self):return self.url


class ProvenanceTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.now=1700000000.0
        self.clock=lambda:self.now
        source=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())['submission']
        self.submission=copy.deepcopy(source)
        self.obj=self.submission['manifest']['objects'][0]
        self.obj.update(id='cloud-1',kind='lidar',name='cloud.las',byteSize=3,sha256=hashlib.sha256(b'LAS').hexdigest())
        manifest=self.submission['manifest']
        manifest.update(objectCount=1,totalBytes=3,inputCounts=dict(images=0,lidarObjects=1,auxiliaryObjects=0))
        manifest['sha256']=p.manifest_sha256(manifest)
        pipeline=self.submission['pipeline']
        pipeline['odm'].update(requested=False,options={})
        pipeline['treedar'].update(requested=True,inputObjectIds=['cloud-1'],provenanceReceiptId='provenance-1')
        request=dict(reservationId='reservation-1',generation=1,
            expiresAt=datetime.fromtimestamp(self.now+120,timezone.utc).isoformat(timespec='milliseconds').replace('+00:00','Z'),
            manifestSha256=manifest['sha256'],byteBudget=9,nonce='nonce-1')
        self.job=dict(jobId='00000000-0000-4000-8000-000000000123',generation=1,environment='staging',
            manifestSha256=manifest['sha256'],pipelineDigest=p.digest(pipeline),submission=self.submission,transferRequest=request)
        self.raw_grant=dict(request,jobId=self.job['jobId'],environment='staging',objectIds=['cloud-1'],token='test-only-private-grant-token')
        self.grant=validate_grant(self.raw_grant,job=self.job,clock=self.clock)
        self.receipt=dict(schemaVersion=1,receiptId='provenance-1',jobId=self.job['jobId'],generation=1,
            environment='staging',objectId='cloud-1',objectSha256=self.obj['sha256'],byteSize=3,
            sourceKind='lidar',verticalUnits='metre',verticalDatum='navd88')
        self.wrapper=dict(schemaVersion=1,inputManifestSha256=self.job['manifestSha256'],pipelineDigest=self.job['pipelineDigest'],provenance=self.receipt)
        self.requests=[];self.responses=[];self.reply_override=None
        self.broker=BrokerClient('https://broker.invalid:18444',ssl_context=ssl.create_default_context(),clock=self.clock)
        self.broker._open=self.open
        self.client=LidarProvenanceClient(self.broker,state_path=self.root/'provenance.sqlite',clock=self.clock,max_concurrent=1)

    def open(self,request,timeout):
        self.requests.append(request)
        if self.reply_override:return self.reply_override(request,timeout)
        reply=Reply(p.canonical_json(self.wrapper),request.full_url)
        self.responses.append(reply)
        return reply

    def test_authenticated_exact_receipt_is_durable_detached_and_does_not_spend_input_budget(self):
        original=asdict(self.grant)
        value=self.client.fetch(self.job,self.grant)
        self.assertEqual(value,self.receipt);self.assertTrue(self.client.verify(value))
        value['sourceKind']='synthetic_lidar'
        self.assertFalse(self.client.verify(value))
        restart=LidarProvenanceClient(self.broker,state_path=self.client.path,clock=self.clock,max_concurrent=1)
        self.assertTrue(restart.verify(self.receipt))
        self.assertEqual(len(self.requests),1)
        self.assertEqual(asdict(self.grant),original)
        headers={key.lower():value for key,value in self.requests[0].header_items()}
        self.assertEqual(headers['authorization'],'Bearer '+self.grant.token)
        self.assertEqual(headers['x-ltds-generation'],'1')
        self.assertEqual(headers['x-ltds-object-bytes'],'3')
        self.assertEqual(headers['x-ltds-pipeline-digest'],self.job['pipelineDigest'])
        self.assertTrue(all(reply.closed for reply in self.responses))

    def test_missing_or_revoked_reread_disables_previous_cache_without_deleting_evidence(self):
        self.client.fetch(self.job,self.grant)
        def denied(request,timeout):raise HTTPError(request.full_url,403,'denied',{},io.BytesIO(b'private issuer text'))
        self.reply_override=denied
        with self.assertRaises(ProvenanceUnavailable):self.client.fetch(self.job,self.grant)
        self.assertFalse(self.client.verify(self.receipt))
        with self.client._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM provenance_receipts').fetchone()[0],1)
        self.reply_override=None
        self.assertEqual(self.client.fetch(self.job,self.grant),self.receipt)
        self.assertTrue(self.client.verify(self.receipt))

    def test_same_id_cannot_reseal_for_another_job_generation_or_pipeline(self):
        self.client.fetch(self.job,self.grant)
        for field,value in (('jobId','00000000-0000-4000-8000-000000000124'),('generation',2)):
            changed=copy.deepcopy(self.job);changed[field]=value
            if field=='generation':changed['transferRequest']['generation']=value
            grant=replace(self.grant,job_id=changed['jobId'],generation=changed['generation'])
            with self.subTest(field=field),self.assertRaises(ProvenanceUnavailable):self.client.fetch(changed,grant)
        changed=copy.deepcopy(self.job)
        changed['submission']['pipeline']['treedar']['extra']='unsupported'
        changed['pipelineDigest']=p.digest(changed['submission']['pipeline'])
        with self.assertRaises(ProvenanceUnavailable):self.client.fetch(changed,self.grant)
        self.assertEqual(len(self.requests),1)

    def test_fresh_intent_is_required_for_new_generation(self):
        self.client.fetch(self.job,self.grant)
        job=copy.deepcopy(self.job);job['generation']=2;job['transferRequest']['generation']=2
        job['submission']['pipeline']['treedar']['provenanceReceiptId']='provenance-2'
        job['pipelineDigest']=p.digest(job['submission']['pipeline'])
        self.receipt=dict(self.receipt,receiptId='provenance-2',generation=2)
        self.wrapper=dict(self.wrapper,pipelineDigest=job['pipelineDigest'],provenance=self.receipt)
        self.assertEqual(self.client.fetch(job,replace(self.grant,generation=2)),self.receipt)
        with self.client._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM provenance_receipts').fetchone()[0],2)

    def test_wrapper_and_object_bindings_synthetic_units_and_datum_fail_closed(self):
        for field,value in (('jobId','other'),('generation',2),('environment','production'),('objectId','other'),
                ('objectSha256','f'*64),('byteSize',4),('receiptId','other'),('sourceKind','synthetic_lidar'),
                ('verticalUnits','feet'),('verticalDatum','unknown')):
            self.wrapper['provenance']={**self.receipt,field:value}
            with self.subTest(field=field),self.assertRaises(ProvenanceUnavailable):self.client.fetch(self.job,self.grant)
        self.wrapper['provenance']=self.receipt
        for field in ('inputManifestSha256','pipelineDigest'):
            wrapper=copy.deepcopy(self.wrapper);wrapper[field]='f'*64
            self.reply_override=lambda request,timeout:Reply(p.canonical_json(wrapper),request.full_url)
            with self.subTest(field=field),self.assertRaises(ProvenanceUnavailable):self.client.fetch(self.job,self.grant)
        self.reply_override=None
        self.assertTrue(self.client.fetch(self.job,self.grant))

    def test_cache_cannot_authorize_expiry_unknown_clock_or_changed_issuer(self):
        self.client.fetch(self.job,self.grant)
        self.now-=1;self.assertFalse(self.client.verify(self.receipt))
        with self.assertRaises(ProvenanceUnavailable):self.client.fetch(self.job,self.grant)
        self.now+=122;self.assertFalse(self.client.verify(self.receipt))
        with self.assertRaises(ProvenanceUnavailable):self.client.fetch(self.job,self.grant)
        self.assertEqual(len(self.requests),1)
        self.broker.origin='https://other.invalid'
        self.assertFalse(self.client.verify(self.receipt))

    def test_receipt_evidence_cannot_be_resealed_and_freshness_is_separate_from_grant(self):
        client=LidarProvenanceClient(self.broker,state_path=self.root/'short-freshness.sqlite',
            clock=self.clock,freshness_seconds=2)
        client.fetch(self.job,self.grant)
        self.now+=3
        self.assertLess(self.now,self.grant.expires)
        self.assertFalse(client.verify(self.receipt))
        self.assertEqual(client.fetch(self.job,self.grant),self.receipt)
        self.wrapper['provenance']={**self.receipt,'verticalDatum':'ellipsoidal'}
        # Even another policy-approved datum cannot replace the exact evidence
        # already sealed under this receipt ID.
        with self.assertRaises(ProvenanceUnavailable):client.fetch(self.job,self.grant)
        self.assertFalse(client.verify(self.receipt))

    def test_metadata_size_redirect_encoding_and_duplicate_keys_release_capacity(self):
        variants=[lambda request:Reply(b'x'*70000,request.full_url),
            lambda request:Reply(p.canonical_json(self.wrapper),'https://other.invalid/provenance'),
            lambda request:Reply(p.canonical_json(self.wrapper),request.full_url,{'Content-Type':'application/json','Content-Encoding':'gzip'}),
            lambda request:Reply(b'{"schemaVersion":1,"schemaVersion":1}',request.full_url),
            lambda request:Reply(b'\xff',request.full_url),
            lambda request:Reply(p.canonical_json(self.wrapper),request.full_url,{'Content-Type':'application/json','Content-Length':'1'})]
        for variant in variants:
            self.reply_override=lambda request,timeout:variant(request)
            with self.assertRaises(ProvenanceUnavailable) as error:self.client.fetch(self.job,self.grant)
            self.assertNotIn(self.grant.token,str(error.exception));self.assertNotIn(self.broker.origin,str(error.exception))
        self.reply_override=None
        self.assertEqual(self.client.fetch(self.job,self.grant),self.receipt)

    def test_expiry_during_body_read_never_seals(self):
        fixture=self
        class Expiring(Reply):
            def read1(self,count):
                result=super().read1(count);fixture.now+=121;return result
        self.reply_override=lambda request,timeout:Expiring(p.canonical_json(self.wrapper),request.full_url)
        with self.assertRaises(ProvenanceUnavailable):self.client.fetch(self.job,self.grant)
        self.assertFalse(self.client.verify(self.receipt))

    def test_concurrent_metadata_read_is_bounded(self):
        def recursive(request,timeout):
            with self.assertRaisesRegex(ProvenanceUnavailable,'capacity'):self.client.fetch(self.job,self.grant)
            return Reply(p.canonical_json(self.wrapper),request.full_url)
        self.reply_override=recursive
        self.assertEqual(self.client.fetch(self.job,self.grant),self.receipt)
        self.assertEqual(len(self.requests),1)

    def test_late_eof_cannot_seal_after_total_read_deadline(self):
        ticks=[100.0]
        fixture=self
        class LateEof(Reply):
            def read1(self,count):
                result=super().read1(count)
                if not result:ticks[0]+=31
                return result
        self.reply_override=lambda request,timeout:LateEof(p.canonical_json(fixture.wrapper),request.full_url)
        with patch('processing.reference_provenance.time.monotonic',side_effect=lambda:ticks[0]):
            with self.assertRaisesRegex(ProvenanceUnavailable,'deadline'):self.client.fetch(self.job,self.grant)
        self.assertFalse(self.client.verify(self.receipt))

    def test_issuer_change_and_hardlink_journal_rejected(self):
        self.client.fetch(self.job,self.grant)
        other=BrokerClient('https://other.invalid',clock=self.clock)
        with self.assertRaises(ValueError):LidarProvenanceClient(other,state_path=self.client.path,clock=self.clock)
        alias=self.root/'alias.sqlite'
        try:alias.hardlink_to(self.client.path)
        except (OSError,NotImplementedError):self.skipTest('hardlinks unavailable')
        with self.assertRaises(ValueError):LidarProvenanceClient(self.broker,state_path=alias,clock=self.clock)


if __name__=='__main__':unittest.main()
