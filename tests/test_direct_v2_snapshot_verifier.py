"""Synthetic signed Viewer-source preimage tests, never a custody ACK."""
import copy
import hashlib
import json
import os
import subprocess
import sqlite3
import tempfile
import unittest
from contextlib import closing, contextmanager, nullcontext
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives import serialization
from processing import reference_protocol as p
from processing.direct_v2_snapshot_verifier import verify_snapshot, SNAPSHOT_DOMAIN, PAGE_DOMAIN
from processing.direct_v2_reservation import DirectV2Reservations
from processing.maintenance import Conflict

RES='11111111-1111-4111-8111-111111111111'
TASK='22222222-2222-4222-8222-222222222222'
ATTEMPT='33333333-3333-4333-8333-333333333333'
DATASET='44444444-4444-4444-8444-444444444444'
FILE='55555555-5555-4555-8555-555555555555'

class SnapshotTests(unittest.TestCase):
    def setUp(self):
        self.key=Ed25519PrivateKey.generate()
        self.source={'schemaVersion':1,'id':DATASET,'datasetManifestSha256':'a'*64,'totalBytes':3,'objectCount':1,
            'objects':[{'id':FILE,'name':'a.jpg','byteSize':3,'sha256':'b'*64,'sourceType':'raw_input',
                        'retentionClass':'release_after_acceptance','retentionHoldId':None}]}
        self.source['sha256']=p.digest(self.source)
        self.derived={'schemaVersion':1,'attemptId':ATTEMPT,'objects':[]};self.derived['sha256']=p.digest(self.derived)
        self.record=dict(type='dataset_file',datasetFileId=FILE,relativePath='flight/a.jpg',byteSize=3,sha256='b'*64,
                         processingRole='image',mediaType='image/jpeg',kind='image')
        self.page=dict(version='ltds-viewer-source-frames-v1',index=0,records=[self.record])
        self.intent=dict(protocol='ltds-viewer-held-source-snapshot-v1',snapshotId=RES,issuerId='issuer',keyId='key',
            createdAt='2026-11-06T12:00:00.000Z',datasetId=DATASET,datasetManifestSha256='a'*64,taskId=TASK,
            attemptId=ATTEMPT,inputManifestSha256='c'*64,sourceCount=1,datasetFileCount=1,sourcePageHashes=[],
            sourceFrameBytes=0,selectedCount=1,selectedPageHashes=['d'*64],selectedFrameBytes=10,gcp=None)
        self.prepare()

    def prepare(self):
        page=p.canonical_json(self.page);self.frames=page+b'\n'
        self.intent['sourcePageHashes']=[hashlib.sha256(PAGE_DOMAIN+page).hexdigest()]
        self.intent['sourceFrameBytes']=len(self.frames);self.raw=p.canonical_json(self.intent)
        self.sha=hashlib.sha256(self.raw).hexdigest();self.signature=self.key.sign(SNAPSHOT_DOMAIN+self.raw)

    def verify(self,**overrides):
        values=dict(intent_bytes=self.raw,signature=self.signature,source_frames=self.frames,public_key=self.key.public_key(),
            issuer_id='issuer',key_id='key',snapshot_sha=self.sha,source_manifest=self.source,
            derived_manifest=self.derived,task_id=TASK,pipeline_sha='c'*64)
        values.update(overrides);return verify_snapshot(**values)

    def test_signature_pages_and_source_closure(self):
        result=self.verify();self.assertEqual(result['state'],'source_pages_verified_not_custody')
        self.assertFalse(result['sourceCustodyAccepted']);self.assertFalse(result['processingAuthorized'])
        self.assertNotIn('paths',result)
        for field,value in [('snapshot_sha','0'*64),('task_id',RES),('pipeline_sha','d'*64),('key_id','other'),
                            ('source_frames',self.frames.replace(b'flight/',b'other/'))]:
            with self.subTest(field=field),self.assertRaises(ValueError):self.verify(**{field:value})
        with self.assertRaises(ValueError):self.verify(public_key=Ed25519PrivateKey.generate().public_key())

    def test_resigned_wrong_object_or_duplicate_still_rejected(self):
        self.page['records'][0]['relativePath']='flight/b.jpg';self.prepare()
        with self.assertRaises(ValueError):self.verify()
        self.page['records'][0]['relativePath']='flight/a.jpg';self.page['records'].append(copy.deepcopy(self.record))
        self.intent['sourceCount']=2;self.prepare()
        with self.assertRaises(ValueError):self.verify()

    def test_duplicate_path_or_case_folded_basename_across_distinct_ids_rejected(self):
        for path in ('flight/a.jpg','another/A.JPG'):
            with self.subTest(path=path):
                self.setUp();other='66666666-6666-4666-8666-666666666666'
                obj=copy.deepcopy(self.source['objects'][0]);obj['id']=other;obj['name']=path.rsplit('/',1)[-1]
                self.source['objects'].append(obj);self.source['objectCount']=2;self.source['totalBytes']=6
                self.source['sha256']=p.digest({k:v for k,v in self.source.items() if k!='sha256'})
                record=copy.deepcopy(self.record);record['datasetFileId']=other;record['relativePath']=path
                self.page['records'].append(record);self.intent['datasetFileCount']=2;self.intent['sourceCount']=2;self.prepare()
                with self.assertRaises(ValueError):self.verify()

    def test_source_type_role_and_media_mismatches_fail_closed(self):
        for changes in ({'byteSize':True},{'kind':'lidar'},{'mediaType':'text/plain'},{'processingRole':'gcp_source'}):
            with self.subTest(changes=changes):
                self.setUp();self.record.update(changes);self.prepare()
                with self.assertRaises(ValueError):self.verify()
        self.setUp();self.record['relativePath']='flight/source.csv';self.record['kind']='auxiliary';self.record['mediaType']='text/csv'
        self.source['objects'][0].update(name='source.csv',sourceType='gcp_source_csv',retentionClass='permanent_viewer_hold',retentionHoldId=FILE)
        self.prepare()
        with self.assertRaises(ValueError):self.verify()

    def test_generated_gcp_snapshot_is_explicitly_held(self):
        self.intent['gcp']={'generatedGcp':{'sha256':'a'*64,'byteSize':True}}
        self.prepare()
        with self.assertRaisesRegex(ValueError,'GCP snapshot inspection unsupported'):self.verify()

    def test_raw_gcp_source_without_generated_snapshot_is_explicitly_held(self):
        self.record.update(relativePath='flight/source.csv',kind='auxiliary',mediaType='text/csv',processingRole='gcp_source')
        self.source['objects'][0].update(name='source.csv',sourceType='gcp_source_csv',
            retentionClass='permanent_viewer_hold',retentionHoldId=FILE)
        self.prepare()
        with self.assertRaisesRegex(ValueError,'GCP snapshot inspection unsupported'):self.verify()

    @unittest.skipUnless(os.environ.get('LTDS_VIEWER_SOURCE_ROOT'),'actual Viewer source not supplied')
    def test_actual_viewer_node_frame_encoder_and_signer(self):
        script=r"""
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const input=JSON.parse(fs.readFileSync(0,'utf8'));
const {frameStream,SOURCE_VERSION,SOURCE_DOMAIN}=require(path.join(input.root,'server/viewerPagedSourceExport.js'));
const {canonicalBytes}=require(path.join(input.root,'server/referenceProtocol.js'));
const pages=frameStream([input.record],SOURCE_VERSION,SOURCE_DOMAIN);
input.intent.sourcePageHashes=pages.pageHashes;input.intent.sourceFrameBytes=pages.bytes.length;
const raw=canonicalBytes(input.intent),sig=crypto.sign(null,Buffer.concat([Buffer.from('ltds-viewer-held-source-snapshot-v1\0','ascii'),raw]),input.privateKey);
process.stdout.write(JSON.stringify({intent:raw.toString('base64'),frames:pages.bytes.toString('base64'),signature:sig.toString('base64')}));
"""
        import base64
        pem=self.key.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,serialization.NoEncryption()).decode()
        result=subprocess.run(['node','-e',script],input=json.dumps(dict(root=os.environ['LTDS_VIEWER_SOURCE_ROOT'],
            record=self.record,intent=self.intent,privateKey=pem)),capture_output=True,text=True,check=True,timeout=15)
        value=json.loads(result.stdout);raw=base64.b64decode(value['intent']);frames=base64.b64decode(value['frames'])
        observed=self.verify(intent_bytes=raw,source_frames=frames,signature=base64.b64decode(value['signature']),
                             snapshot_sha=hashlib.sha256(raw).hexdigest())
        self.assertFalse(observed['sourceCustodyAccepted'])

    def test_actual_reservation_method_uses_pinned_owner_and_stored_snapshot_hash(self):
        with tempfile.TemporaryDirectory() as root:
            path=Path(root)/'queue.db'
            with closing(sqlite3.connect(path)) as db:
                db.execute('CREATE TABLE reference_source_intakes(job_id TEXT,source_manifest_json TEXT,derived_manifest_json TEXT,viewer_task_id TEXT,source_attestation_json TEXT)')
                db.execute('CREATE TABLE reference_jobs(job_id TEXT,pipeline_digest TEXT)')
                db.execute('INSERT INTO reference_jobs VALUES(?,?)',('job','e'*64))
                db.execute('INSERT INTO reference_source_intakes VALUES(?,?,?,?,?)',('job',json.dumps(self.source),json.dumps(self.derived),TASK,json.dumps({'proof':{'principalId':'viewer','sourceSnapshotSha256':self.sha,'sourceManifestSha256':self.source['sha256'],'derivedManifestSha256':self.derived['sha256']}})))
                db.commit()
            class Queue:
                authority_id=RES
                @contextmanager
                def _db(self):
                    db=sqlite3.connect(path);db.row_factory=sqlite3.Row
                    try:yield db
                    finally:db.close()
                def coordinator_operation(self,_name):return nullcontext()
            candidate=object.__new__(DirectV2Reservations);candidate.queue=Queue()
            candidate.store=SimpleNamespace(_source_routing_keys={'synthetic':'pinned'})
            candidate._snapshot_pins={('viewer','staging','issuer','key'):self.key.public_key()}
            row={'principal':'viewer','environment':'staging','job_id':'job','pipeline_manifest_sha':'c'*64,
                 'source_manifest_sha':self.source['sha256'],'derived_manifest_sha':self.derived['sha256'],
                 'item_json':json.dumps({'attemptId':ATTEMPT,'jobId':'job'})}
            with patch('processing.direct_v2_reservation_schema.validate_receiving',return_value=row), patch('processing.viewer_source_attestation.validate_and_verify'):
                args=dict(authenticated_principal='viewer',issuer_id='issuer',key_id='key',intent_bytes=self.raw,signature=self.signature,source_frames=self.frames)
                self.assertFalse(candidate.inspect_held_snapshot(RES,**args)['sourceCustodyAccepted'])
                with self.assertRaises(Conflict):candidate.inspect_held_snapshot(RES,**{**args,'authenticated_principal':'other'})
                with self.assertRaises(Conflict):candidate.inspect_held_snapshot(RES,**{**args,'key_id':'other'})
                with closing(sqlite3.connect(path)) as db:
                    db.execute('UPDATE reference_source_intakes SET source_attestation_json=?',(json.dumps({'proof':{'principalId':'viewer','sourceSnapshotSha256':'0'*64,'sourceManifestSha256':self.source['sha256'],'derivedManifestSha256':self.derived['sha256']}}),))
                    db.commit()
                with self.assertRaises(Conflict):candidate.inspect_held_snapshot(RES,**args)

if __name__=='__main__':unittest.main()
