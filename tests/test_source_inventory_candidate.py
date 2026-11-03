import copy
import hashlib
import json
import os
import subprocess
import unittest
from unittest.mock import patch

from processing import source_inventory_candidate as candidate


class SourceInventoryCandidateTests(unittest.TestCase):
    def setUp(self):
        self.gcp=b'EPSG:4326\n-88 44 100 1 2 photo-1.jpg\n-88 44 100 3 4 photo-1.jpg\n-88 44 100 5 6 photo-2.jpg\n'
        self.expected=dict(datasetId='dataset-1',manifestSha256='a'*64,
            viewerTaskId='10000000-0000-4000-8000-000000000001',
            viewerAttemptId='20000000-0000-4000-8000-000000000001',pipelineDigest='b'*64)
        self.inv=dict(protocol=candidate.PROTOCOL,schemaVersion=1,
            dataset=dict(id='dataset-1',manifestSha256='a'*64),
            binding={k:self.expected[k] for k in ('viewerTaskId','viewerAttemptId','pipelineDigest')},
            processing=[dict(datasetFileId='photo-1',relativePath='A/cafe\u0301/photo-1.jpg',byteSize=1,sha256='c'*64,processingRole='image',contentType='image/jpeg'),
                        dict(datasetFileId='photo-2',relativePath='B/café/photo-2.jpg',byteSize=2,sha256='d'*64,processingRole='image',contentType='image/jpeg')],
            retained=[dict(datasetFileId='csv-1',relativePath='Original/GCP.csv',byteSize=3,sha256='e'*64,processingRole='gcp_source',contentType='text/csv')],
            generated=[dict(snapshotId=self.expected['viewerAttemptId'],byteSize=len(self.gcp),
                sha256=hashlib.sha256(self.gcp).hexdigest(),sourceGcpDatasetFileIds=['csv-1'],
                imageDatasetFileIds=['photo-1','photo-2'],correspondenceCount=3)])
        fields=('datasetFileId','relativePath','byteSize','sha256','contentType')
        self.authoritative=dict(id='dataset-1',manifestSha256='a'*64,files=sorted(
            [{**{key:v[key] for key in fields},'mimeType':v['contentType'],'imageWidth':6000 if v['processingRole']=='image' else None,'imageHeight':4000 if v['processingRole']=='image' else None} for v in self.inv['processing']+self.inv['retained']],key=lambda v:v['datasetFileId']))
        self.selection={k:self.expected[k] for k in ('viewerTaskId','viewerAttemptId','pipelineDigest')}
        self.selection.update(gcpRequired=True,files=sorted(
            [dict(datasetFileId=v['datasetFileId'],processingRole=v['processingRole'],disposition=disposition)
             for disposition, group in (('processing',self.inv['processing']),('retained',self.inv['retained']))
             for v in group],key=lambda v:v['datasetFileId']))
        def row(i,image,x,y):return dict(correspondenceId=f'mark-{i}',gcpPointId=f'point-{i}',gcpSetId='set-1',externalId=f'P{i}',label=f'P{i}',
            sourceGcpDatasetFileId='csv-1',sourceGcpSha256='e'*64,sourceGcpRelativePath='Original/GCP.csv',imageDatasetFileId=image,
            imageRelativePath=self.inv['processing'][int(image[-1])-1]['relativePath'],imageWidth=6000,imageHeight=4000,
            longitude=-88,latitude=44,elevationM=100,pixelX=x,pixelY=y)
        self.evidence=dict(schemaVersion=1,snapshot=copy.deepcopy(self.inv['generated'][0]),correspondences=[
            row(1,'photo-1',1,2),row(2,'photo-1',3,4),row(3,'photo-2',5,6)])

    def validate(self, inventory=None, *, expected=None, gcp=None):
        raw=json.dumps(self.inv if inventory is None else inventory,ensure_ascii=False).encode()
        return candidate.validate(raw,expected=self.expected if expected is None else expected,
                                  authoritative_dataset=self.authoritative,trusted_selection=self.selection,
                                  trusted_gcp_evidence=json.dumps(self.evidence).encode(),
                                  generated_gcp_bytes=self.gcp if gcp is None else gcp)

    def test_exact_paths_bytes_and_immutable_binding(self):
        canonical,digest,hold=self.validate()
        self.assertEqual(hold,'held_gcp_correspondence_unverified')
        decoded=json.loads(canonical)
        self.assertEqual(decoded['processing'][0]['relativePath'],'A/cafe\u0301/photo-1.jpg')
        self.assertEqual(decoded['processing'][1]['relativePath'],'B/café/photo-2.jpg')
        self.assertEqual(digest,hashlib.sha256(candidate.DOMAIN+canonical).hexdigest())
        for key in self.expected:
            expected=copy.deepcopy(self.expected);expected[key]='0'*64 if key.endswith('Digest') or key.endswith('Sha256') else 'other'
            with self.subTest(key=key),self.assertRaises(ValueError):self.validate(expected=expected)

    def test_paths_exact_duplicates_rejected_and_case_variants_preserved(self):
        for path in ('../photo.jpg','a/../photo.jpg','/absolute','C:/file','a\\b','a//b','a/./b','x/','a/\u0000b'):
            obj=copy.deepcopy(self.inv);obj['processing'][0]['relativePath']=path
            with self.subTest(path=path),self.assertRaises(ValueError):self.validate(obj)
        obj=copy.deepcopy(self.inv);obj['processing'][1]['relativePath']=obj['processing'][0]['relativePath']
        with self.assertRaises(ValueError):self.validate(obj)
        obj=copy.deepcopy(self.inv);obj['processing'][1]['datasetFileId']='photo-1'
        with self.assertRaises(ValueError):self.validate(obj)
        obj=copy.deepcopy(self.inv);obj['processing'][0]['relativePath']='a/Photo.jpg';obj['processing'][1]['relativePath']='a/photo.jpg'
        self.authoritative['files'][1]['relativePath']='a/Photo.jpg';self.authoritative['files'][2]['relativePath']='a/photo.jpg'
        obj['generated']=[]; self.selection['gcpRequired']=False
        self.assertEqual(candidate.validate(json.dumps(obj).encode(),expected=self.expected,authoritative_dataset=self.authoritative,trusted_selection=self.selection,trusted_gcp_evidence=None,generated_gcp_bytes=None)[2],'held_source_authority_unverified')
        with self.assertRaisesRegex(ValueError,'basename collision'):candidate.reference_v1_names(obj['processing'])
        obj['processing'][0]['relativePath']='a/café.jpg'
        with self.assertRaises(ValueError):candidate.reference_v1_names(obj['processing'])

    def test_gcp_snapshot_requires_exact_bytes_and_retained_source(self):
        with self.assertRaises(ValueError):self.validate(gcp=self.gcp+b'x')
        for section,key,value in [('generated','sourceGcpDatasetFileIds',['missing']),('generated','byteSize',1),
                                  ('generated','sha256','0'*64),('retained','processingRole','unknown'),
                                  ('generated','snapshotId','wrong-attempt'),('generated','imageDatasetFileIds',['photo-1','photo-1']),
                                  ('generated','correspondenceCount',1)]:
            obj=copy.deepcopy(self.inv);obj[section][0][key]=value
            with self.subTest(key=key),self.assertRaises(ValueError):self.validate(obj)
        obj=copy.deepcopy(self.inv);obj['generated']=[]
        with self.assertRaisesRegex(ValueError,'GCP requirement mismatch'):
            candidate.validate(json.dumps(obj).encode(),expected=self.expected,authoritative_dataset=self.authoritative,trusted_selection=self.selection,trusted_gcp_evidence=None,generated_gcp_bytes=None)
        self.selection['gcpRequired']=False
        self.assertEqual(candidate.validate(json.dumps(obj).encode(),expected=self.expected,authoritative_dataset=self.authoritative,trusted_selection=self.selection,trusted_gcp_evidence=None,generated_gcp_bytes=None)[2],'held_source_authority_unverified')
        with self.assertRaisesRegex(ValueError,'GCP requirement mismatch'):self.validate()

    def test_relabelled_or_removed_photo_and_attempt_selection_hold(self):
        obj=copy.deepcopy(self.inv)
        photo=obj['processing'].pop();photo['processingRole']='administrative';obj['retained'].append(photo)
        with self.assertRaisesRegex(ValueError,'attempt selection mismatch'):self.validate(obj)
        self.selection['viewerAttemptId']='30000000-0000-4000-8000-000000000001'
        with self.assertRaisesRegex(ValueError,'trusted attempt selection mismatch'):self.validate()

    def test_exact_authoritative_union_and_order(self):
        obj=copy.deepcopy(self.inv);obj['processing'].pop()
        with self.assertRaisesRegex(ValueError,'closure mismatch'):self.validate(obj)
        obj=copy.deepcopy(self.inv);obj['processing'].reverse()
        with self.assertRaisesRegex(ValueError,'order invalid'):self.validate(obj)
        obj=copy.deepcopy(self.inv);obj['processing'][0]['processingRole']='auto'
        with self.assertRaises(ValueError):self.validate(obj)
        self.authoritative['manifestSha256']='0'*64
        with self.assertRaisesRegex(ValueError,'authoritative dataset mismatch'):self.validate()

    def test_gcp_counts_against_ten_thousand_engine_object_cap(self):
        photo=self.inv['processing'][0]
        self.inv['processing']=[dict(photo,datasetFileId=f'photo-{i:05d}',relativePath=f'photos/{i:05d}.jpg') for i in range(9999)]
        self.inv['generated'][0]['imageDatasetFileIds']=['photo-00000']
        self.inv['generated'][0]['correspondenceCount']=1
        self.gcp=b'EPSG:4326\n-88 44 100 1 2 00000.jpg\n'
        self.inv['generated'][0].update(byteSize=len(self.gcp),sha256=hashlib.sha256(self.gcp).hexdigest())
        self.evidence=dict(schemaVersion=1,snapshot=copy.deepcopy(self.inv['generated'][0]),correspondences=[dict(correspondenceId='mark-1',gcpPointId='point-1',gcpSetId='set-1',externalId='P1',label='P1',sourceGcpDatasetFileId='csv-1',sourceGcpSha256='e'*64,sourceGcpRelativePath='Original/GCP.csv',imageDatasetFileId='photo-00000',imageRelativePath='photos/00000.jpg',imageWidth=6000,imageHeight=4000,longitude=-88,latitude=44,elevationM=100,pixelX=1,pixelY=2)])
        fields=('datasetFileId','relativePath','byteSize','sha256','contentType')
        self.authoritative['files']=sorted([{**{k:v[k] for k in fields},'mimeType':v['contentType'],'imageWidth':6000 if v['processingRole']=='image' else None,'imageHeight':4000 if v['processingRole']=='image' else None} for v in self.inv['processing']+self.inv['retained']],key=lambda v:v['datasetFileId'])
        self.selection['files']=sorted([dict(datasetFileId=v['datasetFileId'],processingRole=v['processingRole'],disposition=disposition)
            for disposition,group in (('processing',self.inv['processing']),('retained',self.inv['retained'])) for v in group],key=lambda v:v['datasetFileId'])
        self.validate()
        self.inv['processing'].append(dict(photo,datasetFileId='photo-99999',relativePath='photos/99999.jpg'))
        with self.assertRaisesRegex(ValueError,'engine object capacity'):self.validate()

    def test_trusted_gcp_rows_and_null_content_type_fallback(self):
        self.authoritative['files'][1]['contentType']=None
        self.validate()
        self.evidence['correspondences'][0]['imageDatasetFileId']='other'
        with self.assertRaisesRegex(ValueError,'correspondence mismatch'):self.validate()

    def test_viewer_fields_source_dimensions_and_unique_identity(self):
        changes=[('sourceGcpSha256','0'*64),('sourceGcpRelativePath','wrong.csv'),
                 ('imageRelativePath','other.jpg'),('imageWidth',5),('pixelX',6000),
                 ('elevationM',100001),('longitude',181)]
        for key,value in changes:
            original=copy.deepcopy(self.evidence);self.evidence['correspondences'][0][key]=value
            with self.subTest(key=key),self.assertRaises(ValueError):self.validate()
            self.evidence=original
        self.evidence['correspondences'][1]['correspondenceId']=self.evidence['correspondences'][0]['correspondenceId']
        with self.assertRaises(ValueError):self.validate()

    def test_evidence_bounds_precede_json_allocation(self):
        with patch.object(candidate,'MAX_CORRESPONDENCES',2):
            raw=b'{"snapshot":{},"correspondences":[{},{},{}]}'
            with patch.object(candidate,'_parse',side_effect=AssertionError('parser must not run')):
                with self.assertRaisesRegex(ValueError,'row bound'):candidate.decode_viewer_gcp_evidence(raw)
        with patch.object(candidate,'MAX_EVIDENCE_BYTES',8):
            with patch.object(candidate,'_parse',side_effect=AssertionError('parser must not run')):
                with self.assertRaisesRegex(ValueError,'bounded evidence'):candidate.decode_viewer_gcp_evidence(b' '*9)

    def test_twenty_thousand_custody_sources_with_ten_thousand_engine_objects(self):
        photo=self.inv['processing'][0]; aux=self.inv['retained'][0]
        self.inv['processing']=[dict(photo,datasetFileId=f'photo-{i:05d}',relativePath=f'photos/{i:05d}.jpg') for i in range(10000)]
        self.inv['retained']=[dict(aux,datasetFileId=f'admin-{i:05d}',relativePath=f'other/{i:05d}.txt',processingRole='administrative') for i in range(10000)]
        self.inv['generated']=[]; self.selection['gcpRequired']=False
        fields=('datasetFileId','relativePath','byteSize','sha256','contentType')
        self.authoritative['files']=sorted([{**{k:v[k] for k in fields},'mimeType':v['contentType'],'imageWidth':6000 if v['processingRole']=='image' else None,'imageHeight':4000 if v['processingRole']=='image' else None}
            for v in self.inv['processing']+self.inv['retained']],key=lambda v:v['datasetFileId'])
        self.selection['files']=sorted([dict(datasetFileId=v['datasetFileId'],processingRole=v['processingRole'],disposition=disposition)
            for disposition,group in (('processing',self.inv['processing']),('retained',self.inv['retained'])) for v in group],key=lambda v:v['datasetFileId'])
        raw=json.dumps(self.inv,separators=(',',':')).encode()
        self.assertEqual(candidate.validate(raw,expected=self.expected,authoritative_dataset=self.authoritative,
            trusted_selection=self.selection,trusted_gcp_evidence=None,generated_gcp_bytes=None)[2], 'held_source_authority_unverified')

    def test_node_jcs_produces_identical_original_unicode_bytes(self):
        module=os.environ.get('NODE_JCS_MODULE')
        if not module:self.skipTest('pinned Node canonicalize module required')
        canonical,digest,_=self.validate()
        script="const j=require(process.argv[1]),c=require('node:crypto');let s='';process.stdin.setEncoding('utf8');process.stdin.on('data',x=>s+=x);process.stdin.on('end',()=>{let b=Buffer.from(j(JSON.parse(s)));console.log(JSON.stringify([b.toString('hex'),c.createHash('sha256').update(Buffer.concat([Buffer.from('ltds-source-inventory-v1\\0'),b])).digest('hex')]))})"
        result=subprocess.run(['node','-e',script,module],input=json.dumps(self.inv,ensure_ascii=True),text=True,capture_output=True,check=True)
        self.assertEqual(json.loads(result.stdout),[canonical.hex(),digest])


if __name__ == '__main__':unittest.main()
