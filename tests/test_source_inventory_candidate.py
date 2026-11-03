import copy
import hashlib
import json
import os
import subprocess
import unittest

from processing import source_inventory_candidate as candidate


class SourceInventoryCandidateTests(unittest.TestCase):
    def setUp(self):
        self.gcp=b'EPSG:32616\n1 2 3 4 5 image.jpg\n'
        self.expected=dict(datasetId='dataset-1',manifestSha256='a'*64,
            viewerTaskId='10000000-0000-4000-8000-000000000001',
            viewerAttemptId='20000000-0000-4000-8000-000000000001',pipelineDigest='b'*64)
        self.inv=dict(protocol=candidate.PROTOCOL,schemaVersion=1,
            dataset=dict(id='dataset-1',manifestSha256='a'*64),
            binding={k:self.expected[k] for k in ('viewerTaskId','viewerAttemptId','pipelineDigest')},
            processing=[dict(datasetFileId='photo-1',relativePath='A/cafe\u0301/photo.jpg',byteSize=1,sha256='c'*64,processingRole='image',contentType='image/jpeg'),
                        dict(datasetFileId='photo-2',relativePath='B/café/photo.jpg',byteSize=2,sha256='d'*64,processingRole='image',contentType='image/jpeg')],
            retained=[dict(datasetFileId='csv-1',relativePath='Original/GCP.csv',byteSize=3,sha256='e'*64,processingRole='gcp_source',contentType='text/csv')],
            generated=[dict(snapshotId=self.expected['viewerAttemptId'],byteSize=len(self.gcp),
                sha256=hashlib.sha256(self.gcp).hexdigest(),sourceGcpDatasetFileIds=['csv-1'],
                imageDatasetFileIds=['photo-1','photo-2'],correspondenceCount=3)])
        fields=('datasetFileId','relativePath','byteSize','sha256','contentType')
        self.authoritative=dict(id='dataset-1',manifestSha256='a'*64,files=sorted(
            [{key:v[key] for key in fields} for v in self.inv['processing']+self.inv['retained']],key=lambda v:v['datasetFileId']))

    def validate(self, inventory=None, *, expected=None, gcp=None):
        raw=json.dumps(self.inv if inventory is None else inventory,ensure_ascii=False).encode()
        return candidate.validate(raw,expected=self.expected if expected is None else expected,
                                  authoritative_dataset=self.authoritative,generated_gcp_bytes=self.gcp if gcp is None else gcp)

    def test_exact_paths_bytes_and_immutable_binding(self):
        canonical,digest=self.validate()
        decoded=json.loads(canonical)
        self.assertEqual(decoded['processing'][0]['relativePath'],'A/cafe\u0301/photo.jpg')
        self.assertEqual(decoded['processing'][1]['relativePath'],'B/café/photo.jpg')
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
        self.validate(obj)  # Inventory preserves case; a target must independently reject collisions.
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
        candidate.validate(json.dumps(obj).encode(),expected=self.expected,authoritative_dataset=self.authoritative,generated_gcp_bytes=None)

    def test_exact_authoritative_union_and_order(self):
        obj=copy.deepcopy(self.inv);obj['processing'].pop()
        with self.assertRaisesRegex(ValueError,'closure mismatch'):self.validate(obj)
        obj=copy.deepcopy(self.inv);obj['processing'].reverse()
        with self.assertRaisesRegex(ValueError,'order invalid'):self.validate(obj)
        obj=copy.deepcopy(self.inv);obj['processing'][0]['processingRole']='auto'
        with self.assertRaises(ValueError):self.validate(obj)
        self.authoritative['manifestSha256']='0'*64
        with self.assertRaisesRegex(ValueError,'authoritative dataset mismatch'):self.validate()

    def test_node_jcs_produces_identical_original_unicode_bytes(self):
        module=os.environ.get('NODE_JCS_MODULE')
        if not module:self.skipTest('pinned Node canonicalize module required')
        canonical,digest=self.validate()
        script="const j=require(process.argv[1]),c=require('node:crypto');let s='';process.stdin.setEncoding('utf8');process.stdin.on('data',x=>s+=x);process.stdin.on('end',()=>{let b=Buffer.from(j(JSON.parse(s)));console.log(JSON.stringify([b.toString('hex'),c.createHash('sha256').update(Buffer.concat([Buffer.from('ltds-source-inventory-v1\\0'),b])).digest('hex')]))})"
        result=subprocess.run(['node','-e',script,module],input=json.dumps(self.inv,ensure_ascii=True),text=True,capture_output=True,check=True)
        self.assertEqual(json.loads(result.stdout),[canonical.hex(),digest])


if __name__ == '__main__':unittest.main()
