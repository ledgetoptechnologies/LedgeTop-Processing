import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import unittest

from processing.gcp_snapshot_semantics_candidate import validate_gcp_semantics


class GcpSemanticsTests(unittest.TestCase):
    def setUp(self):
        self.processing=[dict(datasetFileId='image-1',relativePath='flight/IMG_0001.JPG',byteSize=1,sha256='a'*64,processingRole='image',contentType='image/jpeg')]
        self.rows=[dict(sourceGcpDatasetFileId='csv-1',imageDatasetFileId='image-1',pointId='point-1',
                        longitude=-88.1002,latitude=44.5001,elevationM=243.84,pixelX=120.5,pixelY=240.25)]
        self.raw=b'EPSG:4326\n-88.1002 44.5001 243.84 120.5 240.25 IMG_0001.JPG\n'

    def test_wrong_coordinates_names_crs_count_and_duplicate_rejected(self):
        validate_gcp_semantics(self.raw,processing=self.processing,correspondences=self.rows)
        for raw in (self.raw.replace(b'-88.1002',b'-88.1003'),self.raw.replace(b'IMG_0001.JPG',b'other.JPG'),
                    self.raw.replace(b'EPSG:4326',b'EPSG:32616'),self.raw.replace(b'120.5',b'NaN'),
                    self.raw.rstrip(b'\n'),self.raw+b'1 2 3 4 5 IMG_0001.JPG\n',self.raw.replace(b' ',b'  ',1)):
            with self.subTest(raw=raw),self.assertRaises(ValueError):validate_gcp_semantics(raw,processing=self.processing,correspondences=self.rows)
        dup=self.rows+self.rows
        with self.assertRaises(ValueError):validate_gcp_semantics(self.raw+self.raw.split(b'\n',1)[1],processing=self.processing,correspondences=dup)
        rows=copy.deepcopy(self.rows);rows[0]['pixelX']=float('nan')
        with self.assertRaises(ValueError):validate_gcp_semantics(self.raw,processing=self.processing,correspondences=rows)
        processing=self.processing+[dict(self.processing[0],datasetFileId='image-2',relativePath='other/img_0001.jpg')]
        with self.assertRaises(ValueError):validate_gcp_semantics(self.raw,processing=processing,correspondences=self.rows)

    def test_actual_pinned_viewer_generator(self):
        source=os.environ.get('VIEWER_GCP_ODM_JS')
        if not source:self.skipTest('pinned Viewer gcpOdm.js required')
        path=Path(source).resolve(strict=True); data=path.read_bytes().replace(b'\r\n',b'\n')
        self.assertEqual(hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest(),'81dc6b7bd20cc3505cd3a33e8222ba5acb9e845c')
        rows=copy.deepcopy(self.rows); rows[0].update(longitude=-88.100200004,latitude=44.500100005,elevationM=243.840000006,pixelX=120.500000005)
        viewer=[dict(longitude=r['longitude'],latitude=r['latitude'],elevationM=r['elevationM'],pixelX=r['pixelX'],pixelY=r['pixelY'],relativePath=self.processing[0]['relativePath']) for r in rows]
        script="const f=require(process.argv[1]).odmGcpList;let s='';process.stdin.on('data',x=>s+=x);process.stdin.on('end',()=>process.stdout.write(f(JSON.parse(s))))"
        result=subprocess.run(['node','-e',script,str(path)],input=json.dumps(viewer),text=True,encoding='utf8',capture_output=True,check=True)
        validate_gcp_semantics(result.stdout.encode(),processing=self.processing,correspondences=rows)

    def test_viewer_rounding_boundaries_order_and_ambiguous_names(self):
        source=os.environ.get('VIEWER_GCP_ODM_JS')
        if not source:self.skipTest('pinned Viewer gcpOdm.js required')
        path=Path(source).resolve(strict=True); data=path.read_bytes().replace(b'\r\n',b'\n')
        self.assertEqual(hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest(),'81dc6b7bd20cc3505cd3a33e8222ba5acb9e845c')
        numbers=[-1.234567895,-1.234567885,-0.0,-0.000000001,0.00000001]
        rows=[dict(self.rows[0],pointId=f'point-{i}',longitude=n,pixelX=i+1) for i,n in enumerate(numbers)]
        viewer=[dict(longitude=r['longitude'],latitude=r['latitude'],elevationM=r['elevationM'],pixelX=r['pixelX'],pixelY=r['pixelY'],relativePath=self.processing[0]['relativePath']) for r in rows]
        script="const f=require(process.argv[1]).odmGcpList;let s='';process.stdin.on('data',x=>s+=x);process.stdin.on('end',()=>process.stdout.write(f(JSON.parse(s))))"
        def generate(values):
            return subprocess.run(['node','-e',script,str(path)],input=json.dumps(values),text=True,encoding='utf8',capture_output=True,check=True).stdout.encode()
        raw=generate(viewer)
        validate_gcp_semantics(raw,processing=self.processing,correspondences=rows)
        self.assertIn(b'\n-0 ',raw)
        self.assertIn(b'\n0.00000001 ',raw)
        with self.assertRaises(ValueError):validate_gcp_semantics(raw,processing=self.processing,correspondences=list(reversed(rows)))
        # Viewer generator emits duplicate-basename lines; our separate v1
        # materialization boundary must reject their ambiguous image identity.
        viewer[1]['relativePath']='other/IMG_0001.JPG'
        dup_processing=self.processing+[dict(self.processing[0],datasetFileId='image-2',relativePath='other/IMG_0001.JPG')]
        dup_rows=copy.deepcopy(rows);dup_rows[1]['imageDatasetFileId']='image-2'
        with self.assertRaises(ValueError):validate_gcp_semantics(generate(viewer),processing=dup_processing,correspondences=dup_rows)


if __name__=='__main__':unittest.main()
