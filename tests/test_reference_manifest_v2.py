import copy, hashlib, json, unittest
from pathlib import Path
from processing import reference_protocol as p
from processing.reference_manifest_v2 import DOMAIN, validate_manifest_v2, decode_manifest_v2

class ManifestTests(unittest.TestCase):
    def setUp(self):
        fixture=json.loads((Path(__file__).resolve().parents[1]/'contracts/input-v2-manifest-fixture.json').read_text(encoding='utf-8'))
        self.manifest=fixture['manifest']; self.expected=fixture['expected']
    def reseal(self,m):
        m['sha256']=hashlib.sha256(DOMAIN+p.canonical_json({k:v for k,v in m.items() if k!='sha256'})).hexdigest()
        return m
    def test_shared_complete_vector(self):
        self.assertEqual(validate_manifest_v2(self.manifest),self.expected)
        canonical=(Path(__file__).resolve().parents[1]/'contracts/input-v2-manifest-canonical.json').read_bytes()
        self.assertEqual(decode_manifest_v2(canonical),self.expected)
        with self.assertRaises(p.ProtocolError): decode_manifest_v2(canonical+b' ')
    def test_each_field_bound(self):
        for key,value in [('id','other'),('revision',2),('inputCounts',{'images':1,'lidarObjects':1,'points':43})]:
            m=copy.deepcopy(self.manifest);m[key]=value
            with self.subTest(key=key),self.assertRaises(p.ProtocolError): validate_manifest_v2(m)
        for key,value in [('id','changed'),('name','changed.jpg'),('sha256','d'*64),('mediaType','image/png')]:
            m=copy.deepcopy(self.manifest);m['objects'][0][key]=value
            with self.subTest(key=key),self.assertRaises(p.ProtocolError): validate_manifest_v2(m)
    def test_invalid_even_if_resealed(self):
        changes=[]
        for obj,key,value in [(1,'name','EMPTY.JPG'),(1,'id','empty'),(0,'mediaType','image/heic'),(0,'kind',['image']),(0,'name','../unsafe'),(0,'byteSize',p.MAX_SAFE_INTEGER)]:
            m=copy.deepcopy(self.manifest);m['objects'][obj][key]=value;changes.append(m)
        m=copy.deepcopy(self.manifest);m['totalBytes']+=1;changes.append(m)
        m=copy.deepcopy(self.manifest);m['inputCounts']['images']+=1;changes.append(m)
        m=copy.deepcopy(self.manifest);m['objects'][1]['kind']='auxiliary';m['objects'][1]['mediaType']='text/plain';m['inputCounts']['lidarObjects']=0;changes.append(m)
        for m in changes:
            with self.assertRaises(p.ProtocolError): validate_manifest_v2(self.reseal(m))
    def test_large_object_count(self):
        m=copy.deepcopy(self.manifest);o=m['objects'][0]
        m['objects']=[dict(o,id=f'id-{i}',name=f'photo-{i}.jpg',byteSize=1) for i in range(16832)]
        m['objectCount']=m['totalBytes']=m['inputCounts']['images']=16832
        m['inputCounts']['lidarObjects']=m['inputCounts']['points']=0
        self.assertEqual(validate_manifest_v2(self.reseal(m))['objectCount'],16832)

if __name__=='__main__': unittest.main()
