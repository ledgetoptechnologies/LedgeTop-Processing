import copy,json,unittest
from pathlib import Path
from processing import native_input_manifest as native,reference_protocol as ref

VECTORS=json.loads((Path(__file__).parent/'fixtures/native_input_manifest_vectors.json').read_text(encoding='utf-8'))['vectors']
REJECTIONS=json.loads((Path(__file__).parent/'fixtures/native_input_manifest_rejections.json').read_text(encoding='utf-8'))

class NativeInputManifestTests(unittest.TestCase):
    def check(self,value,limit=native.MAX_INPUT_BYTES):
        value=copy.deepcopy(value);value['sha256']=ref.manifest_sha256(value)
        return native.validate(ref.canonical_json(value),expected_manifest_id='attempt-0001',max_input_bytes=limit)
    def test_shared_canonical_vectors(self):
        for vector in VECTORS:
            body=vector['canonical'].encode();value=native.validate(body,expected_manifest_id=vector['expectedManifestId'],max_input_bytes=native.MAX_INPUT_BYTES)
            self.assertEqual(value['sha256'],vector['digest']);self.assertEqual(ref.manifest_sha256(value),vector['digest'])
    def test_shared_rejection_vectors(self):
        self.assertEqual(REJECTIONS['schema'],native.PROFILE)
        for vector in REJECTIONS['cases']:
            self.assertEqual(vector['expected'],'reject')
            with self.subTest(name=vector['name']),self.assertRaises(ref.ProtocolError):
                native.validate(bytes.fromhex(vector['wireHex']) if 'wireHex' in vector else vector['body'].encode(),expected_manifest_id='attempt-0001',max_input_bytes=native.MAX_INPUT_BYTES)
    def test_rejects_noncanonical_duplicate_and_bad_utf8(self):
        body=VECTORS[0]['canonical'].encode()
        for bad in (body+b'\n',body.replace(b'"revision":1',b'"revision":1.0'),body.replace(b'"revision":1',b'"revision":1e0'),body.replace(b'{',b'{"id":"other",',1),b'\xff'):
            with self.subTest(bad=bad[:50]),self.assertRaises(ref.ProtocolError):native.validate(bad,expected_manifest_id='attempt-0001',max_input_bytes=100)
    def test_identity_digest_counts_and_filename_fail_closed(self):
        original=json.loads(VECTORS[1]['canonical'])
        for mutation in ('wrong_id','revision','ordinal','duplicate','zero','reserved','unsupported','kind','total','lidar','reverse'):
            value=copy.deepcopy(original)
            if mutation=='wrong_id':value['id']='other'
            elif mutation=='revision':value['revision']=2
            elif mutation=='ordinal':value['objects'][1]['id']='o000003'
            elif mutation=='duplicate':value['objects'][1]['name']='IMAGE-01.JPG'
            elif mutation=='zero':value['objects'][1]['byteSize']=0
            elif mutation=='reserved':value['objects'][0]['name']='CON.jpg'
            elif mutation=='unsupported':value['objects'][0]['name']='image.raw'
            elif mutation=='kind':value['objects'][1]['kind']='image'
            elif mutation=='total':value['totalBytes']+=1
            elif mutation=='lidar':value['inputCounts']['lidarObjects']=1
            else:value['objects'].reverse()
            with self.subTest(mutation=mutation),self.assertRaises(ref.ProtocolError):self.check(value)
        value=copy.deepcopy(original);value['sha256']='0'*64
        with self.assertRaises(ref.ProtocolError):native.validate(ref.canonical_json(value),expected_manifest_id='attempt-0001',max_input_bytes=100)
    def test_20_gib_is_metadata_ceiling_not_allocation(self):
        value=json.loads(VECTORS[0]['canonical']);value['objects'][0]['byteSize']=native.MAX_INPUT_BYTES;value['totalBytes']=native.MAX_INPUT_BYTES
        self.assertEqual(self.check(value)['totalBytes'],native.MAX_INPUT_BYTES)
        value['objects'][0]['byteSize']+=1;value['totalBytes']+=1
        with self.assertRaises(ref.ProtocolError):self.check(value)
        with self.assertRaises(ref.ProtocolError):self.check(value,limit=native.MAX_INPUT_BYTES+1)
    def test_ten_thousand_photos_plus_gcp_exceeds_existing_count(self):
        value=json.loads(VECTORS[0]['canonical']);template=value['objects'][0]
        generated=REJECTIONS['generated'][0];self.assertEqual((generated['photos'],generated['gcp'],generated['expected']),(10000,True,'reject'))
        objects=[dict(template,id=f'o{i+1:06d}',name=f'photo{i:05d}.jpg',byteSize=1) for i in range(generated['photos'])]
        value.update(objects=objects,objectCount=len(objects),totalBytes=len(objects),inputCounts={'images':10000,'lidarObjects':0,'points':0})
        self.assertEqual(self.check(value)['objectCount'],10000)
        value['objects'].append({'id':'o010001','name':'gcp_list.txt','byteSize':1,'sha256':'a'*64,'kind':'auxiliary','mediaType':'text/plain'});value['objectCount']+=1;value['totalBytes']+=1
        with self.assertRaises(ref.ProtocolError):self.check(value)

if __name__=='__main__':unittest.main()
