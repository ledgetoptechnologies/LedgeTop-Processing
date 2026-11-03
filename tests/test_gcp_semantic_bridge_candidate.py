import base64,copy,hashlib,json,unittest
from tests.test_viewer_gcp_pair_candidate import ViewerPair
from processing.gcp_semantic_bridge_candidate import inspect,SemanticHeld

class ActualViewerSemantics(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  ViewerPair.setUpClass(); d=ViewerPair.data; real=d['real']; raw=base64.b64decode(real['decoded'])
  cls.args=dict(attempt_id=real['attempt'],evidence=json.loads(raw),evidence_bytes=raw,evidence_sha256=real['digest'],generated_bytes=base64.b64decode(d['generated']),source_records=json.loads(base64.b64decode(d['sources']))['records'],selected_records=json.loads(base64.b64decode(d['selected']))['records'])
 def test_actual_viewer_rows_and_exact_stored_file(self):
  self.assertEqual(inspect(**self.args)['state'],'held_gcp_authority_correspondence_hash_unverified')
  self.assertEqual(self.args['source_records'][-1]['correspondenceSha256'],ViewerPair.data['jsCorrespondenceSha'])
 def test_modified_bytes_and_unmatched_evidence_rejected(self):
  for key in ('evidence_bytes','generated_bytes'):
   v=copy.deepcopy(self.args);v[key]+=b'x'
   with self.subTest(key=key),self.assertRaises(SemanticHeld):inspect(**v)
  v=copy.deepcopy(self.args);v['evidence']['correspondences'][0]['pixelX']+=1
  with self.assertRaises(SemanticHeld):inspect(**v)
 def test_bad_csv_image_and_selected_binding_rejected(self):
  for kind in ('csv','image','selection','attempt','ambiguity'):
   v=copy.deepcopy(self.args)
   if kind=='csv':v['source_records'][0]['sha256']='d'*64
   elif kind=='image':v['source_records'][1]['relativePath']='other.jpg'
   elif kind=='selection':v['selected_records'][0]['datasetFileId']='lidar'
   elif kind=='attempt':v['attempt_id']='00000000-0000-4000-8000-000000000000'
   else:
    a=copy.deepcopy(v['source_records'][1]);a['datasetFileId']='photo-2';a['relativePath']='other/IMG_00001.jpg';v['source_records'].append(a);v['selected_records'].insert(1,dict(type='dataset_file',ordinal=1,pageObjectId='photo-2',datasetFileId='photo-2'))
    for i,x in enumerate(v['selected_records']):x['ordinal']=i
   with self.subTest(kind=kind),self.assertRaises(SemanticHeld):inspect(**v)

class Scale(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  ActualViewerSemantics.setUpClass();cls.args=ActualViewerSemantics.args
 def test_16832_selected_objects(self):
  v=copy.deepcopy(self.args); extra=[]
  for i in range(16831):
   sid=f'extra-{i}';extra.append(dict(type='dataset_file',datasetFileId=sid,relativePath=f'extra/{sid}.jpg',byteSize=1,sha256='e'*64,processingRole='image',mediaType='image/jpeg',kind='image'))
  v['source_records'].extend(extra); gcp=v['selected_records'].pop()
  for obj in extra:v['selected_records'].append(dict(type='dataset_file',ordinal=len(v['selected_records']),pageObjectId=obj['datasetFileId'],datasetFileId=obj['datasetFileId']))
  gcp['ordinal']=len(v['selected_records']);v['selected_records'].append(gcp)
  self.assertEqual(inspect(**v)['state'],'held_gcp_authority_correspondence_hash_unverified')
