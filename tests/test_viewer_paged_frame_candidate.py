import copy
import hashlib
import io
import os
import shutil
import subprocess
from pathlib import Path
import unittest
from unittest.mock import patch
from processing import reference_protocol as p
from processing.viewer_paged_frame_candidate import verify,FrameHeld,VERSION,DOMAIN,MAX_FRAME_BYTES

class ViewerPagedFrameTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  cls.records=[dict(type='dataset_file',datasetFileId=f'file-{i:05}',relativePath=f'surveys/site/flight-{i%50:02}/raw/IMG_{i:05}.jpg',byteSize=9089354,sha256='a'*64,processingRole='image',mediaType='image/jpeg') for i in range(16832)]
  cls.records.append(dict(type='dataset_file',datasetFileId='raw-gcp-file',relativePath='control/raw.csv',byteSize=5,sha256='d'*64,processingRole='gcp_source',mediaType='text/csv'))
  cls.records.append(dict(type='attempt_generated_gcp',attemptId='901a7098-d215-41aa-9c2a-530c81ecff04',byteSize=10,sha256='b'*64,sourceFiles=[dict(datasetFileId='raw-gcp-file',relativePath='control/raw.csv',sha256='d'*64)],correspondenceSha256='c'*64))
  cls.lines=[];cls.hashes=[]
  for i in range(0,len(cls.records),512):
   raw=p.canonical_json(dict(version=VERSION,index=i//512,records=cls.records[i:i+512]));cls.lines.append(raw+b'\n');cls.hashes.append(hashlib.sha256(DOMAIN+raw).hexdigest())
 def test_large_paged_stream_is_bounded_but_untrusted(self):
  result=verify(io.BytesIO(b''.join(self.lines)),expected_page_hashes=self.hashes,expected_items=len(self.records))
  self.assertEqual((result['state'],result['objects'],result['pages'],result['generatedGcp']),('held_no_viewer_authority_or_mapping',16834,33,1))
 def test_truncated_reordered_extra_and_oversized_frames_hold(self):
  data=b''.join(self.lines)
  cases=(data[:-1],b''.join(reversed(self.lines)),data+b'{}\n',b'x'*(MAX_FRAME_BYTES+1)+b'\n')
  for case in cases:
   with self.subTest(size=len(case)),self.assertRaises(FrameHeld):verify(io.BytesIO(case),expected_page_hashes=self.hashes,expected_items=len(self.records))
 def test_duplicate_identity_even_with_recomputed_frame_pin_holds(self):
  frames=list(self.lines);value=__import__('json').loads(frames[1]);value['records'][0]['datasetFileId']=self.records[0]['datasetFileId']
  raw=p.canonical_json(value);frames[1]=raw+b'\n';pins=list(self.hashes);pins[1]=hashlib.sha256(DOMAIN+raw).hexdigest()
  with self.assertRaises(FrameHeld):verify(io.BytesIO(b''.join(frames)),expected_page_hashes=pins,expected_items=len(self.records))
 def test_generated_gcp_raw_source_reference_mismatch_holds(self):
  frames=list(self.lines);value=__import__('json').loads(frames[-1]);value['records'][-1]['sourceFiles'][0]['sha256']='f'*64
  raw=p.canonical_json(value);frames[-1]=raw+b'\n';pins=list(self.hashes);pins[-1]=hashlib.sha256(DOMAIN+raw).hexdigest()
  with self.assertRaises(FrameHeld):verify(io.BytesIO(b''.join(frames)),expected_page_hashes=pins,expected_items=len(self.records))
 def test_pinned_viewer_canonical_bytes_for_unicode_frame(self):
  node=shutil.which('node')
  if not node:
   if os.environ.get('PAGED_MANIFEST_PAIRED_REQUIRED')=='1':self.fail('Node required in paired mode')
   self.skipTest('Node unavailable')
  fixture=Path(__file__).resolve().parent/'fixtures'/'viewer-referenceProtocol-c92deb5.js'
  self.assertEqual(hashlib.sha256(fixture.read_bytes()).hexdigest(),'030e820b876f4ca6500832a1cfe8b05ea8e7394fed7f3df2be4b070589ccfe20')
  item=dict(self.records[0],relativePath='surveys/\u96ea/flight-01/IMG_00000.jpg')
  frame=dict(version=VERSION,index=0,records=[item]);raw=p.canonical_json(frame);pin=hashlib.sha256(DOMAIN+raw).hexdigest()
  script=r"""const canon=require(process.argv[1]).canonicalBytes,crypto=require('node:crypto');let s='';process.stdin.setEncoding('utf8');process.stdin.on('data',x=>s+=x);process.stdin.on('end',()=>process.stdout.write(crypto.createHash('sha256').update('ltds-viewer-source-frame-v1\0').update(canon(JSON.parse(s))).digest('hex')))"""
  output=subprocess.run([node,'-e',script,str(fixture)],input=__import__('json').dumps(frame,ensure_ascii=False),encoding='utf8',text=True,capture_output=True,check=True,timeout=15).stdout
  self.assertEqual(output,pin);self.assertEqual(verify(io.BytesIO(raw+b'\n'),expected_page_hashes=[pin],expected_items=1)['objects'],1)
 def test_long_paths_do_not_imply_every_512_record_page_fits(self):
  rows=[dict(self.records[i],relativePath=('x'*220)+'/'+('y'*220)+'/'+f'photo-{i:04}.jpg') for i in range(512)]
  raw=p.canonical_json(dict(version=VERSION,index=0,records=rows));self.assertGreater(len(raw),MAX_FRAME_BYTES)
  with patch('processing.viewer_paged_frame_candidate.json.loads',side_effect=AssertionError('oversize frame decoded')):
   with self.assertRaises(FrameHeld):verify(io.BytesIO(raw+b'\n'),expected_page_hashes=[hashlib.sha256(DOMAIN+raw).hexdigest()],expected_items=512)
 def test_boolean_size_and_duplicate_json_keys_hold(self):
  value=copy.deepcopy(self.records[0]);value['byteSize']=True
  raw=p.canonical_json(dict(version=VERSION,index=0,records=[value]));pin=hashlib.sha256(DOMAIN+raw).hexdigest()
  with self.assertRaises(FrameHeld):verify(io.BytesIO(raw+b'\n'),expected_page_hashes=[pin],expected_items=1)
  raw=b'{"index":0,"index":1,"records":[],"version":"ltds-viewer-source-frames-v1"}';pin=hashlib.sha256(DOMAIN+raw).hexdigest()
  with self.assertRaises(FrameHeld):verify(io.BytesIO(raw+b'\n'),expected_page_hashes=[pin],expected_items=1)

if __name__=='__main__':unittest.main()
