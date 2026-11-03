import copy
import hashlib
import io
import json
import os
import shutil
import subprocess
import unittest
from pathlib import Path
from processing import reference_protocol as p
from processing.large_paged_manifest_candidate import build,_digest
from processing.viewer_paged_frame_candidate import VERSION as SOURCE_VERSION,DOMAIN as SOURCE_DOMAIN,FrameHeld
from processing.viewer_selected_frames_candidate import VERSION,DOMAIN,verify,binding_digest

def frames(records,version,domain):
 lines=[];pins=[]
 for i in range(0,len(records),512):
  raw=p.canonical_json(dict(version=version,index=i//512,records=records[i:i+512]));lines.append(raw+b'\n');pins.append(hashlib.sha256(domain+raw).hexdigest())
 return b''.join(lines),pins

class SelectedFramesTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  node=shutil.which('node')
  if not node:
   if os.environ.get('PAGED_MANIFEST_PAIRED_REQUIRED')=='1':raise RuntimeError('Node required')
   raise unittest.SkipTest('Node unavailable')
  fixture=Path(__file__).resolve().parent/'fixtures'/'viewer-processingProvenance-5513f92.js'
  assert hashlib.sha256(fixture.read_bytes()).hexdigest()=='e965ab977c00e2058e9f5ea21a658fb89b2ad1af3c24facefb3a7b030e8eb82c'
  source=[dict(type='dataset_file',datasetFileId=f'file-{i:05}',relativePath=f'flight/IMG_{i:05}.jpg',byteSize=10,sha256='a'*64,processingRole='image',mediaType='image/jpeg',kind='image') for i in range(16832)]
  source[0]['relativePath']='flight/\u96ea/IMG_00000.jpg'
  source.extend([dict(type='dataset_file',datasetFileId='raw-gcp',relativePath='control/raw.csv',byteSize=5,sha256='d'*64,processingRole='gcp_source',mediaType='text/csv',kind='auxiliary'),dict(type='dataset_file',datasetFileId='old-gcp',relativePath='legacy/gcp_list.txt',byteSize=3,sha256='e'*64,processingRole='provider_input',mediaType='text/plain',kind='auxiliary'),dict(type='dataset_file',datasetFileId='lidar',relativePath='survey/forest.laz',byteSize=7,sha256='f'*64,processingRole='provider_input',mediaType='application/octet-stream',kind='lidar'),dict(type='dataset_file',datasetFileId='dotless',relativePath='flight/not-a-photo.\u0131pg',byteSize=2,sha256='9'*64,processingRole='auto',mediaType='image/jpeg',kind='image')])
  content='EPSG:4326\n';gcp_hash=hashlib.sha256(content.encode()).hexdigest();attempt='901a7098-d215-41aa-9c2a-530c81ecff04'
  source.append(dict(type='attempt_generated_gcp',attemptId=attempt,byteSize=len(content.encode()),sha256=gcp_hash,sourceFiles=[dict(datasetFileId='raw-gcp',relativePath='control/raw.csv',sha256='d'*64)],correspondenceSha256='c'*64))
  viewer_files=[{k:x[k] for k in ('relativePath','byteSize','sha256','processingRole')} for x in source if x['type']=='dataset_file']
  script=r"""const f=require(process.argv[1]).inputInventory;let s='';process.stdin.on('data',x=>s+=x);process.stdin.on('end',()=>{const x=JSON.parse(s);process.stdout.write(JSON.stringify(f(x.files,x.gcp)))})"""
  result=subprocess.run([node,'-e',script,str(fixture)],input=json.dumps(dict(files=viewer_files,gcp=dict(content=content,sha256=gcp_hash)),ensure_ascii=False),text=True,encoding='utf8',capture_output=True,check=True,timeout=30)
  viewer=json.loads(result.stdout);cls.input_hash=viewer['inputManifestSha256'];cls.source=source
  cls.source_wire,cls.source_pins=frames(source,SOURCE_VERSION,SOURCE_DOMAIN)
  chosen=source[:16832]+[source[-3],source[-1]]
  objects=[];cls.selection=[]
  for ordinal,item in enumerate(chosen):
   ident=f'object-{ordinal:05}';generated=item['type']=='attempt_generated_gcp'
   objects.append(dict(id=ident,name='gcp_list.txt' if generated else item['relativePath'].rsplit('/',1)[-1],byteSize=item['byteSize'],sha256=item['sha256'],kind='auxiliary' if generated else item['kind'],mediaType='text/plain' if generated else item['mediaType']))
   cls.selection.append(dict(type='attempt_generated_gcp',ordinal=ordinal,pageObjectId=ident,attemptId=attempt,mediaType='text/plain') if generated else dict(type='dataset_file',ordinal=ordinal,pageObjectId=ident,datasetFileId=item['datasetFileId']))
  cls.closure,cls.pages=build(objects);cls.selected_wire,cls.selected_pins=frames(cls.selection,VERSION,DOMAIN)
  assert len(viewer['inputFiles'])==len(cls.selection)
 def binding(self,source_hashes,source_count,selected_hashes,input_hash,closure_hash):
  return dict(datasetId='dataset-1',datasetManifestSha256='a'*64,principalId='viewer-1',environment='staging',authorityId='authority-1',viewerTaskId='56f7e8af-73c7-4a51-9903-c59cc68c033b',viewerAttemptId='901a7098-d215-41aa-9c2a-530c81ecff04',pipelineDigest='b'*64,revocationEpoch=1,sourcePageHashes=source_hashes,sourceCount=source_count,selectedPageHashes=selected_hashes,selectedCount=len(self.selection),inputManifestSha256=input_hash,processingClosureSha256=closure_hash)
 def evaluate(self,**overrides):
  args=dict(source_hashes=self.source_pins,source_count=len(self.source),selected_hashes=self.selected_pins,input_manifest_sha256=self.input_hash,processing_closure=self.closure,processing_pages=self.pages,closure_sha256=_digest(self.closure));args.update(overrides)
  binding=self.binding(args['source_hashes'],args['source_count'],args['selected_hashes'],args['input_manifest_sha256'],args['closure_sha256'])
  return verify(io.BytesIO(self.source_wire),io.BytesIO(self.selected_wire),**args,trusted_binding=binding,expected_binding_sha256=binding_digest(binding))
 def test_real_viewer_hash_order_and_lidar_remain_held(self):
  value=self.evaluate();self.assertEqual(value['state'],'held_authenticated_source_unavailable');self.assertEqual(value['selectedObjects'],16834)
 def test_reordered_omitted_and_wrong_manifest_hold(self):
  for mode in ('reordered','omitted','wrong_hash','wrong_generated_attempt'):
   selection=copy.deepcopy(self.selection);args={}
   if mode=='reordered':selection[511],selection[512]=selection[512],selection[511]
   if mode=='omitted':selection.pop()
   if mode=='wrong_generated_attempt':selection[-1]['attemptId']='5a410780-c3bc-46d1-903a-5713673a5399'
   wire,pins=frames(selection,VERSION,DOMAIN);args['selected_hashes']=pins
   if mode=='wrong_hash':args['input_manifest_sha256']='0'*64
   with self.subTest(mode=mode),self.assertRaises(FrameHeld):
    binding=self.binding(self.source_pins,len(self.source),args['selected_hashes'],args.get('input_manifest_sha256',self.input_hash),_digest(self.closure))
    verify(io.BytesIO(self.source_wire),io.BytesIO(wire),source_hashes=self.source_pins,source_count=len(self.source),selected_hashes=args['selected_hashes'],input_manifest_sha256=args.get('input_manifest_sha256',self.input_hash),processing_closure=self.closure,processing_pages=self.pages,closure_sha256=_digest(self.closure),trusted_binding=binding,expected_binding_sha256=binding_digest(binding))
 def test_independently_pinned_binding_rejects_authority_change(self):
  binding=self.binding(self.source_pins,len(self.source),self.selected_pins,self.input_hash,_digest(self.closure));pin=binding_digest(binding);binding['authorityId']='authority-2'
  with self.assertRaises(FrameHeld):verify(io.BytesIO(self.source_wire),io.BytesIO(self.selected_wire),source_hashes=self.source_pins,source_count=len(self.source),selected_hashes=self.selected_pins,input_manifest_sha256=self.input_hash,processing_closure=self.closure,processing_pages=self.pages,closure_sha256=_digest(self.closure),trusted_binding=binding,expected_binding_sha256=pin)
 def test_unknown_role_is_rejected(self):
  source=copy.deepcopy(self.source);source[0]['processingRole']='unknown'
  wire,pins=frames(source,SOURCE_VERSION,SOURCE_DOMAIN)
  binding=self.binding(pins,len(source),self.selected_pins,self.input_hash,_digest(self.closure))
  with self.assertRaises(FrameHeld):verify(io.BytesIO(wire),io.BytesIO(self.selected_wire),source_hashes=pins,source_count=len(source),selected_hashes=self.selected_pins,input_manifest_sha256=self.input_hash,processing_closure=self.closure,processing_pages=self.pages,closure_sha256=_digest(self.closure),trusted_binding=binding,expected_binding_sha256=binding_digest(binding))

if __name__=='__main__':unittest.main()
