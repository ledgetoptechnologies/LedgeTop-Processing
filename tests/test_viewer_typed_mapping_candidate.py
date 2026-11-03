import copy
import hashlib
import json
import os
import shutil
import subprocess
import unittest
from pathlib import Path
from processing.large_paged_manifest_candidate import build,_digest
from processing.viewer_typed_mapping_candidate import link,mapping_digest,_small_digest,TypedMappingHeld

class ViewerTypedMappingTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  node=shutil.which('node')
  if not node:
   if os.environ.get('PAGED_MANIFEST_PAIRED_REQUIRED')=='1':raise RuntimeError('Node required')
   raise unittest.SkipTest('Node unavailable')
  fixture=Path(__file__).resolve().parent/'fixtures'/'viewer-processingProvenance-5513f92.js'
  assert hashlib.sha256(fixture.read_bytes()).hexdigest()=='e965ab977c00e2058e9f5ea21a658fb89b2ad1af3c24facefb3a7b030e8eb82c'
  content='EPSG:4326\n';cls.gcp_bytes=content.encode();digest=hashlib.sha256(content.encode()).hexdigest();attempt='901a7098-d215-41aa-9c2a-530c81ecff04'
  files=[dict(relativePath='nested/flight/photo.jpg',byteSize=3,sha256='a'*64,processingRole='image'),dict(relativePath='control/raw.csv',byteSize=4,sha256='b'*64,processingRole='gcp_source'),dict(relativePath='legacy/gcp_list.txt',byteSize=5,sha256='c'*64,processingRole='provider_input')]
  script=r"""const f=require(process.argv[1]).inputInventory;let s='';process.stdin.on('data',x=>s+=x);process.stdin.on('end',()=>{const x=JSON.parse(s);process.stdout.write(JSON.stringify(f(x.files,x.gcp)))})"""
  run=subprocess.run([node,'-e',script,str(fixture)],input=json.dumps(dict(files=files,gcp=dict(content=content,sha256=digest))),text=True,encoding='utf8',capture_output=True,check=True,timeout=15)
  viewer=json.loads(run.stdout);cls.snapshot={k:viewer[k] for k in ('sourceFiles','inputFiles')}
  cls.dataset_mapping=[dict(datasetFileId=f'synthetic-file-{i}',**item) for i,item in enumerate(cls.snapshot['sourceFiles'])]
  cls.evidence=dict(attemptId=attempt,byteSize=len(content.encode()),sha256=digest,sourcePaths=['control/raw.csv'],correspondenceSha256='d'*64)
  evidence_hash=_small_digest(b'ltds-viewer-gcp-evidence-v1\0',cls.evidence)
  objects=[dict(id='photo-1',name='photo.jpg',byteSize=3,sha256='a'*64,kind='image',mediaType='image/jpeg'),dict(id='gcp-generated',name='gcp_list.txt',byteSize=len(content.encode()),sha256=digest,kind='auxiliary',mediaType='text/plain')]
  cls.closure,cls.pages=build(objects)
  cls.mapping=[dict(type='dataset_file',pageObjectId='photo-1',datasetFileId='synthetic-file-0',relativePath='nested/flight/photo.jpg',processingRole='image',mediaType='image/jpeg'),dict(type='attempt_generated_gcp',pageObjectId='gcp-generated',attemptId=attempt,gcpEvidenceSha256=evidence_hash,mediaType='text/plain')]
  cls.binding=dict(datasetId='dataset-1',datasetManifestSha256='e'*64,viewerTaskId='56f7e8af-73c7-4a51-9903-c59cc68c033b',viewerAttemptId=attempt,pipelineDigest='f'*64,principalId='viewer-1',environment='staging',authorityId='authority-1',revocationEpoch=1,sourceSnapshotSha256=_small_digest(b'ltds-viewer-typed-snapshot-v1\0',cls.snapshot),datasetMappingSha256=_small_digest(b'ltds-viewer-dataset-mapping-v1\0',cls.dataset_mapping),mappingSha256=mapping_digest(cls.mapping),gcpEvidenceSha256=evidence_hash,closureSha256=_digest(cls.closure))
 def evaluate(self,**overrides):
  args=dict(snapshot=self.snapshot,dataset_mapping=self.dataset_mapping,mapping=self.mapping,gcp_evidence=self.evidence,generated_gcp_bytes=self.gcp_bytes,trusted_binding=self.binding,expected_binding_sha256=_small_digest(b'ltds-viewer-typed-binding-v1\0',self.binding));args.update(overrides)
  return link(self.closure,self.pages,**args)
 def test_actual_viewer_selection_and_retained_raw_gcp(self):
  self.assertEqual(self.evaluate(),dict(state='held_authenticated_snapshot_unavailable',objects=2,retainedSources=2))
  self.assertEqual(self.snapshot['inputFiles'][-1]['relativePath'],'gcp_list.txt')
  self.assertTrue(any(x['relativePath']=='control/raw.csv' and x['processingRole']=='gcp_source' for x in self.snapshot['sourceFiles']))
 def test_wrong_type_order_path_attempt_or_evidence_holds(self):
  for scenario in ('type','order','path','attempt','evidence','bytes','dataset_id'):
   mapping=copy.deepcopy(self.mapping);evidence=copy.deepcopy(self.evidence);binding=dict(self.binding)
   if scenario=='type':mapping[1]=dict(type='dataset_file',pageObjectId='gcp-generated',datasetFileId='synthetic-file-2',relativePath='gcp_list.txt',processingRole='provider_input',mediaType='text/plain')
   if scenario=='order':mapping.reverse()
   if scenario=='dataset_id':mapping[0]['datasetFileId']='synthetic-file-1'
   if scenario=='path':mapping[0]['relativePath']='other/photo.jpg'
   if scenario=='attempt':mapping[1]['attemptId']='5a410780-c3bc-46d1-903a-5713673a5399'
   if scenario=='evidence':evidence['sourcePaths']=['nested/flight/photo.jpg'];binding['gcpEvidenceSha256']=_small_digest(b'ltds-viewer-gcp-evidence-v1\0',evidence);mapping[1]['gcpEvidenceSha256']=binding['gcpEvidenceSha256']
   binding['mappingSha256']=mapping_digest(mapping)
   supplied_bytes=b'bad' if scenario=='bytes' else self.gcp_bytes
   with self.subTest(scenario=scenario),self.assertRaises(TypedMappingHeld):self.evaluate(mapping=mapping,gcp_evidence=evidence,generated_gcp_bytes=supplied_bytes,trusted_binding=binding,expected_binding_sha256=_small_digest(b'ltds-viewer-typed-binding-v1\0',binding))
 def test_authoritative_mapping_missing_extra_or_mismatch_holds(self):
  for scenario in ('missing','extra','id','size','role'):
   dataset=copy.deepcopy(self.dataset_mapping);binding=dict(self.binding)
   if scenario=='missing':dataset.pop()
   elif scenario=='extra':dataset.append(dict(dataset[-1]))
   elif scenario=='id':dataset[0]['datasetFileId']='another-id'
   elif scenario=='size':dataset[0]['byteSize']=True
   else:dataset[0]['processingRole']='administrative'
   binding['datasetMappingSha256']=_small_digest(b'ltds-viewer-dataset-mapping-v1\0',dataset)
   with self.subTest(scenario=scenario),self.assertRaises(TypedMappingHeld):self.evaluate(dataset_mapping=dataset,trusted_binding=binding,expected_binding_sha256=_small_digest(b'ltds-viewer-typed-binding-v1\0',binding))
 def test_more_than_10000_sources_remains_held(self):
  snapshot=dict(sourceFiles=self.snapshot['sourceFiles']*3334,inputFiles=self.snapshot['inputFiles'])
  with self.assertRaises(TypedMappingHeld):self.evaluate(snapshot=snapshot)

if __name__=='__main__':unittest.main()
