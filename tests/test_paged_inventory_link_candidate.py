import copy
import unittest
from unittest.mock import patch
from processing.large_paged_manifest_candidate import build,_digest
from processing.paged_inventory_link_candidate import link,inventory_digest,mapping_digest,binding_digest,InventoryLinkHeld

class PagedInventoryLinkTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  cls.objects=[dict(id=f'img-{i:05}',name=f'DJI_{i:05}.JPG',byteSize=9089354,sha256='a'*64,kind='image',mediaType='image/jpeg') for i in range(16832)]
  cls.closure,cls.pages=build(cls.objects)
  cls.mapping=[dict(pageObjectId=f'img-{i:05}',datasetFileId=f'file-{i:05}') for i in range(16832)]
  cls.inventory=dict(protocol='ltds-source-inventory-v1',schemaVersion=1,dataset=dict(id='dataset-1',manifestSha256='b'*64),binding=dict(viewerTaskId='56f7e8af-73c7-4a51-9903-c59cc68c033b',viewerAttemptId='901a7098-d215-41aa-9c2a-530c81ecff04',pipelineDigest='c'*64),processing=[dict(datasetFileId=f'file-{i:05}',relativePath=f'clients/example/flight-{i%50:02}/raw/DJI_{i:05}.JPG',byteSize=9089354,sha256='a'*64,processingRole='image',contentType='image/jpeg') for i in range(16832)],retained=[dict(datasetFileId='gcp-source',relativePath='control/raw.csv',byteSize=4,sha256='d'*64,processingRole='gcp_source',contentType='text/csv')],generated=[])
  cls.binding=dict(datasetId='dataset-1',datasetManifestSha256='b'*64,inventorySha256=inventory_digest(cls.inventory),mappingSha256=mapping_digest(cls.mapping),closureSha256=_digest(cls.closure),principalId='viewer-1',environment='staging',authorityId='authority-1',viewerTaskId=cls.inventory['binding']['viewerTaskId'],viewerAttemptId=cls.inventory['binding']['viewerAttemptId'],pipelineDigest='c'*64,revocationEpoch=1)
 def test_large_exact_mapping_is_still_held(self):
  result=link(self.closure,self.pages,inventory=self.inventory,mapping=self.mapping,trusted_binding=self.binding,expected_link_sha256=binding_digest(self.binding))
  self.assertEqual((result['state'],result['objects']),('held_source_authority_unverified',16832))
  self.assertEqual(self.inventory['retained'][0]['relativePath'],'control/raw.csv')
 def test_wrong_binding_digest_attempt_or_dataset_holds(self):
  for key,value in [('inventorySha256','f'*64),('mappingSha256','f'*64),('closureSha256','f'*64),('viewerAttemptId','5a410780-c3bc-46d1-903a-5713673a5399'),('datasetId','dataset-2')]:
   binding=dict(self.binding);binding[key]=value
   with self.subTest(key=key),self.assertRaises(InventoryLinkHeld):link(self.closure,self.pages,inventory=self.inventory,mapping=self.mapping,trusted_binding=binding,expected_link_sha256=binding_digest(binding))
 def test_preflight_rejects_bool_size_and_large_fields_before_hashing(self):
  inv=copy.deepcopy(self.inventory);inv['processing'][0]['byteSize']=True
  self.assertTrue(inv['processing'][0]['byteSize']==1)  # The source value must not equal a one-byte integer by coercion.
  with patch('rfc8785.dumps',side_effect=AssertionError('whole inventory serialized')):
   with self.assertRaises(InventoryLinkHeld):inventory_digest(inv)
   with self.assertRaises(InventoryLinkHeld):link(self.closure,self.pages,inventory=inv,mapping=self.mapping,trusted_binding=self.binding,expected_link_sha256=binding_digest(self.binding))
  inv=copy.deepcopy(self.inventory);inv['processing'][0]['relativePath']='x'*1000000
  with patch('rfc8785.dumps',side_effect=AssertionError('whole inventory serialized')):
   with self.assertRaises(InventoryLinkHeld):inventory_digest(inv)
  with patch('processing.paged_inventory_link_candidate.p.canonical_json',side_effect=AssertionError('mapping serialized')):
   with self.assertRaises(InventoryLinkHeld):mapping_digest([dict(pageObjectId='x'*1000000,datasetFileId='id')])
 def test_independent_binding_pin_covers_caller_and_epoch(self):
  for key,value in [('principalId','viewer-2'),('authorityId','authority-2'),('environment','production'),('revocationEpoch',2)]:
   altered=dict(self.binding);altered[key]=value
   with self.subTest(key=key),self.assertRaises(InventoryLinkHeld):link(self.closure,self.pages,inventory=self.inventory,mapping=self.mapping,trusted_binding=altered,expected_link_sha256=binding_digest(self.binding))
 def test_swapped_missing_and_additional_mapping_hold_even_with_matching_mapping_pin(self):
  for how in ('swapped','missing','extra','boundary'):
   changed=[dict(x) for x in self.mapping]
   if how=='swapped':changed[0]['datasetFileId'],changed[1]['datasetFileId']=changed[1]['datasetFileId'],changed[0]['datasetFileId']
   elif how=='missing':changed.pop()
   elif how=='extra':changed.append(dict(changed[-1]))
   else:changed[511],changed[512]=changed[512],changed[511]
   binding=dict(self.binding,mappingSha256=mapping_digest(changed))
   with self.subTest(how=how),self.assertRaises(InventoryLinkHeld):link(self.closure,self.pages,inventory=self.inventory,mapping=changed,trusted_binding=binding,expected_link_sha256=binding_digest(binding))
 def test_inventory_content_or_generated_gcp_change_holds(self):
  for how in ('path','bytes','generated'):
   inv=copy.deepcopy(self.inventory)
   if how=='path':inv['processing'][512]['relativePath']='other/DIFFERENT.JPG'
   elif how=='bytes':inv['processing'][0]['byteSize']+=1
   else:inv['generated']=[{'snapshotId':self.binding['viewerAttemptId']}]
   binding=dict(self.binding,inventorySha256=inventory_digest(inv)) if how!='generated' else dict(self.binding)
   with self.subTest(how=how),self.assertRaises(InventoryLinkHeld):link(self.closure,self.pages,inventory=inv,mapping=self.mapping,trusted_binding=binding,expected_link_sha256=binding_digest(binding))

if __name__=='__main__':unittest.main()
