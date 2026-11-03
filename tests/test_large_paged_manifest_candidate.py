import copy
import unittest
from processing.large_paged_manifest_candidate import build,validate,PagedManifestError,_digest,MAX_OBJECTS,MAX_PAGE_OBJECTS

class LargePagedManifestTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  cls.objects=[dict(id=f'img-{i:05}',name=f'DJI_{i:05}.JPG',byteSize=9089354,sha256='a'*64,kind='image',mediaType='image/jpeg') for i in range(16832)]
  cls.closure,cls.pages=build(cls.objects);cls.pin=_digest(cls.closure)
 def test_large_closure_is_deterministic_and_indexed(self):
  result=validate(self.closure,self.pages,expected_closure_sha256=self.pin)
  self.assertEqual(len(result['byId']),16832);self.assertEqual(result['byId']['img-16831']['byteSize'],9089354)
  self.assertEqual(result['closureSha256'],self.pin);self.assertEqual(len(self.pages),33)
  again,other=build(self.objects);self.assertEqual((again,other),(self.closure,self.pages))
  self.assertIsNot(result['byId']['img-00000'],self.pages[0]['body']['objects'][0])
 def test_tamper_reorder_truncate_and_wrong_pin_hold(self):
  altered=copy.deepcopy(self.pages);altered[0]['body']['objects'][0]['byteSize']+=1
  cases=[(self.closure,altered,self.pin),(self.closure,list(reversed(self.pages)),self.pin),(self.closure,self.pages[:-1],self.pin),(self.closure,self.pages,'f'*64)]
  for closure,pages,pin in cases:
   with self.subTest(size=len(pages),pin=pin[:3]),self.assertRaises(PagedManifestError):validate(closure,pages,expected_closure_sha256=pin)
 def test_duplicate_and_oversize_hold(self):
  duplicate=[dict(self.objects[0]),dict(self.objects[0])]
  with self.assertRaises(PagedManifestError):build(duplicate)
  with self.assertRaises(PagedManifestError):build(self.objects+[self.objects[0]]*(MAX_OBJECTS-len(self.objects)+1))
  bad=copy.deepcopy(self.pages);bad[0]['body']['objects'][0]['name']='x'*1000000
  with self.assertRaises(PagedManifestError):validate(self.closure,bad,expected_closure_sha256=self.pin)
 def test_single_object_canonical_digest_vector(self):
  obj=dict(id='i',name='a.jpg',byteSize=1,sha256='a'*64,kind='image',mediaType='image/jpeg')
  closure,pages=build([obj])
  self.assertEqual(pages[0]['sha256'],'c0f512a2a64fc24849b4cee33c5239773bc665f7b1085c4c0b7f0b94d77a05c2')
  self.assertEqual(_digest(closure),'0987a3830b10ca7c53a9ddc72138fa05a70fa0df96b5353fcbb039fcba6fa7a8')
 def test_detached_pages_from_caller(self):
  values=[dict(self.objects[0])];closure,pages=build(values);values[0]['name']='changed.JPG'
  self.assertEqual(pages[0]['body']['objects'][0]['name'],'DJI_00000.JPG')

if __name__=='__main__':unittest.main()
