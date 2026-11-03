"""Synthetic pinned Viewer exporter / Processing frame byte conformance."""
import base64,hashlib,io,json,os,subprocess,unittest
from pathlib import Path
from processing import reference_protocol as p
from processing.viewer_paged_frame_candidate import DOMAIN as SOURCE_DOMAIN,verify_dataset_count_v2,FrameHeld
from processing.viewer_selected_frames_candidate import DOMAIN as SELECTED_DOMAIN,verify_dataset_count_v2 as verify_selection
import tests.test_gcp_synthetic_atomic_candidate as helpers

ROOT=Path(__file__).resolve().parents[1];PIN='4ed7dcd566380c3c0cd7f03a765ee1badf2004d5'
class FrameVectors(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  viewer=os.environ.get('PINNED_VIEWER_ROOT')
  if not viewer:raise RuntimeError('PINNED_VIEWER_ROOT required')
  cls.viewer=Path(viewer).resolve(strict=True)
  if subprocess.check_output(['git','rev-parse','HEAD'],cwd=cls.viewer,text=True).strip()!=PIN or subprocess.check_output(['git','status','--porcelain'],cwd=cls.viewer,text=True).strip():raise RuntimeError('clean pinned Viewer required')
  out=subprocess.run(['node',str(ROOT/'tests/support/viewer_frame_vector_export.cjs'),'vector'],env=dict(os.environ,PINNED_VIEWER_ROOT=str(cls.viewer)),capture_output=True,text=True,encoding='utf8',check=True,timeout=30)
  cls.value=json.loads(out.stdout)
 def test_actual_pages_bytes_domains_and_selected_mapping(self):
  x=self.value;src=base64.b64decode(x['sourceFrames']);sel=base64.b64decode(x['selectedFrames'])
  for wire,pins,domain in ((src,x['sourcePageHashes'],SOURCE_DOMAIN),(sel,x['selectedPageHashes'],SELECTED_DOMAIN)):
   lines=wire.splitlines(keepends=True);self.assertGreaterEqual(len(lines),2);self.assertEqual(len(lines),len(pins))
   self.assertEqual(len(wire),sum(len(line[:-1])+1 for line in lines))
   for i,(line,pin) in enumerate(zip(lines,pins)):
    self.assertEqual(line[-1:],b'\n');raw=line[:-1];value=json.loads(raw);self.assertEqual(value['index'],i);self.assertEqual(p.canonical_json(value),raw);self.assertEqual(hashlib.sha256(domain+raw).hexdigest(),pin);self.assertNotEqual(hashlib.sha256(domain+line).hexdigest(),pin)
  self.assertTrue('flíght'.encode() in src);self.assertTrue(b'quo\\"te' in src)
  self.assertEqual(verify_dataset_count_v2(io.BytesIO(src),expected_page_hashes=x['sourcePageHashes'],expected_dataset_items=x['sourceCount'])['generatedGcp'],1)
  helpers.AtomicGcp.x=x;bundle=helpers.AtomicGcp('runTest').bundle();b=bundle['trusted_binding']
  out=verify_selection(io.BytesIO(src),io.BytesIO(sel),source_hashes=b['sourcePageHashes'],source_count=b['sourceCount'],selected_hashes=b['selectedPageHashes'],input_manifest_sha256=b['inputManifestSha256'],processing_closure=bundle['processing_closure'],processing_pages=bundle['processing_pages'],closure_sha256=b['processingClosureSha256'],trusted_binding=b,expected_binding_sha256=bundle['expected_binding_sha256'])
  self.assertEqual(out['selectedObjects'],x['selectedCount']);self.assertEqual(out['inputManifestSha256'],x['inputManifestSha256'])
  with self.assertRaises(FrameHeld):verify_dataset_count_v2(io.BytesIO(b''.join(reversed(src.splitlines(keepends=True)))),expected_page_hashes=x['sourcePageHashes'],expected_dataset_items=x['sourceCount'])
  with self.assertRaises(FrameHeld):verify_selection(io.BytesIO(src),io.BytesIO(b''.join(reversed(sel.splitlines(keepends=True)))),source_hashes=b['sourcePageHashes'],source_count=b['sourceCount'],selected_hashes=b['selectedPageHashes'],input_manifest_sha256=b['inputManifestSha256'],processing_closure=bundle['processing_closure'],processing_pages=bundle['processing_pages'],closure_sha256=b['processingClosureSha256'],trusted_binding=b,expected_binding_sha256=bundle['expected_binding_sha256'])
 def test_fixed_cross_language_vector(self):
  js="const p=require(process.argv[1]);let source=[{type:'dataset_file',datasetFileId:'photo',relativePath:'café/quo\\\"te/a.jpg',byteSize:1,sha256:'a'.repeat(64),processingRole:'image',mediaType:'image/jpeg',kind:'image'}],selected=[{type:'dataset_file',ordinal:0,pageObjectId:'photo',datasetFileId:'photo'}];let a=p.frameStream(source,p.SOURCE_VERSION,p.SOURCE_DOMAIN),b=p.frameStream(selected,p.SELECTED_VERSION,p.SELECTED_DOMAIN);process.stdout.write(JSON.stringify({source:a.bytes.toString('base64'),sourceHash:a.pageHashes[0],sourceTotal:a.totalBytes,selected:b.bytes.toString('base64'),selectedHash:b.pageHashes[0],selectedTotal:b.totalBytes}))"
  module=str(self.viewer/'apps/viewer/server/viewerPagedSourceExport.js');out=json.loads(subprocess.check_output(['node','-e',js,module],text=True))
  for label,domain in (('source',SOURCE_DOMAIN),('selected',SELECTED_DOMAIN)):
   wire=base64.b64decode(out[label]);raw=wire[:-1];self.assertEqual(p.canonical_json(json.loads(raw)),raw);self.assertEqual(hashlib.sha256(domain+raw).hexdigest(),out[label+'Hash']);self.assertEqual(len(wire),out[label+'Total'])
  self.assertEqual((out['sourceHash'],out['sourceTotal']),('f3a459d1bf8e25489142527e0ff25b631a633b1ae3085814a1bed4f509e71223',304))
  self.assertEqual((out['selectedHash'],out['selectedTotal']),('d2251fde0bb9f0d711228a99bf0ba515f03984f7780c869597e66f3ff524984b',150))
  self.assertIn('café'.encode(),base64.b64decode(out['source']));self.assertIn(b'quo\\"te',base64.b64decode(out['source']))
  print('synthetic_fixed_frame_vectors',json.dumps({k:v for k,v in out.items() if k.endswith(('Hash','Total'))},sort_keys=True))
