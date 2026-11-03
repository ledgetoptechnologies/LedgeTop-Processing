"""Actual pinned Viewer exporter to held Processing receiver, synthetic data only."""
import base64
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import unittest

from processing.large_paged_manifest_candidate import build, _digest
from processing.paged_frame_receiver_candidate import initialize, stage, ReceiverHeld
from processing.viewer_selected_frames_candidate import binding_digest
from processing.viewer_paged_frame_candidate import verify as verify_source, FrameHeld

ROOT = Path(__file__).resolve().parents[1]
PIN = 'a6ed23b38ea66171fc8884867840b2d699062926'

class PagedViewerPair(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  root=os.environ.get('PINNED_VIEWER_ROOT')
  if not root:
   if os.environ.get('PAGED_VIEWER_PAIR_REQUIRED')=='1':raise RuntimeError('PINNED_VIEWER_ROOT required')
   raise unittest.SkipTest('PINNED_VIEWER_ROOT required')
  viewer=Path(root).resolve(strict=True)
  head=subprocess.run(['git','rev-parse','HEAD'],cwd=viewer,capture_output=True,text=True,check=True).stdout.strip()
  status=subprocess.run(['git','status','--porcelain','--untracked-files=all'],cwd=viewer,capture_output=True,text=True,check=True).stdout
  if head!=PIN or status:raise RuntimeError('clean pinned Viewer checkout required')
  out=subprocess.run(['node',str(ROOT/'tests/support/viewer_paged_pair.cjs')],cwd=ROOT,env=dict(os.environ,PINNED_VIEWER_ROOT=str(viewer)),capture_output=True,text=True,encoding='utf8',check=True,timeout=120)
  cls.export=json.loads(out.stdout)
  cls.source=base64.b64decode(cls.export['sourceFrames'],validate=True);cls.selected=base64.b64decode(cls.export['selectedFrames'],validate=True)
  records=[r for line in cls.source.splitlines() for r in json.loads(line)['records']]
  selected=[r for line in cls.selected.splitlines() for r in json.loads(line)['records']]
  sources={r.get('datasetFileId'):r for r in records if r['type']=='dataset_file'}
  generated=next(r for r in records if r['type']=='attempt_generated_gcp')
  objects=[]
  for item in selected:
   original=sources[item['datasetFileId']] if item['type']=='dataset_file' else generated
   objects.append(dict(id=item['pageObjectId'],name=original.get('relativePath','gcp_list.txt').rsplit('/',1)[-1],byteSize=original['byteSize'],sha256=original['sha256'],kind=original.get('kind','auxiliary'),mediaType=original.get('mediaType',item.get('mediaType'))))
  cls.closure,cls.pages=build(objects)
  e=cls.export
  cls.binding=dict(datasetId=e['datasetId'],datasetManifestSha256=e['datasetManifestSha256'],principalId='synthetic-viewer',environment='staging',authorityId='synthetic-authority',viewerTaskId=e['taskId'],viewerAttemptId=e['attemptId'],pipelineDigest='c'*64,revocationEpoch=1,sourcePageHashes=e['sourcePageHashes'],sourceCount=e['sourceCount'],selectedPageHashes=e['selectedPageHashes'],selectedCount=e['selectedCount'],inputManifestSha256=e['inputManifestSha256'],processingClosureSha256=_digest(cls.closure))

 def test_export_receiver_and_fail_closed(self):
  self.assertEqual(self.export['state'],'held_authenticated_source_unavailable')
  self.assertEqual(self.binding['selectedCount'],16834)
  with tempfile.TemporaryDirectory(prefix='paged-viewer-pair-') as directory:
   db=sqlite3.connect(str(Path(directory)/'synthetic.sqlite'))
   try:
    initialize(db)
    def run(stage_id, source=None, selected=None, pin=None):
     return stage(db,stage_id,trusted_binding=self.binding,expected_binding_sha256=pin or binding_digest(self.binding),source_stream=io.BytesIO(self.source if source is None else source),selected_stream=io.BytesIO(self.selected if selected is None else selected),processing_closure=self.closure,processing_pages=self.pages)
    result=run('actual-export');self.assertEqual(result['state'],'held_authenticated_source_unavailable');self.assertTrue(result['created'])
    self.assertFalse(run('actual-export')['created'])
    with self.assertRaises(ReceiverHeld):run('wrong-pin',pin='0'*64)
    with self.assertRaises(ReceiverHeld):run('truncated',source=self.source[:-1])
    lines=self.selected.splitlines(keepends=True)
    with self.assertRaises(ReceiverHeld):run('reordered',selected=b''.join([lines[1],lines[0],*lines[2:]]))
   finally:db.close()

 def test_actual_mixed_case_gcp_order_is_not_wire_compatible(self):
  out=subprocess.run(['node',str(ROOT/'tests/support/viewer_paged_pair.cjs'),'mixed'],cwd=ROOT,capture_output=True,text=True,encoding='utf8',check=True,timeout=30)
  exported=json.loads(out.stdout)
  self.assertEqual([f['datasetFileId'] for f in exported['generatedGcp']['sourceFiles']],['control','Control'])
  with self.assertRaises(FrameHeld):
   verify_source(io.BytesIO(base64.b64decode(exported['sourceFrames'])),expected_page_hashes=exported['sourcePageHashes'],expected_items=exported['sourceCount'])

 def test_path_and_mime_boundaries_hold(self):
  out=subprocess.run(['node',str(ROOT/'tests/support/viewer_paged_pair.cjs'),'path'],cwd=ROOT,capture_output=True,text=True,encoding='utf8',check=True,timeout=30)
  exported=json.loads(out.stdout)
  with self.assertRaises(FrameHeld):
   verify_source(io.BytesIO(base64.b64decode(exported['sourceFrames'])),expected_page_hashes=exported['sourcePageHashes'],expected_items=exported['sourceCount'])
  out=subprocess.run(['node',str(ROOT/'tests/support/viewer_paged_pair.cjs'),'mime'],cwd=ROOT,capture_output=True,text=True,encoding='utf8',timeout=30)
  self.assertNotEqual(out.returncode,0)
  self.assertIn('unsupported_image_media_type',out.stderr)

if __name__=='__main__':unittest.main()
