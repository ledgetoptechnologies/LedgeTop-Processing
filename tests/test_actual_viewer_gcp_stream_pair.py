"""Synthetic actual Viewer evidence frames paired to inactive Processing decoder."""
import base64
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import unittest

from processing.gcp_evidence_stream_candidate import verify, EvidenceHeld
from processing.viewer_paged_frame_candidate import verify_dataset_count_v2, FrameHeld
from processing.gcp_semantic_bridge_candidate import inspect as semantic_inspect, SemanticHeld
from processing.gcp_file_stream_candidate import verify as verify_file
from processing.gcp_cumulative_quota_candidate import validate_wire_quota

ROOT=Path(__file__).resolve().parents[1]
PIN='4ed7dcd566380c3c0cd7f03a765ee1badf2004d5'
def decoded(s):return base64.b64decode(s,validate=True)

class ViewerGcpPair(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  root=os.environ.get('PINNED_VIEWER_ROOT')
  if not root:
   if os.environ.get('GCP_VIEWER_PAIR_REQUIRED')=='1':raise RuntimeError('PINNED_VIEWER_ROOT required')
   raise unittest.SkipTest('PINNED_VIEWER_ROOT required')
  viewer=Path(root).resolve(strict=True)
  head=subprocess.run(['git','rev-parse','HEAD'],cwd=viewer,capture_output=True,text=True,check=True).stdout.strip()
  status=subprocess.run(['git','status','--porcelain','--untracked-files=all'],cwd=viewer,capture_output=True,text=True,check=True).stdout
  if head!=PIN or status:raise RuntimeError('clean pinned Viewer checkout required')
  out=subprocess.run(['node',str(ROOT/'tests/support/viewer_gcp_export_pair.cjs'),'mixed'],env=dict(os.environ,PINNED_VIEWER_ROOT=str(viewer)),cwd=ROOT,capture_output=True,text=True,encoding='utf8',check=True,timeout=30)
  cls.x=json.loads(out.stdout)

 def run_evidence(self,stream,hashes=None,sizes=None,sha=None,count=None):
  x=self.x
  return verify(io.BytesIO(stream),attempt_id=x['attemptId'],evidence_sha256=sha or x['gcpEvidenceSha256'],page_hashes=hashes or x['gcpEvidencePageHashes'],page_bytes=sizes or x['gcpEvidencePageBytes'],decoded_bytes=count or x['gcpEvidenceByteCount'])

 def test_actual_snapshot_bytes_and_evidence(self):
  x=self.x;evidence=decoded(x['storedEvidence']);file=decoded(x['storedFile'])
  self.assertEqual(hashlib.sha256(evidence).hexdigest(),x['gcpEvidenceSha256'])
  self.assertEqual(hashlib.sha256(file).hexdigest(),x['gcpFileSha256'])
  self.assertEqual((len(evidence),len(file)),(x['gcpEvidenceByteCount'],x['gcpFileByteCount']))
  streams=[decoded(x[k]) for k in ('sourceFrames','selectedFrames','gcpEvidenceFrames','gcpFileFrames')]
  self.assertEqual(sum(map(len,streams)),x['totalStreamBytes']);self.assertLessEqual(x['totalStreamBytes'],72*1024*1024)
  for stream,limit in zip(streams,(12,12,24,24)):self.assertLessEqual(len(stream),limit*1024*1024)
  for stream,key,domain in zip(streams,('sourcePageHashes','selectedPageHashes','gcpEvidencePageHashes','gcpFilePageHashes'),(b'ltds-viewer-source-frame-v1\0',b'ltds-viewer-selected-frame-v1\0',b'ltds-viewer-gcp-evidence-frame-v1\0',b'ltds-viewer-generated-gcp-file-frame-v1\0')):
   lines=stream.splitlines(keepends=True);self.assertEqual(len(lines),len(x[key]))
   for index,(line,pin) in enumerate(zip(lines,x[key])):
    self.assertTrue(line.endswith(b'\n'));self.assertEqual(json.loads(line)['index'],index)
    self.assertEqual(hashlib.sha256(domain+line[:-1]).hexdigest(),pin)
  for stream,expected in ((streams[2],evidence),(streams[3],file)):
   frames=[json.loads(line) for line in stream.splitlines()]
   self.assertIs(frames[-1]['complete'],True)
   # Fixture-only extraction to compare exact stored bytes; not a file decoder.
   chunks=[base64.urlsafe_b64decode(frame['data']+'='*((-len(frame['data']))%4)) for frame in frames[:-1]]
   self.assertEqual(b''.join(chunks),expected)
  self.assertEqual(self.run_evidence(streams[2])['state'],'held_evidence_not_authorized')
  source=[r for line in streams[0].splitlines() for r in json.loads(line)['records']]
  selected=[r for line in streams[1].splitlines() for r in json.loads(line)['records']]
  parsed=json.loads(evidence);rows=parsed['correspondences']
  # Synthetic independent trust fixture; real authenticated loaders do not exist.
  trust=copy.deepcopy(rows);images={r['imageDatasetFileId']:dict(relativePath=r['imageRelativePath'],width=r['imageWidth'],height=r['imageHeight']) for r in rows}
  args=dict(attempt_id=x['attemptId'],evidence=parsed,evidence_bytes=evidence,evidence_sha256=x['gcpEvidenceSha256'],generated_bytes=file,source_records=source,selected_records=selected,trusted_images=images,trusted_correspondences=trust)
  result=semantic_inspect(**args)
  self.assertEqual(result['state'],'held_gcp_authority_correspondence_hash_unverified')
  self.assertEqual(verify_dataset_count_v2(io.BytesIO(streams[0]),expected_page_hashes=x['sourcePageHashes'],expected_dataset_items=x['sourceCount'])['generatedGcp'],1)
  with self.assertRaises(FrameHeld):verify_dataset_count_v2(io.BytesIO(streams[0]),expected_page_hashes=x['sourcePageHashes'],expected_dataset_items=x['sourceCount']+1)
  self.assertEqual(verify_file(io.BytesIO(streams[3]),attempt_id=x['attemptId'],evidence_sha256=x['gcpEvidenceSha256'],file_sha256=x['gcpFileSha256'],page_hashes=x['gcpFilePageHashes'],page_bytes=x['gcpFilePageBytes'],decoded_bytes=x['gcpFileByteCount'])['state'],'held_file_not_authorized')
  self.assertEqual(validate_wire_quota(source_bytes=len(streams[0]),selected_bytes=len(streams[1]),evidence_page_bytes=x['gcpEvidencePageBytes'],file_page_bytes=x['gcpFilePageBytes']),x['totalStreamBytes'])
  # Reject oversized/nested synthetic trust before any serialization.
  from unittest.mock import patch
  for bad_trust in ([rows[0]]*100001, [dict(rows[0], label=['nested'])]):
   bad=copy.copy(args);bad['trusted_correspondences']=bad_trust
   with patch('json.dumps',side_effect=AssertionError('unexpected serialization')):
    with self.assertRaises(SemanticHeld):semantic_inspect(**bad)
  bad=copy.copy(args);bad['trusted_images']={f'image-{i}':None for i in range(20001)}
  with patch('json.dumps',side_effect=AssertionError('unexpected serialization')):
   with self.assertRaises(SemanticHeld):semantic_inspect(**bad)
  for field,value in (('pixelX',4001),('correspondenceId','changed'),('gcpSetId','changed'),('label','changed')):
   bad=copy.deepcopy(args);bad['evidence']['correspondences'][0][field]=value
   bad['evidence_bytes']=json.dumps(bad['evidence'],separators=(',',':')).encode();bad['evidence_sha256']=hashlib.sha256(bad['evidence_bytes']).hexdigest()
   with self.subTest(field=field),self.assertRaises(SemanticHeld):semantic_inspect(**bad)
  bad=copy.deepcopy(args);first=rows[0]['imageDatasetFileId'];bad['trusted_images'][first]['width']+=1
  with self.assertRaises(SemanticHeld):semantic_inspect(**bad)
  bad=copy.deepcopy(args);bad['evidence']['correspondences'][0]['pixelX']=4000;bad['trusted_correspondences'][0]['pixelX']=4000
  bad['evidence_bytes']=json.dumps(bad['evidence'],separators=(',',':')).encode();bad['evidence_sha256']=hashlib.sha256(bad['evidence_bytes']).hexdigest()
  with self.assertRaises(SemanticHeld):semantic_inspect(**bad)

 def test_evidence_tamper_truncation_and_split_utf8(self):
  x=self.x;stream=decoded(x['gcpEvidenceFrames'])
  with self.assertRaises(EvidenceHeld):self.run_evidence(stream[:-1])
  damaged=bytearray(stream);damaged[20]^=1
  with self.assertRaises(EvidenceHeld):self.run_evidence(bytes(damaged))
  with self.assertRaises(EvidenceHeld):self.run_evidence(b''.join(stream.splitlines(keepends=True)[:-1]))
  s=x['split']
  self.assertEqual(self.run_evidence(decoded(s['bytes']),s['hashes'],s['sizes'],s['sha256'],s['count'])['bytes'],46081)

if __name__=='__main__':unittest.main()
