"""Synthetic actual Viewer evidence frames paired to inactive Processing decoder."""
import base64
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import unittest

from processing.gcp_evidence_stream_candidate import verify, EvidenceHeld
from processing.viewer_paged_frame_candidate import verify as verify_source, FrameHeld
from processing.gcp_semantic_bridge_candidate import inspect as semantic_inspect

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
  result=semantic_inspect(attempt_id=x['attemptId'],evidence=json.loads(evidence),evidence_bytes=evidence,evidence_sha256=x['gcpEvidenceSha256'],generated_bytes=file,source_records=source,selected_records=selected)
  self.assertEqual(result['state'],'held_gcp_authority_correspondence_hash_unverified')
  # Current Viewer count excludes generated record; Processing includes it.
  with self.assertRaises(FrameHeld):verify_source(io.BytesIO(streams[0]),expected_page_hashes=x['sourcePageHashes'],expected_items=x['sourceCount'])

 def test_evidence_tamper_truncation_and_split_utf8(self):
  x=self.x;stream=decoded(x['gcpEvidenceFrames'])
  with self.assertRaises(EvidenceHeld):self.run_evidence(stream[:-1])
  damaged=bytearray(stream);damaged[20]^=1
  with self.assertRaises(EvidenceHeld):self.run_evidence(bytes(damaged))
  with self.assertRaises(EvidenceHeld):self.run_evidence(b''.join(stream.splitlines(keepends=True)[:-1]))
  s=x['split']
  self.assertEqual(self.run_evidence(decoded(s['bytes']),s['hashes'],s['sizes'],s['sha256'],s['count'])['bytes'],46081)

if __name__=='__main__':unittest.main()
