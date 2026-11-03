import json, subprocess, unittest, io, base64, hashlib, os
from pathlib import Path
from processing import gcp_evidence_stream_candidate as dec

VIEWER=Path(os.environ.get('VIEWER_PAIRED_ROOT', r'C:\Users\fstor\.codex\worktrees\viewer-accepted-copy-ingest-wiring-20261103'))
REV='6e487e1d578d459a0a49f8eb2a73e35c4cc0770c'

class ViewerPair(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  actual=subprocess.check_output(['git','rev-parse','HEAD'],cwd=VIEWER,text=True).strip()
  if actual!=REV: raise RuntimeError('Viewer checkout is not the pinned commit')
  changed=subprocess.check_output(['git','status','--porcelain','--','apps/viewer/server','apps/viewer/test/viewer-paged-source-export.test.js'],cwd=VIEWER,text=True)
  if changed: raise RuntimeError('Viewer producer paths are modified')
  src=subprocess.check_output(['git','show',REV+':apps/viewer/test/viewer-paged-source-export.test.js'],cwd=VIEWER,text=True)
  prefix=src.split('\nfunction assertFrames',1)[0]
  prefix=prefix.replace("const test = require('node:test');", "const test = null;")
  code="const {createRequire}=require('node:module'); const viewerRequire=createRequire("+json.dumps(str(VIEWER / 'apps/viewer/test/viewer-paged-source-export.test.js'))+");\n"+prefix.replace("require('../server/", "viewerRequire('../server/")
  code += r'''
const callbacks=[]; const t={after: f=>callbacks.push(f)};
try {
 const f=fixture(t); const v=captureHeldSourceExport(f.processing,{datasetId:f.dataset.id,taskId:f.task.id,attemptId:f.attempt.id});
 const evidence=f.database.prepare('SELECT evidence_json FROM processing_attempt_gcp_snapshots WHERE attempt_id=?').get(f.attempt.id).evidence_json;
 const n=fixture(t,{noGcp:true}); const nv=captureHeldSourceExport(n.processing,{datasetId:n.dataset.id,taskId:n.task.id,attemptId:n.attempt.id});
 const crossing=Buffer.concat([Buffer.alloc(46079,0x61), Buffer.from('€'), Buffer.from(' tail')]);
 const split=gcpEvidenceStream(crossing,f.attempt.id);
 function pack(w, a, bytes) {return {attempt:a, wire:w.bytes.toString('base64'), hashes:w.pageHashes,sizes:w.pageBytes, digest:sha(bytes),decoded:bytes.toString('base64')};}
 process.stdout.write(JSON.stringify({real:pack({bytes:v.gcpEvidenceFrames,pageHashes:v.gcpEvidencePageHashes,pageBytes:v.gcpEvidencePageBytes},f.attempt.id,Buffer.from(evidence)), split:pack(split,f.attempt.id,crossing), noGcp:[nv.gcpEvidenceVersion,nv.gcpEvidencePageHashes,nv.gcpEvidencePageBytes,nv.gcpEvidencePageCount,nv.gcpEvidenceByteCount,nv.gcpEvidenceSha256,nv.gcpEvidenceFrames]}));
} finally {for(const f of callbacks.reverse())f();}
'''
  cls.data=json.loads(subprocess.check_output(['node','--no-warnings','-e',code],cwd=VIEWER,text=True,timeout=45))
 def verify(self,v,wire=None, hashes=None, sizes=None):
  raw=base64.b64decode(v['decoded']); wire=base64.b64decode(v['wire']) if wire is None else wire
  return dec.verify(io.BytesIO(wire),attempt_id=v['attempt'],evidence_sha256=v['digest'],page_hashes=v['hashes'] if hashes is None else hashes,page_bytes=v['sizes'] if sizes is None else sizes,decoded_bytes=len(raw))
 def test_actual_repository_evidence(self):
  v=self.data['real']; self.assertEqual(self.verify(v)['state'],'held_evidence_not_authorized')
  raw=base64.b64decode(v['decoded']); self.assertEqual(hashlib.sha256(raw).hexdigest(),v['digest']); json.loads(raw)
  frames=[json.loads(x) for x in base64.b64decode(v['wire']).splitlines()]; reconstructed=b''.join(base64.urlsafe_b64decode(x['data']+'='*((-len(x['data']))%4)) for x in frames[:-1]); self.assertEqual(reconstructed,raw)
 def test_exporter_split_utf8_crosses_chunk_boundary(self):
  v=self.data['split']; raw=base64.b64decode(v['decoded']); self.assertEqual(raw[46079:46082], '€'.encode()); self.assertEqual(self.verify(v)['pages'],3)
 def test_missing_and_malformed_frames_held(self):
  v=self.data['split']; wire=base64.b64decode(v['wire']); lines=wire.splitlines(keepends=True)
  for broken in (b''.join(lines[:-1]),wire[:-1],lines[1]+lines[0]+lines[2],wire+b'{}\n',wire.replace(b'"complete":true',b'"complete":false')):
   with self.subTest(length=len(broken)), self.assertRaises(dec.EvidenceHeld): self.verify(v,broken)
 def test_invalid_trailer_even_with_recomputed_pins(self):
  v=self.data['split']; lines=base64.b64decode(v['wire']).splitlines(keepends=True); lines[-1]=lines[-1].replace(b'\"complete\":true',b'\"complete\":false'); raw=lines[-1][:-1]
  hashes=v['hashes'][:-1]+[hashlib.sha256(dec.DOMAIN+raw).hexdigest()]; sizes=v['sizes'][:-1]+[len(raw)]
  with self.assertRaises(dec.EvidenceHeld): self.verify(v,b''.join(lines),hashes,sizes)
 def test_no_gcp_all_pins_null(self):
  self.assertEqual(self.data['noGcp'],[None]*7)

if __name__=='__main__': unittest.main()
