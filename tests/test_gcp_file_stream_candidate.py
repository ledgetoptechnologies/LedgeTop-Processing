import base64,hashlib,io,json,subprocess,unittest
from processing import gcp_file_stream_candidate as f
from processing.gcp_cumulative_quota_candidate import validate_wire_quota

class FileStream(unittest.TestCase):
 def fixture(self):
  # Independent Node producer models the proposed wire; paired Viewer producer
  # coverage is separate and requires its immutable committed implementation.
  js=r'''const c=require('crypto'), id='00000000-0000-4000-8000-000000000001', ev='a'.repeat(64), raw=Buffer.concat([Buffer.alloc(46079,97),Buffer.from('€ text')]); const hash=x=>c.createHash('sha256').update(x).digest('hex'), fh=hash(raw), domain=Buffer.from('ltds-viewer-generated-gcp-file-frame-v1\0');const canonical=o=>JSON.stringify(Object.fromEntries(Object.entries(o).sort()));let pages=[],offs=0;for(let i=0;offs<raw.length;i++){let data=raw.subarray(offs,offs+46080);pages.push({version:'ltds-viewer-generated-gcp-file-chunks-v1',attemptId:id,evidenceSha256:ev,fileSha256:fh,index:i,offset:offs,decodedByteCount:data.length,data:data.toString('base64url')});offs+=data.length;}pages.push({version:'ltds-viewer-generated-gcp-file-chunks-v1',attemptId:id,evidenceSha256:ev,fileSha256:fh,index:pages.length,offset:offs,totalBytes:raw.length,complete:true});let lines=pages.map(x=>Buffer.from(canonical(x)));process.stdout.write(JSON.stringify({id,ev,fh,size:raw.length,wire:Buffer.concat(lines.map(x=>Buffer.concat([x,Buffer.from('\n')]))).toString('base64'),hashes:lines.map(x=>hash(Buffer.concat([domain,x]))),sizes:lines.map(x=>x.length)}));'''
  return json.loads(subprocess.check_output(['node','-e',js],text=True))
 def call(self,x,wire=None,**kw):
  return f.verify(io.BytesIO(base64.b64decode(x['wire']) if wire is None else wire),attempt_id=x['id'],evidence_sha256=kw.get('ev',x['ev']),file_sha256=kw.get('fh',x['fh']),page_hashes=kw.get('hashes',x['hashes']),page_bytes=kw.get('sizes',x['sizes']),decoded_bytes=x['size'])
 def test_valid_and_bad_binding_and_truncation(self):
  x=self.fixture();self.assertEqual(self.call(x)['state'],'held_file_not_authorized'); wire=base64.b64decode(x['wire']);lines=wire.splitlines(keepends=True)
  for bad in (wire[:-1],b''.join(lines[:-1]),lines[1]+lines[0]+lines[2],wire+b'{}\n'):
   with self.assertRaises(f.EvidenceHeld):self.call(x,bad)
  for key in ('ev','fh'):
   with self.assertRaises(f.EvidenceHeld):self.call(x,**{key:'b'*64})
 def test_quota_includes_lf_and_rejects_partial(self):
  self.assertEqual(validate_wire_quota(source_bytes=10,selected_bytes=20,evidence_page_bytes=[1,2],file_page_bytes=[3,4]),44)
  with self.assertRaises(ValueError):validate_wire_quota(source_bytes=10,selected_bytes=20,evidence_page_bytes=[1,2],file_page_bytes=None)
  with self.assertRaises(ValueError):validate_wire_quota(source_bytes=10,selected_bytes=20,evidence_page_bytes=[True,2],file_page_bytes=[3,4])

class ExactQuota(unittest.TestCase):
 def test_exact_72_mib_and_one_byte_over(self):
  # 384 maximum-size signed pages per stream make exactly 24 MiB each,
  # including the mandatory LF after each page.
  pages=[65535]*384; twelve=12*1024*1024
  self.assertEqual(validate_wire_quota(source_bytes=twelve,selected_bytes=twelve,evidence_page_bytes=pages,file_page_bytes=pages),72*1024*1024)
  with self.assertRaises(ValueError):validate_wire_quota(source_bytes=twelve+1,selected_bytes=twelve,evidence_page_bytes=pages,file_page_bytes=pages)
  extra=pages[:-1]+[65535,1]
  with self.assertRaises(ValueError):validate_wire_quota(source_bytes=twelve,selected_bytes=twelve,evidence_page_bytes=extra,file_page_bytes=pages)
