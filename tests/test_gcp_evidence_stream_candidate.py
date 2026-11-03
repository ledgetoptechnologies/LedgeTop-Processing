import base64
import copy
import hashlib
import io
import json
import unittest
from unittest.mock import patch
from processing import reference_protocol as p
from processing.gcp_evidence_stream_candidate import verify,EvidenceHeld,VERSION,DOMAIN,MAX_DECODED_CHUNK

ATTEMPT='901a7098-d215-41aa-9c2a-530c81ecff04'
def evidence_frames(data):
 sha=hashlib.sha256(data).hexdigest();pages=[]
 for idx,start in enumerate(range(0,len(data),MAX_DECODED_CHUNK)):
  part=data[start:start+MAX_DECODED_CHUNK];pages.append(dict(version=VERSION,attemptId=ATTEMPT,evidenceSha256=sha,index=idx,offset=start,decodedByteCount=len(part),data=base64.urlsafe_b64encode(part).decode().rstrip('=')))
 pages.append(dict(version=VERSION,attemptId=ATTEMPT,evidenceSha256=sha,index=len(pages),offset=len(data),totalBytes=len(data),complete=True))
 return sha,pages
def encoded(pages):
 raws=[p.canonical_json(x) for x in pages];return b''.join(x+b'\n' for x in raws),[hashlib.sha256(DOMAIN+x).hexdigest() for x in raws],[len(x) for x in raws]

class EvidenceStreamTests(unittest.TestCase):
 def setUp(self):
  self.data=b'a'*(MAX_DECODED_CHUNK-1)+'\u96ea'.encode()+b'z';self.sha,self.pages=evidence_frames(self.data)
 def call(self,pages=None,raw=None,**kwargs):
  wire,pins,sizes=encoded(self.pages if pages is None else pages);args=dict(attempt_id=ATTEMPT,evidence_sha256=self.sha,page_hashes=pins,page_bytes=sizes,decoded_bytes=len(self.data));args.update(kwargs)
  return verify(io.BytesIO(wire if raw is None else raw),**args)
 def test_utf8_character_split_across_chunks(self):
  result=self.call();self.assertEqual(result,dict(state='held_evidence_not_authorized',bytes=len(self.data),pages=3))
 def test_truncation_reorder_extra_or_untrusted_hash_holds(self):
  wire,pins,sizes=encoded(self.pages)
  for payload in (wire[:-1],b''.join(x+b'\n' for x in reversed([p.canonical_json(v) for v in self.pages])),wire+b'{}\n'):
   with self.subTest(length=len(payload)),self.assertRaises(EvidenceHeld):self.call(raw=payload)
  with self.assertRaises(EvidenceHeld):self.call(page_hashes=['f'*64]+pins[1:])
 def test_offsets_attempt_trailer_and_base64_hold_even_if_repinned(self):
  for kind in ('offset','attempt','trailer','base64','count','duplicate_keys'):
   pages=copy.deepcopy(self.pages)
   if kind=='offset':pages[1]['offset']+=1
   elif kind=='attempt':pages[0]['attemptId']='5a410780-c3bc-46d1-903a-5713673a5399'
   elif kind=='trailer':pages[-1]['complete']=False
   elif kind=='base64':pages[1]['data']+='='
   elif kind=='count':pages[0]['decodedByteCount']=True
   else:
    wire,pins,sizes=encoded(pages);raw=p.canonical_json(pages[0]);bad=raw[:-1]+b',"index":0}';pins[0]=hashlib.sha256(DOMAIN+bad).hexdigest();sizes[0]=len(bad);parts=wire.splitlines(keepends=True);parts[0]=bad+b'\n'
    with self.assertRaises(EvidenceHeld):self.call(raw=b''.join(parts),page_hashes=pins,page_bytes=sizes)
    continue
   with self.subTest(kind=kind),self.assertRaises(EvidenceHeld):self.call(pages)
 def test_invalid_utf8_holds_even_if_digest_and_pins_match(self):
  data=b'\xff';sha,pages=evidence_frames(data);wire,pins,sizes=encoded(pages)
  with self.assertRaises(EvidenceHeld):verify(io.BytesIO(wire),attempt_id=ATTEMPT,evidence_sha256=sha,page_hashes=pins,page_bytes=sizes,decoded_bytes=1)
 def test_huge_line_rejected_before_json_decoder(self):
  wire,pins,sizes=encoded(self.pages)
  with patch('processing.gcp_evidence_stream_candidate.json.loads',side_effect=AssertionError('decoder reached')):
   with self.assertRaises(EvidenceHeld):self.call(raw=b'x'*65536+b'\n'+wire)

if __name__=='__main__':unittest.main()
