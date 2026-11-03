"""Inactive bounded GCP evidence framing; cryptographic pins must be external."""
import base64
import codecs
import hashlib
import io
import json
import re
from . import reference_protocol as p

VERSION='ltds-viewer-gcp-evidence-chunks-v1'
DOMAIN=b'ltds-viewer-gcp-evidence-frame-v1\0'
MAX_LINE=65536
MAX_PAGE=65535
MAX_PAGES=512
MAX_DECODED_CHUNK=46080
MAX_DECODED=16*1024*1024
MAX_STREAM=24*1024*1024

class EvidenceHeld(ValueError):pass

def _pairs(items):
 out={}
 for k,v in items:
  if k in out:raise ValueError()
  out[k]=v
 return out

def _preflight(raw):
 # A single flat object only. This bounds nesting/members/strings BEFORE the
 # allocating JSON decoder; syntax and duplicate keys are checked again.
 depth=0;quoted=False;escaped=False;length=0;commas=0
 for ch in raw:
  if quoted:
   length+=1
   if length>61441:raise ValueError()
   if escaped:escaped=False
   elif ch==92:escaped=True
   elif ch==34:quoted=False
   elif ch<32:raise ValueError()
  elif ch==34:quoted=True;length=0
  elif ch==123:
   depth+=1
   if depth>1:raise ValueError()
  elif ch==125:
   depth-=1
   if depth<0:raise ValueError()
  elif ch in (91,93):raise ValueError()
  elif ch==44:
   commas+=1
   if commas>7:raise ValueError()
 if quoted or escaped or depth!=0:raise ValueError()

def verify(stream,*,attempt_id,evidence_sha256,page_hashes,page_bytes,decoded_bytes):
 """Validate complete evidence stream but confer no trust, custody or authority."""
 try:
  if type(stream) not in (io.BytesIO,io.BufferedReader) or not stream.readable():raise ValueError()
  p._uuid(attempt_id,'attempt');p._hash(evidence_sha256,'evidence digest');p._integer(decoded_bytes,'evidence size')
  if decoded_bytes>MAX_DECODED or type(page_hashes) is not list or type(page_bytes) is not list or not 2<=len(page_hashes)<=MAX_PAGES or len(page_bytes)!=len(page_hashes):raise ValueError()
  if not len(page_hashes)-1<=decoded_bytes<=(len(page_hashes)-1)*MAX_DECODED_CHUNK:raise ValueError()
  total_stream=0
  for sha,size in zip(page_hashes,page_bytes):
   p._hash(sha,'page hash');p._integer(size,'page size')
   if size>MAX_PAGE:raise ValueError()
   total_stream+=size+1
  if total_stream>MAX_STREAM:raise ValueError()
  content_hash=hashlib.sha256();decoder=codecs.getincrementaldecoder('utf-8')('strict');offset=0
  for index,(sha,size) in enumerate(zip(page_hashes,page_bytes)):
   line=stream.readline(MAX_LINE+1)
   if type(line) is not bytes or not line.endswith(b'\n') or len(line)>MAX_LINE:raise ValueError()
   raw=line[:-1]
   if len(raw)!=size or hashlib.sha256(DOMAIN+raw).hexdigest()!=sha:raise ValueError()
   _preflight(raw)
   obj=json.loads(raw.decode('utf8'),object_pairs_hook=_pairs,parse_constant=lambda _:(_ for _ in ()).throw(ValueError()))
   if p.canonical_json(obj)!=raw:raise ValueError()
   if index==len(page_hashes)-1:
    p._closed(obj,('version','attemptId','evidenceSha256','index','offset','totalBytes','complete'),'trailer')
    if obj['complete'] is not True or type(obj['totalBytes']) is not int or obj['totalBytes']!=decoded_bytes or offset!=decoded_bytes:raise ValueError()
   else:
    p._closed(obj,('version','attemptId','evidenceSha256','index','offset','decodedByteCount','data'),'data')
    data=obj['data'];size_dec=obj['decodedByteCount']
    if type(data) is not str or not re.fullmatch(r'[A-Za-z0-9_-]+',data) or len(data)>61440 or type(size_dec) is not int or not 1<=size_dec<=MAX_DECODED_CHUNK:raise ValueError()
    chunk=base64.b64decode(data+'='*((-len(data))%4),altchars=b'-_',validate=True)
    if len(chunk)!=size_dec or base64.urlsafe_b64encode(chunk).decode().rstrip('=')!=data:raise ValueError()
    decoder.decode(chunk,final=False);content_hash.update(chunk);offset+=len(chunk)
    if offset>decoded_bytes:raise ValueError()
   if obj['version']!=VERSION or obj['attemptId']!=attempt_id or obj['evidenceSha256']!=evidence_sha256 or type(obj['index']) is not int or obj['index']!=index or type(obj['offset']) is not int:raise ValueError()
   expected_offset=offset if index==len(page_hashes)-1 else offset-len(chunk)
   if obj['offset']!=expected_offset:raise ValueError()
  decoder.decode(b'',final=True)
  if content_hash.hexdigest()!=evidence_sha256 or stream.read(1)!=b'':raise ValueError()
  return {'state':'held_evidence_not_authorized','bytes':offset,'pages':len(page_hashes)}
 except (ValueError,TypeError,KeyError,UnicodeError,OverflowError,AttributeError):raise EvidenceHeld('evidence_stream_unavailable') from None
