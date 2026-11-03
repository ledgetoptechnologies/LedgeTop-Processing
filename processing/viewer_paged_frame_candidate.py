"""Held bounded framing primitive; not a Viewer loader or mapping verifier."""
import hashlib
import io
import json
import re
import unicodedata
from . import reference_protocol as p
from .viewer_import_confirmation_store import _preflight_stored_json

VERSION='ltds-viewer-source-frames-v1'
DOMAIN=b'ltds-viewer-source-frame-v1\0'
MAX_FRAME_BYTES=256*1024
MAX_TOTAL_BYTES=12*1024*1024
MAX_FRAMES=64
MAX_ITEMS_PER_FRAME=512
MAX_ITEMS=20000

class FrameHeld(ValueError):pass

def _pairs(values):
 obj={}
 for key,value in values:
  if key in obj:raise ValueError()
  obj[key]=value
 return obj

def _path(value):
 if type(value) is not str or not value or len(value)>4096 or value.startswith('/') or '\\' in value or re.match(r'^[A-Za-z]:',value) or any(unicodedata.category(c).startswith('C') for c in value) or any(c in ('','.','..') for c in value.split('/')):raise ValueError()
 if len(value.encode('utf8'))>4096 or any(len(c.encode('utf8'))>255 for c in value.split('/')):raise ValueError()

def _item(value):
 if type(value) is not dict:raise ValueError()
 if value.get('type')=='dataset_file':
  p._closed(value,('type','datasetFileId','relativePath','byteSize','sha256','processingRole','mediaType'),'dataset file')
  p._slug(value['datasetFileId'],'source ID');_path(value['relativePath']);p._string(value['processingRole'],'role',80);p._string(value['mediaType'],'media type',127)
 elif value.get('type')=='attempt_generated_gcp':
  p._closed(value,('type','attemptId','byteSize','sha256','sourceFiles','correspondenceSha256'),'GCP')
  p._uuid(value['attemptId'],'attempt');p._hash(value['correspondenceSha256'],'correspondence')
  refs=value['sourceFiles']
  if type(refs) is not list or not 0<len(refs)<=64:raise ValueError()
  ids=[]
  for ref in refs:
   p._closed(ref,('datasetFileId','relativePath','sha256'),'GCP source');p._slug(ref['datasetFileId'],'source ID');_path(ref['relativePath']);p._hash(ref['sha256'],'source hash');ids.append(ref['datasetFileId'])
  if ids!=sorted(set(ids)):raise ValueError()
 else:raise ValueError()
 p._integer(value['byteSize'],'size',allow_zero=True);p._hash(value['sha256'],'digest')

def verify(stream,*,expected_page_hashes,expected_items):
 """Bound read/parse and validate pins; consume no untrusted external side effects.

 Expected hashes/count must be independently trusted. A successful result is
 held and DOES NOT prove dataset completeness, selection, custody or auth.
 """
 try:
  if type(stream) not in (io.BytesIO,io.BufferedReader) or not stream.readable():raise ValueError()
  if type(expected_page_hashes) is not list or not 0<len(expected_page_hashes)<=MAX_FRAMES:raise ValueError()
  for h in expected_page_hashes:p._hash(h,'page digest')
  p._integer(expected_items,'count')
  if expected_items>MAX_ITEMS:raise ValueError()
  count=0;total=0;seen=set();paths=set();generated=0;source_facts={};gcp_refs=[]
  for index,pin in enumerate(expected_page_hashes):
   line=stream.readline(MAX_FRAME_BYTES+2)
   if type(line) is not bytes or not line.endswith(b'\n') or len(line)>MAX_FRAME_BYTES+1:raise ValueError()
   raw=line[:-1];total+=len(line)
   if total>MAX_TOTAL_BYTES:raise ValueError()
   _preflight_stored_json(raw)
   frame=json.loads(raw.decode('utf8'),object_pairs_hook=_pairs,parse_constant=lambda _:(_ for _ in ()).throw(ValueError()))
   p._closed(frame,('version','index','records'),'frame')
   if frame['version']!=VERSION or type(frame['index']) is not int or frame['index']!=index or type(frame['records']) is not list or not 0<len(frame['records'])<=MAX_ITEMS_PER_FRAME:raise ValueError()
   if p.canonical_json(frame)!=raw or hashlib.sha256(DOMAIN+raw).hexdigest()!=pin:raise ValueError()
   for item in frame['records']:
    _item(item);count+=1
    if count>MAX_ITEMS:raise ValueError()
    if item['type']=='dataset_file':
     if item['datasetFileId'] in seen or item['relativePath'] in paths:raise ValueError()
     seen.add(item['datasetFileId']);paths.add(item['relativePath'])
     if item['processingRole']=='gcp_source':source_facts[item['datasetFileId']]=(item['relativePath'],item['sha256'])
    else:
     generated+=1
     if generated>1:raise ValueError()
     gcp_refs=item['sourceFiles']
  if count!=expected_items or stream.read(1)!=b'':raise ValueError()
  if any(source_facts.get(ref['datasetFileId'])!=(ref['relativePath'],ref['sha256']) for ref in gcp_refs):raise ValueError()
  return {'state':'held_no_viewer_authority_or_mapping','objects':count,'pages':len(expected_page_hashes),'generatedGcp':generated,'framedBytes':total}
 except (ValueError,TypeError,KeyError,UnicodeError,OverflowError):raise FrameHeld('frame_validation_unavailable') from None
