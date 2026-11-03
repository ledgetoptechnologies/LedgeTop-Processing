"""Inactive candidate: bounded ordered manifest pages; no source authority or activation."""
import hashlib
from . import reference_protocol as p

VERSION='ltds-paged-source-v1'
CLOSURE_DOMAIN=b'ltds-paged-source-v1-closure\x00'
PAGE_DOMAIN=b'ltds-paged-source-v1-page\x00'
MAX_OBJECTS=25_000
MAX_PAGE_OBJECTS=512
MAX_PAGES=64
MAX_PAGE_BYTES=256*1024
MAX_TOTAL_BYTES=8*1024*1024

class PagedManifestError(ValueError): pass

def _digest(value):
 return hashlib.sha256(CLOSURE_DOMAIN+p.canonical_json(value)).hexdigest()

def _check_object(obj):
 p._closed(obj,('id','name','byteSize','sha256','kind','mediaType'),'object')
 p._slug(obj['id'],'object id');p._basename(obj['name']);p._integer(obj['byteSize'],'object size');p._hash(obj['sha256'],'object hash');p._string(obj['mediaType'],'media type',127)
 if obj['kind'] not in ('image','lidar','auxiliary'):raise ValueError()

def build(objects):
 """Prepare bounded metadata only; caller must not treat this as authorization."""
 if type(objects) is not list or not 0 < len(objects) <= MAX_OBJECTS: raise PagedManifestError('object count')
 try:
  for obj in objects:_check_object(obj)
 except (ValueError,TypeError,KeyError):raise PagedManifestError('invalid paged manifest') from None
 pages=[]
 for start in range(0,len(objects),MAX_PAGE_OBJECTS):
  body={'index':len(pages),'objects':[dict(obj) for obj in objects[start:start+MAX_PAGE_OBJECTS]]}
  pages.append(dict(body=body,sha256=hashlib.sha256(PAGE_DOMAIN+p.canonical_json(body)).hexdigest()))
 closure={'version':VERSION,'objectCount':len(objects),'totalBytes':sum(o['byteSize'] for o in objects),'pageCount':len(pages),'pages':[page['sha256'] for page in pages]}
 # Building does not bypass input validation or resource limits.
 validate(closure,pages,expected_closure_sha256=_digest(closure))
 return closure,pages

def validate(closure,pages,*,expected_closure_sha256):
 """Validate complete ordered closure; return detached indexed object metadata."""
 try:
  p._hash(expected_closure_sha256,'pinned closure digest')
  p._closed(closure,('version','objectCount','totalBytes','pageCount','pages'),'closure')
  if closure['version']!=VERSION or type(pages) is not list or not 0<len(pages)<=MAX_PAGES: raise ValueError()
  if type(closure['pageCount']) is not int or closure['pageCount']!=len(pages) or type(closure['objectCount']) is not int or not 0<closure['objectCount']<=MAX_OBJECTS: raise ValueError()
  p._integer(closure['totalBytes'],'totalBytes')
  if type(closure['pages']) is not list or len(closure['pages'])!=len(pages):raise ValueError()
  for page_hash in closure['pages']:p._hash(page_hash,'page hash')
  if _digest(closure)!=expected_closure_sha256:raise ValueError()
  ids={};names=set();used=0;total=0
  for idx,page in enumerate(pages):
   p._closed(page,('body','sha256'),'page');body=page['body'];p._closed(body,('index','objects'),'page body')
   if type(body['index']) is not int or body['index']!=idx or type(body['objects']) is not list or not 0<len(body['objects'])<=MAX_PAGE_OBJECTS:raise ValueError()
   snapshot=[]
   for source_obj in body['objects']:
    _check_object(source_obj)
    obj=dict(source_obj);_check_object(obj);snapshot.append(obj)
    if obj['id'] in ids or obj['name'].casefold() in names:raise ValueError()
    ids[obj['id']]=dict(obj);names.add(obj['name'].casefold());total+=obj['byteSize']
    if total>p.MAX_SAFE_INTEGER or len(ids)>MAX_OBJECTS:raise ValueError()
   raw=p.canonical_json({'index':idx,'objects':snapshot});used+=len(raw)
   if len(raw)>MAX_PAGE_BYTES or used>MAX_TOTAL_BYTES:raise ValueError()
   digest=hashlib.sha256(PAGE_DOMAIN+raw).hexdigest()
   if page['sha256']!=digest or closure['pages'][idx]!=digest:raise ValueError()
  if len(ids)!=closure['objectCount'] or total!=closure['totalBytes']:raise ValueError()
  return {'closureSha256':_digest(closure),'byId':ids,'metadataBytes':used}
 except (ValueError,TypeError,KeyError,OverflowError):raise PagedManifestError('invalid paged manifest') from None
