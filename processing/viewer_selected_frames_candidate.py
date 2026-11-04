"""Held selected-frame prototype; no authenticated exporter or authorization."""
import hashlib
import io
import json
from . import reference_protocol as p
from .viewer_import_confirmation_store import _preflight_stored_json
from .viewer_paged_frame_candidate import verify as source_verify,FrameHeld,_pairs,MAX_FRAME_BYTES,MAX_TOTAL_BYTES,MAX_FRAMES,MAX_ITEMS_PER_FRAME,MAX_ITEMS
from .large_paged_manifest_candidate import validate as validate_pages

VERSION='ltds-viewer-selected-frames-v1'
BINDING_DOMAIN=b'ltds-viewer-selected-binding-v1\0'
DOMAIN=b'ltds-viewer-selected-frame-v1\0'

def binding_digest(binding):
 return hashlib.sha256(BINDING_DOMAIN+p.canonical_json(binding)).hexdigest()

def verify(source_stream,selected_stream,*,source_hashes,source_count,selected_hashes,input_manifest_sha256,processing_closure,processing_pages,closure_sha256,trusted_binding,expected_binding_sha256):
 """Pins are independent trusted inputs. No authority or custody is conferred."""
 try:
  p._hash(input_manifest_sha256,'Viewer input hash');p._hash(expected_binding_sha256,'binding pin')
  p._closed(trusted_binding,('datasetId','datasetManifestSha256','principalId','environment','authorityId','viewerTaskId','viewerAttemptId','pipelineDigest','revocationEpoch','sourcePageHashes','sourceCount','selectedPageHashes','selectedCount','inputManifestSha256','processingClosureSha256'),'selected binding')
  for key in ('datasetId','principalId','authorityId'):p._slug(trusted_binding[key],key)
  for key in ('datasetManifestSha256','pipelineDigest','inputManifestSha256','processingClosureSha256'):p._hash(trusted_binding[key],key)
  p._environment(trusted_binding['environment']);p._uuid(trusted_binding['viewerTaskId'],'task');p._uuid(trusted_binding['viewerAttemptId'],'attempt');p._integer(trusted_binding['revocationEpoch'],'epoch',allow_zero=True);p._integer(trusted_binding['sourceCount'],'source count');p._integer(trusted_binding['selectedCount'],'selected count')
  for key in ('sourcePageHashes','selectedPageHashes'):
   if type(trusted_binding[key]) is not list or not 0<len(trusted_binding[key])<=MAX_FRAMES:raise ValueError()
   for value in trusted_binding[key]:p._hash(value,key)
  if (trusted_binding['sourcePageHashes']!=source_hashes or trusted_binding['sourceCount']!=source_count or trusted_binding['selectedPageHashes']!=selected_hashes or trusted_binding['inputManifestSha256']!=input_manifest_sha256 or trusted_binding['processingClosureSha256']!=closure_sha256 or binding_digest(trusted_binding)!=expected_binding_sha256):raise ValueError()
  _,selected=source_verify(source_stream,expected_page_hashes=source_hashes,expected_items=source_count,_capture_selection=True)
  if type(selected_stream) not in (io.BytesIO,io.BufferedReader) or not selected_stream.readable():raise ValueError()
  if type(selected_hashes) is not list or not 0<len(selected_hashes)<=MAX_FRAMES:raise ValueError()
  for value in selected_hashes:p._hash(value,'selected page digest')
  by_id=validate_pages(processing_closure,processing_pages,expected_closure_sha256=closure_sha256)['byId']
  pages_ordered=[o for page in processing_pages for o in page['body']['objects']]
  if len(selected)!=len(by_id) or len(selected)!=len(pages_ordered):raise ValueError()
  digest=hashlib.sha256();digest.update(b'[');ordinal=0;total=0;seen=set()
  for index,pin in enumerate(selected_hashes):
   line=selected_stream.readline(MAX_FRAME_BYTES+2)
   if type(line) is not bytes or not line.endswith(b'\n') or len(line)>MAX_FRAME_BYTES+1:raise ValueError()
   raw=line[:-1];total+=len(line)
   if total>MAX_TOTAL_BYTES:raise ValueError()
   _preflight_stored_json(raw)
   frame=json.loads(raw.decode('utf8'),object_pairs_hook=_pairs,parse_constant=lambda _:(_ for _ in ()).throw(ValueError()))
   p._closed(frame,('version','index','records'),'selected frame')
   if frame['version']!=VERSION or type(frame['index']) is not int or frame['index']!=index or type(frame['records']) is not list or not 0<len(frame['records'])<=MAX_ITEMS_PER_FRAME:raise ValueError()
   if p.canonical_json(frame)!=raw or hashlib.sha256(DOMAIN+raw).hexdigest()!=pin:raise ValueError()
   for record in frame['records']:
    if ordinal>=len(selected) or ordinal>=MAX_ITEMS:raise ValueError()
    source=selected[ordinal];page=pages_ordered[ordinal]
    if record.get('type')=='dataset_file':
     p._closed(record,('type','ordinal','pageObjectId','datasetFileId'),'selected source')
     p._slug(record['datasetFileId'],'source ID')
     if source['type']!='dataset_file' or record['datasetFileId']!=source['datasetFileId']:raise ValueError()
     expected_kind=source['kind'];expected_mime=source['mediaType']
    elif record.get('type')=='attempt_generated_gcp':
     p._closed(record,('type','ordinal','pageObjectId','attemptId','mediaType'),'selected generated')
     p._uuid(record['attemptId'],'attempt');p._string(record['mediaType'],'generated media type',127)
     if source['type']!='attempt_generated_gcp' or record['attemptId']!=source['attemptId'] or record['attemptId']!=trusted_binding['viewerAttemptId']:raise ValueError()
     expected_kind='auxiliary';expected_mime=record['mediaType']
    else:raise ValueError()
    p._slug(record['pageObjectId'],'page ID')
    if type(record['ordinal']) is not int or record['ordinal']!=ordinal or record['pageObjectId'] in seen:raise ValueError()
    seen.add(record['pageObjectId'])
    if (page['id'],page['name'],page['byteSize'],page['sha256'],page['kind'],page['mediaType'])!=(record['pageObjectId'],source['relativePath'].rsplit('/',1)[-1],source['byteSize'],source['sha256'],expected_kind,expected_mime):raise ValueError()
    # Viewer inputManifestSha256 is JSON.stringify(array) with these keys in
    # insertion order; accepted paths contain no control characters.
    obj={'relativePath':source['relativePath'],'byteSize':source['byteSize'],'sha256':source['sha256']}
    if ordinal:digest.update(b',')
    digest.update(json.dumps(obj,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode('utf8'));ordinal+=1
  digest.update(b']')
  if selected_stream.read(1)!=b'' or ordinal!=len(selected) or ordinal!=trusted_binding['selectedCount'] or digest.hexdigest()!=input_manifest_sha256:raise ValueError()
  return {'state':'held_authenticated_source_unavailable','selectedObjects':ordinal,'inputManifestSha256':input_manifest_sha256}
 except (ValueError,TypeError,KeyError,UnicodeError,OverflowError,AttributeError):raise FrameHeld('selected_frame_unavailable') from None
