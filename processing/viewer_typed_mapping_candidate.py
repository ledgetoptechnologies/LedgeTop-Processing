"""Inactive typed Viewer-551 mapping; no source authentication or activation."""
import hashlib
import re
import unicodedata
from . import reference_protocol as p
from .large_paged_manifest_candidate import validate as validate_pages

VERSION='ltds-viewer-typed-mapping-v1'
DOMAIN=b'ltds-viewer-typed-mapping-v1\0'
MAX_ITEMS=10000
MAX_METADATA=8*1024*1024
PHOTO=re.compile(r'\.(?:jpe?g|png|tiff?|dng|raw|heic)$',re.I)
class TypedMappingHeld(ValueError):pass

def _safe_path(value):
 if type(value) is not str or not value or len(value)>4096 or value.startswith('/') or '\\' in value or any(x in ('','.','..') for x in value.split('/')) or re.match(r'^[A-Za-z]:',value) or any(unicodedata.category(c).startswith('C') for c in value):raise ValueError()
 if len(value.encode('utf8'))>4096 or any(len(x.encode('utf8'))>255 for x in value.split('/')):raise ValueError()

def _small_digest(domain,value,limit=MAX_METADATA):
 raw=p.canonical_json(value)
 if len(raw)>limit:raise ValueError()
 return hashlib.sha256(domain+raw).hexdigest()

def mapping_digest(mapping):
 try:
  if type(mapping) is not list or not 0<len(mapping)<=MAX_ITEMS:raise ValueError()
  total=2
  for item in mapping:
   if type(item) is not dict:raise ValueError()
   if item.get('type')=='dataset_file':
    p._closed(item,('type','pageObjectId','datasetFileId','relativePath','processingRole','mediaType'),'dataset mapping');p._slug(item['datasetFileId'],'dataset file ID');_safe_path(item['relativePath']);p._string(item['processingRole'],'role',80)
   elif item.get('type')=='attempt_generated_gcp':
    p._closed(item,('type','pageObjectId','attemptId','gcpEvidenceSha256','mediaType'),'generated mapping');p._uuid(item['attemptId'],'attempt');p._hash(item['gcpEvidenceSha256'],'GCP evidence')
   else:raise ValueError()
   p._slug(item['pageObjectId'],'page ID');p._string(item['mediaType'],'media type',127)
   total+=len(p.canonical_json(item))+1
   if total>MAX_METADATA:raise ValueError()
  return _small_digest(DOMAIN,mapping)
 except (ValueError,TypeError,KeyError,UnicodeError):raise TypedMappingHeld('typed_mapping_unavailable') from None

def link(closure,pages,*,snapshot,dataset_mapping,mapping,gcp_evidence,generated_gcp_bytes,trusted_binding,expected_binding_sha256):
 """Inputs are independent trusted snapshots, not authenticated here; always held."""
 try:
  p._closed(trusted_binding,('datasetId','datasetManifestSha256','viewerTaskId','viewerAttemptId','pipelineDigest','principalId','environment','authorityId','revocationEpoch','sourceSnapshotSha256','datasetMappingSha256','mappingSha256','gcpEvidenceSha256','closureSha256'),'binding')
  for k in ('datasetId','principalId','authorityId'):p._slug(trusted_binding[k],k)
  for k in ('datasetManifestSha256','pipelineDigest','sourceSnapshotSha256','datasetMappingSha256','mappingSha256','closureSha256'):p._hash(trusted_binding[k],k)
  p._uuid(trusted_binding['viewerTaskId'],'task');p._uuid(trusted_binding['viewerAttemptId'],'attempt');p._environment(trusted_binding['environment']);p._integer(trusted_binding['revocationEpoch'],'epoch',allow_zero=True)
  p._hash(expected_binding_sha256,'binding pin')
  if _small_digest(b'ltds-viewer-typed-binding-v1\0',trusted_binding)!=expected_binding_sha256:raise ValueError()
  p._closed(snapshot,('sourceFiles','inputFiles'),'Viewer snapshot');source=snapshot['sourceFiles'];inputs=snapshot['inputFiles']
  if type(source) is not list or type(inputs) is not list or not 0<len(source)<=MAX_ITEMS or not 0<len(inputs)<=MAX_ITEMS:raise ValueError()
  # Bounded field preflight precedes whole snapshot serialization.
  budget=4096;paths=set()
  for item in source:
   p._closed(item,('relativePath','byteSize','sha256','processingRole'),'source');_safe_path(item['relativePath']);p._integer(item['byteSize'],'size',allow_zero=True);p._hash(item['sha256'],'hash');p._string(item['processingRole'],'role',80)
   if item['relativePath'] in paths:raise ValueError()
   paths.add(item['relativePath']);budget+=len(p.canonical_json(item))+1
   if budget>MAX_METADATA:raise ValueError()
  for item in inputs:
   p._closed(item,('relativePath','byteSize','sha256'),'input');_safe_path(item['relativePath']);p._integer(item['byteSize'],'size',allow_zero=True);p._hash(item['sha256'],'hash');budget+=len(p.canonical_json(item))+1
   if budget>MAX_METADATA:raise ValueError()
  if type(dataset_mapping) is not list or len(dataset_mapping)!=len(source):raise ValueError()
  dataset_by_path={};dataset_ids=set();dataset_budget=2
  for record in dataset_mapping:
   p._closed(record,('datasetFileId','relativePath','byteSize','sha256','processingRole'),'dataset source')
   p._slug(record['datasetFileId'],'dataset file ID');_safe_path(record['relativePath']);p._integer(record['byteSize'],'size',allow_zero=True);p._hash(record['sha256'],'hash');p._string(record['processingRole'],'role',80)
   if record['datasetFileId'] in dataset_ids or record['relativePath'] in dataset_by_path:raise ValueError()
   dataset_ids.add(record['datasetFileId']);dataset_by_path[record['relativePath']]=record
   dataset_budget+=len(p.canonical_json(record))+1
   if dataset_budget>MAX_METADATA:raise ValueError()
  if _small_digest(b'ltds-viewer-dataset-mapping-v1\0',dataset_mapping)!=trusted_binding['datasetMappingSha256']:raise ValueError()
  for item in source:
   record=dataset_by_path.get(item['relativePath'])
   if record is None or any(record[k]!=item[k] for k in ('relativePath','byteSize','sha256','processingRole')):raise ValueError()
  if _small_digest(b'ltds-viewer-typed-snapshot-v1\0',snapshot)!=trusted_binding['sourceSnapshotSha256'] or mapping_digest(mapping)!=trusted_binding['mappingSha256']:raise ValueError()
  selected=[x for x in source if x['processingRole'] not in ('gcp_source','administrative') and (x['processingRole']!='auto' or PHOTO.search(x['relativePath']))]
  if gcp_evidence is not None:
   p._closed(gcp_evidence,('attemptId','byteSize','sha256','sourcePaths','correspondenceSha256'),'GCP evidence');p._uuid(gcp_evidence['attemptId'],'attempt');p._integer(gcp_evidence['byteSize'],'GCP size');p._hash(gcp_evidence['sha256'],'GCP hash');p._hash(gcp_evidence['correspondenceSha256'],'correspondence hash')
   if type(gcp_evidence['sourcePaths']) is not list or not 0<len(gcp_evidence['sourcePaths'])<=64:raise ValueError()
   for path in gcp_evidence['sourcePaths']:_safe_path(path)
   if len(set(gcp_evidence['sourcePaths']))!=len(gcp_evidence['sourcePaths']):raise ValueError()
   if type(generated_gcp_bytes) is not bytes or len(generated_gcp_bytes)>16*1024*1024 or len(generated_gcp_bytes)!=gcp_evidence['byteSize'] or hashlib.sha256(generated_gcp_bytes).hexdigest()!=gcp_evidence['sha256']:raise ValueError()
   if gcp_evidence['attemptId']!=trusted_binding['viewerAttemptId'] or any(not any(x['relativePath']==path and x['processingRole']=='gcp_source' for x in source) for path in gcp_evidence['sourcePaths']):raise ValueError()
   p._hash(trusted_binding['gcpEvidenceSha256'],'GCP evidence pin')
   if _small_digest(b'ltds-viewer-gcp-evidence-v1\0',gcp_evidence)!=trusted_binding['gcpEvidenceSha256']:raise ValueError()
   selected=[x for x in selected if x['relativePath'].rsplit('/',1)[-1].lower()!='gcp_list.txt']
   selected.append(dict(relativePath='gcp_list.txt',byteSize=gcp_evidence['byteSize'],sha256=gcp_evidence['sha256'],processingRole='generated'))
  elif trusted_binding['gcpEvidenceSha256'] is not None or generated_gcp_bytes is not None:raise ValueError()
  selected=[x for x in selected if PHOTO.search(x['relativePath'])]+[x for x in selected if not PHOTO.search(x['relativePath'])]
  if [dict(relativePath=x['relativePath'],byteSize=x['byteSize'],sha256=x['sha256']) for x in selected]!=inputs:raise ValueError()
  by_id=validate_pages(closure,pages,expected_closure_sha256=trusted_binding['closureSha256'])['byId'];ordered=[o for page in pages for o in page['body']['objects']]
  if len(mapping)!=len(selected) or len(ordered)!=len(selected) or len(by_id)!=len(selected):raise ValueError()
  for source_item,page_obj,mapped in zip(selected,ordered,mapping):
   if page_obj['kind']!=('image' if PHOTO.search(source_item['relativePath']) else 'auxiliary'):raise ValueError()
   if mapped['pageObjectId']!=page_obj['id'] or (page_obj['name'],page_obj['byteSize'],page_obj['sha256'],page_obj['mediaType'])!=(source_item['relativePath'].rsplit('/',1)[-1],source_item['byteSize'],source_item['sha256'],mapped['mediaType']):raise ValueError()
   if source_item['processingRole']=='generated':
    if mapped['type']!='attempt_generated_gcp' or mapped['attemptId']!=trusted_binding['viewerAttemptId'] or mapped['gcpEvidenceSha256']!=trusted_binding['gcpEvidenceSha256']:raise ValueError()
   elif mapped['type']!='dataset_file' or mapped['datasetFileId']!=dataset_by_path[source_item['relativePath']]['datasetFileId'] or mapped['relativePath']!=source_item['relativePath'] or mapped['processingRole']!=source_item['processingRole']:raise ValueError()
  return {'state':'held_authenticated_snapshot_unavailable','objects':len(selected),'retainedSources':len(source)-len([x for x in selected if x['processingRole']!='generated'])}
 except (ValueError,TypeError,KeyError,UnicodeError):raise TypedMappingHeld('typed_link_unavailable') from None
