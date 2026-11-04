"""Inactive linkage proposal; all trust inputs must come from a future verified loader."""
import hashlib
import re
import unicodedata
from importlib.metadata import version
from . import reference_protocol as p
from .large_paged_manifest_candidate import validate as validate_pages

INVENTORY_DOMAIN=b'ltds-source-inventory-v1\0'
MAPPING_DOMAIN=b'ltds-paged-inventory-mapping-v1\0'
LINK_DOMAIN=b'ltds-paged-inventory-link-v1\0'
MAX_INVENTORY_BYTES=12*1024*1024
MAX_MAPPING_BYTES=4*1024*1024

class InventoryLinkHeld(ValueError):pass

def _preflight_inventory(inventory):
 p._closed(inventory,('protocol','schemaVersion','dataset','binding','processing','retained','generated'),'inventory')
 if inventory['protocol']!='ltds-source-inventory-v1' or type(inventory['schemaVersion']) is not int or inventory['schemaVersion']!=1:raise ValueError()
 p._closed(inventory['dataset'],('id','manifestSha256'),'dataset');p._slug(inventory['dataset']['id'],'dataset ID');p._hash(inventory['dataset']['manifestSha256'],'dataset hash')
 p._closed(inventory['binding'],('viewerTaskId','viewerAttemptId','pipelineDigest'),'binding')
 p._uuid(inventory['binding']['viewerTaskId'],'task');p._uuid(inventory['binding']['viewerAttemptId'],'attempt');p._hash(inventory['binding']['pipelineDigest'],'pipeline')
 if type(inventory['generated']) is not list or inventory['generated']:raise ValueError()
 processing,retained=inventory['processing'],inventory['retained']
 if type(processing) is not list or type(retained) is not list or not processing or len(processing)+len(retained)>20000:raise ValueError()
 estimate=2048
 for obj in (*processing,*retained):
  p._closed(obj,('datasetFileId','relativePath','byteSize','sha256','processingRole','contentType'),'source')
  p._slug(obj['datasetFileId'],'dataset file ID');p._integer(obj['byteSize'],'file byteSize',allow_zero=True);p._hash(obj['sha256'],'source digest')
  path=obj['relativePath'];role=obj['processingRole'];mime=obj['contentType']
  if type(path) is not str or not path or len(path)>4096 or len(path.encode('utf8'))>4096 or path.startswith('/') or '\\' in path or re.match(r'^[A-Za-z]:',path) or any(unicodedata.category(ch).startswith('C') for ch in path):raise ValueError()
  if any(not part or part in ('.','..') or len(part.encode('utf8'))>255 for part in path.split('/')):raise ValueError()
  if type(role) is not str or not re.fullmatch(r'[a-z][a-z0-9_-]{0,79}',role):raise ValueError()
  if type(mime) is not str or len(mime)>127 or not re.fullmatch(r'[A-Za-z0-9!#$&^_.+-]+/[A-Za-z0-9!#$&^_.+-]+',mime):raise ValueError()
  # Each row is field-bounded before this small per-row serialization.
  estimate+=len(p.canonical_json(obj))+1
  if estimate>MAX_INVENTORY_BYTES:raise ValueError()

def inventory_digest(inventory):
 try:
  _preflight_inventory(inventory)
  import rfc8785
  if version('rfc8785')!='0.1.4':raise ValueError()
  wire=rfc8785.dumps(inventory)
  if len(wire)>MAX_INVENTORY_BYTES:raise ValueError()
  return hashlib.sha256(INVENTORY_DOMAIN+wire).hexdigest()
 except (ImportError,ValueError,TypeError,OverflowError):raise InventoryLinkHeld('inventory_digest_unavailable') from None

def mapping_digest(mapping):
 try:
  if type(mapping) is not list or not 0<len(mapping)<=25000:raise ValueError()
  estimate=2
  for item in mapping:
   p._closed(item,('pageObjectId','datasetFileId'),'mapping')
   p._slug(item['pageObjectId'],'page ID');p._slug(item['datasetFileId'],'dataset ID')
   estimate+=len(p.canonical_json(item))+1
   if estimate>MAX_MAPPING_BYTES:raise ValueError()
  wire=p.canonical_json(mapping)
  if len(wire)>MAX_MAPPING_BYTES:raise ValueError()
  return hashlib.sha256(MAPPING_DOMAIN+wire).hexdigest()
 except (ValueError,TypeError,KeyError):raise InventoryLinkHeld('invalid_mapping') from None

def binding_digest(binding):
 return hashlib.sha256(LINK_DOMAIN+p.canonical_json(binding)).hexdigest()

def link(closure,pages,*,inventory,mapping,trusted_binding,expected_link_sha256):
 """Check agreement only; inputs are NOT authenticated by this function.

 Caller must obtain an independently authenticated immutable, fully validated
 inventory and binding; result stays held. All decoded inputs must be owned
 exclusively throughout this call. Generated GCP and substitute handling hold.
 """
 try:
  p._hash(expected_link_sha256,'pinned link digest')
  p._closed(trusted_binding,('datasetId','datasetManifestSha256','inventorySha256','mappingSha256','closureSha256','principalId','environment','authorityId','viewerTaskId','viewerAttemptId','pipelineDigest','revocationEpoch'),'trusted binding')
  for key in ('datasetId','principalId','authorityId'):p._slug(trusted_binding[key],key)
  for key in ('datasetManifestSha256','inventorySha256','mappingSha256','closureSha256','pipelineDigest'):p._hash(trusted_binding[key],key)
  p._environment(trusted_binding['environment']);p._uuid(trusted_binding['viewerTaskId'],'task');p._uuid(trusted_binding['viewerAttemptId'],'attempt');p._integer(trusted_binding['revocationEpoch'],'epoch',allow_zero=True)
  if binding_digest(trusted_binding)!=expected_link_sha256:raise ValueError()
  p._closed(inventory,('protocol','schemaVersion','dataset','binding','processing','retained','generated'),'inventory')
  if inventory['protocol']!='ltds-source-inventory-v1' or type(inventory['schemaVersion']) is not int or inventory['schemaVersion']!=1:raise ValueError()
  p._closed(inventory['dataset'],('id','manifestSha256'),'dataset');p._closed(inventory['binding'],('viewerTaskId','viewerAttemptId','pipelineDigest'),'binding')
  if inventory['dataset']!={'id':trusted_binding['datasetId'],'manifestSha256':trusted_binding['datasetManifestSha256']}:raise ValueError()
  if inventory['binding']!={k:trusted_binding[k] for k in ('viewerTaskId','viewerAttemptId','pipelineDigest')}:raise ValueError()
  if inventory_digest(inventory)!=trusted_binding['inventorySha256'] or mapping_digest(mapping)!=trusted_binding['mappingSha256']:raise ValueError()
  # GCP bytes, selected substitutions and custody evidence need their own
  # authenticated immutable mapping. Never infer generated semantics here.
  if inventory['generated']!=[] or type(inventory['retained']) is not list:raise ValueError()
  processed=inventory['processing']
  if type(processed) is not list or len(processed)!=len(mapping):raise ValueError()
  by_id=validate_pages(closure,pages,expected_closure_sha256=trusted_binding['closureSha256'])['byId']
  ordered=[obj for page in pages for obj in page['body']['objects']]
  if len(ordered)!=len(mapping) or len(by_id)!=len(mapping):raise ValueError()
  seen=set();previous=None
  for page_obj,original,pair in zip(ordered,processed,mapping):
   p._closed(original,('datasetFileId','relativePath','byteSize','sha256','processingRole','contentType'),'source')
   p._slug(original['datasetFileId'],'dataset file ID')
   if previous is not None and original['datasetFileId']<=previous:raise ValueError()
   previous=original['datasetFileId']
   if original['datasetFileId'] in seen or pair!={'pageObjectId':page_obj['id'],'datasetFileId':original['datasetFileId']}:raise ValueError()
   seen.add(original['datasetFileId'])
   path=original['relativePath']
   if type(path) is not str or not path or '\\' in path or path.startswith('/') or any(x in ('','.','..') for x in path.split('/')):raise ValueError()
   role=original['processingRole'];kind={'image':'image','provider_input':'auxiliary'}.get(role)
   if kind is None or (page_obj['name'],page_obj['byteSize'],page_obj['sha256'],page_obj['kind'],page_obj['mediaType'])!=(path.rsplit('/',1)[-1],original['byteSize'],original['sha256'],kind,original['contentType']):raise ValueError()
  return {'state':'held_source_authority_unverified','objects':len(ordered),'inventorySha256':trusted_binding['inventorySha256'],'closureSha256':trusted_binding['closureSha256']}
 except (ValueError,TypeError,KeyError,OverflowError):raise InventoryLinkHeld('inventory_link_unavailable') from None
