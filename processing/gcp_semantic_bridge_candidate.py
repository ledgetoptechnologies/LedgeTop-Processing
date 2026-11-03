"""Held semantic bridge. Inputs require independent authentication and bounded decoding."""
import hashlib
from . import reference_protocol as p
from .gcp_snapshot_semantics_candidate import validate_gcp_semantics
from .viewer_paged_frame_candidate import _item

class SemanticHeld(ValueError): pass

def inspect(*, attempt_id, evidence, evidence_bytes, evidence_sha256, generated_bytes, source_records, selected_records):
 """Compare supplied evidence and inventory; never authorize or accept a snapshot.

 Evidence and source/selection MUST originate from independently authenticated,
 bounded immutable loaders in the same transaction. This helper has no loader,
 stream signature, JS correspondence hash or raw file content verification.
 """
 try:
  p._uuid(attempt_id,'attempt');p._hash(evidence_sha256,'evidence digest')
  if type(evidence_bytes) is not bytes or not 0<len(evidence_bytes)<=16*1024*1024 or hashlib.sha256(evidence_bytes).hexdigest()!=evidence_sha256:raise ValueError()
  from .source_inventory_candidate import _parse
  parsed=_parse(evidence_bytes)
  if parsed!=evidence:raise ValueError()
  p._closed(evidence,('schemaVersion','snapshot','correspondences'),'evidence')
  if type(evidence['schemaVersion']) is not int or evidence['schemaVersion']!=1:raise ValueError()
  snapshot=evidence['snapshot']; rows=evidence['correspondences']
  p._closed(snapshot,('snapshotId','byteSize','sha256','sourceGcpDatasetFileIds','imageDatasetFileIds','correspondenceCount'),'snapshot')
  if snapshot['snapshotId']!=attempt_id or type(generated_bytes) is not bytes or not 0<len(generated_bytes)<=16*1024*1024 or type(snapshot['byteSize']) is not int or snapshot['byteSize']!=len(generated_bytes) or hashlib.sha256(generated_bytes).hexdigest()!=snapshot['sha256']:raise ValueError()
  if type(source_records) is not list or not 0<len(source_records)<=20001 or type(selected_records) is not list or not 0<len(selected_records)<=20001:raise ValueError()
  sources={}; generated=[]
  for obj in source_records:
   _item(obj)
   if obj['type']=='dataset_file':
    if obj['datasetFileId'] in sources:raise ValueError()
    sources[obj['datasetFileId']]=obj
   else:generated.append(obj)
  if len(generated)!=1 or generated[0]['attemptId']!=attempt_id or generated[0]['byteSize']!=len(generated_bytes) or generated[0]['sha256']!=snapshot['sha256']:raise ValueError()
  selected_ids=[]; selected_gcp=0
  for ordinal,obj in enumerate(selected_records):
   if type(obj) is not dict or type(obj.get('ordinal')) is not int or obj['ordinal']!=ordinal:raise ValueError()
   if obj.get('type')=='dataset_file':
    p._closed(obj,('type','ordinal','pageObjectId','datasetFileId'),'selected');sid=obj['datasetFileId']
    if sid not in sources or sid in selected_ids:raise ValueError()
    selected_ids.append(sid)
   elif obj.get('type')=='attempt_generated_gcp':
    p._closed(obj,('type','ordinal','pageObjectId','attemptId','mediaType'),'selected gcp')
    if obj['attemptId']!=attempt_id or obj['mediaType']!='text/plain':raise ValueError()
    selected_gcp+=1
   else:raise ValueError()
  if selected_gcp!=1:raise ValueError()
  if type(rows) is not list or not rows or len(rows)>100000 or type(snapshot['correspondenceCount']) is not int or len(rows)!=snapshot['correspondenceCount']:raise ValueError()
  reduced=[]; raw_refs={}; image_ids=set()
  for row in rows:
   p._closed(row,('correspondenceId','gcpPointId','gcpSetId','externalId','label','latitude','longitude','elevationM','sourceGcpDatasetFileId','sourceGcpSha256','sourceGcpRelativePath','imageDatasetFileId','imageRelativePath','imageWidth','imageHeight','pixelX','pixelY'),'correspondence')
   src=sources.get(row['sourceGcpDatasetFileId']); img=sources.get(row['imageDatasetFileId'])
   if not src or src['processingRole']!='gcp_source' or src['relativePath']!=row['sourceGcpRelativePath'] or src['sha256']!=row['sourceGcpSha256'] or not img or img['kind']!='image' or img['relativePath']!=row['imageRelativePath'] or img['datasetFileId'] not in selected_ids:raise ValueError()
   raw_refs[src['datasetFileId']]={'datasetFileId':src['datasetFileId'],'relativePath':src['relativePath'],'sha256':src['sha256']};image_ids.add(img['datasetFileId'])
   reduced.append(dict(sourceGcpDatasetFileId=src['datasetFileId'],imageDatasetFileId=img['datasetFileId'],pointId=row['gcpPointId'],**{k:row[k] for k in ('longitude','latitude','elevationM','pixelX','pixelY')}))
  if sorted(raw_refs)!=snapshot['sourceGcpDatasetFileIds'] or sorted(image_ids)!=snapshot['imageDatasetFileIds'] or [raw_refs[x] for x in sorted(raw_refs)]!=generated[0]['sourceFiles']:raise ValueError()
  processing=[dict(datasetFileId=sources[x]['datasetFileId'],relativePath=sources[x]['relativePath'],byteSize=sources[x]['byteSize'],sha256=sources[x]['sha256'],processingRole='image' if sources[x]['kind']=='image' else 'provider_input',contentType=sources[x]['mediaType']) for x in selected_ids]
  validate_gcp_semantics(generated_bytes,processing=processing,correspondences=reduced)
  return {'state':'held_gcp_authority_correspondence_hash_unverified','correspondences':len(rows)}
 except (ValueError,TypeError,KeyError,UnicodeError,OverflowError,AttributeError):raise SemanticHeld('gcp_semantics_unavailable') from None
