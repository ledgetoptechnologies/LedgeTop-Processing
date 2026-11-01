"""Unwired candidate native profile for the reference-v1 input manifest.

Validation is not a receipt, capacity reservation, transfer, or authority.
"""
import copy
from pathlib import Path

from . import reference_protocol as ref
from .viewer_tasks import TaskStore

PROFILE='native-odm-input-v1-candidate'
MAX_INPUT_BYTES=20*1024**3
MEDIA={'.jpg':('image','image/jpeg'),'.jpeg':('image','image/jpeg'),
       '.png':('image','image/png'),'.tif':('image','image/tiff'),
       '.tiff':('image','image/tiff'),'.txt':('auxiliary','text/plain'),
       '.json':('auxiliary','application/json'),'.csv':('auxiliary','text/csv')}

def validate(body, *, expected_manifest_id, max_input_bytes):
    """Validate canonical bytes and an expected opaque attempt identifier.

    The caller must separately and durably bind task UUID, coordinator-issued
    generation, environment and capability. This function authenticates none of
    them. Object IDs are one-based ordinals in exact transmission order.
    """
    ref._slug(expected_manifest_id,'manifest.id')
    ref._integer(max_input_bytes,'input byte limit')
    ref._require(max_input_bytes<=MAX_INPUT_BYTES,'native profile exceeds 20 GiB ceiling')
    ref._require(type(body) is bytes,'manifest must be canonical UTF-8 bytes')
    manifest=ref.decode_submission(body)
    ref._require(ref.canonical_json(manifest)==body,'manifest bytes are not canonical')
    ref._closed(manifest,('id','revision','sha256','totalBytes','objectCount','inputCounts','objects'),'manifest')
    ref._require(manifest['id']==expected_manifest_id and type(manifest['revision']) is int and manifest['revision']==1,'manifest identity mismatch')
    ref._hash(manifest['sha256'],'manifest.sha256');ref._integer(manifest['totalBytes'],'manifest.totalBytes')
    ref._require(manifest['totalBytes']<=max_input_bytes,'input byte limit exceeded')
    objects=manifest['objects'];ref._require(type(objects) is list and 0<len(objects)<=ref.MAX_OBJECTS,'invalid object count')
    ref._integer(manifest['objectCount'],'manifest.objectCount');ref._require(manifest['objectCount']==len(objects),'object count mismatch')
    counts=manifest['inputCounts'];ref._closed(counts,('images','lidarObjects','points'),'inputCounts')
    for name in counts:ref._integer(counts[name],name,allow_zero=True)
    ref._require(counts['lidarObjects']==0 and counts['points']==0,'native ODM profile excludes lidar')
    names=set();total=0;images=0;aux_seen=False
    for index,obj in enumerate(objects):
        ref._closed(obj,('id','name','byteSize','sha256','kind','mediaType'),'object')
        ref._require(obj['id']==f'o{index+1:06d}','object ID/order mismatch')
        ref._basename(obj['name'])
        try:key=TaskStore._filename(obj['name'])
        except (TypeError,ValueError):raise ref.ProtocolError('unsupported native filename') from None
        ref._require(key not in names,'duplicate basename');names.add(key)
        ref._integer(obj['byteSize'],'object.byteSize');ref._hash(obj['sha256'],'object.sha256')
        expected=MEDIA.get(Path(obj['name']).suffix.lower());ref._require(expected is not None and (obj['kind'],obj['mediaType'])==expected,'native kind/media mismatch')
        if obj['kind']=='image':
            ref._require(not aux_seen,'images must precede auxiliary objects');images+=1
        else:aux_seen=True
        total+=obj['byteSize'];ref._require(total<=ref.MAX_SAFE_INTEGER and total<=max_input_bytes,'input byte limit exceeded')
    ref._require(images>0 and counts['images']==images and total==manifest['totalBytes'],'manifest counts mismatch')
    ref._require(ref.manifest_sha256(manifest)==manifest['sha256'],'manifest digest mismatch')
    return copy.deepcopy(manifest)
