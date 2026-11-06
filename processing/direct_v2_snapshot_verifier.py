"""Verify Viewer's signed held-source pages; never grants custody or release."""
import hashlib
import json
import re
import unicodedata
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from . import reference_protocol as p

SNAPSHOT_DOMAIN=b'ltds-viewer-held-source-snapshot-v1\0'
PAGE_DOMAIN=b'ltds-viewer-source-frame-v1\0'
INTENT_FIELDS=frozenset(('protocol','snapshotId','issuerId','keyId','createdAt','datasetId',
    'datasetManifestSha256','taskId','attemptId','inputManifestSha256','sourceCount','datasetFileCount',
    'sourcePageHashes','sourceFrameBytes','selectedCount','selectedPageHashes','selectedFrameBytes','gcp'))
FILE_FIELDS=frozenset(('type','datasetFileId','relativePath','byteSize','sha256','processingRole','mediaType','kind'))

def _decode(raw, limit):
    if type(raw) is not bytes or not 0<len(raw)<=limit:raise ValueError('bounded canonical bytes required')
    def pairs(items):
        result={}
        for key,value in items:
            if key in result:raise ValueError('duplicate key')
            result[key]=value
        return result
    def invalid(_):raise ValueError('non-integer')
    try:
        value=json.loads(raw.decode('utf-8'),object_pairs_hook=pairs,parse_float=invalid,parse_constant=invalid)
        def check(v,depth=0):
            if depth>24:raise ValueError('depth')
            if type(v) is dict:
                for k,x in v.items():
                    if type(k) is not str or not re.fullmatch(r'[\x20-\x7e]+',k):raise ValueError('key')
                    check(x,depth+1)
            elif type(v) is list:
                for x in v:check(x,depth+1)
            elif type(v) is int:
                if not 0<=v<=2**53-1:raise ValueError('integer')
            elif v is not None and type(v) not in (str,bool):raise ValueError('type')
        check(value)
        if p.canonical_json(value)!=raw:raise ValueError('noncanonical')
        return value
    except (ValueError,TypeError,UnicodeError,RecursionError):raise ValueError('invalid signed snapshot') from None

def verify_snapshot(*, intent_bytes, signature, source_frames, public_key, issuer_id, key_id,
                    snapshot_sha, source_manifest, derived_manifest, task_id, pipeline_sha):
    """Verify preimage and exact source objects against independently loaded pins.

    Does not prove mount/custody, Viewer snapshot status, source release, or a
    portable original-path inventory digest. It intentionally returns no paths.
    """
    if not isinstance(public_key,Ed25519PublicKey) or type(signature) is not bytes or len(signature)!=64:
        raise ValueError('snapshot signer unavailable')
    intent=_decode(intent_bytes,1024*1024)
    if type(intent) is not dict or set(intent)!=INTENT_FIELDS or intent['protocol']!='ltds-viewer-held-source-snapshot-v1':
        raise ValueError('snapshot identity invalid')
    if any(type(intent[k]) is not int for k in ('sourceFrameBytes','sourceCount','datasetFileCount','selectedCount','selectedFrameBytes')):
        raise ValueError('snapshot count invalid')
    if (hashlib.sha256(intent_bytes).hexdigest()!=snapshot_sha or intent['issuerId']!=issuer_id
            or intent['keyId']!=key_id or intent['datasetId']!=source_manifest['id']
            or intent['datasetManifestSha256']!=source_manifest['datasetManifestSha256']
            or intent['taskId']!=task_id or intent['attemptId']!=derived_manifest['attemptId']
            or intent['inputManifestSha256']!=pipeline_sha):raise ValueError('snapshot binding changed')
    try:public_key.verify(signature,SNAPSHOT_DOMAIN+intent_bytes)
    except InvalidSignature:raise ValueError('snapshot signature invalid') from None
    if (type(source_frames) is not bytes or not 0<len(source_frames)<=12*1024*1024
            or not source_frames.endswith(b'\n') or intent['sourceFrameBytes']!=len(source_frames)):
        raise ValueError('source pages unavailable')
    lines=source_frames[:-1].split(b'\n'); page_hashes=intent['sourcePageHashes']
    if type(page_hashes) is not list or len(lines)!=len(page_hashes) or not 1<=len(lines)<=64:
        raise ValueError('source page count changed')
    objects={o['id']:o for o in source_manifest['objects']}; seen=set(); total=0;generated=[]
    for index,line in enumerate(lines):
        page=_decode(line,256*1024)
        if (type(page) is not dict or set(page)!={'version','index','records'}
                or page['version']!='ltds-viewer-source-frames-v1' or type(page['index']) is not int
                or page['index']!=index or type(page['records']) is not list or not 1<=len(page['records'])<=512
                or hashlib.sha256(PAGE_DOMAIN+line).hexdigest()!=page_hashes[index]):
            raise ValueError('source page changed')
        for record in page['records']:
            total+=1
            if total>20000 or type(record) is not dict:raise ValueError('source records invalid')
            if record.get('type')=='attempt_generated_gcp':generated.append(record);continue
            if set(record)!=FILE_FIELDS or record.get('type')!='dataset_file':raise ValueError('source record invalid')
            name=record['relativePath']; identifier=record['datasetFileId']
            if (type(name) is not str or not name or len(name.encode('utf-8'))>4096 or name.startswith('/')
                    or '\\' in name or any(unicodedata.category(c).startswith('C') for c in name)
                    or any(x in ('','.','..') or len(x.encode('utf-8'))>255 for x in name.split('/'))
                    or identifier in seen or identifier not in objects):raise ValueError('source path invalid')
            obj=objects[identifier]
            if (name.rsplit('/',1)[-1]!=obj['name'] or type(record['byteSize']) is not int
                    or record['byteSize']!=obj['byteSize'] or record['sha256']!=obj['sha256']
                    or record['processingRole'] not in ('auto','image','gcp_source','provider_input','administrative')
                    or (record['processingRole']=='gcp_source' and
                        (obj['sourceType']!='gcp_source_csv' or obj['retentionClass']!='permanent_viewer_hold'))):
                raise ValueError('source object changed')
            seen.add(identifier)
    if (seen!=set(objects) or intent['datasetFileCount']!=len(objects) or intent['sourceCount']!=total
            or len(generated)!=len(derived_manifest['objects'])):raise ValueError('source closure incomplete')
    if generated:
        obj=derived_manifest['objects'][0]; record=generated[0];gcp=intent['gcp']
        if (type(gcp) is not dict or type(gcp.get('generatedGcp')) is not dict
                or record.get('attemptId')!=obj['snapshotAttemptId'] or record.get('byteSize')!=obj['byteSize']
                or record.get('sha256')!=obj['sha256'] or gcp['generatedGcp'].get('sha256')!=obj['sha256']
                or gcp['generatedGcp'].get('byteSize')!=obj['byteSize']):raise ValueError('generated source changed')
    elif intent['gcp'] is not None:raise ValueError('unexpected GCP snapshot')
    return {'state':'source_snapshot_verified_not_custody','sourceSnapshotSha256':snapshot_sha,
            'datasetFileCount':len(seen),'sourceCustodyAccepted':False,'processingAuthorized':False}
