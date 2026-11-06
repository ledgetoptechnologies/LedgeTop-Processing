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
                    snapshot_sha, source_manifest, derived_manifest, task_id, pipeline_sha,
                    selected_frames=None, pipeline_manifest=None, gcp_evidence_frames=None, gcp_file_frames=None):
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
    derived_objects=derived_manifest.get('objects')
    if type(derived_objects) is not list:
        raise ValueError('derived manifest invalid')
    generated=intent['gcp'].get('generatedGcp') if type(intent['gcp']) is dict else None
    if generated is None:
        if derived_objects:raise ValueError('unbound derived objects')
    else:
        expected_derived={'id':f"gcp_{intent['attemptId']}",'name':'gcp_list.txt','byteSize':generated.get('byteSize'),
            'sha256':generated.get('sha256'),'snapshotAttemptId':intent['attemptId'],'snapshotContentSha256':generated.get('sha256'),
            'snapshotEvidenceSha256':intent['gcp'].get('evidenceSha256'),'sourceType':'attempt_derived_gcp_list',
            'retentionClass':'release_after_acceptance','retentionHoldId':None}
        if derived_objects!=[expected_derived]:raise ValueError('GCP derived manifest changed')
    if (hashlib.sha256(intent_bytes).hexdigest()!=snapshot_sha or intent['issuerId']!=issuer_id
            or intent['keyId']!=key_id or intent['datasetId']!=source_manifest['id']
            or intent['datasetManifestSha256']!=source_manifest['datasetManifestSha256']
            or intent['taskId']!=task_id or intent['attemptId']!=derived_manifest['attemptId']):raise ValueError('snapshot binding changed')
    try:public_key.verify(signature,SNAPSHOT_DOMAIN+intent_bytes)
    except InvalidSignature:raise ValueError('snapshot signature invalid') from None
    if (type(source_frames) is not bytes or not 0<len(source_frames)<=12*1024*1024
            or not source_frames.endswith(b'\n') or intent['sourceFrameBytes']!=len(source_frames)):
        raise ValueError('source pages unavailable')
    lines=source_frames[:-1].split(b'\n'); page_hashes=intent['sourcePageHashes']
    if type(page_hashes) is not list or len(lines)!=len(page_hashes) or not 1<=len(lines)<=64:
        raise ValueError('source page count changed')
    objects={o['id']:o for o in source_manifest['objects']}; seen=set(); paths=set(); basenames=set(); total=0
    source_records=[]
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
            if record.get('type')=='attempt_generated_gcp':
                generated=intent['gcp'].get('generatedGcp') if type(intent['gcp']) is dict else None
                expected={'type':'attempt_generated_gcp',**generated} if type(generated) is dict else None
                if record!=expected or record in [r for r in source_records if r.get('type')=='attempt_generated_gcp']:
                    raise ValueError('generated GCP source record changed')
                source_records.append(record)
                continue
            if set(record)!=FILE_FIELDS or record.get('type')!='dataset_file':raise ValueError('source record invalid')
            name=record['relativePath']; identifier=record['datasetFileId']
            if (type(name) is not str or not name or len(name.encode('utf-8'))>4096 or name.startswith('/')
                    or '\\' in name or any(unicodedata.category(c).startswith('C') for c in name)
                    or any(x in ('','.','..') or len(x.encode('utf-8'))>255 for x in name.split('/'))
                    or re.match(r'^[A-Za-z]:',name) or name in paths
                    or identifier in seen or identifier not in objects):raise ValueError('source path invalid')
            obj=objects[identifier]
            basename=name.rsplit('/',1)[-1].lower()
            if basename in basenames:raise ValueError('source basename duplicated')
            extension=name.rsplit('.',1)[-1].lower() if '.' in name.rsplit('/',1)[-1] else ''
            kind,media=record['kind'],record['mediaType']
            if extension in ('jpg','jpeg','png','tif','tiff','dng','raw','heic'):
                valid_media=kind=='image' and media in ('image/jpeg','image/png','image/tiff')
            elif extension in ('las','laz'):
                valid_media=kind=='lidar' and media in ('application/vnd.las','application/vnd.laz')
            else:
                valid_media=kind=='auxiliary' and media in ('application/octet-stream','application/json','text/plain','text/csv')
            if (name.rsplit('/',1)[-1]!=obj['name'] or type(record['byteSize']) is not int
                    or record['byteSize']<=0 or record['byteSize']!=obj['byteSize'] or record['sha256']!=obj['sha256'] or not valid_media
                    or record['processingRole'] not in ('auto','image','gcp_source','provider_input','administrative')
                    or (record['processingRole']=='gcp_source' and
                        (obj['sourceType']!='gcp_source_csv' or obj['retentionClass']!='permanent_viewer_hold' or obj.get('retentionHoldId')!=identifier))
                    or (obj['sourceType']=='gcp_source_csv' and record['processingRole']!='gcp_source')):
                raise ValueError('source object changed')
            seen.add(identifier);paths.add(name);basenames.add(basename);source_records.append(record)
    gcp_source_ids={record['datasetFileId'] for record in source_records if record.get('type')=='dataset_file' and record.get('processingRole')=='gcp_source'}
    generated_records=[record for record in source_records if record.get('type')=='attempt_generated_gcp']
    if gcp_source_ids and not intent['gcp']:raise ValueError('GCP snapshot required for retained GCP source')
    if intent['gcp'] and (not gcp_source_ids or len(generated_records)!=1):raise ValueError('GCP snapshot source binding incomplete')
    if bool(intent['gcp'])!=bool(generated_records):raise ValueError('GCP generated source record mismatch')
    if seen!=set(objects) or intent['datasetFileCount']!=len(objects) or intent['sourceCount']!=total:
        raise ValueError('source closure incomplete')
    result={'state':'source_pages_verified_not_custody','sourceSnapshotSha256':snapshot_sha,
            'datasetFileCount':len(seen),'sourceCustodyAccepted':False,'processingAuthorized':False}
    selected=None
    if selected_frames is not None:
        if type(pipeline_manifest) is not dict or pipeline_manifest.get('sha256')!=pipeline_sha or p.manifest_sha256(pipeline_manifest)!=pipeline_sha:
            raise ValueError('pipeline manifest changed')
        from .direct_selected_gcp_frames import verify_selected
        selected=verify_selected(intent,selected_frames,source_records=source_records,pipeline_manifest=pipeline_manifest)
        result['state']='source_and_selected_pages_verified_not_custody'
        result['selectedCount']=selected['selectedCount']
    elif pipeline_manifest is not None:raise ValueError('selected pages unavailable')
    if intent['gcp'] is not None:
        if selected is None:raise ValueError('GCP selected pages unavailable')
        from .direct_selected_gcp_frames import _verify_gcp_chunks_payload,verify_gcp_semantics
        gcp=intent['gcp'];required_gcp={'evidenceVersion','evidencePageHashes','evidencePageBytes','evidencePageCount',
            'evidenceByteCount','evidenceSha256','fileVersion','filePageHashes','filePageBytes','filePageCount',
            'fileByteCount','fileSha256','generatedGcp'}
        if type(gcp) is not dict or set(gcp)!=required_gcp:raise ValueError('GCP descriptor invalid')
        attempt_id=intent['attemptId']
        if (gcp['evidenceVersion'] not in ('ltds-viewer-gcp-evidence-chunks-v1','ltds-viewer-gcp-evidence-chunks-v2')
                or gcp['fileVersion']!='ltds-viewer-generated-gcp-file-chunks-v1'
                or type(gcp['evidencePageCount']) is not int or gcp['evidencePageCount']!=len(gcp['evidencePageHashes'])
                or type(gcp['filePageCount']) is not int or gcp['filePageCount']!=len(gcp['filePageHashes'])):
            raise ValueError('GCP descriptor invalid')
        evidence_bytes=_verify_gcp_chunks_payload(gcp_evidence_frames,hashes=gcp['evidencePageHashes'],page_bytes=gcp['evidencePageBytes'],
            stream_bytes=len(gcp_evidence_frames) if type(gcp_evidence_frames) is bytes else -1,byte_count=gcp['evidenceByteCount'],
            sha256=gcp['evidenceSha256'],attempt_id=attempt_id,kind='evidence',version=gcp['evidenceVersion'])
        file_bytes=_verify_gcp_chunks_payload(gcp_file_frames,hashes=gcp['filePageHashes'],page_bytes=gcp['filePageBytes'],
            stream_bytes=len(gcp_file_frames) if type(gcp_file_frames) is bytes else -1,byte_count=gcp['fileByteCount'],
            sha256=gcp['fileSha256'],attempt_id=attempt_id,kind='file',evidence_sha256=gcp['evidenceSha256'],version=gcp['fileVersion'])
        semantic=verify_gcp_semantics(evidence_bytes=evidence_bytes,file_bytes=file_bytes,
            generated_gcp=gcp['generatedGcp'],source_records=source_records,attempt_id=attempt_id,
            selected_image_ids=selected['selectedImageIds'])
        result['gcpState']=semantic['state'];result['gcpCorrespondenceCount']=semantic['correspondenceCount']
        result['gcpSourceRetentionDeclarationsVerified']=True
        result['state']='source_selected_gcp_evidence_and_file_verified_not_custody'
    elif gcp_evidence_frames is not None or gcp_file_frames is not None:
        raise ValueError('unbound GCP streams')
    return result
