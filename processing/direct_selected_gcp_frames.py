"""Unwired held-snapshot frame checks; caller authenticates signed intent.

These routines do not validate GCP coordinate semantics, grant custody, or
allow release. They must not be used with unverified intent or manifests.
"""
import base64
import hashlib
import re
from .direct_v2_snapshot_verifier import _decode

SELECTED_DOMAIN=b'ltds-viewer-selected-frame-v1\0'
EVIDENCE_DOMAIN=b'ltds-viewer-gcp-evidence-frame-v2\0'
FILE_DOMAIN=b'ltds-viewer-generated-gcp-file-frame-v1\0'


def _lines(raw, hashes, *, total_bytes, limit, page_limit):
    if (type(raw) is not bytes or type(total_bytes) is not int or total_bytes!=len(raw)
            or not raw.endswith(b'\n') or not 0<len(raw)<=limit or type(hashes) is not list
            or not 1<=len(hashes)<=page_limit):raise ValueError('frame stream invalid')
    lines=raw[:-1].split(b'\n')
    if len(lines)!=len(hashes) or any(type(x) is not str or not re.fullmatch('[a-f0-9]{64}',x) for x in hashes):
        raise ValueError('frame inventory invalid')
    return lines


def verify_selected(intent, frames, *, source_records, pipeline_manifest):
    """Validate exact ordered selected IDs against signed sources and pipeline."""
    lines=_lines(frames,intent['selectedPageHashes'],total_bytes=intent['selectedFrameBytes'],
                 limit=12*1024*1024,page_limit=64)
    sources={r['datasetFileId']:r for r in source_records if r.get('type')=='dataset_file'}
    if len(sources)!=sum(r.get('type')=='dataset_file' for r in source_records):raise ValueError('duplicate source')
    objects=pipeline_manifest['objects']; selected=[]
    for index,line in enumerate(lines):
        page=_decode(line,256*1024)
        if (type(page) is not dict or set(page)!={'version','index','records'}
                or page['version']!='ltds-viewer-selected-frames-v1' or type(page['index']) is not int
                or page['index']!=index or type(page['records']) is not list or not 1<=len(page['records'])<=512
                or hashlib.sha256(SELECTED_DOMAIN+line).hexdigest()!=intent['selectedPageHashes'][index]):
            raise ValueError('selected page invalid')
        for record in page['records']:
            if (type(record) is not dict or set(record)!={'type','ordinal','pageObjectId','datasetFileId'}
                    or record['type']!='dataset_file' or type(record['ordinal']) is not int
                    or record['ordinal']!=len(selected) or record['pageObjectId']!=record['datasetFileId']):
                raise ValueError('selected record invalid or unsupported GCP')
            selected.append(record['datasetFileId'])
            if len(selected)>20000:raise ValueError('selected count invalid')
    if (type(intent['selectedCount']) is not int or intent['selectedCount']!=len(selected)
            or len(selected)!=len(set(selected)) or len(selected)!=len(objects)):
        raise ValueError('selected closure invalid')
    for identifier,obj in zip(selected,objects):
        source=sources.get(identifier)
        if (source is None or source['processingRole'] in ('gcp_source','administrative')
                or identifier!=obj['id'] or source['relativePath'].rsplit('/',1)[-1]!=obj['name']
                or source['byteSize']!=obj['byteSize'] or source['sha256']!=obj['sha256']):
            raise ValueError('selected object mismatch')
    return {'state':'selected_pages_verified_not_custody','selectedCount':len(selected),
            'sourceCustodyAccepted':False,'processingAuthorized':False}


def verify_gcp_chunks(raw, *, hashes, page_bytes, stream_bytes, byte_count,
                      sha256, attempt_id, kind, evidence_sha256=None):
    """Check signed chunk bytes only; GCP semantic evidence remains held."""
    if kind=='evidence':version,domain='ltds-viewer-gcp-evidence-chunks-v2',EVIDENCE_DOMAIN
    elif kind=='file':version,domain='ltds-viewer-generated-gcp-file-chunks-v1',FILE_DOMAIN
    else:raise ValueError('chunk kind invalid')
    if (type(byte_count) is not int or not 1<=byte_count<=16*1024*1024
            or type(sha256) is not str or not re.fullmatch('[a-f0-9]{64}',sha256)
            or (kind=='file' and (type(evidence_sha256) is not str or not re.fullmatch('[a-f0-9]{64}',evidence_sha256)))):
        raise ValueError('chunk metadata invalid')
    lines=_lines(raw,hashes,total_bytes=stream_bytes,limit=24*1024*1024,page_limit=512)
    if type(page_bytes) is not list or len(page_bytes)!=len(lines) or len(lines)<2:raise ValueError('chunk inventory invalid')
    digest=hashlib.sha256(); offset=0; utf8=__import__('codecs').getincrementaldecoder('utf-8')('strict')
    for index,line in enumerate(lines):
        frame=_decode(line,65535)
        common={'version','attemptId','index','offset','evidenceSha256'}
        if kind=='file':common.add('fileSha256')
        terminal=index==len(lines)-1
        required=common|({'complete','totalBytes'} if terminal else {'data','decodedByteCount'})
        if (type(frame) is not dict or set(frame)!=required or frame['version']!=version
                or frame['attemptId']!=attempt_id or type(frame['index']) is not int or frame['index']!=index
                or type(frame['offset']) is not int or frame['offset']!=offset
                or type(page_bytes[index]) is not int or page_bytes[index]!=len(line)
                or hashlib.sha256(domain+line).hexdigest()!=hashes[index]
                or frame['evidenceSha256']!=(sha256 if kind=='evidence' else evidence_sha256)
                or (kind=='file' and frame['fileSha256']!=sha256)):
            raise ValueError('chunk binding changed')
        if terminal:
            if frame['complete'] is not True or type(frame['totalBytes']) is not int or frame['totalBytes']!=offset:raise ValueError('chunk closure invalid')
            continue
        data=frame['data']
        if type(data) is not str or not re.fullmatch('[A-Za-z0-9_-]+',data) or len(data)>61440:
            raise ValueError('chunk data invalid')
        chunk=base64.urlsafe_b64decode(data+'='*((-len(data))%4))
        if (base64.urlsafe_b64encode(chunk).decode().rstrip('=')!=data or type(frame['decodedByteCount']) is not int
                or not 1<=len(chunk)<=46080 or frame['decodedByteCount']!=len(chunk)):
            raise ValueError('chunk data invalid')
        offset+=len(chunk)
        if offset>byte_count:raise ValueError('chunk size exceeded')
        utf8.decode(chunk,final=False);digest.update(chunk)
    utf8.decode(b'',final=True)
    if offset!=byte_count or digest.hexdigest()!=sha256:raise ValueError('chunk digest changed')
    return {'state':'gcp_chunk_bytes_verified_not_semantics','sourceCustodyAccepted':False,'processingAuthorized':False}
