"""Inactive lossless Viewer source-inventory proposal; not an intake route."""
import hashlib
import re
import unicodedata

from . import reference_protocol as p
from .prepared_envelope_jcs_candidate import _parse

PROTOCOL = 'ltds-source-inventory-v1'
DOMAIN = b'ltds-source-inventory-v1\0'
MAX_FILES = 10000
MAX_GENERATED_GCP_BYTES = 16 * 1024 * 1024
_ROLE = re.compile(r'[a-z][a-z0-9_-]{0,79}\Z')
_MEDIA = re.compile(r'[A-Za-z0-9!#$&^_.+-]+/[A-Za-z0-9!#$&^_.+-]+\Z')


def reference_v1_names(processing):
    """Preflight an already validated closure; never rename or flatten on collision."""
    if type(processing) is not list or not processing: raise ValueError('invalid processing closure')
    names=[]; seen=set()
    for item in processing:
        _descriptor(item)
        name=item['relativePath'].split('/')[-1]
        p._basename(name)
        folded=name.lower()  # _basename has already restricted it to ASCII.
        if folded in seen: raise ValueError('reference v1 basename collision')
        seen.add(folded); names.append(name)
    return tuple(names)


def _path(value):
    if (type(value) is not str or not value or len(value.encode('utf8')) > 4096
            or value.startswith('/') or '\\' in value
            or any(unicodedata.category(ch).startswith('C') for ch in value)):
        raise ValueError('invalid source relative path')
    parts = value.split('/')
    if any(not part or part in ('.','..') or len(part.encode('utf8')) > 255 for part in parts):
        raise ValueError('invalid source relative path')
    if re.match(r'^[A-Za-z]:', value): raise ValueError('invalid source relative path')


def _descriptor(obj):
    fields=('relativePath','byteSize','sha256','processingRole','contentType')
    p._closed(obj, (*fields, 'datasetFileId'), 'file')
    p._slug(obj['datasetFileId'], 'dataset file ID')
    try: _path(obj['relativePath'])
    except UnicodeError: raise ValueError('invalid source relative path') from None
    p._integer(obj['byteSize'], 'file byteSize', allow_zero=True)
    p._hash(obj['sha256'], 'file sha256')
    if type(obj['processingRole']) is not str or not _ROLE.fullmatch(obj['processingRole']): raise ValueError('invalid role')
    if type(obj['contentType']) is not str or len(obj['contentType']) > 127 or not _MEDIA.fullmatch(obj['contentType']):
        raise ValueError('invalid content type')


def validate(raw, *, expected, authoritative_dataset, generated_gcp_bytes):
    """Compare against independent binding and hash actual generated GCP bytes.

    Does not verify source file content, authenticate callers, store evidence,
    copy files, or authorize a job. Exact path/case is retained. Materializers
    must refuse filesystem-specific name collisions before any writes.
    """
    import rfc8785
    value = _parse(raw)
    p._closed(value, ('protocol','schemaVersion','dataset','binding','processing','retained','generated'), 'inventory')
    if value['protocol'] != PROTOCOL or type(value['schemaVersion']) is not int or value['schemaVersion'] != 1:
        raise ValueError('unsupported source inventory')
    p._closed(expected, ('datasetId','manifestSha256','viewerTaskId','viewerAttemptId','pipelineDigest'), 'trusted binding')
    p._closed(value['dataset'], ('id','manifestSha256'), 'dataset')
    p._closed(value['binding'], ('viewerTaskId','viewerAttemptId','pipelineDigest'), 'binding')
    p._slug(expected['datasetId'],'dataset ID'); p._hash(expected['manifestSha256'],'dataset manifest')
    p._uuid(expected['viewerTaskId'],'Viewer task'); p._uuid(expected['viewerAttemptId'],'Viewer attempt')
    p._hash(expected['pipelineDigest'],'pipeline digest')
    if value['dataset'] != dict(id=expected['datasetId'],manifestSha256=expected['manifestSha256']) or value['binding'] != {
            k:expected[k] for k in ('viewerTaskId','viewerAttemptId','pipelineDigest')}:
        raise ValueError('source inventory binding mismatch')
    processing, retained, generated = (value[k] for k in ('processing','retained','generated'))
    if (any(type(items) is not list for items in (processing,retained,generated))
            or not processing or len(processing)+len(retained)+len(generated)>MAX_FILES or len(generated)>1):
        raise ValueError('invalid file closure')
    for obj in processing+retained: _descriptor(obj)
    if any(obj['processingRole'] not in ('image','provider_input') for obj in processing): raise ValueError('unresolved processing role')
    if any(obj['processingRole'] not in ('gcp_source','administrative') for obj in retained): raise ValueError('unresolved retained role')
    if any([obj['datasetFileId'] for obj in seq] != sorted(obj['datasetFileId'] for obj in seq) for seq in (processing,retained)):
        raise ValueError('file order invalid')
    sources=processing+retained
    ids=[obj['datasetFileId'] for obj in sources]
    paths=[obj['relativePath'] for obj in sources]
    if len(set(ids)) != len(ids) or len(set(paths)) != len(paths): raise ValueError('duplicate file identity or path')
    # This is a trusted caller-provided independently authenticated mapping,
    # not a recomputation of the as-yet-unagreed Viewer manifest algorithm.
    p._closed(authoritative_dataset, ('id','manifestSha256','files'), 'authoritative dataset')
    if (authoritative_dataset['id'] != expected['datasetId']
            or authoritative_dataset['manifestSha256'] != expected['manifestSha256']
            or type(authoritative_dataset['files']) is not list): raise ValueError('authoritative dataset mismatch')
    fields=('datasetFileId','relativePath','byteSize','sha256','contentType')
    actual=sorted(({key:obj[key] for key in fields} for obj in sources),key=lambda obj:obj['datasetFileId'])
    if p.canonical_json(actual) != p.canonical_json(authoritative_dataset['files']):
        raise ValueError('dataset source closure mismatch')
    if generated:
        aux=generated[0]
        p._closed(aux, ('snapshotId','byteSize','sha256','sourceGcpDatasetFileIds',
                        'imageDatasetFileIds','correspondenceCount'), 'generated GCP')
        p._integer(aux['byteSize'], 'GCP byteSize', allow_zero=True); p._hash(aux['sha256'], 'GCP sha256')
        p._integer(aux['correspondenceCount'], 'GCP correspondence count')
        gcp_ids=aux['sourceGcpDatasetFileIds']; image_ids=aux['imageDatasetFileIds']
        if (aux['snapshotId'] != expected['viewerAttemptId']
                or type(gcp_ids) is not list or not gcp_ids or any(type(x) is not str for x in gcp_ids) or gcp_ids != sorted(set(gcp_ids))
                or type(image_ids) is not list or not image_ids or any(type(x) is not str for x in image_ids) or image_ids != sorted(set(image_ids))
                or not set(gcp_ids).issubset({obj['datasetFileId'] for obj in retained if obj['processingRole']=='gcp_source'})
                or not set(image_ids).issubset({obj['datasetFileId'] for obj in processing if obj['processingRole']=='image'})
                or aux['correspondenceCount'] < len(image_ids)
                or type(generated_gcp_bytes) is not bytes
                or len(generated_gcp_bytes) > MAX_GENERATED_GCP_BYTES
                or len(generated_gcp_bytes) != aux['byteSize']
                or hashlib.sha256(generated_gcp_bytes).hexdigest() != aux['sha256']):
            raise ValueError('generated GCP source or bytes mismatch')
    elif generated_gcp_bytes is not None:
        raise ValueError('unexpected GCP bytes')
    try: canonical=rfc8785.dumps(value)
    except (TypeError,ValueError,UnicodeError): raise ValueError('invalid canonical inventory') from None
    return canonical, hashlib.sha256(DOMAIN+canonical).hexdigest()
