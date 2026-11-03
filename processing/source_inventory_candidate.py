"""Inactive lossless Viewer source-inventory proposal; not an intake route."""
import hashlib
import json
import math
import re
import unicodedata

from . import reference_protocol as p

PROTOCOL = 'ltds-source-inventory-v1'
DOMAIN = b'ltds-source-inventory-v1\0'
MAX_PHOTOS = 10000
MAX_ENGINE_OBJECTS = 10000
MAX_SOURCES = 20000
MAX_AUXILIARY = 10000
MAX_RAW_BYTES = 12 * 1024 * 1024
MAX_EVIDENCE_BYTES = 12 * 1024 * 1024  # Separate provisional bound, not Viewer 64 MiB.
MAX_NODES = 200000
MAX_CORRESPONDENCES = 10000  # Viewer allows 100k; larger evidence remains held here.
MAX_GENERATED_GCP_BYTES = 16 * 1024 * 1024
_ROLE = re.compile(r'[a-z][a-z0-9_-]{0,79}\Z')
_MEDIA = re.compile(r'[A-Za-z0-9!#$&^_.+-]+/[A-Za-z0-9!#$&^_.+-]+\Z')


def _parse(raw):
    if type(raw) is not bytes or len(raw) > MAX_RAW_BYTES: raise ValueError('bounded inventory bytes required')
    depth=0; quoted=False; escaped=False
    for byte in raw:
        if quoted:
            if escaped: escaped=False
            elif byte==92: escaped=True
            elif byte==34: quoted=False
        elif byte==34: quoted=True
        elif byte in (91,123):
            depth+=1
            if depth>16: raise ValueError('inventory depth limit')
        elif byte in (93,125): depth-=1
    def pairs(items):
        out={}
        for key,value in items:
            if key in out: raise ValueError('duplicate key')
            out[key]=value
        return out
    try:
        value=json.loads(raw.decode('utf8'),object_pairs_hook=pairs,
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError('nonfinite number')))
    except (UnicodeError,json.JSONDecodeError,RecursionError): raise ValueError('invalid inventory JSON') from None
    todo=[value]; count=0
    while todo:
        item=todo.pop();count+=1
        if count>MAX_NODES: raise ValueError('inventory node limit')
        if type(item) is int and abs(item)>p.MAX_SAFE_INTEGER: raise ValueError('unsafe integer')
        if type(item) is float and (not math.isfinite(item) or abs(item)>p.MAX_SAFE_INTEGER): raise ValueError('invalid number')
        if type(item) is dict: todo.extend(item.values())
        elif type(item) is list: todo.extend(item)
    return value


def decode_viewer_gcp_evidence(raw):
    """Bound encoded evidence rows/structure before allocating JSON objects."""
    if type(raw) is not bytes or len(raw)>MAX_EVIDENCE_BYTES: raise ValueError('bounded evidence bytes required')
    quoted=False;escaped=False; objects=0; separators=0
    for byte in raw:
        if quoted:
            if escaped: escaped=False
            elif byte==92: escaped=True
            elif byte==34: quoted=False
        elif byte==34: quoted=True
        elif byte==123:
            objects+=1
            if objects>MAX_CORRESPONDENCES+2: raise ValueError('evidence row bound exceeded')
        elif byte in (44,91):
            separators+=1
            if separators>MAX_NODES: raise ValueError('evidence structure bound exceeded')
    return _parse(raw)


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


def validate(raw, *, expected, authoritative_dataset, trusted_selection, trusted_gcp_evidence, generated_gcp_bytes):
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
            or not processing or len(processing)+len(retained)>MAX_SOURCES or len(generated)>1):
        raise ValueError('invalid file closure')
    for obj in processing+retained: _descriptor(obj)
    if any(obj['processingRole'] not in ('image','provider_input') for obj in processing): raise ValueError('unresolved processing role')
    if any(obj['processingRole'] not in ('gcp_source','administrative') for obj in retained): raise ValueError('unresolved retained role')
    photos=sum(obj['processingRole']=='image' for obj in processing)
    if not photos or photos>MAX_PHOTOS or len(processing)+len(retained)-photos>MAX_AUXILIARY:
        raise ValueError('source inventory capacity exceeded')
    if len(processing)+len(generated)>MAX_ENGINE_OBJECTS:
        raise ValueError('reference engine object capacity exceeded')
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
    trusted=[]
    for obj in authoritative_dataset['files']:
        p._closed(obj, (*fields,'mimeType','imageWidth','imageHeight'), 'authoritative file')
        resolved={key:obj[key] for key in fields}
        if resolved['contentType'] is None: resolved['contentType']=obj['mimeType']
        trusted.append(resolved)
    if p.canonical_json(actual) != p.canonical_json(trusted):
        raise ValueError('dataset source closure mismatch')
    # Attempt selection is independently authenticated, never derived from
    # the inventory it is intended to constrain.
    p._closed(trusted_selection, ('viewerTaskId','viewerAttemptId','pipelineDigest','gcpRequired','files'), 'trusted attempt selection')
    if (any(trusted_selection[k] != expected[k] for k in ('viewerTaskId','viewerAttemptId','pipelineDigest'))
            or type(trusted_selection['gcpRequired']) is not bool or type(trusted_selection['files']) is not list):
        raise ValueError('trusted attempt selection mismatch')
    selected=sorted([dict(datasetFileId=obj['datasetFileId'],processingRole=obj['processingRole'],disposition=disposition)
        for disposition, group in (('processing',processing),('retained',retained)) for obj in group],
        key=lambda obj:obj['datasetFileId'])
    if p.canonical_json(selected) != p.canonical_json(trusted_selection['files']):
        raise ValueError('attempt selection mismatch')
    if bool(generated) != trusted_selection['gcpRequired']:
        raise ValueError('GCP requirement mismatch')
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
        trusted_gcp_evidence=decode_viewer_gcp_evidence(trusted_gcp_evidence)
        p._closed(trusted_gcp_evidence, ('schemaVersion','snapshot','correspondences'), 'trusted GCP evidence')
        if type(trusted_gcp_evidence['schemaVersion']) is not int or trusted_gcp_evidence['schemaVersion'] != 1:
            raise ValueError('unsupported GCP evidence')
        if p.canonical_json(aux) != p.canonical_json(trusted_gcp_evidence['snapshot']):
            raise ValueError('trusted GCP snapshot mismatch')
        rows=trusted_gcp_evidence['correspondences']
        if type(rows) is not list or len(rows)>MAX_CORRESPONDENCES or len(rows)!=aux['correspondenceCount']:
            raise ValueError('trusted GCP correspondence mismatch')
        for row in rows:
            p._closed(row, ('correspondenceId','gcpPointId','gcpSetId','externalId','label','latitude','longitude','elevationM',
                            'sourceGcpDatasetFileId','sourceGcpSha256','sourceGcpRelativePath',
                            'imageDatasetFileId','imageRelativePath','imageWidth','imageHeight','pixelX','pixelY'), 'GCP correspondence')
            if type(row['sourceGcpDatasetFileId']) is not str or type(row['imageDatasetFileId']) is not str:
                raise ValueError('invalid GCP correspondence')
        if (sorted({r['sourceGcpDatasetFileId'] for r in rows}) != gcp_ids
                or sorted({r['imageDatasetFileId'] for r in rows}) != image_ids):
            raise ValueError('trusted GCP correspondence mismatch')
        from .gcp_snapshot_semantics_candidate import validate_gcp_semantics
        validate_gcp_semantics(generated_gcp_bytes,processing=processing,retained=retained,
                               authoritative_files=authoritative_dataset['files'],correspondences=rows)
    elif generated_gcp_bytes is not None:
        raise ValueError('unexpected GCP bytes')
    elif trusted_gcp_evidence is not None:
        raise ValueError('unexpected GCP evidence')
    try: canonical=rfc8785.dumps(value)
    except (TypeError,ValueError,UnicodeError): raise ValueError('invalid canonical inventory') from None
    # Structural validity and a byte hash are not semantic source evidence.
    # There is intentionally no ready/authorized return state in this candidate.
    hold = 'held_gcp_correspondence_unverified' if generated else 'held_source_authority_unverified'
    return canonical, hashlib.sha256(DOMAIN+canonical).hexdigest(), hold
