"""Unmounted pre-activation source projection; no authenticated transport."""
import hashlib
import json
import re

from . import reference_protocol as p
from . import source_inventory_candidate as inventory

DOMAIN=b'ltds-viewer-preactivation-source-v1\0'
_PHOTO=re.compile(r'\.(?:jpe?g|png|tiff?|dng|raw|heic)$',re.I)
_AUXILIARY=re.compile(r'\.(?:txt|geojson|json|zip|las|laz)$',re.I)


def _viewer_input_bytes(value):
    # Viewer JSON.stringify preserves insertion order for these exact keys.
    return json.dumps(value,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode('utf8')


def evaluate(raw_inventory, *, expected, authoritative_dataset, trusted_selection, trusted_gcp_evidence,
             generated_gcp_bytes, viewer_submission, source_policy):
    """Compare independently supplied facts. Always returns held, never a grant.

    All arguments are assumed to be provisioned by a future authenticated,
    transactionally coherent authority; this function cannot establish that.
    """
    import rfc8785
    p._closed(expected, ('principalId','environment','authorityId','datasetId','manifestSha256',
                         'viewerTaskId','viewerAttemptId','pipelineDigest'), 'source scope')
    for name in ('principalId','datasetId'): p._slug(expected[name],name)
    p._environment(expected['environment']);p._uuid(expected['authorityId'],'authority')
    for name in ('viewerTaskId','viewerAttemptId'):p._uuid(expected[name],name)
    for name in ('manifestSha256','pipelineDigest'):p._hash(expected[name],name)
    p._closed(source_policy, ('objectLimit','byteLimit','transferByteBudget'), 'source policy')
    for key in source_policy:p._integer(source_policy[key],key)
    if source_policy['objectLimit']>inventory.MAX_ENGINE_OBJECTS:raise ValueError('engine object policy invalid')
    binding={k:expected[k] for k in ('datasetId','manifestSha256','viewerTaskId','viewerAttemptId','pipelineDigest')}
    canonical, inventory_digest, _ = inventory.validate(raw_inventory,expected=binding,
        authoritative_dataset=authoritative_dataset,trusted_selection=trusted_selection,
        trusted_gcp_evidence=trusted_gcp_evidence,generated_gcp_bytes=generated_gcp_bytes)
    item=inventory._parse(canonical)
    if type(viewer_submission) is not dict or not {'attemptId','datasetId','datasetManifestSha256','sourceFiles',
            'sourceFilesSha256','inputFiles','inputManifestSha256','gcpSnapshotSha256'}.issubset(viewer_submission):
        raise ValueError('Viewer attempt provenance unavailable')
    if (viewer_submission['attemptId'],viewer_submission['datasetId'],viewer_submission['datasetManifestSha256']) != (
            expected['viewerAttemptId'],expected['datasetId'],expected['manifestSha256']):
        raise ValueError('Viewer attempt provenance differs')
    source=viewer_submission['sourceFiles']; sent=viewer_submission['inputFiles']
    if type(source) is not list or len(source)>inventory.MAX_SOURCES or type(sent) is not list:
        raise ValueError('Viewer input inventory unavailable')
    if p.digest(source)!=viewer_submission['sourceFilesSha256']:
        raise ValueError('Viewer source digest differs')
    by_path={v['relativePath']:v for v in item['processing']+item['retained']}
    if len(source)!=len(by_path): raise ValueError('Viewer source closure differs')
    for record in source:
        p._closed(record,('relativePath','byteSize','sha256','processingRole'),'Viewer source')
        found=by_path.get(record['relativePath'])
        if (found is None or type(record['byteSize']) is not int
                or (record['byteSize'],record['sha256']) != (found['byteSize'],found['sha256'])
                or record['processingRole'] not in ('auto','image','provider_input','gcp_source','administrative')):
            raise ValueError('Viewer source mapping differs')
    if len({v['relativePath'] for v in source})!=len(source):raise ValueError('Viewer source path duplicated')
    selected={v['relativePath']:v for v in item['processing']}
    derived=[v for v in source if v['processingRole'] not in ('gcp_source','administrative')
             and (v['processingRole']!='auto' or _PHOTO.search(v['relativePath']))]
    if item['generated']:
        derived=[v for v in derived if v['relativePath'].split('/')[-1].lower()!='gcp_list.txt']
    if {v['relativePath'] for v in derived} != set(selected):
        raise ValueError('Viewer attempt source selection differs')
    for v in derived:
        expected_role='image' if v['processingRole']=='auto' else v['processingRole']
        if (selected[v['relativePath']]['processingRole'] != expected_role
                or (expected_role=='image' and not _PHOTO.search(v['relativePath']))
                or (expected_role=='provider_input' and not _AUXILIARY.search(v['relativePath']))):
            raise ValueError('Viewer resolved processing role differs')
    source_selected=[v for v in source if v['relativePath'] in selected]
    projected=[{k:v[k] for k in ('relativePath','byteSize','sha256')} for v in source_selected if _PHOTO.search(v['relativePath'])]
    projected += [{k:v[k] for k in ('relativePath','byteSize','sha256')} for v in source_selected if not _PHOTO.search(v['relativePath'])]
    if item['generated']:
        aux=item['generated'][0]
        if any(v['relativePath'].split('/')[-1].lower()=='gcp_list.txt' for v in source_selected):
            raise ValueError('generated GCP shadows selected source')
        projected.append(dict(relativePath='gcp_list.txt',byteSize=aux['byteSize'],sha256=aux['sha256']))
        if viewer_submission['gcpSnapshotSha256']!=aux['sha256']:raise ValueError('GCP provenance differs')
    elif viewer_submission['gcpSnapshotSha256'] is not None:raise ValueError('unexpected GCP provenance')
    if p.canonical_json(sent) != p.canonical_json(projected) or hashlib.sha256(_viewer_input_bytes(projected)).hexdigest()!=viewer_submission['inputManifestSha256']:
        raise ValueError('Viewer transmitted inventory differs')
    total=sum(v['byteSize'] for v in projected)
    if (len(projected)>source_policy['objectLimit'] or total>source_policy['byteLimit']
            or not total<=source_policy['transferByteBudget']<=min(p.MAX_SAFE_INTEGER,3*total)):
        raise ValueError('source budget invalid')
    payload=dict(protocol='ltds-viewer-preactivation-source-v1',schemaVersion=1,scope=expected,
        sourceInventorySha256=inventory_digest,sourceFilesSha256=viewer_submission['sourceFilesSha256'],
        inputManifestSha256=viewer_submission['inputManifestSha256'],gcpSnapshotSha256=viewer_submission['gcpSnapshotSha256'],
        objectCount=len(projected),inputBytes=total,policy=source_policy)
    wire=rfc8785.dumps(payload)
    return wire, hashlib.sha256(DOMAIN+wire).hexdigest(), 'held_authenticated_source_authority_unavailable'
