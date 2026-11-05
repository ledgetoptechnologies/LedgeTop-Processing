"""Closed release-role authorization, separate from source intake permission."""
import base64
from copy import deepcopy
from datetime import datetime
import hashlib
import re

from cryptography.exceptions import InvalidSignature

from . import reference_protocol as p
from .reference_acceptance_observed import PAIR_FIELDS
from .viewer_source_attestation import ValidatedSourceKeys, validate_trusted_keys

PROTOCOL='ltds-viewer-source-release-v1'
DOMAIN=(PROTOCOL+'\0').encode('ascii')
POLICY={'id':'viewer-ready-plus-24h-v1','minimumAgeSeconds':86400}
MAX_BYTES=4*1024*1024
HOLD_REASONS=('disabled','release_role_unavailable','qualification_changed','custody_held',
    'binding_changed','timer_clock_invalid','active_source_io','metadata_quota',
    'source_identity_changed','source_missing','quarantine_conflict','durability_uncertain',
    'filesystem_unsupported','journal_invalid','filesystem_unavailable')
FIELDS=('schemaVersion','protocol','releaseId','issuerId','keyId','principalId',
    'coordinatorAuthorityId','jobId','generation','environment','viewerTaskId',
    'providerTaskId','viewerAttemptId','activationId','sourceSnapshotId',
    'preparedEnvelopeSha256','sourceSnapshotSha256','inputManifestSha256',
    'pipelineDigest','resultManifestSha256','sourceManifestSha256','derivedManifestSha256',
    'sourceReceiptSha256','derivedReceiptSha256','workerObservationId',
    'workerObservationAckSha256','receipts','readyProof','policy','eligibleObjects',
    'eligibleClosureSha256')


def utc(value):
    p._require(type(value) is str and re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z',value) is not None,'canonical UTC milliseconds required')
    parsed=datetime.fromisoformat(value.replace('Z','+00:00'))
    p._require(parsed.isoformat(timespec='milliseconds').replace('+00:00','Z')==value,'invalid UTC timestamp')
    return parsed


def objects(value):
    p._require(type(value) is list and len(value)<=2*p.MAX_OBJECTS,'bounded eligible closure required')
    identities=[]
    for item in value:
        p._closed(item,('id','objectKind','byteSize','sha256'),'eligible source object')
        p._slug(item['id'],'source object identity')
        p._require(item['objectKind'] in ('source','derived'),'invalid source object kind')
        p._integer(item['byteSize'],'source object size',allow_zero=True)
        p._hash(item['sha256'],'source object digest')
        identities.append((item['objectKind'],item['id']))
    p._require(identities==sorted(set(identities)),'eligible closure must be sorted and unique')
    p._require(sum(item['byteSize'] for item in value)<=p.MAX_SAFE_INTEGER,'source bytes exceed safe integer')
    return value


def validate_authorization(value):
    p._closed(value,FIELDS,'source release authorization')
    p._require(type(value['schemaVersion']) is int and value['schemaVersion']==1
        and value['protocol']==PROTOCOL,'unsupported source release authorization')
    for key in ('releaseId','viewerTaskId','providerTaskId','viewerAttemptId','activationId',
                'sourceSnapshotId','workerObservationId'):
        p._uuid(value[key],key)
    for key in ('issuerId','keyId','principalId','coordinatorAuthorityId','jobId'):
        p._slug(value[key],key)
    p._integer(value['generation'],'source release generation')
    p._require(value['providerTaskId']==value['jobId'],'release provider task differs from job')
    p._environment(value['environment'])
    for key in FIELDS:
        if key.endswith('Sha256') or key=='pipelineDigest':p._hash(value[key],key)
    p._closed(value['policy'],('id','minimumAgeSeconds'),'source release policy')
    p._integer(value['policy']['minimumAgeSeconds'],'minimum source age')
    p._require(value['policy']==POLICY,'unapproved source release policy')
    ready=value['readyProof']
    p._closed(ready,('kind','attemptId','modelId','modelVersionId','outputInventorySha256','verifiedAt'),'Viewer readiness proof')
    p._require(ready['kind']=='verified_outputs_ready','verified outputs ready proof required')
    for key in ('attemptId','modelId','modelVersionId'):p._uuid(ready[key],key)
    p._require(ready['attemptId']==value['viewerAttemptId'],'ready attempt differs')
    p._hash(ready['outputInventorySha256'],'ready output inventory digest');utc(ready['verifiedAt'])
    objects(value['eligibleObjects'])
    p._require(p.digest(value['eligibleObjects'])==value['eligibleClosureSha256'],'eligible closure digest mismatch')
    pairs=value['receipts']
    p._require(type(pairs) is list and 1<=len(pairs)<=p.MAX_OBJECTS,'bounded observed receipts required')
    identities=[];viewer_ids=[]
    for pair in pairs:
        p._closed(pair,PAIR_FIELDS,'observed acceptance pair')
        for key in ('acceptanceReceiptId','viewerReceiptId'):p._slug(pair[key],key)
        for key in ('acceptanceSha256','viewerReceiptSha256'):p._hash(pair[key],key)
        identities.append(pair['acceptanceReceiptId']);viewer_ids.append(pair['viewerReceiptId'])
    p._require(identities==sorted(set(identities)) and len(viewer_ids)==len(set(viewer_ids)),'observed receipts must be sorted and unique')
    p._require(len(p.canonical_json(value))<=MAX_BYTES,'source release authorization too large')
    return deepcopy(value)


def verify(value,keys):
    p._closed(value,('authorization','signature','authorizationSha256'),'signed source release')
    authorization=validate_authorization(value['authorization'])
    p._hash(value['authorizationSha256'],'source release authorization digest')
    message=DOMAIN+p.canonical_json(authorization)
    p._require(hashlib.sha256(message).hexdigest()==value['authorizationSha256'],'source release authorization digest mismatch')
    encoded=value['signature']
    p._require(type(encoded) is str and re.fullmatch(r'[A-Za-z0-9_-]{86}',encoded) is not None,'invalid release signature')
    signature=base64.urlsafe_b64decode(encoded+'==')
    p._require(base64.urlsafe_b64encode(signature).decode('ascii').rstrip('=')==encoded,'noncanonical release signature')
    registry=keys if isinstance(keys,ValidatedSourceKeys) else validate_trusted_keys(keys)
    key=registry.records.get((authorization['environment'],authorization['issuerId'],authorization['keyId']))
    p._require(key is not None and key[0]==authorization['principalId'],'untrusted release role')
    try:key[1].verify(signature,message)
    except InvalidSignature:raise ValueError('release signature rejected') from None
    p._require(len(p.canonical_json(value))<=MAX_BYTES,'signed source release too large')
    return authorization


def closure(source,derived,retained=False):
    expected='permanent_viewer_hold' if retained else 'release_after_acceptance'
    result=[dict(id=obj['id'],objectKind=kind,byteSize=obj['byteSize'],sha256=obj['sha256'])
        for kind,manifest in (('source',source),('derived',derived))
        for obj in manifest['objects'] if obj['retentionClass']==expected]
    return sorted(result,key=lambda obj:(obj['objectKind'],obj['id']))
