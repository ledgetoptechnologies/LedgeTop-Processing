"""UNMOUNTED, UNAGREED v2 signature experiment; never a custody authority.

The public entry point is deliberately unavailable. The private fixture
function has no HTTP authentication, durable journal, namespace fence,
protocol-negotiation pin or cleanup authority and must not be wired to runtime.
"""
import base64
from copy import deepcopy
from datetime import datetime
import hashlib
import re
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization

from . import reference_protocol as p
from .viewer_acceptance_receipt import validate_trusted_viewer_receipt_keys, _load_key
from .viewer_result_manifest import validate_viewer_result_manifest, validate_viewer_acceptance

DOMAIN=b'LTDS-VIEWER-ACCEPTED-COPY-ACK-V2\0'
ACCEPTANCE_DOMAIN=b'LTDS-OUTER-ACCEPTANCE-V1\0'
FIELDS=frozenset(('protocol','schemaVersion','issuerId','keyId','receiptId','storageReceiptId',
    'acceptanceReceiptId','acceptanceStorageReceiptId','acceptancePayloadSha256',
    'coordinatorAuthorityId','viewerPrincipalId','submissionId','activationId','viewerTaskId',
    'viewerAttemptId','preparedEnvelopeSha256','providerTaskId','jobId','generation','environment',
    'inputManifestSha256','pipelineDigest','resultManifestSha256','sourceManifestSha256',
    'derivedManifestSha256','sourceReceiptSha256','derivedReceiptSha256','acceptedObjectIds','objects',
    'acceptedAt','signature'))
BINDINGS=frozenset(('coordinatorAuthorityId','viewerPrincipalId','submissionId','activationId',
    'viewerTaskId','viewerAttemptId','preparedEnvelopeSha256','providerTaskId','jobId','generation','environment',
    'inputManifestSha256','pipelineDigest','resultManifestSha256','sourceManifestSha256',
    'derivedManifestSha256','sourceReceiptSha256','derivedReceiptSha256'))

class CandidateUnavailable(RuntimeError): pass
class CandidateRejected(ValueError): pass

def verify_copy_v2(*_args, **_kwargs):
    raise CandidateUnavailable('unagreed and unmounted v2 copy verifier')

def _ascii_tree(value):
    if type(value) is str:
        return value.isascii() and all(ord(c)>=32 and ord(c)!=127 for c in value)
    if value is None or type(value) is bool:return True
    if type(value) is int:return 0<=value<=p.MAX_SAFE_INTEGER
    if type(value) is list:return all(_ascii_tree(x) for x in value)
    if type(value) is dict:return all(type(k) is str and _ascii_tree(k) and _ascii_tree(v) for k,v in value.items())
    return False

def _scoped_keys(registry, expected):
    if type(registry) is not dict or not 1 <= len(registry) <= 32:raise ValueError('invalid scope registry')
    keys={}; owners={}; issuers={}
    for key_id, entry in registry.items():
        p._slug(key_id,'key id')
        p._closed(entry,('issuerId','publicKeyPem','coordinatorAuthorityId','viewerPrincipalId','environment'),'scope key')
        scope=(entry['coordinatorAuthorityId'],entry['viewerPrincipalId'],entry['environment'])
        p._slug(scope[0],'authority');p._slug(scope[1],'principal');p._environment(scope[2])
        clean=validate_trusted_viewer_receipt_keys({key_id:{'issuerId':entry['issuerId'],'publicKeyPem':entry['publicKeyPem']}})[key_id]
        public=_load_key(clean['publicKeyPem']).public_bytes(serialization.Encoding.DER,serialization.PublicFormat.SubjectPublicKeyInfo)
        if (public in owners and owners[public]!=scope) or (clean['issuerId'] in issuers and issuers[clean['issuerId']]!=scope):raise ValueError('cross-scope signer')
        owners[public]=scope;issuers[clean['issuerId']]=scope
        keys[key_id]=(clean,scope)
    return keys

def _verify_v2_for_fixtures(raw, *, expected, result_manifest, trusted_keys):
    """Fixture-only verification; expected and pins must be independently supplied."""
    try:
        if type(raw) is not bytes:raise ValueError('raw bytes required')
        envelope=p.decode_submission(raw)
        p._closed(envelope,('acceptance','viewerReceipt'),'copy envelope')
        acceptance, receipt=envelope['acceptance'],envelope['viewerReceipt']
        if type(receipt) is not dict or set(receipt)!=FIELDS or not _ascii_tree(envelope):raise ValueError('closed ascii object required')
        if type(expected) is not dict or set(expected)!=BINDINGS or not _ascii_tree(expected):raise ValueError('independent binding required')
        if receipt['protocol']!='ltds-viewer-accepted-copy-v2' or type(receipt['schemaVersion']) is not int or receipt['schemaVersion']!=2:raise ValueError('wrong protocol')
        for field in ('issuerId','keyId','receiptId','storageReceiptId','acceptanceReceiptId','acceptanceStorageReceiptId','coordinatorAuthorityId','viewerPrincipalId','providerTaskId','jobId'):
            p._slug(receipt[field],field)
        for field in ('submissionId','activationId','viewerTaskId','viewerAttemptId'):p._uuid(receipt[field],field)
        for field in ('preparedEnvelopeSha256','acceptancePayloadSha256','inputManifestSha256','pipelineDigest','resultManifestSha256',
                      'sourceManifestSha256','derivedManifestSha256','sourceReceiptSha256','derivedReceiptSha256'):
            p._hash(receipt[field],field)
        if receipt['providerTaskId'] != receipt['jobId']:raise ValueError('provider task binding mismatch')
        p._integer(receipt['generation'],'generation');p._environment(receipt['environment'])
        timestamp=receipt['acceptedAt']
        if type(timestamp) is not str or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z',timestamp):raise ValueError('invalid timestamp')
        datetime.fromisoformat(timestamp.replace('Z','+00:00'))
        if any(type(receipt[k]) is not type(expected[k]) or receipt[k]!=expected[k] for k in BINDINGS):raise ValueError('identity mismatch')
        manifest=validate_viewer_result_manifest(result_manifest,job_id=expected['jobId'],generation=expected['generation'],environment=expected['environment'],pipeline_digest=expected['pipelineDigest'])
        if manifest['resultManifestSha256']!=expected['resultManifestSha256']:raise ValueError('result mismatch')
        accepted=validate_viewer_acceptance(acceptance,result_manifest=manifest,input_manifest_sha256=expected['inputManifestSha256'])
        outer_time=accepted['acceptedAt']
        if type(outer_time) is not str or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z',outer_time):raise ValueError('invalid outer timestamp')
        datetime.fromisoformat(outer_time.replace('Z','+00:00'))
        ids=[item['id'] for item in manifest['objects']]
        objects=[{'id':item['id'],'byteSize':item['byteSize'],'sha256':item['sha256']} for item in manifest['objects']]
        if type(receipt['objects']) is not list:raise ValueError('objects must be a list')
        for item in receipt['objects']:
            p._closed(item,('id','byteSize','sha256'),'receipt object')
            p._slug(item['id'],'object id');p._integer(item['byteSize'],'byteSize',allow_zero=True);p._hash(item['sha256'],'object sha256')
        if not ids or accepted['acceptedObjectIds']!=ids or receipt['acceptedObjectIds']!=ids or receipt['objects']!=objects:raise ValueError('closure mismatch')
        if (receipt['acceptanceReceiptId'],receipt['acceptanceStorageReceiptId'])!=(accepted['receiptId'],accepted['storageReceiptId']):raise ValueError('outer identity mismatch')
        if len({receipt['receiptId'],receipt['storageReceiptId'],accepted['receiptId'],accepted['storageReceiptId']})!=4:raise ValueError('receipt collision')
        if hashlib.sha256(ACCEPTANCE_DOMAIN+p.canonical_json(acceptance)).hexdigest()!=receipt['acceptancePayloadSha256']:raise ValueError('outer digest mismatch')
        keys=_scoped_keys(trusted_keys,expected)
        if receipt['keyId'] not in keys:raise ValueError('untrusted signer')
        key,scope=keys[receipt['keyId']]
        if key['issuerId']!=receipt['issuerId'] or scope!=(expected['coordinatorAuthorityId'],expected['viewerPrincipalId'],expected['environment']):raise ValueError('untrusted scope')
        signature=receipt['signature']
        if type(signature) is not str or not re.fullmatch(r'[A-Za-z0-9_-]{86}',signature):raise ValueError('invalid signature')
        decoded=base64.urlsafe_b64decode(signature+'==')
        if len(decoded)!=64 or base64.urlsafe_b64encode(decoded).decode().rstrip('=')!=signature:raise ValueError('noncanonical signature')
        _load_key(key['publicKeyPem']).verify(decoded,DOMAIN+p.canonical_json({k:v for k,v in receipt.items() if k!='signature'}))
        return deepcopy(receipt)
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError, InvalidSignature, OverflowError) as error:
        raise CandidateRejected('fixture copy proof rejected') from error
