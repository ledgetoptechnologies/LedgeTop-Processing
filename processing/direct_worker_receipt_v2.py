"""Signed V2 local closure observation, never Viewer custody acceptance."""
import base64
import copy
import hashlib
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
from . import reference_protocol as p

DOMAIN=b'LTDS-DIRECT-WORKER-LOCAL-CLOSURE-V2\0'
PROTOCOL='ltds-direct-worker-local-closure-v2'
IDENTITY=('authorityId','nodeId','workerId','workerInstanceId')
FIELDS=frozenset(('protocol','reservationId','scratchBindingSha256','jobId','attemptId','generation',
    'authorityId','nodeId','workerId','workerInstanceId','sourceManifestSha256','derivedManifestSha256',
    'pipelineManifestSha256','inventorySha256','sourceObjectCount','derivedObjectCount',
    'localClosureComplete','sourceCustodyAccepted','transferComplete','processingAuthorized'))

class ReceiptRejected(ValueError):pass

def validate_closure(value):
    try:
        p._closed(value,FIELDS,'V2 local closure')
        p._require(value['protocol']=='ltds-direct-local-closure-v2'
            and value['localClosureComplete'] is True and value['sourceCustodyAccepted'] is False
            and value['transferComplete'] is False and value['processingAuthorized'] is False,
            'V2 local closure flags')
        for key in ('reservationId','jobId','attemptId','authorityId'):p._uuid(value[key],key)
        for key in ('nodeId','workerId','workerInstanceId'):p._slug(value[key],key)
        for key in ('scratchBindingSha256','sourceManifestSha256','derivedManifestSha256',
                    'pipelineManifestSha256','inventorySha256'):p._hash(value[key],key)
        p._integer(value['generation'],'generation')
        p._integer(value['sourceObjectCount'],'source count')
        p._integer(value['derivedObjectCount'],'derived count',allow_zero=True)
        p._require(value['sourceObjectCount'] <= p.MAX_OBJECTS and value['derivedObjectCount'] <= 1,
                   'V2 inventory count exceeds current source profile')
        return copy.deepcopy(value)
    except (ValueError,TypeError,KeyError):raise ReceiptRejected('invalid V2 local closure') from None

class WorkerClosureSignerV2:
    def __init__(self,*,issuer_id,key_id,identity,private_key):
        p._slug(issuer_id,'issuer');p._slug(key_id,'key')
        if type(identity) is not dict or set(identity)!=set(IDENTITY) or not isinstance(private_key,Ed25519PrivateKey):
            raise ReceiptRejected('invalid V2 pinned worker signer')
        p._uuid(identity['authorityId'],'authority')
        for key in IDENTITY[1:]:p._slug(identity[key],key)
        self.issuer_id,self.key_id,self.identity,self._key=issuer_id,key_id,dict(identity),private_key

    def issue(self,closure):
        closure=validate_closure(closure)
        if any(closure[key]!=self.identity[key] for key in IDENTITY):raise ReceiptRejected('V2 worker identity mismatch')
        body=dict(protocol=PROTOCOL,schemaVersion=2,issuerId=self.issuer_id,keyId=self.key_id,closure=closure)
        return dict(body,signature=base64.urlsafe_b64encode(self._key.sign(DOMAIN+p.canonical_json(body))).decode('ascii').rstrip('='))

    def retention_observation(self, signed_closure, *, binding_sha, closure_sha):
        closure=validate_closure(signed_closure['closure'])
        verify(signed_closure,expected_closure=closure,issuer_id=self.issuer_id,key_id=self.key_id,public_key=self._key.public_key())
        if any(closure[key]!=self.identity[key] for key in IDENTITY):raise ReceiptRejected('V2 worker identity mismatch')
        if (closure['scratchBindingSha256']!=binding_sha
                or hashlib.sha256(p.canonical_json(closure)).hexdigest()!=closure_sha):
            raise ReceiptRejected('retention fence binding differs')
        body=dict(protocol='ltds-direct-worker-retention-observation-v2',schemaVersion=2,
            issuerId=self.issuer_id,keyId=self.key_id,reservationId=closure['reservationId'],
            scratchBindingSha256=binding_sha,localClosureSha256=closure_sha,
            workerClosureReceiptSha256=hashlib.sha256(p.canonical_json(signed_closure)).hexdigest(),
            retentionFenceObserved=True,sourceCustodyAccepted=False,transferComplete=False,processingAuthorized=False)
        return dict(body,signature=base64.urlsafe_b64encode(self._key.sign(
            b'LTDS-DIRECT-WORKER-RETENTION-OBSERVATION-V2\0'+p.canonical_json(body))).decode('ascii').rstrip('='))

def verify_retention(receipt,*,signed_closure,expected_closure,issuer_id,key_id,public_key):
    verify(signed_closure,expected_closure=expected_closure,issuer_id=issuer_id,key_id=key_id,public_key=public_key)
    expected=dict(protocol='ltds-direct-worker-retention-observation-v2',schemaVersion=2,issuerId=issuer_id,keyId=key_id,
        reservationId=expected_closure['reservationId'],scratchBindingSha256=expected_closure['scratchBindingSha256'],
        localClosureSha256=hashlib.sha256(p.canonical_json(expected_closure)).hexdigest(),
        workerClosureReceiptSha256=hashlib.sha256(p.canonical_json(signed_closure)).hexdigest(),
        retentionFenceObserved=True,sourceCustodyAccepted=False,transferComplete=False,processingAuthorized=False)
    if (type(receipt) is not dict or set(receipt)!=set(expected)|{'signature'}
            or p.canonical_json({k:v for k,v in receipt.items() if k!='signature'})!=p.canonical_json(expected)):
        raise ReceiptRejected('retention observation binding differs')
    try:
        value=receipt['signature']
        if type(value) is not str or len(value)!=86:raise ValueError()
        signature=base64.b64decode(value+'==',altchars=b'-_',validate=True)
        if len(signature)!=64 or base64.urlsafe_b64encode(signature).decode().rstrip('=')!=value:raise ValueError()
        public_key.verify(signature,b'LTDS-DIRECT-WORKER-RETENTION-OBSERVATION-V2\0'+p.canonical_json(expected))
    except (ValueError,TypeError,InvalidSignature):raise ReceiptRejected('retention observation signature invalid') from None
    return copy.deepcopy(receipt)

def verify(receipt,*,expected_closure,issuer_id,key_id,public_key):
    expected=validate_closure(expected_closure)
    if (type(receipt) is not dict or set(receipt)!={'protocol','schemaVersion','issuerId','keyId','closure','signature'}
            or receipt['protocol']!=PROTOCOL or type(receipt['schemaVersion']) is not int or receipt['schemaVersion']!=2
            or receipt['issuerId']!=issuer_id or receipt['keyId']!=key_id or not isinstance(public_key,Ed25519PublicKey)):
        raise ReceiptRejected('V2 worker receipt binding mismatch')
    actual=validate_closure(receipt['closure'])
    if p.canonical_json(actual)!=p.canonical_json(expected):raise ReceiptRejected('V2 worker receipt binding mismatch')
    try:
        value=receipt['signature']
        if type(value) is not str or len(value)!=86:raise ValueError()
        signature=base64.b64decode(value+'==',altchars=b'-_',validate=True)
        if len(signature)!=64 or base64.urlsafe_b64encode(signature).decode().rstrip('=')!=value:raise ValueError()
        public_key.verify(signature,DOMAIN+p.canonical_json({k:v for k,v in receipt.items() if k!='signature'}))
    except (ValueError,TypeError,InvalidSignature):raise ReceiptRejected('V2 worker receipt signature invalid') from None
    return copy.deepcopy(receipt)
