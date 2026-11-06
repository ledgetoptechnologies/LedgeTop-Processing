"""Unwired Ed25519 local-closure evidence; never a Viewer acceptance."""
import base64
import copy
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
from . import reference_protocol as p
from . import direct_limits

DOMAIN=b'LTDS-DIRECT-WORKER-LOCAL-CLOSURE-V1\0'
PROTOCOL='ltds-direct-worker-local-closure-v1'
IDENTITY=('authorityId','nodeId','workerId','workerInstanceId')
CLOSURE_FIELDS=frozenset(('protocol','reservationId','scratchBindingSha256','authorityId','nodeId','workerId',
    'workerInstanceId','jobId','attemptId','generation','environment','manifestSha256','derivedManifestSha256',
    'sourceObjectCount','derivedObjectCount','localClosureComplete','sourceCustodyAccepted','processingAuthorized'))


class ReceiptRejected(ValueError):pass


def validate_closure(value):
    """Closed exact-type local observation; not an upstream custody receipt."""
    try:
        if type(value) is not dict or (set(value)!=CLOSURE_FIELDS and set(value)!=CLOSURE_FIELDS | {'limitProfile'}):
            raise ValueError()
        if 'limitProfile' in value and value['limitProfile'] != direct_limits.PROFILE:
            raise ValueError()
        if (type(value['protocol']) is not str or value['protocol']!='ltds-direct-local-closure-v1'
                or value['localClosureComplete'] is not True
                or value['sourceCustodyAccepted'] is not False
                or value['processingAuthorized'] is not False):
            raise ValueError()
        for name in ('reservationId','authorityId','jobId','attemptId'):
            p._uuid(value[name],name)
        for name in ('nodeId','workerId','workerInstanceId'):
            p._slug(value[name],name)
        for name in ('scratchBindingSha256','manifestSha256','derivedManifestSha256'):
            p._hash(value[name],name)
        p._environment(value['environment'])
        p._integer(value['generation'],'generation')
        p._integer(value['sourceObjectCount'],'sourceObjectCount')
        p._integer(value['derivedObjectCount'],'derivedObjectCount',allow_zero=True)
        direct_limits.counts(value.get('limitProfile'),value['sourceObjectCount'],value['derivedObjectCount'])
        return copy.deepcopy(value)
    except (ValueError,TypeError,KeyError):
        raise ReceiptRejected('invalid worker closure') from None


class WorkerClosureSigner:
    def __init__(self,*,issuer_id,key_id,identity,private_key):
        p._slug(issuer_id,'issuer');p._slug(key_id,'key')
        if (type(identity) is not dict or set(identity)!=set(IDENTITY)
                or not isinstance(private_key,Ed25519PrivateKey)):
            raise ReceiptRejected('invalid pinned worker signer')
        p._uuid(identity['authorityId'],'authority')
        for name in IDENTITY[1:]:p._slug(identity[name],name)
        self.issuer_id,self.key_id,self.identity,self._key=issuer_id,key_id,dict(identity),private_key

    def issue(self,closure):
        closure=validate_closure(closure)
        if any(closure[k]!=self.identity[k] for k in IDENTITY):
            raise ReceiptRejected('worker closure identity mismatch')
        body=dict(protocol=PROTOCOL,schemaVersion=1,issuerId=self.issuer_id,keyId=self.key_id,closure=copy.deepcopy(closure))
        signature=self._key.sign(DOMAIN+p.canonical_json(body))
        return dict(body,signature=base64.urlsafe_b64encode(signature).decode('ascii').rstrip('='))


def verify(receipt,*,expected_closure,issuer_id,key_id,public_key):
    expected_closure=validate_closure(expected_closure)
    if (type(receipt) is not dict or set(receipt)!={'protocol','schemaVersion','issuerId','keyId','closure','signature'}
            or receipt['protocol']!=PROTOCOL or type(receipt['schemaVersion']) is not int or receipt['schemaVersion']!=1
            or receipt['issuerId']!=issuer_id or receipt['keyId']!=key_id
            or not isinstance(public_key,Ed25519PublicKey)):
        raise ReceiptRejected('worker receipt binding mismatch')
    observed=validate_closure(receipt['closure'])
    if p.canonical_json(observed)!=p.canonical_json(expected_closure):
        raise ReceiptRejected('worker receipt binding mismatch')
    value=receipt['signature']
    try:
        if type(value) is not str or len(value)!=86:raise ValueError()
        signature=base64.b64decode(value+'==',altchars=b'-_',validate=True)
        if len(signature)!=64 or base64.urlsafe_b64encode(signature).decode().rstrip('=')!=value:raise ValueError()
        public_key.verify(signature,DOMAIN+p.canonical_json({k:v for k,v in receipt.items() if k!='signature'}))
    except (ValueError,TypeError,InvalidSignature):raise ReceiptRejected('worker receipt signature invalid') from None
    return copy.deepcopy(receipt)
