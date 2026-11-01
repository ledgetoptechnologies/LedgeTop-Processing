"""Disabled synthetic transition hashing proposal; no Viewer authentication."""
import hashlib
import json
import re
from . import reference_protocol as p

QUALIFIED=False
DOMAIN=b'ltds-viewer-ledger-transition-v2\0'
RECEIPT_BOUND_DOMAIN=b'ltds-viewer-ledger-transition-experiment-v3\0'
RECEIPT_DIGEST_DOMAIN=b'ltds-v2-reservation-receipt-digest-experiment-v1\0'
MAX_BODY=16384
FIELDS={'version','type','environment','trustDomain','ledgerId','ledgerEpoch','authorityEpoch','operationId',
        'jobId','attemptId','reservationId','generation','workerInstanceId','claimLineageSha256',
        'inputManifestSha256','requestId','requestSha256','deliveryId','objectId','objectSha256',
        'objectLength','range','chargedBytes','previousRevision','nextRevision'}
class TransitionHeld(ValueError):pass

def calculate(previous_digest,body_raw):
    """Validate bounded proposed event and derive digest; caller data is not authority."""
    if QUALIFIED is not True:raise TransitionHeld('transition is not qualified')
    try:
        if (type(previous_digest) is not str or not re.fullmatch(r'[0-9a-f]{64}',previous_digest)
                or type(body_raw) is not bytes or not 0<len(body_raw)<=MAX_BODY):raise ValueError()
        def unique(pairs):
            out={}
            for key,value in pairs:
                if key in out:raise ValueError()
                out[key]=value
            return out
        body=json.loads(body_raw.decode('ascii'),object_pairs_hook=unique,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        if type(body) is not dict or set(body)!=FIELDS or p.canonical_json(body)!=body_raw or type(body['version']) is not int or body['version']!=2 or body['type']!='delivery-admit':raise ValueError()
        p._environment(body['environment'])
        for key in ('trustDomain','ledgerId','ledgerEpoch','authorityEpoch','operationId','reservationId','workerInstanceId','objectId'):p._slug(body[key],key)
        for key in ('jobId','attemptId'):p._uuid(body[key],key)
        for key in ('claimLineageSha256','inputManifestSha256','requestSha256','objectSha256'):p._hash(body[key],key)
        for key in ('requestId','deliveryId'):
            import base64
            value=body[key]
            if type(value) is not str or not re.fullmatch(r'[A-Za-z0-9_-]{43}',value):raise ValueError()
            decoded=base64.urlsafe_b64decode(value+'=')
            if len(decoded)!=32 or base64.urlsafe_b64encode(decoded).decode().rstrip('=')!=value:raise ValueError()
        p._integer(body['generation'],'generation');p._integer(body['objectLength'],'length',allow_zero=True);p._integer(body['chargedBytes'],'charge',allow_zero=True)
        p._integer(body['previousRevision'],'previous',allow_zero=True);p._integer(body['nextRevision'],'next')
        if body['previousRevision']>=p.MAX_SAFE_INTEGER or body['nextRevision']!=body['previousRevision']+1 or body['chargedBytes']!=body['objectLength']:raise ValueError()
        if type(body['range']) is not dict or set(body['range'])!={'kind'}:raise ValueError()
        if body['range']['kind']=='whole':
            if body['objectLength']==0:raise ValueError()
        elif body['range']['kind']=='empty':
            if body['objectLength']!=0 or body['objectSha256']!=hashlib.sha256(b'').hexdigest():raise ValueError()
        else:raise ValueError()
        return hashlib.sha256(DOMAIN+bytes.fromhex(previous_digest)+len(body_raw).to_bytes(8,'big')+body_raw).hexdigest()
    except Exception:raise TransitionHeld('invalid transition') from None

def calculate_receipt_bound(previous_digest,body_raw):
    """Separate provisional v3 encoding, with no trust or custody semantics."""
    if QUALIFIED is not True:raise TransitionHeld('transition is not qualified')
    try:
        if type(body_raw) is not bytes or not 0<len(body_raw)<=MAX_BODY:raise ValueError()
        def unique(pairs):
            out={}
            for key,value in pairs:
                if key in out:raise ValueError()
                out[key]=value
            return out
        body=json.loads(body_raw.decode('ascii'),object_pairs_hook=unique,parse_float=lambda _: (_ for _ in ()).throw(ValueError()),parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        added={'receiptId','intentDigest','receiptBodyDigest'}
        if type(body) is not dict or set(body)!=FIELDS|added or p.canonical_json(body)!=body_raw or type(body['version']) is not int or body['version']!=3:raise ValueError()
        from . import reference_v2_envelope_candidate as e
        for field in ('receiptId','reservationId'):e._bytes64(body[field],32)
        for field in ('intentDigest','receiptBodyDigest'):p._hash(body[field],field)
        base={k:v for k,v in body.items() if k not in added};base['version']=2
        calculate(previous_digest,p.canonical_json(base))
        return hashlib.sha256(RECEIPT_BOUND_DOMAIN+bytes.fromhex(previous_digest)+len(body_raw).to_bytes(8,'big')+body_raw).hexdigest()
    except Exception:raise TransitionHeld('invalid receipt-bound transition') from None
