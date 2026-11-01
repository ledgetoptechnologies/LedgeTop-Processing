"""Disabled synthetic reservation pair; no Viewer custody or current authority."""
import hashlib
import json
from . import reference_protocol as p,reference_v2_envelope_candidate as e

QUALIFIED=False
INTENT_DOMAIN=b'ltds-v2-reservation-intent-experiment-v1\0'
INTENT_DIGEST_DOMAIN=b'ltds-v2-reservation-intent-digest-experiment-v1\0'
RECEIPT_DOMAIN=b'ltds-v2-reservation-receipt-experiment-v1\0'
INTENT=set('version kind issuer keyId audience operation issuedAt expiresAt environment trustDomain coordinatorDeploymentId coordinatorAuthorityId coordinatorEpoch viewerDeploymentId viewerLedgerId viewerLedgerEpoch intentId idempotencyId jobId attemptId generation reservationRequestId inputManifestSha256 objectCount totalSourceBytes byteBudget policyId claimBindingSha256'.split())
RECEIPT=set('version kind issuer keyId audience operation issuedAt expiresAt environment trustDomain coordinatorDeploymentId viewerDeploymentId viewerLedgerId viewerLedgerEpoch receiptId idempotencyId intentId intentDigest reservationId jobId attemptId generation inputManifestSha256 objectCount totalSourceBytes byteBudget policyId sourceRevisionId'.split())
SHARED=set('environment trustDomain coordinatorDeploymentId viewerDeploymentId viewerLedgerId viewerLedgerEpoch idempotencyId intentId jobId attemptId generation inputManifestSha256 objectCount totalSourceBytes byteBudget policyId'.split())
class ReservationHeld(ValueError):pass

def _parse(raw,fields,kind):
    if type(raw) is not bytes or not 0<len(raw)<=16384:raise ValueError('length')
    def unique(pairs):
        out={}
        for key,value in pairs:
            if key in out:raise ValueError('duplicate')
            out[key]=value
        return out
    value=json.loads(raw.decode('ascii'),object_pairs_hook=unique,parse_float=lambda _: (_ for _ in ()).throw(ValueError()),parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    if type(value) is not dict or set(value)!=fields or e._canonical(value)!=raw or type(value['version']) is not int or value['version']!=1 or value['kind']!=kind:raise ValueError('shape')
    for field in ('issuer','keyId','audience','operation','trustDomain','coordinatorDeploymentId','viewerDeploymentId','viewerLedgerId','viewerLedgerEpoch','policyId'):
        p._slug(value[field],field)
    for field in ('jobId','attemptId'):p._uuid(value[field],field)
    for field in ('intentId','idempotencyId')+ (('reservationRequestId',) if kind=='reservation-intent' else ('receiptId','reservationId')):e._bytes64(value[field],32)
    if kind=='reservation-intent':
        for field in ('coordinatorAuthorityId','coordinatorEpoch'):p._slug(value[field],field)
        p._hash(value['claimBindingSha256'],'claim')
    else:
        p._slug(value['sourceRevisionId'],'source');p._hash(value['intentDigest'],'intent')
    p._environment(value['environment']);p._hash(value['inputManifestSha256'],'manifest')
    for field in ('generation','objectCount','totalSourceBytes','byteBudget'):p._integer(value[field],field)
    if value['byteBudget']<value['totalSourceBytes']:raise ValueError('budget')
    for field in ('issuedAt','expiresAt'):
        if type(value[field]) is not int or not 0<=value[field]<=p.MAX_SAFE_INTEGER:raise ValueError('time')
    return value

def _verify(value,raw,signature,*,registry,purpose,domain,now):
    if type(registry) is not e.Registry:raise ValueError('registry')
    key=registry.entries.get((value['issuer'],value['keyId'],purpose))
    if (key is None or key.revoked or (key.audience,key.operation)!=(value['audience'],value['operation'])
            or not key.not_before<=value['issuedAt']<=now<value['expiresAt']<=key.not_after
            or value['expiresAt']-value['issuedAt']>300):raise ValueError('key/time')
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
    Ed25519PublicKey.from_public_bytes(key.public_key).verify(e._bytes64(signature,64),domain+raw)

def verify_pair(intent_raw,intent_signature,receipt_raw,receipt_signature,*,registry,expected_intent,expected_receipt,now):
    """Verify exact synthetic pair against independent expected bodies, not authority."""
    if QUALIFIED is not True:raise ReservationHeld('reservation is not qualified')
    try:
        if type(now) is not int or not 0<=now<=p.MAX_SAFE_INTEGER or type(expected_intent) is not dict or type(expected_receipt) is not dict:raise ValueError('context')
        intent=_parse(intent_raw,INTENT,'reservation-intent');receipt=_parse(receipt_raw,RECEIPT,'reservation-receipt')
        if e._canonical(expected_intent)!=intent_raw or e._canonical(expected_receipt)!=receipt_raw:raise ValueError('context')
        if (intent['audience']!=intent['viewerDeploymentId'] or receipt['audience']!=receipt['coordinatorDeploymentId']
                or intent['operation']!='reserve-source' or receipt['operation']!='reserve-source-committed'
                or receipt['intentDigest']!=hashlib.sha256(INTENT_DIGEST_DOMAIN+intent_raw).hexdigest()
                or any(intent[k]!=receipt[k] or type(intent[k]) is not type(receipt[k]) for k in SHARED)):raise ValueError('binding')
        _verify(intent,intent_raw,intent_signature,registry=registry,purpose='grant',domain=INTENT_DOMAIN,now=now)
        _verify(receipt,receipt_raw,receipt_signature,registry=registry,purpose='observation',domain=RECEIPT_DOMAIN,now=now)
        return dict(intentDigest=receipt['intentDigest'],receiptSha256=hashlib.sha256(receipt_raw).hexdigest(),scope='verified_bytes_not_authority')
    except Exception:raise ReservationHeld('invalid or unverifiable reservation pair') from None
