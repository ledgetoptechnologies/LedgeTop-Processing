"""Disabled Ed25519 interoperability experiment, not a live authority API."""
import base64
import hashlib
from dataclasses import dataclass
import json
import re
from types import MappingProxyType

QUALIFIED = False
DOMAINS = {'grant': b'ltds-v2-grant-experiment-v1\0',
           'observation': b'ltds-v2-observation-experiment-v1\0'}
FIELDS = frozenset(('version','kind','issuer','keyId','audience','operation','issuedAt',
                    'expiresAt','epoch','requestHash','deliveryId','nonce','actor','payloadSha256'))
ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z')
HEX = re.compile(r'[0-9a-f]{64}\Z')

class EnvelopeHeld(ValueError): pass

def _canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False).encode('ascii')

def _bytes64(value, size):
    if type(value) is not str or not re.fullmatch(r'[A-Za-z0-9_-]+',value): raise EnvelopeHeld('invalid base64url')
    try: result=base64.urlsafe_b64decode(value+'='*((-len(value))%4))
    except Exception: raise EnvelopeHeld('invalid base64url') from None
    if len(result)!=size or base64.urlsafe_b64encode(result).decode().rstrip('=')!=value:
        raise EnvelopeHeld('invalid base64url')
    return result

@dataclass(frozen=True)
class Key:
    issuer: str
    key_id: str
    purpose: str
    public_key: bytes  # 32-byte raw Ed25519 key; Node uses SPKI wrapper.
    not_before: int
    not_after: int
    revoked: bool = False

class Registry:
    """Immutable caller-provided snapshot; freshness is not established here."""
    def __init__(self,keys):
        if type(keys) not in (tuple,list) or not 1<=len(keys)<=128:raise EnvelopeHeld('invalid registry')
        entries={}
        for key in keys:
            if (type(key) is not Key or not ID.fullmatch(key.issuer) or not ID.fullmatch(key.key_id)
                    or key.purpose not in DOMAINS or type(key.public_key) is not bytes or len(key.public_key)!=32
                    or type(key.not_before) is not int or type(key.not_after) is not int
                    or not 0<=key.not_before<key.not_after<=2**53-1 or type(key.revoked) is not bool):
                raise EnvelopeHeld('invalid registry key')
            name=(key.issuer,key.key_id,key.purpose)
            if name in entries:raise EnvelopeHeld('duplicate registry key')
            entries[name]=key
        self.entries=MappingProxyType(entries)

def verify(raw, signature, *, payload_raw, expected_payload, registry, expected, now):
    """Verify fixed bytes and caller-supplied context; cannot establish freshness or fence."""
    if QUALIFIED is not True:raise EnvelopeHeld('envelope is not qualified')
    if (type(raw) is not bytes or not 0<len(raw)<=4096 or type(payload_raw) is not bytes or not 0<len(payload_raw)<=4096
            or type(expected_payload) is not dict or type(registry) is not Registry
            or type(expected) is not dict or set(expected)!={'kind','issuer','audience','operation','epoch','requestHash','deliveryId','nonce','actor','payloadSha256'}
            or type(now) is not int or not 0<=now<=2**53-1):raise EnvelopeHeld('invalid envelope context')
    def unique(pairs):
        result={}
        for key,value in pairs:
            if key in result:raise ValueError('duplicate')
            result[key]=value
        return result
    try:
        value=json.loads(raw.decode('ascii'),object_pairs_hook=unique,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError('float')),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError('constant')))
        if type(value) is not dict or set(value)!=FIELDS or _canonical(value)!=raw:raise ValueError('shape')
        payload=json.loads(payload_raw.decode('ascii'),object_pairs_hook=unique,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError('float')),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError('constant')))
        if (type(payload) is not dict or set(payload)!={'jobId','generation','manifestSha256','reservationId','claimLineageSha256'}
                or _canonical(payload)!=payload_raw or _canonical(expected_payload)!=payload_raw
                or hashlib.sha256(payload_raw).hexdigest()!=value['payloadSha256']):raise ValueError('payload')
        for field in ('jobId','reservationId'):
            if type(payload[field]) is not str or not ID.fullmatch(payload[field]):raise ValueError('payload identity')
        for field in ('manifestSha256','claimLineageSha256'):
            if type(payload[field]) is not str or not HEX.fullmatch(payload[field]):raise ValueError('payload hash')
        if type(payload['generation']) is not int or not 1<=payload['generation']<=2**53-1:raise ValueError('payload generation')
        if type(value['version']) is not int or value['version']!=1 or value['kind'] not in DOMAINS:raise ValueError('version')
        for field in ('issuer','keyId','audience','operation','epoch','deliveryId','nonce','actor'):
            if type(value[field]) is not str or not ID.fullmatch(value[field]):raise ValueError('id')
        for field in ('requestHash','payloadSha256'):
            if type(value[field]) is not str or not HEX.fullmatch(value[field]):raise ValueError('hash')
        if (type(value['issuedAt']) is not int or type(value['expiresAt']) is not int
                or not 0<=value['issuedAt']<=now<value['expiresAt']<=2**53-1
                or value['expiresAt']-value['issuedAt']>300):raise ValueError('time')
        if any(value.get(k)!=v or type(value.get(k)) is not type(v) for k,v in expected.items()):raise ValueError('scope')
        key=registry.entries.get((value['issuer'],value['keyId'],value['kind']))
        if key is None or key.revoked or not key.not_before<=value['issuedAt']<=now<value['expiresAt']<=key.not_after:raise ValueError('key')
        sig=_bytes64(signature,64)
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
        Ed25519PublicKey.from_public_bytes(key.public_key).verify(sig,DOMAINS[value['kind']]+raw)
        return value
    except Exception:raise EnvelopeHeld('invalid or unverifiable envelope') from None
