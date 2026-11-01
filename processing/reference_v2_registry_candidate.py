"""Disabled pinned-root registry experiment. It does not anchor rollback or fence jobs."""
import base64
import json
from . import reference_v2_envelope_candidate as envelope

QUALIFIED=False
DOMAIN=b'ltds-v2-registry-experiment-v1\0'

class RegistryHeld(ValueError): pass

def load_signed(raw,signature,*,root_public_key,expected_epoch,minimum_revision,now,role_policy):
    """Return a signature-verified key snapshot, not current-authority proof.

    expected_epoch and minimum_revision must come from a separately trusted,
    rollback-resistant source. Such a source is not implemented here.
    """
    if QUALIFIED is not True:raise RegistryHeld('registry is not qualified')
    if (type(raw) is not bytes or not 0<len(raw)<=65536 or type(root_public_key) is not bytes
            or len(root_public_key)!=32 or type(expected_epoch) is not str
            or type(minimum_revision) is not int or minimum_revision<1
            or type(now) is not int or not 0<=now<=2**53-1):raise RegistryHeld('invalid registry context')
    def unique(pairs):
        value={}
        for key,item in pairs:
            if key in value:raise ValueError('duplicate')
            value[key]=item
        return value
    try:
        value=json.loads(raw.decode('ascii'),object_pairs_hook=unique,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError('float')),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError('constant')))
        if (type(value) is not dict or set(value)!={'version','epoch','revision','issuedAt','expiresAt','keys'}
                or envelope._canonical(value)!=raw or type(value['version']) is not int or value['version']!=1
                or type(value['epoch']) is not str or value['epoch']!=expected_epoch
                or not envelope.ID.fullmatch(value['epoch']) or type(value['revision']) is not int
                or not minimum_revision<=value['revision']<=2**53-1
                or type(value['issuedAt']) is not int or type(value['expiresAt']) is not int
                or not 0<=value['issuedAt']<=now<value['expiresAt']<=2**53-1
                or value['expiresAt']-value['issuedAt']>3600
                or type(value['keys']) is not list or not 1<=len(value['keys'])<=128):raise ValueError('registry shape')
        sig=envelope._bytes64(signature,64)
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
        Ed25519PublicKey.from_public_bytes(root_public_key).verify(sig,DOMAIN+raw)
        keys=[]
        for item in value['keys']:
            if type(item) is not dict or set(item)!={'issuer','keyId','purpose','audience','operation','publicKey','notBefore','notAfter','revoked'}:raise ValueError('key shape')
            # A key inherited from this snapshot cannot outlive the signed
            # snapshot even if the individual key validity is longer.
            if type(item['notBefore']) is not int or type(item['notAfter']) is not int:raise ValueError('key time')
            keys.append(envelope.Key(item['issuer'],item['keyId'],item['purpose'],envelope._bytes64(item['publicKey'],32),
                max(item['notBefore'],value['issuedAt']),min(item['notAfter'],value['expiresAt']),item['revoked'],item['audience'],item['operation']))
        return envelope.Registry(keys,role_policy=role_policy)
    except Exception:raise RegistryHeld('invalid or unverifiable registry') from None
