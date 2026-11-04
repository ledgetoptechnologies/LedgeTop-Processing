"""Unmounted fixture for a complete, server-owned identity/key registry.

No transport authentication, persistence, authority configuration or runtime
route is implemented here. The public constructor always refuses operation.
"""
from copy import deepcopy
from cryptography.hazmat.primitives import serialization
from . import reference_protocol as p
from .viewer_acceptance_receipt import _load_key


class IdentityUnavailable(RuntimeError): pass
class IdentityRejected(ValueError): pass


def create_registry(*_args, **_kwargs):
    raise IdentityUnavailable('unagreed registry is disabled')


def _registry_for_fixtures(config):
    """Validate an entire synthetic registry; never accept a filtered view."""
    try:
        if type(config) is not list or not 1<=len(config)<=128:raise ValueError('invalid registry')
        scopes={};key_ids=set();key_owners={};issuer_owners={}
        for item in config:
            p._closed(item,('owner','environment','authority','service','keys'),'identity')
            p._slug(item['owner'],'owner');p._environment(item['environment']);p._uuid(item['authority'],'authority');p._slug(item['service'],'service')
            scope=(item['owner'],item['environment'],item['authority'])
            identity=scope+(item['service'],)
            if scope in scopes:raise ValueError('duplicate scope')
            if type(item['keys']) is not list or not 1<=len(item['keys'])<=32:raise ValueError('invalid keys')
            keys={}
            for key in item['keys']:
                p._closed(key,('keyId','issuerId','publicKeyPem','revoked'),'key')
                p._slug(key['keyId'],'key id');p._slug(key['issuerId'],'issuer')
                if type(key['revoked']) is not bool or key['keyId'] in key_ids:raise ValueError('duplicate key id or invalid status')
                if type(key['publicKeyPem']) is not str or not 1<=len(key['publicKeyPem'])<=8192:raise ValueError('invalid key')
                der=_load_key(key['publicKeyPem']).public_bytes(serialization.Encoding.DER,serialization.PublicFormat.SubjectPublicKeyInfo)
                if (der in key_owners and key_owners[der]!=identity) or (key['issuerId'] in issuer_owners and issuer_owners[key['issuerId']]!=identity):
                    raise ValueError('signer identity reused')
                key_ids.add(key['keyId']);key_owners[der]=identity;issuer_owners[key['issuerId']]=identity
                keys[key['keyId']]=deepcopy(key)
            scopes[scope]={'service':item['service'],'keys':keys}
        return scopes
    except (ValueError,TypeError,KeyError) as error:
        raise IdentityRejected('invalid fixture registry') from error


def _resolve_for_fixtures(registry, *, owner, environment, authority, service, key_id, issuer_id):
    """Require all independently authenticated coordinates; no default mapping."""
    if any(type(v) is not str or not v for v in (owner,environment,authority,service,key_id,issuer_id)):
        raise IdentityRejected('identity not permitted')
    entry=registry.get((owner,environment,authority))
    if entry is None or entry['service']!=service:raise IdentityRejected('identity not permitted')
    key=entry['keys'].get(key_id)
    if key is None or key['revoked'] or key['issuerId']!=issuer_id:raise IdentityRejected('identity not permitted')
    return deepcopy(key)
