"""Unwired signed observation of an exact worker fence, not a Viewer ACK.

Issuance deliberately retains a scratch writer pin. No automatic pin release
or transfer transition is provided: losing an issuance response or process
requires independently qualified reconciliation. This module has no network
listener, trusted clock, signer provisioning, or coordinator integration.
"""
import base64
import copy
import hmac
import re
import time
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
from . import reference_protocol as p
from .direct_worker_scratch_v2 import require_fenced, settings as scratch_settings, identity as scratch_identity
from .direct_worker_receiver import check_scope
from .node_gate import NodeGate

DOMAIN=b'ltds.processing.worker-fence-proof/v2\0'
PROTOCOL='ltds.processing.worker-fence-proof/v2'
MAX_VALIDITY_SECONDS=60
FIELDS=frozenset(('protocol','challengeId','scratchBindingSha256','pinId','gateGeneration',
                  'issuedAt','expiresAt','sourceCustodyAccepted','transferAuthorized','processingAuthorized'))

class FenceProofRejected(ValueError):pass

def _body(value):
    try:
        if type(value) is not dict or set(value)!=FIELDS or value['protocol']!=PROTOCOL:raise ValueError()
        p._uuid(value['challengeId'],'challenge');p._hash(value['scratchBindingSha256'],'binding')
        if type(value['pinId']) is not str or not re.fullmatch('[a-f0-9]{32}',value['pinId']):raise ValueError()
        for field in ('gateGeneration','issuedAt','expiresAt'):p._integer(value[field],field)
        if not 1<=value['expiresAt']-value['issuedAt']<=MAX_VALIDITY_SECONDS:raise ValueError()
        if any(value[name] is not False for name in ('sourceCustodyAccepted','transferAuthorized','processingAuthorized')):raise ValueError()
        return copy.deepcopy(value)
    except (ValueError,TypeError,KeyError):raise FenceProofRejected('invalid fence proof') from None

def issue_fenced(ledger,spool,item,*,expected_worker,engine_id,coordinator_origin,gate_generation,
                 challenge_id,validity_seconds,issuer_id,key_id,private_key):
    """Sign only after holding exact gate and persistent pin; pin remains held."""
    if not isinstance(private_key,Ed25519PrivateKey):raise FenceProofRejected('signing key required')
    p._slug(issuer_id,'issuer');p._slug(key_id,'key');p._uuid(challenge_id,'challenge')
    if type(validity_seconds) is not int or not 1<=validity_seconds<=MAX_VALIDITY_SECONDS:
        raise FenceProofRejected('bounded readiness validity required')
    issued_at=int(time.time());expires_at=issued_at+validity_seconds
    item,sha,scratch_id=require_fenced(ledger,item,expected_worker=expected_worker)
    # Validate caller-supplied time bounds before taking a durable pin.
    _body(dict(protocol=PROTOCOL,challengeId=challenge_id,scratchBindingSha256=sha,pinId='0'*32,
        gateGeneration=gate_generation,issuedAt=issued_at,expiresAt=expires_at,
        sourceCustodyAccepted=False,transferAuthorized=False,processingAuthorized=False))
    with NodeGate(engine_id) as gate:
        check_scope(ledger,spool,item,sha,scratch_id,gate,coordinator_origin=coordinator_origin,gate_generation=gate_generation)
        gate._arm_direct_hold(sha[:32],sha)
        pin=ledger.begin_direct_writer(scratch_id,scratch_settings(item,scratch_id))
        # Any later failure or lost reply retains the pin; no expiry frees it.
        require_fenced(ledger,item,expected_worker=expected_worker)
        body=_body(dict(protocol=PROTOCOL,challengeId=challenge_id,scratchBindingSha256=sha,pinId=pin,
            gateGeneration=gate_generation,issuedAt=issued_at,expiresAt=expires_at,
            sourceCustodyAccepted=False,transferAuthorized=False,processingAuthorized=False))
        envelope=dict(protocol=PROTOCOL,issuerId=issuer_id,keyId=key_id,body=body)
        return dict(envelope,signature=base64.urlsafe_b64encode(private_key.sign(DOMAIN+p.canonical_json(envelope))).decode().rstrip('='))

def verify(proof,*,expected_item,expected_worker,challenge_id,gate_generation,issuer_id,key_id,public_key,now):
    """Verify a short-lived observation; does not prove current fence or custody."""
    if not isinstance(public_key,Ed25519PublicKey):raise FenceProofRejected('pinned worker key required')
    _,sha,_=scratch_identity(expected_item,expected_worker=expected_worker)
    if (type(proof) is not dict or set(proof)!={'protocol','issuerId','keyId','body','signature'}
            or proof['protocol']!=PROTOCOL or proof['issuerId']!=issuer_id or proof['keyId']!=key_id):
        raise FenceProofRejected('worker proof identity changed')
    body=_body(proof['body'])
    if (type(now) is not int or now<body['issuedAt'] or now>=body['expiresAt']
            or type(gate_generation) is not int or body['gateGeneration']!=gate_generation
            or body['challengeId']!=challenge_id or not hmac.compare_digest(body['scratchBindingSha256'],sha)):
        raise FenceProofRejected('worker proof binding or time changed')
    try:
        value=proof['signature']
        if type(value) is not str or len(value)!=86:raise ValueError()
        signature=base64.b64decode(value+'==',altchars=b'-_',validate=True)
        if len(signature)!=64 or base64.urlsafe_b64encode(signature).decode().rstrip('=')!=value:raise ValueError()
        public_key.verify(signature,DOMAIN+p.canonical_json({k:v for k,v in proof.items() if k!='signature'}))
    except (ValueError,TypeError,InvalidSignature):raise FenceProofRejected('worker proof signature invalid') from None
    return copy.deepcopy(body)
