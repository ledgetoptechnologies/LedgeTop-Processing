"""Disabled pinned mTLS capacity session experiment; no listener is installed.

Caller must create a verified mTLS channel and own its lifecycle. The session
only addresses server-side fixed plans. No physical storage guarantee or remote
coordinator integration is provided.
"""
import hashlib
import hmac
import json
import re
import secrets
import socket
import ssl
import struct
import time

from .crossfs_admission_candidate import CrossFsHeld
from .local_capacity_authority_candidate import LocalCapacityAuthority
from .local_capacity_protocol_candidate import MAX_MESSAGE, _read_exact, _unique

REMOTE_CAPACITY_QUALIFIED=False


def _peer(channel, pin, server_side):
    if (not isinstance(channel,ssl.SSLSocket) or channel.server_side is not server_side
            or channel.context.verify_mode!=ssl.CERT_REQUIRED
            or (not server_side and not channel.context.check_hostname)
            or type(pin) is not str or not re.fullmatch('[a-f0-9]{64}',pin)):
        raise CrossFsHeld('verified mTLS context required')
    cert=channel.getpeercert(binary_form=True)
    if not cert or not hmac.compare_digest(hashlib.sha256(cert).hexdigest(),pin):
        raise CrossFsHeld('mTLS peer identity changed')


def _read(channel,deadline):
    size=struct.unpack('!I',_read_exact(channel,4,deadline))[0]
    if not 0<size<=MAX_MESSAGE:raise CrossFsHeld('invalid capacity frame')
    return json.loads(_read_exact(channel,size,deadline),object_pairs_hook=_unique,
                      parse_constant=lambda _: (_ for _ in ()).throw(CrossFsHeld('invalid JSON')))


def _write(channel,value,deadline):
    data=json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
    if not 0<len(data)<=MAX_MESSAGE:raise CrossFsHeld('invalid capacity frame')
    remaining=deadline-time.monotonic()
    if remaining<=0:raise CrossFsHeld('capacity session deadline exceeded')
    channel.settimeout(remaining);channel.sendall(struct.pack('!I',len(data))+data)


def serve_capacity_session(channel, *, authority, plans, client_pin, timeout_seconds=3):
    """Serve exactly one request; caller owns and closes the TLS socket."""
    if REMOTE_CAPACITY_QUALIFIED is not True:raise CrossFsHeld('remote capacity not qualified')
    if (type(authority) is not LocalCapacityAuthority or type(plans) is not dict
            or type(timeout_seconds) not in (int,float) or not 0<timeout_seconds<=5):
        raise CrossFsHeld('fixed capacity authority required')
    _peer(channel,client_pin,True)
    deadline=time.monotonic()+timeout_seconds
    request=_read(channel,deadline)
    if (type(request) is not dict or set(request)!={'op','attemptId','nonce'}
            or request['op'] not in ('reserve','pin') or type(request['attemptId']) is not str
            or request['attemptId'] not in plans or type(request['nonce']) is not str
            or not re.fullmatch('[a-f0-9]{64}',request['nonce'])):
        raise CrossFsHeld('invalid remote capacity request')
    attempt=request['attemptId']
    encoded=json.dumps(plans[attempt],sort_keys=True,separators=(',',':'),allow_nan=False).encode()
    plan=json.loads(encoded,object_pairs_hook=_unique)
    if type(plan) is not dict or plan.get('attempt_id')!=attempt:
        raise CrossFsHeld('fixed capacity plan required')
    if request['op']=='pin':
        authority._check()
        with authority._gate._db() as db:
            row=db.execute('SELECT state FROM attempts WHERE id=?',(attempt,)).fetchone()
        if row is None or row[0]!='admitted':raise CrossFsHeld('no admitted reservation to recheck')
    result=authority.acquire(**plan)
    authority._check()
    binding=json.loads(authority._binding_value)
    hold_sha=authority.hold_evidence(attempt)
    _write(channel,{'attemptId':attempt,'authorityId':binding['authorityId'],'state':result['state'],
                    'planSha256':hashlib.sha256(encoded).hexdigest(),'holdSha256':hold_sha,'nonce':request['nonce']},deadline)


def request_capacity_session(channel, *, operation, attempt, plan, authority_id, server_pin, timeout_seconds=3):
    """One pinned session; result is evidence only, not a transfer grant."""
    if REMOTE_CAPACITY_QUALIFIED is not True:raise CrossFsHeld('remote capacity not qualified')
    if (operation not in ('reserve','pin') or type(attempt) is not str or type(plan) is not dict
            or plan.get('attempt_id')!=attempt or type(authority_id) is not str
            or not re.fullmatch('[a-f0-9]{32}',authority_id) or type(timeout_seconds) not in (int,float)
            or not 0<timeout_seconds<=5):raise CrossFsHeld('fixed capacity request required')
    _peer(channel,server_pin,False)
    deadline=time.monotonic()+timeout_seconds;nonce=secrets.token_hex(32)
    _write(channel,{'op':operation,'attemptId':attempt,'nonce':nonce},deadline)
    expected={'attemptId':attempt,'authorityId':authority_id,'state':'admitted',
              'planSha256':hashlib.sha256(json.dumps(plan,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest(),
              'nonce':nonce}
    received=_read(channel,deadline)
    if (type(received) is not dict or set(received)!=set(expected)|{'holdSha256'}
            or any(received.get(key)!=value for key,value in expected.items())
            or type(received['holdSha256']) is not str or not re.fullmatch('[a-f0-9]{64}',received['holdSha256'])):
        raise CrossFsHeld('remote capacity receipt changed')
    return {'state':'observed','receipt':received}
