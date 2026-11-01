"""Inactive mutually-authenticated live coordinator session for node broker.

Callers must establish TLS with verified peer certificates and a fixed peer pin.
The session pins an actual local Queue and AuthorityLease until EOF or expiry.
It cannot stop an already-forwarded engine request or fence another node.
"""
import base64
from contextlib import contextmanager
import hashlib
import json
import os
import re
import uuid
import secrets
import socket
import ssl
import threading
import time

MAX_FRAME=100*1024


class SessionHeld(ValueError): pass


def _peer(sock, pin, server_side):
    if (type(sock) is not ssl.SSLSocket or sock.server_side is not server_side
            or sock.context.verify_mode != ssl.CERT_REQUIRED or type(pin) is not str
            or len(pin)!=64):raise SessionHeld('authenticated TLS session required')
    cert=sock.getpeercert(binary_form=True)
    if not cert or hashlib.sha256(cert).hexdigest()!=pin:raise SessionHeld('TLS peer differs')


def _remaining(deadline):
    remaining=deadline-time.monotonic()
    if remaining<=0:raise SessionHeld('session deadline exceeded')
    return remaining


def _write(sock, value, deadline):
    data=json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode('utf-8')
    if not 0<len(data)<=MAX_FRAME:raise SessionHeld('session frame exceeds bound')
    sock.settimeout(_remaining(deadline));sock.sendall(len(data).to_bytes(4,'big')+data)


def _read(sock, deadline):
    def exact(size):
        chunks=[]
        while size:
            sock.settimeout(_remaining(deadline));data=sock.recv(size)
            if not data:raise SessionHeld('session EOF')
            chunks.append(data);size-=len(data)
        return b''.join(chunks)
    size=int.from_bytes(exact(4),'big')
    if not 0<size<=MAX_FRAME:raise SessionHeld('session frame exceeds bound')
    def unique(pairs):
        out={}
        for key,value in pairs:
            if key in out:raise ValueError()
            out[key]=value
        return out
    try:return json.loads(exact(size).decode('utf-8'),object_pairs_hook=unique,
                          parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except (ValueError,UnicodeError,RecursionError):raise SessionHeld('invalid session frame') from None


def serve_coordinator(sock, *, queue, witness, secret, peer_sha256, session_seconds=60, idle_seconds=15):
    """Serve one node connection; no listener, reconnect or authority adoption."""
    try:
        return _serve_coordinator(sock,queue=queue,witness=witness,secret=secret,peer_sha256=peer_sha256,
                                  session_seconds=session_seconds,idle_seconds=idle_seconds)
    finally:sock.close()


def _serve_coordinator(sock, *, queue, witness, secret, peer_sha256, session_seconds, idle_seconds):
    from .node_mutation_broker import LocalCoordinatorAuthority, issue_init_grant
    if (type(witness) is not LocalCoordinatorAuthority or witness.queue is not queue
            or type(session_seconds) not in (int,float) or not 12<=session_seconds<=60
            or type(idle_seconds) not in (int,float) or not 12<=idle_seconds<=30
            or idle_seconds>session_seconds):
        raise SessionHeld('bounded coordinator session required')
    _peer(sock,peer_sha256,True);end=time.monotonic()+session_seconds;sequence=0
    try:
        with queue.pin_transfer(end), witness.authority_lease.held(end):
            while True:
                try:request=_read(sock,min(end,time.monotonic()+idle_seconds))
                except (OSError,SessionHeld):return
                if type(request) is not dict or set(request)!={'sequence','nonce','grant','body'}:
                    raise SessionHeld('invalid challenge')
                if (type(request['sequence']) is not int or request['sequence']!=sequence+1
                        or type(request['nonce']) is not str or len(request['nonce'])!=64
                        or any(c not in '0123456789abcdef' for c in request['nonce'])):
                    raise SessionHeld('invalid challenge')
                grant=request['grant']
                if type(grant) is not dict:raise SessionHeld('invalid grant')
                try:body=base64.b64decode(request['body'],validate=True)
                except Exception:raise SessionHeld('invalid challenge') from None
                if len(body)>65536:raise SessionHeld('challenge exceeds bound')
                witness._check()
                observed,signature=issue_init_grant(queue,job_id=grant.get('jobId'),
                    claim_token=grant.get('claimToken',''),engine_id=grant.get('engineId'),
                    gate_generation=grant.get('gateGeneration'),operation_id=grant.get('operationId'),
                    body=body,content_type=grant.get('contentType'),secret=secret,_authority_required=True)
                # Claim token is session-only and is never part of the signed grant.
                public={k:v for k,v in grant.items() if k!='claimToken'}
                if observed!=public:raise SessionHeld('coordinator grant changed')
                witness._check();sequence+=1
                _write(sock,{'sequence':sequence,'nonce':request['nonce'],'signature':signature},min(end,time.monotonic()+idle_seconds))
    finally:sock.close()


# This diagnostic endpoint does not hold the node reservation after reporting.
# It cannot open admissions and has no production listener or bootstrap.
RECOVERY_ENDPOINT_QUALIFIED = False


def serve_node_recovery(sock, *, engine_id, peer_sha256, timeout_seconds=5):
    """Answer one pinned-coordinator request with the actual local snapshot.

    Socket I/O is bounded; filesystem syscalls during inspection are not.
    """
    from .node_mutation_broker import inspect_prior_node_state
    try:
        if RECOVERY_ENDPOINT_QUALIFIED is not True:
            raise SessionHeld('node recovery endpoint is not qualified')
        if type(timeout_seconds) not in (int,float) or not 0<timeout_seconds<=10:
            raise SessionHeld('invalid recovery deadline')
        _peer(sock,peer_sha256,True)
        deadline=time.monotonic()+timeout_seconds
        request=_read(sock,deadline)
        if (type(request) is not dict or set(request)!={'version','nonce'} or type(request['version']) is not int or request['version']!=1
                or type(request['nonce']) is not str or len(request['nonce'])!=64
                or any(c not in '0123456789abcdef' for c in request['nonce'])):
            raise SessionHeld('invalid recovery request')
        report=inspect_prior_node_state(engine_id)
        _write(sock,{'version':1,'nonce':request['nonce'],'report':report},deadline)
    finally:sock.close()


def _recovery_report(report, node_id, engine_id, authority_id):
    from .node_gate import _binding, ENGINE, GateError
    def canonical_uuid(value):
        try:return type(value) is str and str(uuid.UUID(value))==value
        except (ValueError,AttributeError):return False
    if (node_id not in ('main','cluster') or type(engine_id) is not str or not ENGINE.fullmatch(engine_id)
            or not canonical_uuid(authority_id)):
        raise SessionHeld('invalid expected recovery identity')
    if (type(report) is not dict or set(report)!={'identity','gateReservation','journalReservation','operations','authorizesAdmission'}
            or report['authorizesAdmission'] is not False):raise SessionHeld('invalid recovery report')
    identity=report['identity']
    fields={'version','nodeId','engineId','authorityId','coordinatorOrigin','generation','coordinatorEpoch','gateRoot','gateLock','journal'}
    if (type(identity) is not dict or set(identity)!=fields or type(identity['version']) is not int or identity['version']!=1
            or (identity['nodeId'],identity['engineId'],identity['authorityId'])!=(node_id,engine_id,authority_id)
            or not canonical_uuid(identity['coordinatorEpoch']) or type(identity['generation']) is not int
            or not 1<=identity['generation']<=2**63-1):raise SessionHeld('invalid recovery identity')
    try:_binding(node_id,identity['coordinatorOrigin'],authority_id)
    except (GateError,TypeError,ValueError):raise SessionHeld('invalid recovery identity') from None
    for key in ('gateRoot','gateLock','journal'):
        pair=identity[key]
        if (type(pair) is not list or len(pair)!=2 or any(type(x) is not int or not 0<=x<=2**64-1 for x in pair)):
            raise SessionHeld('invalid recovery identity')
    reservation=report['gateReservation']
    if reservation is not None:
        if (type(reservation) is not dict or set(reservation)!={'receiptId','job','completionDigest'}
                or type(reservation['receiptId']) is not str or not re.fullmatch('[a-f0-9]{32}',reservation['receiptId'])):
            raise SessionHeld('invalid gate reservation')
        job=reservation['job'];digest=reservation['completionDigest']
        if job is not None:
            if (type(job) is not dict or set(job)!={'id','workerId','nodeId','authorityId'}
                    or not canonical_uuid(job['id']) or job['nodeId']!=node_id or job['authorityId']!=authority_id
                    or type(job['workerId']) is not str or not re.fullmatch('[A-Za-z0-9][A-Za-z0-9._-]{0,63}',job['workerId'])):
                raise SessionHeld('invalid gate reservation')
        if digest is not None and (job is None or type(digest) is not str or not re.fullmatch('[a-f0-9]{64}',digest)):
            raise SessionHeld('invalid gate reservation')
    stored=report['journalReservation']
    if stored is not None:
        if (type(stored) is not list or len(stored)!=8 or stored[:4]!=[authority_id,node_id,engine_id,identity['coordinatorEpoch']]
                or not canonical_uuid(stored[4]) or not canonical_uuid(stored[5]) or stored[6] not in ('production','staging')
                or type(stored[7]) is not int or stored[7]!=identity['generation']):
            raise SessionHeld('invalid journal reservation')
    operations=report['operations']
    if type(operations) is not list or len(operations)>2:raise SessionHeld('invalid node operations')
    seen_ids=set();seen_kinds=set()
    for operation in operations:
        if (type(operation) is not dict or set(operation)!={'id','operation','state'}
                or not canonical_uuid(operation['id']) or operation['operation'] not in ('init','commit')
                or operation['state'] not in ('started','complete')
                or operation['id'] in seen_ids or operation['operation'] in seen_kinds):
            raise SessionHeld('invalid node operations')
        seen_ids.add(operation['id']);seen_kinds.add(operation['operation'])
    # Incomplete intermediate writes are possible after a crash. Report them
    # as held rather than inferring ownership or a completed operation.
    if (reservation is None)!=(stored is None):
        raise SessionHeld('inconsistent node reservation')
    if stored is None:
        if operations:raise SessionHeld('operation without node reservation')
    else:
        job=reservation['job']
        if (job is None or job['id']!=stored[4] or job['workerId']!='node-broker'
                or reservation['receiptId']!=uuid.UUID(stored[4]).hex or not operations):
            raise SessionHeld('inconsistent node reservation')
    by_kind={operation['operation']:operation['state'] for operation in operations}
    if 'commit' in by_kind and by_kind.get('init')!='complete':
        raise SessionHeld('inconsistent node operations')
    if reservation is not None and reservation['completionDigest'] is not None and by_kind.get('commit')!='complete':
        raise SessionHeld('inconsistent node completion')
    return report


def request_node_recovery(sock, *, node_id, engine_id, authority_id, peer_sha256, timeout_seconds=5):
    """Read one node snapshot, tied to a configured peer and expected identity.

    This evidence is a point-in-time diagnostic and cannot permit admissions.
    """
    try:
        if RECOVERY_ENDPOINT_QUALIFIED is not True:
            raise SessionHeld('node recovery endpoint is not qualified')
        if type(timeout_seconds) not in (int,float) or not 0<timeout_seconds<=10:
            raise SessionHeld('invalid recovery deadline')
        from .node_gate import ENGINE
        try:valid_authority=type(authority_id) is str and str(uuid.UUID(authority_id))==authority_id
        except (ValueError,AttributeError):valid_authority=False
        if (node_id not in ('main','cluster') or type(engine_id) is not str or not ENGINE.fullmatch(engine_id) or not valid_authority):
            raise SessionHeld('invalid expected recovery identity')
        _peer(sock,peer_sha256,False)
        deadline=time.monotonic()+timeout_seconds;nonce=secrets.token_hex(32)
        _write(sock,{'version':1,'nonce':nonce},deadline)
        response=_read(sock,deadline)
        if (type(response) is not dict or set(response)!={'version','nonce','report'}
                or type(response['version']) is not int or response['version']!=1 or response['nonce']!=nonce or type(response['report']) is not dict):
            raise SessionHeld('invalid recovery response')
        return _recovery_report(response['report'],node_id,engine_id,authority_id)
    finally:sock.close()


class RemoteAuthoritySession:
    """Node-side single-use mTLS stream witness, tied to one init claim."""
    def __init__(self,sock,*,authority,epoch,claim_token,peer_sha256,timeout_seconds=5):
        _peer(sock,peer_sha256,False)
        if (type(claim_token) is not str or not 32<=len(claim_token)<=128 or not claim_token.isascii()
                or type(timeout_seconds) not in (int,float) or not 0<timeout_seconds<=10):
            raise SessionHeld('invalid session binding')
        self.sock=sock;self.authority=authority;self.epoch=epoch;self.claim_token=claim_token
        self.timeout=timeout_seconds;self._lock=threading.Lock();self.sequence=0;self.failed=False;self.current=None;self.pid=os.getpid()

    def _challenge(self,grant,body):
        if self.failed or os.getpid()!=self.pid:raise SessionHeld('authority session unavailable')
        sequence=self.sequence+1;nonce=secrets.token_hex(32);deadline=time.monotonic()+self.timeout
        try:
            _write(self.sock,{'sequence':sequence,'nonce':nonce,'grant':dict(grant,claimToken=self.claim_token),
                'body':base64.b64encode(body).decode('ascii')},deadline)
            value=_read(self.sock,deadline)
            if (type(value) is not dict or set(value)!={'sequence','nonce','signature'} or value['sequence']!=sequence
                    or value['nonce']!=nonce or value['signature']!=self.current[2]):raise SessionHeld('authority challenge refused')
            self.sequence=sequence
        except Exception:
            self.failed=True
            raise SessionHeld('authority session unavailable') from None

    @contextmanager
    def hold(self,grant,timeout,*,body,signature):
        if os.getpid()!=self.pid:raise SessionHeld('authority session owner changed')
        if not self._lock.acquire(timeout=timeout):raise SessionHeld('authority session busy')
        try:
            self.current=(grant,body,signature);self._challenge(grant,body)
            yield
        finally:self.current=None;self._lock.release()

    def check(self):
        if self.current is None:raise SessionHeld('authority session not held')
        self._challenge(self.current[0],self.current[1])

    def close(self):
        if os.getpid()!=self.pid:raise SessionHeld('authority session owner changed')
        self.failed=True
        try:self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:pass
        self.sock.close()
