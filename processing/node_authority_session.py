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
