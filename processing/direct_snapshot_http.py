"""Explicit private snapshot mTLS listener; no source custody acknowledgement.

This is not registered with a host service or existing public listener. The
caller must explicitly enable and start it after provisioning a dedicated TLS
context, immutable certificate mapping, bounded backend and network policy.
"""
import hashlib
import json
import math
import re
import socket
import ssl
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import MappingProxyType
from . import reference_protocol as p

BEGIN_LIMIT=1400000
PAGE_LIMIT=256*1024
MAX_CONNECTIONS=4
_PATH=re.compile(r'/internal/v2/reservations/([0-9a-f-]{36})/source-snapshots/([0-9a-f-]{36})/(begin|complete|pages/(?:0|[1-9][0-9]?))\Z')


def create_snapshot_server(host,port,*,enabled=False,ssl_context=None,client_scopes=None,backend=None,
                           idle_timeout=10,total_body_timeout=30,header_deadline=15,max_connections=MAX_CONNECTIONS):
    if enabled is not True:raise ValueError('private snapshot listener disabled')
    if (not isinstance(ssl_context,ssl.SSLContext) or ssl_context.protocol!=ssl.PROTOCOL_TLS_SERVER
            or ssl_context.minimum_version<ssl.TLSVersion.TLSv1_2 or ssl_context.verify_mode!=ssl.CERT_REQUIRED):
        raise ValueError('required mutual TLS server context required')
    if type(client_scopes) is not dict or not 1<=len(client_scopes)<=64:
        raise ValueError('enrolled snapshot client scopes required')
    pinned={}
    for fingerprint,scope in client_scopes.items():
        if type(fingerprint) is not str or not re.fullmatch('[a-f0-9]{64}',fingerprint):raise ValueError('invalid client fingerprint')
        p._closed(scope,('principal','environment'),'snapshot client scope')
        p._slug(scope['principal'],'principal');p._environment(scope['environment'])
        pinned[fingerprint]=(scope['principal'],scope['environment'])
    pinned=MappingProxyType(pinned)
    if (type(host) is not str or type(port) is not int or not 0<=port<=65535
            or type(idle_timeout) not in (int,float) or not 0<idle_timeout<=60
            or type(total_body_timeout) not in (int,float) or not 0<total_body_timeout<=300
            or type(header_deadline) not in (int,float) or not 0<header_deadline<=60
            or type(max_connections) is not int or not 1<=max_connections<=MAX_CONNECTIONS):
        raise ValueError('invalid snapshot listener limits')
    if backend is not None and not callable(getattr(backend,'_dispatch_from_listener',None)):
        raise ValueError('snapshot backend unavailable')

    class Handler(BaseHTTPRequestHandler):
        protocol_version='HTTP/1.1';server_version='PrivateSnapshot';sys_version=''
        def setup(self):
            self.request.settimeout(idle_timeout);super().setup()
        def log_message(self,*_args):pass
        def parse_request(self):
            try:parsed=super().parse_request()
            except BaseException:
                self.server._local.header_deadline.finish();raise
            return parsed if self.server._local.header_deadline.finish() else False
        def send_error(self,*_args,**_kwargs):self._reply(400,{'error':'invalid_request'})
        def handle_expect_100(self):self._reply(417,{'error':'expectation_not_supported'});return False
        def _reply(self,status,value):
            self.close_connection=True
            raw=json.dumps(value,separators=(',',':'),allow_nan=False).encode('utf-8')
            self.send_response(status);self.send_header('Content-Type','application/json')
            self.send_header('Cache-Control','no-store');self.send_header('Content-Length',str(len(raw)))
            self.send_header('Connection','close');self.end_headers();self.wfile.write(raw)
        def do_POST(self):
            from .coordinator_freeze import CoordinatorFrozen, request_scope
            try:
                with request_scope(getattr(backend,'queue',None),'POST',self.path):self._dispatch()
            except CoordinatorFrozen:self._reply(503,{'error':'coordinator_maintenance'})
            except Exception:
                try:self._reply(500,{'error':'request_unavailable'})
                except (OSError,ssl.SSLError):pass
        def _dispatch(self):
            with self.server._activity:
                draining=self.server._draining
            if draining:self._reply(503,{'error':'snapshot_draining'});return
            match=_PATH.fullmatch(self.path)
            if match is None:self._reply(404,{'error':'not_found'});return
            try:p._uuid(match[1],'reservation');p._uuid(match[2],'snapshot')
            except ValueError:self._reply(400,{'error':'invalid_request'});return
            if (self.headers.get_all('Transfer-Encoding') or self.headers.get_all('Expect')
                    or self.headers.get_all('X-Client-Principal') or self.headers.get_all('X-Forwarded-Client-Cert')
                    or len(self.headers.get_all('Content-Length',[]))!=1):
                self._reply(400,{'error':'invalid_request'});return
            lengths=self.headers.get_all('Content-Length');length=lengths[0]
            if not re.fullmatch(r'0|[1-9][0-9]{0,7}',length):self._reply(400,{'error':'invalid_request'});return
            length=int(length);operation=match[3]
            if operation.startswith('pages/') and int(operation[6:])>=64:
                self._reply(400,{'error':'invalid_request'});return
            maximum=0 if operation=='complete' else PAGE_LIMIT if operation.startswith('pages/') else BEGIN_LIMIT
            if length>maximum:self._reply(413,{'error':'request_too_large'});return
            deadline=time.monotonic()+total_body_timeout;chunks=[];remaining=length
            while remaining:
                budget=deadline-time.monotonic()
                if budget<=0:self._reply(408,{'error':'request_timeout'});return
                self.connection.settimeout(min(idle_timeout,budget))
                try:chunk=self.rfile.read1(min(65536,remaining))
                except (TimeoutError,OSError):self._reply(408,{'error':'request_timeout'});return
                if not chunk:self._reply(400,{'error':'truncated_request'});return
                chunks.append(chunk);remaining-=len(chunk)
            scope=getattr(self.server._local,'scope',None)
            if scope is None:self._reply(403,{'error':'client_unavailable'});return
            if backend is None:self._reply(503,{'error':'snapshot_backend_unavailable'});return
            result=backend._dispatch_from_listener(scope,match[1],match[2],operation,b''.join(chunks))
            self._reply(result.status,result.body)
        def _unsupported(self):self._reply(405,{'error':'method_not_allowed'})
        do_GET=_unsupported;do_PUT=_unsupported;do_DELETE=_unsupported;do_PATCH=_unsupported

    class Server(ThreadingHTTPServer):
        address_family=socket.AF_INET6 if ':' in host else socket.AF_INET
        daemon_threads=True;block_on_close=False
        def __init__(self):
            self._local=threading.local();self._slots=threading.BoundedSemaphore(max_connections)
            self._activity=threading.Condition();self._draining=False;self._active_connections=0
            super().__init__((host,port),Handler)
        def drain(self,timeout):
            """Close admission and wait; never terminate an active writer."""
            if type(timeout) not in (int,float) or not math.isfinite(timeout) or timeout<0:
                raise ValueError('invalid drain timeout')
            deadline=time.monotonic()+timeout
            with self._activity:
                self._draining=True
                while self._active_connections:
                    remaining=deadline-time.monotonic()
                    if remaining<=0:return False
                    self._activity.wait(remaining)
                return True
        def get_request(self):
            connection,address=super().get_request();connection.settimeout(idle_timeout)
            return connection,address
        def process_request(self,request,address):
            with self._activity:
                if self._draining or not self._slots.acquire(blocking=False):self.shutdown_request(request);return
                self._active_connections+=1
            try:super().process_request(request,address)
            except BaseException:
                with self._activity:
                    self._active_connections-=1;self._slots.release();self._activity.notify_all()
                raise
        def process_request_thread(self,request,address):
            from .direct_worker_http import _HeaderDeadline
            wrapped=deadline=None
            try:
                wrapped=ssl_context.wrap_socket(request,server_side=True,do_handshake_on_connect=False)
                deadline=_HeaderDeadline(wrapped,header_deadline);self._local.header_deadline=deadline
                wrapped.do_handshake()
                certificate=wrapped.getpeercert(binary_form=True)
                scope=pinned.get(hashlib.sha256(certificate).hexdigest()) if certificate else None
                if scope is None:return
                self._local.scope=scope
                self.finish_request(wrapped,address)
            except Exception:self.handle_error(request,address)
            finally:
                if deadline is not None:deadline.finish()
                if hasattr(self._local,'scope'):del self._local.scope
                if hasattr(self._local,'header_deadline'):del self._local.header_deadline
                self.shutdown_request(wrapped if wrapped is not None else request)
                with self._activity:
                    self._active_connections-=1;self._slots.release();self._activity.notify_all()
        def handle_error(self,*_args):pass
    return Server()
