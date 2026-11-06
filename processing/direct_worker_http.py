"""Unwired TLS-only direct worker listener for a prebound trusted boundary.

This listener does not provision a worker, authenticate a Viewer, negotiate a
manifest, create a node reservation, or issue a Viewer acceptance. The caller
must supply the actual worker boundary and its authenticated coordinator bearer.
"""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import math
import re
import socket
import ssl
import threading
import time

from .direct_worker_boundary import DirectBoundaryDenied, DirectWorkerBoundary


class _BoundedReader:
    """Own a single request body, not the HTTP socket used for its response."""
    def __init__(self, handler, length, deadline):
        self._handler, self._remaining, self._deadline = handler, length, deadline
        self.closed = False

    def read(self, count=-1):
        if self.closed:
            raise ValueError('request body already retired')
        if type(count) is not int or count < 0:
            raise ValueError('bounded read required')
        remaining_time = self._deadline - time.monotonic()
        if remaining_time <= 0:
            raise TimeoutError('request body deadline expired')
        self._handler.connection.settimeout(min(self._handler.server.idle_timeout, remaining_time))
        amount = min(count, self._remaining)
        # read1 returns after at most one buffered/raw read; a trickling client
        # must not reset the per-socket timeout inside one unbounded read loop.
        data = self._handler.rfile.read1(amount) if amount else b''
        self._remaining -= len(data)
        return data

    def close(self):
        # No other consumer receives this wrapper or its underlying stream.
        self.closed = True


def create_direct_worker_server(boundary, host, port, *, ssl_context,
                                idle_timeout=10, body_deadline=120,
                                max_object_bytes=20 * 1024**3, max_connections=4):
    """Create a bounded server without starting it or changing live services."""
    if not isinstance(boundary, DirectWorkerBoundary):
        raise ValueError('trusted worker boundary required')
    if (not isinstance(ssl_context, ssl.SSLContext)
            or ssl_context.protocol != ssl.PROTOCOL_TLS_SERVER
            or ssl_context.minimum_version < ssl.TLSVersion.TLSv1_2):
        raise ValueError('TLS server context with TLS 1.2 minimum required')
    if (type(host) is not str or type(port) is not int or not 0 <= port <= 65535
            or any(type(v) not in (int, float) or not math.isfinite(v) or v <= 0
                   for v in (idle_timeout, body_deadline))
            or type(max_object_bytes) is not int or not 1 <= max_object_bytes <= 2**53-1
            or type(max_connections) is not int or not 1 <= max_connections <= 16):
        raise ValueError('invalid worker listener limits')

    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'
        server_version, sys_version = 'DirectWorker', ''

        def setup(self):
            self.request.settimeout(self.server.idle_timeout)
            super().setup()

        def log_message(self, *_):
            pass

        def handle_expect_100(self):
            self._reply(417, {'error': 'expectation_not_supported'})
            return False

        def _reply(self, status, payload):
            self.close_connection = True
            raw = json.dumps(payload, sort_keys=True, separators=(',', ':')).encode('utf-8')
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(raw)))
            self.send_header('Connection', 'close')
            self.end_headers()
            self.wfile.write(raw)

        def _dispatch(self):
            try:
                if not isinstance(self.connection, ssl.SSLSocket) or self.connection.cipher() is None:
                    self._reply(403, {'error': 'tls_required'}); return
                if self.headers.get_all('Transfer-Encoding') or self.headers.get_all('Expect'):
                    self._reply(400, {'error': 'invalid_transfer_encoding'}); return
                for name in ('Authorization', 'X-Coordinator-Authority', 'Content-Length', 'Content-SHA256'):
                    if len(self.headers.get_all(name, [])) > 1:
                        self._reply(400, {'error': 'duplicate_header'}); return
                lengths = self.headers.get_all('Content-Length', [])
                if len(lengths) != 1 or not re.fullmatch(r'0|[1-9][0-9]{0,15}', lengths[0]):
                    self._reply(400, {'error': 'content_length_required'}); return
                length = int(lengths[0])
                if length > self.server.max_object_bytes:
                    self._reply(413, {'error': 'object_too_large'}); return
                headers = dict(self.headers.items())
                match = re.fullmatch(r'/direct/v1/objects/([A-Za-z0-9][A-Za-z0-9_-]{0,127})', self.path)
                if self.command == 'PUT' and match:
                    body = _BoundedReader(self, length, time.monotonic() + self.server.body_deadline)
                    result = boundary.put(headers, protected_transport=True, object_id=match[1],
                                          content_length=length,
                                          content_sha256=self.headers.get('Content-SHA256'), reader=body)
                    self._reply(200, result); return
                if self.command == 'POST' and self.path == '/direct/v1/complete' and length == 0:
                    result = boundary.complete(headers, protected_transport=True)
                    self._reply(200, result); return
                self._reply(404, {'error': 'not_found'})
            except DirectBoundaryDenied:
                self._reply(403, {'error': 'access_denied'})
            except (TimeoutError, OSError):
                try: self._reply(503, {'error': 'transfer_unavailable'})
                except OSError: pass
            except Exception:
                self._reply(409, {'error': 'operation_not_completed'})

        do_PUT = _dispatch
        do_POST = _dispatch

    class Server(ThreadingHTTPServer):
        address_family = socket.AF_INET6 if ':' in host else socket.AF_INET
        daemon_threads = True
        block_on_close = False

        def __init__(self):
            self.idle_timeout, self.body_deadline = idle_timeout, body_deadline
            self.max_object_bytes = max_object_bytes
            self._slots = threading.BoundedSemaphore(max_connections)
            super().__init__((host, port), Handler)

        def get_request(self):
            connection, address = self.socket.accept()
            connection.settimeout(idle_timeout)
            return connection, address

        def process_request(self, request, client_address):
            if not self._slots.acquire(blocking=False):
                self.shutdown_request(request)
                return
            try:
                super().process_request(request, client_address)
            except BaseException:
                self._slots.release()
                raise

        def process_request_thread(self, request, client_address):
            wrapped = None
            try:
                # Acquire a bounded slot before handshaking so slow TLS peers
                # cannot accumulate unbounded threads or block the accept loop.
                wrapped = ssl_context.wrap_socket(request, server_side=True)
                self.finish_request(wrapped, client_address)
            except Exception:
                self.handle_error(request, client_address)
            finally:
                self.shutdown_request(wrapped if wrapped is not None else request)
                self._slots.release()

        def handle_error(self, *_):
            pass

    return Server()
