"""Explicitly started, protected HTTP listener for reference metadata only."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import ipaddress
import json
import math
import re
import socket
import ssl
import threading

from .reference_api import Response
from .reference_binary import BinaryResponse
from .reference_protocol import MAX_BODY_BYTES
from .reference_limits import V2_SOURCE_INTAKE_BYTES


def _loopback(address):
    try:
        parsed = ipaddress.ip_address(address.split("%", 1)[0])
        return parsed.is_loopback or bool(getattr(parsed, "ipv4_mapped", None)
                                          and parsed.ipv4_mapped.is_loopback)
    except (ValueError, AttributeError):
        return False


def _protected(connection):
    if isinstance(connection, ssl.SSLSocket) and connection.cipher() is not None:
        return True
    return _loopback(connection.getsockname()[0]) and _loopback(connection.getpeername()[0])


def create_reference_http_server(api, host="127.0.0.1", port=0, *, ssl_context=None, request_timeout=10):
    """Create a listener; caller must explicitly call serve_forever().

    Plaintext LAN/wildcard binding is forbidden. TLS is supplied by a real
    server SSLContext; forwarding headers are irrelevant to connection trust.
    """
    if (type(host) is not str or type(port) is not int or not 0 <= port <= 65535
            or type(request_timeout) not in (int, float) or not math.isfinite(request_timeout)
            or not 0 < request_timeout <= 60):
        raise ValueError("invalid reference listener configuration")
    if ssl_context is not None and not isinstance(ssl_context, ssl.SSLContext):
        raise ValueError("TLS requires a server SSLContext")
    if ssl_context is not None and ssl_context.protocol == ssl.PROTOCOL_TLS_CLIENT:
        raise ValueError("TLS requires a server SSLContext")
    family = socket.AF_INET6 if ":" in host else socket.AF_INET
    # Check before creating a socket, including hostname resolution. The actual
    # bound address is checked again below to avoid trusting a name alone.
    if ssl_context is None:
        addresses = socket.getaddrinfo(host, port, family, socket.SOCK_STREAM)
        if not addresses or any(not _loopback(item[4][0]) for item in addresses):
            raise ValueError("plaintext reference listener requires loopback binding")

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        server_version = "ReferenceMetadata"
        sys_version = ""

        def setup(self):
            self.request.settimeout(request_timeout)
            super().setup()

        def log_message(self, *_args):
            pass

        def _reply(self, response, *, retry_after=None):
            self.close_connection = True
            if isinstance(response, BinaryResponse):
                self._binary(response)
                return
            try:
                encoded = json.dumps(response.body, ensure_ascii=True, allow_nan=False,
                                     separators=(",", ":")).encode("utf-8")
                self.send_response(response.status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("X-Content-Type-Options", "nosniff")
                self.send_header("Content-Length", str(len(encoded)))
                self.send_header("Connection", "close")
                if retry_after is not None:
                    self.send_header("Retry-After", retry_after)
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(encoded)
            except (OSError, ValueError):
                pass

        def _binary(self, response):
            # Only trusted adapters create this envelope. Never forward upstream
            # cookies, credentials, redirects or arbitrary response headers.
            close_attempted = False
            def close_once():
                nonlocal close_attempted
                if not close_attempted:
                    close_attempted = True
                    response.stream.close()
            try:
                length = response.headers.get('Content-Length', '')
                mime = response.headers.get('Content-Type', '')
                if (response.status not in (200, 206) or not re.fullmatch(r'[0-9]{1,20}', str(length))
                        or mime not in ('application/zip', 'application/octet-stream')):
                    self._error(500, 'invalid_binary_response')
                    return
                if self.command == 'HEAD':
                    # A HEAD has no body/descriptor lifetime after metadata is
                    # validated. Complete pin release before publishing success.
                    # Never replay a close whose outcome is uncertain.
                    close_once()
                elif int(length) == 0:
                    # An empty GET completes at its headers; validate EOF and
                    # release its reader before making that success observable.
                    for chunk in response.stream.chunks():
                        if not isinstance(chunk, bytes) or chunk:
                            raise ValueError('nonempty zero-length binary stream')
                    close_once()
                self.send_response(response.status)
                self.send_header('Content-Type', mime)
                self.send_header('Content-Length', str(length))
                self.send_header('Cache-Control', 'no-store')
                self.send_header('X-Content-Type-Options', 'nosniff')
                self.send_header('Connection', 'close')
                # Preserve the bounded source-range protocol metadata needed by
                # SourceRangeClient. Keep this allowlist explicit; never forward
                # arbitrary upstream headers from a binary adapter.
                for key in ('Content-Range', 'Accept-Ranges', 'ETag', 'X-Processing-Request-ID',
                            'X-Result-Sha256', 'X-Result-Manifest-Sha256'):
                    value = response.headers.get(key)
                    if value is not None and isinstance(value, str) and all(0x20 <= ord(c) <= 0x7e for c in value):
                        self.send_header(key, value)
                self.end_headers()
                if self.command != 'HEAD' and int(length):
                    count = 0
                    pending = b''
                    for chunk in response.stream.chunks():
                        if not isinstance(chunk, bytes) or count + len(chunk) > int(length):
                            raise ValueError('invalid binary stream')
                        count += len(chunk)
                        if not chunk:
                            continue
                        if pending:
                            self.wfile.write(pending)
                        self.wfile.write(chunk[:-1])
                        pending = chunk[-1:]
                    if count != int(length):
                        raise ValueError('truncated binary stream')
                    # A complete Content-Length body is observable success.
                    # Retain one byte until EOF validation and reader release
                    # finish, including failures raised after the final yield.
                    close_once()
                    if pending:
                        self.wfile.write(pending)
            except Exception:
                # Headers may already be sent: close, never append a JSON error
                # or mark delivery accepted. The consumer verifies length/hash.
                self.close_connection = True
            finally:
                close_once()

        def _error(self, status, code):
            self._reply(Response(status, {"error": code}))

        def send_error(self, code, message=None, explain=None):
            # BaseHTTPRequestHandler's parser errors must remain JSON and never
            # echo request text, unknown methods, headers or exception details.
            self._error(code, "invalid_http_request")

        def handle_expect_100(self):
            self._error(417, "expectation_not_supported")
            return False

        def _dispatch(self):
            source_put = (self.command == "PUT" and re.fullmatch(
                r"/ltds/v2/jobs/[A-Za-z0-9][A-Za-z0-9_-]{0,127}/source/"
                r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}\?generation=[1-9][0-9]{0,8}", self.path))
            log_get = False
            if self.command == 'GET' and '?' in self.path:
                from .reference_logs import request as log_request
                try:
                    log_get = any(log_request(self.path, prefix) is not None
                                  for prefix in ('/ltds/v1', '/ltds/worker/v1'))
                except ValueError:
                    pass
            if "#" in self.path or ("?" in self.path and not source_put and not log_get):
                self._error(400, "invalid_request_target")
                return
            if self.headers.get_all("Transfer-Encoding"):
                self._error(400, "transfer_encoding_not_supported")
                return
            for name in ("Authorization", "Content-Length", "X-Claim-Token", "Range"):
                if len(self.headers.get_all(name, [])) > 1:
                    self._error(400, "duplicate_header")
                    return
            lengths = self.headers.get_all("Content-Length", [])
            if not lengths and self.command in ("POST", "PUT"):
                self._error(411, "content_length_required")
                return
            length = 0
            if lengths:
                text = lengths[0]
                if not re.fullmatch(r"[0-9]+", text):
                    self._error(400, "invalid_content_length")
                    return
                body_limit = (V2_SOURCE_INTAKE_BYTES if
                              self.command == "POST" and self.path == "/ltds/v2/jobs"
                              else MAX_BODY_BYTES)
                if len(text) > 20 or (self.command != "PUT" and int(text) > body_limit):
                    self._error(413, "metadata_too_large")
                    return
                length = int(text)
            if self.command not in ("POST", "PUT") and length:
                self._error(400, "unexpected_request_body")
                return
            source_submit = self.command == "POST" and self.path == "/ltds/v2/jobs"
            source_slot_acquired = False
            if source_submit:
                source_slot_acquired = self.server.source_submit_semaphore.acquire(blocking=False)
                if not source_slot_acquired:
                    # The request body remains unread. Closing the connection
                    # avoids an attacker keeping a worker thread occupied while
                    # retaining the full metadata-body memory budget.
                    self._reply(Response(429, {"error": "source_intake_busy"}), retry_after="2")
                    return
            try:
                if self.command == "PUT":
                    if not source_put or not callable(getattr(api, "stream_source_object", None)):
                        self._error(404, "not_found")
                        return
                    try:
                        response = api.stream_source_object(self.path, dict(self.headers.items()), self.rfile, length,
                            protected_transport=_protected(self.connection))
                    except Exception:
                        self._error(500, "internal_error")
                        return
                    self._reply(response)
                    return
                try:
                    body = self.rfile.read(length) if length else b""
                    if len(body) != length:
                        self._error(400, "truncated_request_body")
                        return
                except (TimeoutError, OSError):
                    self._error(408, "request_timeout")
                    return
                try:
                    protected = _protected(self.connection)
                    response = api.handle(self.command, self.path, dict(self.headers.items()), body,
                                          protected_transport=protected)
                except Exception:
                    self._error(500, "internal_error")
                    return
                self._reply(response)
            finally:
                if source_slot_acquired:
                    self.server.source_submit_semaphore.release()

        do_GET = _dispatch
        do_POST = _dispatch
        do_HEAD = _dispatch
        do_PUT = _dispatch
        do_DELETE = _dispatch
        do_PATCH = _dispatch
        do_OPTIONS = _dispatch

    class Server(ThreadingHTTPServer):
        address_family = family
        daemon_threads = True
        block_on_close = False

        def __init__(self, *args, **kwargs):
            self.source_submit_semaphore = threading.BoundedSemaphore(1)
            super().__init__(*args, **kwargs)

        def get_request(self):
            connection, address = self.socket.accept()
            connection.settimeout(request_timeout)
            if ssl_context is not None:
                try:
                    connection = ssl_context.wrap_socket(connection, server_side=True)
                except Exception:
                    connection.close()
                    raise
            return connection, address

        def handle_error(self, request, client_address):
            # No request or exception logs: headers may contain bearer tokens.
            pass

    server = Server((host, port), Handler)
    try:
        if ssl_context is None:
            if not _loopback(server.socket.getsockname()[0]):
                raise ValueError("plaintext reference listener requires loopback binding")
    except Exception:
        server.server_close()
        raise
    return server
