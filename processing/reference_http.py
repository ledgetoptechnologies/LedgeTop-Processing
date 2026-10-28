"""Explicitly started, protected HTTP listener for reference metadata only."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import ipaddress
import json
import math
import re
import socket
import ssl

from .reference_api import Response
from .reference_protocol import MAX_BODY_BYTES


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

        def _reply(self, response):
            self.close_connection = True
            try:
                encoded = json.dumps(response.body, ensure_ascii=True, allow_nan=False,
                                     separators=(",", ":")).encode("utf-8")
                self.send_response(response.status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("X-Content-Type-Options", "nosniff")
                self.send_header("Content-Length", str(len(encoded)))
                self.send_header("Connection", "close")
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(encoded)
            except (OSError, ValueError):
                pass

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
            if "?" in self.path or "#" in self.path:
                self._error(400, "invalid_request_target")
                return
            if self.headers.get_all("Transfer-Encoding"):
                self._error(400, "transfer_encoding_not_supported")
                return
            for name in ("Authorization", "Content-Length", "X-Claim-Token"):
                if len(self.headers.get_all(name, [])) > 1:
                    self._error(400, "duplicate_header")
                    return
            lengths = self.headers.get_all("Content-Length", [])
            if not lengths and self.command == "POST":
                self._error(411, "content_length_required")
                return
            length = 0
            if lengths:
                text = lengths[0]
                if not re.fullmatch(r"[0-9]+", text):
                    self._error(400, "invalid_content_length")
                    return
                if len(text) > 10 or int(text) > MAX_BODY_BYTES:
                    self._error(413, "metadata_too_large")
                    return
                length = int(text)
            if self.command != "POST" and length:
                self._error(400, "unexpected_request_body")
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
