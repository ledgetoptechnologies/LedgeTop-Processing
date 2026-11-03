"""Experimental, disabled-by-default mTLS transport for the node guard.

This is not a NodeODM-compatible endpoint or a complete deployment. It does
not secure engine ingress, authenticate a job claim, spool uploads, or release
reservations. Callers supply only paths already in the adapter's private spool.
"""
import hashlib
import hmac
import json
import re
import socket
import socketserver
import ssl
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

MAX_BODY = 65536


def create_guarded_https_server(adapter, *, bind_host, port, certificate, private_key,
                                client_ca, client_fingerprint, enabled=False,
                                request_deadline=5):
    if enabled is not True:
        raise ValueError('guarded listener is disabled')
    # Until an operational ingress policy exists, do not expose this prototype
    # beyond the host. An authenticated tunnel can be tested separately.
    if bind_host != '127.0.0.1' or type(port) is not int or not 0 <= port <= 65535:
        raise ValueError('loopback bind required')
    if type(client_fingerprint) is not str or not re.fullmatch(r'[0-9a-f]{64}', client_fingerprint):
        raise ValueError('pinned client fingerprint required')
    if type(request_deadline) not in (int, float) or not 0.1 <= request_deadline <= 30:
        raise ValueError('bounded request deadline required')
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.minimum_version = ssl.TLSVersion.TLSv1_2
    tls.load_cert_chain(certificate, private_key)
    tls.load_verify_locations(cafile=client_ca)
    tls.verify_mode = ssl.CERT_REQUIRED

    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def log_message(self, *_):
            pass

        def _reply(self, status, body):
            data = json.dumps(body, separators=(',', ':')).encode()
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(data)))
            self.send_header('Connection', 'close')
            self.end_headers()
            self.wfile.write(data)
            self.close_connection = True

        def do_POST(self):
            try:
                peer = self.connection.getpeercert(binary_form=True)
            except (OSError, ValueError, AttributeError):
                self.close_connection = True
                return
            actual = hashlib.sha256(peer).hexdigest() if peer else ''
            if not hmac.compare_digest(actual, client_fingerprint):
                return self._reply(403, {'error': 'unauthorized'})
            auth = self.headers.get_all('Authorization', [])
            if len(auth) != 1 or not auth[0].startswith('Bearer '):
                return self._reply(403, {'error': 'unauthorized'})
            lengths = self.headers.get_all('Content-Length', [])
            if (self.headers.get_all('Transfer-Encoding', []) or len(lengths) != 1
                    or not re.fullmatch(r'[0-9]{1,5}', lengths[0])
                    or int(lengths[0]) > MAX_BODY):
                return self._reply(400, {'error': 'invalid request'})
            def unique(pairs):
                result = {}
                for key, value in pairs:
                    if key in result:
                        raise ValueError('duplicate field')
                    result[key] = value
                return result
            try:
                request = json.loads(self.rfile.read(int(lengths[0])), object_pairs_hook=unique,
                                     parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
                if type(request) is not dict:
                    raise ValueError()
                # The input deadline must not abort an already-issued engine
                # operation. An expired input never reaches the adapter.
                if not self.server.input_complete(self.connection):
                    self.close_connection = True
                    return
                args = dict(bearer=auth[0][7:], protected_transport=True)
                if self.path == '/v1/initialize' and set(request) == {'task'}:
                    adapter.initialize(request['task'], **args)
                    result = {'accepted': True}
                elif self.path == '/v1/upload' and set(request) == {'task', 'item'}:
                    adapter.upload(request['task'], request['item'], **args)
                    result = {'accepted': True}
                elif self.path == '/v1/commit' and set(request) == {'task'}:
                    result = adapter.commit(request['task'], **args)
                else:
                    return self._reply(404, {'error': 'unknown operation'})
                return self._reply(200, result)
            except Exception:
                # A mutation may have happened before an error. The underlying
                # durable journal retains its reservation; callers must not retry.
                return self._reply(409, {'error': 'operation unavailable or held'})

    class Server(socketserver.ThreadingMixIn, HTTPServer):
        # Closing the listening socket is bounded even if an engine call is
        # stuck. Active mutations are NOT cancelled; wait_drained reports them.
        daemon_threads = True
        block_on_close = False

        def __init__(self, *args):
            self.slots = threading.BoundedSemaphore(4)
            self.entries = {}
            self.entries_lock = threading.Lock()
            self.drained = threading.Event()
            self.drained.set()
            super().__init__(*args)

        def handle_error(self, request, address):
            if not isinstance(sys.exc_info()[1], OSError):
                super().handle_error(request, address)

        def wait_drained(self, timeout):
            return self.drained.wait(timeout)

        def input_complete(self, connection):
            with self.entries_lock:
                entry = self.entries.get(connection)
                if not entry or entry['expired']:
                    return False
                entry['timer'].cancel()
                entry['input_complete'] = True
                return True

        def process_request(self, connection, address):
            if not self.slots.acquire(blocking=False):
                self.shutdown_request(connection)
                return
            entry = {'connection': connection, 'expired': False, 'input_complete': False}
            def expire():
                with self.entries_lock:
                    if entry['input_complete']:
                        return
                    entry['expired'] = True
                    current = entry['connection']
                    try:
                        current.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
            timer = threading.Timer(request_deadline, expire)
            timer.daemon = True
            entry['timer'] = timer
            with self.entries_lock:
                self.entries[connection] = entry
                self.drained.clear()
            timer.start()
            try:
                super().process_request(connection, address)
            except BaseException:
                with self.entries_lock:
                    self.entries.pop(connection, None)
                    if not self.entries: self.drained.set()
                timer.cancel()
                self.slots.release()
                raise

        def process_request_thread(self, connection, address):
            current = connection
            try:
                connection.settimeout(request_deadline)
                current = tls.wrap_socket(connection, server_side=True, do_handshake_on_connect=False)
                with self.entries_lock:
                    entry = self.entries.pop(connection)
                    self.entries[current] = entry
                    entry['connection'] = current
                    if entry['expired']:
                        return
                current.do_handshake()
                self.finish_request(current, address)
            except Exception:
                self.handle_error(current, address)
            finally:
                self.shutdown_request(current)
                with self.entries_lock:
                    entry = self.entries.pop(current, None) or self.entries.pop(connection, None)
                    if entry: entry['timer'].cancel()
                    if not self.entries: self.drained.set()
                self.slots.release()

    return Server((bind_host, port), Handler)
