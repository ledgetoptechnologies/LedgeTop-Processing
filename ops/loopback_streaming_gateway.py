"""Loopback-only streaming gateway prototype; not qualified for deployment.

No topology, Docker lifecycle, TLS or host access proof exists here. Fixed
Content-Length framing only. Control and data credentials are synthetic and
loopback transport is plaintext. Never use for real datasets or credentials.
"""
import hmac
import http.client
from http.server import BaseHTTPRequestHandler
import ipaddress
import threading

CHUNK = 65536
MAX_BODY = 64 * 1024**3
TIMEOUT = 10
EXCHANGE_DEADLINE = 15

class Admission:
    def __init__(self, token):
        if not isinstance(token, str) or len(token) < 32:
            raise ValueError('credential required')
        self.token = token
        self.lock = threading.Lock()
        self.opened = False
        self.active = 0

    def auth(self, candidate):
        return isinstance(candidate, str) and hmac.compare_digest(candidate, self.token)

    def set_open(self, token, value):
        if not self.auth(token) or type(value) is not bool: return False
        with self.lock: self.opened = value
        return True

    def enter(self):
        with self.lock:
            if not self.opened: return False
            self.active += 1
            return True

    def allow_request(self, token):
        with self.lock:
            return self.opened and self.auth(token)

    def leave(self):
        with self.lock: self.active -= 1

    def status(self, token):
        if not self.auth(token): return None
        with self.lock: return {'open': self.opened, 'active': self.active,
                                'drained': not self.opened and self.active == 0}


def handler_for(admission, upstream, *, connection_factory=None):
    address, port = upstream
    if not ipaddress.ip_address(address).is_loopback or type(port) is not int or not 0 < port < 65536:
        raise ValueError('numeric loopback upstream required')

    if connection_factory is None: connection_factory = http.client.HTTPConnection

    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'
        def log_message(self, *args): pass
        def setup(self):
            super().setup()
            self.connection.settimeout(TIMEOUT)
            self.admitted = admission.enter()
            self.expired = threading.Event()
            self.peer_lock = threading.Lock()
            self.upstream_socket = None
            def expire():
                self.expired.set()
                with self.peer_lock:
                    for sock in (self.connection, self.upstream_socket):
                        if sock is not None:
                            try: sock.shutdown(2)
                            except OSError: pass
            self.timer = threading.Timer(EXCHANGE_DEADLINE, expire)
            self.timer.daemon = True
            self.timer.start()
        def finish(self):
            self.timer.cancel()
            self.timer.join()
            try:
                super().finish()
            finally:
                try:
                    self.connection.close()
                finally:
                    if self.admitted:
                        self.admitted = False
                        admission.leave()
        def do_GET(self): self.forward()
        def do_POST(self): self.forward()
        def reject(self, status):
            self.send_response(status)
            self.send_header('Content-Length', '0')
            self.send_header('Connection', 'close')
            self.end_headers()
            self.close_connection = True
        def forward(self):
            token = self.headers.get('X-Synthetic-Gateway-Token')
            if not self.admitted or not admission.allow_request(token):
                try: self.reject(503)
                except OSError: pass
                return
            self.close_connection = True
            sent_headers = False
            peer = None
            try:
                lengths = self.headers.get_all('Content-Length', [])
                if (len(lengths) > 1 or self.headers.get('Transfer-Encoding') is not None
                        or (self.command == 'POST' and len(lengths) != 1)):
                    raise ValueError('request framing')
                raw = lengths[0] if lengths else '0'
                if not raw.isascii() or not raw.isdecimal(): raise ValueError('request length')
                length = int(raw)
                if length > MAX_BODY: raise ValueError('request too large')
                peer = connection_factory(address, port, timeout=TIMEOUT)
                peer.connect()
                with self.peer_lock:
                    if self.expired.is_set(): raise TimeoutError('deadline')
                    self.upstream_socket = peer.sock
                peer.putrequest(self.command, self.path, skip_accept_encoding=True)
                for name in ('Content-Type', 'Accept', 'Authorization', 'Range', 'If-None-Match', 'If-Modified-Since', 'X-Claim-Token', 'X-LedgeTop-Capability', 'X-LTDS-Job-Id', 'X-LTDS-Generation', 'X-LTDS-Environment', 'X-LTDS-Input-Manifest-Sha256', 'X-LTDS-Pipeline-Digest', 'X-LTDS-Object-Id', 'X-LTDS-Object-Sha256', 'X-LTDS-Object-Bytes'):
                    values = self.headers.get_all(name, [])
                    if len(values) > 1: raise ValueError('duplicate request header')
                    if values: peer.putheader(name, values[0])
                peer.putheader('Content-Length', str(length))
                peer.putheader('Connection', 'close')
                peer.endheaders()
                remaining = length
                while remaining:
                    block = self.rfile.read1(min(CHUNK, remaining))
                    if not block: raise ValueError('truncated request')
                    peer.send(block)
                    remaining -= len(block)
                response = peer.getresponse()
                response_lengths = response.headers.get_all('Content-Length', [])
                if (response.headers.get_all('Transfer-Encoding', []) or len(response_lengths) != 1
                        or not response_lengths[0].isascii() or not response_lengths[0].isdecimal()
                        or response.length is None or response.length > MAX_BODY):
                    raise ValueError('response framing')
                self.send_response(response.status)
                self.send_header('Content-Length', str(response.length))
                self.send_header('Connection', 'close')
                for name in ('Content-Type', 'Content-Disposition', 'ETag', 'Last-Modified', 'Accept-Ranges', 'Content-Range', 'X-Result-Generation', 'X-Result-Sha256', 'X-Result-Manifest-Sha256', 'X-Content-Type-Options'):
                    values = response.headers.get_all(name, [])
                    if len(values) > 1: raise ValueError('duplicate response header')
                    if values: self.send_header(name, values[0])
                self.end_headers()
                sent_headers = True
                remaining = response.length
                while remaining:
                    block = response.read(min(CHUNK, remaining))
                    if not block: raise ValueError('truncated response')
                    self.wfile.write(block)
                    remaining -= len(block)
                self.wfile.flush()
            except (OSError, ValueError, http.client.HTTPException):
                if not sent_headers:
                    try: self.reject(502)
                    except OSError: pass
            finally:
                if peer is not None: peer.close()
    return Handler
