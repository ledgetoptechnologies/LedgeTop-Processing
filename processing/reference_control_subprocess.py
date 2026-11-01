"""Opt-in process-isolated control transport; inactive unless explicitly installed.

The child is owned by the request and normally reaped before returning; an OS
that fails to reap after termination produces a transport error. TLS settings are
explicit and independent of any SSLContext supplied to ReferenceDriver.
"""
import base64
import hashlib
import json
import os
from pathlib import Path
import ssl
import subprocess
import sys
import stat
import time
import threading
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPSHandler, HTTPRedirectHandler, ProxyHandler, Request, build_opener

from .reference_protocol import MAX_BODY_BYTES

MAX_TLS_FILE = 1024 * 1024
MAX_OUTPUT = ((MAX_BODY_BYTES + 2) // 3) * 4 + 4096
REAP_TIMEOUT = 2

class ControlTransportUnavailable(Exception):
    pass

class ControlTransportHeld(Exception):
    """Process-wide uncertainty requires inspection before any restart."""
    pass

# Serializing the entire child lifetime ensures that an uncertainty is latched
# before another caller can pass the spawn boundary. Never clear this at runtime.
_PROCESS_LOCK = threading.Lock()
_PROCESS_PID = os.getpid()
_PROCESS_HELD = False
_PROCESS_UNREAPED = None


def _stop_and_reap(child):
    global _PROCESS_HELD, _PROCESS_UNREAPED
    try:
        if child.poll() is None:
            child.kill()
        child.communicate(timeout=REAP_TIMEOUT)
        if child.poll() is None:
            raise RuntimeError('child remains active')
    except BaseException:
        _PROCESS_UNREAPED = child
        _PROCESS_HELD = True
        raise ControlTransportHeld('control_child_reaping_uncertain') from None

def unreaped_child_status():
    """Return sanitized evidence of the retained handle, without clearing the latch."""
    if os.getpid() != _PROCESS_PID:
        raise ControlTransportHeld('control_child_reaping_uncertain')
    with _PROCESS_LOCK:
        if _PROCESS_UNREAPED is None:
            return None
        return {'pid': _PROCESS_UNREAPED.pid,
                'returncode': _PROCESS_UNREAPED.poll(), 'latched': _PROCESS_HELD}


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise ControlTransportUnavailable('redirect rejected')

class ProcessControlTransport:
    """Explicit CA-only trust policy; client certificates are not yet supported.

    CA digest binds the requested trust file to operator-reviewed contents. Only
    the supplied CA is trusted. This deliberately differs from a general Python
    SSLContext; callers must explicitly opt in to this policy.
    """
    def __init__(self, origin, ca_file, ca_sha256, *, client_cert=None, client_key=None, deadline=10):
        parsed = urlsplit(origin)
        if (parsed.scheme != 'https' or not parsed.hostname or parsed.username is not None
            or parsed.password is not None or parsed.path not in ('', '/') or parsed.query
            or parsed.fragment or '\\' in origin or any(ord(c) <= 32 for c in origin)):
            raise ValueError('fixed HTTPS origin required')
        if (type(ca_sha256) is not str or len(ca_sha256) != 64
            or any(c not in '0123456789abcdef' for c in ca_sha256)):
            raise ValueError('CA sha256 required')
        if client_cert is not None or client_key is not None:
            raise ValueError('client certificate support is not yet qualified')
        if type(deadline) not in (float, int) or not 0 < deadline <= 60:
            raise ValueError('bounded deadline required')
        paths = [ca_file] + ([client_cert, client_key] if client_cert else [])
        if any(type(p) is not str or not Path(p).is_absolute() for p in paths):
            raise ValueError('absolute TLS paths required')
        self.origin = origin.rstrip('/')
        self._config = dict(origin=self.origin, ca_file=ca_file, ca_sha256=ca_sha256,
                            client_cert=client_cert, client_key=client_key)
        self.deadline = deadline

    def request(self, method, suffix, bearer, payload=None, claim_token=None):
        # Never acquire a lock inherited from a possibly multithreaded parent.
        if os.getpid() != _PROCESS_PID:
            raise ControlTransportHeld('control_child_reaping_uncertain')
        expires = time.monotonic() + self.deadline
        if not _PROCESS_LOCK.acquire(timeout=self.deadline):
            raise ControlTransportUnavailable('control_transport_busy')
        try:
            if os.getpid() != _PROCESS_PID or _PROCESS_HELD:
                raise ControlTransportHeld('control_child_reaping_uncertain')
            if time.monotonic() >= expires:
                raise ControlTransportUnavailable('control_transport_busy')
            return self._request_locked(method, suffix, bearer, payload, claim_token, expires)
        finally:
            _PROCESS_LOCK.release()

    def _request_locked(self, method, suffix, bearer, payload, claim_token, expires):
        if method not in ('GET', 'POST') or not suffix.startswith('/ltds/worker/v1/') or any(c in suffix for c in ('\\', '\r', '\n', '#', '?')):
            raise ValueError('invalid control request')
        message = dict(config=self._config, method=method, suffix=suffix, bearer=bearer,
                       payload=payload, claim_token=claim_token)
        encoded = json.dumps(message, separators=(',', ':')).encode()
        if len(encoded) > MAX_BODY_BYTES * 2:
            raise ControlTransportUnavailable('request too large')
        environment = {'PYTHONNOUSERSITE': '1', 'PYTHONDONTWRITEBYTECODE': '1'}
        if os.name == 'nt' and 'SYSTEMROOT' in os.environ:
            environment['SYSTEMROOT'] = os.environ['SYSTEMROOT']
        if time.monotonic() >= expires:
            raise ControlTransportUnavailable('control_transport_busy')
        child = subprocess.Popen([sys.executable, '-m', 'processing.reference_control_subprocess'],
                                 stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                 stderr=subprocess.DEVNULL, close_fds=True, env=environment)
        try:
            try:
                output, _ = child.communicate(encoded, timeout=max(0, expires - time.monotonic()))
            except subprocess.TimeoutExpired:
                _stop_and_reap(child)
                raise ControlTransportUnavailable('deadline exceeded') from None
            if child.returncode != 0 or len(output) > MAX_OUTPUT:
                raise ControlTransportUnavailable('transport failed')
            answer = json.loads(output)
            if type(answer) is not dict or set(answer) != {'status','markers','body'}:
                raise ControlTransportUnavailable('transport response invalid')
            return answer
        except ControlTransportHeld:
            raise
        except BaseException:
            _stop_and_reap(child)
            raise


def _child():
    try:
        raw = sys.stdin.buffer.read(MAX_BODY_BYTES * 2 + 1)
        if len(raw) > MAX_BODY_BYTES * 2: return 1
        m = json.loads(raw); cfg = m['config']
        # Bounded descriptor read; reject final symlinks and non-regular files.
        # The loaded CA is the exact content whose digest was checked.
        fd = os.open(cfg['ca_file'], os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0))
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_TLS_FILE: return 1
            with os.fdopen(fd, 'rb', closefd=False) as source: ca = source.read(MAX_TLS_FILE + 1)
        finally: os.close(fd)
        if len(ca) > MAX_TLS_FILE or hashlib.sha256(ca).hexdigest() != cfg['ca_sha256']: return 1
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.load_verify_locations(cadata=ca.decode('ascii'))
        if cfg['client_cert'] is not None or cfg['client_key'] is not None: return 1
        opener = build_opener(ProxyHandler({}), _NoRedirect(), HTTPSHandler(context=context))
        headers = {'Authorization':'Bearer ' + m['bearer'], 'Accept':'application/json', 'Accept-Encoding':'identity'}
        if m['claim_token'] is not None: headers['X-Claim-Token'] = m['claim_token']
        data = None if m['payload'] is None else json.dumps(m['payload'], ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()
        if data is not None: headers['Content-Type'] = 'application/json'
        url = cfg['origin'] + m['suffix']
        req = Request(url, data=data, headers=headers, method=m['method'])
        try:
            response = opener.open(req, timeout=10)
        except HTTPError as error:
            try:
                markers=error.headers.get_all('X-LedgeTop-Reference-Inspection', [])
                if markers and markers != ['1']: markers=['invalid']
                answer = dict(status=error.code, markers=markers, body=None)
            finally: error.close()
        else:
            try:
                markers=response.headers.get_all('X-LedgeTop-Reference-Inspection', [])
                if markers: markers=['invalid']
                if (response.status != 200 or response.geturl() != url
                    or response.headers.get('Content-Type','').split(';')[0] != 'application/json'
                    or response.headers.get('Content-Encoding','identity') != 'identity'): return 1
                body=response.read(MAX_BODY_BYTES + 1)
                if len(body) > MAX_BODY_BYTES: return 1
                length=response.headers.get('Content-Length')
                if length is not None and (not length.isascii() or not length.isdecimal() or int(length) != len(body)): return 1
                answer=dict(status=200, markers=markers, body=base64.b64encode(body).decode('ascii'))
            finally: response.close()
        sys.stdout.write(json.dumps(answer, separators=(',', ':')))
        return 0
    except Exception:
        return 1

if __name__ == '__main__':
    sys.exit(_child())
