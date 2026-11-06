"""Opt-in, single-reservation V2 worker listener factory; not runtime wired.

The caller supplies trusted, locally enrolled reservation dependencies.  This
module never accepts reservation data from a remote request or starts a thread.
"""
import ipaddress
import os
import threading
import math
import time

from . import reference_protocol as protocol
from .reference_runtime import _file, RuntimeConfigurationError
from .direct_worker_receiver_v2 import DirectWorkerBoundaryV2
from .direct_worker_http import create_direct_worker_server


def load_listener_settings(path=None):
    """Read owner-private opt-in settings; absence means disabled without I/O."""
    if path is None:
        return None
    try:
        candidate, initial = _file(path, private=True)
        fd = os.open(candidate, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
        try:
            observed = os.fstat(fd)
            if (observed.st_dev, observed.st_ino) != (initial.st_dev, initial.st_ino):
                raise ValueError('listener configuration changed')
            with os.fdopen(fd, 'rb', closefd=False) as stream:
                raw = stream.read(4097)
        finally:
            os.close(fd)
        if len(raw) > 4096:
            raise ValueError('listener configuration too large')
        value = protocol.decode_submission(raw)
        protocol._closed(value, ('schemaVersion', 'enabled', 'host', 'port'), 'V2 listener settings')
        if type(value['schemaVersion']) is not int or value['schemaVersion'] != 1 or type(value['enabled']) is not bool:
            raise ValueError('invalid listener configuration')
        # Initial integration is loopback-only until network identity and
        # installed ingress are independently qualified.
        if type(value['host']) is not str or not ipaddress.ip_address(value['host']).is_loopback:
            raise ValueError('loopback listener required')
        if type(value['port']) is not int or not 1 <= value['port'] <= 65535:
            raise ValueError('explicit listener port required')
        return value if value['enabled'] else None
    except (OSError, ValueError, TypeError, KeyError, RuntimeConfigurationError):
        raise RuntimeConfigurationError('invalid private V2 worker listener settings') from None


def create_listener(settings_path=None, *, boundary_kwargs=None, tls_context=None):
    """Construct a pinned boundary and TLS listener, or return None if disabled.

    `boundary_kwargs` and `tls_context` must come from a trusted local owner;
    neither may be populated from an HTTP request. The result owns the socket.
    """
    settings = load_listener_settings(settings_path)
    if settings is None:
        return None
    if type(boundary_kwargs) is not dict:
        raise RuntimeConfigurationError('trusted V2 boundary dependencies required')
    boundary = DirectWorkerBoundaryV2(**boundary_kwargs)
    server = create_direct_worker_server(boundary, settings['host'], settings['port'], ssl_context=tls_context)
    return ListenerOwner(server)


class ListenerOwner:
    """Own a server thread; never stop it while a handler remains admitted."""
    def __init__(self, server):
        if server is None:
            raise ValueError('enabled listener required')
        self._server = server
        self._thread = None
        self._lock = threading.Lock()
        self._closed = False
        self._stop = threading.Event()
        self._failed = threading.Event()
        self._draining = threading.Event()

    def _serve(self):
        try:
            self._server.timeout = 0.05
            while not self._stop.is_set():
                self._server.handle_request()
        except BaseException:
            self._failed.set()

    def status(self):
        """Report local listener health, never job or custody readiness."""
        if self._failed.is_set():
            return 'failed'
        if self._closed:
            return 'closed'
        if self._thread is None:
            return 'not_started'
        if self._stop.is_set():
            return 'stopping'
        if self._draining.is_set():
            return 'draining'
        return 'running' if self._thread.is_alive() else 'failed'

    def start(self):
        with self._lock:
            if self._closed or self._thread is not None:
                raise RuntimeError('listener owner already started or closed')
            thread = threading.Thread(target=self._serve, daemon=False)
            try:
                thread.start()
            except BaseException:
                self._server.server_close()
                self._closed = True
                raise
            self._thread = thread

    def close(self, timeout):
        """Drain first; on timeout keep listener draining and owner alive."""
        if type(timeout) not in (int, float) or not math.isfinite(timeout) or not 0 <= timeout <= 3600:
            raise ValueError('bounded drain timeout required')
        deadline = time.monotonic() + timeout
        if not self._lock.acquire(timeout=timeout):
            return False
        try:
            if self._closed:
                return True
            if self._thread is None:
                self._server.server_close()
                self._closed = True
                return True
            self._draining.set()
            if not self._server.drain(max(0, deadline - time.monotonic())):
                return False
            self._stop.set()
            self._thread.join(max(0, deadline - time.monotonic()))
            if self._thread.is_alive():
                return False
            self._server.server_close()
            self._closed = True
            return True
        finally:
            self._lock.release()
