"""Sender-only socket watchdog; no strict wall-clock execution bound.

TLS wrapping can detach the raw socket before the replacement SSL socket is
registered. Expiry during that gap may miss shutdown until the next deadline
check. DNS, filesystem operations, timer scheduling and close/join can delay
return. This is a copy, not the broker transfer watchdog.
"""
import http.client
import socket
import threading
import time
from urllib.request import HTTPSHandler

class TransferResponseUnavailable(ValueError):pass

class _TransferWatchdog:
    """Interrupt active sockets at one monotonic object-transfer deadline.

    DNS resolution and filesystem syscalls cannot be interrupted by this
    watchdog; an expired deadline is checked before any result is published.
    """
    def __init__(self, seconds):
        self.deadline = time.monotonic() + seconds
        self._lock = threading.Lock()
        self._connection = None
        self._socket = None
        self._expired = False
        self._timer = threading.Timer(seconds, self._expire)
        self._timer.daemon = True
        self._timer.start()

    def _expire(self):
        with self._lock:
            self._expired = True
            sockets = (self._socket, getattr(self._connection, 'sock', None))
            for sock in sockets:
                if sock is not None:
                    try: sock.shutdown(socket.SHUT_RDWR)
                    except OSError: pass

    def connection(self, connection):
        with self._lock:
            if self._expired: raise TransferResponseUnavailable('transfer deadline expired')
            self._connection = connection
            self._socket = None

    def response(self, response):
        sock = getattr(getattr(getattr(response, 'fp', None), 'raw', None), '_sock', None)
        with self._lock:
            if self._expired: raise TransferResponseUnavailable('transfer deadline expired')
            self._socket = sock

    def remaining(self):
        remaining = self.deadline - time.monotonic()
        if self._expired or remaining <= 0: raise TransferResponseUnavailable('transfer deadline expired')
        return remaining

    def expired(self):
        return self._expired or time.monotonic() >= self.deadline

    def close(self):
        self._timer.cancel()
        self._timer.join()
        with self._lock:
            self._connection = self._socket = None


class _DeadlineHTTPSConnection(http.client.HTTPSConnection):
    def __init__(self, host, *, watchdog, **kwargs):
        super().__init__(host, **kwargs)
        self._watchdog = watchdog
        watchdog.connection(self)

    def connect(self):
        # HTTPConnection assigns the raw socket before proxy tunnelling. The
        # The watchdog can interrupt tracked sockets. During wrapping the raw
    # descriptor can be detached before the SSL socket is registered.
        http.client.HTTPConnection.connect(self)
        self._watchdog.connection(self)  # DNS/connect may have exceeded the budget.
        server_hostname = self._tunnel_host or self.host
        self.sock = self._context.wrap_socket(self.sock, server_hostname=server_hostname,
                                              do_handshake_on_connect=False)
        self._watchdog.connection(self)
        self.sock.settimeout(min(self.timeout, self._watchdog.remaining()))
        self.sock.do_handshake()


class _DeadlineHTTPSHandler(HTTPSHandler):
    def __init__(self, watchdog, *, context):
        super().__init__(context=context)
        self._watchdog = watchdog

    def https_open(self, request):
        watchdog = self._watchdog
        def connection(host, **kwargs):
            return _DeadlineHTTPSConnection(host, watchdog=watchdog, **kwargs)
        response = self.do_open(connection, request, context=self._context)
        try:
            watchdog.response(response)
            return response
        except BaseException:
            response.close()
            raise


