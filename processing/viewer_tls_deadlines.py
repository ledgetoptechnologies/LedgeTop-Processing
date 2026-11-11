"""Bounded request I/O for the Viewer TLS listener.

After the TLS handshake every Viewer TLS connection is served through
``DeadlineSocket``. It enforces two limits without a fixed connection lifetime:

* Request headers must arrive completely within an absolute deadline.
* After the headers, a request body or response transfer must keep making
  progress: at least ``progress_bytes`` must move for every ``progress_window``
  seconds spent *waiting on the socket*. Time the handler spends computing,
  hashing or touching disk is not counted, so large uploads and downloads of any
  size stay valid while they keep moving.

The handler's own per-operation timeout (``settimeout``) still caps every
single socket wait. A connection that misses a limit raises
``ViewerTLSDeadlineExceeded``. That is a ``TimeoutError``, so the standard HTTP
handler closes the connection the same way it does for an ordinary timeout.
"""
from __future__ import annotations

import io
import socket
import time


class ViewerTLSDeadlineExceeded(TimeoutError):
    """A Viewer TLS connection missed a header or progress limit."""


SEND_SLICE = 16 * 1024  # One TLS record; each slice is accounted separately.


class DeadlineSocket:
    """Socket view whose waits are bounded by request-phase deadlines."""

    def __init__(self, sock, *, header_deadline, progress_window, progress_bytes,
                 clock=time.monotonic):
        if not (header_deadline > 0 and progress_window > 0 and progress_bytes > 0):
            raise ValueError('Viewer TLS deadlines must be positive')
        self._sock = sock
        self._clock = clock
        self._header_until = clock() + header_deadline
        self._headers_done = False
        self._tail = b''
        self._window = progress_window
        self._min_bytes = progress_bytes
        self._waited = 0.0
        self._moved = 0
        self._op_timeout = None

    @property
    def headers_complete(self):
        return self._headers_done

    # Phase accounting -------------------------------------------------
    def _budget(self):
        if self._headers_done:
            remaining = self._window - self._waited
        else:
            remaining = self._header_until - self._clock()
        if remaining <= 0:
            raise ViewerTLSDeadlineExceeded('viewer_tls_deadline')
        if self._op_timeout is not None:
            remaining = min(remaining, self._op_timeout)
        return remaining

    def _account(self, started, count):
        if not self._headers_done:
            return
        self._waited += self._clock() - started
        self._moved += count
        if self._moved >= self._min_bytes:
            self._waited, self._moved = 0.0, 0

    def _scan_headers(self, data):
        # HTTP headers end with an empty line; http.client accepts CRLF or LF.
        if self._headers_done or not data:
            return
        joined = self._tail + bytes(data)
        if b'\n\r\n' in joined or b'\n\n' in joined:
            self._headers_done = True
            self._waited, self._moved = 0.0, 0
        else:
            self._tail = joined[-2:]

    def _wait(self, operation, *args):
        budget = self._budget()
        self._sock.settimeout(budget)
        started = self._clock()
        try:
            return operation(*args)
        except TimeoutError:
            self._account(started, 0)
            raise ViewerTLSDeadlineExceeded('viewer_tls_deadline') from None

    # Socket surface used by socketserver/http.server -------------------
    def recv_into(self, buffer, nbytes=0, flags=0):
        if not nbytes:
            # SSLSocket.recv_into treats an explicit 0 as a zero-byte read.
            with memoryview(buffer) as view:
                nbytes = view.nbytes
        started_phase_done = self._headers_done
        started = self._clock()
        count = self._wait(self._sock.recv_into, buffer, nbytes, flags)
        if started_phase_done:
            self._account(started, count)
        else:
            with memoryview(buffer) as view:
                self._scan_headers(view[:count])
        return count

    def recv(self, bufsize, flags=0):
        buffer = bytearray(bufsize)
        count = self.recv_into(buffer, bufsize, flags)
        return bytes(buffer[:count])

    def send(self, data, flags=0):
        started = self._clock()
        count = self._wait(self._sock.send, data, flags)
        self._account(started, count)
        return count

    def sendall(self, data, flags=0):
        with memoryview(data) as view, view.cast('B') as octets:
            offset = 0
            while offset < len(octets):
                offset += self.send(octets[offset:offset + SEND_SLICE], flags)

    def settimeout(self, value):
        self._op_timeout = value

    def gettimeout(self):
        return self._op_timeout

    def makefile(self, mode='r', buffering=None, **_kwargs):
        if set(mode) - {'r', 'w', 'b'}:
            raise ValueError('unsupported Viewer TLS file mode')
        raw_mode = ('r' if 'r' in mode else '') + ('w' if 'w' in mode else '')
        raw = socket.SocketIO(self, raw_mode)
        if buffering is None or buffering < 0:
            buffering = io.DEFAULT_BUFFER_SIZE
        if buffering == 0:
            return raw
        if 'r' in raw_mode and 'w' in raw_mode:
            return io.BufferedRWPair(raw, raw, buffering)
        return io.BufferedReader(raw, buffering) if 'r' in raw_mode else io.BufferedWriter(raw, buffering)

    def _decref_socketios(self):
        # socketserver closes the underlying socket explicitly in shutdown_request.
        pass

    def __getattr__(self, name):
        return getattr(self._sock, name)
