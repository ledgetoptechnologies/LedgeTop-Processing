"""Viewer TLS request deadlines: trickled headers/bodies, slow readers,
honest large transfers, bounded shutdown and slot/thread cleanup.

Loopback only with synthetic certificates and payloads; limits are shrunk to
fractions of a second so every case finishes quickly and deterministically.
"""
import contextlib
import hashlib
import io
import socket
import ssl
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler

from processing.app import create_viewer_tls_server
from processing.viewer_tls import standalone_viewer_tls_context
from processing.viewer_tls_deadlines import DeadlineSocket, ViewerTLSDeadlineExceeded
from tests import test_viewer_tls_listener as listener_tests  # module import: don't re-collect its tests

DOWNLOAD = bytes(range(256)) * 256 * 128  # 8 MiB, larger than loopback buffers


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


class ScriptedSocket:
    """recv_into returns scripted chunks, advancing a fake clock per call."""

    def __init__(self, chunks, clock, cost=0.0):
        self.chunks, self.clock, self.cost = list(chunks), clock, cost
        self.timeouts = []

    def settimeout(self, value):
        self.timeouts.append(value)

    def recv_into(self, buffer, nbytes=0, flags=0):
        self.clock.now += self.cost
        if not self.chunks:
            raise TimeoutError('synthetic wait')
        data = self.chunks.pop(0)
        buffer[:len(data)] = data
        return len(data)

    def send(self, data, flags=0):
        self.clock.now += self.cost
        return len(data)


class DeadlineSocketUnitTests(unittest.TestCase):
    def test_header_terminator_split_across_reads_is_detected(self):
        for chunks in ([b'GET / HTTP/1.0\r', b'\nHost: x\r\n\r', b'\n'],
                       [b'GET / HTTP/1.0\n', b'\n'],
                       [b'GET / HTTP/1.0\r\n', b'\r\n']):
            with self.subTest(chunks=chunks):
                clock = FakeClock()
                conn = DeadlineSocket(ScriptedSocket(chunks, clock), header_deadline=5,
                                      progress_window=5, progress_bytes=10, clock=clock)
                buffer = bytearray(64)
                for _ in chunks:
                    self.assertFalse(conn.headers_complete)
                    conn.recv_into(buffer)
                self.assertTrue(conn.headers_complete)

    def test_header_deadline_is_absolute_even_with_steady_bytes(self):
        clock = FakeClock()
        conn = DeadlineSocket(ScriptedSocket([b'G'] * 100, clock, cost=1.0), header_deadline=5,
                              progress_window=60, progress_bytes=1, clock=clock)
        conn.settimeout(10)
        buffer = bytearray(1)
        with self.assertRaises(ViewerTLSDeadlineExceeded):
            for _ in range(100):
                conn.recv_into(buffer)
        self.assertLessEqual(clock.now, 5.0)

    def test_progress_counts_only_socket_waits_and_resets_on_progress(self):
        clock = FakeClock()
        sock = ScriptedSocket([b'GET / HTTP/1.0\r\n\r\n'] + [b'x' * 4] * 50, clock, cost=1.0)
        conn = DeadlineSocket(sock, header_deadline=5, progress_window=3, progress_bytes=8, clock=clock)
        buffer = bytearray(64)
        conn.recv_into(buffer)
        for _ in range(20):
            conn.recv_into(buffer)   # 4 bytes per 1 s wait: 8 bytes per 2 s keeps up
            clock.now += 100.0       # handler compute between reads is not counted
        slow = DeadlineSocket(ScriptedSocket([b'GET / HTTP/1.0\r\n\r\n'] + [b'x'] * 50, clock, cost=1.0),
                              header_deadline=5, progress_window=3, progress_bytes=8, clock=clock)
        slow.recv_into(buffer)
        with self.assertRaises(ViewerTLSDeadlineExceeded):
            for _ in range(50):
                slow.recv_into(buffer)  # 1 byte per 1 s cannot reach 8 bytes in 3 s

    def test_waits_never_exceed_handler_timeout_or_remaining_budget(self):
        clock = FakeClock()
        sock = ScriptedSocket([b'GET / HTTP/1.0\r\n'], clock)
        conn = DeadlineSocket(sock, header_deadline=4, progress_window=30, progress_bytes=8, clock=clock)
        conn.settimeout(10)
        conn.recv_into(bytearray(64))
        clock.now = 3.5
        with self.assertRaises(ViewerTLSDeadlineExceeded):
            conn.recv_into(bytearray(64))
        self.assertEqual(sock.timeouts[0], 4)
        self.assertAlmostEqual(sock.timeouts[1], 0.5)
        self.assertEqual(conn.gettimeout(), 10)

    def test_sendall_fails_closed_when_send_moves_nothing(self):
        clock = FakeClock()
        request = b'GET / HTTP/1.0\r\n\r\n'
        zero = ScriptedSocket([request], clock)
        zero_calls = []
        zero.send = lambda data, flags=0: zero_calls.append(len(data)) or 0
        conn = DeadlineSocket(zero, header_deadline=5, progress_window=30, progress_bytes=8, clock=clock)
        conn.recv_into(bytearray(64))
        with self.assertRaises(ViewerTLSDeadlineExceeded):
            conn.sendall(b'x' * 40000)
        self.assertEqual(len(zero_calls), 1)  # fails at once; no spin until the window
        partial = ScriptedSocket([request], clock)
        sizes = []
        partial.send = lambda data, flags=0: sizes.append(len(data)) or min(len(data), 1000)
        conn = DeadlineSocket(partial, header_deadline=5, progress_window=30, progress_bytes=8, clock=clock)
        conn.recv_into(bytearray(64))
        conn.sendall(b'x' * 40000)  # short but non-zero sends still complete
        self.assertEqual(len(sizes), 40)
        self.assertLessEqual(max(sizes), 16 * 1024)

    def test_invalid_limits_are_rejected(self):
        for values in ((0, 1, 1), (1, 0, 1), (1, 1, 0)):
            with self.subTest(values=values), self.assertRaises(ValueError):
                DeadlineSocket(object(), header_deadline=values[0], progress_window=values[1],
                               progress_bytes=values[2])


class ViewerTlsDeadlineListenerTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.cert, key = listener_tests.ViewerTlsListenerTests._pair(directory.name, 'deadline')
        self.client = ssl.create_default_context(cafile=str(self.cert))
        self.events = []
        events = self.events

        class Handler(BaseHTTPRequestHandler):
            reference_listener = None

            def setup(self):
                super().setup()
                self.connection.settimeout(10)  # as processing.app's Handler

            def do_GET(self):
                self.send_response(200)
                self.send_header('Content-Length', str(len(DOWNLOAD) if self.path == '/big' else 2))
                self.end_headers()
                if self.path == '/big':
                    for offset in range(0, len(DOWNLOAD), 64 * 1024):
                        self.wfile.write(DOWNLOAD[offset:offset + 64 * 1024])
                    events.append('download_finished')
                else:
                    self.wfile.write(b'ok')

            def do_POST(self):
                remaining, digest = int(self.headers['Content-Length']), hashlib.sha256()
                while remaining:
                    chunk = self.rfile.read(min(remaining, 64 * 1024))
                    if not chunk:
                        return
                    digest.update(chunk)
                    remaining -= len(chunk)
                    if self.path == '/compute':
                        time.sleep(.15)  # handler work longer than the progress window
                body = digest.hexdigest().encode()
                self.send_response(200)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_):
                pass

        context = standalone_viewer_tls_context(str(self.cert), str(key))
        self.server = create_viewer_tls_server(Handler, '127.0.0.1', 0, server_context=context)
        self.server.daemon_threads, self.server.block_on_close = False, True
        self.server._tls_slots = threading.BoundedSemaphore(2)
        self.server.handshake_timeout = 2.0
        self.server.header_deadline = .6
        self.server.progress_window = .5
        self.server.progress_bytes = 16 * 1024
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.port = self.server.server_port
        self.stderr = io.StringIO()
        redirect = contextlib.redirect_stderr(self.stderr)
        redirect.__enter__()
        self.addCleanup(redirect.__exit__, None, None, None)
        self.addCleanup(self._close_server)

    def _close_server(self):
        if self.thread.is_alive():
            self.server.shutdown()
            self.thread.join(timeout=5)
            self.server.server_close()
        self.assertFalse(self.thread.is_alive())

    def _tls(self):
        return self.client.wrap_socket(socket.create_connection(('127.0.0.1', self.port), timeout=5),
                                       server_hostname='127.0.0.1')

    def _wait_slots(self, value, limit=4.0):
        deadline = time.monotonic() + limit
        while self.server._tls_slots._value != value and time.monotonic() < deadline:
            time.sleep(.01)
        self.assertEqual(self.server._tls_slots._value, value)

    def _honest_get(self):
        with self._tls() as conn:
            conn.sendall(b'GET /small HTTP/1.0\r\n\r\n')
            data = b''
            while chunk := conn.recv(4096):
                data += chunk
        self.assertTrue(data.startswith(b'HTTP/1.0 200'))
        self.assertTrue(data.endswith(b'ok'))

    @staticmethod
    def _closed(conn):
        try:
            return conn.recv(1) == b''
        except (ConnectionError, ssl.SSLError, OSError):
            return True

    def _trickle(self, conns, payload, stop_after):
        """Send payload one byte at a time to every conn until each is closed."""
        started = time.monotonic()
        alive = set(range(len(conns)))
        for index in range(len(payload)):
            for number in list(alive):
                try:
                    conns[number].send(payload[index:index + 1])
                except (ConnectionError, ssl.SSLError, OSError):
                    alive.discard(number)
            if not alive or time.monotonic() - started > stop_after:
                break
            time.sleep(.1)
        return time.monotonic() - started, alive

    def test_trickled_headers_are_cut_and_honest_client_is_served(self):
        trick = [self._tls() for _ in range(2)]
        try:
            self._wait_slots(0)
            elapsed, alive = self._trickle(trick, b'GET /small HTTP/1.0\r\nX-Pad: ' + b'a' * 200, 5)
            self.assertFalse(alive)
            self.assertLess(elapsed, 2.5)
        finally:
            for conn in trick:
                conn.close()
        self._wait_slots(2)
        self._honest_get()
        self.assertEqual(self.stderr.getvalue(), '')

    def test_trickled_small_body_is_cut_by_progress_rule(self):
        conn = self._tls()
        try:
            conn.sendall(b'POST /ack HTTP/1.0\r\nContent-Length: 400\r\n\r\n')
            elapsed, alive = self._trickle([conn], b'{' + b'a' * 398 + b'}', 5)
            self.assertFalse(alive)
            self.assertLess(elapsed, 2.5)
        finally:
            conn.close()
        self._wait_slots(2)
        self._honest_get()
        self.assertEqual(self.stderr.getvalue(), '')

    def test_stalled_download_reader_is_cut_and_slot_released(self):
        conn = self._tls()
        try:
            conn.sendall(b'GET /big HTTP/1.0\r\n\r\n')
            conn.recv(1)  # then stop reading
            self._wait_slots(2, limit=5.0)
            self.assertNotIn('download_finished', self.events)
        finally:
            conn.close()
        self._honest_get()
        self.assertEqual(self.stderr.getvalue(), '')

    def test_honest_large_download_outlasts_every_limit(self):
        with self._tls() as conn:
            conn.sendall(b'GET /big HTTP/1.0\r\n\r\n')
            received, started, paused = bytearray(), time.monotonic(), 0
            while chunk := conn.recv(256 * 1024):
                received += chunk
                if paused < 8 and len(received) >= (paused + 1) * 256 * 1024:
                    paused += 1
                    time.sleep(.1)  # slow but steady start, longer than the header deadline
        head, _, body = bytes(received).partition(b'\r\n\r\n')
        self.assertTrue(head.startswith(b'HTTP/1.0 200'))
        self.assertEqual(hashlib.sha256(body).digest(), hashlib.sha256(DOWNLOAD).digest())
        self.assertGreater(time.monotonic() - started, self.server.header_deadline)
        self.assertIn('download_finished', self.events)

    def test_steady_upload_with_handler_compute_is_not_cut(self):
        payload = bytes(range(256)) * 4 * 64  # 64 KiB
        chunks = [payload] * 12
        for path in ('/upload', '/compute'):
            with self.subTest(path=path), self._tls() as conn:
                conn.sendall(f'POST {path} HTTP/1.0\r\nContent-Length: {len(payload) * len(chunks)}\r\n\r\n'.encode())
                started = time.monotonic()
                for chunk in chunks:
                    conn.sendall(chunk)
                    time.sleep(.1)
                data = b''
                while part := conn.recv(4096):
                    data += part
                self.assertGreater(time.monotonic() - started, self.server.progress_window)
                self.assertTrue(data.startswith(b'HTTP/1.0 200'), data[:40])
                self.assertTrue(data.endswith(hashlib.sha256(payload * len(chunks)).hexdigest().encode()))

    def test_shutdown_is_bounded_with_trickling_clients_and_threads_drain(self):
        header = self._tls()
        body = self._tls()
        body.sendall(b'POST /ack HTTP/1.0\r\nContent-Length: 400\r\n\r\n{')
        header.send(b'GET')
        self._wait_slots(0)
        try:
            started = time.monotonic()
            self.server.shutdown()
            self.thread.join(timeout=5)
            self.server.server_close()  # joins every request worker
            closed_in = time.monotonic() - started
        finally:
            header.close()
            body.close()
        self.assertLess(closed_in, 2.5)
        self.assertFalse(self.thread.is_alive())
        self.assertFalse([t for t in getattr(self.server, '_threads', []) if t.is_alive()])
        self.assertEqual(self.server._tls_slots._value, 2)
        self.assertEqual(self.stderr.getvalue(), '')


if __name__ == '__main__':
    unittest.main()
