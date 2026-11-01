import http.client
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from ops.loopback_streaming_gateway import Admission, handler_for

TOKEN = 'synthetic-' + 'x' * 40
class Upstream(BaseHTTPRequestHandler):
    claim = None
    begun = threading.Event()
    finish_event = threading.Event()
    def log_message(self, *args): pass
    def do_POST(self):
        type(self).claim = self.headers.get('X-Claim-Token')
        remaining = int(self.headers['Content-Length'])
        size = 0
        while remaining:
            part = self.rfile.read(min(65536, remaining))
            if not part: return
            size += len(part)
            remaining -= len(part)
        self.begun.set()
        if not self.finish_event.wait(3): return
        result = str(size).encode()
        self.send_response(200)
        self.send_header('Content-Length', str(len(result)))
        self.end_headers()
        self.wfile.write(result)

class Tests(unittest.TestCase):
    def test_large_upload_draining_and_unauthorized(self):
        Upstream.begun.clear()
        Upstream.finish_event.clear()
        admission = Admission(TOKEN)
        upstream = ThreadingHTTPServer(('127.0.0.1', 0), Upstream)
        upstream.daemon_threads = False
        gateway = ThreadingHTTPServer(('127.0.0.1', 0), handler_for(admission, ('127.0.0.1', upstream.server_port)))
        gateway.daemon_threads = False
        servers = [upstream, gateway]
        for server in servers: server.daemon_threads = False
        threads, client = [], None
        output, errors = [], []
        def submit():
            conn = http.client.HTTPConnection('127.0.0.1', gateway.server_port, timeout=4)
            try:
                conn.request('POST', '/', b'x' * (1024 * 1024 + 1), {'X-Synthetic-Gateway-Token': TOKEN, 'X-Claim-Token': 'synthetic-claim'})
                response = conn.getresponse()
                output.append((response.status, response.read()))
            except BaseException as exc: errors.append(exc)
            finally: conn.close()
        try:
            for server in servers:
                thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': 0.05})
                thread.start()
                threads.append(thread)
            self.assertFalse(admission.set_open('wrong', True))
            self.assertTrue(admission.set_open(TOKEN, True))
            client = threading.Thread(target=submit)
            client.start()
            self.assertTrue(Upstream.begun.wait(3))
            self.assertTrue(admission.set_open(TOKEN, False))
            self.assertEqual(admission.status(TOKEN)['active'], 1)
            denied = http.client.HTTPConnection('127.0.0.1', gateway.server_port, timeout=2)
            try:
                denied.request('GET', '/', headers={'X-Synthetic-Gateway-Token': TOKEN, 'X-Claim-Token': 'synthetic-claim'})
                self.assertEqual(denied.getresponse().status, 503)
            finally: denied.close()
            Upstream.finish_event.set()
            client.join(4)
            self.assertFalse(client.is_alive())
            self.assertEqual(errors, [])
            self.assertEqual(output, [(200, b'1048577')])
            self.assertEqual(Upstream.claim, 'synthetic-claim')
        finally:
            Upstream.finish_event.set()
            if client is not None and client.ident is not None: client.join(4)
            for index, server in enumerate(servers):
                if index < len(threads): server.shutdown()
            for thread in threads: thread.join(4)
            for server in servers: server.server_close()
        self.assertTrue(admission.status(TOKEN)['drained'])

if __name__ == '__main__': unittest.main()

class FramingAndTimeoutTests(unittest.TestCase):
    def test_slow_header_is_counted_until_deadline(self):
        import socket, time
        import ops.loopback_streaming_gateway as module
        old_deadline = module.EXCHANGE_DEADLINE
        module.EXCHANGE_DEADLINE = 0.4
        admission = Admission(TOKEN)
        admission.set_open(TOKEN, True)
        server = ThreadingHTTPServer(('127.0.0.1', 0), handler_for(admission, ('127.0.0.1', 1)))
        server.daemon_threads = False
        thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': 0.05})
        started = False
        client = None
        try:
            thread.start()
            started = True
            client = socket.create_connection(('127.0.0.1', server.server_port), timeout=2)
            client.sendall(b'POST / HTTP/1.1\r\nHost: local\r\n')
            limit = time.monotonic() + 2
            while admission.status(TOKEN)['active'] != 1 and time.monotonic() < limit:
                threading.Event().wait(0.01)
            self.assertEqual(admission.status(TOKEN)['active'], 1)
            admission.set_open(TOKEN, False)
            self.assertFalse(admission.status(TOKEN)['drained'])
            limit = time.monotonic() + 2
            while not admission.status(TOKEN)['drained'] and time.monotonic() < limit:
                threading.Event().wait(0.01)
            self.assertTrue(admission.status(TOKEN)['drained'])
        finally:
            if client is not None: client.close()
            if started: server.shutdown()
            if started: thread.join(3)
            server.server_close()
            module.EXCHANGE_DEADLINE = old_deadline
        self.assertFalse(thread.is_alive())

    def test_ambiguous_request_length_is_not_forwarded(self):
        import socket
        admission = Admission(TOKEN)
        admission.set_open(TOKEN, True)
        server = ThreadingHTTPServer(('127.0.0.1', 0), handler_for(admission, ('127.0.0.1', 1)))
        thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': 0.05})
        started = False
        try:
            thread.start(); started = True
            with socket.create_connection(('127.0.0.1', server.server_port), timeout=2) as client:
                client.sendall((f'POST / HTTP/1.1\r\nHost: local\r\nX-Synthetic-Gateway-Token: {TOKEN}\r\n'
                                'Content-Length: 0\r\nContent-Length: 1\r\n\r\n').encode())
                self.assertIn(b' 502 ', client.recv(1024))
        finally:
            if started: server.shutdown()
            if started: thread.join(3)
            server.server_close()
        self.assertFalse(thread.is_alive())

    def test_slow_downstream_holds_until_socket_deadline(self):
        import socket, time
        import ops.loopback_streaming_gateway as module
        old_deadline = module.EXCHANGE_DEADLINE
        module.EXCHANGE_DEADLINE = 0.5
        begun = threading.Event()
        class LargeResponse(BaseHTTPRequestHandler):
            def setup(self):
                super().setup()
                self.connection.settimeout(3)
            def log_message(self, *args): pass
            def do_GET(self):
                self.send_response(200)
                self.send_header('Content-Length', str(16 * 1024 * 1024))
                self.end_headers()
                begun.set()
                try:
                    for _ in range(256): self.wfile.write(b'x' * 65536)
                except OSError: pass
        admission = Admission(TOKEN)
        admission.set_open(TOKEN, True)
        upstream = ThreadingHTTPServer(('127.0.0.1', 0), LargeResponse)
        gateway = ThreadingHTTPServer(('127.0.0.1', 0), handler_for(admission, ('127.0.0.1', upstream.server_port)))
        servers = [upstream, gateway]
        for server in servers: server.daemon_threads = False
        threads, client = [], None
        try:
            for server in servers:
                thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': 0.05})
                thread.start(); threads.append(thread)
            client = socket.socket()
            client.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
            client.settimeout(2)
            client.connect(('127.0.0.1', gateway.server_port))
            client.sendall((f'GET / HTTP/1.1\r\nHost: local\r\nX-Synthetic-Gateway-Token: {TOKEN}\r\n\r\n').encode())
            self.assertTrue(begun.wait(2))
            admission.set_open(TOKEN, False)
            self.assertFalse(admission.status(TOKEN)['drained'])
            deadline = time.monotonic() + 3
            while not admission.status(TOKEN)['drained'] and time.monotonic() < deadline:
                threading.Event().wait(0.01)
            self.assertTrue(admission.status(TOKEN)['drained'])
        finally:
            if client is not None: client.close()
            for index, server in enumerate(servers):
                if index < len(threads): server.shutdown()
            for thread in threads: thread.join(3)
            for server in servers: server.server_close()
            module.EXCHANGE_DEADLINE = old_deadline
        self.assertTrue(all(not thread.is_alive() for thread in threads))



    def test_duplicate_upstream_length_rejected(self):
        import socket
        class BadResponse(BaseHTTPRequestHandler):
            def setup(self):
                super().setup()
                self.connection.settimeout(3)
            def log_message(self, *args): pass
            def do_GET(self):
                self.connection.sendall(b'HTTP/1.1 200 OK\r\nContent-Length: 0\r\nContent-Length: 1\r\n\r\n')
        admission = Admission(TOKEN)
        admission.set_open(TOKEN, True)
        upstream = ThreadingHTTPServer(('127.0.0.1', 0), BadResponse)
        gateway = ThreadingHTTPServer(('127.0.0.1', 0), handler_for(admission, ('127.0.0.1', upstream.server_port)))
        servers, threads = [upstream, gateway], []
        for server in servers: server.daemon_threads = False
        try:
            for server in servers:
                thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': 0.05})
                thread.start(); threads.append(thread)
            with socket.create_connection(('127.0.0.1', gateway.server_port), timeout=2) as client:
                client.sendall((f'GET / HTTP/1.1\r\nHost: local\r\nX-Synthetic-Gateway-Token: {TOKEN}\r\n\r\n').encode())
                self.assertIn(b' 502 ', client.recv(1024))
        finally:
            for index, server in enumerate(servers):
                if index < len(threads): server.shutdown()
            for thread in threads: thread.join(3)
            for server in servers: server.server_close()
        self.assertTrue(all(not thread.is_alive() for thread in threads))
