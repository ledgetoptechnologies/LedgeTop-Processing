"""Synthetic TLS-only subprocess deadline tests. Temp roots are retained."""
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import shutil
import ssl
import subprocess
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from processing.reference_control_subprocess import ControlTransportUnavailable, ProcessControlTransport


class DeadlineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if shutil.which('openssl') is None:
            raise unittest.SkipTest('openssl is required for synthetic TLS certificates')
        cls.root = Path(tempfile.mkdtemp(prefix='control-deadline-'))
        cls.cert = cls.root / 'server.pem'
        cls.key = cls.root / 'server.key'
        subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes',
                        '-keyout',str(cls.key),'-out',str(cls.cert),'-days','1',
                        '-subj','/CN=localhost','-addext','subjectAltName=DNS:localhost'],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                       timeout=15)
        cls.digest = hashlib.sha256(cls.cert.read_bytes()).hexdigest()

    def test_slow_headers_and_body_are_killed_and_reaped(self):
        release = threading.Event()
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                try:
                    if self.path.endswith('/headers'):
                        self.wfile.write(b'HTTP/1.1 200 OK\r\nX-Slow: '); self.wfile.flush()
                        while not release.wait(.15): self.wfile.write(b'a'); self.wfile.flush()
                    else:
                        self.send_response(200); self.send_header('Content-Type','application/json')
                        self.send_header('Content-Length','4'); self.end_headers()
                        self.wfile.write(b'{'); self.wfile.flush()
                        while not release.wait(.15): self.wfile.write(b' '); self.wfile.flush()
                except OSError: pass
            def log_message(self, *_): pass
        server = ThreadingHTTPServer(('127.0.0.1',0), Handler)
        server.daemon_threads = False
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(str(self.cert),str(self.key))
        server.socket = context.wrap_socket(server.socket,server_side=True)
        thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval':.01})
        thread.start()
        try:
            transport=ProcessControlTransport('https://localhost:'+str(server.server_port),str(self.cert),self.digest,deadline=1)
            real_popen = subprocess.Popen
            children = []
            def track_child(*args, **kwargs):
                child = real_popen(*args, **kwargs)
                children.append(child)
                return child
            with patch('processing.reference_control_subprocess.subprocess.Popen', side_effect=track_child):
                for suffix in ('headers','body'):
                    start=time.monotonic()
                    with self.assertRaises(ControlTransportUnavailable):
                        transport.request('GET','/ltds/worker/v1/'+suffix,'synthetic-test-token')
                    self.assertLess(time.monotonic()-start,3.5)
                    self.assertIsNotNone(children[-1].poll(), 'owned child must be reaped')
                    self.assertIsNotNone(children[-1].returncode)
            self.assertEqual(len(children), 2)
        finally:
            release.set(); server.shutdown(); server.server_close(); thread.join(3)
            self.assertFalse(thread.is_alive())

if __name__ == '__main__': unittest.main()
