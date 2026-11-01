"""Loopback-only integration tests; no real engine or credentials."""
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import shutil
import ssl
import subprocess
import tempfile
import threading
import unittest
from urllib.parse import urlsplit, parse_qs
from unittest.mock import patch

from processing.config import from_environment
from processing.gateway import gateway_headers
from processing.odx_smoke_worker import Client, SmokeError
from processing.viewer_service import NativeEngine, EngineUnavailable


class GatewayTests(unittest.TestCase):
    def test_header_transport_and_streamed_upload(self):
        secret, native_token = 'G' * 40, 'N' * 40
        seen = []
        task_id = '00000000-0000-4000-8000-000000000001'

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def handle_request(self):
                path = urlsplit(self.path)
                query = parse_qs(path.query)
                length = int(self.headers.get('Content-Length', 0))
                body = self.rfile.read(length) if length else b''
                seen.append((self.command, path.path, query, self.headers.get('X-LedgeTop-Gateway'), body))
                if self.headers.get('X-LedgeTop-Gateway') != secret:
                    self.send_response(403); self.end_headers(); return
                if path.path == '/reflect_gateway':
                    payload, media = json.dumps({'value': secret}).encode(), 'application/json'
                elif path.path == '/reflect_native':
                    payload, media = json.dumps({'value': native_token}).encode(), 'application/json'
                elif path.path == '/reflect_escaped_gateway':
                    payload, media = b'{"value":"' + b'\\u0047' * 40 + b'"}', 'application/json'
                elif path.path == '/reflect_escaped_native':
                    payload, media = b'{"value":"' + b'\\u004e' * 40 + b'"}', 'application/json'
                elif path.path.endswith('/download/all.zip'):
                    payload, media = b'PK synthetic', 'application/zip'
                elif path.path.endswith('/init'):
                    payload, media = json.dumps({'uuid': self.headers.get('set-uuid')}).encode(), 'application/json'
                elif self.command == 'POST':
                    payload, media = b'{"success":true}', 'application/json'
                else:
                    payload, media = b'{"ready":true}', 'application/json'
                self.send_response(200)
                self.send_header('Content-Type', media)
                self.send_header('Content-Length', str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            do_GET = handle_request
            do_POST = handle_request

        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        fd, filename = tempfile.mkstemp(prefix='gateway-synthetic-')
        try:
            with os.fdopen(fd, 'wb') as stream:
                stream.write(b'synthetic-photo')
            engine = NativeEngine(f'http://127.0.0.1:{server.server_port}', native_token, secret)
            self.assertEqual(engine.read('/info'), {'ready': True})
            engine.initialize({'name': 'synthetic', 'options': [], 'outputs': None, 'engine_uuid': task_id})
            engine.upload(task_id, {'path': filename, 'bytes': 15, 'name': 'a.jpg',
                                    'sha256': hashlib.sha256(b'synthetic-photo').hexdigest()})
            engine.action('commit', task_id)
            with engine.download(task_id) as response:
                self.assertEqual(response.read(), b'PK synthetic')
            self.assertEqual(len(seen), 5)
            for method, path, query, header, body in seen:
                self.assertEqual(header, secret)
                self.assertEqual(query.get('token'), [native_token])
                self.assertNotIn(secret, str(query))
            self.assertIn(b'synthetic-photo', seen[2][4])
            for route in ('/reflect_gateway', '/reflect_native'):
                with self.assertRaises(EngineUnavailable) as error:
                    engine.read(route)
                self.assertNotIn(secret, str(error.exception))
                self.assertNotIn(native_token, str(error.exception))
                with self.assertRaises(SmokeError) as error:
                    engine.client.request('GET', engine.route(route))
                self.assertNotIn(secret, str(error.exception))
                self.assertNotIn(native_token, str(error.exception))
            for route in ('/reflect_escaped_gateway', '/reflect_escaped_native'):
                with self.assertRaises(EngineUnavailable): engine.read(route)
                with self.assertRaises(SmokeError): engine.client.json('GET', engine.route(route))
        finally:
            os.unlink(filename)
            server.shutdown(); server.server_close(); thread.join(5)
            self.assertFalse(thread.is_alive())

    def test_credential_is_bounded_and_never_sent_over_remote_http(self):
        for secret in (None, False, 0, 'x' * 31, 'x' * 129, 'x' * 32 + '\n'):
            with self.subTest(secret=secret):
                with self.assertRaises(ValueError):
                    gateway_headers('https://example.invalid', secret)
        for origin in ('http://192.168.50.88:3004', 'http://localhost:3004'):
            with self.subTest(origin=origin):
                with self.assertRaises(ValueError):
                    gateway_headers(origin, 'x' * 40)
        self.assertEqual(gateway_headers('http://127.0.0.1:3004', 'x' * 40),
                         {'X-LedgeTop-Gateway': 'x' * 40})
        with self.assertRaises(SmokeError):
            Client('http://127.0.0.1:1', gateway_secret='x' * 40).request(
                'GET', '/info', headers={'x-ledgetop-gateway': 'override'})

    def test_config_does_not_expose_or_reuse_gateway_credentials(self):
        base = {'PROCESSING_API_TOKEN': 'A' * 40, 'LOCAL_NODE_URL': 'https://node.example',
                'LOCAL_NODE_GATEWAY_SECRET': 'G' * 40}
        with patch.dict(os.environ, base, clear=True):
            with self.assertRaises(ValueError): from_environment()
        with patch('processing.config.GATEWAY_CLIENT_QUALIFIED', True), patch.dict(os.environ, base, clear=True):
            config = from_environment()
            self.assertNotIn('G' * 40, repr(config))
            self.assertEqual(config.local_gateway_secret, 'G' * 40)
        with patch('processing.config.GATEWAY_CLIENT_QUALIFIED', True), patch.dict(os.environ, {**base, 'MAIN_NODE_URL': 'https://other.example',
                                    'MAIN_NODE_GATEWAY_SECRET': 'G' * 40}, clear=True):
            with self.assertRaises(ValueError): from_environment()
        with patch('processing.config.GATEWAY_CLIENT_QUALIFIED', True), patch.dict(os.environ, {**base, 'LOCAL_NODE_URL': 'http://192.168.50.89:3001'}, clear=True):
            with self.assertRaises(ValueError): from_environment()
        with patch('processing.config.GATEWAY_CLIENT_QUALIFIED', True), patch.dict(os.environ, {**base, 'LOCAL_NODE_GATEWAY_SECRET': 'A' * 40}, clear=True):
            with self.assertRaises(ValueError): from_environment()

    @unittest.skipUnless(os.name == 'posix' and shutil.which('openssl'), 'POSIX openssl fixture required')
    def test_https_validates_certificate_and_hostname_before_sending_header(self):
        # Synthetic certificate and files are retained for ownership inspection.
        root = Path(tempfile.mkdtemp(prefix='lt-gateway-tls-test-'))
        print('Retained synthetic TLS root:', root, flush=True)
        key, cert = root / 'server.key', root / 'server.crt'
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                        '-keyout', str(key), '-out', str(cert), '-days', '1',
                        '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost'],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=15)
        seen = []
        secret = 'G' * 40

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args): pass
            def do_GET(self):
                seen.append(self.headers.get('X-LedgeTop-Gateway'))
                payload = b'{"ready":true}'
                self.send_response(200); self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(payload))); self.end_headers()
                self.wfile.write(payload)

        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls.load_cert_chain(cert, key)
        class BoundedTLSServer(ThreadingHTTPServer):
            daemon_threads = False
            def get_request(self):
                sock, address = super().get_request()
                sock.settimeout(2)
                try:
                    return tls.wrap_socket(sock, server_side=True), address
                except BaseException:
                    sock.close()
                    raise
        server = BoundedTLSServer(('127.0.0.1', 0), Handler)
        thread = None
        started = False
        try:
            thread = threading.Thread(target=server.serve_forever)
            thread.start()
            started = True
            with patch.dict(os.environ, {'SSL_CERT_FILE': str(cert)}):
                valid = NativeEngine(f'https://localhost:{server.server_port}', '', secret)
                self.assertEqual(valid.read('/info'), {'ready': True})
                mismatched = NativeEngine(f'https://127.0.0.1:{server.server_port}', '', secret)
                with self.assertRaises(EngineUnavailable): mismatched.read('/info')
            self.assertEqual(seen, [secret], 'certificate mismatch must fail before HTTP header')
        finally:
            if started and thread.is_alive():
                server.shutdown()
            server.server_close()
            if started:
                thread.join(5)
                self.assertFalse(thread.is_alive())


if __name__ == '__main__':
    unittest.main()
