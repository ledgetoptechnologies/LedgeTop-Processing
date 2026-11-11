import datetime
import ipaddress
import json
import os
import socket
import ssl
import tempfile
import threading
import time
import unittest
from urllib.error import HTTPError
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from processing.app import CoordinatorService, create_viewer_tls_server
from processing.viewer_http import ViewerFacade
from processing.viewer_tls import standalone_viewer_tls_context


class PassThroughContext:
    def __init__(self):
        self.calls = []

    def wrap_socket(self, sock, *, server_side):
        self.calls.append(server_side)
        return sock


class ViewerTlsListenerTests(unittest.TestCase):
    @staticmethod
    def _pair(directory, prefix):
        cert, key = Path(directory)/(prefix+'-cert.pem'), Path(directory)/(prefix+'-key.pem')
        private_key = ec.generate_private_key(ec.SECP256R1())
        subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'synthetic-loopback')])
        now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        certificate = (x509.CertificateBuilder().subject_name(subject).issuer_name(subject).public_key(private_key.public_key())
            .serial_number(x509.random_serial_number()).not_valid_before(now-datetime.timedelta(minutes=1))
            .not_valid_after(now+datetime.timedelta(hours=1))
            .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address('127.0.0.1'))]), critical=False)
            .add_extension(x509.BasicConstraints(ca=True,path_length=None), critical=True).sign(private_key, hashes.SHA256()))
        cert.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
        key.write_bytes(private_key.private_bytes(serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL, serialization.NoEncryption()))
        key.chmod(0o600)
        return cert, key

    def test_standalone_context_serves_verified_viewer_without_reference_runtime(self):
        with tempfile.TemporaryDirectory() as directory:
            cert, key = self._pair(directory, 'standalone')
            context = standalone_viewer_tls_context(str(cert), str(key))
            self.assertEqual(context.minimum_version, ssl.TLSVersion.TLSv1_2)
            service = SimpleNamespace(capabilities=lambda: {'engine': 'synthetic'}, options=lambda: [],
                                      accept_viewer_delivery=lambda *_: {'accepted': True})
            facade = ViewerFacade(service, 'production-test-token', 'staging-test-token')
            class Handler(BaseHTTPRequestHandler):
                reference_listener = None
                def do_GET(self):
                    if not facade.handle(self): self.send_error(404)
                def log_message(self, *_): pass
            server = create_viewer_tls_server(Handler, '127.0.0.1', 0, server_context=context)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                client = ssl.create_default_context(cafile=str(cert))
                with urllib.request.urlopen(f'https://127.0.0.1:{server.server_port}/info?token=staging-test-token',
                                            context=client, timeout=3) as response:
                    self.assertEqual(response.status, 200)
                    self.assertEqual(json.loads(response.read())['viewerAcceptanceAck']['transport'], 'https')
            finally:
                server.shutdown(); thread.join(timeout=3); server.server_close()
                self.assertFalse(thread.is_alive())

    def test_explicit_context_overrides_only_viewer_listener_reference_is_untouched(self):
        fallback, standalone = PassThroughContext(), PassThroughContext()
        handler = type('Handler', (BaseHTTPRequestHandler,), {})
        handler.reference_listener = SimpleNamespace(_server_context=fallback)
        server = create_viewer_tls_server(handler, '127.0.0.1', 0, server_context=standalone)
        try:
            self.assertTrue(server.viewer_tls)
            self.assertEqual(standalone.calls, [])
            self.assertIs(server.viewer_tls_context, standalone)
            self.assertEqual(fallback.calls, [])
            self.assertIs(handler.reference_listener._server_context, fallback)
        finally:
            server.server_close()

    def test_stalled_tls_handshake_does_not_block_valid_client_and_times_out(self):
        for use_standalone in (True, False):
            with self.subTest(standalone=use_standalone), tempfile.TemporaryDirectory() as directory:
                cert, key = self._pair(directory, 'handshake')
                context = standalone_viewer_tls_context(str(cert), str(key))
                class Handler(BaseHTTPRequestHandler):
                    reference_listener = None if use_standalone else SimpleNamespace(_server_context=context)
                    def do_GET(self):
                        self.send_response(200)
                        self.end_headers()
                        self.wfile.write(b'ok')
                    def log_message(self, *_):
                        pass
                server = create_viewer_tls_server(Handler, '127.0.0.1', 0,
                                                  server_context=context if use_standalone else None)
                server.daemon_threads, server.block_on_close = False, True
                server.handshake_timeout = 2.0
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()
                stalled = socket.create_connection(('127.0.0.1', server.server_port), timeout=2)
                try:
                    client = ssl.create_default_context(cafile=str(cert))
                    with urllib.request.urlopen(f'https://127.0.0.1:{server.server_port}/',
                                                context=client, timeout=1) as response:
                        self.assertEqual(response.read(), b'ok')
                    stalled.close()
                    server.handshake_timeout = .25
                    with socket.create_connection(('127.0.0.1', server.server_port), timeout=2) as second:
                        second.settimeout(2)
                        self.assertEqual(second.recv(1), b'')
                finally:
                    stalled.close()
                    server.shutdown()
                    thread.join(timeout=3)
                    server.server_close()
                    self.assertFalse(thread.is_alive())

    def test_stalled_connection_burst_is_bounded_and_service_recovers(self):
        with tempfile.TemporaryDirectory() as directory:
            cert, key = self._pair(directory, 'bounded')
            context = standalone_viewer_tls_context(str(cert), str(key))
            class Handler(BaseHTTPRequestHandler):
                reference_listener = None
                def do_GET(self):
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(b'ok')
                def log_message(self, *_):
                    pass
            server = create_viewer_tls_server(Handler, '127.0.0.1', 0, server_context=context)
            server.daemon_threads, server.block_on_close = False, True
            server._tls_slots = threading.BoundedSemaphore(2)
            server.handshake_timeout = 2.0
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            stalled = []
            try:
                for _ in range(2):
                    stalled.append(socket.create_connection(('127.0.0.1', server.server_port), timeout=2))
                deadline = time.monotonic() + 1
                while server._tls_slots._value and time.monotonic() < deadline:
                    time.sleep(.01)
                self.assertEqual(server._tls_slots._value, 0)
                with socket.create_connection(('127.0.0.1', server.server_port), timeout=2) as excess:
                    excess.settimeout(1)
                    try:
                        self.assertEqual(excess.recv(1), b'')
                    except ConnectionResetError:
                        pass
                self.assertLessEqual(sum(t.is_alive() for t in server._threads), 2)
                deadline = time.monotonic() + 4
                while server._tls_slots._value != 2 and time.monotonic() < deadline:
                    time.sleep(.01)
                self.assertEqual(server._tls_slots._value, 2)
                client = ssl.create_default_context(cafile=str(cert))
                with urllib.request.urlopen(f'https://127.0.0.1:{server.server_port}/',
                                            context=client, timeout=2) as response:
                    self.assertEqual(response.read(), b'ok')
            finally:
                for s in stalled:
                    s.close()
                server.shutdown()
                thread.join(timeout=3)
                server.server_close()
                self.assertFalse(thread.is_alive())
                self.assertEqual(server._tls_slots._value, 2)

    def test_failed_worker_start_closes_socket_releases_slot_and_drains(self):
        handler = type('Handler', (BaseHTTPRequestHandler,), {'reference_listener': None})
        server = create_viewer_tls_server(handler, '127.0.0.1', 0, server_context=PassThroughContext())
        server.daemon_threads, server.block_on_close = False, True
        server._tls_slots = threading.BoundedSemaphore(1)
        accepted, peer = socket.socketpair()
        try:
            with patch('processing.app.threading.Thread.start', side_effect=RuntimeError('synthetic start failure')):
                server.process_request(accepted, ('127.0.0.1', 1))
            self.assertEqual(server._tls_slots._value, 1)
            peer.settimeout(2)
            self.assertEqual(peer.recv(1), b'')
            server.server_close()  # Must not attempt to join an unstarted thread.
        finally:
            peer.close()

    def test_standalone_rejects_missing_mismatched_and_unprivate_key(self):
        with tempfile.TemporaryDirectory() as directory:
            cert, key = self._pair(directory, 'valid')
            _, other_key = self._pair(directory, 'other')
            for bad_cert, bad_key in ((cert, other_key), (cert, Path(directory)/'missing.pem'),
                                      ('relative.pem', key), (Path(directory), key)):
                with self.subTest(bad_cert=bad_cert, bad_key=bad_key), self.assertRaisesRegex(ValueError, '^Viewer TLS certificate or key is invalid$'):
                    standalone_viewer_tls_context(str(bad_cert), str(bad_key))
            if os.name != 'nt':
                link = Path(directory)/'linked-key.pem'
                link.symlink_to(key)
                with self.assertRaises(ValueError):
                    standalone_viewer_tls_context(str(cert), str(link))
                key.chmod(0o644)
                with self.assertRaises(ValueError):
                    standalone_viewer_tls_context(str(cert), str(key))

    def test_encrypted_private_key_fails_without_interactive_prompt(self):
        with tempfile.TemporaryDirectory() as directory:
            cert, _ = self._pair(directory, 'plain')
            key = Path(directory)/'encrypted-key.pem'
            private_key = ec.generate_private_key(ec.SECP256R1())
            key.write_bytes(private_key.private_bytes(serialization.Encoding.PEM,
                serialization.PrivateFormat.TraditionalOpenSSL,
                serialization.BestAvailableEncryption(b'synthetic-passphrase')))
            key.chmod(0o600)
            with self.assertRaises(ValueError):
                standalone_viewer_tls_context(str(cert), str(key))

    def test_standalone_invalid_key_fails_before_queue_creation(self):
        import processing.app as app
        config = SimpleNamespace(viewer_tls_certificate='/missing/certificate', viewer_tls_private_key='/missing/key')
        with patch.object(app, 'from_environment', return_value=config), patch.object(app, 'make_handler') as handler:
            with self.assertRaises(ValueError):
                app.main()
            handler.assert_not_called()

    def test_authenticated_viewer_capabilities_work_over_verified_tls(self):
        with tempfile.TemporaryDirectory() as directory:
            cert, key = Path(directory) / 'cert.pem', Path(directory) / 'key.pem'
            private_key = ec.generate_private_key(ec.SECP256R1())
            subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'synthetic-loopback')])
            now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
            certificate = (x509.CertificateBuilder()
                .subject_name(subject).issuer_name(subject).public_key(private_key.public_key())
                .serial_number(x509.random_serial_number()).not_valid_before(now - datetime.timedelta(minutes=1))
                .not_valid_after(now + datetime.timedelta(days=1))
                .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address('127.0.0.1'))]), critical=False)
                .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
                .sign(private_key, hashes.SHA256()))
            cert.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
            key.write_bytes(private_key.private_bytes(serialization.Encoding.PEM,
                serialization.PrivateFormat.TraditionalOpenSSL, serialization.NoEncryption()))
            server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            server_context.load_cert_chain(str(cert), str(key))

            service = SimpleNamespace(
                capabilities=lambda: {'engine': 'ledgetop-processing', 'version': 'synthetic'},
                accept_viewer_delivery=lambda *_args: {'accepted': True},
                options=lambda: [{'name': 'pc-ept', 'type': 'boolean'}])
            facade = ViewerFacade(service, 'production-test-token', 'staging-test-token')

            class Handler(BaseHTTPRequestHandler):
                def do_GET(self):
                    if not facade.handle(self):
                        self.send_error(404)

                def log_message(self, *_args):
                    pass

            Handler.reference_listener = SimpleNamespace(_server_context=server_context)
            viewer_tls = create_viewer_tls_server(Handler, '127.0.0.1', 0)
            thread = threading.Thread(target=viewer_tls.serve_forever, daemon=True)
            thread.start()
            origin = f'https://127.0.0.1:{viewer_tls.server_port}'
            client_context = ssl.create_default_context(cafile=str(cert))
            try:
                for path, expected in (
                        ('/info', {'engine': 'ledgetop-processing', 'version': 'synthetic',
                            'viewerAcceptanceAck': {'version': 1, 'transport': 'https',
                                'pathTemplate': '/task/{taskId}/viewer-acceptance'}}),
                        ('/options', [{'name': 'pc-ept', 'type': 'boolean'}])):
                    request = urllib.request.Request(origin + path + '?token=staging-test-token')
                    with urllib.request.urlopen(request, context=client_context, timeout=3) as response:
                        self.assertEqual(response.status, 200)
                        self.assertEqual(json.loads(response.read()), expected)
                bad_request = urllib.request.Request(origin + '/info?token=invalid-test-token')
                with self.assertRaises(HTTPError) as caught:
                    urllib.request.urlopen(bad_request, context=client_context, timeout=3)
                self.assertEqual(caught.exception.code, 401)
                self.assertEqual(caught.exception.read(), b'{"error":"unauthorized"}')
            finally:
                viewer_tls.shutdown()
                thread.join(timeout=3)
                viewer_tls.server_close()
                self.assertFalse(thread.is_alive())

    def test_listener_completes_hostname_verified_tls_handshake(self):
        with tempfile.TemporaryDirectory() as directory:
            cert, key = Path(directory) / 'cert.pem', Path(directory) / 'key.pem'
            private_key = ec.generate_private_key(ec.SECP256R1())
            subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'synthetic-loopback')])
            now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
            certificate = (x509.CertificateBuilder()
                .subject_name(subject).issuer_name(subject).public_key(private_key.public_key())
                .serial_number(x509.random_serial_number()).not_valid_before(now - datetime.timedelta(minutes=1))
                .not_valid_after(now + datetime.timedelta(days=1))
                .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address('127.0.0.1'))]), critical=False)
                .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
                .sign(private_key, hashes.SHA256()))
            cert.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
            key.write_bytes(private_key.private_bytes(serialization.Encoding.PEM,
                serialization.PrivateFormat.TraditionalOpenSSL, serialization.NoEncryption()))
            server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            server_context.load_cert_chain(str(cert), str(key))

            class Handler(BaseHTTPRequestHandler):
                def do_GET(self):
                    payload = b'verified-tls'
                    self.send_response(200)
                    self.send_header('Content-Length', str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)

                def log_message(self, *_args):
                    pass

            Handler.reference_listener = SimpleNamespace(_server_context=server_context)
            viewer_tls = create_viewer_tls_server(Handler, '127.0.0.1', 0)
            thread = threading.Thread(target=viewer_tls.serve_forever, daemon=True)
            thread.start()
            try:
                client_context = ssl.create_default_context(cafile=str(cert))
                with urllib.request.urlopen(
                        f'https://127.0.0.1:{viewer_tls.server_port}/',
                        context=client_context, timeout=3) as response:
                    self.assertEqual(response.status, 200)
                    self.assertEqual(response.read(), b'verified-tls')
            finally:
                viewer_tls.shutdown()
                thread.join(timeout=3)
                viewer_tls.server_close()
                self.assertFalse(thread.is_alive())

    def test_listener_uses_server_tls_context_and_is_supervised_and_drained(self):
        context = PassThroughContext()
        handler = type('Handler', (BaseHTTPRequestHandler,), {})
        handler.reference_listener = SimpleNamespace(_server_context=context)
        handler.viewer_service = None
        handler.prototype_queue = None

        viewer_tls = create_viewer_tls_server(handler, '127.0.0.1', 0)
        self.assertTrue(viewer_tls.viewer_tls)
        self.assertEqual(context.calls, [])
        self.assertIs(viewer_tls.viewer_tls_context, context)
        handler.reference_listener = None
        native = ThreadingHTTPServer(('127.0.0.1', 0), handler)
        service = CoordinatorService(native, handler, viewer_tls)
        stop = threading.Event()
        result = []
        thread = threading.Thread(target=lambda: result.append(service.run(stop)))
        thread.start()
        try:
            self._wait_for(service.healthy)
            self.assertTrue(service.health()['viewerTlsAlive'])
            stop.set()
            thread.join(timeout=4)
            self.assertFalse(thread.is_alive())
            self.assertEqual(result, [0])
            self.assertFalse(service.health()['viewerTlsAlive'])
        finally:
            stop.set()
            thread.join(timeout=4)
            native.server_close()

    def test_listener_refuses_to_start_without_reference_certificate_context(self):
        handler = type('Handler', (BaseHTTPRequestHandler,), {})
        handler.reference_listener = None
        with self.assertRaisesRegex(ValueError, 'reference TLS configuration'):
            create_viewer_tls_server(handler, '127.0.0.1', 0)

    @staticmethod
    def _wait_for(predicate):
        import time
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.01)
        raise AssertionError('listener did not become healthy')


if __name__ == '__main__':
    unittest.main()
