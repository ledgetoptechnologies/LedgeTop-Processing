import datetime
import ipaddress
import json
import ssl
import tempfile
import threading
import unittest
from urllib.error import HTTPError
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from processing.app import CoordinatorService, create_viewer_tls_server
from processing.viewer_http import ViewerFacade


class PassThroughContext:
    def __init__(self):
        self.calls = []

    def wrap_socket(self, sock, *, server_side):
        self.calls.append(server_side)
        return sock


class ViewerTlsListenerTests(unittest.TestCase):
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
        self.assertEqual(context.calls, [True])
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
