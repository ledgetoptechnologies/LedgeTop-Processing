"""Synthetic local TLS handshake for the source-range broker trust context."""
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
import ssl
import tempfile
import threading
import unittest
from urllib.error import URLError
from urllib.request import Request

from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from processing.reference_transfer import BrokerClient
from processing.source_range_transfer import SourceRangeClient


class PrivateBrokerTLSTests(unittest.TestCase):
    def test_private_ca_is_reused_and_untrusted_or_wrong_hostname_is_rejected(self):
        with tempfile.TemporaryDirectory(prefix='lt-source-range-tls-') as root:
            directory = Path(root)
            now = datetime.now(timezone.utc)
            ca_key = Ed25519PrivateKey.generate()
            ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'Synthetic CA')])
            ca = (x509.CertificateBuilder().subject_name(ca_name).issuer_name(ca_name)
                  .public_key(ca_key.public_key()).serial_number(x509.random_serial_number())
                  .not_valid_before(now - timedelta(minutes=1)).not_valid_after(now + timedelta(days=1))
                  .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
                  .add_extension(x509.KeyUsage(digital_signature=True, content_commitment=False,
                      key_encipherment=False, data_encipherment=False, key_agreement=False,
                      key_cert_sign=True, crl_sign=True, encipher_only=False, decipher_only=False), critical=True)
                  .add_extension(x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), critical=False)
                  .sign(ca_key, algorithm=None))
            key = Ed25519PrivateKey.generate()
            name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'localhost')])
            cert = (x509.CertificateBuilder().subject_name(name).issuer_name(ca_name)
                    .public_key(key.public_key()).serial_number(x509.random_serial_number())
                    .not_valid_before(now - timedelta(minutes=1)).not_valid_after(now + timedelta(hours=1))
                    .add_extension(x509.SubjectAlternativeName([x509.DNSName('localhost')]), critical=False)
                    .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
                    .add_extension(x509.KeyUsage(digital_signature=True, content_commitment=False,
                        key_encipherment=False, data_encipherment=False, key_agreement=False,
                        key_cert_sign=False, crl_sign=False, encipher_only=False, decipher_only=False), critical=True)
                    .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
                    .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
                    .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), critical=False)
                    .sign(ca_key, algorithm=None))
            ca_path, cert_path, key_path = (directory / n for n in ('ca.pem', 'server.pem', 'server.key'))
            ca_path.write_bytes(ca.public_bytes(serialization.Encoding.PEM))
            cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
            key_path.write_bytes(key.private_bytes(serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))

            class Handler(BaseHTTPRequestHandler):
                def do_GET(self):
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(b'ok')
                def log_message(self, *_args):
                    pass

            tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            tls.load_cert_chain(cert_path, key_path)

            class LocalTLSServer(HTTPServer):
                def get_request(self):
                    connection, address = super().get_request()
                    connection.settimeout(3)
                    try:
                        return tls.wrap_socket(connection, server_side=True), address
                    except BaseException:
                        connection.close()
                        raise

            server = LocalTLSServer(('127.0.0.1', 0), Handler)
            thread = None
            try:
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()
                port = server.server_port
                context = ssl.create_default_context(cafile=str(ca_path))
                broker = BrokerClient(f'https://localhost:{port}', ssl_context=context)
                client = SourceRangeClient(broker.origin, directory / 'good.sqlite', ssl_context=broker.ssl_context)
                with client._open(Request(f'https://localhost:{port}/probe'), timeout=3) as response:
                    self.assertEqual(response.read(), b'ok')
                with self.assertRaises(URLError):
                    client._open(Request(f'https://127.0.0.1:{port}/probe'), timeout=3)
                untrusted = SourceRangeClient(broker.origin, directory / 'untrusted.sqlite')
                with self.assertRaises(URLError):
                    untrusted._open(Request(f'https://localhost:{port}/probe'), timeout=3)
            finally:
                stopper = threading.Thread(target=server.shutdown, daemon=True) if thread and thread.is_alive() else None
                if stopper:
                    stopper.start()
                    stopper.join(timeout=6)
                server.server_close()
                if thread:
                    thread.join(timeout=5)
                    self.assertFalse(thread.is_alive())
                if stopper:
                    self.assertFalse(stopper.is_alive())


if __name__ == '__main__':
    unittest.main()
