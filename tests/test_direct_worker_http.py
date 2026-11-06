"""Disposable loopback TLS tests; never starts a production worker."""
import http.client
import io
import json
from pathlib import Path
import socket
import ssl
import tempfile
import threading
import time
import unittest
import uuid
from types import SimpleNamespace
from unittest.mock import Mock, patch

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from ops.qualify_enrolled_phase_transport import configuration_material
from processing.direct_worker_boundary import DirectWorkerBoundary
from processing.direct_worker_http import _BoundedReader, create_direct_worker_server
from processing.direct_worker_receipt import WorkerClosureSigner
from processing.reference_tls import verified_client_context
from tests import test_direct_worker_receiver as fixture


class DirectWorkerHTTPTests(unittest.TestCase):
    def setUp(self):
        self.f = fixture.DirectWorkerReceiverTests()
        self.f.with_derived = True
        self.f.setUp()
        self.addCleanup(self.f.doCleanups)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        material = configuration_material(str(uuid.uuid4()), str(uuid.uuid4()))['configurationFiles']
        for name in ('fixture-ca.pem', 'fixture-cert.pem', 'fixture-key.pem'):
            (root / name).write_bytes(material[name])
        server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_context.minimum_version = ssl.TLSVersion.TLSv1_2
        server_context.load_cert_chain(str(root/'fixture-cert.pem'), str(root/'fixture-key.pem'))
        self.client_context = verified_client_context(str(root/'fixture-ca.pem'))
        self.signer = WorkerClosureSigner(issuer_id='worker', key_id='key', identity=self.f.worker,
                                           private_key=Ed25519PrivateKey.generate())
        self.token = 'synthetic-coordinator-token-0123456789'
        self.headers = {'Authorization':'Bearer '+self.token,
                        'X-Coordinator-Authority':self.f.worker['authorityId']}
        self.boundary = DirectWorkerBoundary(ledger=self.f.ledger, spool=self.f.spool,
            manifest=self.f.manifest, item=self.f.item, derived_ids=self.f.derived_ids,
            expected_worker=self.f.worker, engine_id=self.f.engine, coordinator_origin=self.f.origin,
            gate_generation=1, bearer=self.token, signer=self.signer)
        self.server = create_direct_worker_server(self.boundary, '127.0.0.1', 0,
            ssl_context=server_context, idle_timeout=2, header_deadline=0.6)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.stop_server)

    def stop_server(self):
        self.server.shutdown(); self.server.server_close(); self.thread.join(3)
        self.assertFalse(self.thread.is_alive())

    def request(self, method, path, body=b'', headers=None):
        connection = http.client.HTTPSConnection('127.0.0.1', self.server.server_port,
                                                 context=self.client_context, timeout=3)
        try:
            connection.request(method, path, body=body, headers=headers or self.headers)
            response = connection.getresponse()
            return response.status, json.loads(response.read())
        finally:
            connection.close()

    def test_tls_objects_then_signed_complete(self):
        for obj, data in ((self.f.manifest['objects'][0], self.f.data),
                          (self.f.manifest['objects'][1], self.f.derived_data)):
            status, reply = self.request('PUT', '/direct/v1/objects/'+obj['id'], data,
                                         {**self.headers, 'Content-SHA256':obj['sha256']})
            self.assertEqual(status, 200); self.assertTrue(reply['localObjectDurable'])
            self.assertFalse(reply['sourceCustodyAccepted'])
        status, reply = self.request('POST', '/direct/v1/complete')
        self.assertEqual(status, 200)
        self.assertFalse(reply['closure']['sourceCustodyAccepted'])
        self.assertFalse(reply['closure']['processingAuthorized'])

    def test_bad_auth_and_wrong_length_never_publish(self):
        obj = self.f.manifest['objects'][0]
        status, _ = self.request('PUT', '/direct/v1/objects/'+obj['id'], self.f.data,
            {**self.headers, 'Authorization':'Bearer wrong', 'Content-SHA256':obj['sha256']})
        self.assertEqual(status, 403)
        status, _ = self.request('PUT', '/direct/v1/objects/'+obj['id'], b'x',
                                 {**self.headers, 'Content-SHA256':obj['sha256']})
        self.assertEqual(status, 403)
        self.assertFalse((self.f.spool.root/self.f.item['jobId']).exists())

    def test_duplicate_authorization_is_rejected_before_upload(self):
        context = self.client_context
        with socket.create_connection(('127.0.0.1', self.server.server_port), timeout=3) as plain:
            with context.wrap_socket(plain, server_hostname='127.0.0.1') as sock:
                raw = (b'PUT /direct/v1/objects/object-1 HTTP/1.1\r\nHost: localhost\r\n'
                       b'Authorization: Bearer one\r\nauthorization: Bearer two\r\n'
                       b'Content-Length: 1\r\n\r\nx')
                sock.sendall(raw)
                self.assertIn(b' 400 ', sock.recv(1024))
        self.assertFalse((self.f.spool.root/self.f.item['jobId']).exists())

    def test_plaintext_and_client_tls_context_cannot_create_listener(self):
        for context in (None, ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)):
            with self.assertRaises(ValueError):
                create_direct_worker_server(self.boundary, '127.0.0.1', 0, ssl_context=context)

    def test_body_reader_honors_total_deadline_and_retires(self):
        stream = SimpleNamespace(read1=Mock(return_value=b'a'))
        connection = SimpleNamespace(settimeout=Mock())
        handler = SimpleNamespace(rfile=stream, connection=connection,
                                  server=SimpleNamespace(idle_timeout=10))
        body = _BoundedReader(handler, 3, 12)
        with patch('processing.direct_worker_http.time.monotonic', side_effect=[10, 13]):
            self.assertEqual(body.read(3), b'a')
            with self.assertRaises(TimeoutError): body.read(2)
        self.assertEqual(stream.read1.call_count, 1)
        body.close()
        with self.assertRaises(ValueError): body.read(1)

    def test_drip_fed_headers_hit_absolute_deadline(self):
        started = time.monotonic()
        with socket.create_connection(('127.0.0.1', self.server.server_port), timeout=3) as plain:
            with self.client_context.wrap_socket(plain, server_hostname='127.0.0.1') as sock:
                sock.settimeout(1)
                # Every byte arrives faster than the 2-second idle timeout.
                # The complete headers would otherwise take many seconds.
                for byte in b'PUT /direct/v1/objects/object-1 HTTP/1.1\r\nHost: localhost\r\n':
                    try:
                        sock.sendall(bytes([byte]))
                    except (OSError, ssl.SSLError):
                        break
                    time.sleep(0.08)
                try:
                    self.assertEqual(sock.recv(1024), b'')
                except (OSError, ssl.SSLError):
                    pass
        self.assertLess(time.monotonic()-started, 3)
        self.assertFalse((self.f.spool.root/self.f.item['jobId']).exists())
        # A later client still reaches the listener after the timed-out peer.
        status, _ = self.request('PUT', '/direct/v1/objects/object-1', b'x',
                                 {**self.headers, 'Content-SHA256':'0'*64})
        self.assertEqual(status, 403)

    def test_drip_fed_tls_handshake_hits_same_deadline(self):
        started = time.monotonic()
        with socket.create_connection(('127.0.0.1', self.server.server_port), timeout=3) as sock:
            sock.settimeout(1)
            # Plausible TLS handshake record with a declared body much larger
            # than the dripped bytes, so OpenSSL must wait for more input.
            for byte in b'\x16\x03\x01\x20\x00' + b'\x01' * 32:
                try:
                    sock.sendall(bytes([byte]))
                except OSError:
                    break
                time.sleep(0.08)
            try:
                self.assertEqual(sock.recv(1024), b'')
            except OSError:
                pass
        self.assertLess(time.monotonic()-started, 3)
        self.assertFalse((self.f.spool.root/self.f.item['jobId']).exists())


if __name__ == '__main__': unittest.main()
