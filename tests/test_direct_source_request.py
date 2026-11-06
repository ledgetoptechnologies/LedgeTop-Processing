"""Real TLS and bearer boundary for disabled direct admission."""
import http.client
from pathlib import Path
import socket
import ssl
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid

from ops.qualify_enrolled_phase_transport import configuration_material
from processing.reference_api import ReferenceAPI
from processing.reference_http import create_reference_http_server
from processing.reference_runtime import _DynamicAPI
from processing.direct_source_request import authenticate_source_request, DirectSourceRequest


class DirectSourceRequestTest(unittest.TestCase):
    def test_closed_tls_connection_cannot_reuse_context(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            files = configuration_material(str(uuid.uuid4()), str(uuid.uuid4()))['configurationFiles']
            for name in ('fixture-ca.pem', 'fixture-cert.pem', 'fixture-key.pem'):
                (root / name).write_bytes(files[name])
            tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            tls.load_cert_chain(str(root / 'fixture-cert.pem'), str(root / 'fixture-key.pem'))
            viewer = object.__new__(ReferenceAPI)
            token = 'test-source-token-with-enough-length'
            viewer._bearers, viewer._source_principals = {token: 'staging'}, {token: 'viewer-staging'}
            left, right = socket.socketpair()
            errors = []
            def server_side():
                try:
                    conn = tls.wrap_socket(left, server_side=True)
                    context = authenticate_source_request(viewer, conn, {'Authorization': 'Bearer ' + token})
                    context.require_current()
                    conn.close()
                    try:
                        context.require_current()
                    except ValueError:
                        return
                    errors.append('closed connection accepted')
                except Exception as exc:
                    errors.append(type(exc).__name__)
            thread = threading.Thread(target=server_side)
            thread.start()
            client = ssl.create_default_context(cafile=str(root / 'fixture-ca.pem')).wrap_socket(right, server_hostname='localhost')
            thread.join(5)
            client.close()
            self.assertFalse(thread.is_alive())
            self.assertEqual([], errors)

    def test_tls_uses_registered_identity_and_intake_remains_disabled(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            files = configuration_material(str(uuid.uuid4()), str(uuid.uuid4()))['configurationFiles']
            for name in ('fixture-ca.pem', 'fixture-cert.pem', 'fixture-key.pem'):
                (root / name).write_bytes(files[name])
            tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            tls.load_cert_chain(str(root / 'fixture-cert.pem'), str(root / 'fixture-key.pem'))
            viewer = object.__new__(ReferenceAPI)
            token = 'test-source-token-with-enough-length'
            viewer._bearers = {token: 'staging'}
            viewer._source_principals = {token: 'viewer-staging'}
            api = object.__new__(_DynamicAPI)
            api.control = SimpleNamespace(viewer=viewer)
            server = create_reference_http_server(api, '127.0.0.1', 0, ssl_context=tls)
            thread = threading.Thread(target=server.serve_forever)
            thread.start()
            try:
                client_tls = ssl.create_default_context(cafile=str(root / 'fixture-ca.pem'))
                identities, captured, cross_thread_denials = [], [], []
                def observe(*args):
                    context = authenticate_source_request(*args)
                    identities.append((context.environment, context.principal))
                    captured.append(context)
                    def other_thread():
                        try:
                            context.require_current()
                        except ValueError:
                            cross_thread_denials.append(True)
                    t = threading.Thread(target=other_thread)
                    t.start()
                    t.join(2)
                    return context
                with patch('processing.direct_source_request.authenticate_source_request', side_effect=observe):
                    for supplied, expected in ((token, 503), ('incorrect-token-with-enough-length', 401)):
                        conn = http.client.HTTPSConnection('localhost', server.server_port, context=client_tls, timeout=5)
                        try:
                            conn.request('POST', '/ltds/v2/direct/jobs', body=b'{}', headers={
                                'Authorization': 'Bearer ' + supplied,
                                'X-Source-Principal': 'forged-principal',
                                'X-Forwarded-Proto': 'https',
                            })
                            response = conn.getresponse()
                            self.assertEqual(expected, response.status)
                            self.assertNotIn(token.encode(), response.read())
                        finally:
                            conn.close()
                self.assertEqual([('staging', 'viewer-staging')], identities)
                self.assertEqual([True], cross_thread_denials)
                with self.assertRaises(ValueError):
                    captured[0].require_current()
                with self.assertRaises(TypeError):
                    DirectSourceRequest('production', 'forged', None, 1)
                conn = http.client.HTTPSConnection('localhost', server.server_port, context=client_tls, timeout=5)
                try:
                    conn.putrequest('POST', '/ltds/v2/direct/jobs')
                    conn.putheader('Authorization', 'Bearer ' + token)
                    conn.putheader('Authorization', 'Bearer ' + token)
                    conn.putheader('Content-Length', '0')
                    conn.endheaders()
                    self.assertEqual(400, conn.getresponse().status)
                finally:
                    conn.close()
                conn = http.client.HTTPSConnection('localhost', server.server_port, context=client_tls, timeout=5)
                try:
                    conn.putrequest('POST', '/ltds/v2/direct/jobs')
                    conn.putheader('Authorization', 'Bearer ' + token)
                    conn.putheader('Content-Length', '1000000')
                    conn.endheaders()  # Deliberately do not send the body.
                    self.assertEqual(503, conn.getresponse().status)
                finally:
                    conn.close()
            finally:
                server.shutdown()
                thread.join(5)
                server.server_close()
            self.assertFalse(thread.is_alive())

    def test_plaintext_and_forwarded_headers_cannot_create_context(self):
        api = object.__new__(_DynamicAPI)
        api.control = SimpleNamespace(viewer=object())
        server = create_reference_http_server(api, '127.0.0.1', 0)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            conn = http.client.HTTPConnection('127.0.0.1', server.server_port, timeout=5)
            try:
                conn.request('POST', '/ltds/v2/direct/jobs', body=b'{}', headers={
                    'Authorization': 'Bearer fake-token-with-enough-length',
                    'X-Forwarded-Proto': 'https',
                })
                response = conn.getresponse()
                self.assertEqual(401, response.status)
                response.read()
            finally:
                conn.close()
        finally:
            server.shutdown()
            thread.join(5)
            server.server_close()
        self.assertFalse(thread.is_alive())

