import json
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from processing.direct_worker_service_v2 import create_listener, load_listener_settings, ListenerOwner
from processing.reference_runtime import RuntimeConfigurationError


class WorkerListenerSettingsTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'worker.json'

    def write(self, value):
        self.path.write_text(json.dumps(value), encoding='utf-8')
        self.path.chmod(0o600)
        return str(self.path)

    def config(self, **changes):
        return dict(schemaVersion=1, enabled=True, host='127.0.0.1', port=18181, **changes)

    def test_no_configuration_and_explicit_disable_do_not_open_listener(self):
        self.assertIsNone(create_listener())
        value = self.config(); value['enabled'] = False
        self.assertIsNone(create_listener(self.write(value)))

    def test_invalid_or_nonprivate_configuration_fails_closed(self):
        for field, invalid in [('host', '0.0.0.0'), ('port', True), ('port', 0), ('enabled', 1)]:
            value = self.config(); value[field] = invalid
            with self.subTest(field=field, value=invalid):
                with self.assertRaises(RuntimeConfigurationError):
                    load_listener_settings(self.write(value))
        if os.name != 'nt':
            path = self.write(self.config()); self.path.chmod(0o644)
            with self.assertRaises(RuntimeConfigurationError):
                load_listener_settings(path)

    def test_symlink_and_unknown_settings_are_rejected(self):
        value = self.config(); value['extra'] = 'unexpected'
        with self.assertRaises(RuntimeConfigurationError):
            load_listener_settings(self.write(value))
        self.write(self.config())
        link = self.path.parent / 'alias.json'
        try:
            link.symlink_to(self.path)
        except (OSError, NotImplementedError):
            return
        with self.assertRaises(RuntimeConfigurationError):
            load_listener_settings(str(link))

    def test_opt_in_requires_trusted_dependencies_before_binding(self):
        path = self.write(self.config())
        with patch('processing.direct_worker_service_v2.create_direct_worker_server') as server:
            with self.assertRaises(RuntimeConfigurationError):
                create_listener(path)
            server.assert_not_called()

    def test_opt_in_builds_boundary_before_opening_server(self):
        path = self.write(self.config())
        with patch('processing.direct_worker_service_v2.DirectWorkerBoundaryV2') as boundary, \
             patch('processing.direct_worker_service_v2.create_direct_worker_server') as server:
            result = create_listener(path, boundary_kwargs={'ledger': object()}, tls_context=object(), client_principals={'a'*64:'coordinator'})
            boundary.assert_called_once()
            server.assert_called_once_with(boundary.return_value, '127.0.0.1', 18181, ssl_context=unittest.mock.ANY,
                                           client_principals={'a'*64:'coordinator'})
            self.assertIsInstance(result, ListenerOwner)
            self.assertTrue(result.close(0))
            server.return_value.server_close.assert_called_once()

    def test_owner_leaves_draining_server_alive_until_request_finishes(self):
        class Server:
            def __init__(self):
                self.done = threading.Event(); self.in_request = True
                self.drain_started = False; self.closed = False
            def handle_request(self): self.done.wait(.01)
            def drain(self, timeout):
                self.drain_started = True
                return not self.in_request
            def server_close(self): self.closed = True
        server = Server(); owner = ListenerOwner(server); owner.start()
        try:
            self.assertFalse(owner.close(0))
            self.assertTrue(server.drain_started)
            self.assertFalse(server.closed)
            server.in_request = False
            self.assertTrue(owner.close(1))
            self.assertTrue(owner.close(1))
            self.assertTrue(server.closed)
        finally:
            server.in_request = False
            owner.close(1)

    def test_thread_start_failure_closes_bound_socket(self):
        server = unittest.mock.Mock(); owner = ListenerOwner(server)
        with patch('processing.direct_worker_service_v2.threading.Thread.start', side_effect=RuntimeError('failed')):
            with self.assertRaises(RuntimeError): owner.start()
        server.server_close.assert_called_once()
        self.assertTrue(owner.close(0))

    def test_bounded_join_keeps_socket_open_until_accept_loop_exits(self):
        entered = threading.Event(); release = threading.Event()
        class Server:
            closed = False
            def handle_request(self): entered.set(); release.wait(3)
            def drain(self, _): return True
            def server_close(self): self.closed = True
        server = Server(); owner = ListenerOwner(server); owner.start()
        try:
            self.assertTrue(entered.wait(1))
            self.assertFalse(owner.close(.01))
            self.assertFalse(server.closed)
        finally:
            release.set()
            self.assertTrue(owner.close(2))

    def test_accept_loop_failure_is_reported_and_closes_safely(self):
        entered = threading.Event()
        class Server:
            closed = False
            def handle_request(self): entered.set(); raise OSError('synthetic accept failure')
            def drain(self, _): return True
            def server_close(self): self.closed = True
        server = Server(); owner = ListenerOwner(server)
        self.assertEqual(owner.status(), 'not_started')
        owner.start(); self.assertTrue(entered.wait(1))
        self.assertTrue(owner._failed.wait(1))
        self.assertEqual(owner.status(), 'failed')
        self.assertTrue(owner.close(1)); self.assertTrue(server.closed)
        self.assertEqual(owner.status(), 'failed')

    @unittest.skipIf(os.name == 'nt', 'Linux scratch fixture required')
    def test_owner_drains_real_v2_tls_upload(self):
        import http.client
        import ssl
        import uuid
        from ops.qualify_enrolled_phase_transport import configuration_material
        from processing.direct_worker_http import create_direct_worker_server
        from processing.reference_tls import verified_client_context
        from tests.test_direct_worker_receiver_v2 import WorkerV2Tests
        from tests.test_reference_source_intake import IMAGE_ID

        fixture = WorkerV2Tests(); fixture.setUp(); self.addCleanup(fixture.doCleanups)
        material = configuration_material(str(uuid.uuid4()), str(uuid.uuid4()))['configurationFiles']
        for name in ('fixture-ca.pem', 'fixture-cert.pem', 'fixture-key.pem'):
            (fixture.root / name).write_bytes(material[name])
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(str(fixture.root/'fixture-cert.pem'), str(fixture.root/'fixture-key.pem'))
        server = create_direct_worker_server(fixture.boundary, '127.0.0.1', 0, ssl_context=context)
        owner = ListenerOwner(server); owner.start()
        entered = threading.Event(); release = threading.Event(); outcome = []
        original = fixture.spool.receive_object
        def blocked(*args, **kwargs):
            entered.set()
            if not release.wait(3): raise TimeoutError('test release timed out')
            return original(*args, **kwargs)
        def upload():
            connection = http.client.HTTPSConnection('127.0.0.1', server.server_port,
                context=verified_client_context(str(fixture.root/'fixture-ca.pem')), timeout=3)
            try:
                import hashlib
                connection.request('PUT', '/direct/v2/objects/' + IMAGE_ID, body=b'abc',
                    headers={**fixture.headers, 'Content-SHA256': hashlib.sha256(b'abc').hexdigest()})
                response = connection.getresponse(); response.read(); outcome.append(response.status)
            finally: connection.close()
        try:
            with patch.object(fixture.spool, 'receive_object', side_effect=blocked):
                thread = threading.Thread(target=upload); thread.start()
                try:
                    self.assertTrue(entered.wait(3)); self.assertFalse(owner.close(.05))
                    self.assertEqual(owner.status(), 'draining')
                finally:
                    release.set(); thread.join(4)
                self.assertFalse(thread.is_alive()); self.assertEqual(outcome, [200])
                self.assertTrue(owner.close(3)); self.assertEqual(owner.status(), 'closed')
        finally:
            release.set(); owner.close(3)

    @unittest.skipIf(os.name == 'nt', 'Linux scratch fixture required')
    def test_owner_drains_real_tls_upload_before_shutdown(self):
        import http.client
        import ssl
        from unittest.mock import patch
        from processing.direct_worker_http import create_direct_worker_server
        from tests.test_direct_worker_http import DirectWorkerHTTPTests

        fixture = DirectWorkerHTTPTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        root = Path(fixture.temp.name)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(str(root / 'fixture-cert.pem'), str(root / 'fixture-key.pem'))
        server = create_direct_worker_server(fixture.boundary, '127.0.0.1', 0, ssl_context=context)
        owner = ListenerOwner(server); owner.start()
        entered = threading.Event(); release = threading.Event(); outcome = []
        original = fixture.f.spool.receive_object
        def blocked(*args, **kwargs):
            entered.set()
            if not release.wait(3): raise TimeoutError('test release timed out')
            return original(*args, **kwargs)
        obj = fixture.f.manifest['objects'][0]
        def upload():
            connection = http.client.HTTPSConnection('127.0.0.1', server.server_port,
                context=fixture.client_context, timeout=3)
            try:
                connection.request('PUT', '/direct/v1/objects/' + obj['id'], body=fixture.f.data,
                    headers={**fixture.headers, 'Content-SHA256': obj['sha256']})
                response = connection.getresponse(); response.read(); outcome.append(response.status)
            finally:
                connection.close()
        try:
            with patch.object(fixture.f.spool, 'receive_object', side_effect=blocked):
                thread = threading.Thread(target=upload); thread.start()
                try:
                    self.assertTrue(entered.wait(3))
                    self.assertFalse(owner.close(.05))
                    self.assertTrue(owner._thread.is_alive())
                finally:
                    release.set(); thread.join(4)
                self.assertFalse(thread.is_alive()); self.assertEqual(outcome, [200])
                self.assertTrue(owner.close(3))
                self.assertFalse(owner._thread.is_alive())
        finally:
            release.set()
            owner.close(3)
