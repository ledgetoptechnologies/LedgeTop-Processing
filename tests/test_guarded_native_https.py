import hashlib
import http.client
import json
from pathlib import Path
import ssl
import subprocess
import threading
import time
import unittest
from unittest.mock import patch

from processing.guarded_native_https import create_guarded_https_server
from processing.guarded_native_client import GuardedNativeClient
from processing.node_gate import GateError
from processing.viewer_service import NativeEngine
from tests.test_guarded_native_proxy import GuardedNativeProxyTests, TOKEN


class GuardedHTTPS(GuardedNativeProxyTests):
    def setUp(self):
        super().setUp()
        for name, extension in (('server', 'subjectAltName=IP:127.0.0.1'),
                                ('client', 'extendedKeyUsage=clientAuth'),
                                ('other', 'extendedKeyUsage=clientAuth')):
            subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
                            '-subj', '/CN=' + name, '-addext', extension,
                            '-keyout', str(self.root / (name + '.key')),
                            '-out', str(self.root / (name + '.pem'))],
                           check=True, capture_output=True, timeout=15)
        cert = (self.root / 'client.pem').read_text()
        self.fingerprint = hashlib.sha256(ssl.PEM_cert_to_DER_cert(cert)).hexdigest()

    def start_listener(self, **overrides):
        args = dict(bind_host='127.0.0.1', port=0, certificate=self.root / 'server.pem',
                    private_key=self.root / 'server.key', client_ca=self.root / 'client.pem',
                    client_fingerprint=self.fingerprint, enabled=True)
        args.update(overrides)
        server = create_guarded_https_server(self.proxy, **args)
        thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=.01))
        thread.start()
        def close():
            server.shutdown(); thread.join(3); server.server_close()
            self.assertFalse(thread.is_alive())
        self.addCleanup(close)
        return server

    def request(self, server, path, value, *, client='client', token=TOKEN, trust_server=True):
        context = ssl.create_default_context(cafile=str(self.root / ('server.pem' if trust_server else 'other.pem')))
        if client:
            context.load_cert_chain(self.root / (client + '.pem'), self.root / (client + '.key'))
        conn = http.client.HTTPSConnection('127.0.0.1', server.server_port, context=context, timeout=3)
        try:
            conn.request('POST', path, json.dumps(value), {'Authorization': 'Bearer ' + token})
            response = conn.getresponse()
            return response.status, json.loads(response.read())
        finally:
            conn.close()

    def test_disabled_and_loopback_only(self):
        with self.assertRaises(ValueError): self.start_listener(enabled=False)
        with self.assertRaises(ValueError): self.start_listener(bind_host='0.0.0.0')

    def test_listener_refuses_remote_engine_before_allocating_spool(self):
        for origin in ('http://192.0.2.1:3000', 'http://localhost:3000',
                       'https://127.0.0.1:3000'):
            with self.subTest(origin=origin), patch.object(self.proxy, 'engine', NativeEngine(origin)):
                with self.assertRaises(ValueError): self.start_listener()
                self.assertFalse((self.spool / '.guarded-spool.sqlite3').exists())

    def test_listener_refuses_stale_or_unprovisioned_node_before_allocating_spool(self):
        for name, value in (('node_id', 'other'), ('authority_id', '00000000-0000-4000-8000-000000000000'),
                            ('generation', 2), ('engine_id', 'unprovisioned-engine')):
            with self.subTest(field=name), patch.object(self.proxy, name, value):
                with self.assertRaises((ValueError, GateError)): self.start_listener()
                self.assertFalse((self.spool / '.guarded-spool.sqlite3').exists())

    def test_real_mutual_tls_and_private_engine_http(self):
        server = self.start_listener()
        self.commit_reply(True)
        self.assertEqual(self.request(server, '/v1/initialize', {'task': self.task})[0], 200)
        # An authenticated caller cannot use the old path-bearing upload route
        # to bypass the persistent spool capacity budget.
        self.assertEqual(self.request(server, '/v1/upload', {'task': self.job, 'item': self.item})[0], 404)
        self.assertEqual(self.backend.counts()['upload'], 0)
        context = ssl.create_default_context(cafile=str(self.root / 'server.pem'))
        context.load_cert_chain(self.root / 'client.pem', self.root / 'client.key')
        GuardedNativeClient('127.0.0.1', server.server_port, context, TOKEN, timeout=10).upload(self.job, self.item)
        self.assertEqual(self.request(server, '/v1/commit', {'task': self.job}), (200, {'uuid': self.job}))
        self.assertEqual(self.backend.counts()['commit'], 1)
        self.assertEqual(self.request(server, '/v1/commit', {'task': self.job})[0], 409)
        self.assertEqual(self.backend.counts()['commit'], 1)

    def test_exact_journal_inspection_requires_authenticated_bound_identity(self):
        import uuid
        from processing import reference_protocol as protocol
        from processing.viewer_service import EngineUnavailable
        server = self.start_listener()
        self.assertEqual(self.request(server, '/v1/initialize', {'task': self.task})[0], 200)
        request = dict(task=self.job, name=self.task['name'], outputs=None, options=[])
        claim = dict(jobId=self.job, nodeId='main', authorityId=self.authority, generation=1,
                     engineTask=self.job, operationId=self.proxy._op(self.job, 'init'), kind='init',
                     requestDigest=protocol.digest(request), nonce='a' * 64)
        context = ssl.create_default_context(cafile=str(self.root / 'server.pem'))
        context.load_cert_chain(self.root / 'client.pem', self.root / 'client.key')
        client = GuardedNativeClient('127.0.0.1', server.server_port, context, TOKEN, timeout=10)
        proof = client.inspect_mutation(claim)
        self.assertEqual((proof['state'], proof['held'], proof['claim']), ('done', True, claim))
        self.assertEqual(len(proof['ackDigest']), 64)
        for change in (dict(jobId=str(uuid.uuid4())), dict(nodeId='other'),
                       dict(authorityId=str(uuid.uuid4())), dict(generation=2),
                       dict(engineTask=str(uuid.uuid4())), dict(operationId=str(uuid.uuid4())),
                       dict(requestDigest='b' * 64), dict(kind='commit'), dict(nonce='bad')):
            with self.subTest(change=change), self.assertRaises(EngineUnavailable):
                client.inspect_mutation({**claim, **change})
        self.assertEqual(self.request(server, '/v1/inspect-mutation', {'claim': claim}, token='wrong')[0], 409)
        with self.assertRaises((ssl.SSLError, ConnectionError)):
            self.request(server, '/v1/inspect-mutation', {'claim': claim}, client=None)
        self.assertEqual(self.backend.counts()['initialize'], 1)

    def test_pending_journal_inspection_does_not_authorize_retry(self):
        from processing import reference_protocol as protocol
        self.commit_reply(False)
        server = self.start_listener()
        self.assertEqual(self.request(server, '/v1/initialize', {'task': self.task})[0], 200)
        context = ssl.create_default_context(cafile=str(self.root / 'server.pem'))
        context.load_cert_chain(self.root / 'client.pem', self.root / 'client.key')
        GuardedNativeClient('127.0.0.1', server.server_port, context, TOKEN, timeout=10).upload(self.job, self.item)
        self.assertEqual(self.request(server, '/v1/commit', {'task': self.job})[0], 409)
        claim = dict(jobId=self.job, nodeId='main', authorityId=self.authority, generation=1,
                     engineTask=self.job, operationId=self.proxy._op(self.job, 'commit'), kind='commit',
                     requestDigest=protocol.digest(dict(task=self.job)), nonce='c' * 64)
        status, proof = self.request(server, '/v1/inspect-mutation', {'claim': claim})
        self.assertEqual(status, 200)
        self.assertEqual((proof['state'], proof['ackDigest'], proof['held']), ('pending', None, True))
        self.assertEqual(self.request(server, '/v1/commit', {'task': self.job})[0], 409)
        self.assertEqual(self.backend.counts()['commit'], 1)

    def test_untrusted_clients_and_wrong_bearer_never_mutate(self):
        server = self.start_listener()
        for client in (None, 'other'):
            with self.subTest(client=client), self.assertRaises((ssl.SSLError, ConnectionError)):
                self.request(server, '/v1/initialize', {'task': self.task}, client=client)
        with self.assertRaises(ssl.SSLCertVerificationError):
            self.request(server, '/v1/initialize', {'task': self.task}, trust_server=False)
        self.assertEqual(self.request(server, '/v1/initialize', {'task': self.task}, token='wrong')[0], 409)
        self.assertEqual(self.backend.counts()['initialize'], 0)

    def test_stream_cannot_exceed_persistent_aggregate_capacity(self):
        from processing.viewer_service import EngineUnavailable
        server = self.start_listener(spool_cap=2)
        self.assertEqual(self.request(server, '/v1/initialize', {'task': self.task})[0], 200)
        context = ssl.create_default_context(cafile=str(self.root / 'server.pem'))
        context.load_cert_chain(self.root / 'client.pem', self.root / 'client.key')
        client = GuardedNativeClient('127.0.0.1', server.server_port, context, TOKEN, timeout=10)
        with self.assertRaises(EngineUnavailable): client.upload(self.job, self.item)
        self.assertTrue(server.wait_drained(5))
        self.assertEqual(self.backend.counts()['upload'], 0)
        self.assertEqual(server.spool_budget.inspect()['reservations'], [])
        self.assertEqual(list(self.spool.glob('incoming-*')), [])

    def test_trusted_ca_but_wrong_pinned_identity_is_rejected(self):
        server = self.start_listener(client_fingerprint='0' * 64)
        self.assertEqual(self.request(server, '/v1/initialize', {'task': self.task})[0], 403)
        self.assertEqual(self.backend.counts()['initialize'], 0)

    def test_infinite_and_invalid_options_never_reserve_node(self):
        server = self.start_listener()
        invalid = ({**self.task, 'options': [{'name': 'x', 'value': float('inf')}]},
                   {**self.task, 'options': [{'name': 'x\r\nInjected', 'value': 1}]},
                   {**self.task, 'outputs': [42]})
        for task in invalid:
            self.assertEqual(self.request(server, '/v1/initialize', {'task': task})[0], 409)
        # Python's parser turns this valid JSON number into +inf; parse_constant
        # does not see it, so validation must precede reservation.
        context = ssl.create_default_context(cafile=str(self.root / 'server.pem'))
        context.load_cert_chain(self.root / 'client.pem', self.root / 'client.key')
        conn = http.client.HTTPSConnection('127.0.0.1', server.server_port, context=context, timeout=3)
        body = json.dumps({'task': {**self.task, 'options': [{'name': 'x', 'value': 'INFINITY'}]}}).replace('"INFINITY"', '1e999')
        try:
            conn.request('POST', '/v1/initialize', body, {'Authorization': 'Bearer ' + TOKEN})
            response = conn.getresponse()
            self.assertEqual(response.status, 409)
            response.read()
        finally:
            conn.close()
        from processing.node_gate import NodeGate
        with NodeGate('physical-engine') as gate:
            self.assertIsNone(gate.snapshot()['reservation'])
        self.assertEqual(self.backend.counts()['initialize'], 0)

    def test_slow_client_is_deadlined_and_does_not_block_other_client(self):
        import socket
        server = self.start_listener(request_deadline=.4)
        context = ssl.create_default_context(cafile=str(self.root / 'server.pem'))
        context.load_cert_chain(self.root / 'client.pem', self.root / 'client.key')
        raw = socket.create_connection(('127.0.0.1', server.server_port), timeout=3)
        slow = context.wrap_socket(raw, server_hostname='127.0.0.1')
        slow.settimeout(2)
        try:
            slow.sendall(b'POST /v1/initialize HTTP/1.1\r\nHost: localhost\r\n')
            self.assertEqual(self.request(server, '/v1/initialize', {'task': self.task}, token='wrong')[0], 409)
            start = time.monotonic()
            try:
                self.assertEqual(slow.recv(1), b'')
            except (ssl.SSLError, ConnectionResetError, BrokenPipeError):
                pass
            self.assertLess(time.monotonic() - start, 1.5)
            self.assertEqual(self.backend.counts()['initialize'], 0)
        finally:
            slow.close()

    def test_slow_handshake_is_deadlined_and_does_not_block_accept(self):
        import socket
        server = self.start_listener(request_deadline=.4)
        slow = socket.create_connection(('127.0.0.1', server.server_port), timeout=2)
        slow.settimeout(2)
        try:
            self.assertEqual(self.request(server, '/v1/initialize', {'task': self.task}, token='wrong')[0], 409)
            self.assertEqual(slow.recv(1), b'')
            self.assertEqual(self.backend.counts()['initialize'], 0)
        finally:
            slow.close()

    def test_long_engine_call_survives_input_deadline_and_shutdown_reports_active(self):
        server = self.start_listener(request_deadline=.2)
        started = threading.Event()
        release = threading.Event()
        original = self.proxy.initialize
        def delayed(*args, **kwargs):
            started.set()
            if not release.wait(3):
                raise RuntimeError('test timed out')
            return original(*args, **kwargs)
        self.proxy.initialize = delayed
        result = []
        thread = threading.Thread(target=lambda: result.append(self.request(server, '/v1/initialize', {'task': self.task})))
        thread.start()
        try:
            self.assertTrue(started.wait(2))
            time.sleep(.35)
            self.assertTrue(thread.is_alive())
            server.shutdown()
            server.server_close()
            self.assertFalse(server.wait_drained(.05))
            release.set()
            thread.join(2)
            self.assertFalse(thread.is_alive())
            self.assertEqual(result, [(200, {'accepted': True})])
            self.assertTrue(server.wait_drained(1))
            self.assertEqual(self.backend.counts()['initialize'], 1)
        finally:
            release.set()
            thread.join(3)
