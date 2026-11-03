import hashlib
import http.client
import json
from pathlib import Path
import ssl
import subprocess
import threading
import time
import unittest

from processing.guarded_native_https import create_guarded_https_server
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

    def test_real_mutual_tls_and_private_engine_http(self):
        server = self.start_listener()
        self.commit_reply(True)
        self.assertEqual(self.request(server, '/v1/initialize', {'task': self.task})[0], 200)
        self.assertEqual(self.request(server, '/v1/upload', {'task': self.job, 'item': self.item})[0], 200)
        self.assertEqual(self.request(server, '/v1/commit', {'task': self.job}), (200, {'uuid': self.job}))
        self.assertEqual(self.backend.counts()['commit'], 1)
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
