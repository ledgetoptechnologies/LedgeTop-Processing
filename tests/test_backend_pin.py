"""Pure bounded tests for the local backend identity inspection."""
import json
import time
import unittest
from unittest.mock import patch

from processing.engine_ingress_guard import (AdmissionError, BackendPin, Binding,
    EngineIngressGuard, local_backend_identity, _local_docker_json)


class BackendPinTests(unittest.TestCase):
    def setUp(self):
        self.pin = BackendPin('a' * 64, 'sha256:' + 'b' * 64, 13001)
        self.item = {'Id': self.pin.container_id, 'ImageID': self.pin.image_id,
            'State': 'running', 'Ports': [
                {'IP': '127.0.0.1', 'PrivatePort': 3000, 'PublicPort': 13001, 'Type': 'tcp'}]}

    def probe(self, item):
        with patch('processing.engine_ingress_guard._local_docker_json', return_value=[item]) as run:
            local_backend_identity(self.pin, time.monotonic() + 5)
            self.assertTrue(run.call_args.args[0].startswith('/containers/json?'))

    def test_exact_container_and_private_port(self):
        self.probe(self.item)
        for field, value in [('Id', 'c' * 64), ('ImageID', 'sha256:' + 'd' * 64)]:
            with self.subTest(field=field), self.assertRaises(AdmissionError):
                self.probe({**self.item, field: value})
        item = {**self.item, 'Ports': [
            {'IP': '0.0.0.0', 'PrivatePort': 3000, 'PublicPort': 13001, 'Type': 'tcp'}]}
        with self.assertRaises(AdmissionError): self.probe(item)

    def test_bounded_local_socket_refuses_oversize_and_bad_framing(self):
        class Fake:
            def __init__(self, chunks): self.chunks = iter(chunks); self.path = None; self.sent = b''
            def setblocking(self, _): pass
            def connect_ex(self, path): self.path = path; return 0
            def send(self, value): self.sent += value; return len(value)
            def recv(self, _): return next(self.chunks, b'')
            def close(self): pass
        def read(chunks):
            sock = Fake(chunks)
            with patch('processing.engine_ingress_guard.socket.AF_UNIX', 1, create=True), \
                    patch('processing.engine_ingress_guard.socket.socket', return_value=sock), \
                    patch('processing.engine_ingress_guard.select.select', side_effect=lambda r,w,x,t:(r,w,[])):
                value = _local_docker_json('/info', time.monotonic() + 2)
            self.assertEqual(sock.path, '/var/run/docker.sock')
            return value
        self.assertEqual(read([b'HTTP/1.0 200 OK\r\nContent-Length: 2\r\n\r\n', b'{}']), {})
        for response in (b'HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n',
                         b'HTTP/1.0 200 OK\r\nContent-Length: 3\r\n\r\n{}',
                         b'HTTP/1.0 200 OK\r\n\r\n' + b'x' * (49 * 1024)):
            with self.assertRaises(AdmissionError): read([response])

    def test_management_read_refuses_missing_and_changed_backend(self):
        guard = object.__new__(EngineIngressGuard)
        guard.binding = Binding('engine', 'cluster', 'authority', 'http://127.0.0.1:18080',
            1, 13001, 'c' * 32, 'm' * 32, 'b' * 32)
        guard.engine_probe = lambda _: 'engine'
        guard.backend_pin = None
        guard.backend_probe = lambda *_: None
        guard._exchange = lambda *_: (200, b'{}')
        headers = [('Authorization', 'Bearer ' + 'm' * 32)]
        with self.assertRaises(AdmissionError): guard.read('GET', '/info', headers)
        guard.backend_pin = self.pin
        calls = []
        def changed(*_):
            calls.append(True)
            if len(calls) == 2: raise AdmissionError('backend changed')
        guard.backend_probe = changed
        with self.assertRaises(AdmissionError): guard.read('GET', '/info', headers)
        self.assertEqual(len(calls), 2)


if __name__ == '__main__': unittest.main()
