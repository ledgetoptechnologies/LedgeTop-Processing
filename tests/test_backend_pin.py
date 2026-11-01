"""Pure bounded tests for the local backend identity inspection."""
import json
from types import SimpleNamespace
import time
import unittest
from unittest.mock import patch

from processing.engine_ingress_guard import (AdmissionError, BackendPin, Binding,
    EngineIngressGuard, local_backend_identity)


class BackendPinTests(unittest.TestCase):
    def setUp(self):
        self.pin = BackendPin('a' * 64, 'sha256:' + 'b' * 64, 13001)
        self.item = {'Id': self.pin.container_id, 'Image': self.pin.image_id,
            'State': {'Running': True}, 'NetworkSettings': {'Ports': {
                '3000/tcp': [{'HostIp': '127.0.0.1', 'HostPort': '13001'}]}}}

    def probe(self, item):
        with patch('processing.engine_ingress_guard.subprocess.run',
                   return_value=SimpleNamespace(returncode=0, stdout=json.dumps([item]))) as run:
            local_backend_identity(self.pin, time.monotonic() + 5)
            self.assertEqual(run.call_args.args[0][:3], ['/usr/bin/docker', '--host', 'unix:///var/run/docker.sock'])

    def test_exact_container_and_private_port(self):
        self.probe(self.item)
        for field, value in [('Id', 'c' * 64), ('Image', 'sha256:' + 'd' * 64)]:
            with self.subTest(field=field), self.assertRaises(AdmissionError):
                self.probe({**self.item, field: value})
        item = {**self.item, 'NetworkSettings': {'Ports': {'3000/tcp': [
            {'HostIp': '0.0.0.0', 'HostPort': '13001'}]}}}
        with self.assertRaises(AdmissionError): self.probe(item)

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
