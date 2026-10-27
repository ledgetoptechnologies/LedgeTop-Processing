import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from ops.node_ingress import GateError, Ingress, configuration, save


@unittest.skipIf(os.name == 'nt', 'Linux host ingress uses POSIX durable files')
class IngressTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        (self.root / 'public-config').mkdir()
        self.gate = object.__new__(Ingress)
        self.gate.root = self.root
        self.gate.config = {'listenPort': 13005, 'backendPort': 13004, 'backendName': 'owned-node'}
        self.item = {'Id': 'owned-ingress', 'State': {'Pid': 100}}
        self.image = 'sha256:' + 'a' * 64

    def tearDown(self):
        self.temp.cleanup()

    def test_busy_old_connection_prevents_close_attestation_and_reopen(self):
        with patch.object(self.gate, 'inspect', return_value=self.item), \
                patch.object(self.gate, 'workers', return_value={101}), \
                patch.object(self.gate, 'docker'), patch.object(self.gate, 'http', return_value=(503, None)), \
                patch('ops.node_ingress.time.monotonic', side_effect=[0, 2]):
            with self.assertRaises(GateError):
                self.gate.change(True, seconds=1)
        self.assertEqual(json.loads((self.root / 'state.json').read_text())['state'], 'closing')
        self.assertIn('return 503;', (self.root / 'public-config/nginx.conf').read_text())
        with patch.object(self.gate, 'inspect', return_value=self.item), patch.object(self.gate, 'backend') as backend:
            with self.assertRaises(GateError):
                self.gate.change(False, self.image)
            backend.assert_not_called()

    def test_new_closed_worker_alone_does_not_hide_surviving_old_worker(self):
        with patch.object(self.gate, 'inspect', return_value=self.item), \
                patch.object(self.gate, 'workers', side_effect=[{101}, {101, 102}, {102}]), \
                patch.object(self.gate, 'docker'), patch.object(self.gate, 'http', return_value=(503, None)), \
                patch('ops.node_ingress.time.sleep') as sleep:
            self.assertEqual(self.gate.change(True)['state'], 'closed')
            sleep.assert_called_once()

    def test_volatile_health_fields_do_not_block_verified_backend_open(self):
        save(self.root / 'state.json', {'state': 'closed'})
        info = {'engine': 'odx', 'engineVersion': '3.8.5', 'version': '2.4.0', 'memoryAvailable': 1}
        with patch.object(self.gate, 'inspect', return_value=self.item), \
                patch.object(self.gate, 'workers', side_effect=[{101}, {102}]), \
                patch.object(self.gate, 'docker'), patch.object(self.gate, 'backend', return_value=info), \
                patch.object(self.gate, 'http', return_value=(200, {**info, 'memoryAvailable': 2})):
            self.assertEqual(self.gate.change(False, self.image)['state'], 'open')

    def test_wrong_image_or_public_backend_cannot_open_ingress(self):
        item = {'Image': self.image, 'State': {'Running': True}, 'NetworkSettings': {
            'Ports': {'3000/tcp': [{'HostIp': '0.0.0.0', 'HostPort': '13004'}]}}}
        with patch.object(self.gate, 'docker', return_value=json.dumps([item])), \
                patch.object(self.gate, 'http') as http:
            with self.assertRaises(GateError):
                self.gate.backend(self.image)
            item['Image'] = 'sha256:' + 'b' * 64
            item['NetworkSettings']['Ports']['3000/tcp'][0]['HostIp'] = '127.0.0.1'
            with self.assertRaises(GateError):
                self.gate.backend(self.image)
            http.assert_not_called()

    def test_wrong_config_mount_or_entrypoint_cannot_attest_closed_ingress(self):
        self.gate.config.update(engineId='engine', proxyName='owned', proxyImage=self.image)
        item = {'Image': self.image, 'State': {'Running': True}, 'HostConfig': {'NetworkMode': 'host'},
                'Config': {'Entrypoint': ['nginx'], 'Cmd': ['-c', '/config/nginx.conf', '-g', 'daemon off;']},
                'Mounts': [{'Destination': '/config', 'Type': 'bind', 'RW': False,
                            'Source': str(self.root / 'public-config')}]}
        def docker(args):
            if args[0] == 'info':
                return 'engine\n'
            return json.dumps([item])
        with patch.dict(os.environ, {'DOCKER_HOST': 'unix:///var/run/docker.sock', 'DOCKER_CONTEXT': ''}), \
                patch.object(self.gate, 'docker', side_effect=docker):
            self.gate.inspect()
            item['Mounts'][0]['Source'] = '/other/config'
            with self.assertRaises(GateError):
                self.gate.inspect()
            item['Mounts'][0]['Source'] = str(self.root / 'public-config')
            item['Config']['Cmd'] = ['-c', '/other.conf']
            with self.assertRaises(GateError):
                self.gate.inspect()
        with patch.dict(os.environ, {'DOCKER_HOST': 'ssh://elsewhere', 'DOCKER_CONTEXT': ''}), \
                patch.object(self.gate, 'docker') as command:
            with self.assertRaises(GateError):
                self.gate.inspect()
            command.assert_not_called()


if __name__ == '__main__':
    unittest.main()
