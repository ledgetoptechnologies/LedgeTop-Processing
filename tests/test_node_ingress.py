import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from ops.node_ingress import GateError, Ingress, configuration, save


class IngressConfigurationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.image = 'sha256:' + 'a' * 64
        self.config = {'proxyName': 'owned-proxy', 'backendName': 'owned-node',
                       'proxyImage': self.image, 'listenPort': 13005, 'backendPort': 13004}

    def ingress(self, **changes):
        self.config.update(changes)
        (self.root / 'gateway.json').write_text(json.dumps(self.config))
        return Ingress(self.root)

    def test_restricted_config_allows_exact_peers_and_loopback_then_denies_all(self):
        text = configuration(13005, 13004, False,
                             ['192.168.50.89', '172.17.0.0/16', '2001:db8::1', '2001:db8:1::/64'])
        for address in ('127.0.0.1', '::1', '192.168.50.89', '172.17.0.0/16',
                        '2001:db8::1', '2001:db8:1::/64'):
            self.assertIn('allow ' + address + ';', text)
        self.assertIn('deny all;', text)
        self.assertLess(text.index('allow 192.168.50.89;'), text.index('deny all;'))
        self.assertNotIn('real_ip_header', text)
        self.assertNotIn('X-Forwarded-For', text)
        self.assertNotIn('192.168.50.88', text)

    def test_empty_list_preserves_unrestricted_legacy_config(self):
        text = configuration(13005, 13004, False)
        self.assertNotIn('allow ', text)
        self.assertNotIn('deny all;', text)

    def test_canonical_loopback_is_not_duplicated_and_closed_gate_stays_closed(self):
        text = configuration(13005, 13004, True, ['127.0.0.1', '::1'])
        self.assertEqual(text.count('allow 127.0.0.1;'), 1)
        self.assertEqual(text.count('allow ::1;'), 1)
        self.assertIn('return 503;', text)
        self.assertNotIn('proxy_pass', text)

    def test_invalid_clients_cannot_inject_or_broaden_nginx_config(self):
        invalid = (None, '192.168.50.89', {'ip': '192.168.50.89'}, [True], [''],
                   ['example.com'], ['192.168.50.89; allow all'], ['192.168.50.89\nallow all;'],
                   [' 192.168.50.89'], ['192.168.050.089'], ['192.168.50.89/24'],
                   ['2001:0db8::1'], ['fe80::1%eth0'], ['unix:'],
                   ['192.168.50.89', '192.168.50.89'], ['127.0.0.1'] * 65)
        for clients in invalid:
            with self.subTest(clients=clients), self.assertRaises(GateError):
                configuration(13005, 13004, False, clients)

    def test_private_config_validates_allowlist_engine_and_credential(self):
        gate = self.ingress(allowedClients=['192.168.50.89'], backendEngine='odm',
                            backendToken='synthetic-private-token&?=')
        self.assertEqual(gate.config['backendEngine'], 'odm')
        public = configuration(13005, 13004, False, gate.config['allowedClients'])
        self.assertNotIn('synthetic-private-token', public)
        for key, value in (('backendEngine', 'ODM'), ('backendEngine', 'arbitrary'),
                           ('backendToken', None), ('backendToken', True),
                           ('backendToken', 'has space'), ('backendToken', 'line\nbreak'),
                           ('backendToken', 'x' * 4097), ('allowedClients', ['hostname.invalid'])):
            with self.subTest(key=key, value_type=type(value).__name__), self.assertRaises(GateError):
                self.ingress(**{key: value})
            self.config.update(allowedClients=['192.168.50.89'], backendEngine='odm',
                               backendToken='synthetic-private-token&?=')

    def test_private_health_token_is_urlencoded_not_rendered_into_public_config(self):
        gate = self.ingress(backendToken='synthetic&?=token')
        response = MagicMock(status=200)
        response.read.return_value = b'{"engine":"odx"}'
        opener = MagicMock()
        opener.open.return_value.__enter__.return_value = response
        gate.opener = opener
        self.assertEqual(gate.http(13004), (200, {'engine': 'odx'}))
        opener.open.assert_called_once_with(
            'http://127.0.0.1:13004/info?token=synthetic%26%3F%3Dtoken', timeout=3)

    def test_odm_backend_requires_explicit_engine_identity(self):
        gate = self.ingress()
        item = {'Image': self.image, 'State': {'Running': True}, 'NetworkSettings': {
            'Ports': {'3000/tcp': [{'HostIp': '127.0.0.1', 'HostPort': '13004'}]}}}
        with patch.object(gate, 'docker', return_value=json.dumps([item])), \
                patch.object(gate, 'http', return_value=(200, {'engine': 'odm'})):
            with self.assertRaises(GateError):
                gate.backend(self.image)
            gate.config['backendEngine'] = 'odm'
            self.assertEqual(gate.backend(self.image)['engine'], 'odm')
        with patch.object(gate, 'docker', return_value=json.dumps([item])), \
                patch.object(gate, 'http', return_value=(200, {'engine': 'odx'})):
            with self.assertRaises(GateError):
                gate.backend(self.image)


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
