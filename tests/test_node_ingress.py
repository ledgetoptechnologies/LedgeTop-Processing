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
        if os.name != 'nt':
            (self.root / 'gateway.json').chmod(0o600)
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

    def test_tls_profile_renders_auth_on_closed_and_open_without_secret(self):
        for closed in (True, False):
            text = configuration(13005, 13004, closed, ['192.168.50.89'], True)
            self.assertIn('listen 0.0.0.0:13005 ssl;', text)
            self.assertIn('ssl_certificate /config/gateway.crt;', text)
            self.assertIn('ssl_certificate_key /config/gateway.key;', text)
            self.assertIn('include /config/gateway-auth.conf;', text)
            self.assertIn('access_log off;', text)
            self.assertIn('error_log /dev/null crit;', text)
            if closed:
                self.assertIn('return 503;', text)
                self.assertNotIn('proxy_pass', text)
            else:
                self.assertIn('proxy_set_header X-LedgeTop-Gateway "";', text)
                self.assertIn('proxy_pass http://127.0.0.1:13004;', text)

    def test_tls_profile_requires_qualification_before_reading_credentials(self):
        with self.assertRaisesRegex(GateError, 'not qualified'):
            self.ingress(gatewayAuthEnabled=True, gatewaySecret='a' * 48)
        with self.assertRaisesRegex(GateError, 'invalid gateway'):
            self.ingress(gatewayAuthEnabled='true')

    def test_gateway_probe_uses_verified_https_header_only_for_gateway(self):
        secret = 'SyntheticGatewayKey_' + 'a' * 32
        with patch('ops.node_ingress.GATEWAY_AUTH_QUALIFIED', True), \
                patch.object(Ingress, '_tls_files'), \
                patch('ops.node_ingress.ssl.create_default_context'):
            gate = self.ingress(gatewayAuthEnabled=True, gatewaySecret=secret, backendToken='native-token')
        response = MagicMock(status=200)
        response.read.return_value = b'{"engine":"odx"}'
        gateway_opener = MagicMock()
        gateway_opener.open.return_value.__enter__.return_value = response
        gate.gateway_opener = gateway_opener
        self.assertEqual(gate.http(13005), (200, {'engine': 'odx'}))
        request = gateway_opener.open.call_args.args[0]
        self.assertEqual(request.full_url, 'https://127.0.0.1:13005/info?token=native-token')
        self.assertEqual(request.get_header('X-ledgetop-gateway'), secret)
        self.assertNotIn(secret, request.full_url)
        backend_opener = MagicMock()
        backend_opener.open.return_value.__enter__.return_value = response
        gate.opener = backend_opener
        gate.http(13004)
        backend_opener.open.assert_called_once_with('http://127.0.0.1:13004/info?token=native-token', timeout=3)

    def test_gateway_secret_must_differ_from_native_token(self):
        secret = 'SyntheticGatewayKey_' + 'b' * 32
        with patch('ops.node_ingress.GATEWAY_AUTH_QUALIFIED', True), \
                self.assertRaisesRegex(GateError, 'must differ'):
            self.ingress(gatewayAuthEnabled=True, gatewaySecret=secret, backendToken=secret)

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

    def test_tls_profile_persists_auth_across_close_and_open(self):
        secret = 'SyntheticGatewayKey_' + 'c' * 32
        self.gate.gateway_auth = True
        self.gate.config.update(gatewaySecret=secret, backendToken='native-token')
        (self.root / 'gateway.json').write_text('{}')
        (self.root / 'gateway.json').chmod(0o600)
        info = {'engine': 'odx', 'engineVersion': '3.8.5', 'version': '2.4.0'}
        with patch('ops.node_ingress.GATEWAY_AUTH_QUALIFIED', True), \
                patch.object(self.gate, '_tls_files'), \
                patch.object(self.gate, 'inspect', return_value=self.item), \
                patch.object(self.gate, 'workers', side_effect=[{101}, {102}, {102}, {103}]), \
                patch.object(self.gate, 'docker'), \
                patch.object(self.gate, 'backend', return_value=info), \
                patch.object(self.gate, 'http', side_effect=[(503, None), (200, info)]):
            self.assertEqual(self.gate.change(True)['state'], 'closed')
            closed = (self.root / 'public-config/nginx.conf').read_text()
            self.assertIn('include /config/gateway-auth.conf;', closed)
            self.assertNotIn(secret, closed)
            self.assertEqual(self.gate.change(False, self.image)['state'], 'open')
        opened = (self.root / 'public-config/nginx.conf').read_text()
        private = self.root / 'public-config/gateway-auth.conf'
        self.assertIn('include /config/gateway-auth.conf;', opened)
        self.assertIn('proxy_set_header X-LedgeTop-Gateway "";', opened)
        self.assertNotIn(secret, opened)
        self.assertIn(secret, private.read_text())
        self.assertEqual(private.stat().st_mode & 0o777, 0o600)

    def test_invalid_gateway_secret_does_not_write_state(self):
        self.gate.gateway_auth = True
        self.gate.config['gatewaySecret'] = 'invalid; return 200;'
        with patch('ops.node_ingress.GATEWAY_AUTH_QUALIFIED', True), \
                patch.object(self.gate, '_tls_files'), self.assertRaises(GateError):
            self.gate.change(True)
        self.assertFalse((self.root / 'state.json').exists())

    def test_tls_material_rejects_group_readable_key_and_untrusted_directory(self):
        directory = self.root / 'public-config'
        for name in ('gateway.crt', 'gateway.key', 'gateway-ca.crt'):
            path = directory / name
            path.write_text('synthetic')
            path.chmod(0o600 if name == 'gateway.key' else 0o644)
        self.gate._tls_files()
        (directory / 'gateway.key').chmod(0o640)
        with self.assertRaisesRegex(GateError, 'trust material unavailable'):
            self.gate._tls_files()
        (directory / 'gateway.key').chmod(0o600)
        directory.chmod(0o777)
        with self.assertRaisesRegex(GateError, 'trust material unavailable'):
            self.gate._tls_files()

    def test_gateway_json_must_be_private_before_state_change(self):
        self.gate.gateway_auth = True
        self.gate.config['gatewaySecret'] = 'SyntheticGatewayKey_' + 'd' * 32
        (self.root / 'gateway.json').write_text('{}')
        (self.root / 'gateway.json').chmod(0o644)
        with patch('ops.node_ingress.GATEWAY_AUTH_QUALIFIED', True), \
                patch.object(self.gate, '_tls_files'), self.assertRaises(GateError):
            self.gate.change(True)
        self.assertFalse((self.root / 'state.json').exists())

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
