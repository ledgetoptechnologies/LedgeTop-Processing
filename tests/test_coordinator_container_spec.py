import contextlib
import copy
import io
import json
import os
from pathlib import Path
import stat
import tempfile
import unittest

from ops import coordinator_container_spec as spec

IMAGE_CONFIG = {'Entrypoint': None, 'Cmd': ['python', '-m', 'processing.app'], 'Healthcheck': None,
                'WorkingDir': '/app', 'User': '10001:10001', 'Env': ['PATH=/usr/bin']}
CONTAINER = {
    'Config': dict(IMAGE_CONFIG, Env=['PATH=/usr/bin', 'PROCESSING_API_TOKEN=synthetic-secret-value'],
                   Labels={}),
    'HostConfig': {'ReadonlyRootfs': True, 'Tmpfs': {'/tmp': 'size=64m'}, 'Memory': 268435456,
                   'NanoCpus': 500000000, 'CapDrop': ['ALL'], 'SecurityOpt': ['no-new-privileges'],
                   'NetworkMode': 'ltp', 'PortBindings': {'8080/tcp': [{'HostIp': '0.0.0.0', 'HostPort': '3005'}],
                                                          '8443/tcp': [{'HostIp': '', 'HostPort': '18443'}]},
                   'RestartPolicy': {'Name': 'unless-stopped', 'MaximumRetryCount': 0},
                   'IpcMode': 'private', 'ShmSize': 67108864, 'Runtime': 'runc', 'Privileged': False,
                   'LogConfig': {'Type': 'json-file', 'Config': {}}, 'MaskedPaths': ['/proc/kcore']},
    'Mounts': [{'Type': 'volume', 'Name': 'state', 'Destination': '/state', 'RW': True}],
    'NetworkSettings': {'Networks': {'ltp': {}}},
}
OLD = {'Id': 'sha256:' + 'a' * 64, 'Config': IMAGE_CONFIG}
NEW = {'Id': 'sha256:' + 'b' * 64, 'Config': IMAGE_CONFIG}


def derive(container=CONTAINER, new=NEW, **kwargs):
    options = dict(mode='isolated', volume='state-copy', name='candidate', network='ltp-candidate')
    options.update(kwargs)
    return spec.derive(container, OLD, new, **options)


class ContainerSpecTest(unittest.TestCase):
    def test_isolated_has_no_ports_and_no_restart(self):
        argv = derive()['argv']
        self.assertNotIn('--publish', argv)
        self.assertEqual(argv[argv.index('--network') + 1], 'ltp-candidate')
        self.assertEqual(argv[argv.index('--restart') + 1], 'no')
        self.assertIn('type=volume,source=state-copy,destination=/state', argv)
        self.assertEqual(argv[-1], NEW['Id'])
        for flag in ('--read-only', '--cap-drop', '--security-opt', '--memory', '--cpus', '--tmpfs'):
            self.assertIn(flag, argv)

    def test_promoted_restores_ports_network_and_policy(self):
        argv = derive(mode='promoted', network=None)['argv']
        self.assertIn('0.0.0.0:3005:8080/tcp', argv)
        self.assertIn('18443:8443/tcp', argv)
        self.assertEqual(argv[argv.index('--network') + 1], 'ltp')
        self.assertEqual(argv[argv.index('--restart') + 1], 'unless-stopped')

    def test_isolated_requires_dedicated_network(self):
        with self.assertRaises(spec.SpecError):
            derive(network=None)

    def test_refuses_unreviewed_host_config(self):
        for key, value in (('Privileged', True), ('PidMode', 'host'), ('Devices', [{'PathOnHost': '/dev/x'}]),
                           ('IpcMode', 'host'), ('VolumesFrom', ['other'])):
            container = copy.deepcopy(CONTAINER)
            container['HostConfig'][key] = value
            with self.assertRaises(spec.SpecError, msg=key):
                derive(container)

    def test_refuses_extra_mounts_compose_and_overrides(self):
        container = copy.deepcopy(CONTAINER)
        container['Mounts'].append({'Type': 'bind', 'Destination': '/x', 'RW': True})
        with self.assertRaises(spec.SpecError):
            derive(container)
        container = copy.deepcopy(CONTAINER)
        container['Config']['Labels'] = {'com.docker.compose.project': 'p'}
        with self.assertRaises(spec.SpecError):
            derive(container)
        container = copy.deepcopy(CONTAINER)
        container['Config']['Cmd'] = ['sh']
        with self.assertRaises(spec.SpecError):
            derive(container)
        new = copy.deepcopy(NEW)
        new['Config'] = dict(IMAGE_CONFIG, User='0')
        with self.assertRaises(spec.SpecError):
            derive(new=new)

    def test_cli_writes_private_env_and_never_prints_values(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name, value in (('c', [CONTAINER]), ('o', [OLD]), ('n', [NEW])):
                (root / (name + '.json')).write_text(json.dumps(value))
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                result = spec.main(['--container', str(root / 'c.json'), '--old-image', str(root / 'o.json'),
                                    '--new-image', str(root / 'n.json'), '--mode', 'isolated', '--volume', 'v',
                                    '--name', 'n', '--network', 'net', '--env-out', str(root / 'env')])
            self.assertEqual(result, 0)
            self.assertNotIn('synthetic-secret-value', output.getvalue())
            self.assertIn('PROCESSING_API_TOKEN', json.loads(output.getvalue())['envKeys'])
            self.assertEqual((root / 'env').read_text(), 'PROCESSING_API_TOKEN=synthetic-secret-value\n')
            if os.name == 'posix':
                self.assertEqual(stat.S_IMODE((root / 'env').stat().st_mode), 0o600)
            with contextlib.redirect_stdout(io.StringIO()), self.assertRaises(FileExistsError):
                spec.main(['--container', str(root / 'c.json'), '--old-image', str(root / 'o.json'),
                           '--new-image', str(root / 'n.json'), '--mode', 'isolated', '--volume', 'v',
                           '--name', 'n', '--network', 'net', '--env-out', str(root / 'env')])


if __name__ == '__main__':
    unittest.main()
