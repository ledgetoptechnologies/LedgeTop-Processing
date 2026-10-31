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
    options = dict(mode='isolated', volume='state-copy', name='candidate', network='none')
    options.update(kwargs)
    return spec.derive(container, OLD, new, **options)


class ContainerSpecTest(unittest.TestCase):
    def test_legacy_fence_requires_original_distinct_volume_and_sets_required_flag(self):
        result=derive(legacy_fence_volume='state')
        self.assertIn('type=volume,source=state,destination=/legacy-state,readonly',result['argv'])
        self.assertIn('PROCESSING_LEGACY_FENCE_REQUIRED=1',result['envFile'])
        for options in ({'legacy_fence_volume':'other'}, {'legacy_fence_volume':'state','volume':'state'},
                        {'legacy_fence_volume':'state','lease_volume':'state'}):
            with self.assertRaises(spec.SpecError):derive(**options)
        source=copy.deepcopy(CONTAINER)
        source['Config']['Env'].append('PROCESSING_LEGACY_FENCE_REQUIRED=1')
        with self.assertRaises(spec.SpecError):derive(source)
        with self.assertRaises(spec.SpecError):derive(source,legacy_fence_volume='state')
    def test_isolated_has_no_ports_and_no_restart(self):
        argv = derive()['argv']
        self.assertNotIn('--publish', argv)
        self.assertEqual(argv[argv.index('--network') + 1], 'none')
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

    def test_isolated_requires_network_none(self):
        for network in (None, 'ltp-candidate', 'bridge'):
            with self.assertRaises(spec.SpecError, msg=network):
                derive(network=network)

    def test_lease_mount_and_production_hold(self):
        result = derive(mode='promoted', network=None, lease_dir='/srv/ltp-lease', production_admission='held')
        argv = result['argv']
        self.assertIn('type=bind,source=/srv/ltp-lease,destination=/run/ledgetop-authority-lease', argv)
        self.assertIn('PROCESSING_AUTHORITY_LEASE_DIR=/run/ledgetop-authority-lease', result['envFile'])
        self.assertIn('PROCESSING_VIEWER_PRODUCTION_ADMISSION=held', result['envFile'])
        for bad in ('relative', '/a,b', '/a=b'):
            with self.assertRaises(spec.SpecError):
                derive(lease_dir=bad)
        with self.assertRaises(spec.SpecError):
            derive(production_admission='closed')
        container = copy.deepcopy(CONTAINER)
        container['Config']['Env'].append('PROCESSING_VIEWER_PRODUCTION_ADMISSION=open')
        with self.assertRaises(spec.SpecError):
            derive(container, production_admission='held')

    def test_compose_named_volume_bind_and_local_log_driver_are_carried(self):
        container = copy.deepcopy(CONTAINER)
        container['HostConfig']['Binds'] = ['state:/state:rw']
        container['HostConfig']['LogConfig'] = {'Type': 'local', 'Config': {}}
        argv = derive(container)['argv']
        self.assertIn('type=volume,source=state-copy,destination=/state', argv)
        self.assertEqual(argv[argv.index('--log-driver') + 1], 'local')
        self.assertNotIn('state:/state:rw', argv)

    def test_host_path_binds_and_shipping_log_drivers_refuse(self):
        for binds in (['/srv/state:/state'], ['state:/elsewhere'], ['other:/state'], ['state:/state:ro'],
                      ['state:/state', '/etc:/etc']):
            container = copy.deepcopy(CONTAINER)
            container['HostConfig']['Binds'] = binds
            with self.assertRaises(spec.SpecError, msg=binds):
                derive(container)
        for log in ({'Type': 'syslog', 'Config': {}}, {'Type': 'local', 'Config': {'max-size': '1m'}},
                    {'Type': 'fluentd', 'Config': {}}):
            container = copy.deepcopy(CONTAINER)
            container['HostConfig']['LogConfig'] = log
            with self.assertRaises(spec.SpecError, msg=log):
                derive(container)

    def test_labels(self):
        argv = derive(extra_labels=['ltp.rehearsal=claude-reh-abc'])['argv']
        self.assertEqual(argv[argv.index('--label') + 1], 'ltp.rehearsal=claude-reh-abc')
        for bad in ('noequals', 'a=b c', 'A=1', 'a=$(x)'):
            with self.assertRaises(spec.SpecError, msg=bad):
                derive(extra_labels=[bad])

    def test_lease_volume(self):
        result = derive(mode='promoted', network=None, lease_volume='ltp-authority-lease')
        self.assertIn('type=volume,source=ltp-authority-lease,destination=/run/ledgetop-authority-lease', result['argv'])
        self.assertIn('PROCESSING_AUTHORITY_LEASE_DIR=/run/ledgetop-authority-lease', result['envFile'])
        for bad in ('state-copy', 'a,b', '/abs', ''):
            with self.assertRaises(spec.SpecError, msg=bad):
                derive(lease_volume=bad)
        with self.assertRaises(spec.SpecError):
            derive(lease_volume='lease', lease_dir='/srv/lease')

    def test_compose_source_needs_explicit_acceptance_and_labels_are_not_carried(self):
        container = copy.deepcopy(CONTAINER)
        container['Config']['Labels'] = {'com.docker.compose.project': 'p'}
        argv = derive(container, accept_compose_source=True)['argv']
        self.assertFalse(any('com.docker.compose' in item for item in argv))

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
                                    '--name', 'n', '--network', 'none', '--env-out', str(root / 'env')])
            self.assertEqual(result, 0)
            self.assertNotIn('synthetic-secret-value', output.getvalue())
            self.assertIn('PROCESSING_API_TOKEN', json.loads(output.getvalue())['envKeys'])
            self.assertEqual((root / 'env').read_text(), 'PROCESSING_API_TOKEN=synthetic-secret-value\n')
            if os.name == 'posix':
                self.assertEqual(stat.S_IMODE((root / 'env').stat().st_mode), 0o600)
            with contextlib.redirect_stdout(io.StringIO()), self.assertRaises(FileExistsError):
                spec.main(['--container', str(root / 'c.json'), '--old-image', str(root / 'o.json'),
                           '--new-image', str(root / 'n.json'), '--mode', 'isolated', '--volume', 'v',
                           '--name', 'n', '--network', 'none', '--env-out', str(root / 'env')])


if __name__ == '__main__':
    unittest.main()
