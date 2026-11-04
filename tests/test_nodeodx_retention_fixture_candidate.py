import json
import hashlib
import unittest
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from ops import nodeodx_retention_fixture_candidate as fixture
from ops import nodeodx_retention_lifecycle_candidate as lifecycle


class CandidatePreflightTests(unittest.TestCase):
    startup = fixture.startup_digest('', '/var/www', ['/usr/bin/node'], ['index.js'], None)
    env = ['PATH=/usr/bin', 'NVIDIA_VISIBLE_DEVICES=void']
    def test_expected_image_and_engine_are_read_only(self):
        image = sorted(fixture.IMAGES)[0]
        calls = []
        def runner(argv, **kwargs):
            calls.append(argv)
            value = 'synthetic-engine' if argv[3] == 'info' else json.dumps(
                [image, 'linux', 'amd64', '', '/var/www', ['/usr/bin/node'], ['index.js'], None])
            return SimpleNamespace(stdout=value, stderr='')
        result = fixture.preflight(image, 'synthetic-engine', self.startup, runner=runner)
        self.assertFalse(result['runtimeQualified'])
        self.assertEqual([call[3] for call in calls], ['info', 'image'])
        self.assertTrue(all(call[:3] == [fixture.DOCKER, '--host', fixture.HOST] for call in calls))

    def test_engine_mismatch_and_declared_volume_refuse(self):
        image = sorted(fixture.IMAGES)[0]
        mismatch = lambda *args, **kwargs: SimpleNamespace(stdout='different', stderr='')
        with self.assertRaisesRegex(ValueError, 'Engine identity'):
            fixture.preflight(image, 'synthetic-engine', self.startup, runner=mismatch)
        def with_volume(argv, **kwargs):
            value = 'synthetic-engine' if argv[3] == 'info' else json.dumps(
                [image, 'linux', 'amd64', '', '/var/www', None, None, {'/data': {}}])
            return SimpleNamespace(stdout=value, stderr='')
        with self.assertRaisesRegex(ValueError, 'volumes'):
            fixture.preflight(image, 'synthetic-engine', self.startup, runner=with_volume)

    def test_fixture_names_require_valid_owner(self):
        image = sorted(fixture.IMAGES)[0]
        names = fixture.fixture_names(image, 'a' * 32)
        self.assertEqual(names['volume'], 'ltds-nodeodx-data-' + 'a' * 32)
        self.assertEqual(names['container'], 'ltds-nodeodx-test-' + 'a' * 32)
        with self.assertRaises(ValueError): fixture.fixture_names(image, '../unsafe')

    def test_unverified_inspection_is_refused(self):
        image = sorted(fixture.IMAGES)[0]
        with self.assertRaises(ValueError):
            fixture.verify_owned_container({}, {}, image, 'a' * 32, self.startup, self.env)

    def test_owned_inspection_rejects_runtime_device_and_identity_drift(self):
        image, owner = sorted(fixture.IMAGES)[0], 'a' * 32
        names = fixture.fixture_names(image, owner)
        volume = {'Name':names['volume'], 'Driver':'local', 'Scope':'local',
                  'Labels':{'ltds.fixture.owner':owner}, 'Options':None, 'Mountpoint':'/synthetic-volume'}
        config = {'Labels':{'ltds.fixture.owner':owner}, 'User':'', 'WorkingDir':'/var/www',
                  'Entrypoint':['/usr/bin/node'], 'Cmd':['index.js'], 'Volumes':None, 'Env':self.env}
        host = {'NetworkMode':'none', 'Runtime':'runc', 'Privileged':False, 'RestartPolicy':{'Name':'no'},
                'Devices':[], 'DeviceRequests':None, 'DeviceCgroupRules':None, 'Binds':None,
                'PortBindings':None, 'PublishAllPorts':False, 'PidMode':'', 'IpcMode':'private',
                'UsernsMode':'', 'CgroupnsMode':'private', 'Sysctls':None, 'GroupAdd':None, 'CapAdd':None,
                'Memory':1024**3, 'NanoCpus':10**9, 'PidsLimit':128, 'CapDrop':['ALL'],
                'SecurityOpt':['no-new-privileges'], 'LogConfig':{'Type':'none'},
                'Mounts':[{'Type':'volume','Source':names['volume'],'Target':'/var/www/data',
                           'ReadOnly':False,'VolumeOptions':{'NoCopy':True}}],
                'Tmpfs':{'/var/www/tmp':'rw,nosuid,nodev,size=128m,mode=1777',
                         '/var/www/logs':'rw,nosuid,nodev,size=16m,mode=1777',
                         '/tmp':'rw,nosuid,nodev,size=128m,mode=1777'}}
        container = {'Id':'b'*64, 'Name':'/'+names['container'], 'Image':image, 'Config':config,
                     'HostConfig':host, 'NetworkSettings':{'Ports':None,'Networks':{'none':{}}},
                     'Mounts':[{'Type':'volume','Name':names['volume'],'Source':'/synthetic-volume','Driver':'local','Destination':'/var/www/data','RW':True}]}
        self.assertTrue(fixture.verify_owned_container(container, volume, image, owner, self.startup, self.env))
        for field, value in [('NetworkMode','host'),('Runtime','nvidia'),('Devices',[{'PathOnHost':'/dev/dri'}]),
                             ('DeviceRequests',[{'Driver':'nvidia'}]),('Binds',['/host:/data']),('CapAdd',['SYS_ADMIN']),
                             ('SecurityOpt',['no-new-privileges','seccomp=unconfined']),('CgroupnsMode','host'),
                             ('UsernsMode','host'),('Sysctls',{'net.ipv4.ip_forward':'1'})]:
            bad = deepcopy(container); bad['HostConfig'][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                fixture.verify_owned_container(bad, volume, image, owner, self.startup, self.env)
        bad = deepcopy(volume); bad['Labels'] = {}
        with self.assertRaises(ValueError): fixture.verify_owned_container(container, bad, image, owner, self.startup, self.env)
        bad = deepcopy(container); bad['Config']['Cmd'] = ['different']
        with self.assertRaises(ValueError): fixture.verify_owned_container(bad, volume, image, owner, self.startup, self.env)
        bad = deepcopy(container); bad['Config']['Env'] = self.env + ['UNKNOWN=1']
        with self.assertRaises(ValueError): fixture.verify_owned_container(bad, volume, image, owner, self.startup, self.env)
        bad = deepcopy(container); bad['Mounts'][0]['Source'] = '/different'
        with self.assertRaises(ValueError): fixture.verify_owned_container(bad, volume, image, owner, self.startup, self.env)
        bad = deepcopy(container); bad['HostConfig']['Mounts'][0]['VolumeOptions']['NoCopy'] = False
        with self.assertRaises(ValueError): fixture.verify_owned_container(bad, volume, image, owner, self.startup, self.env)

    def test_lifecycle_is_disabled_before_any_command(self):
        with patch.object(lifecycle, '_docker') as docker:
            with self.assertRaisesRegex(RuntimeError, 'disabled'):
                lifecycle.execute_candidate(sorted(fixture.IMAGES)[0], 'engine', '0'*64, '0'*64, '/not-used', self.env)
            docker.assert_not_called()

    def test_effective_probe_requires_distinct_namespaces_and_exact_volume(self):
        host = {'user':{'dev':'1','ino':'2'}, 'cgroup':{'dev':'1','ino':'3'}}
        volume = {'dev':'4','ino':'5'}
        value = {'ok':True,'mode':'device','userns':{'dev':'1','ino':'20'},
                 'cgroupns':{'dev':'1','ino':'30'},'volume':volume}
        lifecycle._check_effective_probe(value, host, volume)
        for field, replacement in [('userns',host['user']),('cgroupns',host['cgroup']),('volume',{'dev':'4','ino':'6'})]:
            bad=deepcopy(value);bad[field]=replacement
            with self.subTest(field=field), self.assertRaises(ValueError):
                lifecycle._check_effective_probe(bad, host, volume)

    def test_uncertain_mutation_cannot_be_replayed(self):
        probe = Path(lifecycle.__file__).with_name('nodeodx_retention_probe.js')
        digest = hashlib.sha256(probe.read_bytes()).hexdigest()
        owner = lifecycle.OwnedLifecycle(sorted(fixture.IMAGES)[0], 'engine', self.startup,
                                         'a'*32, SimpleNamespace(record=lambda *a, **kw: None), digest, self.env)
        owner.uncertain = True
        with patch.object(lifecycle, 'EXECUTION_ENABLED', True), patch.object(lifecycle, '_docker') as docker:
            with self.assertRaisesRegex(RuntimeError, 'uncertain'): owner.start()
            docker.assert_not_called()

    def test_retention_requires_same_task_info_before_and_after_restart(self):
        probe = Path(lifecycle.__file__).with_name('nodeodx_retention_probe.js')
        digest = hashlib.sha256(probe.read_bytes()).hexdigest()
        owner = lifecycle.OwnedLifecycle(sorted(fixture.IMAGES)[0], 'engine', self.startup,
                                         'a'*32, SimpleNamespace(record=lambda *a, **kw: None), digest, self.env)
        owner.container_id = 'b'*64
        owner.runtime_verified = True
        first = '11111111-1111-4111-8111-111111111111'
        other = '22222222-2222-4222-8222-222222222222'
        with patch.object(lifecycle, 'EXECUTION_ENABLED', True), \
             patch.object(owner, '_engine'), \
             patch.object(owner, '_container', return_value={'State': {'Running': True}}), \
             patch.object(lifecycle, '_docker', side_effect=lambda args, **kw: json.dumps({'ok': True, 'mode': args[5]})):
            owner.starts = 1
            with self.assertRaises(ValueError): owner.probe('marker-check', first)
            with self.assertRaisesRegex(ValueError, 'empty task list'): owner.probe('init', first)
            owner.probe('empty', first)
            owner.probe('init', first)
            owner.probe('upload', first)
            owner.probe('info', first)
            owner.probe('marker-put', first)
            owner.starts = 2
            with self.assertRaises(ValueError): owner.probe('info', other)
            self.assertFalse(owner.retention_verified)
            owner.probe('info', first)
            self.assertTrue(owner.retention_verified)
            with self.assertRaises(ValueError): owner.probe('remove', first)
            owner.probe('marker-check', first)
            with self.assertRaises(ValueError): owner.probe('remove', other)
            owner.probe('remove', first)
            self.assertTrue(owner.task_removed)
