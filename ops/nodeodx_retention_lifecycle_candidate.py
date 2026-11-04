"""Unreviewed disabled draft. Never call against a live host.

All mutation entry points remain hard-disabled pending independent review.
"""
import json
import hashlib
import os
import re
import stat
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

from ops import nodeodx_retention_fixture_candidate as policy

EXECUTION_ENABLED = False


def _docker(args, *, input_text=None, timeout=15, runner=subprocess.run):
    env = os.environ.copy()
    env.pop('DOCKER_HOST', None); env.pop('DOCKER_CONTEXT', None)
    result = runner([policy.DOCKER, '--host', policy.HOST, *args], input=input_text,
                    text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    timeout=timeout, check=True, env=env)
    if len(result.stdout.encode()) > 65536:
        raise ValueError('Docker response too large')
    return result.stdout.strip()


class Journal:
    def __init__(self, parent, owner):
        parent = Path(parent)
        if not parent.is_absolute() or parent.resolve() != parent:
            raise ValueError('private absolute journal parent required')
        info = parent.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise ValueError('private owned journal parent required')
        self.directory = Path(tempfile.mkdtemp(prefix='nodeodx-fixture-' + owner + '-', dir=parent))
        self.fd = os.open(self.directory / 'events.jsonl', os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        try:
            for path in (self.directory, parent):
                fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                try: os.fsync(fd)
                finally: os.close(fd)
        except BaseException:
            os.close(self.fd)
            raise

    def record(self, event, **values):
        raw = (json.dumps({'event': event, **values}, sort_keys=True, separators=(',', ':')) + '\n').encode()
        if len(raw) > 4096:
            raise ValueError('journal event too large')
        written = os.write(self.fd, raw)
        if written != len(raw):
            raise OSError('incomplete journal write')
        os.fsync(self.fd)

    def close(self):
        os.close(self.fd)


def volume_valid(volume, image, owner):
    names = policy.fixture_names(image, owner)
    if (not isinstance(volume, dict) or volume.get('Name') != names['volume'] or
            volume.get('Driver') != 'local' or volume.get('Scope') != 'local' or
            volume.get('Options') not in (None, {}) or
            (volume.get('Labels') or {}).get('ltds.fixture.owner') != owner):
        raise ValueError('owned local volume identity mismatch')


def _fixed_probe(probe_sha256):
    if not isinstance(probe_sha256, str) or not re.fullmatch(r'[a-f0-9]{64}', probe_sha256):
        raise ValueError('independently approved probe digest required')
    probe_path = Path(__file__).absolute().with_name('nodeodx_retention_probe.js')
    if probe_path.resolve() != probe_path or probe_path.is_symlink() or not probe_path.is_file():
        raise ValueError('fixed probe path not safe')
    raw = probe_path.read_bytes()
    if len(raw) > 65536 or hashlib.sha256(raw).hexdigest() != probe_sha256:
        raise ValueError('fixed probe digest differs')
    return raw.decode('utf-8')


def _identity(info):
    return {'dev': str(info.st_dev), 'ino': str(info.st_ino)}


def _host_namespaces():
    """Refuse a launcher already isolated from the PID-1 namespace view."""
    result = {}
    for name in ('pid', 'user', 'cgroup'):
        local = _identity(os.stat('/proc/self/ns/' + name))
        if local != _identity(os.stat('/proc/1/ns/' + name)):
            raise ValueError('launcher is not in the PID-1 namespace view')
        result[name] = local
    return result


def _check_effective_probe(result, host, volume_identity):
    if (not isinstance(result, dict) or set(result) != {'ok','mode','userns','cgroupns','volume'} or
            result.get('ok') is not True or result.get('mode') != 'device'):
        raise ValueError('unexpected effective runtime evidence')
    for field in ('userns', 'cgroupns', 'volume'):
        value = result[field]
        if (not isinstance(value, dict) or set(value) != {'dev','ino'} or
                any(not isinstance(value[k], str) or not re.fullmatch(r'0|[1-9][0-9]{0,30}', value[k]) for k in ('dev','ino'))):
            raise ValueError('invalid runtime identity')
    if result['userns'] == host['user'] or result['cgroupns'] == host['cgroup']:
        raise ValueError('container namespace not distinct')
    if result['volume'] != volume_identity:
        raise ValueError('container volume identity differs')


class OwnedLifecycle:
    """Command implementation is deliberately unreachable while disabled."""
    def __init__(self, image, engine_id, startup_sha256, owner, journal, probe_sha256, expected_env):
        policy.fixture_names(image, owner)
        self.image, self.engine_id, self.startup_sha256 = image, engine_id, startup_sha256
        self.owner, self.journal = owner, journal
        self._probe_source = _fixed_probe(probe_sha256)
        self._expected_env = tuple(expected_env)
        self.names = policy.fixture_names(image, owner)
        self.container_id = None
        self.starts = 0
        self.retention_verified = False
        self.input_observed = None
        self.fixture_task = None
        self.fixture_stage = 0
        self.empty_verified = False
        self.task_removed = False
        self.runtime_verified = False
        self.uncertain = False
        self.stop_uncertain = False
        self.volume_fd = None
        self.volume_identity = None
        self.volume_path = None

    def _enabled(self):
        if not EXECUTION_ENABLED:
            raise RuntimeError('fixture execution disabled pending review')

    def _certain(self):
        if self.uncertain:
            raise RuntimeError('prior mutation outcome uncertain; retain resources')

    def _open_volume(self, value):
        path = Path(value['Mountpoint'])
        if not path.is_absolute() or path.resolve() != path:
            raise ValueError('volume mountpoint path not safe')
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            info = os.fstat(fd)
            if not stat.S_ISDIR(info.st_mode) or _identity(info) != _identity(path.lstat()):
                raise ValueError('volume directory identity differs')
            self.volume_fd, self.volume_identity, self.volume_path = fd, _identity(info), path
        except BaseException:
            os.close(fd)
            raise

    def _volume_still_owned(self):
        if self.volume_fd is None or self.volume_path is None:
            raise ValueError('volume directory not retained')
        if (_identity(os.fstat(self.volume_fd)) != self.volume_identity or
                _identity(self.volume_path.lstat()) != self.volume_identity or
                self.volume_path.resolve() != self.volume_path):
            raise ValueError('volume directory changed')

    def close(self):
        if self.volume_fd is not None:
            fd, self.volume_fd = self.volume_fd, None
            os.close(fd)

    def _engine(self):
        if _docker(['info', '--format', '{{.ID}}']) != self.engine_id:
            raise ValueError('Docker Engine changed')

    def _volume(self):
        value = json.loads(_docker(['volume', 'inspect', '--format', '{{json .}}', self.names['volume']]))
        volume_valid(value, self.image, self.owner)
        return value

    def _container(self):
        if not self.container_id:
            raise ValueError('container identity not recorded')
        template = ('{"Id":{{json .Id}},"Name":{{json .Name}},"Image":{{json .Image}},'
                    '"HostConfig":{{json .HostConfig}},"Mounts":{{json .Mounts}},'
                    '"NetworkSettings":{"Ports":{{json .NetworkSettings.Ports}},"Networks":{{json .NetworkSettings.Networks}}},'
                    '"Config":{"Labels":{{json .Config.Labels}},"User":{{json .Config.User}},'
                    '"WorkingDir":{{json .Config.WorkingDir}},"Entrypoint":{{json .Config.Entrypoint}},'
                    '"Cmd":{{json .Config.Cmd}},"Volumes":{{json .Config.Volumes}},"Env":{{json .Config.Env}}},"State":{{json .State}}}')
        value = json.loads(_docker(['container', 'inspect', '--format', template, self.container_id]))
        if value.get('Id') != self.container_id:
            raise ValueError('container ID changed')
        self._volume_still_owned()
        policy.verify_owned_container(value, self._volume(), self.image, self.owner, self.startup_sha256, self._expected_env)
        return value

    def create(self):
        self._enabled(); self._certain(); self._engine()
        policy.preflight(self.image, self.engine_id, self.startup_sha256)
        existing = _docker(['volume', 'ls', '--filter', 'name=' + self.names['volume'], '--format', '{{.Name}}']).splitlines()
        if self.names['volume'] in existing:
            raise ValueError('fixture volume already exists')
        existing = _docker(['container', 'ls', '-a', '--filter', 'name=' + self.names['container'], '--format', '{{.Names}}']).splitlines()
        if self.names['container'] in existing:
            raise ValueError('fixture container already exists')
        self.journal.record('volume_create_intent', volume=self.names['volume'])
        self.uncertain = True
        actual = _docker(['volume', 'create', '--driver', 'local', '--label', 'ltds.fixture.owner=' + self.owner, self.names['volume']])
        if actual != self.names['volume']:
            raise ValueError('unexpected volume create response')
        self._open_volume(self._volume()); self.journal.record('volume_created', volume=actual)
        self.uncertain = False
        # Volume was explicitly created and attested. Docker may still race a
        # privileged external deleter; post-create attestation must fail closed.
        args = ['create', '--pull', 'never', '--name', self.names['container'], '--label', 'ltds.fixture.owner=' + self.owner,
                '--runtime', 'runc', '--network', 'none', '--cgroupns', 'private', '--restart', 'no', '--cpus', '1', '--memory', '1g',
                '--pids-limit', '128', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges', '--log-driver', 'none',
                '--env', 'NVIDIA_VISIBLE_DEVICES=void', '--mount', 'type=volume,src=' + self.names['volume'] + ',dst=/var/www/data,volume-nocopy',
                '--tmpfs', '/var/www/tmp:rw,nosuid,nodev,size=128m,mode=1777',
                '--tmpfs', '/var/www/logs:rw,nosuid,nodev,size=16m,mode=1777',
                '--tmpfs', '/tmp:rw,nosuid,nodev,size=128m,mode=1777', self.image]
        self.journal.record('container_create_intent', container=self.names['container'])
        self.uncertain = True
        identifier = _docker(args)
        if not re.fullmatch(r'[a-f0-9]{64}', identifier):
            raise ValueError('unexpected container create response')
        self.container_id = identifier
        self._engine(); self._container()
        self.journal.record('container_created', container_id=identifier)
        self.uncertain = False

    def start(self):
        self._enabled(); self._certain(); self._engine()
        if self._container()['State']['Running'] is not False:
            raise ValueError('owned container not stopped')
        self.journal.record('start_intent', container_id=self.container_id)
        self.runtime_verified = False
        self.uncertain = True
        if _docker(['start', self.container_id]) != self.container_id:
            raise ValueError('unexpected start response')
        self._engine()
        if self._container()['State']['Running'] is not True:
            raise ValueError('owned container not running')
        self.starts += 1
        self.journal.record('started', container_id=self.container_id)
        self.uncertain = False

    def stop(self):
        self._enabled(); self._engine()
        if self.stop_uncertain:
            raise RuntimeError('prior stop outcome uncertain; retain resources')
        if self._container()['State']['Running'] is not True:
            raise ValueError('owned container not running')
        self.journal.record('stop_intent', container_id=self.container_id)
        self.stop_uncertain = True
        if _docker(['stop', '--time', '5', self.container_id], timeout=12) != self.container_id:
            raise ValueError('unexpected stop response')
        self._engine()
        if self._container()['State']['Running'] is not False:
            raise ValueError('owned container did not stop')
        self.journal.record('stopped', container_id=self.container_id)
        self.stop_uncertain = False
        self.runtime_verified = False

    def remove(self):
        self._enabled(); self._certain(); self._engine()
        if not self.retention_verified or not self.task_removed:
            raise ValueError('synthetic retention and removal not proven')
        if self._container()['State']['Running'] is not False:
            raise ValueError('refuse to remove running container')
        self.journal.record('container_remove_intent', container_id=self.container_id)
        self.uncertain = True
        if _docker(['rm', self.container_id]) != self.container_id:
            raise ValueError('unexpected container removal response')
        self._engine()
        remaining = _docker(['container', 'ls', '-a', '--filter', 'name=' + self.names['container'], '--format', '{{.Names}}']).splitlines()
        if self.names['container'] in remaining:
            raise ValueError('owned container still exists')
        self.uncertain = False
        self._volume()
        users = _docker(['container', 'ls', '-a', '--filter', 'volume=' + self.names['volume'], '--format', '{{.ID}}']).splitlines()
        if users:
            raise ValueError('owned volume still attached')
        self.journal.record('volume_remove_intent', volume=self.names['volume'])
        self._volume_still_owned()
        self.uncertain = True
        if _docker(['volume', 'rm', self.names['volume']]) != self.names['volume']:
            raise ValueError('unexpected volume removal response')
        self._engine()
        remaining = _docker(['volume', 'ls', '--filter', 'name=' + self.names['volume'], '--format', '{{.Name}}']).splitlines()
        if self.names['volume'] in remaining:
            raise ValueError('owned volume still exists')
        self.journal.record('removed', container_id=self.container_id, volume=self.names['volume'])
        self.uncertain = False

    def probe(self, mode, task_id):
        self._enabled(); self._certain(); self._engine()
        if self._container()['State']['Running'] is not True:
            raise ValueError('owned container not running')
        if mode not in ('device', 'empty', 'init', 'upload', 'info', 'marker-put', 'marker-check', 'remove'):
            raise ValueError('invalid fixed probe')
        if str(uuid.UUID(task_id, version=4)) != task_id:
            raise ValueError('canonical fixture UUID required')
        sequence = ('init', 'upload', 'info', 'marker-put', 'info', 'marker-check', 'remove')
        if mode in sequence:
            if not self.empty_verified:
                raise ValueError('initial empty task list not verified')
            if self.fixture_stage >= len(sequence) or mode != sequence[self.fixture_stage]:
                raise ValueError('unexpected synthetic task sequence')
            if self.fixture_stage and task_id != self.fixture_task:
                raise ValueError('synthetic task identity changed')
            expected_start = 1 if self.fixture_stage < 4 else 2
            if self.starts != expected_start:
                raise ValueError('unexpected synthetic task restart')
        if mode != 'device' and not self.runtime_verified:
            raise RuntimeError('effective runtime is not verified')
        host = _host_namespaces() if mode == 'device' else None
        if mode in ('init', 'upload', 'marker-put', 'remove'):
            self.journal.record(mode + '_intent', container_id=self.container_id, task_id=task_id)
        self.uncertain = True
        output = _docker(['exec', '-i', self.container_id, '/usr/bin/node', '-', mode, task_id, self.owner],
                         input_text=self._probe_source, timeout=8)
        result = json.loads(output)
        if mode == 'device':
            _check_effective_probe(result, host, self.volume_identity)
            self._volume_still_owned()
            self.runtime_verified = True
        elif result != {'ok': True, 'mode': mode}:
            raise ValueError('unexpected fixture probe response')
        if mode == 'info':
            if self.starts == 1:
                self.input_observed = task_id
            elif self.starts == 2 and self.input_observed == task_id:
                # This establishes only the task UUID and API-reported count.
                self.retention_verified = True
        if mode == 'remove':
            self.task_removed = True
        if mode == 'empty':
            if self.starts != 1 or self.fixture_stage != 0:
                raise ValueError('empty task list observed at unexpected stage')
            self.empty_verified = True
        if mode in sequence:
            if self.fixture_stage == 0:
                self.fixture_task = task_id
            self.fixture_stage += 1
        if mode in ('init', 'upload', 'marker-put', 'remove'):
            self.journal.record(mode + '_acknowledged', container_id=self.container_id, task_id=task_id)
        self.uncertain = False


def execute_candidate(image, engine_id, startup_sha256, probe_sha256, journal_parent, expected_env):
    """Draft orchestration, deliberately unreachable until reviewed and enabled.

    On failure, only an exactly attested running container may be stopped;
    container, volume and journal are retained for inspection, not deleted.
    """
    if not EXECUTION_ENABLED:
        raise RuntimeError('fixture execution disabled pending review')
    owner, task_id = uuid.uuid4().hex, str(uuid.uuid4())
    journal = Journal(journal_parent, owner)
    try:
        lifecycle = OwnedLifecycle(image, engine_id, startup_sha256, owner, journal, probe_sha256, expected_env)
        journal.record('begin', owner=owner, image=image, engine_id=engine_id, task_id=task_id)
        lifecycle.create(); lifecycle.start(); lifecycle.probe('device', task_id)
        # A timeout may leave a Docker exec running. Do not retry automatically.
        lifecycle.probe('empty', task_id)
        for mode in ('init', 'upload', 'info', 'marker-put'):
            lifecycle.probe(mode, task_id)
        lifecycle.stop(); lifecycle.start(); lifecycle.probe('device', task_id)
        lifecycle.probe('info', task_id)
        for mode in ('marker-check', 'remove'):
            lifecycle.probe(mode, task_id)
        lifecycle.stop(); lifecycle.remove()
        journal.record('success', task_id=task_id)
        return {'ok': True, 'scope': 'synthetic-uncommitted-input-and-volume-marker',
                'journal': str(journal.directory)}
    except BaseException:
        # On any error retain the volume/container/journal. A verified exact
        # running container may be stopped, but failure is never auto-retried.
        try:
            if 'lifecycle' in locals() and lifecycle.container_id and lifecycle._container()['State']['Running'] is True:
                lifecycle.stop()
        except BaseException:
            print('Owned container stop uncertain; retain resources and inspect journal.', file=sys.stderr)
        raise
    finally:
        original = sys.exc_info()[1]
        try:
            if 'lifecycle' in locals():
                lifecycle.close()
        except BaseException:
            if original is None:
                raise
            print('Volume descriptor close failed; retain resources.', file=sys.stderr)
        finally:
            try:
                journal.close()
            except BaseException:
                if original is None:
                    raise
                print('Journal close failed; retain journal and resources.', file=sys.stderr)
