"""Opt-in Linux Docker daemon test; uses disposable synthetic data only."""
import os
import json
import contextlib
import io
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
import uuid


def save_recovery_metadata(ident, volumes, containers, reason):
    """Retain names/owner and best-effort immutable IDs when outcome is unclear."""
    observations = {}
    for name in containers:
        try:
            result = subprocess.run(['docker', 'inspect', '--format',
                                     '{{.Id}} {{index .Config.Labels "ledgetop.codex.owner"}} {{.State.Status}}', name],
                                    text=True, capture_output=True, timeout=2)
            observations[name] = result.stdout.strip() if result.returncode == 0 else 'not observed; may appear later'
        except (OSError, subprocess.TimeoutExpired):
            observations[name] = 'daemon unavailable; outcome unknown'
    for name in volumes:
        try:
            result = subprocess.run(['docker', 'volume', 'inspect', '--format',
                                     '{{.Name}} {{index .Labels "ledgetop.codex.owner"}}', name],
                                    text=True, capture_output=True, timeout=2)
            observations[name] = result.stdout.strip() if result.returncode == 0 else 'not observed; may appear later'
        except (OSError, subprocess.TimeoutExpired):
            observations[name] = 'daemon unavailable; outcome unknown'
    document = {'owner_label': ident, 'candidate_volumes': volumes, 'candidate_containers': containers,
                'observations': observations, 'reason': reason,
                'instruction': 'Retain; inspect exact names, labels and immutable IDs after Docker settles.'}
    fd, path = tempfile.mkstemp(prefix='codex-viewer-docker-recovery-', suffix='.json')
    with os.fdopen(fd, 'w', encoding='utf-8') as stream:
        json.dump(document, stream); stream.flush(); os.fsync(stream.fileno())
    print('Docker cleanup uncertain; recovery metadata retained at ' + path, file=sys.stderr)
    return path


def try_save_recovery_metadata(ident, volumes, containers, reason):
    try:
        save_recovery_metadata(ident, volumes, containers, reason)
    except BaseException:
        # Preserve the original failure; never emit exception details that
        # could contain host paths or daemon responses.
        print('Warning: Docker recovery metadata could not be saved; resource outcome remains unknown.', file=sys.stderr)


@unittest.skipUnless(os.environ.get('LEDGETOP_DOCKER_MOUNT_E2E') == '1', 'requires local Docker')
class MountRestoreE2E(unittest.TestCase):
    def test_backup_remount_restore(self):
        ident = 'codex-viewer-e2e-' + uuid.uuid4().hex
        volume = ident + '-backup'
        scratch_volume = ident + '-scratch'
        repo = Path(__file__).resolve().parents[1]
        owned_volumes = []
        container_names = []
        uncertain = False

        def docker(*args, code=None):
            nonlocal uncertain
            try:
                return subprocess.run(['docker', *args], input=code, text=True, check=True,
                                      capture_output=True, timeout=60)
            except (OSError, subprocess.TimeoutExpired, subprocess.CalledProcessError):
                # Any failed mutating client may have reached the daemon.
                if args[0] in ('run', 'rm') or args[:2] in (('volume', 'create'), ('volume', 'rm')):
                    uncertain = True
                raise

        def run(*mounts, code):
            name = ident + '-run-' + str(len(container_names))
            container_names.append(name)
            # In addition to the client timeout, each task process has its
            # own deadline, including if the daemon starts it much later.
            return docker('run', '--rm', '-i', '--name', name, '--label', 'ledgetop.codex.owner=' + ident,
                          '--network', 'none', '--memory', '256m',
                          '--cpus', '1', '-v', str(repo) + ':/work:ro', *mounts,
                          '-w', '/work', 'python:3.12-slim', 'python', '-B', '-c',
                          'import signal,sys; signal.alarm(120); exec(sys.stdin.read())', code=code)

        def volume_create(name, *options):
            nonlocal uncertain
            existing = docker('volume', 'ls', '--format', '{{.Name}}').stdout.splitlines()
            if name in existing:
                raise RuntimeError('refusing existing volume')
            docker('volume', 'create', '--label', 'ledgetop.codex.owner=' + ident, *options, name)
            uncertain = True
            details = json.loads(docker('volume', 'inspect', name).stdout)[0]
            if details.get('Labels', {}).get('ledgetop.codex.owner') != ident:
                raise RuntimeError('volume identity mismatch')
            owned_volumes.append(name)
            uncertain = False

        scratch = f'type=volume,src={scratch_volume},dst=/scratch'
        try:
            volume_create(volume)
            volume_create(scratch_volume, '--opt', 'type=tmpfs', '--opt', 'device=tmpfs')
            # Keep the task-owned tmpfs mounted between short test containers.
            # The bounded keeper is always reconciled by the finally block.
            keeper = ident + '-keeper'
            container_names.append(keeper)
            docker('run', '-d', '--name', keeper, '--label', 'ledgetop.codex.owner=' + ident,
                   '--network', 'none', '--memory', '128m', '--cpus', '0.25',
                   '--mount', scratch, 'python:3.12-slim', 'python', '-c',
                   'import time; time.sleep(120)')
            run('--mount', scratch, code=f'''\
from pathlib import Path
import sqlite3
p=Path('/scratch/{ident}'); p.mkdir(mode=0o700); s=p/'source'; s.mkdir(mode=0o700)
(p/'leases').mkdir(mode=0o700); (p/'restore-parent').mkdir(mode=0o700)
a='12345678-1234-1234-1234-123456789abc'; (p/'leases'/(a+'.lease')).touch()
(s/'coordinator.lock').touch(); (s/'viewer-tasks').mkdir(); (s/'viewer-tasks'/'photo').write_bytes(bytes(range(256))*7)
d=sqlite3.connect(s/'jobs.sqlite3'); d.executescript("CREATE TABLE jobs(id INTEGER,status TEXT); CREATE TABLE viewer_tasks(uuid TEXT,stage TEXT,removed INTEGER); CREATE TABLE coordinator_identity(singleton INTEGER,authority_id TEXT); INSERT INTO jobs VALUES(1,'succeeded'); INSERT INTO viewer_tasks VALUES('synthetic','succeeded',0);"); d.execute('INSERT INTO coordinator_identity VALUES(1,?)',(a,)); d.commit(); d.close()
''')
            run('--mount', scratch,
                '--mount', f'type=volume,src={scratch_volume},dst=/source,readonly,volume-subpath={ident}/source',
                '--mount', f'type=volume,src={volume},dst=/backups', code=f'''\
from pathlib import Path
import json, os
from ops.stopped_viewer_backup import backup
p=Path('/scratch/{ident}'); Path('/backups').chmod(0o700)
r=backup(Path('/source'),Path('/backups/snapshot'),p/'leases','12345678-1234-1234-1234-123456789abc',bytes(range(32)),stopped_writers_confirmed=True,independent_destination_confirmed=True)
fd=os.open(p/'inventory.json',os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
with os.fdopen(fd,'w') as f:
    json.dump({{'inventory':r['snapshot']}},f); f.flush(); os.fsync(f.fileno())
''')
            run('--mount', scratch,
                '--mount', f'type=volume,src={volume},dst=/backup,readonly,volume-subpath=snapshot',
                code=f'''\
from pathlib import Path
import sqlite3
from ops.stopped_viewer_restore import restore, read_inventory
p=Path('/scratch/{ident}'); destination=p/'restore-parent'/'restored'
restore(Path('/backup'),destination,p/'leases','12345678-1234-1234-1234-123456789abc',bytes(range(32)),read_inventory(p/'inventory.json'),stopped_writers_confirmed=True,independent_destination_confirmed=True,original_inventory_trusted=True)
assert (destination/'viewer-tasks/photo').read_bytes()==bytes(range(256))*7
assert (destination/'jobs.sqlite3').read_bytes()==(p/'source/jobs.sqlite3').read_bytes()
d=sqlite3.connect(destination/'jobs.sqlite3'); assert d.execute('SELECT * FROM viewer_tasks').fetchall()==[('synthetic','succeeded',0)]; d.close()
''')
        finally:
            # A timed-out Docker client can leave its container behind. Only
            # reconcile exact names whose labels prove ownership, and retain
            # every volume if any container's identity or removal is unclear.
            if uncertain:
                try_save_recovery_metadata(ident, [volume, scratch_volume], container_names, 'mutating request outcome uncertain')
            else:
                try:
                    for name in container_names:
                        names = docker('ps', '-a', '--format', '{{.Names}}').stdout.splitlines()
                        if name in names:
                            details = json.loads(docker('inspect', name).stdout)[0]
                            if details.get('Config', {}).get('Labels', {}).get('ledgetop.codex.owner') != ident:
                                raise RuntimeError('container identity mismatch; retaining volumes')
                            docker('rm', '-f', details['Id'])
                        names = docker('ps', '-a', '--format', '{{.Names}}').stdout.splitlines()
                        if name in names:
                            raise RuntimeError('container removal unverified; retaining volumes')
                    for name in reversed(owned_volumes):
                        details = json.loads(docker('volume', 'inspect', name).stdout)[0]
                        if details.get('Labels', {}).get('ledgetop.codex.owner') != ident:
                            raise RuntimeError('volume identity changed; retaining volume')
                        docker('volume', 'rm', name)
                        if name in docker('volume', 'ls', '--format', '{{.Name}}').stdout.splitlines():
                            raise RuntimeError('volume removal unverified')
                except BaseException:
                    try_save_recovery_metadata(ident, [volume, scratch_volume], container_names, 'cleanup outcome uncertain')
                    raise


class OwnershipTest(unittest.TestCase):
    def test_recovery_metadata_survives_daemon_unavailable(self):
        real_mkstemp = tempfile.mkstemp
        with tempfile.TemporaryDirectory() as directory, \
             mock.patch.object(subprocess, 'run', side_effect=OSError('offline')), \
             mock.patch.object(tempfile, 'mkstemp', side_effect=lambda **kw: real_mkstemp(dir=directory, **kw)):
            path = save_recovery_metadata('owner', ['vol'], ['container'], 'timeout')
            data = json.loads(Path(path).read_text())
            self.assertEqual(data['owner_label'], 'owner')
            self.assertEqual(data['observations']['container'], 'daemon unavailable; outcome unknown')
            self.assertEqual(data['observations']['vol'], 'daemon unavailable; outcome unknown')

    def test_existing_volume_is_never_adopted_or_removed(self):
        token = '1234567890abcdef1234567890abcdef'
        existing = 'codex-viewer-e2e-' + token + '-backup'
        completed = subprocess.CompletedProcess([], 0, stdout=existing + '\n', stderr='')
        with mock.patch.object(uuid, 'uuid4') as new_id, \
             mock.patch.object(subprocess, 'run', return_value=completed) as command:
            new_id.return_value.hex = token
            with self.assertRaisesRegex(RuntimeError, 'existing volume'):
                MountRestoreE2E('test_backup_remount_restore').test_backup_remount_restore()
        self.assertEqual(command.call_count, 1)
        self.assertEqual(command.call_args.args[0][1:3], ['volume', 'ls'])

    def test_timed_out_container_retains_resources_even_if_not_yet_visible(self):
        token = 'fedcba0987654321fedcba0987654321'
        ident = 'codex-viewer-e2e-' + token
        running = True
        actions = []
        def fake(argv, **kwargs):
            nonlocal running
            args = argv[1:]
            actions.append(args)
            output = ''
            if args[:2] == ['volume', 'inspect']:
                output = json.dumps([{'Labels': {'ledgetop.codex.owner': ident}}])
            elif args[:2] == ['ps', '-a']:
                output = ident + '-keeper\n' if running else ''
            elif args[0] == 'inspect':
                output = json.dumps([{'Id': 'owned-id', 'Config': {'Labels': {'ledgetop.codex.owner': ident}}}])
            elif args[:2] == ['rm', '-f']:
                running = False
            elif args[:2] == ['run', '-d']:
                raise subprocess.TimeoutExpired(argv, 60)
            return subprocess.CompletedProcess(argv, 0, stdout=output, stderr='')
        with mock.patch.object(uuid, 'uuid4') as new_id, mock.patch.object(subprocess, 'run', side_effect=fake), \
             mock.patch(__name__ + '.save_recovery_metadata') as record:
            new_id.return_value.hex = token
            with self.assertRaises(subprocess.TimeoutExpired):
                MountRestoreE2E('test_backup_remount_restore').test_backup_remount_restore()
        self.assertFalse(any(action[:2] in (['rm', '-f'], ['volume', 'rm']) for action in actions))
        self.assertEqual(record.call_args.args[2], [ident + '-keeper'])

    def test_volume_create_lost_response_is_retained(self):
        token = 'aabbccddeeff00112233445566778899'
        actions = []
        def fake(argv, **kwargs):
            args = argv[1:]; actions.append(args)
            if args[:2] == ['volume', 'create']:
                raise subprocess.TimeoutExpired(argv, 60)
            return subprocess.CompletedProcess(argv, 0, stdout='', stderr='')
        with mock.patch.object(uuid, 'uuid4') as new_id, mock.patch.object(subprocess, 'run', side_effect=fake), \
             mock.patch(__name__ + '.save_recovery_metadata') as record:
            new_id.return_value.hex = token
            with self.assertRaises(subprocess.TimeoutExpired):
                MountRestoreE2E('test_backup_remount_restore').test_backup_remount_restore()
        self.assertFalse(any(action[:2] == ['volume', 'rm'] for action in actions))
        self.assertIn('codex-viewer-e2e-' + token + '-backup', record.call_args.args[1])

    def test_metadata_write_failure_preserves_original_timeout(self):
        def fake(argv, **kwargs):
            if argv[1:3] == ['volume', 'create']:
                raise subprocess.TimeoutExpired(argv, 60)
            return subprocess.CompletedProcess(argv, 0, stdout='', stderr='')
        stderr = io.StringIO()
        with mock.patch.object(subprocess, 'run', side_effect=fake), \
             mock.patch(__name__ + '.save_recovery_metadata', side_effect=OSError('private path')), \
             contextlib.redirect_stderr(stderr):
            with self.assertRaises(subprocess.TimeoutExpired):
                MountRestoreE2E('test_backup_remount_restore').test_backup_remount_restore()
        self.assertIn('resource outcome remains unknown', stderr.getvalue())
        self.assertNotIn('private path', stderr.getvalue())

    def test_test_container_has_independent_deadline(self):
        token = '1234567890abcdef1234567890abcdef'
        ident = 'codex-viewer-e2e-' + token
        observed = []
        def fake(argv, **kwargs):
            args = argv[1:]
            if args[:2] == ['volume', 'inspect']:
                return subprocess.CompletedProcess(argv, 0, stdout=json.dumps([{'Labels': {'ledgetop.codex.owner': ident}}]), stderr='')
            if args[:2] == ['run', '--rm']:
                observed.append(argv)
                raise subprocess.TimeoutExpired(argv, 60)
            return subprocess.CompletedProcess(argv, 0, stdout='', stderr='')
        with mock.patch.object(uuid, 'uuid4') as new_id, mock.patch.object(subprocess, 'run', side_effect=fake), \
             mock.patch(__name__ + '.save_recovery_metadata'):
            new_id.return_value.hex = token
            with self.assertRaises(subprocess.TimeoutExpired):
                MountRestoreE2E('test_backup_remount_restore').test_backup_remount_restore()
        self.assertIn('import signal,sys; signal.alarm(120); exec(sys.stdin.read())', observed[0])


if __name__ == '__main__':
    unittest.main()
