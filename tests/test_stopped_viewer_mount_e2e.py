"""Opt-in Linux Docker daemon test; uses disposable synthetic data only."""
import os
import json
from pathlib import Path
import subprocess
import unittest
from unittest import mock
import uuid


@unittest.skipUnless(os.environ.get('LEDGETOP_DOCKER_MOUNT_E2E') == '1', 'requires local Docker')
class MountRestoreE2E(unittest.TestCase):
    def test_backup_remount_restore(self):
        ident = 'codex-viewer-e2e-' + uuid.uuid4().hex
        volume = ident + '-backup'
        scratch_volume = ident + '-scratch'
        repo = Path(__file__).resolve().parents[1]
        owned_volumes = []
        container_names = []

        def docker(*args, code=None):
            return subprocess.run(['docker', *args], input=code, text=True, check=True,
                                  capture_output=True, timeout=60)

        def run(*mounts, code):
            name = ident + '-run-' + str(len(container_names))
            container_names.append(name)
            return docker('run', '--rm', '-i', '--name', name, '--label', 'ledgetop.codex.owner=' + ident,
                          '--network', 'none', '--memory', '256m',
                          '--cpus', '1', '-v', str(repo) + ':/work:ro', *mounts,
                          '-w', '/work', 'python:3.12-slim', 'python', '-B', '-', code=code)

        def volume_create(name, *options):
            existing = docker('volume', 'ls', '--format', '{{.Name}}').stdout.splitlines()
            if name in existing:
                raise RuntimeError('refusing existing volume')
            docker('volume', 'create', '--label', 'ledgetop.codex.owner=' + ident, *options, name)
            details = json.loads(docker('volume', 'inspect', name).stdout)[0]
            if details.get('Labels', {}).get('ledgetop.codex.owner') != ident:
                raise RuntimeError('volume identity mismatch')
            owned_volumes.append(name)

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


class OwnershipTest(unittest.TestCase):
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

    def test_timed_out_owned_container_is_removed_before_volumes(self):
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
        with mock.patch.object(uuid, 'uuid4') as new_id, mock.patch.object(subprocess, 'run', side_effect=fake):
            new_id.return_value.hex = token
            with self.assertRaises(subprocess.TimeoutExpired):
                MountRestoreE2E('test_backup_remount_restore').test_backup_remount_restore()
        container_rm = next(i for i, action in enumerate(actions) if action[:2] == ['rm', '-f'])
        volume_rm = [i for i, action in enumerate(actions) if action[:2] == ['volume', 'rm']]
        self.assertEqual(len(volume_rm), 2)
        self.assertTrue(all(container_rm < index for index in volume_rm))


if __name__ == '__main__':
    unittest.main()
