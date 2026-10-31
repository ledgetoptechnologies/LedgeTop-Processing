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
        image_compat = os.environ.get('LEDGETOP_IMAGE_COMPAT_E2E') == '1'
        old_image = 'sha256:7fae9385fb5ca829783fe05eef2fdb1983abf4d491b2b9f795e55b617a5a0004'
        candidate_image = 'sha256:4d4d6b7c4af869a611a7a55e911ceb9cd1cb4610b4374c942dc5ad60d21a2254'
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

        def run(*mounts, code, image='python:3.12-slim', uid=None):
            name = ident + '-run-' + str(len(container_names))
            container_names.append(name)
            # In addition to the client timeout, each task process has its
            # own deadline, including if the daemon starts it much later.
            options = ['--user', uid] if uid else []
            return docker('run', '--rm', '-i', '--name', name, '--label', 'ledgetop.codex.owner=' + ident,
                          '--network', 'none', '--memory', '256m', *options,
                          '--cpus', '1', '-v', str(repo) + ':/work:ro', *mounts,
                          '-w', '/work' if image == 'python:3.12-slim' else '/app', image, 'python', '-B', '-c',
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

        def server_script(state, *, upload=False):
            # Each application runs in a separate process in a network-none
            # container. Only loopback is reachable and no host port is published.
            return f'''\
from pathlib import Path
import json, os, signal, sqlite3, subprocess, sys, time, urllib.error, urllib.request
root=Path('/scratch/{ident}/{state}'); uid='12345678-1234-4234-8234-123456789abc'
env=os.environ.copy(); env.update(PROCESSING_API_TOKEN='a'*32,PROCESSING_PROTOTYPE_STATE_DIR=str(root),
 PROCESSING_PROTOTYPE_NODE_IDS='cluster,main',PROCESSING_VIEWER_ENABLED='1',
 PROCESSING_VIEWER_TOKEN='b'*32,LOCAL_NODE_URL='http://127.0.0.1:19991',MAIN_NODE_URL='http://127.0.0.1:19992',
 PROCESSING_BIND_HOST='127.0.0.1',PROCESSING_PORT='18080')
server=subprocess.Popen([sys.executable,'-m','processing.app'],env=env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
base='http://127.0.0.1:18080'; opener=urllib.request.build_opener(urllib.request.ProxyHandler({{}}))
try:
    end=time.monotonic()+8
    while True:
        if server.poll() is not None: raise AssertionError('coordinator process exited')
        try:
            with opener.open(base+'/healthz',timeout=.5) as response:
                if response.status==200: break
        except (OSError,urllib.error.URLError): pass
        if time.monotonic()>=end: raise AssertionError('coordinator startup timeout')
        time.sleep(.05)
    if {upload!r}:
        body=(b'--boundary123\\r\\nContent-Disposition: form-data; name="images"; filename="photo.jpg"\\r\\n'
              b'Content-Type: image/jpeg\\r\\n\\r\\nsynthetic-image\\r\\n--boundary123--\\r\\n')
        request=urllib.request.Request(base+'/task/new/upload/'+uid+'?token='+('b'*32),data=body,method='POST',
            headers={{'Content-Type':'multipart/form-data; boundary=boundary123'}})
        with opener.open(request,timeout=3) as response: assert json.load(response)=={{'success':True}}
    with opener.open(base+'/task/'+uid+'/info?token='+('b'*32),timeout=3) as response: info=json.load(response)
    assert info['name']=='synthetic task' and info['imagesCount']==1 and info['status']['code']==({10 if upload else 40})
finally:
    server.send_signal(signal.SIGTERM)
    try: exit_code=server.wait(timeout=8)
    except subprocess.TimeoutExpired:
        server.kill();server.wait(timeout=3);raise AssertionError('coordinator failed graceful shutdown')
    if exit_code!=0: raise AssertionError('coordinator exited unsuccessfully')
db=sqlite3.connect('file:'+str(root/'jobs.sqlite3')+'?mode=ro',uri=True)
rows=db.execute('SELECT t.directory,f.path FROM viewer_tasks t JOIN viewer_files f ON t.uuid=f.uuid WHERE t.uuid=?',(uid,)).fetchall()
db.close(); assert len(rows)==1
directory,filename=rows[0]; assert Path(directory).name==directory and Path(filename).name==filename
assert (root/'viewer-tasks'/directory/filename).read_bytes()==b'synthetic-image'
'''

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
            if image_compat:
                for image, revision in ((old_image, '693cb7eca32295da959a4f42399f317577ef8a8a'),
                                        (candidate_image, 'd90e81a01efec5ccf004eb3ade48e7480a273398')):
                    observed = json.loads(docker('image', 'inspect', image).stdout)[0]
                    if observed.get('Config', {}).get('Labels', {}).get('org.opencontainers.image.revision') != revision:
                        raise RuntimeError('exact image revision required')
                run('--mount', scratch, '--mount', f'type=volume,src={volume},dst=/backups', code=f'''\
from pathlib import Path
import os
p=Path('/scratch/{ident}'); p.mkdir(mode=0o700); (p/'source').mkdir(mode=0o700)
(p/'leases').mkdir(mode=0o700); (p/'restore-parent').mkdir(mode=0o700)
a='12345678-1234-1234-1234-123456789abc'; (p/'leases'/(a+'.lease')).touch(mode=0o600)
for path in (p,p/'source',p/'leases',p/'restore-parent',p/'leases'/(a+'.lease'),Path('/backups')):
    os.chown(path,10001,10001)
Path('/backups').chmod(0o700)
''')
                run('--mount', scratch, image=old_image, uid='10001:10001', code=f'''\
from pathlib import Path
from processing.prototype import Queue
from processing.viewer_tasks import TaskStore
p=Path('/scratch/{ident}'); root=p/'source'
q=Queue(root, node_ids=('cluster','main'), viewer_enabled=True)
with q._db() as db: db.execute("UPDATE coordinator_identity SET authority_id='12345678-1234-1234-1234-123456789abc' WHERE singleton=1")
t=TaskStore(q, root/'viewer-tasks'); uid='12345678-1234-4234-8234-123456789abc'
t.initialize(uid,name='synthetic task',options=[],outputs=[],environment='production')
q.close()
''')
                run('--mount', scratch, image=old_image, uid='10001:10001', code=server_script('source', upload=True))
                run('--mount', scratch, image=old_image, uid='10001:10001', code=f'''\
from pathlib import Path
import sqlite3
root=Path('/scratch/{ident}/source');db=sqlite3.connect(root/'jobs.sqlite3')
db.execute("UPDATE viewer_tasks SET stage='succeeded',phase='complete' WHERE uuid=?",('12345678-1234-4234-8234-123456789abc',))
db.commit();db.close()
''')
            else:
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
                '--mount', f'type=volume,src={volume},dst=/backups', uid='10001:10001' if image_compat else None, code=f'''\
from pathlib import Path
import json, os
from ops.stopped_viewer_backup import backup
p=Path('/scratch/{ident}'); Path('/backups').chmod(0o700)
r=backup(Path('/source'),Path('/backups/snapshot'),p/'leases','12345678-1234-1234-1234-123456789abc',bytes(range(32)),stopped_writers_confirmed=True,independent_destination_confirmed=True)
fd=os.open(p/'inventory.json',os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
with os.fdopen(fd,'w') as f:
    json.dump({{'inventory':r['snapshot']}},f); f.flush(); os.fsync(f.fileno())
''')
            if image_compat:
                run('--mount', scratch, image=candidate_image, uid='10001:10001', code=f'''\
from pathlib import Path
import shutil
p=Path('/scratch/{ident}'); source=p/'source'; candidate=p/'candidate'
shutil.copytree(source,candidate)
''')
                run('--mount', scratch, image=candidate_image, uid='10001:10001', code=server_script('candidate'))
                run('--mount', scratch, uid='10001:10001', code=f'''\
from pathlib import Path
from ops.state_census import census
p=Path('/scratch/{ident}'); key=bytes(range(32))
before=census(p/'source',key); after=census(p/'candidate',key)
for field in ('objects','rowCounts','tableDigest','viewerTaskProjection','authorityId','active'):
    assert before['database'][field]==after['database'][field], field
assert before['database']['rowCounts']['viewer_tasks']==1
assert before['database']['rowCounts']['viewer_files']==1
''')
            run('--mount', scratch,
                '--mount', f'type=volume,src={volume},dst=/backup,readonly,volume-subpath=snapshot',
                uid='10001:10001' if image_compat else None, code=f'''\
from pathlib import Path
import sqlite3
from ops.stopped_viewer_restore import restore, read_inventory
p=Path('/scratch/{ident}'); destination=p/'restore-parent'/'restored'
restore(Path('/backup'),destination,p/'leases','12345678-1234-1234-1234-123456789abc',bytes(range(32)),read_inventory(p/'inventory.json'),stopped_writers_confirmed=True,independent_destination_confirmed=True,original_inventory_trusted=True)
assert {image_compat!r} or (destination/'viewer-tasks/photo').read_bytes()==bytes(range(256))*7
assert (destination/'jobs.sqlite3').read_bytes()==(p/'source/jobs.sqlite3').read_bytes()
d=sqlite3.connect(destination/'jobs.sqlite3')
if {image_compat!r}: assert d.execute('SELECT name,stage FROM viewer_tasks').fetchall()==[('synthetic task','succeeded')]
else: assert d.execute('SELECT * FROM viewer_tasks').fetchall()==[('synthetic','succeeded',0)]
d.close()
''')
            if image_compat:
                run('--mount', scratch, image=old_image, uid='10001:10001', code=server_script('restore-parent/restored'))
        finally:
            primary_error = sys.exc_info()[1]
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
                    if primary_error is None:
                        raise
                    print('Warning: Docker cleanup could not be verified; resources may remain.', file=sys.stderr)


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

    def test_cleanup_failure_does_not_replace_primary_error(self):
        token = '1234567890abcdef1234567890abcdef'
        ident = 'codex-viewer-e2e-' + token
        calls = {'list': 0, 'inspect': 0}
        def fake(argv, **kwargs):
            args = argv[1:]
            if args[:2] == ['volume', 'ls']:
                calls['list'] += 1
                output = '' if calls['list'] == 1 else ident + '-scratch\n'
            elif args[:2] == ['volume', 'inspect']:
                calls['inspect'] += 1
                if calls['inspect'] > 1:
                    raise OSError('daemon unavailable')
                output = json.dumps([{'Labels': {'ledgetop.codex.owner': ident}}])
            else:
                output = ''
            return subprocess.CompletedProcess(argv, 0, stdout=output, stderr='')
        stderr = io.StringIO()
        with mock.patch.object(uuid, 'uuid4') as new_id, mock.patch.object(subprocess, 'run', side_effect=fake), \
             mock.patch(__name__ + '.save_recovery_metadata') as record, contextlib.redirect_stderr(stderr):
            new_id.return_value.hex = token
            with self.assertRaisesRegex(RuntimeError, 'refusing existing volume'):
                MountRestoreE2E('test_backup_remount_restore').test_backup_remount_restore()
        self.assertIn('cleanup could not be verified', stderr.getvalue())
        self.assertEqual(record.call_args.args[3], 'cleanup outcome uncertain')

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
