"""Opt-in Linux Docker daemon test; uses disposable synthetic data only."""
import os
from pathlib import Path
import subprocess
import unittest
import uuid


@unittest.skipUnless(os.environ.get('LEDGETOP_DOCKER_MOUNT_E2E') == '1', 'requires local Docker')
class MountRestoreE2E(unittest.TestCase):
    def test_backup_remount_restore(self):
        ident = 'codex-viewer-e2e-' + uuid.uuid4().hex
        volume = ident + '-backup'
        repo = Path(__file__).resolve().parents[1]

        def docker(*args, code=None):
            return subprocess.run(['docker', *args], input=code, text=True, check=True,
                                  capture_output=True, timeout=60)

        def run(*mounts, code):
            return docker('run', '--rm', '-i', '--network', 'none', '--memory', '256m',
                          '--cpus', '1', '-v', str(repo) + ':/work:ro', *mounts,
                          '-w', '/work', 'python:3.12-slim', 'python', '-B', '-', code=code)

        scratch = 'type=bind,src=/dev/shm,dst=/scratch'
        created = False
        try:
            docker('volume', 'create', volume)
            created = True
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
                '--mount', f'type=bind,src=/dev/shm/{ident}/source,dst=/source,readonly',
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
            try:
                run('--mount', scratch, code=f'''\
from pathlib import Path
import shutil
p=Path('/scratch/{ident}')
if p.is_dir(): shutil.rmtree(p)
''')
            finally:
                if created:
                    docker('volume', 'rm', volume)


if __name__ == '__main__':
    unittest.main()
