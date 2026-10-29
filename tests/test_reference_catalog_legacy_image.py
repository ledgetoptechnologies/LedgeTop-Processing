"""Opt-in local Docker oracle: real catalog admission versus installed6bb.

Only a uniquely labelled disposable volume is writable. No installed state,
private configuration, network, Docker socket or host service enters a child.
An unknown outcome retains its volume and receipts for inspection, not replay.
"""
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import unittest
import uuid

from tests.test_coordinator_application_migration import IMAGES, CANDIDATE_COMMIT, SOURCE_PINS

ROOT = Path(__file__).resolve().parents[1]
SEED_IMAGE = 'sha256:83028785a336b420f42fd4c6a2219f289437e6406255b555aa7b5c70dcdf24fb'
OLD_IMAGE = IMAGES[CANDIDATE_COMMIT]
CENSUS = '''
import hashlib,json,sqlite3
from contextlib import closing
from pathlib import Path
state=Path('/qualstate');database=state/'jobs.sqlite3'
def census():
 with closing(sqlite3.connect(database.as_uri()+'?mode=ro',uri=True)) as db:
  schema=db.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name').fetchall()
  rows={name:db.execute('SELECT * FROM "'+name+'" ORDER BY rowid').fetchall()
        for kind,name,_,_ in schema if kind=='table'}
  raw=json.dumps({'schema':schema,'rows':rows},sort_keys=True,separators=(',',':')).encode()
  return {'logicalSha256':hashlib.sha256(raw).hexdigest(),
    'rawSha256':hashlib.sha256(database.read_bytes()).hexdigest(),
    'jobs':len(rows['jobs']),'evidence':len(rows['reference_option_evidence']),
    'authority':rows['coordinator_identity'][0][1]}
'''


@unittest.skipUnless(os.environ.get('PROCESSING_RUN_CATALOG_IMAGE_ORACLE') == '1',
                     'explicit local Docker image qualification required')
class CatalogLegacyImageTests(unittest.TestCase):
    def test_actual_installed_image_cannot_write_catalog_admitted_state(self):
        parent = Path(os.environ['PROCESSING_CATALOG_IMAGE_ORACLE_ARTIFACT']).resolve()
        self.assertTrue(parent.is_dir())
        operation = 'ltds-catalog-abi-' + uuid.uuid4().hex
        artifact = parent / operation
        artifact.mkdir()
        docker = (['C:/Program Files/Docker/Docker/resources/bin/docker.exe', '--host',
                   'npipe:////./pipe/dockerDesktopLinuxEngine'] if os.name == 'nt' else
                  ['/usr/bin/docker', '--host', 'unix:///run/docker.sock'])
        env = {k: v for k, v in os.environ.items() if not k.startswith('DOCKER_')}
        records = []

        def command(args, timeout=60):
            completed = subprocess.run(docker + args, env=env, capture_output=True,
                                       text=True, timeout=timeout)
            records.append(dict(command=docker + args, returncode=completed.returncode,
                                stdout=completed.stdout, stderr=completed.stderr))
            (artifact / 'commands.json').write_text(json.dumps(records, indent=2) + '\n')
            self.assertEqual(completed.returncode, 0, completed.stderr[-2000:])
            return completed.stdout

        engine = command(['info', '--format', '{{.ID}}']).strip()
        labels = {'purpose': 'processing-catalog-abi-oracle', 'operation': operation}
        (artifact / 'intent.json').write_text(json.dumps(dict(operation=operation, engineId=engine,
            oldImage=OLD_IMAGE, seedImage=SEED_IMAGE, observedAt=datetime.now(timezone.utc).isoformat()), indent=2))
        command(['volume', 'create', '--label', 'purpose='+labels['purpose'],
                 '--label', 'operation='+operation, operation])

        def volume():
            value = json.loads(command(['volume', 'inspect', operation]))[0]
            self.assertEqual((value['Name'], value['Driver'], value['Labels']),
                             (operation, 'local', labels))
            self.assertFalse(value.get('Options'))
            self.assertEqual(command(['info', '--format', '{{.ID}}']).strip(), engine)

        def run(phase, image, code, *, source=False, uid='10001:10001', chown=False):
            volume()
            args = ['run', '--rm', '--pull', 'never', '--name', operation+'-'+phase,
                    '--label', 'operation='+operation, '--network', 'none', '--read-only',
                    '--cpus', '2', '--memory', '2g', '--pids-limit', '128', '--cap-drop', 'ALL',
                    '--security-opt', 'no-new-privileges', '--user', uid,
                    '--tmpfs', '/tmp:rw,nosuid,nodev,size=256m,mode=1777',
                    '--mount', 'type=volume,source='+operation+',target=/qualstate']
            if chown:
                args += ['--cap-add', 'CHOWN']
            if source:
                args += ['--mount', 'type=bind,source='+ROOT.as_posix()+',target=/work,readonly',
                         '--workdir', '/work', '--env', 'PYTHONPATH=/work']
            args += ['--entrypoint', 'python', image, '-B', '-W', 'error::ResourceWarning', '-c', code]
            return json.loads(command(args, timeout=120))

        run('init', OLD_IMAGE, '''import os,json
from pathlib import Path
p=Path('/qualstate');assert not list(p.iterdir()) and p.stat().st_uid==0
fd=os.open(p,os.O_RDONLY|os.O_DIRECTORY)
try:
 os.chmod(p,0o700);os.chown(p,10001,10001);os.fsync(fd)
finally:os.close(fd)
print(json.dumps({'ownedEmptyVolume':True}))''', uid='0:0', chown=True)
        seeded = run('seed', SEED_IMAGE, '''import json
from tests.seed_reference_catalog_state import seed
print(json.dumps(seed('/qualstate')))''', source=True)
        self.assertEqual((seeded['jobs'], seeded['terminalJobs'], seeded['queuedJobs'],
                          seeded['optionEvidenceRows']), (13, 12, 1, 1))

        old = run('old', OLD_IMAGE, CENSUS + '''
files={'processing/'+p.relative_to('/app/processing').as_posix():hashlib.sha256(p.read_bytes()).hexdigest()
       for p in sorted(Path('/app/processing').rglob('*.py'))}
pin=hashlib.sha256(json.dumps(files,sort_keys=True,separators=(',',':')).encode()).hexdigest()
assert (len(files),pin)==EXPECTED_PIN
from processing.prototype import Queue
before=census();coldBlocked=False
try:q=Queue(state,node_ids=('cluster','main'),treedar_node_ids=('main',),reference_enabled=True)
except sqlite3.OperationalError as error:
 assert 'reference_catalog_writer_v1' in str(error);coldBlocked=True
else:q.close()
with closing(sqlite3.connect(database)) as db:
 target=db.execute("SELECT id FROM jobs WHERE status='succeeded' LIMIT 1").fetchone()[0]
 attempts=[("UPDATE jobs SET status='queued' WHERE id=?",(target,)),
           ('DELETE FROM jobs WHERE id=?',(target,)),
           ("INSERT INTO jobs(id,request_id,environment,profile,status) VALUES(?,?,'staging','treedar-synthetic-v1','queued')",('denied-catalog-job','denied-catalog-request'))]
 blocked=0
 for sql,args in attempts:
  try:db.execute(sql,args)
  except sqlite3.OperationalError as error:
   assert 'reference_catalog_writer_v1' in str(error);blocked+=1
  finally:db.rollback()
 assert blocked==3
after=census();assert after==before
print(json.dumps({'coldOpenBlocked':coldBlocked,'oldWriterOperationsBlocked':blocked,
 'stateUnchanged':True,'census':after,'oldSourceFiles':len(files),'oldSourceSha256':pin}))
'''.replace('EXPECTED_PIN', repr(SOURCE_PINS[CANDIDATE_COMMIT])))
        self.assertTrue(old['coldOpenBlocked'])

        current = run('current', SEED_IMAGE, CENSUS + '''
from processing.prototype import Queue
before=census();modeOffBlocked=False
try:q=Queue(state,node_ids=('cluster','main'),reference_enabled=True)
except RuntimeError as error:
 assert 'catalog admission ABI' in str(error);modeOffBlocked=True
else:q.close()
assert modeOffBlocked and census()==before
q=Queue(state,node_ids=('cluster','main'),treedar_node_ids=('main',),reference_enabled=True,catalog_admission=True)
try:assert q.authority_id==before['authority']
finally:q.close()
assert census()==before
print(json.dumps({'modeOffBlocked':True,'explicitModeColdReopen':True,'stateUnchanged':True,'census':census()}))
''', source=True)
        self.assertEqual(old['census'], current['census'])
        volume()
        self.assertFalse(command(['ps', '--all', '--filter', 'volume='+operation, '--format', '{{.ID}}']).strip())
        result = dict(status='actual-installed-image-downgrade-fenced', engineId=engine,
                      seeded=seeded, old=old, current=current, volume=operation,
                      liveStateUsed=False, cleanup='owned volume removal next')
        (artifact / 'verified-before-cleanup.json').write_text(json.dumps(result, indent=2)+'\n')
        command(['volume', 'rm', operation])
        result['cleanup'] = 'owned disposable volume removed'
        (artifact / 'verified.json').write_text(json.dumps(result, indent=2)+'\n')

