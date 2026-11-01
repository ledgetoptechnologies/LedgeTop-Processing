"""Owned synthetic container runner. DO NOT EXECUTE BEFORE INDEPENDENT REVIEW.

No deletion: retains snapshot, test files, and stopped container for inspection.
The Windows parent owns the overall deadline and reconciles ambiguous outcomes.
"""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tarfile
import time

source = Path(__file__).resolve().parents[1]
run_id = os.environ['LT_SYNTHETIC_RUN_ID']
root = Path(os.environ['LT_SYNTHETIC_ROOT']).resolve()
if len(run_id) != 32 or any(c not in '0123456789abcdef' for c in run_id) or not root.is_dir():
    raise ValueError('invalid retained run identity')
name = 'synthetic-coordinator-restart-' + run_id
label = 'ledgetop.synthetic.rehearsal'
owned = None

def cmd(*args, timeout=8, stdout=subprocess.PIPE):
    return subprocess.run(args, cwd=source, check=True, timeout=timeout,
                          stdout=stdout, stderr=subprocess.PIPE).stdout

def docker(*args, timeout=8):
    return cmd('docker', *args, timeout=timeout).decode().strip()

def inspect(reference):
    item=json.loads(docker('inspect',reference))[0]
    if (item['Name'].lstrip('/')!=name or item['Config']['Labels'].get(label)!=run_id
            or len(item['Id'])!=64 or (owned is not None and item['Id']!=owned)):
        raise RuntimeError('container ownership mismatch')
    return item

try:
    head=cmd('git','rev-parse','HEAD').decode().strip()
    print('PROCESSING_COMMIT',head,'RETAINED_ROOT',root,'RUN_ID',run_id,flush=True)
    snapshot=root/'processing';snapshot.mkdir()
    archive=root/'processing.tar'
    with archive.open('wb') as output:
        cmd('git','archive','--format=tar',head,timeout=30,stdout=output)
    with tarfile.open(archive) as tar:
        tar.extractall(snapshot,filter='data')
    for name_part in ('worker_process_outage_fixture.py','worker_process_outage_engine.py',
                      'worker_process_coordinator.py','coordinator_process_restart_supervisor.py',
                      'coordinator_process_restart_watchdog.py'):
        contents=(source/'tests'/name_part).read_bytes()
        target=snapshot/'tests'/name_part;target.write_bytes(contents)
        digest=hashlib.sha256(contents).hexdigest()
        if hashlib.sha256(target.read_bytes()).hexdigest()!=digest:raise RuntimeError('fixture snapshot mismatch')
        print('FIXTURE_SHA256',name_part,digest,flush=True)
    retained=root/'retained';retained.mkdir()
    image=docker('image','inspect','--format','{{.Id}}','python:3.12-slim')
    if not image.startswith('sha256:') or len(image)!=71:raise RuntimeError('invalid local image id')
    print('IMAGE_ID',image,'PLANNED_CONTAINER',name,flush=True)
    try:
        owned=docker('create','--name',name,'--label',f'{label}={run_id}',
            '--network','none','--init','--pids-limit','48','--memory','512m','--cpus','1',
            '--cap-drop','ALL','--security-opt','no-new-privileges',
            '--mount',f'type=bind,src={snapshot},dst=/processing,readonly',
            '--mount',f'type=bind,src={retained},dst=/retained',
            '--workdir','/processing','--env','SYNTHETIC_RETAINED_DIR=/retained',
            image,'python','-u','-m','tests.coordinator_process_restart_watchdog')
    except (subprocess.TimeoutExpired,subprocess.CalledProcessError):
        try:
            item=inspect(name);owned=item['Id'];print('RECONCILED_CREATE',name,owned,flush=True)
        except Exception as error:
            print('UNCERTAIN_CREATE',name,type(error).__name__,flush=True)
        raise
    if len(owned)!=64:raise RuntimeError('invalid container id')
    inspect(owned);print('OWNED_CONTAINER',name,owned,flush=True)
    docker('start',owned)
    deadline=time.monotonic()+175
    while time.monotonic()<deadline:
        status=inspect(owned)['State']
        if not status['Running']:break
        time.sleep(.3)
    else:raise RuntimeError('owned container deadline exceeded')
    result=subprocess.run(['docker','logs',owned],cwd=source,stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT,check=True,timeout=8)
    logs=result.stdout.decode('utf-8','replace')
    print('CONTAINER_OUTPUT_BEGIN',flush=True);print(logs,flush=True);print('CONTAINER_OUTPUT_END',flush=True)
    if status['ExitCode']!=0 or 'INNER_PROCESS_GROUP_CHECK_PASSED' not in logs:
        raise RuntimeError('inner rehearsal did not complete')
    print('CONTAINER_EXIT_VERIFIED',owned,status['ExitCode'],flush=True)
finally:
    if owned is not None:
        try:
            status=inspect(owned)['State']
            if status['Running']:
                docker('kill','--signal','KILL',owned)
                until=time.monotonic()+8
                while time.monotonic()<until:
                    status=inspect(owned)['State']
                    if not status['Running']:break
                    time.sleep(.2)
            if status['Running']:raise RuntimeError('owned container still running')
            print('OWNED_CONTAINER_EXIT',owned,status['ExitCode'],flush=True)
        except Exception as error:
            print('OWNED_CONTAINER_UNCERTAIN',owned,type(error).__name__,flush=True)
            raise
