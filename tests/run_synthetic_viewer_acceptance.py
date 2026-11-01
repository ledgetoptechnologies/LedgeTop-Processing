"""Owned synthetic Docker harness. REVIEW BEFORE EXECUTING.

No `docker rm`, image cleanup, volume cleanup or recursive filesystem cleanup.
All shutdown operations target only the exact IDs this process created.
"""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import time
import uuid

PROCESSING = Path(__file__).resolve().parents[1]
VIEWER = Path(r'C:\Projects\viewer-download-retention-20261031')
VIEWER_COMMIT = '3f150734aa5f5359f004a3984d1df806113ddbf6'
PYTHON_IMAGE = 'python:3.12-slim'
NODE_IMAGE = 'node:24-bookworm-slim'
LABEL = 'ledgetop.synthetic.rehearsal'


def command(args, *, timeout=8, stdout=subprocess.PIPE):
    return subprocess.run(args, cwd=PROCESSING, stdout=stdout, stderr=subprocess.PIPE,
                          check=True, timeout=timeout)


def docker(*args):
    return command(['docker', *args]).stdout.decode('utf-8', 'replace').strip()


def state(container_id):
    value = json.loads(docker('inspect', container_id))[0]
    if value['Id'] != container_id or value['Config']['Labels'].get(LABEL) != run_id:
        raise RuntimeError('owned container identity mismatch')
    return value['State']


def reconcile_created(name):
    value = json.loads(docker('inspect', name))[0]
    if (value['Name'].lstrip('/') != name or value['Config']['Labels'].get(LABEL) != run_id
            or len(value['Id']) != 64):
        raise RuntimeError('ambiguous create did not match owned name and run label')
    return value['Id']


def create_owned(name, *args):
    try:
        container_id = docker('create', '--name', name, *args)
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
        try:
            container_id = reconcile_created(name)
            containers.append(container_id)
            print('RECONCILED_OWNED_CREATE', name, container_id, flush=True)
        except Exception as error:
            print('UNCERTAIN_CREATE', name, type(error).__name__, flush=True)
        raise
    if len(container_id) != 64:
        raise RuntimeError('invalid owned container ID')
    containers.append(container_id)
    state(container_id)  # verify full ID and exact label before start
    return container_id


def stop_owned(container_id):
    # No names, partial IDs, process enumeration, or shared resources are used.
    if state(container_id)['Running']:
        docker('kill', '--signal', 'KILL', container_id)
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        result = state(container_id)
        if not result['Running']:
            return result
        time.sleep(.2)
    raise RuntimeError('owned container still running')


run_id = os.environ['LT_SYNTHETIC_RUN_ID']
root = Path(os.environ['LT_SYNTHETIC_ROOT']).resolve()
if len(run_id) != 32 or any(c not in '0123456789abcdef' for c in run_id):
    raise ValueError('invalid owned run identity')
if not root.is_dir(): raise ValueError('owned retained root required')
containers = []
print('RETAINED_ROOT', root, 'REHEARSAL', run_id, flush=True)
try:
    current = command(['git', 'rev-parse', 'HEAD']).stdout.decode().strip()
    print('PROCESSING_COMMIT', current, 'VIEWER_COMMIT', VIEWER_COMMIT, flush=True)
    snapshot = root/'viewer'; snapshot.mkdir()
    processing_snapshot = root/'processing'; processing_snapshot.mkdir()
    for source, commit, destination, filename in (
        (VIEWER, VIEWER_COMMIT, snapshot, 'viewer.tar'),
        (PROCESSING, current, processing_snapshot, 'processing.tar')):
        archive = root/filename
        with archive.open('wb') as output:
            command(['git','-C',str(source),'archive','--format=tar',commit],
                    timeout=30, stdout=output)
        with tarfile.open(archive) as tar:
            tar.extractall(destination, filter='data')
    # Only these explicit fixture bytes are taken from the working directory.
    for name in ('synthetic_viewer_acceptance_server.py','synthetic_viewer_acceptance_sender.js'):
        raw=(PROCESSING/'tests'/name).read_bytes()
        target=processing_snapshot/'tests'/name
        target.write_bytes(raw)
        if hashlib.sha256(target.read_bytes()).digest() != hashlib.sha256(raw).digest():
            raise RuntimeError('fixture snapshot mismatch')
        print('FIXTURE_SHA256', name, hashlib.sha256(raw).hexdigest(), flush=True)
    for name in ('nativeReferenceAcceptanceCandidate.js', 'nativeReferenceReceiptCandidate.js', 'nativeReferenceProtocolCandidate.js', 'nativeReferenceBoundSenderCandidate.js', 'nativeReferenceProviderAuthCandidate.js'):
        path=snapshot/'apps'/'viewer'/'server'/name
        print('VIEWER_MODULE_SHA256',name,hashlib.sha256(path.read_bytes()).hexdigest(),flush=True)
    for name in ('reference_api.py','reference_http.py','reference_store.py','reference_protocol.py'):
        path=processing_snapshot/'processing'/name
        print('PROCESSING_MODULE_SHA256',name,hashlib.sha256(path.read_bytes()).hexdigest(),flush=True)
    image_ids = {}
    for image in (PYTHON_IMAGE, NODE_IMAGE):
        image_ids[image]=docker('image','inspect','--format','{{.Id}}',image)
        if not image_ids[image].startswith('sha256:'): raise RuntimeError('invalid image ID')
        print('IMAGE',image,image_ids[image],flush=True)
    binds=['--mount',f'type=bind,src={processing_snapshot},dst=/processing,readonly',
           '--mount',f'type=bind,src={snapshot},dst=/viewer,readonly']
    common=['--label',f'{LABEL}={run_id}','--pids-limit','32','--memory','384m',
            '--cpus','1','--cap-drop','ALL','--security-opt','no-new-privileges']
    print('PLANNED_OWNED_NAMESPACE_NAME','synthetic-namespace-'+run_id,flush=True)
    namespace_id=create_owned('synthetic-namespace-'+run_id,'--network','none',
                     *common,image_ids[PYTHON_IMAGE], 'python','-c','import time; time.sleep(75)')
    print('OWNED_NAMESPACE',namespace_id,flush=True)
    docker('start',namespace_id)
    if not state(namespace_id)['Running']:raise RuntimeError('isolated namespace did not start')
    print('PLANNED_OWNED_SERVER_NAME','synthetic-processing-'+run_id,flush=True)
    server_id=create_owned('synthetic-processing-'+run_id,'--network','container:'+namespace_id,
                     *common,*binds,'--workdir','/processing',image_ids[PYTHON_IMAGE],
                     'python','-u','-m','tests.synthetic_viewer_acceptance_server')
    print('OWNED_SERVER',server_id,flush=True)
    docker('start',server_id)
    deadline=time.monotonic()+15
    while time.monotonic()<deadline:
        status=state(server_id); logs=docker('logs',server_id)
        if 'READY synthetic loopback 17864' in logs: break
        if not status['Running']: raise RuntimeError('server exited before ready')
        time.sleep(.2)
    else: raise RuntimeError('server did not become ready')
    print('PLANNED_OWNED_SENDER_NAME','synthetic-viewer-'+run_id,flush=True)
    sender_id=create_owned('synthetic-viewer-'+run_id,'--network','container:'+namespace_id,
                     *common,*binds,'--workdir','/processing',image_ids[NODE_IMAGE],
                     'node','tests/synthetic_viewer_acceptance_sender.js')
    print('OWNED_SENDER',sender_id,flush=True)
    docker('start',sender_id)
    deadline=time.monotonic()+15
    while time.monotonic()<deadline:
        first_state=state(sender_id)
        if not first_state['Running']: break
        time.sleep(.2)
    else: raise RuntimeError('first owned sender exceeded deadline')
    first_log=docker('logs',sender_id)
    if first_state['ExitCode']!=0 or 'FIRST_SEND_RETRY_HELD synthetic-only' not in first_log:
        raise RuntimeError('first sender did not retain a retryable receipt')
    deadline=time.monotonic()+12
    while time.monotonic()<deadline:
        first_server=state(server_id)
        if not first_server['Running']:break
        time.sleep(.2)
    else:raise RuntimeError('first server did not stop after lost reply')
    if first_server['ExitCode']!=0:raise RuntimeError('first server failed')
    first_server_log=docker('logs',server_id)
    if ('SERVER_START_RECEIPTS 0' not in first_server_log
        or 'RETAINED_RECEIPTS 1' not in first_server_log):
        raise RuntimeError('first server did not durably retain acceptance')
    print('FIRST_SENDER_STOPPED',sender_id,first_state['ExitCode'],flush=True)
    print('FIRST_SERVER_STOPPED',server_id,first_server['ExitCode'],flush=True)
    # The dedicated owned namespace remains up, but no Processing listener is
    # running. Restart the sender and require a second real retry exhaustion.
    if state(server_id)['Running'] or not state(namespace_id)['Running']:
        raise RuntimeError('offline precondition not met')
    docker('start',sender_id)
    deadline=time.monotonic()+12
    while time.monotonic()<deadline:
        offline_state=state(sender_id)
        if not offline_state['Running']:break
        time.sleep(.2)
    else:raise RuntimeError('offline sender exceeded deadline')
    offline_log=docker('logs',sender_id)
    if offline_state['ExitCode']!=0 or 'OFFLINE_RETRY_HELD synthetic-only' not in offline_log:
        raise RuntimeError('sender did not retain its receipt during server outage')
    if state(server_id)['Running']:raise RuntimeError('server was not offline during retry')
    print('OFFLINE_RETRY_CONFIRMED',sender_id,flush=True)
    # Restart the same owned Processing container with its retained SQLite file.
    docker('start',server_id)
    deadline=time.monotonic()+12
    while time.monotonic()<deadline:
        resumed=state(server_id); logs=docker('logs',server_id)
        if ('SERVER_START_RECEIPTS 1' in logs and
            logs.count('READY synthetic loopback 17864')>=2):break
        if not resumed['Running']:raise RuntimeError('restarted server exited before ready')
        time.sleep(.2)
    else:raise RuntimeError('restarted server did not become ready')
    # A second start creates a new Node process with the same SQLite receipt.
    docker('start',sender_id)
    deadline=time.monotonic()+30
    while time.monotonic()<deadline:
        current_states=[state(server_id), state(sender_id)]
        if all(not item['Running'] for item in current_states): break
        time.sleep(.2)
    else: raise RuntimeError('owned containers exceeded deadline')
    sender_log=docker('logs',sender_id);server_log=docker('logs',server_id)
    print('SENDER_LOG',sender_log,flush=True);print('SERVER_LOG',server_log,flush=True)
    if any(item['ExitCode']!=0 for item in current_states): raise RuntimeError('container failed')
    if ('FIRST_SEND_RETRY_HELD synthetic-only' not in sender_log
        or 'OFFLINE_RETRY_HELD synthetic-only' not in sender_log
        or 'OFFLINE_CREDENTIAL_RELOAD_OK' not in sender_log
        or 'AUTH_REVISION v1' not in server_log
        or 'AUTH_REVISION v2' not in server_log
        or sender_log.count('SYNTHETIC_RESTART_EXACT_PAYLOAD_OK') != 2
        or 'VIEWER_RESTART_REPLAY_OK held=true synthetic-only' not in sender_log
        or 'ACCEPTANCE_RESULT 200 True' not in server_log
        or 'SYNTHETIC_FIRST_REPLY_DISCARDED_AFTER_COMMIT' not in server_log
        or 'ACCEPTANCE_RESULT 200 False' not in server_log
        or server_log.count('RETAINED_RECEIPTS 1') != 2
        or 'SERVER_START_RECEIPTS 0' not in server_log
        or 'SERVER_START_RECEIPTS 1' not in server_log):
        raise RuntimeError('rehearsal assertions incomplete')
    if not state(namespace_id)['Running']:
        raise RuntimeError('owned namespace expired before verification')
    holder_exit=stop_owned(namespace_id)  # exact owned bounded holder; retain container
    if holder_exit['Running'] or holder_exit['ExitCode'] != 137:
        raise RuntimeError('owned namespace did not stop as expected')
    print('OWNED_NAMESPACE_STOPPED',namespace_id,holder_exit['ExitCode'],flush=True)
    print('REHEARSAL_PASS',flush=True)
finally:
    failures=[]
    for container_id in reversed(containers):
        try:
            result=stop_owned(container_id)
            print('OWNED_EXIT',container_id,result['ExitCode'],flush=True)
        except Exception as error:
            failures.append((container_id,type(error).__name__))
    if failures:
        print('UNCERTAIN_OWNED_CONTAINERS',failures,file=sys.stderr,flush=True)
        raise RuntimeError('owned container cleanup uncertain')
