"""Real Docker acceptance for serial synthetic dispatch, using existing images.

Creates only an isolated temporary coordinator, two synthetic jobs and their
TreeDar containers. No builds, downloads, NAS mounts or actual data processing.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from urllib.error import URLError
from urllib.request import Request, urlopen
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--coordinator-image', required=True)
    parser.add_argument('--engine-image', required=True)
    parser.add_argument('--work-parent', required=True, help='Private approved test scratch parent')
    parser.add_argument('--evidence', required=True, help='New metadata-only evidence file outside scratch')
    args = parser.parse_args()
    for item in (args.coordinator_image, args.engine_image):
        if not re.fullmatch(r'sha256:[0-9a-f]{64}', item):
            parser.error('immutable existing image IDs required')
    parent = Path(args.work_parent).absolute()
    evidence_path = Path(args.evidence).absolute()
    if (not parent.is_dir() or parent.is_symlink() or parent.resolve() != parent
            or evidence_path.exists() or not evidence_path.parent.is_dir()):
        parser.error('approved real parent and new evidence file required')
    root = Path(__file__).resolve().parents[1]
    scratch = Path(tempfile.mkdtemp(prefix='dispatch-smoke-', dir=parent))
    scratch.chmod(0o700)
    state, work = scratch / 'state', scratch / 'work'
    state.mkdir(mode=0o700)
    work.mkdir(mode=0o700)
    name = 'lt-dispatch-smoke-' + uuid.uuid4().hex[:12]
    env = {**os.environ, 'PROCESSING_API_TOKEN': secrets.token_urlsafe(36),
           'PROCESSING_PROTOTYPE_OPERATOR_TOKEN': secrets.token_urlsafe(36)}
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        port = listener.getsockname()[1]
    origin = 'http://127.0.0.1:' + str(port)
    created = []
    engine_containers = []
    evidence = {'scope': 'two synthetic TreeDar jobs, no real datasets',
                'coordinatorImage': args.coordinator_image, 'engineImage': args.engine_image}

    def command(command_args, timeout=30):
        result = subprocess.run(command_args, env=env, capture_output=True, text=True, timeout=timeout)
        if result.returncode:
            raise RuntimeError('test operation failed; inspect private scratch: ' + str(scratch))
        return result.stdout

    def request(path, payload=None):
        req = Request(origin + path, data=None if payload is None else json.dumps(payload).encode(),
                      headers={'Authorization': 'Bearer ' + env['PROCESSING_API_TOKEN'],
                               'Content-Type': 'application/json'})
        with urlopen(req, timeout=3) as response:
            return json.load(response)

    passed = False
    try:
        for image in (args.coordinator_image, args.engine_image):
            assert command(['docker', 'image', 'inspect', '--format', '{{.Id}}', image]).strip() == image
        # An isolated RAM-backed queue is sufficient for this drain test; durable
        # coordinator restart acceptance was performed in the separate API test.
        command(['docker', 'create', '--name', name, '--read-only', '--init',
                 '--user', '10001:10001', '--cpus', '0.5', '--memory', '256m',
                 '--pids-limit', '64', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
                 '--tmpfs', '/prototype-state:rw,nosuid,nodev,size=16m,uid=10001,gid=10001,mode=700',
                 '--publish', f'127.0.0.1:{port}:8080',
                 '--env', 'PROCESSING_API_TOKEN', '--env', 'PROCESSING_PROTOTYPE_OPERATOR_TOKEN',
                 '--env', 'PROCESSING_PROTOTYPE_STATE_DIR=/prototype-state', args.coordinator_image])
        created.append(name)
        command(['docker', 'start', name])
        for _ in range(40):
            try:
                request('/v1/prototype/jobs')
                break
            except (OSError, URLError):
                time.sleep(0.25)
        else:
            raise RuntimeError('isolated coordinator failed readiness')
        jobs = []
        for request_id, environment in [('staging-first', 'staging'), ('production-second', 'production')]:
            jobs.append(request('/v1/prototype/jobs', {'requestId': request_id,
                        'environment': environment, 'profile': 'treedar-synthetic-v1'})['job'])
        started = time.monotonic()
        output = command([sys.executable, str(root / 'scripts/prototype_dispatcher.py'),
                          '--coordinator', origin, '--image', args.engine_image,
                          '--scratch', str(work), '--state', str(state),
                          '--worker-id', 'dispatch-smoke', '--max-jobs', '2'], timeout=650)
        evidence['elapsedSeconds'] = round(time.monotonic() - started, 2)
        evidence['dispatcherOutput'] = json.loads(output)
        final = request('/v1/prototype/jobs')['jobs']
        assert len(final) == 2 and all(j['status'] == 'succeeded' for j in final)
        receipts = [json.loads(p.read_text()) for p in (work / '.prototype-receipts').glob('*.json')]
        assert len(receipts) == 2
        for receipt in receipts:
            assert receipt['state'] == 'completed'
            container = receipt['container']
            assert re.fullmatch(r'treedar-prototype-[0-9a-f]{32}', container)
            engine_containers.append(container)
            result = receipt['payload']['result']
            assert result['n_trees'] == 3 and result['image_id'] == args.engine_image
            assert result['jobId'] == receipt['job']['id']
            folder = work / receipt['job']['environment'] / receipt['job']['id']
            assert hashlib.sha256((folder / 'synthetic/manifest.json').read_bytes()).hexdigest() == result['manifest_sha256']
            info = json.loads(command(['docker', 'inspect', container]))[0]
            assert not info['State']['Running'] and info['State']['ExitCode'] == 0
            assert info['HostConfig']['ReadonlyRootfs'] and info['HostConfig']['NetworkMode'] == 'none'
        evidence['jobs'] = final
        # Persisted dispatcher history, not wall-clock timestamp ordering, proves
        # that the later-submitted production job ran before earlier staging.
        evidence['dispatcherState'] = [json.loads(p.read_text()) for p in state.glob('*.json')]
        assert evidence['dispatcherOutput']['completed'] == 2
        sequence = evidence['dispatcherOutput'].get('completedJobIds', [])
        # Dispatcher returns its completed IDs in execution order.
        assert sequence == [jobs[1]['id'], jobs[0]['id']]
        evidence['productionBeforeStaging'] = True
        evidence['workerContainersInspectedStopped'] = len(engine_containers)
        # Save sanitized acceptance before cleanup, so a host permission failure
        # cannot lose already-verified results or require another processing run.
        evidence['testContainersAndScratchRemoved'] = False
        evidence_path.write_text(json.dumps(evidence, indent=2) + '\n')
        # Engine outputs are owned by image UID10001 on Linux. Delete only the
        # verified job directory's children using that same UID, without granting
        # root privileges or changing shared host directory permissions.
        for receipt in receipts:
            folder = work / receipt['job']['environment'] / receipt['job']['id']
            assert folder.resolve() == folder and folder.parent.parent == work
            command(['docker', 'run', '--rm', '--network', 'none', '--read-only',
                     '--user', '10001:10001', '--cap-drop', 'ALL',
                     '--security-opt', 'no-new-privileges', '--cpus', '0.25',
                     '--memory', '64m', '--pids-limit', '32',
                     '--mount', 'type=bind,source=' + str(folder) + ',target=/cleanup',
                     '--entrypoint', 'python', args.engine_image, '-B', '-c',
                     'from pathlib import Path; import shutil; '
                     '[(p.unlink() if p.is_symlink() or not p.is_dir() else shutil.rmtree(p)) '
                     'for p in Path("/cleanup").iterdir()]'])
        for container in engine_containers:
            command(['docker', 'rm', container])
        command(['docker', 'stop', '--time', '10', name])
        info = json.loads(command(['docker', 'inspect', name]))[0]
        assert not info['State']['Running'] and info['State']['Pid'] == 0
        command(['docker', 'rm', name])
        created.clear()
        assert scratch.parent == parent and scratch.resolve() == scratch
        shutil.rmtree(scratch)
        assert not scratch.exists()
        evidence['testContainersAndScratchRemoved'] = True
        evidence_path.write_text(json.dumps(evidence, indent=2) + '\n')
        passed = True
        print(json.dumps({'status': 'PASS', 'jobs': 2, 'treesPerJob': 3,
                          'productionBeforeStaging': True, 'cleanupVerified': True,
                          'evidence': str(evidence_path)}))
    finally:
        if not passed:
            # Preserve uncertain containers, private receipts and outputs for
            # inspection. Never purge a job whose child may still be running.
            for container in created:
                subprocess.run(['docker', 'stop', '--time', '10', container],
                               capture_output=True, timeout=30)
            print('Acceptance incomplete; inspect private scratch ' + str(scratch), file=sys.stderr)


if __name__ == '__main__':
    main()
