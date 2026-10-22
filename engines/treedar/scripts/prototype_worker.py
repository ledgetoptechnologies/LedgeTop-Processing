"""One-shot host worker for the explicitly synthetic coordinator profile.

Only the standard library runs on the host. Docker receives no credentials.
Receipts contain claim secrets: keep the approved scratch directory private.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import urllib.parse
import urllib.request
import uuid


PROFILE = 'treedar-synthetic-v1'
IMAGE = re.compile(r'sha256:[0-9a-f]{64}\Z')
SLUG = re.compile(r'[A-Za-z0-9][A-Za-z0-9._-]{0,63}\Z')
TOKEN = re.compile(r'[A-Za-z0-9_-]{32,128}\Z')
ROOT = Path(__file__).resolve().parents[1]


class WorkerError(Exception):
    """Safe public failure category, without server bodies or secrets."""


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise WorkerError('coordinator redirect refused')


def origin(value, verified=None):
    parsed = urllib.parse.urlsplit(value)
    if (parsed.scheme not in ('http', 'https') or not parsed.hostname
            or parsed.username or parsed.password or parsed.path not in ('', '/')
            or parsed.query or parsed.fragment):
        raise WorkerError('coordinator must be an exact HTTP(S) origin')
    try:
        parsed.port
    except ValueError:
        raise WorkerError('invalid coordinator port') from None
    normalized = value.rstrip('/')
    if parsed.hostname not in ('localhost', '127.0.0.1', '::1') and normalized != verified:
        raise WorkerError('non-loopback coordinator requires matching --verified-origin')
    return normalized


def post(base, token, route, payload):
    request = urllib.request.Request(base + route, method='POST',
        data=json.dumps(payload, allow_nan=False).encode(),
        headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    try:
        with opener.open(request, timeout=15) as response:
            body = response.read(65537)
            if len(body) > 65536:
                raise WorkerError('coordinator response too large')
            result = json.loads(body)
            if not isinstance(result, dict):
                raise ValueError()
            return result
    except Exception:
        # A claim/completion might have committed even if its reply was lost.
        raise WorkerError('coordinator response uncertain; do not repeat claim') from None


def validate_job(job):
    if not isinstance(job, dict):
        raise WorkerError('invalid claimed job')
    identifier = job.get('id')
    try:
        valid_id = isinstance(identifier, str) and str(uuid.UUID(identifier)) == identifier
    except (ValueError, AttributeError):
        valid_id = False
    if (not valid_id or job.get('environment') not in ('production', 'staging')
            or job.get('profile') != PROFILE
            or not isinstance(job.get('claimToken'), str)
            or not TOKEN.fullmatch(job['claimToken'])):
        raise WorkerError('invalid claimed job; operator inspection required')
    return {key: job[key] for key in ('id', 'environment', 'profile', 'claimToken')}


def save(path, receipt):
    temporary = path.with_suffix('.tmp')
    with os.fdopen(os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), 'w') as stream:
        json.dump(receipt, stream, sort_keys=True, allow_nan=False)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def docker(args, timeout=20):
    return subprocess.run(['docker', *args], capture_output=True, text=True,
                          encoding='utf-8', errors='replace', timeout=timeout)


def stopped(container):
    """Prove this exact retained container stopped; uncertainty never frees a slot."""
    try:
        state = docker(['inspect', '--format', '{{.State.Running}}', container])
        return state.returncode == 0 and state.stdout.strip() == 'false'
    except (OSError, subprocess.SubprocessError):
        return False


def stop_and_verify(container):
    try:
        docker(['stop', '--time', '5', container], timeout=15)
    except (OSError, subprocess.SubprocessError):
        pass
    return stopped(container)


def mount(path, target, readonly=False):
    if ',' in str(path):
        raise WorkerError('scratch/source paths cannot contain commas')
    return 'type=bind,source=' + str(path) + ',target=' + target + (',readonly' if readonly else '')


def execute(image, output, container, timeout):
    helper = ROOT / 'scripts' / 'prototype_synthetic.py'
    fixture = ROOT / 'tests' / 'synthetic_forest.py'
    command = ['create', '--name', container, '--network', 'none', '--cpus', '2',
               '--memory', '2g', '--pids-limit', '128', '--cap-drop', 'ALL',
               '--security-opt', 'no-new-privileges', '--read-only',
               '--tmpfs', '/tmp:rw,nosuid,nodev,size=512m',
               '--env', 'OMP_NUM_THREADS=2', '--env', 'OPENBLAS_NUM_THREADS=2',
               '--mount', mount(output, '/runs'),
               '--mount', mount(helper, '/worker/prototype_synthetic.py', True),
               '--mount', mount(fixture, '/worker/synthetic_forest.py', True),
               '--entrypoint', 'python', image, '-B', '/worker/prototype_synthetic.py']
    try:
        created = docker(command)
        if created.returncode != 0:
            raise WorkerError('container creation failed')
        completed = docker(['start', '--attach', container], timeout=timeout)
        if not stopped(container):
            raise WorkerError('container state uncertain')
        if completed.returncode != 0:
            raise WorkerError('synthetic pipeline failed')
        lines = [line[len('PROTOTYPE_RESULT='):] for line in completed.stdout.splitlines()
                 if line.startswith('PROTOTYPE_RESULT=')]
        if len(lines) != 1:
            raise WorkerError('missing synthetic result')
        result = json.loads(lines[0])
        expected = {'n_trees', 'treedar_version', 'manifest_sha256', 'outputs_sha256'}
        if (not isinstance(result, dict) or set(result) != expected or result['n_trees'] != 3
                or not isinstance(result['treedar_version'], str)
                or len(result['treedar_version']) > 64
                or any(not isinstance(result[k], str) or not re.fullmatch('[0-9a-f]{64}', result[k])
                       for k in ('manifest_sha256', 'outputs_sha256'))):
            raise WorkerError('invalid synthetic result')
        # Independently confirm hashes against the mounted artifacts on the host.
        for key, relative in [('manifest_sha256', 'synthetic/manifest.json'),
                              ('outputs_sha256', 'output-sha256.json')]:
            path = output / relative
            if path.is_symlink() or hashlib.sha256(path.read_bytes()).hexdigest() != result[key]:
                raise WorkerError('output hash mismatch')
        hashes = json.loads((output / 'output-sha256.json').read_text())
        if (not isinstance(hashes, dict) or not {'summary.json', 'report.html'} <= hashes.keys()
                or len(hashes) > 100):
            raise WorkerError('invalid output index')
        artifact_root = (output / 'synthetic').resolve()
        for relative, digest in hashes.items():
            if (not isinstance(relative, str) or '\\' in relative or ':' in relative
                    or Path(relative).is_absolute() or '..' in Path(relative).parts
                    or not isinstance(digest, str) or not re.fullmatch('[0-9a-f]{64}', digest)):
                raise WorkerError('invalid output index')
            path = output / 'synthetic' / relative
            if (path.is_symlink() or not path.resolve().is_relative_to(artifact_root)
                    or hashlib.sha256(path.read_bytes()).hexdigest() != digest):
                raise WorkerError('output hash mismatch')
        return 'succeeded', result
    except Exception:
        if not stop_and_verify(container):
            raise WorkerError('container stop unverified; claim remains running') from None
        return 'failed', {'error': 'synthetic_execution_failed'}


def complete(receipt_path, receipt, token):
    post(receipt['coordinator'], token,
         '/v1/prototype/jobs/' + receipt['job']['id'] + '/complete', receipt['payload'])
    receipt['state'] = 'completed'
    save(receipt_path, receipt)


def run(args):
    base = origin(args.coordinator, args.verified_origin)
    if not IMAGE.fullmatch(args.image) or not SLUG.fullmatch(args.worker_id):
        raise WorkerError('use an immutable sha256 image ID and an ASCII worker ID')
    token = os.environ.get('PROCESSING_API_TOKEN', '')
    if not token or '\n' in token or '\r' in token:
        raise WorkerError('PROCESSING_API_TOKEN is required')
    scratch = Path(args.scratch).absolute()
    if not scratch.is_dir() or scratch.is_symlink() or scratch.resolve() != scratch:
        raise WorkerError('scratch must be an existing real approved directory')
    # A private receipt directory also prevents simultaneous invocations on this root.
    receipts = scratch / '.prototype-receipts'
    receipts.mkdir(mode=0o700, exist_ok=True)
    if receipts.is_symlink() or receipts.resolve().parent != scratch:
        raise WorkerError('invalid receipt directory')
    lock = receipts / 'worker.lock'
    try:
        fd = os.open(lock, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        raise WorkerError('worker lock exists; inspect previous worker before removing it') from None
    os.close(fd)
    try:
        if args.resume:
            receipt_path = Path(args.resume).absolute()
            if receipt_path.parent != receipts or receipt_path.is_symlink():
                raise WorkerError('resume requires a receipt in the approved scratch root')
            receipt = json.loads(receipt_path.read_text())
            validate_job(receipt['job'])
            if receipt['coordinator'] != base or receipt['state'] != 'completion_pending':
                raise WorkerError('receipt cannot be resumed; inspect manually')
            complete(receipt_path, receipt, token)
            return 0 if receipt['payload']['status'] == 'succeeded' else 1
        if any(json.loads(path.read_text()).get('state') != 'completed'
               for path in receipts.glob('*.json')):
            raise WorkerError('unresolved local receipt; inspect or resume before claiming')
        inspected = docker(['image', 'inspect', '--format', '{{.Id}}', args.image])
        if inspected.returncode or inspected.stdout.strip() != args.image:
            raise WorkerError('exact image ID must already exist locally')
        receipt_path = receipts / (uuid.uuid4().hex + '.json')
        receipt = {'coordinator': base, 'state': 'claim_pending', 'workerId': args.worker_id}
        save(receipt_path, receipt)
        response = post(base, token, '/v1/prototype/claim', {'workerId': args.worker_id})
        if 'job' not in response:
            raise WorkerError('invalid claim response; inspect coordinator before retry')
        if response['job'] is None:
            receipt['state'] = 'completed'
            save(receipt_path, receipt)
            print('No work available.')
            return 0
        # Persist the claim immediately so a rejected job cannot silently be retried.
        receipt['job'] = response['job']
        receipt['state'] = 'claimed'
        save(receipt_path, receipt)
        job = validate_job(response['job'])
        receipt['job'] = job
        receipt['container'] = 'treedar-prototype-' + uuid.uuid4().hex
        save(receipt_path, receipt)
        environment = scratch / job['environment']
        environment.mkdir(exist_ok=True)
        if environment.is_symlink() or environment.resolve().parent != scratch:
            raise WorkerError('invalid environment output directory')
        output = environment / job['id']
        output.mkdir(mode=0o777)
        output.chmod(0o777)
        status, result = execute(args.image, output, receipt['container'], args.timeout)
        result.update(environment=job['environment'], jobId=job['id'], image_id=args.image)
        if len(json.dumps(result, allow_nan=False).encode()) > 4096:
            raise WorkerError('result exceeds coordinator limit')
        receipt['payload'] = {'claimToken': job['claimToken'], 'status': status, 'result': result}
        receipt['state'] = 'completion_pending'
        save(receipt_path, receipt)
        complete(receipt_path, receipt, token)
        print(json.dumps({'status': status, **result}, sort_keys=True))
        return 0 if status == 'succeeded' else 1
    finally:
        lock.unlink()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--coordinator', required=True)
    parser.add_argument('--verified-origin', help='Exact independently verified non-loopback origin')
    parser.add_argument('--image', required=True, help='Locally built sha256 image ID')
    parser.add_argument('--scratch', required=True, help='Existing approved private scratch root')
    parser.add_argument('--worker-id', required=True)
    parser.add_argument('--timeout', type=int, choices=range(180, 301), default=240)
    parser.add_argument('--resume', help='Resend only an exact pending completion receipt')
    args = parser.parse_args()
    try:
        return run(args)
    except WorkerError as error:
        print(str(error), file=sys.stderr)
        return 2
    except Exception:
        print('Worker interrupted; inspect local receipts and container before retrying.', file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main())
