"""One-shot host worker for the explicitly synthetic coordinator profile.

Only the standard library runs on the host. Docker receives no credentials.
Receipts contain claim secrets: keep the approved scratch directory private.
Requires a node-aware coordinator returning the exact claimed nodeId; no legacy
claim fallback may bypass maintenance admission.
"""

import argparse
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import random
import stat
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


PROFILE = 'treedar-synthetic-v1'
IMAGE = re.compile(r'sha256:[0-9a-f]{64}\Z')
SLUG = re.compile(r'[A-Za-z0-9][A-Za-z0-9._-]{0,63}\Z')
TOKEN = re.compile(r'[A-Za-z0-9_-]{32,128}\Z')
ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from processing.node_gate import GateError, NodeGate

DOCKER_ENDPOINT = None


class WorkerError(Exception):
    """Safe public failure category, without server bodies or secrets."""


class RetryableDelivery(WorkerError):
    """The exact completion may be resent; claims must never be replayed."""


class PermanentDelivery(WorkerError):
    """An operator must reconcile this response before further delivery."""


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise PermanentDelivery('coordinator redirect refused')


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
                raise PermanentDelivery('coordinator response too large')
            result = json.loads(body)
            if not isinstance(result, dict):
                raise ValueError()
            return result
    except urllib.error.HTTPError as error:
        if error.code in (408, 429) or 500 <= error.code <= 599:
            raise RetryableDelivery('coordinator temporarily unavailable') from None
        raise PermanentDelivery('coordinator rejected request; operator inspection required') from None
    except PermanentDelivery:
        raise
    except (urllib.error.URLError, OSError, TimeoutError, http.client.HTTPException):
        # A claim/completion might have committed even if its reply was lost.
        raise RetryableDelivery('coordinator response uncertain; do not repeat claim') from None
    except Exception:
        raise PermanentDelivery('coordinator response invalid; operator inspection required') from None


def validate_job(job, node_id='cluster'):
    if not isinstance(job, dict):
        raise WorkerError('invalid claimed job')
    identifier = job.get('id')
    try:
        valid_id = isinstance(identifier, str) and str(uuid.UUID(identifier)) == identifier
    except (ValueError, AttributeError):
        valid_id = False
    if (not valid_id or job.get('environment') not in ('production', 'staging')
            or job.get('profile') != PROFILE
            or not isinstance(job.get('nodeId'), str) or not SLUG.fullmatch(job['nodeId'])
            or job['nodeId'] != node_id
            or not isinstance(job.get('claimToken'), str)
            or not TOKEN.fullmatch(job['claimToken'])):
        raise WorkerError('invalid claimed job; operator inspection required')
    result = {key: job[key] for key in ('id', 'environment', 'profile', 'claimToken', 'nodeId')}
    if 'authorityId' in job:
        try:
            if not isinstance(job['authorityId'], str) or str(uuid.UUID(job['authorityId'])) != job['authorityId']:
                raise ValueError()
        except (ValueError, AttributeError):
            raise WorkerError('invalid claim authority; operator inspection required') from None
        result['authorityId'] = job['authorityId']
    if 'workerId' in job:
        if not isinstance(job['workerId'], str) or not SLUG.fullmatch(job['workerId']):
            raise WorkerError('invalid claimed worker identity')
        result['workerId'] = job['workerId']
    return result


def save(path, receipt):
    temporary = path.with_suffix('.tmp')
    if path.is_symlink() or temporary.is_symlink() or temporary.exists():
        raise WorkerError('unfinished or invalid receipt write; inspect manually')
    with os.fdopen(os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as stream:
        json.dump(receipt, stream, sort_keys=True, allow_nan=False)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    if os.name != 'nt':
        descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def docker(args, timeout=20):
    command = ['docker']
    environment = None
    if DOCKER_ENDPOINT is not None:
        command.extend(['--host', DOCKER_ENDPOINT])
        environment = {key: value for key, value in os.environ.items()
                       if key.upper() not in ('DOCKER_CONTEXT', 'DOCKER_HOST', 'DOCKER_TLS_VERIFY',
                                              'DOCKER_CERT_PATH', 'DOCKER_TLS')}
    return subprocess.run([*command, *args], capture_output=True, text=True,
                          encoding='utf-8', errors='replace', timeout=timeout, env=environment)


def resolve_docker_endpoint():
    """Resolve Docker's selected context once, then use only a local socket."""
    context = os.environ.get('DOCKER_CONTEXT')
    endpoint = os.environ.get('DOCKER_HOST') if not context else None
    if not endpoint:
        if not context:
            selected = subprocess.run(['docker', 'context', 'show'], capture_output=True,
                                      text=True, timeout=15)
            if selected.returncode:
                raise WorkerError('Docker context unavailable')
            context = selected.stdout.strip()
        if not context or context.startswith('-') or '\n' in context or '\r' in context:
            raise WorkerError('invalid Docker context')
        selected = subprocess.run(['docker', 'context', 'inspect', context], capture_output=True,
                                  text=True, timeout=15)
        try:
            contexts = json.loads(selected.stdout)
            if selected.returncode or not isinstance(contexts, list) or len(contexts) != 1:
                raise ValueError()
            endpoint = contexts[0]['Endpoints']['docker']['Host']
        except (ValueError, KeyError, TypeError):
            raise WorkerError('Docker endpoint unavailable') from None
    if (not isinstance(endpoint, str) or not endpoint.isascii()
            or any(character.isspace() or ord(character) < 32 for character in endpoint)):
        raise WorkerError('invalid Docker endpoint')
    parsed = urllib.parse.urlsplit(endpoint)
    if (parsed.scheme not in ('unix', 'npipe') or parsed.netloc or not parsed.path.startswith('/')
            or parsed.query or parsed.fragment or '..' in parsed.path.split('/')):
        raise WorkerError('worker requires a local Unix or named-pipe Docker endpoint')
    return endpoint


def authority(base, token):
    request = urllib.request.Request(base + '/v1/prototype/authority',
                                     headers={'Authorization': 'Bearer ' + token})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    try:
        with opener.open(request, timeout=15) as response:
            raw = response.read(8193)
            if len(raw) > 8192:
                raise ValueError()
            identifier = json.loads(raw)['authorityId']
            if not isinstance(identifier, str) or str(uuid.UUID(identifier)) != identifier:
                raise ValueError()
            return identifier
    except Exception:
        raise WorkerError('coordinator authority read unavailable or invalid; no claim sent') from None


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


def durable_artifacts(output, paths):
    """Flush verified local outputs before a durable success receipt can exist.

    The processing container is already stopped. Refuse links and unexpected
    file types rather than flushing an artifact through an indirect path.
    """
    root = output.absolute()
    if root.is_symlink() or root.resolve() != root or not root.is_dir():
        raise WorkerError('invalid artifact root')
    directories = {root, root.parent, root.parent.parent}
    for path in set(paths):
        if (not path.is_relative_to(root) or path.is_symlink()
                or path.resolve() != path or not path.is_file()):
            raise WorkerError('invalid artifact path')
        parent = path.parent
        while parent != root:
            if parent.is_symlink() or not parent.is_dir():
                raise WorkerError('invalid artifact directory')
            directories.add(parent)
            parent = parent.parent
        # Windows _commit requires a writable descriptor. O_RDWR changes no
        # bytes and never creates/truncates; POSIX accepts a read-only fd.
        flags = os.O_RDWR if os.name == 'nt' else os.O_RDONLY
        descriptor = os.open(path, flags | getattr(os, 'O_NOFOLLOW', 0))
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise WorkerError('artifact must be a regular file')
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    if os.name != 'nt':
        for directory in sorted(directories, key=lambda value: len(value.parts), reverse=True):
            descriptor = os.open(directory, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0)
                                 | getattr(os, 'O_NOFOLLOW', 0))
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)


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
        verified = []
        for key, relative in [('manifest_sha256', 'synthetic/manifest.json'),
                              ('outputs_sha256', 'output-sha256.json')]:
            path = output / relative
            if path.is_symlink() or hashlib.sha256(path.read_bytes()).hexdigest() != result[key]:
                raise WorkerError('output hash mismatch')
            verified.append(path)
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
            verified.append(path)
        durable_artifacts(output, verified)
        return 'succeeded', result
    except Exception:
        if not stop_and_verify(container):
            raise WorkerError('container stop unverified; claim remains running') from None
        return 'failed', {'error': 'synthetic_execution_failed'}


def validate_completion(receipt):
    """Only the persisted, original completion is eligible for delivery."""
    payload = receipt.get('payload')
    job = validate_job(receipt.get('job'), receipt.get('nodeId'))
    if (not isinstance(receipt.get('workerId'), str) or not SLUG.fullmatch(receipt['workerId'])
            or not isinstance(payload, dict) or set(payload) != {'claimToken', 'status', 'result'}
            or payload.get('claimToken') != job['claimToken']
            or payload.get('status') not in ('succeeded', 'failed')
            or not isinstance(payload.get('result'), dict)
            or len(json.dumps(payload['result'], allow_nan=False).encode()) > 4096):
        raise WorkerError('invalid pending completion; inspect manually')


def acknowledged(receipt, response):
    job = response.get('job') if isinstance(response, dict) else None
    expected = receipt['job']
    return (isinstance(job, dict)
            and all(job.get(key) == expected[key]
                    for key in ('id', 'environment', 'profile', 'nodeId'))
            and job.get('workerId') == receipt['workerId']
            and job.get('status') == receipt['payload']['status']
            and ('authorityId' not in expected or job.get('authorityId') == expected['authorityId'])
            and json.dumps(job.get('result'), sort_keys=True, allow_nan=False, separators=(',', ':'))
                == json.dumps(receipt['payload']['result'], sort_keys=True, allow_nan=False, separators=(',', ':')))


def complete(receipt_path, receipt, token, wait_seconds=0, *, clock=None,
             sleep=None, monotonic=None, jitter=None):
    """Deliver a durable outbox item without reclaiming or rerunning the job.

    The retry schedule is written before sending, including the crash/lost-ACK
    case. A foreground wait is bounded; a supervisor may resume this receipt
    later. Auth/conflict/protocol failures retain the outbox for inspection.
    """
    clock, sleep = clock or time.time, sleep or time.sleep
    monotonic, jitter = monotonic or time.monotonic, jitter or random.uniform
    if type(wait_seconds) is not int or not -1 <= wait_seconds <= 86400:
        raise WorkerError('invalid delivery wait bound')
    validate_completion(receipt)
    delivery = receipt.setdefault('delivery', {'attempts': 0, 'nextAttemptAt': 0})
    if (not isinstance(delivery, dict) or type(delivery.get('attempts')) is not int
            or not 0 <= delivery['attempts'] <= 1_000_000
            or type(delivery.get('nextAttemptAt')) not in (int, float)
            or not 0 <= delivery['nextAttemptAt'] < 1e12):
        raise WorkerError('invalid delivery schedule; inspect manually')
    if delivery.get('blockedReason'):
        raise WorkerError('completion delivery blocked; operator inspection required')
    deadline = float('inf') if wait_seconds == -1 else monotonic() + wait_seconds
    # Wall time survives restart, but may have moved backwards. Never inherit
    # more than the maximum backoff, and use monotonic time while this process
    # lives so NTP/clock corrections cannot stall delivery indefinitely.
    remaining = min(300, max(0, delivery['nextAttemptAt'] - clock()))
    if delivery['nextAttemptAt'] - clock() > 300:
        delivery['nextAttemptAt'] = clock() + remaining
        save(receipt_path, receipt)
    next_due = monotonic() + remaining
    while True:
        delay = max(0, next_due - monotonic())
        if delay:
            if delay > max(0, deadline - monotonic()):
                raise WorkerError('completion pending; resume after its persisted retry time')
            sleep(delay)
            continue
        # Persist a backoff before the request so a process interruption cannot
        # turn repeated resumes into a hot retry loop.
        if delivery['attempts'] >= 1_000_000:
            delivery['blockedReason'] = 'delivery_attempt_limit_reached'
            save(receipt_path, receipt)
            raise WorkerError('completion delivery blocked; operator inspection required')
        delivery['attempts'] += 1
        base_delay = min(300, 2 ** min(delivery['attempts'] - 1, 9))
        backoff = min(300, max(1, base_delay * jitter(0.8, 1.2)))
        delivery['nextAttemptAt'] = clock() + backoff
        next_due = monotonic() + backoff
        delivery['lastError'] = 'delivery_unconfirmed'
        save(receipt_path, receipt)
        try:
            response = post(receipt['coordinator'], token,
                '/v1/prototype/jobs/' + receipt['job']['id'] + '/complete', receipt['payload'])
            try:
                exact_ack = acknowledged(receipt, response)
            except (ValueError, TypeError, RecursionError):
                exact_ack = False
            if not exact_ack:
                raise PermanentDelivery('completion acknowledgment differs')
        except RetryableDelivery:
            delivery['lastError'] = 'coordinator_unavailable'
            save(receipt_path, receipt)
            if monotonic() >= deadline:
                raise WorkerError('completion saved locally; coordinator unavailable') from None
            continue
        except WorkerError:
            delivery['blockedReason'] = 'request_rejected_or_invalid_ack'
            save(receipt_path, receipt)
            raise WorkerError('completion delivery blocked; operator inspection required') from None
        receipt['acknowledgment'] = response['job']
        receipt['state'] = 'completed'
        delivery['lastError'] = None
        save(receipt_path, receipt)
        return


def run(args):
    global DOCKER_ENDPOINT
    base = origin(args.coordinator, args.verified_origin)
    node_id = getattr(args, 'node_id', 'cluster')
    delivery_wait = getattr(args, 'delivery_wait_seconds', 0)
    if type(delivery_wait) is not int or not -1 <= delivery_wait <= 86400:
        raise WorkerError('invalid delivery wait bound')
    if (not IMAGE.fullmatch(args.image) or not SLUG.fullmatch(args.worker_id)
            or not isinstance(node_id, str) or not SLUG.fullmatch(node_id)):
        raise WorkerError('use an immutable sha256 image ID and ASCII worker/node IDs')
    engine_id = getattr(args, 'docker_engine_id', None)
    generation = getattr(args, 'node_generation', None)
    if not isinstance(engine_id, str) or not engine_id or type(generation) is not int or generation < 1:
        raise WorkerError('explicit Docker Engine identity and node generation are required')
    token = os.environ.get('PROCESSING_API_TOKEN', '')
    if not token or '\n' in token or '\r' in token:
        raise WorkerError('PROCESSING_API_TOKEN is required')
    scratch = Path(args.scratch).absolute()
    if not scratch.is_dir() or scratch.is_symlink() or scratch.resolve() != scratch:
        raise WorkerError('scratch must be an existing real approved directory')
    receipts = scratch / '.prototype-receipts'
    receipts.mkdir(mode=0o700, exist_ok=True)
    if receipts.is_symlink() or receipts.resolve().parent != scratch:
        raise WorkerError('invalid receipt directory')
    lock = receipts / 'worker.lock'
    if lock.exists() or lock.is_symlink():
        raise WorkerError('worker lock exists; inspect previous worker before removing it') from None
    previous_endpoint = DOCKER_ENDPOINT
    try:
        DOCKER_ENDPOINT = resolve_docker_endpoint()
        inspected_engine = docker(['info', '--format', '{{.ID}}'])
        if inspected_engine.returncode or inspected_engine.stdout.strip() != engine_id:
            raise WorkerError('local Docker Engine identity differs from approved node')
        with NodeGate(engine_id) as gate:
            binding = gate.snapshot()
            if (binding['generation'] != generation or binding['nodeId'] != node_id
                    or binding['coordinatorOrigin'] != base):
                raise WorkerError('physical node ownership differs from worker configuration')
            if args.resume:
                return resume(args, base, node_id, token, receipts, gate, binding, delivery_wait)
            if any(json.loads(path.read_text()).get('state') != 'completed'
                   for path in receipts.glob('*.json')):
                raise WorkerError('unresolved local receipt; inspect or resume before claiming')
            authority_id = authority(base, token)
            if authority_id != binding['authorityId']:
                raise WorkerError('coordinator does not own this physical node')
            inspected = docker(['image', 'inspect', '--format', '{{.Id}}', args.image])
            if inspected.returncode or inspected.stdout.strip() != args.image:
                raise WorkerError('exact image ID must already exist locally')
            receipt_path = receipts / (uuid.uuid4().hex + '.json')
            gate.reserve(node_id, base, authority_id, generation, receipt_path.stem)
            receipt = {'coordinator': base, 'state': 'claim_pending', 'workerId': args.worker_id,
                       'nodeId': node_id, 'authorityId': authority_id, 'engineId': engine_id,
                       'nodeGeneration': generation, 'reservationId': receipt_path.stem}
            save(receipt_path, receipt)
            response = post(base, token, '/v1/prototype/claim',
                            {'workerId': args.worker_id, 'nodeId': node_id})
            if not isinstance(response, dict) or 'job' not in response:
                raise WorkerError('invalid claim response; inspect coordinator before retry')
            if response['job'] is None:
                if response != {'job': None}:
                    raise WorkerError('invalid empty claim response')
                receipt['state'] = 'completed'
                receipt['claimAcknowledgedEmpty'] = True
                save(receipt_path, receipt)
                gate.release_empty(receipt_path.stem, response)
                print('No work available.')
                return 0
            # Persist the original claim even when validation rejects it.
            receipt['job'] = response['job']
            receipt['state'] = 'claimed'
            save(receipt_path, receipt)
            job = validate_job(response['job'], node_id)
            if job.get('authorityId') != authority_id or job.get('workerId') != args.worker_id:
                raise WorkerError('claim authority or worker identity differs')
            gate.bind(receipt_path.stem, job)
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
            result.update(environment=job['environment'], jobId=job['id'], image_id=args.image,
                          nodeId=node_id)
            if len(json.dumps(result, allow_nan=False).encode()) > 4096:
                raise WorkerError('result exceeds coordinator limit')
            receipt['payload'] = {'claimToken': job['claimToken'], 'status': status, 'result': result}
            receipt['state'] = 'completion_pending'
            save(receipt_path, receipt)
            gate.prepare_completion(receipt_path.stem, receipt['payload'])
            complete(receipt_path, receipt, token, delivery_wait)
            gate.acknowledge(receipt_path.stem, receipt['acknowledgment'])
            print(json.dumps({'status': status, **result}, sort_keys=True))
            return 0 if status == 'succeeded' else 1
    except GateError:
        raise WorkerError('physical node gate refused work; inspect ownership and reservation') from None
    finally:
        DOCKER_ENDPOINT = previous_endpoint


def resume(args, base, node_id, token, receipts, gate, binding, delivery_wait):
    """Reconcile only this exact receipt; never claim or start a container."""
    receipt_path = Path(args.resume).absolute()
    if (receipt_path.parent != receipts or receipt_path.is_symlink() or not receipt_path.is_file()
            or not re.fullmatch('[0-9a-f]{32}\\.json', receipt_path.name)
            or receipt_path.stat().st_size > 65536):
        raise WorkerError('resume requires an exact receipt in the approved scratch root')
    receipt = json.loads(receipt_path.read_bytes())
    if (receipt.get('coordinator') != base or receipt.get('nodeId') != node_id
            or receipt.get('workerId') != args.worker_id or receipt.get('engineId') != binding['engineId']
            or receipt.get('authorityId') != binding['authorityId']
            or receipt.get('nodeGeneration') != binding['generation']
            or receipt.get('reservationId') != receipt_path.stem):
        raise WorkerError('receipt cannot be resumed under this node ownership')
    if receipt.get('claimAcknowledgedEmpty') is True:
        if receipt.get('state') != 'completed' or not {'job', 'container', 'payload'}.isdisjoint(receipt):
            raise WorkerError('invalid empty claim receipt')
        gate.release_empty(receipt_path.stem, {'job': None})
        return 0
    validate_completion(receipt)
    if receipt['job'].get('authorityId') != binding['authorityId']:
        raise WorkerError('pending completion belongs to another authority')
    if receipt.get('state') == 'completed':
        if not acknowledged(receipt, {'job': receipt.get('acknowledgment')}):
            raise WorkerError('completed receipt lacks exact acknowledgment')
        gate.acknowledge(receipt_path.stem, receipt['acknowledgment'])
    elif receipt.get('state') == 'completion_pending':
        reservation = binding['reservation']
        if reservation is None:
            # A persisted exact ACK tombstone is the only legal idle recovery.
            gate.acknowledge(receipt_path.stem, receipt.get('acknowledgment'))
        else:
            gate.bind(receipt_path.stem, receipt['job'])
            gate.prepare_completion(receipt_path.stem, receipt['payload'])
        complete(receipt_path, receipt, token, delivery_wait)
        gate.acknowledge(receipt_path.stem, receipt['acknowledgment'])
    else:
        raise WorkerError('receipt is not eligible for completion-only recovery')
    return 0 if receipt['payload']['status'] == 'succeeded' else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--coordinator', required=True)
    parser.add_argument('--verified-origin', help='Exact independently verified non-loopback origin')
    parser.add_argument('--image', required=True, help='Locally built sha256 image ID')
    parser.add_argument('--scratch', required=True, help='Existing approved private scratch root')
    parser.add_argument('--worker-id', required=True)
    parser.add_argument('--node-id', default='cluster', help='Exact coordinator node identity')
    parser.add_argument('--docker-engine-id', required=True, help='Exact local Docker Engine ID')
    parser.add_argument('--node-generation', required=True, type=int, help='Provisioned node ownership generation')
    parser.add_argument('--timeout', type=int, choices=range(180, 301), default=240)
    parser.add_argument('--resume', help='Resend only an exact pending completion receipt')
    parser.add_argument('--delivery-wait-seconds', type=int, choices=range(-1, 86401), default=-1,
                        help='Completion retry window: -1 until acknowledged, 0 single due attempt')
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
