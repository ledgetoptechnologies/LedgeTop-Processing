"""Drain at most 1..10 already-queued synthetic TreeDar jobs serially.

This is a bounded foreground command, not a service. Production takes priority
without preempting running work. It never creates jobs or runs photo profiles.
Private durable state must survive host restarts. An unresolved dispatch record
requires operator inspection; this command never discards or retries it.

The existing worker bounds Docker execution. Deliberately no parent subprocess
timeout kills that worker mid-container. If the host/child hangs, stop and inspect
the recorded worker receipt/container manually before resolving state.
"""

from contextlib import contextmanager
import datetime
import http.client
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from urllib.parse import urlsplit
import uuid
import argparse


ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / 'engines/treedar/scripts/prototype_worker.py'
PROFILE = 'treedar-synthetic-v1'
SLUG = re.compile(r'[A-Za-z0-9][A-Za-z0-9._-]{0,63}\Z')
HEX = re.compile(r'[0-9a-f]{64}\Z')


class DispatchError(Exception):
    """Only sanitized failure categories may reach the terminal."""


def origin(value, verified=None):
    parsed = urlsplit(value)
    if (parsed.scheme not in ('http', 'https') or not parsed.hostname
            or parsed.username or parsed.password or parsed.path not in ('', '/')
            or parsed.query or parsed.fragment):
        raise DispatchError('coordinator must be an exact HTTP(S) origin')
    try:
        parsed.port
    except ValueError:
        raise DispatchError('invalid coordinator port') from None
    normalized = value.rstrip('/')
    if parsed.hostname not in ('localhost', '127.0.0.1', '::1') and normalized != verified:
        raise DispatchError('non-loopback coordinator needs matching --verified-origin')
    return normalized


def directory(value):
    path = Path(value).absolute()
    if not path.is_dir() or path.is_symlink() or path.resolve() != path:
        raise DispatchError('use existing real private directories for scratch and state')
    return path


@contextmanager
def process_lock(path):
    if path.is_symlink():
        raise DispatchError('invalid dispatcher lock path')
    stream = path.open('a+b')
    acquired = False
    try:
        if os.name == 'nt':
            import msvcrt
            stream.seek(0, os.SEEK_END)
            if stream.tell() == 0:
                stream.write(b'0')
                stream.flush()
            stream.seek(0)
            try:
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError:
                raise DispatchError('another dispatcher owns this state directory') from None
        else:
            import fcntl
            try:
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                raise DispatchError('another dispatcher owns this state directory') from None
        acquired = True
        yield
    finally:
        if acquired:
            if os.name == 'nt':
                import msvcrt
                stream.seek(0)
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
        stream.close()
        # Never unlink an OS lock file: another process may already hold its inode.


def read_json(path):
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 65536:
        raise DispatchError('invalid private state or receipt file')
    try:
        with path.open('rb') as stream:
            data = stream.read(65537)
        if len(data) > 65536:
            raise ValueError()
        value = json.loads(data)
        if not isinstance(value, dict):
            raise ValueError()
        return value
    except Exception:
        raise DispatchError('private state or receipt needs operator inspection') from None


def save(path, value):
    temporary = path.with_suffix('.tmp')
    if temporary.exists() or temporary.is_symlink():
        raise DispatchError('unfinished state write needs operator inspection')
    with os.fdopen(os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as stream:
        json.dump(value, stream, sort_keys=True, allow_nan=False)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    # Make the rename durable on POSIX as well as the file contents.
    if os.name != 'nt':
        descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def queue(base, token):
    parsed = urlsplit(base)
    connection = None
    try:
        kind = http.client.HTTPSConnection if parsed.scheme == 'https' else http.client.HTTPConnection
        connection = kind(parsed.hostname, parsed.port, timeout=15)
        connection.request('GET', '/v1/prototype/jobs', headers={'Authorization': 'Bearer ' + token})
        response = connection.getresponse()
        if response.status != 200:
            raise ValueError()
        data = response.read(8 * 1024 * 1024 + 1)
        if len(data) > 8 * 1024 * 1024:
            raise ValueError()
        jobs = json.loads(data)['jobs']
        if not isinstance(jobs, list) or len(jobs) > 1000:
            raise ValueError()
        seen = set()
        for job in jobs:
            if (not isinstance(job, dict) or str(uuid.UUID(job['id'])) != job['id']
                    or job['id'] in seen or job['environment'] not in ('production', 'staging')
                    or job['status'] not in ('queued', 'running', 'held', 'succeeded', 'failed')
                    or not isinstance(job['profile'], str) or not SLUG.fullmatch(job['profile'])):
                raise ValueError()
            seen.add(job['id'])
        return jobs
    except Exception:
        raise DispatchError('queue read uncertain; no further worker launched') from None
    finally:
        if connection:
            connection.close()


def select_head(jobs):
    if any(job['status'] == 'held' for job in jobs):
        return None, 'held'
    if any(job['status'] == 'running' for job in jobs):
        return None, 'running'
    pending = [(index, job) for index, job in enumerate(jobs) if job['status'] == 'queued']
    if not pending:
        return None, 'empty'
    _, head = min(pending, key=lambda pair: (pair[1]['environment'] != 'production', pair[0]))
    if head['profile'] != PROFILE:
        return None, 'unsupported_head'
    return head, None


def receipts(scratch):
    root = scratch / '.prototype-receipts'
    if not root.exists():
        if root.is_symlink():
            raise DispatchError('invalid worker receipt directory')
        return {}
    if not root.is_dir() or root.is_symlink() or root.resolve().parent != scratch:
        raise DispatchError('invalid worker receipt directory')
    paths = list(root.iterdir())
    if len(paths) > 4096:
        raise DispatchError('worker receipt history needs operator inspection')
    result = {}
    for path in paths:
        if path.name == 'worker.lock':
            raise DispatchError('worker lock exists; inspect worker before dispatching')
        if not re.fullmatch('[0-9a-f]{32}\\.json', path.name):
            raise DispatchError('unfinished or unexpected worker receipt needs inspection')
        receipt = read_json(path)
        if receipt.get('state') != 'completed':
            raise DispatchError('unresolved worker receipt; operator inspection required')
        result[path.name] = receipt
    return result


def verify_image(image):
    try:
        check = subprocess.run(['docker', 'image', 'inspect', '--format', '{{.Id}}', image],
                               capture_output=True, text=True, timeout=15)
        if check.returncode or check.stdout.strip() != image:
            raise ValueError()
    except Exception:
        raise DispatchError('pinned image ID must already exist locally') from None


def launch(args, base, scratch):
    command = [sys.executable, str(WORKER), '--coordinator', base, '--image', args.image,
               '--scratch', str(scratch), '--worker-id', args.worker_id,
               '--timeout', str(args.timeout)]
    if args.verified_origin:
        command.extend(['--verified-origin', args.verified_origin])
    environment = {key: value for key, value in os.environ.items()
                   if key.upper() != 'PROCESSING_PROTOTYPE_OPERATOR_TOKEN'}
    # No timeout here: do not kill a worker while its Docker container may run.
    completed = subprocess.run(command, env=environment,
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return completed.returncode


def verify_progress(args, base, expected, before, after, jobs):
    new = set(after) - set(before)
    if set(before) - set(after) or len(new) != 1:
        raise DispatchError('no uniquely verified worker receipt; inspect dispatch')
    receipt_name = new.pop()
    receipt = after[receipt_name]
    job = receipt.get('job')
    payload = receipt.get('payload')
    if (receipt.get('coordinator') != base or receipt.get('workerId') != args.worker_id
            or not isinstance(job, dict) or job.get('id') != expected['id']
            or job.get('environment') != expected['environment'] or job.get('profile') != PROFILE
            or not isinstance(payload, dict) or payload.get('status') != 'succeeded'
            or not isinstance(job.get('claimToken'), str)
            or not re.fullmatch('[A-Za-z0-9_-]{32,128}', job['claimToken'])
            or payload.get('claimToken') != job['claimToken']):
        raise DispatchError('worker receipt does not prove expected job completion')
    finished = next((item for item in jobs if item['id'] == expected['id']), None)
    result = payload.get('result')
    if (not isinstance(result, dict) or result.get('jobId') != expected['id']
            or result.get('environment') != expected['environment'] or result.get('image_id') != args.image
            or type(result.get('n_trees')) is not int or result['n_trees'] != 3
            or not isinstance(result.get('treedar_version'), str)
            or not 1 <= len(result['treedar_version']) <= 64
            or any(not isinstance(result.get(key), str) or not HEX.fullmatch(result[key])
                   for key in ('manifest_sha256', 'outputs_sha256'))
            or not finished or finished.get('status') != 'succeeded'
            or finished.get('workerId') != args.worker_id
            or finished.get('profile') != PROFILE or finished.get('environment') != expected['environment']
            or finished.get('result') != result):
        raise DispatchError('post-run queue does not verify worker progress')
    return receipt_name


def run(args):
    base = origin(args.coordinator, args.verified_origin)
    if (not re.fullmatch('sha256:[0-9a-f]{64}', args.image)
            or not SLUG.fullmatch(args.worker_id) or type(args.max_jobs) is not int
            or not 1 <= args.max_jobs <= 10 or type(args.timeout) is not int
            or not 180 <= args.timeout <= 300):
        raise DispatchError('invalid pinned image, worker ID or bounded limits')
    token = os.environ.get('PROCESSING_API_TOKEN', '')
    if not token or '\r' in token or '\n' in token:
        raise DispatchError('PROCESSING_API_TOKEN is required')
    scratch, state = directory(args.scratch), directory(args.state)
    if scratch == state or scratch in state.parents or state in scratch.parents:
        raise DispatchError('scratch and durable state must be separate directories')
    with process_lock(state / 'dispatcher.lock'):
        paths = list(state.iterdir())
        if len(paths) > 4096:
            raise DispatchError('dispatcher history needs operator inspection')
        for path in paths:
            if path.name == 'dispatcher.lock':
                continue
            if (not re.fullmatch('dispatch-[0-9a-f]{32}\\.json', path.name)
                    or read_json(path).get('state') != 'completed'):
                raise DispatchError('unresolved dispatch state; operator inspection required')
        before = receipts(scratch)
        verify_image(args.image)
        completed = []
        for _ in range(args.max_jobs):
            jobs = queue(base, token)
            expected, reason = select_head(jobs)
            if reason:
                print(json.dumps({'completed': len(completed), 'completedJobIds': completed,
                                  'reason': reason}))
                return 0
            dispatch_path = state / ('dispatch-' + uuid.uuid4().hex + '.json')
            record = {'state': 'launch_pending', 'expectedJobId': expected['id'],
                      'environment': expected['environment'], 'coordinator': base,
                      'workerId': args.worker_id, 'image': args.image, 'scratch': str(scratch),
                      'startedAt': datetime.datetime.now(datetime.timezone.utc).isoformat()}
            save(dispatch_path, record)
            try:
                code = launch(args, base, scratch)
                if code != 0:
                    raise DispatchError('child failed or is uncertain; inspect dispatch and worker')
                after = receipts(scratch)
                post_run = queue(base, token)
                receipt_name = verify_progress(args, base, expected, before, after, post_run)
            except BaseException:
                # Keep the persisted pre-launch record unresolved even after Ctrl+C.
                raise DispatchError('dispatch uncertain; operator inspection required before restart') from None
            record.update(state='completed', receipt=receipt_name)
            save(dispatch_path, record)
            before = after
            completed.append(expected['id'])
        print(json.dumps({'completed': len(completed), 'completedJobIds': completed,
                          'reason': 'max_jobs'}))
        return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('coordinator', 'image', 'scratch', 'state', 'worker-id'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--verified-origin')
    parser.add_argument('--max-jobs', type=int, choices=range(1, 11), default=2)
    parser.add_argument('--timeout', type=int, choices=range(180, 301), default=240)
    args = parser.parse_args()
    try:
        return run(args)
    except DispatchError as error:
        print(str(error), file=sys.stderr)
        return 2
    except BaseException:
        print('Dispatcher interrupted; inspect durable state and worker before restarting.', file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main())
