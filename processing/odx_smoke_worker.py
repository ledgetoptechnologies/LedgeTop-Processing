"""One-shot, staging-only NodeODX photo smoke test. Host/runtime: stdlib only.

Use only with the dedicated disposable engine verified by the operator. Input
photos are read-only; receipts contain claim secrets and must remain private.
An uncertain mutation is never retried and never releases the queue slot.
"""

import argparse
import hashlib
import http.client
import io
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import stat
import sys
import time
from urllib.parse import urlencode, urlsplit
import uuid
import zipfile
from .gateway import gateway_headers
from .options import _contains_secret


PROFILE = 'odx-photo-smoke-v1'
OPTIONS = [{'name': 'end-with', 'value': 'opensfm'},
           {'name': 'feature-quality', 'value': 'low'},
           {'name': 'max-concurrency', 'value': 2}]
OUTPUTS = ['opensfm/reconstruction.json', 'cameras.json', 'task_output.txt']
MAX_INPUT = 512 * 1024 * 1024
MAX_ARCHIVE = 64 * 1024 * 1024
MAX_EXPANDED = 128 * 1024 * 1024
SHA = re.compile(r'[0-9a-f]{64}\Z')
NAME = re.compile(r'[A-Za-z0-9][A-Za-z0-9._ -]{0,180}\.(?:jpg|jpeg)\Z', re.IGNORECASE)


class SmokeError(Exception):
    """Sanitized operator message; never includes response bodies or URLs."""


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def exact_origin(value):
    parsed = urlsplit(value)
    if (parsed.scheme not in ('http', 'https') or not parsed.hostname or parsed.username
            or parsed.password or parsed.path not in ('', '/') or parsed.query or parsed.fragment):
        raise SmokeError('expected exact HTTP(S) origin without credentials or path')
    try:
        parsed.port
    except ValueError:
        raise SmokeError('invalid origin port') from None
    return value.rstrip('/')


class Client:
    """Direct HTTP connections: no redirects, proxies, request logging or retries."""

    def __init__(self, origin, token='', node=False, gateway_secret='', reject_response_secrets=()):
        self.origin = exact_origin(origin)
        self.parsed = urlsplit(self.origin)
        self.token = token
        self.node = node
        self._gateway_secret = gateway_secret
        self._gateway_headers = gateway_headers(self.origin, gateway_secret)
        if (not isinstance(reject_response_secrets, tuple) or
                any(not isinstance(item, str) for item in reject_response_secrets)):
            raise SmokeError('invalid response redaction configuration')
        self._response_secrets = tuple(item.encode('utf-8') for item in
                                       (token, gateway_secret, *reject_response_secrets) if item)
        self._response_secret_text = tuple(item for item in
                                           (token, gateway_secret, *reject_response_secrets) if item)
        if '\r' in token or '\n' in token:
            raise SmokeError('invalid credential')

    def request(self, method, route, body=b'', headers=None, limit=65536,
                file=None, prefix=b'', suffix=b'', expected_hash=None, timeout=30):
        connection = None
        try:
            headers = dict(headers or {})
            if self._gateway_headers:
                if any(str(key).lower() == 'x-ledgetop-gateway' for key in headers):
                    raise SmokeError('gateway header override refused')
                headers.update(self._gateway_headers)
            if self.token:
                if self.node:
                    route += '?' + urlencode({'token': self.token})
                else:
                    headers['Authorization'] = 'Bearer ' + self.token
            kind = http.client.HTTPSConnection if self.parsed.scheme == 'https' else http.client.HTTPConnection
            connection = kind(self.parsed.hostname, self.parsed.port, timeout=timeout)
            if file is None:
                connection.request(method, route, body=body, headers=headers)
            else:
                # Hash the same bytes sent; a changed source aborts before commit.
                with file.open('rb') as stream:
                    st = os.fstat(stream.fileno())
                    if not stat.S_ISREG(st.st_mode) or file.is_symlink():
                        raise SmokeError('photo changed before upload')
                    headers['Content-Length'] = str(len(prefix) + st.st_size + len(suffix))
                    connection.putrequest(method, route)
                    for key, value in headers.items():
                        connection.putheader(key, value)
                    connection.endheaders()
                    connection.send(prefix)
                    digest = hashlib.sha256()
                    sent = 0
                    while chunk := stream.read(1024 * 1024):
                        sent += len(chunk)
                        if sent > st.st_size:
                            raise SmokeError('photo changed during upload')
                        digest.update(chunk)
                        connection.send(chunk)
                    if sent != st.st_size or digest.hexdigest() != expected_hash:
                        raise SmokeError('photo changed during upload')
                    connection.send(suffix)
            response = connection.getresponse()
            if not 200 <= response.status < 300:
                raise SmokeError('HTTP request rejected or redirected; inspect private receipt')
            declared = response.getheader('Content-Length')
            if declared is not None and (int(declared) < 0 or int(declared) > limit):
                raise SmokeError('HTTP response exceeds limit')
            result = response.read(limit + 1)
            if len(result) > limit:
                raise SmokeError('HTTP response exceeds limit')
            if any(secret in result for secret in self._response_secrets):
                raise SmokeError('engine response contains credential')
            return result
        except SmokeError:
            raise
        except Exception:
            raise SmokeError('HTTP response uncertain; mutation must not be retried') from None
        finally:
            if connection:
                connection.close()

    def json(self, method, route, payload=None, *, fields=None, headers=None, timeout=30):
        headers = dict(headers or {})
        if fields is not None:
            boundary = 'smoke-' + uuid.uuid4().hex
            chunks = []
            for name, value in fields.items():
                chunks.append((f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"'
                               f'\r\n\r\n{value}\r\n').encode())
            chunks.append(f'--{boundary}--\r\n'.encode())
            body = b''.join(chunks)
            headers['Content-Type'] = 'multipart/form-data; boundary=' + boundary
        elif payload is not None:
            body = canonical(payload)
            headers['Content-Type'] = 'application/json'
        else:
            body = b''
        raw = self.request(method, route, body, headers, timeout=timeout)
        try:
            value = json.loads(raw)
            if any(_contains_secret(value, secret) for secret in self._response_secret_text):
                raise ValueError()
            if isinstance(value, dict) and 'error' in value:
                raise ValueError()
            if not isinstance(value, (dict, list)):
                raise ValueError()
            return value
        except Exception:
            raise SmokeError('invalid JSON or engine error response; inspect private receipt') from None

    def upload(self, identifier, path, digest):
        boundary = 'smoke-' + uuid.uuid4().hex
        prefix = (f'--{boundary}\r\nContent-Disposition: form-data; name="images"; '
                  f'filename="{path.name}"\r\nContent-Type: image/jpeg\r\n\r\n').encode('ascii')
        suffix = f'\r\n--{boundary}--\r\n'.encode()
        raw = self.request('POST', '/task/new/upload/' + identifier,
                           headers={'Content-Type': 'multipart/form-data; boundary=' + boundary},
                           file=path, prefix=prefix, suffix=suffix, expected_hash=digest)
        try:
            value = json.loads(raw)
            if not isinstance(value, dict) or 'error' in value or value.get('success') is not True:
                raise ValueError()
        except Exception:
            raise SmokeError('upload acknowledgment uncertain; do not retry') from None


def real_directory(path):
    path = Path(path).absolute()
    if not path.is_dir() or path.is_symlink() or path.resolve() != path:
        raise SmokeError('expected an existing real directory')
    return path


def read_manifest(manifest_path, photos):
    manifest_path = Path(manifest_path)
    if manifest_path.is_symlink() or not manifest_path.is_file() or manifest_path.stat().st_size > 65536:
        raise SmokeError('invalid private manifest')
    try:
        manifest = json.loads(manifest_path.read_bytes())
        entries = manifest['files']
        if not isinstance(entries, list) or not 10 <= len(entries) <= 50:
            raise ValueError()
        files = []
        seen = set()
        for entry in entries:
            name, size, digest = entry['name'], entry['bytes'], entry['sha256']
            if (not isinstance(name, str) or not NAME.fullmatch(name) or name.casefold() in seen
                    or type(size) is not int or not 4 <= size <= MAX_INPUT
                    or not isinstance(digest, str) or not SHA.fullmatch(digest)):
                raise ValueError()
            seen.add(name.casefold())
            files.append({'name': name, 'bytes': size, 'sha256': digest})
        if sum(item['bytes'] for item in files) > MAX_INPUT:
            raise ValueError()
        if {path.name for path in photos.iterdir()} != {item['name'] for item in files}:
            raise ValueError()
        for item in files:
            path = photos / item['name']
            if path.is_symlink() or not path.is_file() or path.stat().st_size != item['bytes']:
                raise ValueError()
            with path.open('rb') as stream:
                if stream.read(3) != b'\xff\xd8\xff':
                    raise ValueError()
                stream.seek(0)
                # Ubuntu's main-node host runtime is Python 3.10; file_digest
                # is available only in 3.11+. Keep hashing bounded and streaming.
                hasher = hashlib.sha256()
                for block in iter(lambda: stream.read(1024 * 1024), b''):
                    hasher.update(block)
                digest = hasher.hexdigest()
            if digest != item['sha256']:
                raise ValueError()
        files.sort(key=lambda item: item['name'])
        return files, hashlib.sha256(canonical(files)).hexdigest()
    except Exception:
        raise SmokeError('manifest/photo validation failed before admission') from None


def reconstruction(archive, files):
    """Inspect bounded ZIP contents, never extract paths or preserve the archive."""
    if len(archive) > MAX_ARCHIVE:
        raise SmokeError('archive exceeds limit')
    try:
        with zipfile.ZipFile(io.BytesIO(archive)) as zipped:
            members = zipped.infolist()
            if not 1 <= len(members) <= 512 or sum(item.file_size for item in members) > MAX_EXPANDED:
                raise ValueError()
            seen = set()
            target = None
            for item in members:
                # ZipInfo normalizes backslashes on Windows and truncates NULs.
                name = item.orig_filename
                path = PurePosixPath(name)
                if (not name or '\\' in name or '\x00' in name or ':' in name
                        or path.is_absolute() or '..' in path.parts or name in seen
                        or stat.S_ISLNK(item.external_attr >> 16) or item.flag_bits & 1):
                    raise ValueError()
                seen.add(name)
                if name == 'opensfm/reconstruction.json':
                    target = item
            if target is None or target.is_dir():
                raise ValueError()
            with zipped.open(target) as stream:
                raw = stream.read(MAX_EXPANDED + 1)
            if len(raw) > MAX_EXPANDED:
                raise ValueError()
            components = json.loads(raw)
            if not isinstance(components, list) or not components:
                raise ValueError()
            allowed = {item['name'] for item in files}
            registered, cameras = set(), set()
            point_count = 0
            for component in components:
                if (not isinstance(component, dict)
                        or any(not isinstance(component.get(key), dict)
                               for key in ('shots', 'cameras', 'points'))):
                    raise ValueError()
                if not set(component['shots']) <= allowed:
                    raise ValueError()
                registered.update(component['shots'])
                cameras.update(component['cameras'])
                point_count += len(component['points'])
            if len(registered) < math.ceil(0.8 * len(files)) or not cameras or not point_count:
                raise ValueError()
            return {'components': len(components), 'registeredShots': len(registered),
                    'cameras': len(cameras), 'points': point_count,
                    'archiveSha256': hashlib.sha256(archive).hexdigest()}
    except Exception:
        raise SmokeError('bounded reconstruction validation failed') from None


def save(path, receipt):
    temporary = path.with_suffix('.tmp')
    with os.fdopen(os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), 'wb') as stream:
        stream.write(canonical(receipt))
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def stage(path, receipt, name, **values):
    receipt.update(stage=name, **values)
    save(path, receipt)


def job_fields(job):
    try:
        if (not isinstance(job, dict) or str(uuid.UUID(job['id'])) != job['id']
                or job['environment'] != 'staging' or job['profile'] != PROFILE
                or not isinstance(job['claimToken'], str)
                or not re.fullmatch(r'[A-Za-z0-9_-]{32,128}', job['claimToken'])):
            raise ValueError()
        return {key: job[key] for key in ('id', 'environment', 'profile', 'claimToken')}
    except Exception:
        raise SmokeError('claimed job is not the admitted staging photo profile') from None


def task_list(node):
    listing = node.json('GET', '/task/list')
    if (not isinstance(listing, list) or any(not isinstance(item, dict)
                                           or not isinstance(item.get('uuid'), str) for item in listing)):
        raise SmokeError('invalid task listing; cleanup cannot be verified')
    return listing


def remove_terminal(node, identifier, receipt_path, receipt):
    stage(receipt_path, receipt, 'remove_pending')
    removed = node.json('POST', '/task/remove', {'uuid': identifier})
    if not isinstance(removed, dict) or removed.get('success') is not True:
        raise SmokeError('terminal task removal uncertain')
    if any(item['uuid'] == identifier for item in task_list(node)):
        raise SmokeError('task remains after removal')
    stage(receipt_path, receipt, 'removed', cleanupVerified=True)


def run_engine(node, files, photos, receipt_path, receipt):
    identifier = str(uuid.uuid4())
    stage(receipt_path, receipt, 'init_pending', engineUUID=identifier)
    initialized = node.json('POST', '/task/new/init', fields={
        'name': 'isolated-photo-smoke', 'options': json.dumps(OPTIONS),
        'skipPostProcessing': 'true', 'outputs': json.dumps(OUTPUTS)},
        headers={'set-uuid': identifier})
    if not isinstance(initialized, dict) or initialized.get('uuid') != identifier:
        raise SmokeError('initialization acknowledgment uncertain')
    for index, item in enumerate(files):
        stage(receipt_path, receipt, 'upload_pending', uploadIndex=index)
        node.upload(identifier, photos / item['name'], item['sha256'])
    stage(receipt_path, receipt, 'commit_pending')
    committed = node.json('POST', '/task/new/commit/' + identifier)
    if not isinstance(committed, dict) or committed.get('uuid') != identifier:
        raise SmokeError('commit acknowledgment uncertain')
    stage(receipt_path, receipt, 'polling')
    deadline = time.monotonic() + 600
    while time.monotonic() < deadline:
        info = node.json('GET', '/task/' + identifier + '/info',
                         timeout=max(0.1, min(30, deadline - time.monotonic())))
        code = info.get('status', {}).get('code') if isinstance(info, dict) else None
        if type(code) is not int or code not in (10, 20, 30, 40, 50):
            raise SmokeError('unknown engine task status')
        if code == 50:
            raise SmokeError('canceled task is not proof that processing stopped')
        if code in (30, 40):
            break
        time.sleep(max(0, min(3, deadline - time.monotonic())))
    else:
        raise SmokeError('processing timeout; leave claim running for container shutdown')
    stage(receipt_path, receipt, 'terminal', engineStatus=code)
    if code == 30:
        remove_terminal(node, identifier, receipt_path, receipt)
        return 'failed', {'engineStatus': 30, 'failure': 'engine_terminal_failure'}
    # A completed task may still fail artifact acceptance; remove it either way.
    status, result = 'succeeded', {'engineStatus': 40}
    try:
        if type(info.get('imagesCount')) is not int or info['imagesCount'] != len(files):
            raise SmokeError('completed image count differs from manifest')
        archive = node.request('GET', '/task/' + identifier + '/download/all.zip', limit=MAX_ARCHIVE)
        result.update(reconstruction(archive, files))
        del archive
    except SmokeError:
        status, result = 'failed', {'engineStatus': 40, 'failure': 'artifact_validation_failed'}
    remove_terminal(node, identifier, receipt_path, receipt)
    return status, result


def complete(coordinator, receipt_path, receipt):
    reply = coordinator.json('POST', '/v1/prototype/jobs/' + receipt['job']['id'] + '/complete',
                             receipt['payload'])
    job = reply.get('job') if isinstance(reply, dict) else None
    if (not isinstance(job, dict) or job.get('id') != receipt['job']['id']
            or job.get('status') != receipt['payload']['status']):
        raise SmokeError('completion acknowledgment uncertain; preserve exact receipt')
    stage(receipt_path, receipt, 'completed')


def run(args):
    base = exact_origin(args.coordinator)
    node_origin = exact_origin(args.node_origin)
    if base == node_origin:
        raise SmokeError('coordinator and disposable engine must be separate origins')
    if not re.fullmatch(r'sha256:[0-9a-f]{64}', args.engine_image):
        raise SmokeError('parent-verified pinned engine image ID required')
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,63}', args.worker_id):
        raise SmokeError('invalid worker ID')
    api_token = os.environ.get('PROCESSING_API_TOKEN', '')
    if not api_token:
        raise SmokeError('PROCESSING_API_TOKEN is required')
    photos, work = real_directory(args.photos), real_directory(args.work)
    if photos == work or photos in work.parents or work in photos.parents:
        raise SmokeError('private work and read-only photos must have separate roots')
    receipt_path = work / 'receipt.json'
    lock_path = work / 'worker.lock'
    try:
        descriptor = os.open(lock_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        raise SmokeError('worker lock exists; inspect old process and disposable engine') from None
    os.close(descriptor)
    try:
        coordinator = Client(base, api_token)
        if args.resume_completion:
            receipt = json.loads(receipt_path.read_bytes())
            job_fields(receipt['job'])
            if (receipt['coordinator'] != base or receipt['stage'] != 'completion_pending'
                    or receipt.get('cleanupVerified') is not True):
                raise SmokeError('receipt cannot be safely resumed')
            complete(coordinator, receipt_path, receipt)
            return 0 if receipt['payload']['status'] == 'succeeded' else 1
        if receipt_path.exists():
            raise SmokeError('work already contains a receipt; never repeat an uncertain claim')
        files, digest = read_manifest(args.manifest, photos)
        node_token = os.environ.get('NODEODX_API_TOKEN', '')
        gateway_secret = os.environ.get('NODE_GATEWAY_SECRET', '')
        from .config import GATEWAY_CLIENT_QUALIFIED
        if gateway_secret and GATEWAY_CLIENT_QUALIFIED is not True:
            raise SmokeError('gateway client profile not qualified')
        if gateway_secret and gateway_secret in (api_token, node_token):
            raise SmokeError('gateway credential must be separate')
        node = Client(node_origin, node_token, node=True, gateway_secret=gateway_secret)
        engine = node.json('GET', '/info')
        options = node.json('GET', '/options')
        if (not isinstance(engine, dict) or engine.get('engine') != 'odx'
                or not isinstance(engine.get('version'), str) or len(engine['version']) > 64
                or not isinstance(engine.get('engineVersion'), str) or len(engine['engineVersion']) > 64
                or not isinstance(options, list)
                or not {item['name'] for item in OPTIONS} <= {
                    item.get('name') for item in options if isinstance(item, dict)}):
            raise SmokeError('disposable NodeODX identity or required options not verified')
        if task_list(node):
            raise SmokeError('smoke engine is not empty; do not use an existing engine')
        receipt = {'coordinator': base, 'nodeOrigin': node_origin, 'stage': 'claim_pending',
                   'engineImage': args.engine_image, 'inputDigest': digest}
        save(receipt_path, receipt)
        claimed = coordinator.json('POST', '/v1/prototype/claim',
                                   {'workerId': args.worker_id, 'profiles': [PROFILE]})
        if not isinstance(claimed, dict) or 'job' not in claimed:
            raise SmokeError('claim acknowledgment uncertain')
        if claimed['job'] is None:
            stage(receipt_path, receipt, 'completed', noWork=True)
            print('No eligible photo smoke job.')
            return 0
        stage(receipt_path, receipt, 'claimed', job=claimed['job'])
        job = job_fields(claimed['job'])
        receipt['job'] = job
        status, result = run_engine(node, files, photos, receipt_path, receipt)
        result.update(environment='staging', jobId=job['id'], profile=PROFILE,
                      inputCount=len(files), inputBytes=sum(item['bytes'] for item in files),
                      inputDigest=digest, engineUUID=receipt['engineUUID'],
                      engine='odx', engineImage=args.engine_image,
                      engineVersion=engine['engineVersion'], apiVersion=engine['version'],
                      options=OPTIONS, cleanupVerified=True)
        if len(canonical(result)) > 4096:
            raise SmokeError('completion result exceeds limit')
        payload = {'claimToken': job['claimToken'], 'status': status, 'result': result}
        stage(receipt_path, receipt, 'completion_pending', payload=payload)
        complete(coordinator, receipt_path, receipt)
        print(json.dumps({'status': status, **result}, sort_keys=True))
        return 0 if status == 'succeeded' else 1
    finally:
        lock_path.unlink()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('coordinator', 'node-origin', 'manifest', 'photos', 'work', 'worker-id', 'engine-image'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--resume-completion', action='store_true',
                        help='Resend only the exact saved completion after verified task removal')
    args = parser.parse_args()
    try:
        return run(args)
    except SmokeError as error:
        print(str(error), file=sys.stderr)
        return 2
    except BaseException:
        print('Worker stopped unexpectedly; inspect private receipt and disposable engine.', file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main())
