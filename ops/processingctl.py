"""Local node control commands for the private Processing prototype.

The same wrapper may be installed on cluster and main. Dispatch is bound to the
local Engine and node identity in installation.json; it cannot select a remote
node and run Docker against the wrong host.
"""

import argparse
from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import (HTTPRedirectHandler, ProxyHandler, Request,
                            build_opener)
import uuid


DEFAULT_WORKER_IMAGE = (
    'sha256:f22ac13e70dbbabbc7d59f56d4eca8ec04476e711e2dd96354ba273011b3e029')
NODE_IDS = frozenset(('cluster', 'main'))
SLUG = re.compile(r'[A-Za-z0-9][A-Za-z0-9._-]{0,63}\Z')
ENGINE_ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9:._-]{0,255}\Z')
IMAGE_ID = re.compile(r'sha256:[0-9a-f]{64}\Z')
CHILD_ENV_KEYS = frozenset((
    'PATH', 'HOME', 'USER', 'LOGNAME', 'LANG', 'LC_ALL', 'TMPDIR', 'TMP', 'TEMP',
    'DOCKER_HOST', 'DOCKER_CONTEXT', 'DOCKER_CONFIG', 'DOCKER_TLS_VERIFY',
    'DOCKER_CERT_PATH', 'XDG_STATE_HOME', 'XDG_RUNTIME_DIR'))


class ControlError(Exception):
    """Sanitized private-installation or local-control failure."""


@dataclass(frozen=True)
class Installation:
    root: Path
    origin: str
    api_token: str = field(repr=False)
    operator_token: str = field(repr=False)
    node_id: str
    engine_id: str
    generation: int
    image: str
    source: Path
    scratch: Path
    state: Path
    worker_id: str
    resource_policy: str


def _private_file(path):
    if path.is_symlink() or not path.is_file():
        raise ControlError('private installation file is unavailable')
    if os.name != 'nt':
        info = path.stat()
        if info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise ControlError('private installation file permissions are invalid')


def _private_directory(path):
    if path.is_symlink() or not path.is_dir() or path.resolve() != path:
        raise ControlError('configured private directory is unavailable')
    if os.name != 'nt':
        info = path.stat()
        if info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise ControlError('configured private directory permissions are invalid')


def _contained_path(root, value, default):
    path = Path(value) if value else default
    if not path.is_absolute():
        path = root / path
    try:
        resolved = path.resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, ValueError):
        raise ControlError('configured path must be an existing child of the installation root') from None
    if path.is_symlink():
        raise ControlError('configured path cannot be a symlink')
    return resolved


def _origin(value):
    if not isinstance(value, str):
        raise ControlError('invalid coordinator origin')
    try:
        parsed = urlsplit(value)
        _ = parsed.port
    except ValueError:
        raise ControlError('invalid coordinator origin') from None
    if (parsed.scheme not in ('http', 'https') or parsed.hostname not in
            ('localhost', '127.0.0.1', '::1') or parsed.username or parsed.password
            or parsed.path not in ('', '/') or parsed.query or parsed.fragment):
        raise ControlError('coordinator origin must be an exact loopback origin')
    return value.rstrip('/')


def load_installation(root):
    root = Path(root).absolute()
    if root.is_symlink() or not root.is_dir() or root.resolve() != root:
        raise ControlError('installation root must be a real directory')
    if os.name != 'nt':
        info = root.stat()
        if info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise ControlError('installation root must be private and owned by this account')

    metadata_path = root / 'installation.json'
    env_path = root / 'runtime.env'
    _private_file(metadata_path)
    _private_file(env_path)
    try:
        metadata = json.loads(metadata_path.read_text(encoding='utf-8'))
        env = {}
        for line in env_path.read_text(encoding='utf-8').splitlines():
            if not line or line.lstrip().startswith('#'):
                continue
            key, separator, value = line.partition('=')
            if not separator or key in env:
                raise ValueError()
            env[key] = value
    except (OSError, ValueError, UnicodeError, json.JSONDecodeError):
        raise ControlError('private installation metadata is invalid') from None

    if not isinstance(metadata, dict):
        raise ControlError('private installation metadata is invalid')
    origin = _origin(metadata.get('origin'))
    token = env.get('PROCESSING_API_TOKEN', '')
    if (len(token) < 32 or len(token) > 4096
            or any(ord(char) < 33 or ord(char) > 126 for char in token)):
        raise ControlError('private coordinator credential is invalid')
    operator_token = env.get('PROCESSING_PROTOTYPE_OPERATOR_TOKEN', '')
    if (operator_token and (len(operator_token) < 32 or len(operator_token) > 4096
            or operator_token == token
            or any(ord(char) < 33 or ord(char) > 126 for char in operator_token))):
        raise ControlError('private operator credential is invalid')

    # Older installations predate nodeId and are the cluster-only wrapper.
    node_id = metadata.get('nodeId', 'cluster')
    engine_id = metadata.get('engineId')
    generation = metadata.get('generation')
    image = metadata.get('workerImage', DEFAULT_WORKER_IMAGE)
    if (not isinstance(node_id, str) or node_id not in NODE_IDS
            or not isinstance(engine_id, str)
            or not ENGINE_ID.fullmatch(engine_id) or type(generation) is not int
            or generation < 1 or not isinstance(image, str) or not IMAGE_ID.fullmatch(image)):
        raise ControlError('local node identity or pinned worker image is invalid')

    source = _contained_path(root, metadata.get('sourcePath'), root / 'source')
    dispatcher = source / 'scripts' / 'prototype_dispatcher.py'
    if (not source.is_dir() or dispatcher.is_symlink() or not dispatcher.is_file()
            or dispatcher.resolve() != dispatcher):
        raise ControlError('installed dispatcher source is unavailable')
    scratch = _contained_path(root, metadata.get('scratchPath'), root / 'work')
    state = _contained_path(root, metadata.get('dispatchStatePath'), root / 'dispatch-state')
    if scratch == state or scratch in state.parents or state in scratch.parents:
        raise ControlError('scratch and dispatcher state must be separate')
    _private_directory(scratch)
    _private_directory(state)

    worker_id = metadata.get('workerId', f'{node_id}-synthetic')
    if not isinstance(worker_id, str) or not SLUG.fullmatch(worker_id):
        raise ControlError('invalid local worker identity')
    resource_policy = 'main-unrestricted' if node_id == 'main' else 'bounded-fixture'
    return Installation(root, origin, token, operator_token, node_id, engine_id, generation,
                        image, source, scratch, state, worker_id, resource_policy)


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, newurl):
        return None


def api_request(installation, path, payload=None, *, operator=False):
    token = installation.operator_token if operator else installation.api_token
    if not token:
        raise ControlError('private operator credential is unavailable')
    request = Request(installation.origin + path,
        data=None if payload is None else json.dumps(payload).encode(),
        headers={'Authorization': 'Bearer ' + token,
                 'Content-Type': 'application/json'})
    opener = build_opener(ProxyHandler({}), NoRedirect())
    try:
        with opener.open(request, timeout=10) as response:
            return json.load(response)
    except HTTPError:
        raise ControlError('coordinator request failed') from None


def parser():
    root = Path(__file__).resolve().parent
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument('--root', default=str(root),
                     help='private installation root containing installation.json and runtime.env')
    commands = cli.add_subparsers(dest='command', required=True)
    commands.add_parser('status')
    commands.add_parser('options')
    submit = commands.add_parser('submit-synthetic')
    submit.add_argument('--environment', choices=('production', 'staging'), default='staging')
    submit.add_argument('--request-id', help='Stable unique slug; reuse only for an identical retry')
    cancel = commands.add_parser('cancel-queued')
    cancel.add_argument('--job-id', required=True)
    cancel.add_argument('--note', required=True)
    dispatch = commands.add_parser('dispatch')
    dispatch.add_argument('--max-jobs', type=int, choices=range(1, 11), default=2)
    return cli


def dispatch_command(installation, max_jobs):
    return [sys.executable, str(installation.source / 'scripts' / 'prototype_dispatcher.py'),
        '--coordinator', installation.origin, '--image', installation.image,
        '--scratch', str(installation.scratch), '--state', str(installation.state),
        '--worker-id', installation.worker_id, '--docker-engine-id', installation.engine_id,
        '--node-generation', str(installation.generation), '--max-jobs', str(max_jobs),
        '--node-id', installation.node_id, '--resource-policy', installation.resource_policy]


def child_environment(installation, parent):
    environment = {key: value for key, value in parent.items() if key in CHILD_ENV_KEYS}
    environment['PROCESSING_API_TOKEN'] = installation.api_token
    return environment


def run(args, *, process_runner=subprocess.run):
    root = Path(args.root).absolute()
    installation = load_installation(root)
    if args.command == 'status':
        jobs = api_request(installation, '/v1/prototype/jobs')['jobs']
        nodes = api_request(installation, '/v1/prototype/nodes')['nodes']
        health = api_request(installation, '/healthz')['status']
        authority = api_request(installation, '/v1/prototype/authority')['authorityId']
        print(json.dumps({'health': health, 'authorityId': authority,
            'jobCounts': {state: sum(job['status'] == state for job in jobs)
                          for state in ('queued', 'running', 'held', 'succeeded', 'failed', 'cancelled')},
            'nodes': [{key: node[key] for key in ('id', 'state')} for node in nodes]}))
    elif args.command == 'options':
        discovered = api_request(installation, '/v1/options')
        print(json.dumps({'mode': discovered['mode'], 'ready': discovered['ready'],
            'viewerProviderReady': discovered['viewerProviderReady'],
            'commonOptionCount': len(discovered['commonOptions']),
            'nodes': [{key: node[key] for key in
                       ('role', 'available', 'engine', 'engineVersion', 'optionsSha256')}
                      | {'optionCount': len(node['options'])} for node in discovered['nodes']]}))
    elif args.command == 'submit-synthetic':
        request_id = args.request_id or 'manual-synthetic-' + uuid.uuid4().hex
        job = api_request(installation, '/v1/prototype/jobs', {
            'requestId': request_id, 'environment': args.environment,
            'profile': 'treedar-synthetic-v1'})['job']
        print(json.dumps({'id': job['id'], 'environment': job['environment'],
                          'status': job['status']}))
    elif args.command == 'cancel-queued':
        try:
            identifier = str(uuid.UUID(args.job_id))
        except (ValueError, AttributeError):
            raise ControlError('job ID must be a UUID') from None
        if identifier != args.job_id or not 1 <= len(args.note) <= 256:
            raise ControlError('invalid queued-job cancellation request')
        result = api_request(installation, f'/v1/prototype/jobs/{identifier}/cancel',
                             {'note': args.note}, operator=True).get('job')
        if (not isinstance(result, dict) or result.get('id') != identifier
                or result.get('status') != 'cancelled'):
            raise ControlError('coordinator did not confirm queued-job cancellation')
        print(json.dumps({'id': result['id'], 'status': result['status']}))
    else:
        environment = child_environment(installation, os.environ)
        result = process_runner(dispatch_command(installation, args.max_jobs),
                                env=environment, check=False)
        return result.returncode
    return 0


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        return run(args)
    except (ControlError, OSError, ValueError, KeyError, TypeError):
        print('Prototype command failed; inspect node installation and private receipts.',
              file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
