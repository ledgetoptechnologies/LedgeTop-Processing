"""One planned NodeODX Compose-service update on an exclusively managed node.

Host command only; never add a Docker socket to the coordinator. Images must
already exist locally. Uncertain mutations retain the durable receipt and gate.
"""
import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import time
from urllib.parse import urlsplit

from .odx_smoke_worker import Client, SmokeError, exact_origin


PIN = re.compile(r'sha256:[0-9a-f]{64}\Z')
NAME = re.compile(r'[a-z0-9][a-z0-9_-]{0,62}\Z')


class UpdateError(Exception):
    pass


class Uncertain(UpdateError):
    pass


class PreflightError(UpdateError):
    """No replacement command was issued; recreating the old service is wrong."""


def save(path, value):
    temporary = path.with_suffix('.tmp')
    with os.fdopen(os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as stream:
        json.dump(value, stream, sort_keys=True, allow_nan=False)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    if os.name != 'nt':
        fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


@contextmanager
def lock(root):
    # Keep an explicit lock after interruption. It is a separate process guard;
    # the durable receipt also prevents replay if the lock is removed manually.
    path = root / 'updater.lock'
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        raise UpdateError('updater lock exists; inspect previous execution') from None
    os.close(fd)
    try:
        yield
    finally:
        path.unlink()


def loopback(value):
    value = exact_origin(value)
    if urlsplit(value).hostname not in ('127.0.0.1', '::1', 'localhost'):
        raise UpdateError('host updater requires verified loopback origins')
    return value


class ComposeNode:
    """Only one operator-selected Compose service; no image pulls or pruning."""
    def __init__(self, args, root):
        self.args, self.root = args, root
        self.base = ['docker', 'compose', '-p', args.project, '-f', str(Path(args.compose_file).resolve())]
        self.node = Client(loopback(args.node_origin), os.environ.get('NODEODX_API_TOKEN', ''), node=True)

    def docker(self, command, *, timeout=30):
        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
        except (OSError, subprocess.SubprocessError):
            raise Uncertain('Docker response uncertain; inspect service and receipt') from None
        if result.returncode:
            raise UpdateError('Docker operation failed; private operator inspection required')
        return result.stdout

    def engine(self):
        context = os.environ.get('DOCKER_CONTEXT')
        endpoint = os.environ.get('DOCKER_HOST') if not context else None
        if not endpoint:
            command = ['docker', 'context', 'inspect', '--format', '{{json .Endpoints.docker.Host}}']
            if context:
                command.append(context)
            endpoint = json.loads(self.docker(command).strip())
        if not (endpoint.startswith('unix:///') or endpoint.startswith('npipe:////./pipe/')):
            raise UpdateError('remote Docker contexts are not supported by loopback maintenance')
        identity = self.docker(['docker', 'info', '--format', '{{.ID}}']).strip()
        if identity != self.args.docker_engine_id:
            raise UpdateError('Docker Engine identity does not match operator pin')

    def inspect(self):
        self.engine()
        identifiers = self.docker([*self.base, 'ps', '--all', '--quiet', self.args.service]).split()
        if len(identifiers) != 1:
            raise UpdateError('expected exactly one managed service container')
        item = json.loads(self.docker(['docker', 'inspect', identifiers[0]]))[0]
        labels = item['Config'].get('Labels') or {}
        if (labels.get('com.docker.compose.project') != self.args.project
                or labels.get('com.docker.compose.service') != self.args.service):
            raise UpdateError('managed service ownership mismatch')
        endpoint = urlsplit(self.node.origin)
        expected_port = endpoint.port or (443 if endpoint.scheme == 'https' else 80)
        bindings = item.get('NetworkSettings', {}).get('Ports', {}).get(
            str(getattr(self.args, 'node_port', 3000)) + '/tcp') or []
        if (endpoint.hostname not in ('127.0.0.1', '::1') or len(bindings) != 1
                or bindings[0].get('HostIp') != endpoint.hostname
                or bindings[0].get('HostPort') != str(expected_port)):
            raise UpdateError('probe origin is not the exact private port of the managed container')
        return item

    def image(self, image):
        if not PIN.fullmatch(image):
            raise UpdateError('immutable image ID required')
        actual = self.docker(['docker', 'image', 'inspect', '--format', '{{.Id}}', image]).strip()
        if actual != image:
            raise UpdateError('pinned image must already exist locally')

    def idle(self):
        item = self.inspect()
        if not item['State']['Running']:
            raise UpdateError('engine must be running for independent idle checks')
        info = self.node.json('GET', '/info')
        tasks = self.node.json('GET', '/task/list')
        if (not isinstance(info, dict) or info.get('engine') != 'odx'
                or type(info.get('taskQueueCount')) is not int or info['taskQueueCount'] != 0
                or tasks != []):
            return False
        # Unfinished uploads are not necessarily registered tasks. Never remove
        # them automatically. The fixed NodeODX staging path is part of this adapter.
        raw = self.docker(['docker', 'exec', item['Id'], '/bin/ls', '-A', '/var/www/tmp'])
        return raw == '' and self.processing_idle(item['Id'])

    def processing_idle(self, identifier):
        # Canceled/removed tasks can still have running children. A conservative
        # image-specific allowlist blocks replacement until every engine child
        # has exited. This does not infer idleness from CPU utilization.
        rows = self.docker(['docker', 'top', identifier, '-eo', 'pid,args']).splitlines()[1:]
        server, wrapper, init = 0, 0, 0
        for row in rows:
            parts = row.strip().split(None, 1)
            if len(parts) != 2 or not parts[0].isdigit():
                return False
            value = parts[1]
            if re.match(r'^(?:/usr/bin/)?node /var/www/index\.js(?: |$)', value):
                server += 1
            elif re.match(r'^(?:/bin/)?bash /usr/bin/node /var/www/index\.js(?: |$)', value):
                wrapper += 1
            elif re.match(r'^(?:/sbin/)?docker-init(?: |$)', value):
                init += 1
            else:
                return False
        return server == 1 and wrapper <= 1 and init <= 1

    def prepare(self, image):
        self.engine()
        self.image(image)
        override = self.root / 'image-override.json'
        data = json.dumps({'services': {self.args.service: {'image': image, 'pull_policy': 'never'}}})
        with os.fdopen(os.open(override, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), 'w') as stream:
            stream.write(data)
        # Normal application hooks are outside this adapter's scope.
        configuration = json.loads(self.docker([*self.base, '-f', str(override), 'config', '--format', 'json']))
        service = configuration['services'][self.args.service]
        deployment = service.get('deploy') or {}
        if (service.get('post_start') or service.get('pre_stop') or service.get('scale', 1) != 1
                or deployment.get('replicas', 1) != 1 or deployment.get('mode', 'replicated') != 'replicated'):
            raise UpdateError('service hooks/scaling are not supported by maintenance adapter')
        # Execute the exact resolved configuration we validated. Source edits
        # between validation and 'up' must not introduce a new hook or mount.
        resolved = self.root / 'resolved-compose.json'
        with os.fdopen(os.open(resolved, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), 'w') as stream:
            json.dump(configuration, stream)
            stream.flush()
            os.fsync(stream.fileno())
        return ['docker', 'compose', '-p', self.args.project, '-f', str(resolved),
                'up', '-d', '--no-deps', '--no-build',
                '--pull', 'never', '--force-recreate', self.args.service]

    def replace(self, image):
        try:
            command = self.prepare(image)
        except Uncertain:
            raise
        except UpdateError:
            raise PreflightError('replacement validation failed before container mutation') from None
        self.docker(command, timeout=120)

    def healthy(self, image):
        item = self.inspect()
        if (item['Image'] != image or not item['State']['Running'] or item['State'].get('OOMKilled')
                or item['State'].get('Health', {}).get('Status', 'healthy') != 'healthy'):
            return False
        info = self.node.json('GET', '/info')
        return isinstance(info, dict) and info.get('engine') == 'odx' and self.idle()


def wait_healthy(adapter, image, seconds):
    deadline = time.monotonic() + seconds
    while True:
        try:
            if adapter.healthy(image):
                return True
        except (UpdateError, SmokeError):
            pass
        if time.monotonic() >= deadline:
            return False
        time.sleep(min(1, max(0, deadline - time.monotonic())))


def execute(coordinator, operator, adapter, node_id, root, wait_seconds=0,
            health_seconds=30, poll_seconds=3):
    path = root / 'update.json'
    if path.exists() or path.is_symlink():
        raise UpdateError('existing update receipt; never replay an uncertain update')
    deadline = time.monotonic() + wait_seconds
    while True:
        node = coordinator.json('GET', '/v1/prototype/nodes/' + node_id)['node']
        if node['state'] not in ('scheduled', 'draining'):
            return {'status': 'not_eligible', 'nodeState': node['state']}
        due = datetime.fromisoformat(node['notBefore'].replace('Z', '+00:00')) <= datetime.now(timezone.utc)
        if node['state'] == 'draining' and due and adapter.idle():
            jobs = coordinator.json('GET', '/v1/prototype/jobs')['jobs']
            if not any(j.get('nodeId') == node_id and j['status'] in ('running', 'held') for j in jobs):
                break
        if time.monotonic() >= deadline:
            return {'status': 'waiting', 'nodeState': node['state']}
        time.sleep(min(poll_seconds, max(0, deadline - time.monotonic())))
    old = adapter.inspect()['Image']
    target = node['targetImage']
    adapter.image(old)
    adapter.image(target)
    # Validate both forward and rollback configuration before reserving the node.
    adapter.prepare(target)
    adapter.prepare(old)
    receipt = {'stage': 'begin_pending', 'nodeId': node_id, 'planId': node['planId'],
               'oldImage': old, 'targetImage': target, 'startedAt': datetime.now(timezone.utc).isoformat()}
    save(path, receipt)
    # Both the queue fence and private intent exist before any container mutation.
    begun = operator.json('POST', '/v1/prototype/nodes/' + node_id + '/begin',
                          {'planId': node['planId'], 'expectedRevision': node['revision']})
    receipt.update(stage='reserved', updateToken=begun['updateToken'])
    save(path, receipt)
    if not adapter.idle():
        raise UpdateError('engine changed after reservation; leave gate for inspection')
    try:
        receipt['stage'] = 'replace_pending'
        save(path, receipt)
        adapter.replace(target)
        if not wait_healthy(adapter, target, health_seconds):
            raise UpdateError('replacement health failed')
        status, result = 'succeeded', {'healthy': True, 'image': target, 'previousImage': old,
                                       'composePinUpdateRequired': True}
    except Uncertain:
        # The Docker daemon may still be acting. Rollback here could race it.
        raise
    except PreflightError:
        status, result = 'failed', {'healthy': False, 'rollbackPerformed': False,
                                    'reason': 'validation_changed_before_mutation', 'previousImage': old}
    except (UpdateError, SmokeError):
        receipt['stage'] = 'rollback_pending'
        save(path, receipt)
        adapter.replace(old)
        restored = wait_healthy(adapter, old, health_seconds)
        status, result = 'failed', {'healthy': False, 'rollbackHealthy': restored, 'restoredImage': old}
    receipt.update(stage='finish_pending', status=status, result=result)
    save(path, receipt)
    operator.json('POST', '/v1/prototype/nodes/' + node_id + '/finish',
                  {'planId': receipt['planId'], 'updateToken': receipt['updateToken'],
                   'status': status, 'result': result})
    receipt['stage'] = 'completed'
    save(path, receipt)
    return {'status': status, 'nodeId': node_id, **result}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('coordinator', 'node-id', 'node-origin', 'compose-file', 'project', 'service',
                 'docker-engine-id', 'work'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--exclusive-admission', action='store_true',
                        help='Attest all engine writers/uploads are fenced through this coordinator')
    parser.add_argument('--node-port', type=int, choices=range(1, 65536), default=3000)
    parser.add_argument('--wait-seconds', type=int, choices=range(0, 86401), default=0)
    parser.add_argument('--health-seconds', type=int, choices=range(1, 121), default=30)
    args = parser.parse_args()
    try:
        root = Path(args.work).absolute()
        compose = Path(args.compose_file).absolute()
        if (not args.exclusive_admission or not root.is_dir() or root.is_symlink()
                or root.resolve() != root or not compose.is_file() or compose.is_symlink()
                or any(not NAME.fullmatch(x) for x in (args.node_id, args.project, args.service))
                or not args.docker_engine_id or len(args.docker_engine_id) > 128):
            raise UpdateError('private paths, exact service identity and exclusive admission attestation required')
        base = loopback(args.coordinator)
        api, secret = os.environ.get('PROCESSING_API_TOKEN', ''), os.environ.get('PROCESSING_PROTOTYPE_OPERATOR_TOKEN', '')
        if not api or not secret or api == secret:
            raise UpdateError('separate worker and maintenance operator credentials required')
        adapter = ComposeNode(args, root)
        with lock(root):
            result = execute(Client(base, api), Client(base, secret), adapter, args.node_id, root,
                             args.wait_seconds, args.health_seconds)
        print(json.dumps(result, sort_keys=True))
        return 0 if result['status'] != 'failed' else 1
    except (UpdateError, SmokeError, OSError, ValueError, KeyError):
        print('Maintenance stopped; inspect private receipt, node gate and exact service. No automatic retry.')
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
