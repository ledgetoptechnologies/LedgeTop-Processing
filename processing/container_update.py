"""One planned ODM-family Compose-service update on an exclusively managed node.

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
import stat
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
        self.expected_engine = getattr(args,'expected_engine','odx')
        upload,process = getattr(args,'upload_layout',None),getattr(args,'process_layout',None)
        if (self.expected_engine not in ('odx','odm')
                or (self.expected_engine=='odm' and (upload is None or process is None))
                or upload not in (None,'var-www-tmp-v1') or process not in (None,'node-var-www-v1')):
            raise UpdateError('explicit verified engine and fixed layout required')
        self.base = ['docker', 'compose', '-p', args.project, '-f', str(Path(args.compose_file).resolve())]
        token = getattr(args,'node_token',os.environ.get('NODEODX_API_TOKEN',''))
        if type(token) is not str or len(token)>4096 or not all(0x21<=ord(c)<=0x7e for c in token):
            raise UpdateError('invalid private engine credential')
        gateway_secret = os.environ.get('NODE_GATEWAY_SECRET', '')
        from .config import GATEWAY_CLIENT_QUALIFIED
        if gateway_secret and GATEWAY_CLIENT_QUALIFIED is not True:
            raise UpdateError('gateway client profile not qualified')
        if gateway_secret and gateway_secret == token:
            raise UpdateError('gateway credential must be separate')
        self.node = Client(loopback(args.node_origin), token, node=True, gateway_secret=gateway_secret)

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
        if (not isinstance(info, dict) or info.get('engine') != self.expected_engine
                or type(info.get('taskQueueCount')) is not int or info['taskQueueCount'] != 0
                or tasks != []):
            return False
        # Unfinished uploads are not necessarily registered tasks. Never remove
        # them automatically. The explicitly verified fixed layout is shared by
        # the supported NodeODX and NodeODM deployments.
        return self.uploads_idle(item['Id']) and self.processing_idle(item['Id'])

    def uploads_idle(self, identifier):
        raw = self.docker(['docker', 'exec', identifier, '/bin/ls', '-A', '/var/www/tmp'])
        if raw == '':
            return True
        # NodeODX ships this one-byte tracked placeholder. Do not mistake it
        # for an unfinished upload, or accept arbitrary hidden files/symlinks.
        if raw != '.gitignore\n':
            return False
        path = '/var/www/tmp/.gitignore'
        kind = self.docker(['docker', 'exec', identifier, '/usr/bin/stat', '-c', '%F %s', path])
        return kind == 'regular file 1\n' and self.docker(
            ['docker', 'exec', identifier, '/bin/cat', path]) == '*'

    def processing_idle(self, identifier):
        # Canceled/removed tasks can still have running children. A conservative
        # image-specific allowlist blocks replacement until every engine child
        # has exited. This does not infer idleness from CPU utilization.
        rows = self.docker(['docker', 'top', identifier, '-eo', 'pid,args']).splitlines()
        if not rows or rows[0].split() not in (['PID','COMMAND'], ['PID','ARGS']):
            return False
        rows = rows[1:]
        server, wrapper, init = 0, 0, 0
        seen = set()
        for row in rows:
            parts = row.strip().split(None, 1)
            if len(parts) != 2 or not re.fullmatch(r'[1-9][0-9]{0,9}',parts[0]):
                return False
            if parts[0] in seen or int(parts[0]) <= 0:
                return False
            seen.add(parts[0])
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

    def serving(self, image):
        item = self.inspect()
        if (item['Image'] != image or not item['State']['Running'] or item['State'].get('OOMKilled')
                or item['State'].get('Health', {}).get('Status', 'healthy') != 'healthy'):
            return False
        info = self.node.json('GET', '/info')
        return isinstance(info, dict) and info.get('engine') == self.expected_engine

    def healthy(self, image):
        return self.serving(image) and self.idle()


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
            health_seconds=30, poll_seconds=3, *, expected_plan_id=None, expected_target=None,defer_finish=False,
            on_reserved=None):
    path = root / 'update.json'
    if type(defer_finish) is not bool:
        raise UpdateError('explicit completion policy required')
    if on_reserved is not None and not callable(on_reserved):
        raise UpdateError('trusted reservation callback required')
    if path.exists() or path.is_symlink():
        raise UpdateError('existing update receipt; never replay an uncertain update')
    deadline = time.monotonic() + wait_seconds
    while True:
        node = coordinator.json('GET', '/v1/prototype/nodes/' + node_id)['node']
        if ((expected_plan_id is not None and node.get('planId') != expected_plan_id)
                or (expected_target is not None and node.get('targetImage') != expected_target)):
            raise UpdateError('maintenance plan changed; do not adopt another operation')
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
    receipt.update(stage='reserved', updateToken=begun['updateToken'],reservedRevision=begun['node']['revision'])
    save(path, receipt)
    if not adapter.idle():
        raise UpdateError('engine changed after reservation; leave gate for inspection')
    if on_reserved is not None:
        receipt['stage']='reserved_hook_pending';save(path,receipt)
        # Deliberately outside replacement/rollback handling: an uncertain
        # ingress drain must never recreate either container or finish success.
        on_reserved(dict(nodeId=node_id,planId=receipt['planId'],targetImage=target,
            reservedRevision=receipt['reservedRevision']))
        if not adapter.idle():
            raise UpdateError('engine changed while reserved ingress drained; inspect without replacement')
        receipt.update(stage='reserved_ready',reservationCallbackCompleted=True)
        save(path,receipt)
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
    receipt.update(stage='finalization_pending' if status=='succeeded' and defer_finish else 'finish_pending', status=status, result=result)
    save(path, receipt)
    if status=='succeeded' and defer_finish:
        return {'status':status,'nodeId':node_id,**result,'finalizationRequired':True}
    operator.json('POST', '/v1/prototype/nodes/' + node_id + '/finish',
                  {'planId': receipt['planId'], 'updateToken': receipt['updateToken'],
                   'status': status, 'result': result})
    receipt['stage'] = 'completed'
    save(path, receipt)
    return {'status': status, 'nodeId': node_id, **result}


def deferred_receipt(root, node_id, *, expected_plan_id, expected_target):
    path=Path(root)/'update.json'
    info=path.lstat()
    if (path.is_symlink() or not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_size>65536
            or (os.name!='nt' and (info.st_uid!=os.geteuid() or info.st_mode & 0o077))):
        raise UpdateError('private update receipt required')
    receipt=json.loads(path.read_text())
    if (receipt.get('nodeId')!=node_id or receipt.get('planId')!=expected_plan_id
            or receipt.get('targetImage')!=expected_target or receipt.get('status')!='succeeded'
            or type(expected_target) is not str or not PIN.fullmatch(expected_target)
            or type(receipt.get('oldImage')) is not str or not PIN.fullmatch(receipt['oldImage'])
            or receipt.get('stage') not in ('finalization_pending','finish_pending','completed')
            or receipt.get('result',{}).get('image')!=expected_target
            or receipt['result'].get('healthy') is not True
            or type(receipt.get('reservedRevision')) is not int):
        raise UpdateError('definite owned replacement success required')
    return receipt


def finish_matches(node,receipt,result):
    return (node.get('id')==receipt['nodeId'] and node.get('planId')==receipt['planId']
        and node.get('targetImage')==receipt['targetImage'] and node.get('state')=='active'
        and type(node.get('revision')) is int and node['revision']==receipt['reservedRevision']+1 and node.get('result')==result)


def finalize_deferred(coordinator,operator,node_id,root,*,expected_plan_id,expected_target,result):
    """Finish only a frozen successful operation; never inspect/replace Docker.

    Trusted managed callers must first prove durable Compose pin, exact backend
    health and owned reopened ingress. Replays reconcile only this final CAS.
    """
    receipt=deferred_receipt(root,node_id,expected_plan_id=expected_plan_id,expected_target=expected_target)
    if (type(result) is not dict or set(result)!={'healthy','image','previousImage','composePinUpdateRequired','persistentComposePin','ingress'}
            or result.get('healthy') is not True or result.get('image')!=expected_target
            or result.get('previousImage')!=receipt['oldImage'] or result.get('composePinUpdateRequired') is not False
            or result.get('persistentComposePin')!=expected_target or result.get('ingress')!='open'):
        raise UpdateError('verified managed finalization proof required')
    if receipt.get('finishResult') is not None and receipt['finishResult']!=result:
        raise UpdateError('final completion cannot change')
    node=coordinator.json('GET','/v1/prototype/nodes/'+node_id)['node']
    if not finish_matches(node,receipt,result):
        if (node.get('state')!='updating' or node.get('revision')!=receipt['reservedRevision']
                or node.get('planId')!=receipt['planId'] or node.get('targetImage')!=expected_target):
            raise UpdateError('maintenance plan/revision changed before finalization')
        receipt.update(stage='finish_pending',finishResult=result)
        save(Path(root)/'update.json',receipt)
        try:
            operator.json('POST','/v1/prototype/nodes/'+node_id+'/finish',dict(planId=receipt['planId'],
                updateToken=receipt['updateToken'],status='succeeded',result=result))
        except SmokeError:
            node=coordinator.json('GET','/v1/prototype/nodes/'+node_id)['node']
            if not finish_matches(node,receipt,result):raise
        else:
            node=coordinator.json('GET','/v1/prototype/nodes/'+node_id)['node']
        if not finish_matches(node,receipt,result):
            raise UpdateError('final completion readback unavailable')
    receipt.update(stage='completed',finishResult=result)
    save(Path(root)/'update.json',receipt)
    return {'status':'succeeded','nodeId':node_id,**result}


def main():
    parser = argparse.ArgumentParser(description='Standalone node maintenance is disabled; use the managed workflow only after qualifying its ingress fence.')
    for name in ('coordinator', 'node-id', 'node-origin', 'compose-file', 'project', 'service',
                 'docker-engine-id', 'work'):
        parser.add_argument('--' + name)
    parser.add_argument('--exclusive-admission', action='store_true',
                        help='Legacy flag; does not verify ingress or enable this command')
    parser.add_argument('--node-port', type=int, choices=range(1, 65536), default=3000)
    parser.add_argument('--expected-engine',choices=('odx','odm'),default='odx')
    parser.add_argument('--upload-layout',choices=('var-www-tmp-v1',))
    parser.add_argument('--process-layout',choices=('node-var-www-v1',))
    parser.add_argument('--wait-seconds', type=int, choices=range(0, 86401), default=0)
    parser.add_argument('--health-seconds', type=int, choices=range(1, 121), default=30)
    parser.parse_args()
    # A command-line assertion is not proof that the backend has no direct
    # writers. The managed workflow must close and verify its owned ingress
    # after reservation and repeat physical-idle checks before replacement.
    print('Standalone maintenance is disabled: exclusive admission was not verified. '
          'Use the managed node maintenance workflow after qualifying its ingress fence.')
    return 2


if __name__ == '__main__':
    raise SystemExit(main())
