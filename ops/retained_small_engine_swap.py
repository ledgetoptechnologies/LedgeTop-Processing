"""One exact small-node retention swap; inspect is read-only by default.

Prepare seals private inspected configuration, not permission to run it. Apply
requires that preparation's independently reviewed SHA256. No old-image rollback
or replay is attempted. Main, coordinator and unrelated containers are untouched.
"""
import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from processing.container_update import save
from processing.odx_smoke_worker import Client


BASE = 'sha256:5ce31be03711d280e7f97ee3820dd982b68227181165b3f430b037447c3d13b6'
TARGET = 'sha256:9c9f7ed05e0a8d2e4a370a75dd7b8d8524a0603205e79dfc9046239818237940'
ENGINE = 'ea6ee2936d845cb80bb2a653684e5313fd07e7d6fe3e8583bf99a02171bb922b'
INGRESS = 'a7d70a45f9d8504660b4840e0adf9009ed862a326f0dab81f70f3aa66997509f'
INGRESS_IMAGE = 'sha256:84ed8c0dd296c188bfc24ed506f46589e9285fb0b11cf3a04d91d524d3067e11'
PROJECT = 'lt-migration-1120ae14a8988e84513a4a8a46b78741'
VOLUME = PROJECT + '-data'
DATA = Path('/var/lib/docker/volumes') / VOLUME / '_data'
COMPOSE = Path('/var/lib/ledgetop-node89-migration-1120ae14a8988e84513a4a8a46b78741/work/candidate-compose.json')
NGINX = Path('/home/bkoltz/.local/share/ledgetop-processing-prototype/viewer-e82b795/cluster-node/ingress/public-config/nginx.conf')
PASS = b'proxy_pass http://127.0.0.1:13001;'
FLAG = '--disable_scheduled_cleanup'
ENVIRONMENT_LABEL = 'com.docker.compose.project.environment_file'
SOURCE_HASHES = {
    'config.js': 'cc422dceebea33876082c17084e165e40bd2f389df63f0e3afe3ba37e3edf2a5',
    'libs/TaskManager.js': '326639d70c12cf62acf979633937861ff929da7eda9d3805b9bf4f43af2ea2c5',
}
RESERVE = 8 * 1024 ** 3
# This exact base exported 6,365,878,272 bytes on the qualified daemon, whose
# image Size reported only 3,952,627,133. Do not treat image accounting as tar
# capacity. This fixed-image backup budget is unrelated to job eligibility.
BASE_EXPORT_BUDGET = 8 * 1024 ** 3


class Held(RuntimeError):
    """Redacted admission failure. Private receipts contain recovery detail."""


def require(condition, code):
    if not condition:
        raise Held(code)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def regular(path, private=False):
    require(path.is_absolute() and path.resolve() == path, 'aliased operator file')
    s = path.lstat()
    require(stat.S_ISREG(s.st_mode) and s.st_nlink == 1, 'operator file is not regular')
    require(not s.st_mode & (0o077 if private else 0o022), 'operator file permissions changed')
    return s


def verified_empty_environment(label):
    path = COMPOSE.parent / 'empty.env'
    info = regular(path, private=True)
    require(info.st_uid == 0 and stat.S_IMODE(info.st_mode) == 0o600 and
            info.st_size == 0 and path.read_bytes() == b'', 'empty Compose environment file changed')
    require(label == str(path), 'old Compose environment-file binding changed')
    return path


def command_with_retention(command):
    require(isinstance(command, list) and len(command) == 4 and
            command[:3] == ['--max-images', '1000', '--token'] and
            isinstance(command[3], str) and bool(command[3]), 'unexpected engine command')
    return [*command, FLAG]


def require_backup_headroom(free_bytes, writable_bytes, data_bytes):
    require(all(type(value) is int and value >= 0 for value in
                (free_bytes, writable_bytes, data_bytes)), 'invalid retained backup accounting')
    minimum = BASE_EXPORT_BUDGET + writable_bytes + data_bytes + RESERVE
    require(free_bytes > minimum, 'insufficient space for retained backup and reserve')
    return minimum


def host_config_equivalent(before, after):
    """One observed Docker default representation, not general normalization.

    Compose rendering can make OomKillDisable:null explicit false. Both permit
    OOM killing; true is a different policy. Unknown/missing/numeric values are
    refused. Every other field retains exact canonical JSON comparison.
    """
    if type(before) is not dict or type(after) is not dict or set(before) != set(after):
        return False
    if 'OomKillDisable' not in before or 'OomKillDisable' not in after:
        return False
    a, b = before['OomKillDisable'], after['OomKillDisable']
    if any(value is not None and type(value) is not bool for value in (a, b)):
        return False
    if (a is True or b is True) and a is not b:
        return False
    try:
        return canonical({k: v for k, v in before.items() if k != 'OomKillDisable'}) == \
               canonical({k: v for k, v in after.items() if k != 'OomKillDisable'})
    except (ValueError, TypeError):
        return False


def recreation_labels_equivalent(before, after, old_name):
    """Preserve labels except existing bookkeeping and one proven replacement."""
    if type(before) is not dict or type(after) is not dict:
        return False
    replacement = 'com.docker.compose.replace'
    adjusted = dict(after)
    if replacement in adjusted and replacement not in before:
        if (type(old_name) is not str or not old_name.lstrip('/') or
                type(adjusted[replacement]) is not str or adjusted[replacement] != old_name.lstrip('/')):
            return False
        del adjusted[replacement]
    allowed = {'com.docker.compose.config-hash', 'com.docker.compose.image',
               'com.docker.compose.project.config_files', 'com.ledgetop.retention.commit'}
    try:
        return canonical({k: v for k, v in before.items() if k not in allowed}) == \
               canonical({k: v for k, v in adjusted.items() if k not in allowed})
    except (ValueError, TypeError):
        return False


def candidate_config(current, command):
    candidate = json.loads(json.dumps(current))
    require(set(candidate.get('services', {})) == {'nodeodm'}, 'unexpected Compose services')
    service = candidate['services']['nodeodm']
    require(service.get('command') == command and not service.get('build') and
            not service.get('post_start') and not service.get('pre_stop') and
            service.get('scale', 1) == 1 and
            service.get('deploy', {}).get('replicas', 1) == 1, 'unsafe Compose shape')
    service['image'] = TARGET
    service['command'] = command_with_retention(command)
    require(b'$' not in canonical(candidate), 'unresolved Compose interpolation')
    return candidate


def terminal_inventory(tasks, get_info):
    require(isinstance(tasks, list), 'invalid engine inventory')
    found = {}
    for task in tasks:
        identifier = task.get('uuid') if isinstance(task, dict) else None
        require(isinstance(identifier, str) and str(uuid.UUID(identifier)) == identifier and
                identifier not in found, 'invalid engine task identity')
        info = get_info(identifier)
        code = info.get('status', {}).get('code') if isinstance(info, dict) else None
        require(code in (30, 40, 50), 'engine has queued running or initializing task')
        found[identifier] = code
    return found


def no_cluster_claims(jobs):
    require(isinstance(jobs, list), 'invalid coordinator claims')
    require(not any(j.get('nodeId') == 'cluster' and j.get('status') in
                    ('queued', 'running', 'held') for j in jobs), 'cluster owns unfinished claim')


def verify_owned_receipt(receipt, request):
    body = {k: v for k, v in receipt.items() if k != 'receiptSha256'}
    require(receipt.get('receiptSha256') == digest(canonical(body)) and
            receipt.get('requestSha256') == digest(canonical({'nodeId': 'cluster', 'request': request})),
            'owned schedule receipt digest changed')
    for key in ('schemaVersion', 'requestId', 'authorityId', 'expectedRevision',
                'drainAt', 'notBefore', 'afterJobId', 'targetImage'):
        require(receipt.get(key) == request[key], 'owned schedule receipt differs from intent')
    require(receipt.get('nodeId') == 'cluster' and receipt.get('state') == 'draining' and
            str(uuid.UUID(receipt['planId'])) == receipt['planId'], 'owned schedule identity changed')


def snapshot_tree(root, *, deadline):
    """Private inventory; no symlink following or crossing nested filesystems."""
    device, result = root.stat().st_dev, {}
    for directory, dirs, files in os.walk(root, followlinks=False):
        for name in sorted(dirs + files):
            require(time.monotonic() < deadline, 'retained inventory deadline')
            p = Path(directory) / name
            s = p.lstat()
            require(s.st_dev == device, 'unexpected nested data filesystem')
            key = p.relative_to(root).as_posix()
            row = {'mode': stat.S_IMODE(s.st_mode), 'uid': s.st_uid, 'gid': s.st_gid}
            if stat.S_ISLNK(s.st_mode):
                row.update(type='link', target=os.readlink(p))
            elif stat.S_ISDIR(s.st_mode):
                row['type'] = 'directory'
            elif stat.S_ISREG(s.st_mode):
                sha = hashlib.sha256()
                with p.open('rb') as stream:
                    while block := stream.read(1024 * 1024):
                        require(time.monotonic() < deadline, 'retained inventory deadline')
                        sha.update(block)
                row.update(type='file', bytes=s.st_size, sha256=sha.hexdigest())
            else:
                raise Held('unsupported retained file type')
            result[key] = row
    return result


def verify_retained(before, after):
    # NodeODM may serialize its index again; verify its semantic task set apart.
    before = {p: v for p, v in before.items() if p != 'tasks.json'}
    after = {p: v for p, v in after.items() if p != 'tasks.json'}
    require(before == after, 'retained source or result changed')


class Runtime:
    def __init__(self, credentials):
        regular(credentials, private=True)
        env = dict(line.split('=', 1) for line in credentials.read_text().splitlines() if '=' in line)
        worker, operator = env['PROCESSING_API_TOKEN'], env['PROCESSING_PROTOTYPE_OPERATOR_TOKEN']
        require(worker and operator and worker != operator, 'distinct coordinator credentials required')
        self.api = Client('http://127.0.0.1:18080', worker)
        self.operator = Client(self.api.origin, operator)

    def run(self, args, *, timeout=30, output=None, discard=False):
        with output.open('wb') if output else open(os.devnull, 'wb') as sink:
            p = subprocess.run(args, stdout=sink if output or discard else subprocess.PIPE,
                               stderr=subprocess.PIPE, timeout=timeout)
        require(p.returncode == 0, 'bounded host command failed')
        return p.stdout.decode() if output is None and not discard else ''

    def docker(self, *args, **kw):
        return self.run(['docker', '--host', 'unix:///var/run/docker.sock', *args], **kw)

    def inspect(self, identifier):
        rows = json.loads(self.docker('inspect', identifier))
        require(len(rows) == 1, 'ambiguous container identity')
        return rows[0]

    def physical_host(self):
        require(os.name == 'posix' and os.geteuid() == 0, 'root Linux host required')
        addresses = json.loads(self.run(['ip', '-j', 'address', 'show']))
        require(any(a.get('local') == '192.168.50.89' for n in addresses
                    for a in n.get('addr_info', [])), 'wrong physical host')

    def node(self):
        return self.api.json('GET', '/v1/prototype/nodes/cluster')['node']

    def authority(self):
        return self.api.json('GET', '/v1/prototype/authority')['authorityId']

    def claims(self):
        return json.loads(self.api.request('GET', '/v1/prototype/jobs', limit=16 * 1024 ** 2))['jobs']

    def engine_client(self, command):
        return Client('http://127.0.0.1:13001', command[3], node=True)

    def engine_state(self, command):
        client = self.engine_client(command)
        info = client.json('GET', '/info')
        require(info.get('engine') == 'odm' and info.get('maxImages') == 1000 and
                type(info.get('taskQueueCount')) is int and info['taskQueueCount'] == 0,
                'small engine is not independently idle')
        tasks = terminal_inventory(client.json('GET', '/task/list'),
                                   lambda i: client.json('GET', '/task/' + i + '/info'))
        options = client.json('GET', '/options')
        require(isinstance(options, list) and len(options) > 10, 'engine options unavailable')
        return {'tasks': tasks, 'optionsSha256': digest(canonical(options)),
                'version': info.get('version'), 'engineVersion': info.get('engineVersion')}

    def engine_idle(self, identifier, command):
        state = self.engine_state(command)
        processes = self.docker('top', identifier, '-eo', 'pid,args').splitlines()[1:]
        require(len(processes) == 1 and re.fullmatch(
            r'\s*\d+\s+(?:/usr/bin/)?node /var/www/index\.js(?: .*)?', processes[0]),
            'unexpected engine child process')
        entries = self.docker('exec', identifier, '/bin/ls', '-A', '/var/www/tmp')
        require(entries in ('', '.gitignore\n'), 'unfinished upload or temporary state')
        if entries:
            require(self.docker('exec', identifier, '/usr/bin/stat', '-c', '%F %s', '/var/www/tmp/.gitignore') ==
                    'regular file 1\n' and self.docker('exec', identifier, '/bin/cat', '/var/www/tmp/.gitignore') == '*',
                    'unexpected upload placeholder')
        return state

    def ingress_inspect(self):
        c = self.inspect(INGRESS)
        require(c['Id'] == INGRESS and c['Image'] == INGRESS_IMAGE and
                c['State']['Running'] and c['HostConfig']['NetworkMode'] == 'host' and
                c['Config']['Entrypoint'] == ['nginx'] and
                c['Config']['Cmd'] == ['-c', '/config/nginx.conf', '-g', 'daemon off;'],
                'dedicated ingress identity changed')
        require(len(c['Mounts']) == 1 and c['Mounts'][0]['Type'] == 'bind' and
                c['Mounts'][0]['Destination'] == '/config' and not c['Mounts'][0]['RW'] and
                c['Mounts'][0]['Source'] == str(NGINX.parent), 'dedicated ingress mount changed')
        return c

    def nginx_workers(self):
        c = self.ingress_inspect()
        rows = self.docker('top', INGRESS, '-eo', 'pid,comm').splitlines()[1:]
        found = set()
        for row in rows:
            parts = row.split()
            require(len(parts) == 2 and parts[0].isdigit() and parts[1] == 'nginx', 'unknown ingress process')
            found.add(int(parts[0]))
        require(c['State']['Pid'] in found and len(found) > 1, 'ingress workers unavailable')
        return c['State']['Pid'], found - {c['State']['Pid']}

    def change_ingress(self, prepared, *, closed, recover_closed_intent=False):
        old = bytes.fromhex(prepared['nginxHex'])
        shutting = old.replace(PASS, b'return 503;')
        expected, replacement = (old, shutting) if closed else (shutting, old)
        regular(NGINX)
        actual = NGINX.read_bytes()
        require(actual == expected or (closed and recover_closed_intent and actual == shutting),
                'ingress source changed; do not adopt')
        master, workers = self.nginx_workers()
        metadata = NGINX.stat()
        temporary = NGINX.with_name('nginx.conf.retention-' + prepared['requestId'])
        fd = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, stat.S_IMODE(metadata.st_mode))
        with os.fdopen(fd, 'wb') as stream:
            stream.write(replacement); stream.flush()
            os.fchown(stream.fileno(), metadata.st_uid, metadata.st_gid)
            os.fchmod(stream.fileno(), stat.S_IMODE(metadata.st_mode))
            os.fsync(stream.fileno())
        os.replace(temporary, NGINX)
        fd = os.open(NGINX.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        self.docker('exec', INGRESS, 'nginx', '-t', '-c', '/config/nginx.conf')
        self.docker('kill', '--signal', 'HUP', INGRESS)
        deadline = time.monotonic() + 90
        client = self.engine_client(prepared['command'])
        while time.monotonic() < deadline:
            current_master, current = self.nginx_workers()
            require(current_master == master, 'ingress changed while draining')
            try:
                raw = Client('http://127.0.0.1:3001', client.token, node=True).request('GET', '/info', timeout=3)
                public = json.loads(raw)
                observed = not closed and public.get('engine') == 'odm' and public.get('maxImages') == 1000
            except Exception:
                # Client redacts status; curl below independently verifies 503.
                observed = False
            if closed:
                status = self.run(['curl', '--silent', '--show-error', '--max-time', '3', '--output',
                                   '/dev/null', '--write-out', '%{http_code}', 'http://127.0.0.1:3001/info'])
                observed = status.strip() == '503'
            if observed and not workers.intersection(current):
                return
            time.sleep(.25)
        raise Held('ingress drain or reopen unverified')

    def exclusive_volume(self, own):
        identifiers = self.docker('ps', '--quiet').split()
        for identifier in identifiers:
            c = self.inspect(identifier)
            if c['Id'] == own:
                continue
            for mount in c['Mounts']:
                source = mount.get('Source', '')
                if mount.get('RW') and source.startswith('/'):
                    require(os.path.commonpath([source, DATA]) not in (source, str(DATA)),
                            'another writable retained-volume user exists')

    def revalidate(self, prepared):
        self.physical_host()
        verified_empty_environment(prepared['oldContainer']['Config']['Labels'].get(ENVIRONMENT_LABEL))
        require(self.docker('info', '--format', '{{.ID}}').strip() == prepared['dockerEngineId'],
                'physical Docker Engine changed')
        require(self.authority() == prepared['authorityId'], 'coordinator authority changed')
        require(digest(COMPOSE.read_bytes()) == prepared['composeSha256'], 'authoritative Compose changed')
        self.ingress_inspect()
        old = prepared['oldContainer']
        c = self.inspect(ENGINE)
        require(c['Id'] == ENGINE and c['Image'] == BASE and c['State']['Running'], 'old engine changed')
        for key in ('Config', 'HostConfig', 'Mounts'):
            require(c[key] == old[key], 'old engine configuration changed')
        self.exclusive_volume(ENGINE)
        volume = json.loads(self.docker('volume', 'inspect', VOLUME))[0]
        require(volume['Driver'] == 'local' and volume['Mountpoint'] == str(DATA) and
                not volume.get('Options') and DATA.resolve() == DATA, 'volume identity or driver changed')

    def stop_and_backup(self, prepared, root):
        # Reserve enough for Docker's full filesystem export plus data copy.
        base = json.loads(self.docker('image', 'inspect', BASE))[0]
        require(base['Id'] == BASE, 'pinned base image identity changed')
        size = json.loads(self.docker('inspect', '--size', ENGINE))[0].get('SizeRw')
        require(type(size) is int and size >= 0, 'old writable-layer size unknown')
        data_size = int(self.run(['du', '-sb', str(DATA)]).split()[0])
        require_backup_headroom(shutil.disk_usage(root).free, size, data_size)
        self.docker('stop', '--time', '120', ENGINE, timeout=150)
        stopped = self.inspect(ENGINE)
        require(not stopped['State']['Running'] and stopped['State']['ExitCode'] == 0 and
                not stopped['State']['OOMKilled'], 'old engine did not stop cleanly')
        self.exclusive_volume(ENGINE)
        before = snapshot_tree(DATA, deadline=time.monotonic() + 1800)
        save(root / 'retained-before.json', before)
        saved = json.loads((DATA / 'tasks.json').read_text())
        require({t['uuid']: t['status']['code'] for t in saved} == prepared['engine']['tasks'],
                'stopped task index differs from terminal API inventory')
        self.docker('export', '--output', str(root / 'engine-rootfs.tar'), ENGINE, timeout=1800)
        self.run(['tar', '-tf', str(root / 'engine-rootfs.tar')], timeout=1800, discard=True)
        self.run(['tar', '--numeric-owner', '--sparse', '--one-file-system', '-cpf', str(root / 'data.tar'),
                  '-C', str(DATA), '.'], timeout=3600)
        self.run(['tar', '--compare', '--file', str(root / 'data.tar'), '-C', str(DATA)], timeout=3600)
        # Preserve the unmounted writable tmp directory before Compose removes
        # the old container. Admission already rejected every unfinished upload.
        self.docker('cp', '--archive', ENGINE + ':/var/www/tmp', str(root / 'tmp.before'), timeout=60)
        verify_retained(before, snapshot_tree(DATA, deadline=time.monotonic() + 1800))
        hashes = {}
        for name in ('engine-rootfs.tar', 'data.tar'):
            path = root / name
            sha = hashlib.sha256()
            with path.open('rb') as stream:
                while block := stream.read(1024 * 1024):
                    sha.update(block)
                os.fsync(stream.fileno())
            hashes[name] = {'bytes': path.stat().st_size, 'sha256': sha.hexdigest()}
        save(root / 'backup-verified.json', {'archives': hashes, 'originalContainerId': ENGINE,
                                             'retainedEntries': len(before), 'restoreAutomatically': False})
        fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def recreate_and_verify(self, prepared, root):
        empty_env = verified_empty_environment(prepared['oldContainer']['Config']['Labels'].get(ENVIRONMENT_LABEL))
        require((root / 'backup-verified.json').is_file(), 'verified backup required before recreation')
        self.exclusive_volume(ENGINE)
        require(not self.inspect(ENGINE)['State']['Running'], 'old engine restarted unexpectedly')
        require(NGINX.read_bytes() == bytes.fromhex(prepared['nginxHex']).replace(PASS, b'return 503;'),
                'closed ingress source changed')
        config = root / 'candidate.json'
        save(config, prepared['candidate'])
        compose = ['compose', '-p', PROJECT, '--project-directory', str(COMPOSE.parent),
                   '--env-file', str(empty_env), '-f', str(config)]
        require(json.loads(self.docker(*compose, 'config', '--format', 'json')) == prepared['candidate'],
                'resolved candidate changed')
        self.docker(*compose, 'up', '--no-start', '--no-deps', '--no-build', '--pull', 'never',
                    '--force-recreate', 'nodeodm', timeout=150)
        identifiers = self.docker(*compose, 'ps', '--all', '--quiet', 'nodeodm').split()
        require(len(identifiers) == 1, 'candidate service identity ambiguous')
        identifier = identifiers[0]
        c = self.inspect(identifier)
        old = prepared['oldContainer']
        require(c['Image'] == TARGET and c['State']['Status'] == 'created' and
                c['Name'] == old['Name'] and c['Config']['Cmd'] == command_with_retention(prepared['command']),
                'candidate image or command changed')
        require(host_config_equivalent(old['HostConfig'], c['HostConfig']) and c['Mounts'] == old['Mounts'] and
                set(c['NetworkSettings']['Networks']) == set(old['NetworkSettings']['Networks']),
                'candidate host configuration or volume changed')
        # Docker changes hostname/Compose bookkeeping and the child-image label.
        ignored = {'Hostname', 'Image', 'Cmd', 'Labels'}
        require({k: v for k, v in c['Config'].items() if k not in ignored} ==
                {k: v for k, v in old['Config'].items() if k not in ignored},
                'candidate application configuration changed')
        require(recreation_labels_equivalent(old['Config']['Labels'], c['Config']['Labels'], old['Name']),
                'candidate labels changed outside approved bookkeeping')
        for relative, expected in SOURCE_HASHES.items():
            copied = root / ('candidate-' + relative.replace('/', '-'))
            self.docker('cp', identifier + ':/var/www/' + relative, str(copied))
            require(digest(copied.read_bytes()) == expected, 'candidate source hash changed')
        self.docker('cp', '--archive', str(root / 'tmp.before') + '/.', identifier + ':/var/www/tmp/', timeout=60)
        self.exclusive_volume(identifier)
        self.docker('start', identifier)
        deadline = time.monotonic() + 90
        while True:
            require(self.inspect(identifier)['State']['Running'], 'candidate server exited')
            try:
                after = self.engine_idle(identifier, prepared['command'])
                break
            except Exception:
                require(time.monotonic() < deadline, 'candidate server health deadline')
                time.sleep(1)
        require(after == prepared['engine'], 'terminal tasks or dynamic options changed')
        running = self.inspect(identifier)
        require(all(running['NetworkSettings']['Networks'][name]['NetworkID'] == values['NetworkID']
                    for name, values in old['NetworkSettings']['Networks'].items()), 'physical network identity changed')
        before = json.loads((root / 'retained-before.json').read_text())
        verify_retained(before, snapshot_tree(DATA, deadline=time.monotonic() + 1800))
        saved = json.loads((DATA / 'tasks.json').read_text())
        require({t['uuid']: t['status']['code'] for t in saved} == prepared['engine']['tasks'],
                'candidate task index identity or status changed')
        save(root / 'candidate-verified.json', {'containerId': identifier, 'image': TARGET,
                                                'retainedTerminalTasks': len(after['tasks']),
                                                'retainedFilesVerified': True, 'sourceHashes': SOURCE_HASHES})

    def persist_pin(self, prepared):
        regular(COMPOSE, private=True)
        require(digest(COMPOSE.read_bytes()) == prepared['composeSha256'], 'persistent Compose changed')
        save(COMPOSE, prepared['candidate'])

    def hold_closed(self, prepared, state):
        # Best-effort fail-closed only. Never replay replacement or start old.
        outcome = {'ingressClosedVerified': False, 'nodeHeldVerified': False}
        try:
            # A schedule CAS failure may belong to somebody else's operation.
            # Resolve only this exact historical request; never adopt its plan.
            owned = self.operator.json('GET', '/v1/prototype/nodes/cluster/schedule-owned/' +
                                       prepared['requestId'])['receipt']
            node = self.node()
            require(owned['requestId'] == prepared['requestId'] and
                    owned['authorityId'] == prepared['authorityId'] and owned['targetImage'] == TARGET and
                    node['planId'] == owned['planId'] and node['targetImage'] == TARGET,
                    'failure does not own current maintenance plan')
        except Exception:
            return {**outcome, 'reason': 'own_current_plan_not_proven_no_ingress_mutation'}
        try:
            self.change_ingress(prepared, closed=True, recover_closed_intent=True)
            outcome['ingressClosedVerified'] = True
        except Exception:
            pass
        try:
            node = self.node()
            if node.get('planId') == owned['planId'] and node.get('state') == 'updating':
                self.operator.json('POST', '/v1/prototype/nodes/cluster/hold',
                    dict(planId=node['planId'], expectedRevision=node['revision'],
                         note='Retained-small-engine swap uncertain; private receipt requires inspection.'))
            observed = self.node()
            outcome['nodeHeldVerified'] = observed['planId'] == owned['planId'] and observed['state'] == 'held'
        except Exception:
            pass
        return outcome

    def snapshot(self):
        self.physical_host()
        regular(COMPOSE, private=True); regular(NGINX)
        c = self.inspect(ENGINE)
        empty_env = verified_empty_environment(c['Config']['Labels'].get(ENVIRONMENT_LABEL))
        require(c['Id'] == ENGINE and c['Image'] == BASE and c['State']['Running'] and
                c['Config']['Entrypoint'] == ['/usr/bin/node', '/var/www/index.js'] and
                c['HostConfig']['RestartPolicy']['Name'] == 'unless-stopped', 'old engine binding changed')
        command_with_retention(c['Config']['Cmd'])
        require(c['Config']['Labels'].get('com.docker.compose.project') == PROJECT and
                c['Config']['Labels'].get('com.docker.compose.service') == 'nodeodm' and
                c['Config']['Labels'].get('com.docker.compose.project.working_dir') == str(COMPOSE.parent),
                'old Compose ownership changed')
        require(c['NetworkSettings']['Ports'].get('3000/tcp') == [{'HostIp': '127.0.0.1', 'HostPort': '13001'}] and
                set(c['NetworkSettings']['Networks']) == {'clusterodx_default'}, 'old endpoint changed')
        require(len(c['Mounts']) == 1 and c['Mounts'][0]['Type'] == 'volume' and
                c['Mounts'][0]['Name'] == VOLUME and c['Mounts'][0]['Destination'] == '/var/www/data' and
                c['Mounts'][0]['RW'] and c['Mounts'][0]['Source'] == str(DATA) and DATA.resolve() == DATA,
                'named data volume binding changed')
        self.ingress_inspect()
        nginx = NGINX.read_bytes()
        require(nginx.count(PASS) == 1 and b'3001' in nginx and b'return 503;' not in nginx,
                'dedicated ingress route requires review')
        no_cluster_claims(self.claims())
        node = self.node()
        require(node['state'] == 'active', 'small node already has maintenance')
        current = json.loads(self.docker('compose', '-p', PROJECT, '--env-file', str(empty_env),
                                        '-f', str(COMPOSE), 'config', '--format', 'json'))
        candidate = candidate_config(current, c['Config']['Cmd'])
        target = json.loads(self.docker('image', 'inspect', TARGET))[0]
        require(target['Id'] == TARGET and target['Architecture'] == 'amd64' and
                target['Config']['Labels'].get('com.ledgetop.retention.commit') ==
                '984f8bc4bae76b2a1c854f94d4dfa73bb989f250', 'qualified target image unavailable')
        return {'schemaVersion': 1, 'requestId': str(uuid.uuid4()), 'authorityId': self.authority(),
                'dockerEngineId': self.docker('info', '--format', '{{.ID}}').strip(), 'node': node,
                'oldContainer': c, 'command': c['Config']['Cmd'], 'engine': self.engine_idle(ENGINE, c['Config']['Cmd']),
                'composeSha256': digest(COMPOSE.read_bytes()), 'nginxHex': nginx.hex(),
                'candidate': candidate, 'target': TARGET, 'autoRollback': False}


@contextmanager
def operation_lock(root):
    import fcntl
    with (root / 'operation.lock').open('a+b') as stream:
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def apply(runtime, root, prepared, checkpoint):
    """Explicit one-shot control flow; every uncertain mutation leaves evidence."""
    require(not (root / 'operation.json').exists(), 'operation already started; inspect, never replay')
    require(runtime.authority() == prepared['authorityId'], 'coordinator authority changed')
    node = runtime.node()
    require(node == prepared['node'], 'node changed after preparation')
    stamp = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    state = {'stage': 'schedule_pending', 'requestId': prepared['requestId'], 'target': TARGET}
    checkpoint(state)
    try:
        payload = dict(schemaVersion=1, requestId=prepared['requestId'], authorityId=prepared['authorityId'],
                       expectedRevision=node['revision'], drainAt=stamp, notBefore=stamp, afterJobId=None, targetImage=TARGET)
        owned = runtime.operator.json('POST', '/v1/prototype/nodes/cluster/schedule-owned', payload)['receipt']
        verify_owned_receipt(owned, payload)
        state.update(stage='scheduled', planId=owned['planId']); checkpoint(state)
        runtime.revalidate(prepared)
        runtime.engine_idle(ENGINE, prepared['command'])
        no_cluster_claims(runtime.claims())
        node = runtime.node()
        require(node['planId'] == owned['planId'] and node['state'] == 'draining', 'owned maintenance plan changed')
        state['stage'] = 'begin_pending'; checkpoint(state)
        begun = runtime.operator.json('POST', '/v1/prototype/nodes/cluster/begin',
                                     dict(planId=owned['planId'], expectedRevision=node['revision']))
        require(begun['node']['state'] == 'updating' and begun['node']['planId'] == owned['planId'] and
                begun['node']['targetImage'] == TARGET and isinstance(begun['updateToken'], str) and
                bool(begun['updateToken']), 'maintenance reservation not confirmed')
        state.update(stage='reserved', updateToken=begun['updateToken']); checkpoint(state)
        runtime.change_ingress(prepared, closed=True)
        state['stage'] = 'ingress_closed'; checkpoint(state)
        runtime.engine_idle(ENGINE, prepared['command']); no_cluster_claims(runtime.claims())
        state['stage'] = 'stop_pending'; checkpoint(state)
        runtime.stop_and_backup(prepared, root)
        state['stage'] = 'backup_verified'; checkpoint(state)
        runtime.recreate_and_verify(prepared, root)
        state['stage'] = 'candidate_verified'; checkpoint(state)
        runtime.persist_pin(prepared)
        state['stage'] = 'open_pending'; checkpoint(state)
        runtime.change_ingress(prepared, closed=False)
        state['stage'] = 'finish_pending'; checkpoint(state)
        result = dict(healthy=True, image=TARGET, previousImage=BASE, retainedDataVerified=True, automaticRollback=False)
        finished = runtime.operator.json('POST', '/v1/prototype/nodes/cluster/finish',
                              dict(planId=owned['planId'], updateToken=state['updateToken'], status='succeeded', result=result))['node']
        require(finished['state'] == 'active' and finished['planId'] == owned['planId'] and
                finished['targetImage'] == TARGET and finished['result'] == result, 'maintenance finish not confirmed')
        state['stage'] = 'completed'; checkpoint(state)
        return {'status': 'succeeded', 'image': TARGET, 'retainedTasks': len(prepared['engine']['tasks'])}
    except Exception:
        # Never start the old unpatched image or automatically retry Docker.
        state['stage'] = 'held_for_inspection'
        try:
            checkpoint(state)
        finally:
            state['failureAdmission'] = runtime.hold_closed(prepared, state)
            checkpoint(state)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--credentials', type=Path, required=True)
    parser.add_argument('--receipt', type=Path)
    action = parser.add_mutually_exclusive_group()
    action.add_argument('--prepare', action='store_true')
    action.add_argument('--apply', action='store_true')
    parser.add_argument('--prepared-sha256')
    args = parser.parse_args()
    try:
        runtime = Runtime(args.credentials)
        if args.apply:
            require(args.receipt and args.receipt.resolve() == args.receipt and args.receipt.is_dir() and
                    args.receipt.stat().st_uid == 0 and stat.S_IMODE(args.receipt.stat().st_mode) == 0o700,
                    'root-private prepared receipt required')
            regular(args.receipt / 'prepared.json', private=True)
            prepared = json.loads((args.receipt / 'prepared.json').read_text())
            require(args.prepared_sha256 and digest(canonical(prepared)) == args.prepared_sha256,
                    'independently reviewed preparation digest required')
            require(prepared['target'] == TARGET and prepared['oldContainer']['Id'] == ENGINE and
                    prepared['oldContainer']['Image'] == BASE and prepared['autoRollback'] is False,
                    'prepared operation differs from fixed small-engine pins')
            os.umask(0o077)
            with operation_lock(args.receipt):
                runtime.revalidate(prepared)
                result = apply(runtime, args.receipt, prepared,
                               lambda state: save(args.receipt / 'operation.json', state))
            print(json.dumps(result))
            return 0
        prepared = runtime.snapshot()
        if args.prepare:
            require(args.receipt and args.receipt.is_absolute() and args.receipt.resolve() == args.receipt and
                    not args.receipt.exists(), 'new canonical private receipt required')
            require(os.path.commonpath([args.receipt, DATA]) not in (str(args.receipt), str(DATA)), 'receipt overlaps retained data')
            os.umask(0o077); args.receipt.mkdir(mode=0o700)
            save(args.receipt / 'prepared.json', prepared)
        print(json.dumps({'mode': 'prepared' if args.prepare else 'inspect', 'writes': bool(args.prepare),
                          'preparedSha256': digest(canonical(prepared)), 'image': TARGET,
                          'retainedTerminalTasks': len(prepared['engine']['tasks']),
                          'applyRequiresReviewedDigest': True}))
        return 0
    except Held as error:
        print('Small-node swap held: ' + str(error) + '. No automatic rollback or replay.', file=sys.stderr)
        return 2
    except Exception:
        # Do not expose commands, credentials, client paths, names or log text.
        print('Small-node swap held; inspect reviewed source and private receipt. No automatic rollback or replay.', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
