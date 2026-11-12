"""One fixed main-node retained-terminal swap; inspect is read-only by default.

SOURCE CANDIDATE ONLY: native main-layout qualification is still required before
live use. It refuses active tasks, never rolls back automatically, and does not
change the small-node helper. Caller owns the existing private coordinator tunnel.
"""
import argparse
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
from ops.retained_small_engine_swap import (
    Held, require, canonical, digest, regular, host_config_equivalent,
    application_config_equivalent, recreation_labels_equivalent as small_labels_equivalent,
    snapshot_tree, verify_retained, terminal_inventory, operation_lock,
)

BASE = 'sha256:a53446d53dee7de001f6cdc0dd877fcca7844f8b31e3773f463b915f8c9ac654'
TARGET = 'sha256:d1b3a6af8806b867a96c75ba2b22e35c45fd937217ccb9860c3fcbe6e75499bd'
ENGINE = 'a22ca003c7b5e9a0dafb40e4f5019df22e70dd5b5ce92a40a8224e8cae8d67fc'
INGRESS = '7147630118a3b25c6e84317314d02ff957ff8e5b7cc523036b5fe67e16a60ac0'
INGRESS_IMAGE = 'sha256:84ed8c0dd296c188bfc24ed506f46589e9285fb0b11cf3a04d91d524d3067e11'
PROJECT = 'ledgetop-main-processing'
SERVICE = 'nodeodx'
DATA = Path('/dockerfiles/nodeodm/data')
COMPOSE_ROOT = Path('/home/bkoltz/.local/state/processing-engine-update-20261109')
COMPOSE = COMPOSE_ROOT / 'rollback-compose.json'
OVERRIDE = COMPOSE_ROOT / 'main-image.override.json'
ROLE_COMPOSE = Path('/home/bkoltz/.local/share/ledgetop-processing-node/compose.json')
NGINX = Path('/home/bkoltz/.local/share/ledgetop-processing-node/ingress/public-config/nginx.conf')
PASS = b'proxy_pass http://127.0.0.1:13004;'
FLAG = '--disable_scheduled_cleanup'
ENVIRONMENT_LABEL = 'com.docker.compose.project.environment_file'
PATCH_COMMIT = '984f8bc4bae76b2a1c854f94d4dfa73bb989f250'
FIXTURE_LABEL = 'ledgetop.retention.main-local-fixture'
FIXTURE_VALUE = '607b560b94fee3c8ceb3f2bff78aacf6'
SOURCE_HASHES = {
    'config.js': '602666721a385552e89b4c9fd0868c9e370ef534962443e959868abf047090f1',
    'libs/TaskManager.js': 'd7c78e953bf485309647c11ab81278ed12fb8f69013b576bf09c69057955d9a3',
}
# Conservative fixed-base planning allowance, NOT measured main-export proof.
# Native main-layout qualification must establish the real export fits this.
BASE_EXPORT_BUDGET = 32 * 1024 ** 3
RESERVE = 16 * 1024 ** 3


def command_with_retention(command):
    require(command is None, 'main engine command is not the reviewed null default')
    return [FLAG]


def no_main_claims(jobs):
    require(isinstance(jobs, list), 'invalid coordinator claims')
    require(not any(j.get('nodeId') == 'main' and j.get('status') in
                    ('queued', 'running', 'held') for j in jobs), 'main owns unfinished claim')


def verify_owned_receipt(receipt, request):
    body = {k: v for k, v in receipt.items() if k != 'receiptSha256'}
    require(receipt.get('receiptSha256') == digest(canonical(body)) and
            receipt.get('requestSha256') == digest(canonical({'nodeId': 'main', 'request': request})),
            'owned main schedule receipt digest changed')
    for key in ('schemaVersion', 'requestId', 'authorityId', 'expectedRevision',
                'drainAt', 'notBefore', 'afterJobId', 'targetImage'):
        require(receipt.get(key) == request[key], 'owned main schedule differs from intent')
    require(receipt.get('nodeId') == 'main' and receipt.get('state') == 'draining' and
            str(uuid.UUID(receipt['planId'])) == receipt['planId'], 'owned main schedule identity changed')


def require_backup_headroom(free_bytes, writable_bytes, data_bytes):
    require(all(type(value) is int and value >= 0 for value in
                (free_bytes, writable_bytes, data_bytes)), 'invalid retained backup accounting')
    minimum = BASE_EXPORT_BUDGET + writable_bytes + data_bytes + RESERVE
    require(free_bytes > minimum, 'insufficient space for retained main backup and reserve')
    return minimum


def root_identity(path):
    require(path.resolve() == path, 'aliased retained bind root')
    s = path.lstat()
    require(stat.S_ISDIR(s.st_mode), 'retained bind root is not a directory')
    return dict(device=s.st_dev, inode=s.st_ino, uid=s.st_uid, gid=s.st_gid, mode=stat.S_IMODE(s.st_mode))



def verify_main_retained(before, after):
    verify_retained(before, after)
    # JSON spelling/order may change on normal engine startup. Its file kind
    # and numeric ownership/mode must not change alongside semantic task checks.
    for rows in (before, after):
        require(rows.get('tasks.json', {}).get('type') == 'file', 'task index must remain a regular file')
    require(all(before['tasks.json'][k] == after['tasks.json'][k] for k in ('mode', 'uid', 'gid')),
            'task index metadata changed')


def source_record(path):
    s = regular(path, private=True)
    require(s.st_uid == 1000 and stat.S_IMODE(s.st_mode) == 0o600, 'operator Compose ownership changed')
    raw = path.read_bytes()
    require(b'$' not in raw, 'operator Compose interpolation requires separate review')
    return dict(sha256=digest(raw), uid=s.st_uid, gid=s.st_gid, mode=stat.S_IMODE(s.st_mode), value=json.loads(raw))


def verify_no_old_environment(label):
    # Unlike the small node, this actual container has no environment-file label.
    require(label is None, 'old main environment-file binding changed')


def prepare_empty_environment(root):
    path = root / 'empty.env'
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        os.fchmod(stream.fileno(), 0o600); os.fsync(stream.fileno())
    return path


def verified_empty_environment(root):
    path = root / 'empty.env'; s = regular(path, private=True)
    require(s.st_uid == 0 and stat.S_IMODE(s.st_mode) == 0o600 and s.st_size == 0 and path.read_bytes() == b'',
            'new root-private empty Compose environment changed')
    return path


def candidate_config(current, command):
    command_with_retention(command)
    require(set(current) <= {'name', 'services', 'networks'} and
            set(current.get('services', {})) == {'nodeodx', 'ingress'} and
            current.get('name') == PROJECT, 'unexpected main Compose shape')
    service = json.loads(json.dumps(current['services']['nodeodx']))
    require(service.get('command') is None and not service.get('build') and
            not service.get('post_start') and not service.get('pre_stop') and
            not service.get('env_file') and not service.get('environment') and
            service.get('scale', 1) == 1 and service.get('deploy', {}).get('replicas', 1) == 1,
            'unsafe main Compose service')
    service.update(image=TARGET, command=command_with_retention(command), pull_policy='never')
    result = {'name': PROJECT, 'services': {'nodeodx': service}, 'networks': current['networks']}
    require(b'$' not in canonical(result), 'unresolved main Compose interpolation')
    return result


def recreation_labels_equivalent(before, after, old_name, empty_env):
    checked = dict(after)
    if ENVIRONMENT_LABEL in before or checked.pop(ENVIRONMENT_LABEL, None) != str(empty_env):
        return False
    if FIXTURE_LABEL in checked and FIXTURE_LABEL not in before:
        if checked[FIXTURE_LABEL] != FIXTURE_VALUE:
            return False
        del checked[FIXTURE_LABEL]
    return small_labels_equivalent(before, checked, old_name)


def atomic_preserved_json(path, value):
    s = regular(path, private=True)
    temporary = path.with_name(path.name + '.retention-' + uuid.uuid4().hex)
    fd = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, stat.S_IMODE(s.st_mode))
    with os.fdopen(fd, 'wb') as stream:
        stream.write(canonical(value)); stream.flush()
        os.fchown(stream.fileno(), s.st_uid, s.st_gid)
        os.fchmod(stream.fileno(), stat.S_IMODE(s.st_mode)); os.fsync(stream.fileno())
    os.replace(temporary, path)
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)

class Runtime:
    def __init__(self, credentials):
        s = regular(credentials, private=True)
        require(s.st_uid == 0 and stat.S_IMODE(s.st_mode) == 0o600, 'root-private credential copy required')
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
        require(any(a.get('local') == '192.168.50.88' for n in addresses
                    for a in n.get('addr_info', [])), 'wrong physical host')

    def node(self):
        return self.api.json('GET', '/v1/prototype/nodes/main')['node']

    def authority(self):
        return self.api.json('GET', '/v1/prototype/authority')['authorityId']

    def claims(self):
        return json.loads(self.api.request('GET', '/v1/prototype/jobs', limit=16 * 1024 ** 2))['jobs']

    def engine_client(self, command):
        return Client('http://127.0.0.1:13004', '', node=True)

    def engine_state(self, command):
        client = self.engine_client(command)
        info = client.json('GET', '/info')
        require(info.get('engine') == 'odx' and 'maxImages' in info and info['maxImages'] is None and
                type(info.get('taskQueueCount')) is int and info['taskQueueCount'] == 0,
                'main engine is not independently idle')
        tasks = terminal_inventory(client.json('GET', '/task/list'),
                                   lambda i: client.json('GET', '/task/' + i + '/info'))
        options = client.json('GET', '/options')
        require(isinstance(options, list) and len(options) > 10, 'engine options unavailable')
        return {'tasks': tasks, 'optionsSha256': digest(canonical(options)),
                'version': info.get('version'), 'engineVersion': info.get('engineVersion')}

    def engine_idle(self, identifier, command):
        state = self.engine_state(command)
        rows = self.docker('top', identifier, '-eo', 'pid,args').splitlines()[1:]
        server, wrapper, init = 0, 0, 0
        for row in rows:
            parts = row.strip().split(None, 1)
            require(len(parts) == 2 and parts[0].isdigit(), 'unrecognized engine process')
            value = parts[1]
            if re.match(r'^(?:/usr/bin/)?node /var/www/index\.js(?: |$)', value): server += 1
            elif re.match(r'^(?:/bin/)?bash /usr/bin/node /var/www/index\.js(?: |$)', value): wrapper += 1
            elif re.match(r'^(?:/sbin/)?docker-init(?: |$)', value): init += 1
            else: raise Held('main has an engine processing child')
        require(server == 1 and wrapper <= 1 and init <= 1, 'unexpected main server process layout')
        entries = self.docker('exec', identifier, '/bin/ls', '-A', '/var/www/tmp')
        require(entries in ('', '.gitignore\n'), 'unfinished main upload or temporary state')
        if entries:
            require(self.docker('exec', identifier, '/usr/bin/stat', '-c', '%F %s', '/var/www/tmp/.gitignore') ==
                    'regular file 1\n' and self.docker('exec', identifier, '/bin/cat', '/var/www/tmp/.gitignore') == '*',
                    'unexpected main upload placeholder')
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
                raw = Client('http://127.0.0.1:3004', '', node=True).request('GET', '/info', timeout=3)
                public = json.loads(raw)
                observed = not closed and public.get('engine') == 'odx' and 'maxImages' in public and public['maxImages'] is None
            except Exception:
                # Client redacts status; curl below independently verifies 503.
                observed = False
            if closed:
                status = self.run(['curl', '--silent', '--show-error', '--max-time', '3', '--output',
                                   '/dev/null', '--write-out', '%{http_code}', 'http://127.0.0.1:3004/info'])
                observed = status.strip() == '503'
            if observed and not workers.intersection(current):
                return
            time.sleep(.25)
        raise Held('ingress drain or reopen unverified')

    def exclusive_volume(self, own):
        # Bind paths have no Docker volume identity. Seal the real root and reject
        # every active writable container alias plus every nested host mount.
        root_identity(DATA)
        for line in Path('/proc/self/mountinfo').read_text().splitlines():
            mounted = line.split()[4].replace('\\040', ' ').replace('\\011', '\t').replace('\\134', '\\')
            require(not mounted.startswith(str(DATA) + '/'), 'nested retained data mount requires review')
        for identifier in self.docker('ps', '--quiet').split():
            c = self.inspect(identifier)
            if c['Id'] == own: continue
            for mount in c['Mounts']:
                source = mount.get('Source', '')
                if mount.get('RW') and source.startswith('/'):
                    require(os.path.commonpath([source, DATA]) not in (source, str(DATA)),
                            'another writable retained-bind user exists')

    def revalidate(self, prepared):
        self.physical_host()
        require(self.docker('info', '--format', '{{.ID}}').strip() == prepared['dockerEngineId'], 'physical Docker Engine changed')
        require(self.authority() == prepared['authorityId'], 'coordinator authority changed')
        for path in (COMPOSE, OVERRIDE, ROLE_COMPOSE):
            require(source_record(path) == prepared['sourceFiles'][str(path)], 'authoritative main Compose changed')
        self.ingress_inspect()
        c = self.inspect(ENGINE)
        require(c['Id'] == ENGINE and c['Image'] == BASE and c['State']['Running'], 'old main engine changed')
        verify_no_old_environment(c['Config']['Labels'].get(ENVIRONMENT_LABEL))
        for key in ('Config', 'HostConfig', 'Mounts'):
            require(c[key] == prepared['oldContainer'][key], 'old main engine configuration changed')
        self.exclusive_volume(ENGINE)
        require(root_identity(DATA) == prepared['dataRootIdentity'], 'retained bind root identity changed')

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
        self.backup_stopped(prepared, root)

    def backup_stopped(self, prepared, root):
        """Same backup path, independently callable in an owned stopped window.

        This does not schedule, stop, reopen or relax admission. A caller must
        own the stopped window and this method rechecks the original identity.
        """
        stopped = self.inspect(ENGINE)
        require(stopped['Id'] == ENGINE and stopped['Image'] == BASE and
                not stopped['State']['Running'] and stopped['State']['Pid'] == 0 and
                stopped['State']['ExitCode'] == 0 and not stopped['State']['OOMKilled'],
                'clean stopped original required for backup')
        require(json.loads(self.docker('image', 'inspect', BASE))[0]['Id'] == BASE,
                'pinned base image identity changed before stopped backup')
        size = json.loads(self.docker('inspect', '--size', ENGINE))[0].get('SizeRw')
        data_size = int(self.run(['du', '-sb', str(DATA)]).split()[0])
        require_backup_headroom(shutil.disk_usage(root).free, size, data_size)
        self.exclusive_volume(ENGINE)
        before = snapshot_tree(DATA, deadline=time.monotonic() + 1800)
        save(root / 'retained-before.json', before)
        saved = json.loads((DATA / 'tasks.json').read_text())
        require({t['uuid']: t['status']['code'] for t in saved} == prepared['engine']['tasks'],
                'stopped task index differs from terminal API inventory')
        self.docker('export', '--output', str(root / 'engine-rootfs.tar'), ENGINE, timeout=1800)
        require((root / 'engine-rootfs.tar').stat().st_size <= BASE_EXPORT_BUDGET + size,
                'main rootfs export exceeds reviewed planning allowance')
        self.run(['tar', '-tf', str(root / 'engine-rootfs.tar')], timeout=1800, discard=True)
        self.run(['tar', '--acls', '--xattrs', '--numeric-owner', '--sparse', '--one-file-system', '-cpf', str(root / 'data.tar'),
                  '-C', str(DATA), '.'], timeout=3600)
        self.run(['tar', '--acls', '--xattrs', '--compare', '--file', str(root / 'data.tar'), '-C', str(DATA)], timeout=3600)
        # Preserve the unmounted writable tmp directory before Compose removes
        # the old container. Admission already rejected every unfinished upload.
        self.backup_tmp(prepared, root)
        require(root_identity(DATA) == prepared['dataRootIdentity'], 'retained bind root metadata changed')
        verify_main_retained(before, snapshot_tree(DATA, deadline=time.monotonic() + 1800))
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

    def backup_tmp(self, prepared, root):
        self.docker('cp', '--archive', ENGINE + ':/var/www/tmp', str(root / 'tmp.before'), timeout=60)

    def restore_tmp(self, prepared, root, identifier):
        self.docker('cp', '--archive', str(root / 'tmp.before') + '/.', identifier + ':/var/www/tmp/', timeout=60)

    def verify_restored_tmp(self, prepared, root, identifier):
        self.docker('cp', '--archive', identifier + ':/var/www/tmp', str(root / 'tmp.after'), timeout=60)
        before_tmp, after_tmp = root / 'tmp.before', root / 'tmp.after'
        for key in ('st_uid', 'st_gid', 'st_mode'):
            require(getattr(before_tmp.stat(), key) == getattr(after_tmp.stat(), key), 'main temporary root metadata changed')
        require(snapshot_tree(before_tmp, deadline=time.monotonic() + 60) ==
                snapshot_tree(after_tmp, deadline=time.monotonic() + 60), 'main temporary entries changed')

    def recreate_and_verify(self, prepared, root):
        empty_env = verified_empty_environment(root)
        require((root / 'backup-verified.json').is_file(), 'verified backup required before recreation')
        self.exclusive_volume(ENGINE)
        require(not self.inspect(ENGINE)['State']['Running'], 'old engine restarted unexpectedly')
        require(NGINX.read_bytes() == bytes.fromhex(prepared['nginxHex']).replace(PASS, b'return 503;'),
                'closed ingress source changed')
        config = root / 'candidate.json'
        save(config, prepared['candidate'])
        compose = ['compose', '-p', PROJECT, '--project-directory', str(COMPOSE_ROOT),
                   '--env-file', str(empty_env), '-f', str(config)]
        require(json.loads(self.docker(*compose, 'config', '--format', 'json')) == prepared['candidate'],
                'resolved candidate changed')
        self.docker(*compose, 'up', '--no-start', '--no-deps', '--no-build', '--pull', 'never',
                    '--force-recreate', 'nodeodx', timeout=150)
        identifiers = self.docker(*compose, 'ps', '--all', '--quiet', 'nodeodx').split()
        require(len(identifiers) == 1, 'candidate service identity ambiguous')
        identifier = identifiers[0]
        c = self.inspect(identifier)
        # Preserve exact private evidence before any comparison can reject it.
        # This receipt already contains the original full inspect/credentials.
        save(root / 'candidate-inspect.private.json', c)
        old = prepared['oldContainer']
        require(c['Image'] == TARGET and c['State']['Status'] == 'created' and
                c['Name'] == old['Name'] and c['Config']['Cmd'] == command_with_retention(prepared['command']),
                'candidate image or command changed')
        require(host_config_equivalent(old['HostConfig'], c['HostConfig']) and c['Mounts'] == old['Mounts'] and
                set(c['NetworkSettings']['Networks']) == set(old['NetworkSettings']['Networks']),
                'candidate host configuration or volume changed')
        # Docker changes hostname/Compose bookkeeping and the child-image label.
        require(application_config_equivalent(old['Config'], c['Config']),
                'candidate application configuration changed')
        require(recreation_labels_equivalent(old['Config']['Labels'], c['Config']['Labels'], old['Name'], empty_env),
                'candidate labels changed outside approved bookkeeping')
        for relative, expected in SOURCE_HASHES.items():
            copied = root / ('candidate-' + relative.replace('/', '-'))
            self.docker('cp', identifier + ':/var/www/' + relative, str(copied))
            require(digest(copied.read_bytes()) == expected, 'candidate source hash changed')
        self.restore_tmp(prepared, root, identifier)
        self.verify_restored_tmp(prepared, root, identifier)
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
        require(root_identity(DATA) == prepared['dataRootIdentity'], 'retained bind root metadata changed')
        verify_main_retained(before, snapshot_tree(DATA, deadline=time.monotonic() + 1800))
        saved = json.loads((DATA / 'tasks.json').read_text())
        require({t['uuid']: t['status']['code'] for t in saved} == prepared['engine']['tasks'],
                'candidate task index identity or status changed')
        save(root / 'candidate-verified.json', {'containerId': identifier, 'image': TARGET,
                                                'retainedTerminalTasks': len(after['tasks']),
                                                'retainedFilesVerified': True, 'sourceHashes': SOURCE_HASHES})

    def persist_pin(self, prepared):
        # Two known forward entry points must agree; historical rollback source
        # stays byte-for-byte retained. A crash between writes remains held.
        for path in (COMPOSE, OVERRIDE, ROLE_COMPOSE):
            require(source_record(path) == prepared['sourceFiles'][str(path)], 'persistent main Compose changed')
        override = json.loads(json.dumps(prepared['sourceFiles'][str(OVERRIDE)]['value']))
        require(set(override) == {'services'} and set(override['services']) == {'nodeodx'}, 'main override shape changed')
        override['services']['nodeodx'].update(image=TARGET, command=[FLAG])
        role = json.loads(json.dumps(prepared['sourceFiles'][str(ROLE_COMPOSE)]['value']))
        require(role['services']['nodeodx'].get('command') is None, 'main bootstrap command changed')
        role['services']['nodeodx'].update(image=TARGET, command=[FLAG])
        atomic_preserved_json(OVERRIDE, override)
        atomic_preserved_json(ROLE_COMPOSE, role)
        require(json.loads(OVERRIDE.read_text()) == override and json.loads(ROLE_COMPOSE.read_text()) == role,
                'main forward pins not durably confirmed')
        require(source_record(COMPOSE) == prepared['sourceFiles'][str(COMPOSE)], 'historical rollback Compose changed')

    def hold_closed(self, prepared, state):
        # Best-effort fail-closed only. Never replay replacement or start old.
        outcome = {'ingressClosedVerified': False, 'nodeHeldVerified': False}
        try:
            # A schedule CAS failure may belong to somebody else's operation.
            # Resolve only this exact historical request; never adopt its plan.
            owned = self.operator.json('GET', '/v1/prototype/nodes/main/schedule-owned/' +
                                       prepared['requestId'])['receipt']
            node = self.node()
            require(owned['requestId'] == prepared['requestId'] and
                    owned['authorityId'] == prepared['authorityId'] and owned['targetImage'] == TARGET and
                    node['planId'] == owned['planId'] and node['targetImage'] == TARGET,
                    'failure does not own current maintenance plan')
        except Exception:
            return {**outcome, 'reason': 'own_current_plan_not_proven_no_ingress_mutation'}
        try:
            # Owning a drain is not permission to interrupt its existing work.
            # Only a completed begin reservation fences new claims; a claim can
            # have won the precheck/schedule race while the plan is draining.
            require(self.authority() == prepared['authorityId'] and node.get('state') == 'updating'
                    and node.get('revision') == owned['revision'] + 1,
                    'failure_has_no_updating_reservation')
            no_main_claims(self.claims())
            require(self.node() == node, 'failure_reservation_changed')
            no_main_claims(self.claims())
        except Exception:
            return {**outcome, 'reason': 'updating_idle_reservation_not_proven_no_ingress_mutation'}
        try:
            self.change_ingress(prepared, closed=True, recover_closed_intent=True)
            outcome['ingressClosedVerified'] = True
        except Exception:
            pass
        try:
            node = self.node()
            if (node.get('planId') == owned['planId'] and node.get('state') == 'updating'
                    and node.get('targetImage') == TARGET and node.get('revision') == owned['revision'] + 1
                    and self.authority() == prepared['authorityId']):
                no_main_claims(self.claims())
                self.operator.json('POST', '/v1/prototype/nodes/main/hold',
                    dict(planId=node['planId'], expectedRevision=node['revision'],
                         note='Retained-main-engine swap uncertain; private receipt requires inspection.'))
            observed = self.node()
            outcome['nodeHeldVerified'] = (observed['planId'] == owned['planId'] and observed['state'] == 'held'
                and observed['targetImage'] == TARGET and observed['revision'] == owned['revision'] + 2)
        except Exception:
            pass
        return outcome

    def snapshot(self):
        self.physical_host()
        source_files = {str(p): source_record(p) for p in (COMPOSE, OVERRIDE, ROLE_COMPOSE)}
        regular(NGINX)
        c = self.inspect(ENGINE)
        verify_no_old_environment(c['Config']['Labels'].get(ENVIRONMENT_LABEL))
        require(c['Id'] == ENGINE and c['Image'] == BASE and c['State']['Running'] and
                c['Config']['Entrypoint'] == ['/usr/bin/node', '/var/www/index.js'] and
                c['Config']['User'] == '0:0' and c['HostConfig']['RestartPolicy']['Name'] == 'unless-stopped',
                'old main engine binding changed')
        command_with_retention(c['Config']['Cmd'])
        labels = c['Config']['Labels']
        require(labels.get('com.docker.compose.project') == PROJECT and labels.get('com.docker.compose.service') == SERVICE and
                labels.get('com.docker.compose.project.working_dir') == str(COMPOSE_ROOT) and
                labels.get('com.docker.compose.project.config_files') == str(COMPOSE) + ',' + str(OVERRIDE),
                'old main Compose ownership changed')
        require(c['NetworkSettings']['Ports'].get('3000/tcp') == [{'HostIp': '127.0.0.1', 'HostPort': '13004'}] and
                set(c['NetworkSettings']['Networks']) == {'ledgetop-main-processing_default'}, 'old main endpoint changed')
        h = c['HostConfig']
        require(h['Memory'] == 0 and h['NanoCpus'] == 0 and not h['ReadonlyRootfs'] and
                h['DeviceRequests'] == [{'Driver': 'nvidia', 'Count': -1, 'DeviceIDs': None,
                                         'Capabilities': [['gpu']], 'Options': None}], 'main resource/GPU profile changed')
        require(len(c['Mounts']) == 1 and c['Mounts'][0]['Type'] == 'bind' and
                c['Mounts'][0]['Destination'] == '/var/www/data' and c['Mounts'][0]['RW'] and
                c['Mounts'][0]['Source'] == str(DATA), 'main retained bind changed')
        self.ingress_inspect(); nginx = NGINX.read_bytes()
        require(nginx.count(PASS) == 1 and b'3004' in nginx and b'return 503;' not in nginx, 'dedicated main ingress requires review')
        no_main_claims(self.claims()); node = self.node()
        require(node['state'] == 'active', 'main already has maintenance')
        # Refuse busy main BEFORE any data walk, backup, receipt, or Docker stop.
        engine = self.engine_idle(ENGINE, c['Config']['Cmd'])
        self.exclusive_volume(ENGINE)
        current = json.loads(self.docker('compose', '-p', PROJECT, '--project-directory', str(COMPOSE_ROOT),
                                        '--env-file', '/dev/null', '-f', str(COMPOSE), '-f', str(OVERRIDE), 'config', '--format', 'json'))
        candidate = candidate_config(current, c['Config']['Cmd'])
        target = json.loads(self.docker('image', 'inspect', TARGET))[0]
        require(target['Id'] == TARGET and target['Architecture'] == 'amd64' and
                target['Config']['Labels'].get('com.ledgetop.retention.commit') == PATCH_COMMIT and
                target['Config']['Labels'].get(FIXTURE_LABEL) == FIXTURE_VALUE, 'qualified main target unavailable')
        return {'schemaVersion': 1, 'requestId': str(uuid.uuid4()), 'authorityId': self.authority(),
                'dockerEngineId': self.docker('info', '--format', '{{.ID}}').strip(), 'node': node,
                'oldContainer': c, 'command': c['Config']['Cmd'], 'engine': engine,
                'sourceFiles': source_files, 'dataRootIdentity': root_identity(DATA), 'nginxHex': nginx.hex(),
                'candidate': candidate, 'target': TARGET, 'autoRollback': False}


def apply(runtime, root, prepared, checkpoint):
    """Explicit one-shot control flow; every uncertain mutation leaves evidence."""
    require(not (root / 'operation.json').exists(), 'operation already started; inspect, never replay')
    require(runtime.authority() == prepared['authorityId'], 'coordinator authority changed')
    node = runtime.node()
    require(node == prepared['node'], 'node changed after preparation')
    # Refuse an already-busy main before even scheduling this operation. The
    # transactional drain remains necessary for a claim racing this precheck.
    runtime.engine_idle(ENGINE, prepared['command'])
    no_main_claims(runtime.claims())
    stamp = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    state = {'stage': 'schedule_pending', 'requestId': prepared['requestId'], 'target': TARGET}
    checkpoint(state)
    try:
        payload = dict(schemaVersion=1, requestId=prepared['requestId'], authorityId=prepared['authorityId'],
                       expectedRevision=node['revision'], drainAt=stamp, notBefore=stamp, afterJobId=None, targetImage=TARGET)
        owned = runtime.operator.json('POST', '/v1/prototype/nodes/main/schedule-owned', payload)['receipt']
        verify_owned_receipt(owned, payload)
        state.update(stage='scheduled', planId=owned['planId']); checkpoint(state)
        runtime.revalidate(prepared)
        runtime.engine_idle(ENGINE, prepared['command'])
        no_main_claims(runtime.claims())
        node = runtime.node()
        require(node['planId'] == owned['planId'] and node['state'] == 'draining', 'owned maintenance plan changed')
        state['stage'] = 'begin_pending'; checkpoint(state)
        begun = runtime.operator.json('POST', '/v1/prototype/nodes/main/begin',
                                     dict(planId=owned['planId'], expectedRevision=node['revision']))
        require(begun['node']['state'] == 'updating' and begun['node']['planId'] == owned['planId'] and
                begun['node']['targetImage'] == TARGET and isinstance(begun['updateToken'], str) and
                bool(begun['updateToken']), 'maintenance reservation not confirmed')
        state.update(stage='reserved', updateToken=begun['updateToken']); checkpoint(state)
        runtime.change_ingress(prepared, closed=True)
        state['stage'] = 'ingress_closed'; checkpoint(state)
        runtime.engine_idle(ENGINE, prepared['command']); no_main_claims(runtime.claims())
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
        finished = runtime.operator.json('POST', '/v1/prototype/nodes/main/finish',
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
                    'prepared operation differs from fixed main-engine pins')
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
            prepare_empty_environment(args.receipt)
            save(args.receipt / 'prepared.json', prepared)
        print(json.dumps({'mode': 'prepared' if args.prepare else 'inspect', 'writes': bool(args.prepare),
                          'preparedSha256': digest(canonical(prepared)), 'image': TARGET,
                          'retainedTerminalTasks': len(prepared['engine']['tasks']),
                          'applyRequiresReviewedDigest': True}))
        return 0
    except Held as error:
        print('Main-node swap held: ' + str(error) + '. No automatic rollback or replay.', file=sys.stderr)
        return 2
    except Exception:
        # Do not expose commands, credentials, client paths, names or log text.
        print('Main-node swap held; inspect reviewed source and private receipt. No automatic rollback or replay.', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
