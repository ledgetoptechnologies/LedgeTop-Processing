"""Default PLAN; explicit prepare/apply under a pre-established main-node fence.

No stop/start/schedule/reopen/pull or original/result removal exists here.
A reviewed controller must own the full idle/stopped window and hold on failure.
"""
import argparse
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace
import stat
import subprocess
import sys
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from processing.intermediate_cleanup import CacheCleanup, CleanupHeld, require, canonical, digest
from processing.odx_smoke_worker import Client
from processing.intermediate_mounts import parse_mountinfo, mount_overlaps, sealed_metrics_observer
from ops.node_ingress import Ingress, configuration

DATA = Path('/dockerfiles/nodeodm/data')
CONTAINER = 'a22ca003c7b5e9a0dafb40e4f5019df22e70dd5b5ce92a40a8224e8cae8d67fc'
IMAGE = 'sha256:a53446d53dee7de001f6cdc0dd877fcca7844f8b31e3773f463b915f8c9ac654'
INGRESS_ROOT = Path('/home/bkoltz/.local/share/ledgetop-processing-node/ingress')
ORIGIN = 'http://127.0.0.1:18080'


def private_bytes(path):
    p = Path(path).absolute()
    require(p.resolve() == p and not p.is_symlink(), 'private_file_alias')
    info = p.stat()
    require(stat.S_ISREG(info.st_mode) and info.st_uid == 0 and info.st_nlink == 1
            and stat.S_IMODE(info.st_mode) == 0o600, 'root_private_file_required')
    fd = os.open(p, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        opened = os.fstat(fd)
        require((opened.st_dev, opened.st_ino) == (info.st_dev, info.st_ino), 'private_file_replaced')
        with os.fdopen(os.dup(fd), 'rb') as f:
            return f.read()
    finally:
        os.close(fd)


def assert_no_external_readers(task_root, *, proc=Path('/proc'), mounts=None):
    prefix = str(task_root) + '/'
    device = task_root.stat().st_dev
    checked = {}
    def aliases_task(target, source_device):
        if source_device != device or mounts is None:
            return False
        target = target.removesuffix(' (deleted)')
        if not target.startswith('/'):
            return False
        key = (target,source_device)
        if key not in checked:
            checked[key] = mount_overlaps(task_root,target,mounts,
                resolve=lambda p:Path(p).resolve(strict=False),
                stat_path=lambda p:task_root.stat() if str(p)==str(task_root)
                    else SimpleNamespace(st_dev=source_device))
        return checked[key]

    for entry in proc.iterdir():
        if not entry.name.isdecimal() or int(entry.name) == os.getpid():
            continue
        try:
            for descriptor in (entry / 'fd').iterdir():
                try:
                    target = os.readlink(descriptor)
                except FileNotFoundError:
                    continue
                require(target != str(task_root) and not target.startswith(prefix), 'external_task_reader_or_writer')
                try:
                    require(not aliases_task(target,descriptor.stat().st_dev), 'external_task_alias_reader_or_writer')
                except FileNotFoundError:
                    continue
            with (entry / 'maps').open(errors='replace') as stream:
                for line in stream:
                    fields = line.split(None, 5)
                    if len(fields) == 6:
                        target = fields[5].rstrip('\n')
                        require(target != str(task_root) and not target.startswith(prefix), 'external_task_mapping')
                        major,minor=(int(x,16) for x in fields[3].split(':'))
                        require(not aliases_task(target,os.makedev(major,minor)), 'external_task_alias_mapping')
        except (FileNotFoundError, ProcessLookupError):
            continue  # The observed process/descriptor exited; no remaining reader.
        except PermissionError:
            raise CleanupHeld('external_reader_census_unavailable') from None


def verify_execution_fence(auth, *, engine_id, authority, node, owned, jobs, job, container):
    """Pure checks used by the real authenticated/stopped-host observer."""
    target=auth.get('maintenanceTargetImage')
    require(type(target) is str and target.startswith('sha256:') and len(target)==71
            and all(c in '0123456789abcdef' for c in target[7:]), 'sealed_maintenance_target_required')
    require(engine_id == auth['engineId'] and authority == {'authorityId':auth['authorityId']},
            'physical_engine_or_authority_changed')
    require(node['state']=='updating' and node['planId']==auth['maintenancePlanId']
            and node['revision']==auth['maintenanceRevision'] and node['targetImage']==target,
            'owned_maintenance_fence_changed')
    require(owned['requestId']==auth['operationId'] and owned['planId']==node['planId']
            and owned['authorityId']==auth['authorityId'] and owned['nodeId']=='main'
            and owned['targetImage']==target,'owned_schedule_receipt_changed')
    require(not any(j['nodeId']=='main' and j['status'] in ('queued','running','held') for j in jobs),
            'main_has_unsettled_claim')
    request=auth['viewerTaskId'] if auth['generation']==1 else str(
        uuid.uuid5(uuid.UUID(auth['viewerTaskId']),'generation:'+str(auth['generation'])))
    require(job['authorityId']==auth['authorityId'] and job['status']=='succeeded'
            and job['requestId']==request and job['nodeId']=='main'
            and job['result']['engineTaskId']==auth['engineTaskId']
            and job['result']['nodeId']=='main' and job['result']['cancelled'] is False,
            'terminal_generation_or_native_identity_changed')
    require(container['Id']==CONTAINER and container['Image']==IMAGE
            and not container['State']['Running'] and container['State']['Pid']==0
            and container['HostConfig']['RestartPolicy']['Name']=='no',
            'native_engine_not_stopped_and_restart_fenced')


class HostObserver:
    def __init__(self, auth, credentials):
        require(os.geteuid() == 0 and sys.platform.startswith('linux'), 'root_linux_host_required')
        self.auth = auth
        require(auth['nodeId'] == 'main' and auth['containerId'] == CONTAINER and auth['imageId'] == IMAGE,
                'fixed_main_profile_changed')
        addresses = json.loads(self.command(['ip', '-j', 'address', 'show']))
        require(any(a.get('local') == '192.168.50.88' for n in addresses for a in n.get('addr_info', [])),
                'wrong_physical_host')
        values = dict(line.split('=', 1) for line in private_bytes(credentials).decode().splitlines() if '=' in line)
        worker, operator = values['PROCESSING_API_TOKEN'], values['PROCESSING_PROTOTYPE_OPERATOR_TOKEN']
        require(worker and operator and worker != operator, 'distinct_coordinator_roles_required')
        self.api, self.operator = Client(ORIGIN, worker), Client(ORIGIN, operator)
        self.ingress = Ingress(INGRESS_ROOT)
        self.gate_fd = None

    @staticmethod
    def command(args):
        p = subprocess.run(args, capture_output=True, text=True, timeout=30)
        require(p.returncode == 0, 'host_observation_failed')
        return p.stdout

    def inspect(self, identifier):
        rows = json.loads(self.command(['docker', '--host', 'unix:///var/run/docker.sock', 'inspect', identifier]))
        require(len(rows) == 1, 'ambiguous_container')
        return rows[0]

    @contextmanager
    def held(self):
        # Acquire the EXISTING bkoltz-owned canonical lock. Never instantiate a
        # root-user NodeGate, provision a new gate, chmod or replace its files.
        import fcntl
        gate = Path('/home/bkoltz/.local/state/LedgeTop-Processing/node-gates') / hashlib.sha256(self.auth['engineId'].encode()).hexdigest()
        require(gate.resolve() == gate and gate.is_dir(), 'existing_canonical_gate_required')
        info = gate.stat()
        require(info.st_uid == 1000 and stat.S_IMODE(info.st_mode) == 0o700, 'canonical_gate_owner_changed')
        fd = os.open(gate / 'gate.lock', os.O_RDWR | os.O_NOFOLLOW)
        try:
            lock = os.fstat(fd)
            require(stat.S_ISREG(lock.st_mode) and lock.st_uid == 1000 and lock.st_nlink == 1
                    and stat.S_IMODE(lock.st_mode) == 0o600, 'canonical_lock_changed')
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.gate_fd, self.gate = fd, gate
            yield self
        finally:
            self.gate_fd = None
            os.close(fd)

    def __call__(self, auth):
        require(self.gate_fd is not None, 'canonical_fence_not_held')
        require(auth == self.auth, 'authorization_changed')
        engine_id = self.command(['docker', '--host', 'unix:///var/run/docker.sock', 'info', '--format', '{{.ID}}']).strip()
        authority = self.api.json('GET', '/v1/prototype/authority')
        node = self.api.json('GET', '/v1/prototype/nodes/main')['node']
        owned = self.operator.json('GET', '/v1/prototype/nodes/main/schedule-owned/' + auth['operationId'])['receipt']
        jobs = self.api.json('GET', '/v1/prototype/jobs')['jobs']
        job = self.api.json('GET', '/v1/prototype/jobs/' + auth['jobId'])['job']
        c = self.inspect(CONTAINER)
        verify_execution_fence(auth,engine_id=engine_id,authority=authority,node=node,owned=owned,
                               jobs=jobs,job=job,container=c)
        require(len(c['Mounts']) == 1 and c['Mounts'][0]['Type'] == 'bind' and c['Mounts'][0]['RW']
                and c['Mounts'][0]['Source'] == str(DATA) and c['Mounts'][0]['Destination'] == '/var/www/data',
                'native_data_binding_changed')
        mount_text = Path('/proc/self/mountinfo').read_text()
        mounts = parse_mountinfo(mount_text)
        identifiers = self.command(['docker', '--host', 'unix:///var/run/docker.sock', 'ps', '-aq']).split()
        for identifier in identifiers:
            if CONTAINER.startswith(identifier):
                continue
            other = self.inspect(identifier)
            if not other['State']['Running']:
                continue
            host = other['HostConfig']
            require(not host.get('Privileged') and 'SYS_ADMIN' not in (host.get('CapAdd') or []),
                    'unconstrained_container_authority')
            approved = auth.get('readOnlyObservers', {}).get(other['Id'])
            sealed = sealed_metrics_observer(other, approved)
            require(approved is None or sealed, 'reviewed_observer_role_changed')
            for mount in other['Mounts']:
                if mount['Type'] == 'tmpfs':
                    continue  # Private tmpfs cannot alias native data without forbidden mount authority.
                require(mount['Type'] in ('bind','volume'), 'unsupported_container_mount_type')
                if mount['RW'] is False and sealed:
                    continue
                overlap = mount_overlaps(DATA, mount['Source'], mounts)
                if mount['RW']:
                    require(not overlap, 'another_container_can_write_native_data')
                elif overlap:
                    # Only an independently sealed metrics-only observer may
                    # retain a broad RO host-root mount. Never auto-adopt one.
                    require(False, 'unfenced_readonly_container_reader')
        gate_path = self.gate / 'gate.json'
        info = gate_path.lstat()
        require(stat.S_ISREG(info.st_mode) and info.st_uid == 1000 and info.st_nlink == 1
                and stat.S_IMODE(info.st_mode) == 0o600, 'canonical_gate_state_changed')
        gate = json.loads(gate_path.read_bytes())
        require(gate['engineId'] == auth['engineId'] and gate['nodeId'] == 'main'
                and gate['coordinatorOrigin'] == ORIGIN and gate['authorityId'] == auth['authorityId']
                and gate['generation'] == auth['nodeGateGeneration'] and gate['reservation'] is None,
                'canonical_gate_binding_or_reservation_changed')
        named = (self.gate / 'gate.lock').stat()
        opened = os.fstat(self.gate_fd)
        require((named.st_dev, named.st_ino) == (opened.st_dev, opened.st_ino), 'canonical_lock_replaced')
        state = json.loads((INGRESS_ROOT / 'state.json').read_bytes())
        ingress = self.ingress.inspect()
        require(state['state'] == 'closed' and state['containerId'] == ingress['Id']
                and not set(state['oldWorkers']) & self.ingress.workers(ingress),
                'ingress_readers_not_drained')
        require((INGRESS_ROOT / 'public-config/nginx.conf').read_bytes() == configuration(
                self.ingress.config['listenPort'], self.ingress.config['backendPort'], True).encode(),
                'ingress_configuration_not_closed')
        require(self.ingress.http(self.ingress.config['listenPort'])[0] == 503, 'ingress_not_closed')
        task = DATA / auth['engineTaskId']
        # Mount IDs matter too: a same-device bind mount can evade st_dev.
        require(Path('/proc/self/mountinfo').read_text() == mount_text, 'mount_topology_changed')
        for line in mount_text.splitlines():
            mounted = line.split()[4].replace('\\040', ' ')
            require(mounted != str(task) and not mounted.startswith(str(task) + '/'), 'nested_task_mount')
        assert_no_external_readers(task,mounts=mounts)
        return {k: auth[k] for k in ('authorityId', 'nodeId', 'engineId', 'containerId', 'imageId',
                    'viewerTaskId', 'generation', 'jobId', 'engineTaskId', 'maintenancePlanId')}


def save_exclusive(path, value):
    payload = canonical(value)
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--authorization', type=Path, required=True)
    parser.add_argument('--authorization-sha256', required=True)
    parser.add_argument('--credentials', type=Path, required=True)
    parser.add_argument('--receipt-root', type=Path, required=True)
    action = parser.add_mutually_exclusive_group()
    action.add_argument('--prepare', action='store_true')
    action.add_argument('--apply', action='store_true')
    parser.add_argument('--prepared-sha256')
    args = parser.parse_args(argv)
    try:
        payload = private_bytes(args.authorization)
        require(hashlib.sha256(payload).hexdigest() == args.authorization_sha256, 'authorization_receipt_changed')
        auth = json.loads(payload)
        runtime = HostObserver(auth, args.credentials)
        with runtime.held():
            cleaner = CacheCleanup(DATA / auth['engineTaskId'], auth, runtime)
            if args.apply:
                raw = private_bytes(args.receipt_root / 'prepared.json')
                require(args.prepared_sha256 and hashlib.sha256(raw).hexdigest() == args.prepared_sha256,
                        'reviewed_prepared_digest_required')
                result = cleaner.apply(json.loads(raw), args.receipt_root)
            else:
                prepared = cleaner.plan()
                if args.prepare:
                    require(args.receipt_root.resolve() == args.receipt_root.absolute()
                            and args.receipt_root.stat().st_uid == 0
                            and stat.S_IMODE(args.receipt_root.stat().st_mode) == 0o700, 'private_receipt_root_required')
                    save_exclusive(args.receipt_root / 'prepared.json', prepared)
                result = {'mode':'prepared' if args.prepare else 'plan', 'writes':bool(args.prepare),
                          'preparedSha256':digest(prepared), 'reclaimLogicalBytes':prepared['reclaimBytes']}
        print(json.dumps(result, sort_keys=True))
        return 0
    except Exception:
        # Never print credentials, raw archive members, client paths or Docker/API errors.
        print(json.dumps({'status':'held','operatorReviewRequired':True,'automaticReopen':False}))
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
