"""Read-only existing-worker readiness, never a service handover or authority.

The remote caller must dispatch this through an identity-tracked SSH observer,
verify its exact challenge, and retire that process before using its evidence.
No maintenance lock, journal, configuration or service action is created here.
"""
import hashlib
import inspect
import json
import os
from pathlib import Path
import subprocess
import stat
import sys
import uuid

from ops.enrolled_coordinator_transport import BoundedSubprocess, canonical, require
from ops.reference_worker_handover import ReferenceWorkerHandover, SystemdWorkerControl
from processing import reference_protocol as p


PROTOCOL = 'ltds-enrolled-worker-readiness-probe-v1'
EXPECTED = ('nodeId', 'workerId', 'workerInstanceId', 'authorityId', 'engineId',
            'gateGeneration', 'engineVersion', 'configSha256', 'sourceSha256', 'activation')
PROCESS = ('pid', 'active', 'substate', 'cgroup', 'invocationId', 'startTime')


def _read_only_runner(argv, **options):
    require(type(argv) is list and argv[:3] == ['/usr/bin/systemctl', '--user', 'show'],
            'read_only_systemd_probe_required')
    raw = BoundedSubprocess().run(argv, timeout=15, output_limit=32768)
    return subprocess.CompletedProcess(argv, 0, raw.decode('utf8'), '')


def validate_request(worker, challenge):
    p._closed(worker, ('nodeId', 'sshHost', 'sshUid', 'settings', 'expected'), 'existing worker probe')
    require(type(worker['sshUid']) is int and 0 < worker['sshUid'] < 2**31,
            'reviewed_worker_owner_required')
    p._uuid(challenge, 'probe challenge')
    p._closed(worker['settings'], ReferenceWorkerHandover.FIELDS, 'existing worker settings')
    expected = worker['expected']
    p._closed(expected, EXPECTED, 'existing worker expectations')
    for key in ('nodeId', 'workerId', 'workerInstanceId', 'engineId'):
        p._slug(expected[key], key)
    p._uuid(expected['authorityId'], 'worker authority')
    for key in ('configSha256', 'sourceSha256'):
        p._hash(expected[key], key)
    require(type(expected['gateGeneration']) is int and expected['gateGeneration'] >= 0,
            'worker_generation_required')
    require(worker['nodeId'] == expected['nodeId'] and
            worker['settings']['sourceSha256'] == expected['sourceSha256'],
            'worker_probe_binding_changed')


def _source_digest(root):
    """Independent pre-import hash, matching the existing readiness footprint."""
    root = Path(root)
    require(root.is_absolute() and root.resolve() == root and not os.access(root, os.W_OK) and
            not any(part.is_symlink() for part in root.parents), 'original_worker_source_required')
    files, entries, total = {}, 0, 0
    for package in ('processing', 'ops'):
        pending = [root / package]
        while pending:
            directory = pending.pop()
            require(not os.access(directory, os.W_OK), 'readonly_worker_source_required')
            with os.scandir(directory) as items:
                for item in items:
                    entries += 1
                    require(entries <= 4096 and not item.is_symlink(), 'bounded_worker_source_required')
                    if item.is_dir(follow_symlinks=False):
                        pending.append(Path(item.path))
                    elif item.name.endswith('.py'):
                        path = Path(item.path)
                        before = path.lstat()
                        require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and before.st_size <= 4 * 1024**2 and
                                not os.access(path, os.W_OK) and len(files) < 512,
                                'readonly_worker_source_required')
                        total += before.st_size
                        require(total <= 32 * 1024**2, 'bounded_worker_source_required')
                        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
                        key = lambda s: (s.st_dev, s.st_ino, s.st_mode, s.st_uid, s.st_nlink,
                                         s.st_size, s.st_mtime_ns, s.st_ctime_ns)
                        try:
                            require(key(before) == key(os.fstat(fd)), 'worker_source_changed')
                            with os.fdopen(fd, 'rb', closefd=False) as stream:
                                raw = stream.read(4 * 1024**2 + 1)
                            require(len(raw) == before.st_size and
                                    key(before) == key(os.fstat(fd)) == key(path.lstat()), 'worker_source_changed')
                        finally:
                            os.close(fd)
                        files[path.relative_to(root).as_posix()] = hashlib.sha256(raw).hexdigest()
    require('ops/reference_worker_handover.py' in files and
            'processing/reference_driver.py' in files, 'worker_source_incomplete')
    return hashlib.sha256(json.dumps(files, sort_keys=True, separators=(',', ':'),
                                   ensure_ascii=False, allow_nan=False).encode()).hexdigest()


def probe(worker, challenge, *, control=None, readiness_request=None):
    """Fresh authenticated DTO plus actual unit/process observation; no __enter__."""
    validate_request(worker, challenge)
    require(os.geteuid() == worker['sshUid'] and
            Path(sys.executable).resolve() == Path(worker['settings']['pythonExecutable']).resolve(),
            'actual_worker_probe_identity_changed')
    require(_source_digest(worker['settings']['sourceRoot']) == worker['expected']['sourceSha256'],
            'worker_probe_source_changed')
    settings, expected = worker['settings'], worker['expected']
    if control is None:
        control = SystemdWorkerControl(settings, runner=_read_only_runner)
    # Constructor/ready are read-only. Deliberately never enter its mutation
    # context; this unused receipt path must never be inspected or created.
    handover = ReferenceWorkerHandover(settings, '/unused-readonly-probe',
        guard=lambda: require(False, 'probe_has_no_mutation_authority'),
        control=control, readiness_request=readiness_request)
    value, process = handover.ready(expected_sha=expected['configSha256'],
        activation=expected['activation'], require_idle=True)
    require(all(value[key] == expected[key] for key in EXPECTED),
            'reviewed_worker_readiness_changed')
    p._closed(process, PROCESS, 'actual worker process')
    require(type(process['pid']) is int and process['pid'] > 0 and
            process['active'] == 'active' and process['substate'] == 'running' and
            type(process['startTime']) is str and process['startTime'].isdigit(),
            'worker_process_identity_unavailable')
    require(type(process['cgroup']) is str and process['cgroup'].startswith('/') and
            '..' not in Path(process['cgroup']).parts and len(process['cgroup']) <= 4096 and
            type(process['invocationId']) is str and len(process['invocationId']) <= 128,
            'worker_process_identity_unavailable')
    # Recheck process identity after TLS and config/source verification, and
    # reread all pins without sending another request or creating a lock.
    require(control.probe() == process, 'worker_process_changed_during_probe')
    raw, _ = handover._config()
    require(hashlib.sha256(raw).hexdigest() == expected['configSha256'],
            'worker_config_changed_during_probe')
    return dict(schemaVersion=1, protocol=PROTOCOL, challenge=challenge,
        nodeId=worker['nodeId'], settingsSha256=hashlib.sha256(canonical(settings)).hexdigest(),
        readiness=value, process=process)


def serialize_probe(worker, challenge):
    """Public pins only; credentials stay in the existing private worker config.

    This is the body for a tracked remote observer, not an SSH execution API.
    The caller supplies the reviewed Python/source origin and lifecycle wrapper.
    """
    validate_request(worker, challenge)
    from ops.enrolled_coordinator_transport import _terminate_owned
    module = Path(__file__).read_text().split('\ndef serialize_probe(', 1)[0]
    module = module.replace('from ops.enrolled_coordinator_transport import BoundedSubprocess, canonical, require', '')
    module = module.replace('from ops.reference_worker_handover import ReferenceWorkerHandover, SystemdWorkerControl', '')
    module = module.replace('from processing import reference_protocol as p', '')
    # Reuse the reviewed client process-group cleanup without requiring this
    # new controller module to be installed on the existing worker host.
    definitions = ('import selectors,signal,time\nclass TransportHeld(RuntimeError): pass\n' +
                   inspect.getsource(require) + '\n' + inspect.getsource(canonical) + '\n' +
                   inspect.getsource(_terminate_owned) + '\n' + inspect.getsource(BoundedSubprocess))
    setup = ('sys.dont_write_bytecode=True\nworker=' + repr(worker) + '\nchallenge=' + repr(challenge) + '\n'
        'require(os.geteuid()==worker["sshUid"],"actual_worker_probe_identity_changed")\n'
        'require(Path(sys.executable).resolve()==Path(worker["settings"]["pythonExecutable"]).resolve(),"actual_worker_probe_identity_changed")\n'
        'require(_source_digest(worker["settings"]["sourceRoot"])==worker["expected"]["sourceSha256"],"worker_probe_source_changed")\n'
        'sys.path.insert(0,worker["settings"]["sourceRoot"])\n'
        'from ops.reference_worker_handover import ReferenceWorkerHandover,SystemdWorkerControl\n'
        'from processing import reference_protocol as p\n')
    tail = ('try:\n' + ''.join(' '+line+'\n' for line in setup.splitlines()) +
            ' print(json.dumps(probe(worker,challenge),sort_keys=True,separators=(",",":")))\n'
            'except BaseException:\n raise RuntimeError("worker_readiness_probe_unavailable") from None\n')
    return (module + '\n' + definitions + '\n' + tail).encode()
