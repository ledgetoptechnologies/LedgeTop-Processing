"""SOURCE CANDIDATE: one fresh, reviewed main cleanup/retention operation.

No normal PR376 apply/prepare is replayed. Intent phases stop for independent
cleanup-plan review. No CLI applies this candidate until its native/source
qualification manifest and every exact stage command have been reviewed.
"""
from contextlib import contextmanager
import argparse
from datetime import datetime, timezone
import hashlib
import http.client
import importlib
import json
import os
from pathlib import Path, PurePosixPath
import stat
import subprocess
import select
import signal
import socket
import tarfile
import time
import uuid

from ops import retained_main_engine_swap as swap
from processing.intermediate_mounts import mounts_fingerprint as mount_fingerprint, sealed_metrics_observer, constrained_peer_host, MountTopologyHeld
from processing.intermediate_cleanup import CleanupHeld

Held, require, canonical, digest = swap.Held, swap.require, swap.canonical, swap.digest
OLD_UPLOAD = '5805ba8c-21bc-4aa4-8ccc-82bdf6fb1dc0'
OLD_NATIVE = '7d8f1bff-fa84-437a-aa45-3bd7828efa11'
OLD_JOB = '8fbbecc5-bbb2-4da7-a88d-f8bd90406f6f'
# Explicit current user hold: Todd intermediates are investigation inputs.
ALLOWED_CACHE_TASKS = frozenset(('c4e4121a-7848-4290-a3c9-c0537699bbb1',
                               '0f7e6743-63a2-4ff0-91f8-b141569d1a3e'))
CLEANUP_FILES = ('ops/early_intermediate_cleanup.py', 'processing/intermediate_cleanup.py',
                 'processing/intermediate_dependencies.py', 'processing/intermediate_mounts.py',
                 'processing/intermediate_pdf.py')


def mounts_fingerprint(mounts):
    """Normalize inspect array order only; retain every field and multiplicity."""
    require(type(mounts) is list and all(type(row) is dict for row in mounts),
            'invalid_mount_fingerprint_input')
    return mount_fingerprint(mounts)


def safe_failure(error):
    """Only known exception types and fixed underscore codes may leave receipts."""
    import re
    kind = ('Held' if isinstance(error, Held) else 'MountTopologyHeld' if isinstance(error, MountTopologyHeld)
            else 'CleanupHeld' if isinstance(error, CleanupHeld) else 'Exception')
    code = str(error) if kind != 'Exception' else ''
    if re.fullmatch(r'[a-z][a-z0-9_]{0,127}', code) is None:
        code = 'held_unclassified' if kind != 'Exception' else 'unexpected_failure'
    return dict(errorClass=kind, errorCode=code)


def private_json(path):
    info = swap.regular(path, private=True)
    require(info.st_uid == 0 and stat.S_IMODE(info.st_mode) == 0o600, 'root_private_input_required')
    return json.loads(path.read_bytes())


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def operation_root(root, request_id):
    """One canonical root binds both controller and runtime reader journals."""
    require(type(request_id) is str and str(uuid.UUID(request_id)) == request_id,
            'canonical_operation_request_required')
    expected = Path('/var/lib') / ('lt-main-combined-' + uuid.UUID(request_id).hex)
    require(type(root) is type(expected) and root == expected and root.resolve() == root,
            'operation_root_request_binding_changed')
    return root


def inventory_directory(root):
    """One sealed copy: hashes and numeric metadata, no symlink/hardlink escape."""
    root = Path(root)
    require(root.resolve() == root and root.is_dir(), 'real_tmp_copy_required')
    rows, deadline = {}, time.monotonic() + 1800
    def walk(directory):
        yield directory
        with os.scandir(directory) as entries:
            for entry in entries:
                require(time.monotonic() < deadline, 'tmp_enumeration_deadline')
                path = Path(entry.path)
                if entry.is_dir(follow_symlinks=False):
                    yield from walk(path)
                else:
                    yield path
    for path in walk(root):
        require(time.monotonic() < deadline, 'tmp_inventory_deadline')
        info = path.lstat()
        require(info.st_dev == root.stat().st_dev and
                (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)), 'unsupported_tmp_node')
        require(not stat.S_ISREG(info.st_mode) or info.st_nlink == 1, 'tmp_hardlink_requires_review')
        attrs = {key: hashlib.sha256(os.getxattr(path, key)).hexdigest() for key in os.listxattr(path)}
        # This first candidate cannot claim Docker archive ACL/xattr fidelity.
        # Native data-bind ACLs remain covered by the full GNU tar path.
        require(not attrs, 'tmp_extended_attributes_require_separate_native_qualification')
        row = dict(uid=info.st_uid, gid=info.st_gid, mode=stat.S_IMODE(info.st_mode),
                   type='directory' if path.is_dir() else 'file', xattrs=attrs)
        if path.is_file():
            h = hashlib.sha256()
            with path.open('rb') as stream:
                while block := stream.read(1024 * 1024):
                    require(time.monotonic() < deadline, 'tmp_hash_deadline')
                    h.update(block)
                os.fsync(stream.fileno())
            row.update(bytes=info.st_size, sha256=h.hexdigest())
        rows[path.relative_to(root).as_posix()] = row
    return rows


def exec_inspect(identifier):
    require(type(identifier) is str and len(identifier) == 64
            and all(c in '0123456789abcdef' for c in identifier), 'exact_exec_id_required')
    class UnixHTTP(http.client.HTTPConnection):
        def connect(self):
            self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.sock.settimeout(8)
            self.sock.connect('/var/run/docker.sock')
    connection = UnixHTTP('localhost', timeout=8)
    try:
        connection.request('GET', '/exec/' + identifier + '/json')
        response = connection.getresponse()
        try:
            raw = response.read(1024 * 1024 + 1)
            require(response.status == 200 and len(raw) <= 1024 * 1024, 'exec_identity_read_failed')
            value = json.loads(raw)
            require(value.get('ID') == identifier, 'exec_inspect_identity_changed')
            return value
        finally:
            response.close()
    finally:
        connection.close()


def read_probe_line(stream, deadline, maximum):
    data = bytearray()
    while True:
        require(time.monotonic() < deadline and len(data) <= maximum, 'reader_handshake_or_result_deadline')
        ready, _, _ = select.select([stream], [], [], max(0, deadline - time.monotonic()))
        require(bool(ready), 'reader_output_timeout')
        part = os.read(stream.fileno(), min(65536, maximum + 1 - len(data)))
        require(bool(part), 'reader_exited_without_complete_record')
        data.extend(part)
        if b'\n' in data:
            line, tail = data.split(b'\n', 1)
            require(not tail.strip(), 'unexpected_reader_output')
            return json.loads(line)


def process_start_ticks(pid):
    text = (Path('/proc') / str(pid) / 'stat').read_text()
    return int(text[text.rfind(')') + 2:].split()[19])


def verify_exec_identity(record, cid, program, nonce, hello, container_pid):
    config = record.get('ProcessConfig', {})
    require(record.get('ContainerID') == cid and record.get('Running') is True
            and config.get('entrypoint') == 'python3'
            and config.get('arguments') == ['-B', '-c', program, nonce]
            and config.get('privileged') is False and record.get('OpenStdin') is True,
            'exec_command_or_owner_changed')
    host_pid = record.get('Pid')
    require(type(host_pid) is int and host_pid > 0, 'exec_host_pid_missing')
    process = Path('/proc') / str(host_pid)
    start = process_start_ticks(host_pid)
    status = (process / 'status').read_text().splitlines()
    namespaces = [line.split()[1:] for line in status if line.startswith('NSpid:')]
    require(len(namespaces) == 1 and int(namespaces[0][-1]) == hello['pid']
            and start == hello['startTicks']
            and os.readlink(process / 'ns/pid') == os.readlink(Path('/proc') / str(container_pid) / 'ns/pid'),
            'exec_pid_namespace_or_birth_changed')
    require((process / 'cmdline').read_bytes().split(b'\0')[:-1] ==
            [b'python3', b'-B', b'-c', program.encode(), nonce.encode()], 'exec_actual_argv_changed')
    return host_pid


def inventory_inside_container(runtime, cid, probe_nonce):
    """Independent source observation, before stop/copy, not Docker archive data.

    This bounded Python reader returns actual in-container numeric metadata,
    xattr hashes and content hashes. A timeout leaves its intent/nonce for owned
    process reconciliation; no cleanup/deletion is authorized by a partial read.
    """
    program = r'''
import hashlib,json,os,pathlib,stat,time,sys
text=pathlib.Path('/proc/self/stat').read_text();birth=int(text[text.rfind(')')+2:].split()[19])
print(json.dumps(dict(schemaVersion=1,phase='ready',probeNonce=sys.argv[1],pid=os.getpid(),startTicks=birth)),flush=True)
approval=json.loads(sys.stdin.readline(512))
assert approval==dict(probeNonce=sys.argv[1],approval='hash')
root=pathlib.Path('/var/www/tmp');deadline=time.monotonic()+1800;rows={}
assert root.resolve()==root and root.is_dir()
device=root.stat().st_dev
def walk(directory):
 assert time.monotonic()<deadline
 yield directory
 with os.scandir(directory) as entries:
  for entry in entries:
   assert time.monotonic()<deadline
   path=pathlib.Path(entry.path)
   if entry.is_dir(follow_symlinks=False):yield from walk(path)
   else:yield path
for path in walk(root):
 assert time.monotonic()<deadline
 info=path.lstat()
 assert info.st_dev==device and (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode))
 assert not stat.S_ISREG(info.st_mode) or info.st_nlink==1
 attrs={key:hashlib.sha256(os.getxattr(path,key)).hexdigest() for key in os.listxattr(path)}
 row=dict(uid=info.st_uid,gid=info.st_gid,mode=stat.S_IMODE(info.st_mode),
          type='directory' if stat.S_ISDIR(info.st_mode) else 'file',xattrs=attrs)
 if stat.S_ISREG(info.st_mode):
  fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW);h=hashlib.sha256()
  try:
   assert (os.fstat(fd).st_dev,os.fstat(fd).st_ino)==(info.st_dev,info.st_ino)
   while True:
    block=os.read(fd,1024*1024)
    if not block:break
    assert time.monotonic()<deadline
    h.update(block)
   after=path.lstat()
   assert (after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns)==(info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns)
  finally:os.close(fd)
  row.update(bytes=info.st_size,sha256=h.hexdigest())
 rows[path.relative_to(root).as_posix()]=row
print(json.dumps(dict(schemaVersion=1,phase='complete',probeNonce=sys.argv[1],pid=os.getpid(),inventory=rows)),flush=True)
'''
    record = dict(schemaVersion=1, stage='spawn_intent', nonce=probe_nonce, containerId=cid,
                  cliPid=None, insidePid=None, hostPid=None, execId=None,
                  programSha256=hashlib.sha256(program.encode()).hexdigest(), approvalSent=False,
                  exitVerified=False, disposition='UNKNOWN')
    runtime.checkpoint_exec_reader(record)
    environment = {k: v for k, v in os.environ.items() if k not in
                   ('DOCKER_HOST', 'DOCKER_CONTEXT', 'DOCKER_TLS_VERIFY', 'DOCKER_CERT_PATH')}
    process = subprocess.Popen(['docker', '--host', 'unix:///var/run/docker.sock', 'exec', '-i',
        cid, 'python3', '-B', '-c', program, probe_nonce], env=environment,
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    pidfd, pidfd_owned, completed, value = None, False, False, None
    try:
        record.update(stage='cli_started', cliPid=process.pid)
        runtime.checkpoint_exec_reader(record)
        record.update(stage='cli_birth_sealed', cliStartTicks=process_start_ticks(process.pid))
        runtime.checkpoint_exec_reader(record)
        hello = read_probe_line(process.stdout, time.monotonic() + 15, 4096)
        require(hello.get('schemaVersion') == 1 and hello.get('phase') == 'ready'
                and hello.get('probeNonce') == probe_nonce and type(hello.get('pid')) is int and hello['pid'] > 0
                and type(hello.get('startTicks')) is int, 'reader_ready_identity_invalid')
        record.update(stage='inside_ready_unapproved', insidePid=hello['pid'], startTicks=hello['startTicks'])
        runtime.checkpoint_exec_reader(record)
        container = runtime.inspect(cid)
        require(container['Id'] == cid and container['State']['Running'], 'reader_container_changed')
        matches = []
        for identifier in container.get('ExecIDs') or []:
            candidate = exec_inspect(identifier)
            config = candidate.get('ProcessConfig', {})
            if (candidate.get('ContainerID') == cid and config.get('entrypoint') == 'python3'
                    and config.get('arguments') == ['-B', '-c', program, probe_nonce]):
                matches.append((identifier, candidate))
        require(len(matches) == 1, 'reader_exec_identity_missing_or_ambiguous')
        identifier, candidate = matches[0]
        host_pid = verify_exec_identity(candidate, cid, program, probe_nonce, hello, container['State']['Pid'])
        require(hasattr(os, 'pidfd_open') and hasattr(signal, 'pidfd_send_signal'), 'reader_pidfd_ownership_required')
        pidfd = os.pidfd_open(host_pid)
        require(verify_exec_identity(exec_inspect(identifier), cid, program, probe_nonce, hello,
                    container['State']['Pid']) == host_pid, 'reader_identity_changed_at_pidfd_acquire')
        pidfd_owned = True
        record.update(stage='identity_sealed_unapproved', execId=identifier, hostPid=host_pid)
        runtime.checkpoint_exec_reader(record)
        record['stage'] = 'approval_intent'
        runtime.checkpoint_exec_reader(record)
        process.stdin.write((json.dumps({'probeNonce': probe_nonce, 'approval': 'hash'}) + '\n').encode())
        process.stdin.flush()
        process.stdin.close()
        record.update(stage='hash_reader_running', approvalSent=True)
        runtime.checkpoint_exec_reader(record)
        value = read_probe_line(process.stdout, time.monotonic() + 1810, 64 * 1024 * 1024)
        process.wait(timeout=15)
        exited = exec_inspect(identifier)
        require(process.returncode == 0 and exited.get('ID') == identifier
                and exited.get('Running') is False and exited.get('ExitCode') == 0,
                'reader_exit_not_verified')
        record.update(stage='reader_exited', exitVerified=True, disposition='EXITED', cliExitCode=0, execExitCode=0)
        runtime.checkpoint_exec_reader(record)
        completed = True
    finally:
        try:
            if not completed:
                try:
                    record.update(stage='reader_disposition_unknown', disposition='UNKNOWN', exitVerified=False)
                    runtime.checkpoint_exec_reader(record)
                finally:
                    if process.stdin and not process.stdin.closed:
                        try: process.stdin.close()
                        except OSError: pass  # Preserve UNKNOWN; do not skip owned cleanup.
                    # Only the verified read-only exec's retained pidfd may be signaled.
                    if pidfd_owned:
                        try: signal.pidfd_send_signal(pidfd, signal.SIGTERM)
                        except OSError: pass  # Still UNKNOWN, never assume signal delivery.
                    if process.poll() is None:
                        process.terminate()
                        try: process.wait(timeout=3)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            try: process.wait(timeout=3)
                            except subprocess.TimeoutExpired: pass
                    if record.get('execId'):
                        try:
                            observed = exec_inspect(record['execId'])
                            if (observed.get('ID') == record['execId'] and observed.get('Running') is False
                                    and process.poll() is not None):
                                record.update(stage='reader_failed_but_exited', disposition='EXITED_FAILED',
                                    exitVerified=True, execExitCode=observed.get('ExitCode'), cliExitCode=process.returncode)
                                runtime.checkpoint_exec_reader(record)
                        except Exception:
                            pass
        finally:
            if process.stdin and not process.stdin.closed:
                try: process.stdin.close()
                except OSError: pass  # Never mask the original reader failure.
            if process.stdout:
                process.stdout.close()
            if pidfd is not None:
                os.close(pidfd)
    require(completed, 'reader_outcome_unknown_no_stop_cleanup_or_retry')
    return validate_tmp_probe(value, probe_nonce, expected_pid=record['insidePid'])


def validate_tmp_probe(value, probe_nonce, *, expected_pid=None):
    require(value.get('schemaVersion') == 1 and value.get('probeNonce') == probe_nonce
            and type(value.get('pid')) is int and value['pid'] > 0 and type(value.get('inventory')) is dict,
            'independent_tmp_source_probe_unproven')
    require(expected_pid is None or value['pid'] == expected_pid, 'completed_reader_pid_changed')
    rows = value['inventory']
    for name, row in rows.items():
        path = PurePosixPath(name)
        require(type(name) is str and not path.is_absolute() and '..' not in path.parts
                and type(row) is dict and row.get('type') in ('file', 'directory')
                and all(type(row.get(k)) is int and row[k] >= 0 for k in ('uid', 'gid', 'mode'))
                and row['mode'] <= 0o7777 and type(row.get('xattrs')) is dict, 'tmp_probe_row_invalid')
        if row['type'] == 'file':
            require(type(row.get('bytes')) is int and row['bytes'] >= 0
                    and type(row.get('sha256')) is str and len(row['sha256']) == 64,
                    'tmp_probe_file_invalid')
    require('.' in rows and rows['.']['type'] == 'directory'
            and all(row['uid'] == 0 and row['gid'] == 0 and not row['xattrs'] for row in rows.values()),
            'original_tmp_profile_requires_root0_noattrs_native_qualification')
    return rows


def inventory_container_tmp(runtime, cid):
    """Read a Docker archive stream; never extract or allocate another TMP copy.

    Lost CLI/daemon disposition holds. No automatic retry or cleanup is performed.
    Only regular files/directories with no extra attributes are admitted.
    """
    runtime.checkpoint_child('tmp_archive_reader_spawn_intent', None, cid)
    environment = {k: v for k, v in os.environ.items() if k not in
                   ('DOCKER_HOST', 'DOCKER_CONTEXT', 'DOCKER_TLS_VERIFY', 'DOCKER_CERT_PATH')}
    process = subprocess.Popen(['docker', '--host', 'unix:///var/run/docker.sock',
        'cp', '--archive', cid + ':/var/www/tmp', '-'], env=environment,
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    rows, deadline, completed = {}, time.monotonic() + 1800, False
    try:
        runtime.checkpoint_child('tmp_archive_reader', process.pid, cid)
        class DeadlinePipe:
            def read(self, count):
                require(count >= 0 and time.monotonic() < deadline, 'tmp_stream_deadline')
                ready, _, _ = select.select([process.stdout], [], [], max(0, deadline - time.monotonic()))
                require(bool(ready), 'tmp_stream_read_timeout')
                return os.read(process.stdout.fileno(), count)
        with tarfile.open(fileobj=DeadlinePipe(), mode='r|') as archive:
            for item in archive:
                require(time.monotonic() < deadline, 'tmp_archive_deadline')
                name = PurePosixPath(item.name)
                require(not name.is_absolute() and '..' not in name.parts and name.parts and name.parts[0] == 'tmp',
                        'tmp_archive_path_changed')
                relative = PurePosixPath(*name.parts[1:]).as_posix()
                require(relative not in rows and (item.isfile() or item.isdir()), 'tmp_archive_duplicate_or_link')
                require(not any('xattr' in k.lower() or 'acl' in k.lower() for k in item.pax_headers),
                        'tmp_archive_attributes_require_qualification')
                row = dict(uid=item.uid, gid=item.gid, mode=item.mode & 0o7777,
                           type='file' if item.isfile() else 'directory', xattrs={})
                if item.isfile():
                    h = hashlib.sha256()
                    source = archive.extractfile(item)
                    require(source is not None, 'tmp_archive_body_missing')
                    with source:
                        while block := source.read(1024 * 1024):
                            require(time.monotonic() < deadline, 'tmp_archive_hash_deadline')
                            h.update(block)
                    row.update(bytes=item.size, sha256=h.hexdigest())
                rows[relative] = row
        process.wait(timeout=30)
        completed = True
        require(process.returncode == 0 and '.' in rows, 'tmp_archive_reader_failed')
        runtime.checkpoint_child('tmp_archive_reader_exited', process.pid, cid)
        return rows
    finally:
        if process.stdout:
            process.stdout.close()
        if not completed:
            # Killing a CLI does not prove daemon-side archive disposition.
            # Retain exact PID/CID evidence and leave the operation consumed.
            try:
                runtime.checkpoint_child('tmp_archive_reader_disposition_unknown', process.pid, cid)
            finally:
                # Receipt failure must not skip cleanup of this exact owned CLI.
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        try:
                            process.wait(timeout=3)
                        except subprocess.TimeoutExpired:
                            runtime.checkpoint_child('tmp_archive_cli_still_running', process.pid, cid)
                if process.poll() is not None:
                    process.wait()


def load_cleanup(source_hashes):
    root = Path(__file__).resolve().parents[1]
    require(set(source_hashes) == set(CLEANUP_FILES), 'exact_cleanup_source_bindings_required')
    for name, expected in source_hashes.items():
        path = root / name
        require(path.is_file() and not path.is_symlink() and digest(path.read_bytes()) == expected,
                'qualified_cleanup_source_missing_or_changed')
    return importlib.import_module('ops.early_intermediate_cleanup'), importlib.import_module('processing.intermediate_mounts')


def source_inventory():
    root = Path(__file__).resolve().parents[1]
    result = {}
    for folder in ('ops', 'processing'):
        for path in (root / folder).rglob('*.py'):
            require(path.resolve() == path and not path.is_symlink(), 'qualified_source_alias')
            result[path.relative_to(root).as_posix()] = digest(path.read_bytes())
    return result


def prepare_fresh(runtime, qualification_path, reviewed_qualification_sha256, templates):
    """Create a NEW private operation only after independently reviewed evidence.

    No constructor against historical state and no prior journal is consumed.
    This function intentionally cannot accept create-only/local-server evidence
    as the full native qualification. CLI binding remains separately gated.
    """
    qualification = private_json(qualification_path)
    require(digest(canonical(qualification)) == reviewed_qualification_sha256,
            'independently_reviewed_qualification_manifest_required')
    required = {'native-bind-backup-restore-acl-xattr', 'native-rootfs-export',
                'native-writable-tmp-preservation', 'native-compose-recreation',
                'real-server-terminal-start-restart', 'actual-format-cleanup-parsers'}
    require(qualification.get('schemaVersion') == 1 and qualification.get('passed') is True
            and required <= set(qualification.get('completedGates', []))
            and qualification.get('tmpPreservationProfile') == 'root0-gid0-noattrs-v1'
            and qualification.get('baseImage') == swap.BASE and qualification.get('targetImage') == swap.TARGET,
            'native_and_parser_qualification_incomplete')
    source_root = Path(__file__).resolve().parents[1]
    require(source_inventory() == qualification['sourceHashes'], 'complete_qualified_source_binding_changed')
    for name, expected in qualification['sourceHashes'].items():
        relative = PurePosixPath(name)
        require(not relative.is_absolute() and '..' not in relative.parts and relative.parts[0] in ('ops', 'processing')
                and relative.suffix == '.py', 'unsafe_qualified_source_path')
        path = source_root / name
        require(path.resolve() == path and digest(path.read_bytes()) == expected, 'qualified_source_changed')
    require({*CLEANUP_FILES, 'ops/retained_main_engine_swap.py', 'ops/retained_small_engine_swap.py',
             'ops/combined_main_retention.py'} <= set(qualification['sourceHashes']), 'critical_source_binding_missing')
    require(runtime.root.parent == Path('/var/lib') and runtime.root.name.startswith('lt-main-combined-')
            and runtime.root.resolve() == runtime.root and not runtime.root.exists(), 'new_private_operation_root_required')
    nonce = runtime.root.name.removeprefix('lt-main-combined-')
    request_id = str(uuid.UUID(hex=nonce))
    require(uuid.UUID(request_id).hex == nonce, 'canonical_operation_nonce_required')
    operation_root(runtime.root, request_id)
    require(templates and all(t['engineTaskId'] in ALLOWED_CACHE_TASKS for t in templates), 'only_currently_authorized_cache_tasks')
    prepared = runtime.snapshot()
    prepared.update(requestId=request_id, combined=True,
        qualificationSha256=reviewed_qualification_sha256,
        sourceHashes=qualification['sourceHashes'],
        inactiveUploadIdentity=runtime.inactive_identity(prepared['engine']),
        cleanupTemplates=templates, readOnlyObservers=runtime.observers)
    os.umask(0o077)
    runtime.root.mkdir(mode=0o700)
    sync_directory(runtime.root.parent)
    swap.prepare_empty_environment(runtime.root)
    swap.save(runtime.root / 'prepared.private.json', prepared)
    state = dict(schemaVersion=1, requestId=request_id, stage='prepared',
                 preparedSha256=digest(canonical(prepared)), autoRollback=False)
    swap.save(runtime.root / 'combined.private.json', state)
    return dict(preparedSha256=state['preparedSha256'], writes=True, applyRequiresIndependentReview=True)


class CombinedRuntime(swap.Runtime):
    def __init__(self, credentials, root, observers, cleanup_hashes):
        super().__init__(credentials)
        self.credentials, self.root, self.observers = credentials, root, observers
        self.cleanup_hashes = dict(cleanup_hashes)
        self.cleanup, self.mounts = load_cleanup(cleanup_hashes)
        self.state = None

    def checkpoint_child(self, stage, pid, cid):
        require(self.state is not None, 'owned_operation_journal_required')
        self.state['ownedReader'] = dict(stage=stage, pid=pid, containerId=cid)
        swap.save(self.root / 'combined.private.json', self.state)

    def checkpoint_exec_reader(self, record):
        require(self.state is not None, 'owned_operation_journal_required')
        # Copy the record so later in-memory updates do not rewrite history.
        value = json.loads(json.dumps(record))
        self.state['ownedExecReader'] = value
        self.state.setdefault('execReaderHistory', []).append(value)
        swap.save(self.root / 'combined.private.json', self.state)

    def inactive_identity(self, engine):
        job = self.api.json('GET', '/v1/prototype/jobs/' + OLD_JOB)['job']
        require(job['authorityId'] == self.authority() and job['status'] == 'failed'
                and job['requestId'] == OLD_NATIVE and job['nodeId'] == 'main',
                'inactive_upload_resolution_not_current')
        require(OLD_UPLOAD not in engine['tasks'], 'old_upload_is_registered_work')
        return dict(jobId=OLD_JOB, nativeTaskId=OLD_NATIVE, engineTaskId=OLD_UPLOAD,
                    generation=1, status='failed', authorityId=job['authorityId'])

    def engine_idle(self, identifier, command):
        # The normal helper's empty-TMP guard is unchanged. This separate fixed
        # continuation recognizes ONLY the original explicitly resolved upload.
        engine = self.engine_state(command)
        self.inactive_identity(engine)
        swap.no_main_claims(self.claims())
        rows = self.docker('top', identifier, '-eo', 'pid,args').splitlines()[1:]
        server, wrapper, init = 0, 0, 0
        import re
        for row in rows:
            parts = row.strip().split(None, 1)
            require(len(parts) == 2 and parts[0].isdigit(), 'unknown_native_process')
            if re.match(r'^(?:/usr/bin/)?node /var/www/index\.js(?: |$)', parts[1]): server += 1
            elif re.match(r'^(?:/bin/)?bash /usr/bin/node /var/www/index\.js(?: |$)', parts[1]): wrapper += 1
            elif re.match(r'^(?:/sbin/)?docker-init(?: |$)', parts[1]): init += 1
            else: raise Held('native_processing_child_present')
        require(server == 1 and wrapper <= 1 and init <= 1, 'native_server_process_count_changed')
        entries = self.docker('exec', identifier, '/bin/ls', '-A', '/var/www/tmp').splitlines()
        require(set(entries) == {'.gitignore', OLD_UPLOAD}, 'unknown_or_missing_tmp_artifact')
        require(self.docker('exec', identifier, '/usr/bin/stat', '-c', '%F %s', '/var/www/tmp/.gitignore') ==
                'regular file 1\n' and self.docker('exec', identifier, '/bin/cat', '/var/www/tmp/.gitignore') == '*',
                'native_tmp_placeholder_changed')
        return engine

    def exclusive_volume(self, own):
        owned = self.inspect(own)
        if owned['Id'] == swap.ENGINE:
            require(owned['Image'] == swap.BASE, 'original_owned_image_changed')
        else:
            recorded = private_json(self.root / 'candidate-inspect.private.json')
            require(recorded['Id'] == owned['Id'] and owned['Image'] == swap.TARGET
                    and owned['Name'] == recorded['Name'], 'unrecorded_candidate_ownership')
        own = owned['Id']  # Compose quiet output may be a unique short ID.
        swap.root_identity(swap.DATA)
        topology = self.mounts.parse_mountinfo(Path('/proc/self/mountinfo').read_text())
        for mount in topology:
            require(not mount['point'].startswith(str(swap.DATA) + '/'), 'nested_native_mount')
        for identifier in self.docker('ps', '--no-trunc', '--quiet').split():
            other = self.inspect(identifier)
            if other['Id'] == own:
                require(other['Image'] in (swap.BASE, swap.TARGET), 'owned_engine_image_changed')
                continue
            host = other['HostConfig']
            require(constrained_peer_host(host),
                    'unconstrained_peer_including_paused_peer')
            approved = self.observers.get(other['Id'])
            sealed = sealed_metrics_observer(other, approved)
            require(approved is None or sealed, 'reviewed_observer_role_changed')
            for mount in other['Mounts']:
                if mount['Type'] == 'tmpfs':
                    continue
                require(mount['Type'] in ('bind', 'volume'), 'unknown_peer_mount')
                if mount['RW'] is False and sealed:
                    continue
                overlap = self.mounts.mount_overlaps(swap.DATA, mount['Source'], topology)
                if mount['RW']:
                    require(not overlap, 'writable_native_alias')
                elif overlap:
                    require(False, 'unreviewed_readonly_native_observer')

    def backup_tmp(self, prepared, root):
        seal = private_json(root / 'inactive-tmp-sealed.private.json')
        original = private_json(root / 'inactive-tmp-source.private.json')
        require(seal['identity'] == self.inactive_identity(prepared['engine'])
                and seal['inventory'] == original['inventory']
                and inventory_directory(root / 'tmp.before') == seal['inventory']
                and inventory_container_tmp(self, swap.ENGINE) == seal['inventory'], 'sealed_tmp_copy_changed')

    def restore_tmp(self, prepared, root, identifier):
        seal = private_json(root / 'inactive-tmp-sealed.private.json')
        require(inventory_directory(root / 'tmp.before') == seal['inventory'], 'tmp_preservation_copy_changed')
        self.docker('cp', '--archive', str(root / 'tmp.before') + '/.', identifier + ':/var/www/tmp/', timeout=1800)

    def verify_restored_tmp(self, prepared, root, identifier):
        seal = private_json(root / 'inactive-tmp-sealed.private.json')
        require(inventory_container_tmp(self, identifier) == seal['inventory'], 'restored_tmp_stream_changed')


class Operation:
    def __init__(self, runtime, root, prepared, *, reviewed_prepared_sha256):
        operation_root(root, prepared['requestId'])
        require(runtime.root == root and runtime.root.resolve() == root,
                'runtime_and_operation_root_must_match')
        require(root.resolve() == root and root.is_dir() and root.stat().st_uid == 0
                and stat.S_IMODE(root.stat().st_mode) == 0o700, 'root_private_operation_required')
        require(digest(canonical(prepared)) == reviewed_prepared_sha256
                and prepared.get('combined') is True and prepared.get('autoRollback') is False
                and prepared.get('target') == swap.TARGET
                and prepared['oldContainer']['Id'] == swap.ENGINE and prepared['oldContainer']['Image'] == swap.BASE,
                'reviewed_fresh_combined_preparation_required')
        require(prepared['sourceHashes'] == source_inventory()
                and runtime.cleanup_hashes == {k: prepared['sourceHashes'][k] for k in CLEANUP_FILES}
                and runtime.observers == prepared['readOnlyObservers'], 'prepared_code_or_observer_binding_changed')
        self.r, self.root, self.prepared = runtime, root, prepared
        self._executing = False

    def run_phase(self, phase, *, reviewed_cleanup_set_sha256=None):
        """Only entry to a mutation phase: lock then re-read durable state.

        A failed phase is consumed/held. Finish uncertainty never closes a node
        that already became active or automatically replays its POST.
        """
        require(phase in ('begin', 'plan-cleanup', 'apply-cleanup', 'finish'), 'unknown_phase')
        with swap.operation_lock(self.root):
            expected = {'begin': 'prepared', 'plan-cleanup': 'stopped_tmp_preserved',
                        'apply-cleanup': 'cleanup_plans_ready', 'finish': 'cache_cleanup_completed'}[phase]
            require(private_json(self.root / 'combined.private.json')['stage'] == expected,
                    'wrong_or_consumed_phase_no_mutation')
            self._executing = True
            try:
                if phase == 'begin':
                    return self.begin_and_preserve()
                if phase == 'plan-cleanup':
                    return self.plan_cleanup(self.prepared['cleanupTemplates'])
                if phase == 'apply-cleanup':
                    return self.apply_cleanup(reviewed_cleanup_set_sha256)
                return self.finish_swap()
            except Exception as error:
                state = private_json(self.root / 'combined.private.json')
                prior = state['stage']
                state.update(stage='held_for_independent_inspection', failedPhase=phase,
                             priorStage=prior, automaticReplay=False, automaticRollback=False,
                             **safe_failure(error))
                self.r.state = state
                swap.save(self.root / 'combined.private.json', state)
                # Never close/rewrite ingress after a possibly successful finish.
                # Preserve uncertainty for fresh authoritative reconciliation.
                if prior != 'finish_intent':
                    state['failureAdmission'] = self.r.hold_closed(self.prepared, state)
                    swap.save(self.root / 'combined.private.json', state)
                raise
            finally:
                self._executing = False

    def state(self, expected):
        require(self._executing, 'phase_lock_required')
        state = private_json(self.root / 'combined.private.json')
        require(state['requestId'] == self.prepared['requestId'] and state['stage'] == expected,
                'wrong_or_consumed_phase')
        require(state['preparedSha256'] == digest(canonical(self.prepared)) and state['autoRollback'] is False,
                'operation_preparation_binding_changed')
        self.r.state = state
        return state

    def checkpoint(self, state, stage):
        state['stage'] = stage
        self.r.state = state
        swap.save(self.root / 'combined.private.json', state)

    def own_updating(self, state):
        reader = state.get('ownedExecReader')
        require(reader is None or (reader.get('exitVerified') is True and reader.get('disposition') == 'EXITED'
                and reader.get('nonce') == state['requestId']), 'reader_unreconciled_no_stop_cleanup_or_retry')
        require(self.r.authority() == self.prepared['authorityId'], 'authority_changed')
        owned = self.r.operator.json('GET', '/v1/prototype/nodes/main/schedule-owned/' + state['requestId'])['receipt']
        require(owned == state['owned'], 'owned_historical_receipt_changed')
        node = self.r.node()
        require(node['state'] == 'updating' and node['planId'] == owned['planId']
                and node['revision'] == state['reservedRevision'] and node['targetImage'] == swap.TARGET,
                'current_owned_reservation_changed')
        swap.no_main_claims(self.r.claims())
        return node

    def stopped(self, state):
        self.own_updating(state)
        current = self.r.inspect(swap.ENGINE)
        require(current['Image'] == swap.BASE and not current['State']['Running'] and current['State']['Pid'] == 0
                and current['State']['ExitCode'] == 0 and not current['State']['OOMKilled'], 'original_not_cleanly_stopped')
        expected = json.loads(json.dumps(self.prepared['oldContainer']['HostConfig']))
        expected['RestartPolicy'] = {'Name': 'no', 'MaximumRetryCount': 0}
        require(current['Config'] == self.prepared['oldContainer']['Config']
                and current['HostConfig'] == expected and current['Mounts'] == self.prepared['oldContainer']['Mounts'],
                'stopped_original_role_changed')
        for path in (swap.COMPOSE, swap.OVERRIDE, swap.ROLE_COMPOSE):
            require(swap.source_record(path) == self.prepared['sourceFiles'][str(path)], 'future_source_changed')
        self.r.exclusive_volume(swap.ENGINE)
        return current

    def term_only_stop(self, state, *, wait_seconds=120):
        """One TERM to the exact running original; observation never escalates."""
        self.own_updating(state)
        original = self.r.inspect(swap.ENGINE)
        s = original['State']
        require(original['Id'] == swap.ENGINE and original['Image'] == swap.BASE
                and s['Running'] is True and s['Pid'] > 0 and not s.get('Paused')
                and not s.get('Restarting') and not s.get('OOMKilled')
                and original['HostConfig']['RestartPolicy']['Name'] == 'no'
                and type(s.get('StartedAt')) is str and s['StartedAt'], 'term_original_identity_unproven')
        identity = dict(containerId=original['Id'], imageId=original['Image'], pid=s['Pid'],
                        startedAt=s['StartedAt'], restartCount=original['RestartCount'])
        state['cleanStop'] = dict(identity=identity, signal='TERM', accepted=False,
                                  automaticEscalation=False, automaticReplay=False)
        self.checkpoint(state, 'clean_stop_term_intent')
        # Docker kill with an explicit TERM sends only that signal. Unlike stop
        # with a timeout, the daemon installs no later SIGKILL deadline.
        self.r.docker('kill', '--signal', 'TERM', swap.ENGINE, timeout=30)
        state['cleanStop']['accepted'] = True
        self.checkpoint(state, 'clean_stop_term_accepted')
        deadline = time.monotonic() + wait_seconds
        while True:
            current = self.r.inspect(swap.ENGINE)
            observed = current['State']
            require(current['Id'] == identity['containerId'] and current['Image'] == identity['imageId']
                    and observed['StartedAt'] == identity['startedAt']
                    and current['RestartCount'] == identity['restartCount']
                    and current['HostConfig']['RestartPolicy']['Name'] == 'no'
                    and not observed.get('Paused') and not observed.get('Restarting'),
                    'term_observed_identity_changed')
            if observed['Running'] is False:
                require(observed['Pid'] == 0 and observed['ExitCode'] == 0
                        and not observed.get('OOMKilled'), 'term_clean_exit_unproven')
                state['cleanStop']['exitVerified'] = True
                self.checkpoint(state, 'clean_stop_term_exit_verified')
                return
            require(observed['Running'] is True and observed['Pid'] == identity['pid'],
                    'term_running_identity_changed')
            require(time.monotonic() < deadline, 'term_observation_timeout_no_escalation')
            time.sleep(1)

    def begin_and_preserve(self):
        state = self.state('prepared')
        self.r.revalidate(self.prepared)
        require(self.r.node() == self.prepared['node'], 'prepared_node_changed')
        self.r.engine_idle(swap.ENGINE, self.prepared['command'])
        stamp = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        payload = dict(schemaVersion=1, requestId=state['requestId'], authorityId=self.prepared['authorityId'],
            expectedRevision=self.prepared['node']['revision'], drainAt=stamp, notBefore=stamp,
            afterJobId=None, targetImage=swap.TARGET)
        self.checkpoint(state, 'owned_schedule_intent')
        owned = self.r.operator.json('POST', '/v1/prototype/nodes/main/schedule-owned', payload)['receipt']
        swap.verify_owned_receipt(owned, payload)
        state['owned'] = owned
        self.checkpoint(state, 'begin_intent')
        begun = self.r.operator.json('POST', '/v1/prototype/nodes/main/begin',
            {'planId': owned['planId'], 'expectedRevision': owned['revision']})
        node = begun['node']
        import re
        require(node['state'] == 'updating' and node['planId'] == owned['planId']
                and node['revision'] == owned['revision'] + 1 and node['targetImage'] == swap.TARGET
                and type(begun['updateToken']) is str and re.fullmatch(r'[A-Za-z0-9_-]{1,128}', begun['updateToken'])
                and self.r.node() == node and self.r.authority() == self.prepared['authorityId'], 'begin_readback_unproven')
        state.update(reservedRevision=node['revision'], updateToken=begun['updateToken'])
        self.checkpoint(state, 'restart_fence_intent')
        self.r.docker('update', '--restart', 'no', swap.ENGINE)
        require(self.r.inspect(swap.ENGINE)['HostConfig']['RestartPolicy']['Name'] == 'no', 'restart_fence_unproven')
        self.checkpoint(state, 'ingress_close_intent')
        self.r.change_ingress(self.prepared, closed=True)
        self.r.engine_idle(swap.ENGINE, self.prepared['command'])
        self.checkpoint(state, 'independent_tmp_source_probe_intent')
        original_tmp = inventory_inside_container(self.r, swap.ENGINE, state['requestId'])
        swap.save(self.root / 'inactive-tmp-source.private.json', dict(schemaVersion=1,
            originalContainerId=swap.ENGINE, probeNonce=state['requestId'],
            identity=self.r.inactive_identity(self.prepared['engine']), inventory=original_tmp,
            inventorySha256=digest(canonical(original_tmp))))
        self.r.engine_idle(swap.ENGINE, self.prepared['command'])
        self.own_updating(state)
        self.term_only_stop(state)
        self.stopped(state)
        self.checkpoint(state, 'inactive_tmp_copy_intent')
        self.r.docker('cp', '--archive', swap.ENGINE + ':/var/www/tmp', str(self.root / 'tmp.before'), timeout=1800)
        inventory = inventory_directory(self.root / 'tmp.before')
        require(inventory == original_tmp and inventory_container_tmp(self.r, swap.ENGINE) == original_tmp,
                'original_tmp_copy_not_exact')
        seal = dict(schemaVersion=1, originalContainerId=swap.ENGINE,
            identity=self.r.inactive_identity(self.prepared['engine']), inventory=inventory,
            inventorySha256=digest(canonical(inventory)), originalRemoved=False)
        swap.save(self.root / 'inactive-tmp-sealed.private.json', seal)
        sync_directory(self.root)
        self.checkpoint(state, 'stopped_tmp_preserved')

    def bind_cleanup(self, template, state):
        require(template['engineTaskId'] in ALLOWED_CACHE_TASKS, 'task_cleanup_not_authorized_todd_held')
        require(template['authorityId'] == self.prepared['authorityId'] and template['nodeId'] == 'main'
                and template['containerId'] == swap.ENGINE and template['imageId'] == swap.BASE,
                'cleanup_template_wrong_role')
        value = json.loads(json.dumps(template))
        value.update(operationId=state['requestId'], maintenancePlanId=state['owned']['planId'],
            maintenanceRevision=state['reservedRevision'], maintenanceTargetImage=swap.TARGET,
            readOnlyObservers=self.r.observers)
        return value

    def plan_cleanup(self, templates):
        state = self.state('stopped_tmp_preserved')
        self.stopped(state)
        plans = []
        for template in templates:
            auth = self.bind_cleanup(template, state)
            observer = self.r.cleanup.HostObserver(auth, self.r.credentials)
            with observer.held():
                cleaner = self.r.cleanup.CacheCleanup(swap.DATA / auth['engineTaskId'], auth, observer)
                plan = cleaner.plan()
            directory = self.root / ('cleanup-' + auth['engineTaskId'])
            directory.mkdir(mode=0o700)
            swap.save(directory / 'authorization.private.json', auth)
            swap.save(directory / 'prepared.private.json', plan)
            plans.append(dict(taskId=auth['engineTaskId'], authorizationSha256=digest(canonical(auth)),
                              preparedSha256=digest(canonical(plan)), reclaimBytes=plan['reclaimBytes']))
        require(plans and len({p['taskId'] for p in plans}) == len(plans), 'unique_cleanup_tasks_required')
        swap.save(self.root / 'cleanup-set.private.json', plans)
        self.checkpoint(state, 'cleanup_plans_ready')
        return dict(cleanupSetSha256=digest(canonical(plans)), reclaimLogicalBytes=sum(p['reclaimBytes'] for p in plans))

    def apply_cleanup(self, reviewed_set_sha256):
        state = self.state('cleanup_plans_ready')
        self.stopped(state)
        plans = private_json(self.root / 'cleanup-set.private.json')
        require(digest(canonical(plans)) == reviewed_set_sha256, 'independently_reviewed_cleanup_set_required')
        self.checkpoint(state, 'cache_delete_intent')
        for item in plans:
            directory = self.root / ('cleanup-' + str(uuid.UUID(item['taskId'])))
            auth = private_json(directory / 'authorization.private.json')
            plan = private_json(directory / 'prepared.private.json')
            require(auth['engineTaskId'] in ALLOWED_CACHE_TASKS, 'task_cleanup_not_authorized_todd_held')
            require(digest(canonical(auth)) == item['authorizationSha256']
                    and digest(canonical(plan)) == item['preparedSha256'], 'cleanup_plan_bytes_changed')
            observer = self.r.cleanup.HostObserver(auth, self.r.credentials)
            with observer.held():
                cleaner = self.r.cleanup.CacheCleanup(swap.DATA / auth['engineTaskId'], auth, observer)
                cleaner.apply(plan, directory)
        self.stopped(state)
        self.checkpoint(state, 'cache_cleanup_completed')

    def finish_swap(self):
        state = self.state('cache_cleanup_completed')
        self.stopped(state)
        # tmp.before was already allocated and sealed, so real free space here
        # includes that cost. Stream verification avoids another tmp.after copy.
        self.checkpoint(state, 'retained_backup_intent')
        self.r.backup_stopped(self.prepared, self.root)
        self.checkpoint(state, 'candidate_recreate_intent')
        self.r.recreate_and_verify(self.prepared, self.root)
        candidate = private_json(self.root / 'candidate-verified.json')
        self.r.verify_restored_tmp(self.prepared, self.root, candidate['containerId'])
        self.r.exclusive_volume(candidate['containerId'])
        self.own_updating(state)
        self.checkpoint(state, 'forward_pins_intent')
        self.r.persist_pin(self.prepared)
        self.checkpoint(state, 'ingress_reopen_intent')
        self.r.change_ingress(self.prepared, closed=False)
        node = self.own_updating(state)
        self.checkpoint(state, 'finish_intent')
        result = dict(healthy=True, image=swap.TARGET, previousImage=swap.BASE,
                      retainedDataVerified=True, inactiveTmpPreserved=True, automaticRollback=False)
        finished = self.r.operator.json('POST', '/v1/prototype/nodes/main/finish',
            dict(planId=node['planId'], updateToken=state['updateToken'], status='succeeded', result=result))['node']
        require(finished['state'] == 'active' and finished['planId'] == node['planId']
                and finished['revision'] == state['reservedRevision'] + 1 and finished['targetImage'] == swap.TARGET
                and finished['result'] == result and self.r.node() == finished, 'final_owned_finish_unproven')
        self.checkpoint(state, 'completed')


def main(argv=None):
    """Thin, explicit consumer of the reviewed preparation/phase functions."""
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group()
    action.add_argument('--prepare', action='store_true')
    action.add_argument('--stage', choices=('begin', 'plan-cleanup', 'apply-cleanup', 'finish'))
    parser.add_argument('--credentials', type=Path)
    parser.add_argument('--receipt', type=Path)
    parser.add_argument('--qualification', type=Path)
    parser.add_argument('--qualification-sha256')
    parser.add_argument('--inputs', type=Path)
    parser.add_argument('--inputs-sha256')
    parser.add_argument('--prepared-sha256')
    parser.add_argument('--cleanup-set-sha256')
    args = parser.parse_args(argv)
    if not args.prepare and not args.stage:
        print(json.dumps({'mode': 'plan', 'mutations': 0, 'sourceCandidate': True,
            'target': swap.TARGET, 'nativeQualificationRequired': True,
            'explicitStages': ['begin', 'plan-cleanup', 'apply-cleanup', 'finish']}))
        return 0
    try:
        require(args.credentials is not None and args.receipt is not None, 'explicit_private_paths_required')
        if args.prepare:
            require(args.qualification is not None and args.inputs is not None, 'reviewed_inputs_required')
            qualification, inputs = private_json(args.qualification), private_json(args.inputs)
            require(digest(canonical(qualification)) == args.qualification_sha256
                    and source_inventory() == qualification['sourceHashes'], 'reviewed_qualification_source_changed')
            require(digest(canonical(inputs)) == args.inputs_sha256
                    and set(inputs) == {'readOnlyObservers', 'cleanupTemplates'}, 'reviewed_inputs_changed')
            # Check frozen code before importing the cleanup module or reading
            # credentials. prepare_fresh repeats the native/scope gates.
            runtime = CombinedRuntime(args.credentials, args.receipt, inputs['readOnlyObservers'],
                                      {k: qualification['sourceHashes'][k] for k in CLEANUP_FILES})
            result = prepare_fresh(runtime, args.qualification, args.qualification_sha256, inputs['cleanupTemplates'])
            print(json.dumps({'mode': 'prepared', **result}))
            return 0
        prepared = private_json(args.receipt / 'prepared.private.json')
        require(digest(canonical(prepared)) == args.prepared_sha256
                and prepared['sourceHashes'] == source_inventory(), 'reviewed_preparation_or_code_changed')
        runtime = CombinedRuntime(args.credentials, args.receipt, prepared['readOnlyObservers'],
                                  {k: prepared['sourceHashes'][k] for k in CLEANUP_FILES})
        operation = Operation(runtime, args.receipt, prepared, reviewed_prepared_sha256=args.prepared_sha256)
        result = operation.run_phase(args.stage, reviewed_cleanup_set_sha256=args.cleanup_set_sha256)
        state = private_json(args.receipt / 'combined.private.json')
        print(json.dumps({'mode': 'stage', 'stage': state['stage'], 'writes': True, 'result': result,
                          'automaticReplay': False, 'automaticRollback': False}))
        return 0
    except Exception as error:
        # Never echo private paths, credentials, filenames or raw Docker/API errors.
        print(json.dumps({'status': 'held', 'independentInspectionRequired': True,
                          'automaticReplay': False, 'automaticRollback': False, **safe_failure(error)}))
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
