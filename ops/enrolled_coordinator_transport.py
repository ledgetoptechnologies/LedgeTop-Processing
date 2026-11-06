"""Concrete bounded Docker/HTTP boundary for the enrolled first-cutover journal.

No action runs on import. Mutating command failure is an unknown outcome: this
module never retries it, interprets stderr as identity, or invokes a shell.
The journal owns phase ordering and inspection-only recovery.
"""
from contextlib import contextmanager
import hashlib
import ipaddress
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import selectors
import signal
import stat
import subprocess
import tarfile
import time
from decimal import Decimal
import uuid


class TransportHeld(RuntimeError):
    pass


def require(condition, reason):
    if not condition:
        raise TransportHeld(reason)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def decode(raw, maximum=1024 * 1024):
    require(type(raw) is bytes and len(raw) <= maximum, 'bounded_json_required')
    def unique(items):
        result = {}
        for key, item in items:
            require(key not in result, 'duplicate_json_key')
            result[key] = item
        return result
    try:
        return json.loads(raw, object_pairs_hook=unique,
                          parse_constant=lambda _: require(False, 'finite_json_required'))
    except (ValueError, UnicodeError, RecursionError):
        raise TransportHeld('invalid_bounded_json') from None


def _terminate_owned(process):
    """Only our isolated client process group, never the Docker daemon/container."""
    # Successful/failed wait already reaped this PID after all registered pipes
    # closed. Its numeric PID/PGID can be reused; do not signal it afterward.
    if process.returncode is not None:
        return
    try:
        # Do not poll/reap the leader before checking and signalling the group.
        # Even an exited zombie still reserves its original PID/session here.
        if os.getpgid(process.pid) != process.pid:
            raise TransportHeld('owned_client_group_changed')
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=3)
    except subprocess.TimeoutExpired:
        raise TransportHeld('owned_client_cleanup_unverified') from None


class BoundedSubprocess:
    """Linux pipe I/O with output/deadline bounds, including exec stdin scripts."""
    def run(self, argv, *, input_bytes=None, timeout=30, output_limit=1024 * 1024,
            error_limit=16384, mutation=False, sink=None):
        require(os.name == 'posix', 'linux_transport_required')
        require(type(argv) in (list, tuple) and argv and all(type(value) is str and value
                and '\x00' not in value for value in argv), 'structured_command_required')
        require(type(timeout) in (int, float) and 0 < timeout <= 900 and
                type(output_limit) is int and 0 < output_limit <= (1024**4 if sink is not None else 16 * 1024 * 1024) and
                type(error_limit) is int and 0 < error_limit <= 65536,
                'bounded_command_required')
        require(sink is None or callable(sink), 'structured_stream_sink_required')
        require(input_bytes is None or type(input_bytes) is bytes and len(input_bytes) <= 1024 * 1024,
                'bounded_private_stdin_required')
        process = None
        selector = selectors.DefaultSelector()
        output, error = bytearray(), bytearray()
        output_bytes, output_hash = 0, hashlib.sha256()
        deadline, cursor = time.monotonic() + timeout, 0
        try:
            process = subprocess.Popen(list(argv), stdin=subprocess.PIPE if input_bytes is not None else subprocess.DEVNULL,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False, close_fds=True,
                start_new_session=True, bufsize=0)
            for stream, label in ((process.stdout, 'out'), (process.stderr, 'err')):
                os.set_blocking(stream.fileno(), False)
                selector.register(stream, selectors.EVENT_READ, label)
            if input_bytes is not None:
                os.set_blocking(process.stdin.fileno(), False)
                if input_bytes:
                    selector.register(process.stdin, selectors.EVENT_WRITE, 'in')
                else:
                    process.stdin.close()
            while selector.get_map():
                remaining = deadline - time.monotonic()
                require(remaining > 0, 'command_deadline_exceeded')
                for event, _ in selector.select(min(remaining, .1)):
                    stream = event.fileobj
                    if event.data == 'in':
                        try: count = os.write(stream.fileno(), input_bytes[cursor:cursor + 65536])
                        except BlockingIOError: continue
                        cursor += count
                        if cursor == len(input_bytes):
                            selector.unregister(stream); stream.close()
                    else:
                        if event.data == 'out' and sink is not None:
                            try: chunk = os.read(stream.fileno(), min(65536, output_limit + 1 - output_bytes))
                            except BlockingIOError: continue
                            if not chunk:
                                selector.unregister(stream); stream.close()
                            else:
                                output_bytes += len(chunk)
                                require(output_bytes <= output_limit, 'command_output_budget_exceeded')
                                sink(chunk)
                                output_hash.update(chunk)
                            continue
                        target, maximum = (output, output_limit) if event.data == 'out' else (error, error_limit)
                        try: chunk = os.read(stream.fileno(), min(65536, maximum + 1 - len(target)))
                        except BlockingIOError: continue
                        if not chunk:
                            selector.unregister(stream); stream.close()
                        else:
                            target.extend(chunk)
                            require(len(target) <= maximum, 'command_output_budget_exceeded')
            require(time.monotonic() < deadline, 'command_deadline_exceeded')
            remaining = deadline - time.monotonic()
            require(process.wait(timeout=remaining) == 0, 'command_failed')
            if sink is not None:
                return dict(clientExitCode=0, streamClosed=True, bytes=output_bytes,
                            sha256=output_hash.hexdigest())
            return bytes(output)
        except BaseException as error_value:
            if isinstance(error_value, (KeyboardInterrupt, SystemExit)):
                raise
            # No argv, stderr, environment or private stdin enters an exception.
            raise TransportHeld('mutation_outcome_unknown' if mutation else 'command_read_unavailable') from None
        finally:
            selector.close()
            if process is not None:
                try: _terminate_owned(process)
                finally:
                    for stream in (process.stdin, process.stdout, process.stderr):
                        if stream is not None and not stream.closed:
                            stream.close()


ID = re.compile(r'[0-9a-f]{64}\Z')
IMAGE = re.compile(r'sha256:[0-9a-f]{64}\Z')
NAME = re.compile(r'[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}\Z')
HASH = re.compile(r'[0-9a-f]{64}\Z')
OWNER_LABEL = 'org.ledgetop.first-enrolled-cutover'
ROLE_LABEL = OWNER_LABEL + '.role'
INTENT_LABEL = OWNER_LABEL + '.intent'
RECIPE_LABEL = OWNER_LABEL + '.recipe'


class DockerCLI:
    def __init__(self, *, runner=None, binary='/usr/bin/docker'):
        require(type(binary) is str and PurePosixPath(binary).is_absolute(), 'absolute_docker_binary_required')
        self.runner, self.binary = runner or BoundedSubprocess(), binary

    def command(self, *args, mutation=False, input_bytes=None, timeout=30, output_limit=1024 * 1024, sink=None):
        return self.runner.run([self.binary, '--host', 'unix:///run/docker.sock', *args],
            mutation=mutation, input_bytes=input_bytes, timeout=timeout, output_limit=output_limit, sink=sink)

    def inspect(self, identifier):
        require(type(identifier) is str and (ID.fullmatch(identifier) or NAME.fullmatch(identifier)),
                'exact_container_identity_required')
        value = decode(self.command('container', 'inspect', '--', identifier))
        require(type(value) is list and len(value) == 1 and type(value[0]) is dict
                and type(value[0].get('Id')) is str and ID.fullmatch(value[0]['Id']), 'one_exact_container_required')
        return value[0]

    def image(self, identifier):
        require(type(identifier) is str and IMAGE.fullmatch(identifier), 'immutable_image_required')
        value = decode(self.command('image', 'inspect', '--', identifier))
        require(type(value) is list and len(value) == 1 and type(value[0]) is dict and value[0].get('Id') == identifier,
                'one_exact_image_required')
        return value[0]

    def matching(self, filters):
        require(type(filters) in (tuple, list) and all(type(item) is str and
                item.startswith(('label=', 'volume=', 'name=')) and '\n' not in item for item in filters),
                'closed_container_filter_required')
        args = ['container', 'ls', '--all', '--no-trunc', '--format', '{{.ID}}']
        for value in filters: args.extend(('--filter', value))
        try: lines = self.command(*args, output_limit=65536).decode('ascii').splitlines()
        except UnicodeError: raise TransportHeld('bounded_exact_container_inventory_required') from None
        require(len(lines) <= 256 and len(set(lines)) == len(lines) and all(ID.fullmatch(item) for item in lines),
                'bounded_exact_container_inventory_required')
        return lines

    def writer_inventory(self):
        """Fresh complete mount/owner census in one bounded projected inspect.

        Do not preselect by volume or cache an earlier list: bind ancestors and
        children can alias the original volume. Environment/configuration data
        are deliberately absent from this projection.
        """
        def identities():
            raw=self.command('container','ls','--all','--no-trunc','--format','{{.ID}}',
                             output_limit=65536,timeout=30)
            try:values=raw.decode('ascii').splitlines()
            except UnicodeError:raise TransportHeld('bounded_writer_inventory_required') from None
            require(len(values)<=512 and len(set(values))==len(values) and
                    all(ID.fullmatch(value) for value in values),'bounded_writer_inventory_required')
            return values
        before=identities();rows=[]
        if before:
            projection=('{"Id":{{json .Id}},"Mounts":{{json .Mounts}},"State":{'
                '"Running":{{json .State.Running}},"Pid":{{json .State.Pid}},'
                '"FinishedAt":{{json .State.FinishedAt}}}}')
            raw=self.command('container','inspect','--format',projection,'--',*before,
                             output_limit=16*1024**2,timeout=30)
            require(type(raw) is bytes and len(raw)<=16*1024**2,'bounded_writer_projection_required')
            try:lines=raw.decode('utf8').splitlines()
            except UnicodeError:raise TransportHeld('closed_writer_projection_required') from None
            require(len(lines)==len(before),'complete_writer_projection_required')
            for line in lines:
                value=decode(line.encode(),16*1024**2)
                require(type(value) is dict and set(value)=={'Id','Mounts','State'} and
                        type(value['Id']) is str and ID.fullmatch(value['Id']), 'closed_writer_projection_required')
                state=value['State'];mounts=value['Mounts']
                require(type(state) is dict and set(state)=={'Running','Pid','FinishedAt'} and
                        type(state['Running']) is bool and type(state['Pid']) is int and
                        0<=state['Pid']<2**31 and (state['Pid']>0 if state['Running'] else state['Pid']==0) and
                        type(state['FinishedAt']) is str and len(state['FinishedAt'])<=128,
                        'closed_writer_state_required')
                require(type(mounts) is list and len(mounts)<=4096,'bounded_writer_mounts_required')
                for mount in mounts:
                    require(type(mount) is dict and {'Type','Source','Destination','RW'}<=set(mount) and
                            not set(mount)-{'Type','Name','Source','Destination','Driver','Mode','RW','Propagation'} and
                            mount['Type'] in ('bind','volume','tmpfs') and type(mount['RW']) is bool and
                            all(type(item) is str and len(item)<=4096 for name,item in mount.items() if name!='RW') and
                            mount['Destination'].startswith('/') and
                            all(ord(character)>=32 and ord(character)!=127 for character in mount['Source']) and
                            (mount['Source'].startswith('/') if mount['Type']=='volume' else
                             (mount['Source'].startswith('/') or re.match(r'^[A-Za-z]:[/\\]',mount['Source']) is not None)
                             if mount['Type']=='bind' else mount['Source']==''),
                            'closed_writer_mount_required')
                rows.append(value)
            found=[value['Id'] for value in rows]
            require(len(set(found))==len(found) and set(found)==set(before),'complete_writer_projection_required')
        after=identities()
        require(set(after)==set(before),'writer_inventory_changed_during_observation')
        return sorted(rows,key=lambda value:value['Id'])

    def stop(self, identifier):
        require(type(identifier) is str and ID.fullmatch(identifier), 'exact_container_identity_required')
        # Docker's -1 daemon timeout never escalates to SIGKILL. A client timeout
        # remains inspection-only; killing this owned CLI cannot cancel the RPC.
        self.command('container', 'stop', '--signal', 'SIGTERM', '--timeout', '-1', identifier,
                     mutation=True, timeout=60, output_limit=4096)

    def start(self, identifier):
        require(type(identifier) is str and ID.fullmatch(identifier), 'exact_container_identity_required')
        self.command('container', 'start', identifier, mutation=True, timeout=60, output_limit=4096)

    def rename(self, identifier, name):
        require(type(identifier) is str and ID.fullmatch(identifier) and type(name) is str and NAME.fullmatch(name),
                'exact_rename_binding_required')
        self.command('container', 'rename', identifier, name, mutation=True, output_limit=4096)

    def restart_policy(self, identifier, policy):
        require(type(identifier) is str and ID.fullmatch(identifier) and type(policy) is dict
                and set(policy) == {'Name', 'MaximumRetryCount'} and policy['Name'] in ('no', 'always', 'unless-stopped', 'on-failure')
                and type(policy['MaximumRetryCount']) is int and 0 <= policy['MaximumRetryCount'] <= 100,
                'exact_restart_policy_required')
        name = policy['Name']
        require(name == 'on-failure' or policy['MaximumRetryCount'] == 0, 'exact_restart_policy_required')
        if name == 'on-failure' and policy['MaximumRetryCount']:
            name += ':' + str(policy['MaximumRetryCount'])
        self.command('container', 'update', '--restart', name, identifier, mutation=True, output_limit=4096)

    def exec_python(self, identifier, source, *, timeout=30, output_limit=1024 * 1024, mutation=False):
        require(type(identifier) is str and ID.fullmatch(identifier) and type(source) is bytes,
                'exact_helper_identity_required')
        return decode(self.command('container', 'exec', '--interactive', '--user', '10001:10001', identifier,
            'python', '-I', '-', input_bytes=source, timeout=timeout, output_limit=output_limit,
            mutation=mutation), output_limit)

    def read_private_file(self, identifier, path, *, maximum=1024 * 1024):
        """Read a mounted config even from a stopped exact container, no helper start."""
        require(type(identifier) is str and ID.fullmatch(identifier), 'exact_container_identity_required')
        _absolute_path(path)
        require(path.startswith('/run/processing-reference/'), 'reviewed_config_path_required')
        raw = self.command('container', 'cp', identifier + ':' + path, '-',
                           output_limit=maximum + 131072)
        try:
            with tarfile.open(fileobj=io.BytesIO(raw), mode='r|') as archive:
                member = archive.next()
                require(member is not None and member.isfile() and not member.issym() and not member.islnk()
                        and member.name == PurePosixPath(path).name and member.uid == 10001 and member.gid == 10001
                        and member.mode == 0o600 and 0 <= member.size <= maximum,
                        'private_config_archive_required')
                stream = archive.extractfile(member)
                require(stream is not None, 'private_config_archive_required')
                with stream: value = stream.read(maximum + 1)
                require(len(value) == member.size and archive.next() is None, 'private_config_archive_required')
                return value
        except (OSError, tarfile.TarError, ValueError):
            raise TransportHeld('private_config_archive_unavailable') from None

    def archive_to(self, identifier, state_root, sink_callable, *, max_bytes, timeout):
        """Stream original read-only custody to a private caller-owned TAR sink.

        This proves clean local CLI/stream completion only. The caller must
        validate the complete TAR against held original custody before issuing
        any backup completion receipt. No extraction, restore or copy lock.
        """
        require(type(identifier) is str and ID.fullmatch(identifier) and state_root == '/prototype-state',
                'exact_original_archive_required')
        return self.command('container', 'cp', identifier + ':' + state_root + '/.', '-',
                            sink=sink_callable, output_limit=max_bytes, timeout=timeout)

    def read_private_config_set(self, identifier, path, expected):
        """Read one exact small operation directory, refusing parent symlinks.

        Docker's non-following directory TAR must include the private directory
        itself and precisely the two reviewed regular configuration files.
        """
        require(type(identifier) is str and ID.fullmatch(identifier),'exact_container_identity_required')
        _absolute_path(path)
        root=PurePosixPath(path).name
        require(PurePosixPath(path).parent==PurePosixPath('/run/processing-reference')
                and re.fullmatch(r'first-cutover-[a-f0-9-]{36}',root)
                and type(expected) is dict and set(expected)=={'bootstrap.json','final.json'}
                and all(type(pin) is str and HASH.fullmatch(pin) for pin in expected.values()),
                'exact_owned_config_directory_required')
        raw=self.command('container','cp',identifier+':'+path,'-',output_limit=2*1024*1024+131072)
        found={};directory=False;count=0
        try:
            with tarfile.open(fileobj=io.BytesIO(raw),mode='r|') as archive:
                for member in archive:
                    count+=1;require(count<=3 and member.uid==member.gid==10001,
                                     'closed_private_config_archive_required')
                    if member.name.rstrip('/')==root:
                        require(not directory and member.isdir() and member.mode==0o700,
                                'private_config_directory_required')
                        directory=True;continue
                    relative=member.name.removeprefix(root+'/')
                    require(member.name==root+'/'+relative and relative in expected and relative not in found
                            and member.isfile() and not member.issym() and not member.islnk()
                            and member.mode==0o600 and 0<=member.size<=1024*1024,
                            'private_config_member_required')
                    stream=archive.extractfile(member);require(stream is not None,'private_config_member_required')
                    with stream:value=stream.read(1024*1024+1)
                    require(len(value)==member.size and hashlib.sha256(value).hexdigest()==expected[relative],
                            'private_config_content_changed')
                    found[relative]=value
            require(directory and set(found)==set(expected),'complete_private_config_set_required')
            return found
        except (OSError,tarfile.TarError,ValueError):
            raise TransportHeld('private_config_set_unavailable') from None


def _absolute_path(value):
    require(type(value) is str and value.startswith('/') and len(value) <= 4096
            and not any(char in value for char in ('\x00', '\n', '\r', ','))
            and str(PurePosixPath(value)) == value and '..' not in PurePosixPath(value).parts,
            'reviewed_absolute_path_required')
    return value


def private_bytes(path, *, maximum=1024 * 1024):
    path = Path(_absolute_path(str(path)))
    require(path.resolve() == path and not any(parent.is_symlink() for parent in path.parents),
            'original_private_path_required')
    before = path.lstat()
    require(stat.S_ISREG(before.st_mode) and before.st_uid == os.geteuid() and before.st_nlink == 1
            and stat.S_IMODE(before.st_mode) == 0o600 and before.st_size <= maximum,
            'private_bounded_file_required')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    key = lambda value: (value.st_dev, value.st_ino, value.st_mode, value.st_uid, value.st_gid,
                         value.st_nlink, value.st_size, value.st_mtime_ns, value.st_ctime_ns)
    try:
        require(key(before) == key(os.fstat(fd)), 'private_file_identity_changed')
        chunks, count = [], 0
        while count < before.st_size:
            value = os.read(fd, min(65536, before.st_size - count))
            require(bool(value), 'private_file_changed')
            count += len(value); chunks.append(value)
        require(key(before) == key(os.fstat(fd)) == key(path.lstat()), 'private_file_changed')
        return b''.join(chunks)
    finally:
        os.close(fd)


def environment_file(path):
    value = private_bytes(path, maximum=65536)
    try: lines = value.decode('utf-8').splitlines()
    except UnicodeError: raise TransportHeld('private_environment_invalid') from None
    result = {}
    for line in lines:
        if not line or line.startswith('#'): continue
        require('=' in line, 'private_environment_invalid')
        name, item = line.split('=', 1)
        require(re.fullmatch('[A-Z][A-Z0-9_]{0,127}', name) and name not in result
                and '\x00' not in item and len(item) <= 4096, 'private_environment_invalid')
        result[name] = item
    return result


class PrivateProofs:
    """Immutable local evidence, never authority inferred from deployment state.

    The operation directory must already exist. An uncertain write is resolved
    by inspecting the exact bytes; no replacement or automatic repair occurs.
    """
    def __init__(self, root):
        self.root = Path(_absolute_path(str(root)))
        require(self.root.resolve() == self.root and
                not any(part.is_symlink() for part in self.root.parents), 'original_proof_directory_required')
        info = self.root.lstat()
        require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid() and
                stat.S_IMODE(info.st_mode) == 0o700, 'private_proof_directory_required')
        self.identity = (info.st_dev, info.st_ino)

    def _directory(self):
        fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        opened, current = os.fstat(fd), self.root.lstat()
        try:
            require((opened.st_dev, opened.st_ino) == self.identity == (current.st_dev, current.st_ino)
                    and opened.st_uid == os.geteuid() and stat.S_IMODE(opened.st_mode) == 0o700,
                    'proof_directory_identity_changed')
        except BaseException:
            os.close(fd)
            raise
        return fd

    @staticmethod
    def name(intent, dispatch=None, *, kind='facts'):
        require(type(intent) is dict and type(intent.get('intentId')) is str, 'exact_proof_intent_required')
        try: valid = str(uuid.UUID(intent['intentId'])) == intent['intentId']
        except (ValueError, AttributeError): valid = False
        require(valid and kind in ('facts', 'rejection'), 'exact_proof_intent_required')
        suffix = 'read'
        if dispatch is not None:
            require(type(dispatch) is dict and set(dispatch) == {'intentSha256', 'attempt', 'dispatchId'}
                    and dispatch['intentSha256'] == digest(intent) and type(dispatch['attempt']) is int
                    and 1 <= dispatch['attempt'] <= 128, 'exact_proof_dispatch_required')
            try: valid = str(uuid.UUID(dispatch['dispatchId'])) == dispatch['dispatchId']
            except (ValueError, AttributeError): valid = False
            require(valid, 'exact_proof_dispatch_required')
            suffix = str(dispatch['attempt']) + '-' + dispatch['dispatchId']
        require(kind != 'rejection' or dispatch is not None, 'rejection_dispatch_required')
        return kind + '-' + intent['intentId'] + '-' + suffix + '.json'

    def retain(self, intent, facts, *, dispatch=None, kind='facts', must_create=False):
        name = self.name(intent, dispatch, kind=kind)
        document = dict(schemaVersion=1, intentSha256=digest(intent), dispatch=dispatch, facts=facts)
        raw = canonical(document)
        require(len(raw) <= 1024 * 1024, 'bounded_proof_required')
        directory = self._directory()
        try:
            try:
                fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                             0o600, dir_fd=directory)
            except FileExistsError:
                require(not must_create, 'existing_dispatch_is_inspection_only')
                require(private_bytes(self.root / name) == raw, 'immutable_proof_changed')
            else:
                try:
                    offset = 0
                    while offset < len(raw):
                        count = os.write(fd, raw[offset:])
                        require(count > 0, 'proof_write_outcome_unknown')
                        offset += count
                    os.fsync(fd)
                finally:
                    os.close(fd)
                os.fsync(directory)
            require(private_bytes(self.root / name) == raw, 'retained_proof_unverified')
            # A previous write may have produced the exact bytes but failed its
            # durability barrier. Exact replay must complete both barriers on a
            # freshly verified descriptor before it may authorize lock entry.
            verified = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=directory)
            try:
                opened, current = os.fstat(verified), os.stat(name, dir_fd=directory, follow_symlinks=False)
                identity = lambda s: (s.st_dev, s.st_ino, s.st_mode, s.st_uid, s.st_gid,
                                      s.st_nlink, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
                require(identity(opened) == identity(current) and stat.S_ISREG(opened.st_mode)
                        and opened.st_uid == os.geteuid() and opened.st_nlink == 1
                        and stat.S_IMODE(opened.st_mode) == 0o600 and opened.st_size == len(raw),
                        'retained_proof_identity_changed')
                os.fsync(verified)
                os.fsync(directory)
                require(identity(opened) == identity(os.fstat(verified)) ==
                        identity(os.stat(name, dir_fd=directory, follow_symlinks=False)),
                        'retained_proof_identity_changed')
            finally:
                os.close(verified)
            require(private_bytes(self.root / name) == raw, 'retained_proof_unverified')
            # Revalidate the owned directory after the artifact, too.
            current = self._directory(); os.close(current)
        finally:
            os.close(directory)
        return hashlib.sha256(raw).hexdigest()


class HostOperation:
    """Reuse the existing fixed installation lock, retaining one active plan.

    The binding is deliberately not deleted on exit or terminal success. A new
    operation requires a separately reviewed rebaseline, not a new receipt dir.
    """
    def __init__(self, operation_directory):
        self.proofs = PrivateProofs(operation_directory)

    @contextmanager
    def locked(self, plan):
        from ops.install_reference_coordinator import operation_lock, LOCK_PATH
        from ops.enrolled_coordinator_cutover import validate_plan
        plan = validate_plan(plan)
        with operation_lock():
            parent = PrivateProofs(LOCK_PATH.parent)
            binding = dict(schemaVersion=1, operationId=plan['operationId'], planSha256=digest(plan),
                           directoryIdentity=list(self.proofs.identity))
            # The fixed logical identity makes a second operation collide even
            # if it supplies a new private operation/receipt directory.
            fixed = dict(intentId='b769525d-8f74-5b46-8bd6-430739377242')
            parent.retain(fixed, binding)
            yield


class TrackedExec:
    """One private remote request, never re-dispatched after local uncertainty.

    The owning cutover journal supplies an exact request UUID derived from its
    current dispatch nonce. This registry proves observer lifecycle only; it
    does not establish whether an operator POST's deployment effect occurred.
    """
    TARGET_FIELD = 'containerId'
    PROTOCOL = 'ltds-cutover-exec-v1'

    def __init__(self, docker, operation_id, operation_directory, *, owner_uid=10001):
        require(type(owner_uid) is int and 0<owner_uid<2**31,'reviewed_remote_owner_required')
        self.docker = docker
        self.operation_id = operation_id
        self.owner_uid = owner_uid
        self.proofs = PrivateProofs(operation_directory)

    def _binding(self, identifier, request_id, source, maximum):
        from ops import enrolled_coordinator_exec_helper as helper
        require(type(identifier) is str and ID.fullmatch(identifier) and type(source) is bytes,
                'exact_remote_request_required')
        settings = dict(operationId=self.operation_id, requestId=request_id,
                        sourceSha256=hashlib.sha256(source).hexdigest(), maximum=maximum)
        if self.owner_uid != 10001: settings['ownerUid']=self.owner_uid
        helper.validate(settings)
        intent = dict(intentId=request_id, settings=settings, **{self.TARGET_FIELD:identifier})
        return helper, settings, intent

    def dispatch(self, identifier, request_id, source, *, timeout=30, maximum=1024 * 1024):
        helper, settings, intent = self._binding(identifier, request_id, source, maximum)
        # Durable first, exclusive second-call refusal even if Docker never
        # returned or the remote request directory is absent.
        self.proofs.retain(intent, dict(dispatched=True), must_create=True)
        self.docker.exec_python(identifier, helper.serializable_source(settings, source=source),
                                timeout=timeout, output_limit=4096, mutation=True)
        return self.inspect(identifier, request_id, source, maximum=maximum)

    def reserved(self, identifier, request_id, source, *, maximum=1024 * 1024):
        _, _, intent = self._binding(identifier, request_id, source, maximum)
        path = self.proofs.root / self.proofs.name(intent)
        directory = self.proofs._directory(); os.close(directory)
        # Any entry, including malformed/partial evidence, is reserved. It may
        # make inspection unavailable; it must never become fresh execution.
        return os.path.lexists(path)

    def __call__(self, identifier, source, *, request_id, timeout=30, output_limit=1024 * 1024):
        if self.reserved(identifier, request_id, source, maximum=output_limit):
            return self.inspect(identifier, request_id, source, maximum=output_limit)
        return self.dispatch(identifier, request_id, source, timeout=timeout, maximum=output_limit)

    def inspect(self, identifier, request_id, source, *, maximum=1024 * 1024, allow_failed=False):
        helper, settings, intent = self._binding(identifier, request_id, source, maximum)
        expected = dict(schemaVersion=1, intentSha256=digest(intent), dispatch=None,
                        facts=dict(dispatched=True))
        require(private_bytes(self.proofs.root / self.proofs.name(intent)) == canonical(expected),
                'exact_prior_remote_dispatch_required')
        value = self.docker.exec_python(identifier, helper.serializable_source(settings),
                                       timeout=5, output_limit=min(16 * 1024 * 1024, maximum + 16384))
        require(type(value) is dict and set(value) == {'settings', 'processRetired', 'pid', 'start', 'success', 'result'}
                and value['settings'] == settings and value['processRetired'] is True
                and type(value['pid']) is int and value['pid'] > 0 and type(value['start']) is str
                and value['start'].isdigit() and type(value['success']) is bool,
                'actual_remote_retirement_required')
        envelope = dict(schemaVersion=1, protocol=self.PROTOCOL, operationId=self.operation_id,
                        requestId=request_id, sourceSha256=settings['sourceSha256'], **{self.TARGET_FIELD:identifier},
                        process=dict(pid=value['pid'], startTicks=value['start'], retired=True),
                        bodySucceeded=value['success'], result=value['result'])
        retired = dict(intentId=str(uuid.uuid5(uuid.UUID(request_id), 'remote-retired')), request=intent)
        # Retain only result digest; full client metadata never enters the
        # controller lifecycle artifact or an exception.
        self.proofs.retain(retired, dict(process=envelope['process'], bodySucceeded=envelope['bodySucceeded'],
                                        resultSha256=digest(value['result'])))
        require(allow_failed or value['success'], 'remote_script_failed_after_retirement')
        return envelope


RECIPE_FIELDS = frozenset(('schemaVersion', 'name', 'imageId', 'user', 'workingDir', 'environmentFile',
    'environmentSha256', 'mounts', 'ports', 'network', 'tmpfs', 'labels', 'command', 'entrypoint',
    'readOnly', 'init', 'pidsLimit', 'memoryBytes', 'nanoCpus', 'stopSignal', 'stopTimeout', 'restartPolicy'))


def validate_recipe(recipe, *, bootstrap=False):
    require(type(recipe) is dict and set(recipe) == RECIPE_FIELDS and type(recipe['schemaVersion']) is int
            and recipe['schemaVersion'] == 1, 'closed_docker_recipe_required')
    require(type(recipe['name']) is str and NAME.fullmatch(recipe['name'])
            and type(recipe['imageId']) is str and IMAGE.fullmatch(recipe['imageId'])
            and recipe['user'] == '10001:10001' and recipe['workingDir'] == '/app'
            and recipe['readOnly'] is True and type(recipe['init']) is bool
            and recipe['stopSignal'] == 'SIGTERM' and type(recipe['stopTimeout']) is int and recipe['stopTimeout'] == -1,
            'reviewed_unprivileged_recipe_required')
    _absolute_path(recipe['environmentFile'])
    require(type(recipe['environmentSha256']) is str and HASH.fullmatch(recipe['environmentSha256']),
            'private_environment_binding_required')
    require(type(recipe['labels']) is dict and len(recipe['labels']) <= 32 and all(
        type(key) is str and re.fullmatch('[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}', key)
        and not key.startswith(OWNER_LABEL) and type(value) is str and len(value) <= 256
        and all(ord(char) >= 32 for char in value) for key, value in recipe['labels'].items()),
        'closed_container_labels_required')
    require(recipe['entrypoint'] in (None, ['python']) and type(recipe['command']) is list
            and recipe['command'] in (['python', '-m', 'processing.app'], ['-m', 'processing.app']),
            'normal_application_command_required')
    require((recipe['entrypoint'] is None) == (recipe['command'][0] == 'python'),
            'normal_application_command_required')
    require(type(recipe['pidsLimit']) is int and 0 <= recipe['pidsLimit'] <= 1024
            and type(recipe['memoryBytes']) is int and 0 <= recipe['memoryBytes'] <= 16 * 1024**3
            and type(recipe['nanoCpus']) is int and 0 <= recipe['nanoCpus'] <= 16 * 10**9,
            'reviewed_resource_recipe_required')
    require(type(recipe['network']) is str and (recipe['network'] == 'none' if bootstrap else
            recipe['network'] == 'bridge' or re.fullmatch('[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}', recipe['network'])
            and recipe['network'] not in ('host', 'none')), 'reviewed_network_required')
    require(recipe['restartPolicy'] == {'Name': 'no', 'MaximumRetryCount': 0}
            and type(recipe['restartPolicy']['MaximumRetryCount']) is int, 'disabled_candidate_restart_required')
    require(type(recipe['mounts']) is list and 2 <= len(recipe['mounts']) <= 8, 'closed_mount_recipe_required')
    targets = set()
    for mount in recipe['mounts']:
        require(type(mount) is dict and set(mount) == {'type', 'source', 'target', 'readOnly'}
                and mount['type'] in ('volume', 'bind') and type(mount['readOnly']) is bool,
                'closed_mount_recipe_required')
        target = _absolute_path(mount['target'])
        require(target not in targets and target not in ('/', '/app', '/usr', '/bin', '/lib', '/lib64', '/etc', '/proc', '/sys', '/dev')
                and not target.startswith(('/app/', '/usr/', '/bin/', '/lib/', '/lib64/', '/etc/', '/proc/', '/sys/', '/dev/'))
                and 'docker.sock' not in target, 'application_source_or_host_control_mount_refused')
        require(not any(target.startswith(old + '/') or old.startswith(target + '/') for old in targets),
                'overlapping_mount_recipe_refused')
        targets.add(target)
        if mount['type'] == 'bind': _absolute_path(mount['source'])
        else: require(type(mount['source']) is str and NAME.fullmatch(mount['source']), 'named_state_volume_required')
    require('/prototype-state' in targets, 'original_state_mount_required')
    require(type(recipe['ports']) is list and len(recipe['ports']) <= 8 and (not bootstrap or not recipe['ports']),
            'closed_port_recipe_required')
    seen = set()
    for port in recipe['ports']:
        require(type(port) is dict and set(port) == {'containerPort', 'hostIp', 'hostPort'}
                and type(port['containerPort']) is str and re.fullmatch(r'[1-9][0-9]{0,4}/tcp', port['containerPort'])
                and int(port['containerPort'].split('/')[0]) <= 65535
                and type(port['hostPort']) is int and 0 < port['hostPort'] <= 65535,
                'closed_port_recipe_required')
        try: address = ipaddress.ip_address(port['hostIp'])
        except (ValueError, TypeError): raise TransportHeld('literal_private_bind_address_required') from None
        require((address.is_private or address.is_loopback) and not address.is_unspecified,
                'literal_private_bind_address_required')
        binding = (port['hostIp'], port['hostPort'])
        require(binding not in seen, 'duplicate_port_binding')
        seen.add(binding)
    require(type(recipe['tmpfs']) is dict and set(recipe['tmpfs']) == {'/tmp'}
            and type(recipe['tmpfs']['/tmp']) is str and re.fullmatch(
                r'rw,nosuid,nodev,noexec,size=(?:64|128|192|256)m,mode=1777', recipe['tmpfs']['/tmp']),
            'bounded_private_tmpfs_required')
    return json.loads(canonical(recipe))


def create_arguments(recipe, *, operation_id, role, intent_id, holder_settings=None):
    recipe = validate_recipe(recipe, bootstrap=role in ('bootstrap', 'lease-holder'))
    require(role in ('bootstrap', 'final', 'lease-holder') and type(operation_id) is str and type(intent_id) is str
            and re.fullmatch('[0-9a-f-]{36}', operation_id) and re.fullmatch('[0-9a-f-]{36}', intent_id),
            'exact_owned_recipe_binding_required')
    holder_source = None
    if role == 'lease-holder':
        from ops.enrolled_coordinator_lease_helper import validate_settings, serializable_source
        validate_settings(holder_settings)
        require(holder_settings['operationId'] == operation_id and recipe['init'] is False
                and recipe['network'] == 'none' and recipe['ports'] == []
                and holder_settings['stateRoot'] == '/prototype-state', 'isolated_pid1_holder_required')
        state = [m for m in recipe['mounts'] if m['target'] == holder_settings['stateRoot']]
        external = [m for m in recipe['mounts'] if m['target'] == holder_settings['leaseDirectory']]
        require(len(state) == len(external) == 1 and state[0]['type'] == 'volume'
                and state[0]['readOnly'] is True and external[0]['type'] == 'bind'
                and external[0]['readOnly'] is False, 'original_readonly_state_and_external_holder_mounts_required')
        require(all(m['readOnly'] or m is external[0] for m in recipe['mounts']),
                'holder_other_writable_mount_refused')
        holder_source = serializable_source(holder_settings).decode('utf8')
    else:
        require(holder_settings is None, 'holder_settings_on_application_refused')
    bound_recipe = recipe if holder_source is None else dict(recipe=recipe, holderSettings=holder_settings,
                          holderSourceSha256=hashlib.sha256(holder_source.encode('utf8')).hexdigest())
    labels = dict(recipe['labels'], **{OWNER_LABEL: operation_id, ROLE_LABEL: role,
        INTENT_LABEL: intent_id, RECIPE_LABEL: digest(bound_recipe)})
    args = ['container', 'create', '--name', recipe['name'], '--pull', 'never', '--read-only',
            '--user', recipe['user'], '--workdir', recipe['workingDir'], '--network', recipe['network'],
            '--restart', 'no', '--stop-signal', 'SIGTERM', '--stop-timeout', '-1',
            '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges', '--env-file', recipe['environmentFile']]
    if recipe['init']: args.append('--init')
    if recipe['pidsLimit']: args.extend(('--pids-limit', str(recipe['pidsLimit'])))
    if recipe['memoryBytes']: args.extend(('--memory', str(recipe['memoryBytes']), '--memory-swap', str(recipe['memoryBytes'])))
    if recipe['nanoCpus']: args.extend(('--cpus', format(Decimal(recipe['nanoCpus']) / Decimal(10**9), 'f')))
    for key, value in sorted(labels.items()): args.extend(('--label', key + '=' + value))
    for mount in recipe['mounts']:
        value = 'type=' + mount['type'] + ',src=' + mount['source'] + ',dst=' + mount['target']
        if mount['readOnly']: value += ',readonly'
        args.extend(('--mount', value))
    for port in recipe['ports']:
        address = '[' + port['hostIp'] + ']' if ':' in port['hostIp'] else port['hostIp']
        args.extend(('--publish', address + ':' + str(port['hostPort']) + ':' + port['containerPort']))
    args.extend(('--tmpfs', '/tmp:' + recipe['tmpfs']['/tmp']))
    if holder_source is not None:
        # Source has public physical/operation bindings only; credentials and
        # census keys are never serialized into argv or this holder command.
        args.extend(('--no-healthcheck', '--entrypoint', 'python', recipe['imageId'], '-I', '-c', holder_source))
    else:
        if recipe['entrypoint']: args.extend(('--entrypoint', recipe['entrypoint'][0]))
        args.extend((recipe['imageId'], *recipe['command']))
    return args


def immutable_container_recipe(item):
    """Exclude only name/state/restart policy, each guarded separately by phase."""
    require(type(item) is dict and type(item.get('Config')) is dict and type(item.get('HostConfig')) is dict,
            'complete_container_inspection_required')
    config, host = item['Config'], item['HostConfig']
    fields = ('User', 'WorkingDir', 'Entrypoint', 'Cmd', 'Healthcheck', 'Labels', 'StopSignal', 'StopTimeout')
    host_fields = ('ReadonlyRootfs', 'CapAdd', 'CapDrop', 'SecurityOpt', 'Privileged', 'PidMode', 'IpcMode',
        'UsernsMode', 'NetworkMode', 'PortBindings', 'Tmpfs', 'Init', 'PidsLimit', 'Memory', 'MemorySwap',
        'NanoCpus', 'Binds', 'Devices', 'DeviceRequests', 'VolumesFrom', 'ExtraHosts', 'Dns', 'AutoRemove')
    mounts = []
    require(type(item.get('Mounts')) is list and len(item['Mounts']) <= 16, 'bounded_mount_inventory_required')
    for value in item['Mounts']:
        require(type(value) is dict and value.get('Type') in ('bind', 'volume'), 'reviewed_mount_inventory_required')
        mounts.append({key: value.get(key) for key in ('Type', 'Name', 'Source', 'Destination', 'RW', 'Propagation', 'Driver')})
    return dict(imageId=item.get('Image'), config={key: config.get(key) for key in fields},
                host={key: host.get(key) for key in host_fields},
                mounts=sorted(mounts, key=lambda value: value['Destination']))


def container_environment(item):
    values = item.get('Config', {}).get('Env')
    require(type(values) is list and len(values) <= 128, 'bounded_container_environment_required')
    result = {}
    for value in values:
        require(type(value) is str and '=' in value and len(value) <= 8192, 'closed_container_environment_required')
        name, content = value.split('=', 1)
        require(name not in result and re.fullmatch('[A-Za-z_][A-Za-z0-9_]{0,127}', name),
                'closed_container_environment_required')
        result[name] = content
    return result


def operator_script(method, path, payload=None, *, owner_digest=False):
    """Real normal operator HTTP; token is read inside the exact container."""
    require(method in ('GET', 'POST') and path in ('/healthz', '/v1/prototype/authority',
            '/v1/prototype/coordinator-freeze', '/v1/prototype/coordinator-custody')
            and (method == 'GET' or path == '/v1/prototype/coordinator-freeze'), 'fixed_operator_rpc_required')
    require(type(owner_digest) is bool and (not owner_digest or method=='GET' and path=='/v1/prototype/coordinator-freeze'),
            'readonly_gate_owner_digest_required')
    packet = canonical(dict(method=method, path=path, payload=payload,ownerDigest=owner_digest))
    return ('''import http.client,json,os,hashlib
request=json.loads(''' + repr(packet) + ''')
token=os.environ.get('PROCESSING_PROTOTYPE_OPERATOR_TOKEN','')
if len(token)<32:raise RuntimeError('operator_credential_unavailable')
port=int(os.environ.get('PROCESSING_PORT','8080'))
if port!=8080:raise RuntimeError('reviewed_native_listener_required')
connection=http.client.HTTPConnection('127.0.0.1',port,timeout=5)
body=json.dumps(request['payload'],sort_keys=True,separators=(',',':')).encode() if request['payload'] is not None else None
try:
 connection.request(request['method'],request['path'],body=body,headers={'Authorization':'Bearer '+token,'Content-Type':'application/json','Accept':'application/json','Connection':'close'})
 response=connection.getresponse();raw=response.read(65537)
 if len(raw)>65536 or response.getheader('Content-Encoding','identity')!='identity' or response.getheader('Content-Type','').split(';')[0]!='application/json':raise RuntimeError('operator_rpc_unavailable')
 result=dict(status=response.status,body=json.loads(raw))
 if request['ownerDigest']:result['ownerSha256']=hashlib.sha256(token.encode()).hexdigest()
 print(json.dumps(result,sort_keys=True,separators=(',',':')))
finally:connection.close()
''').encode()


def startup_health_script():
    """One retired read-only observer, inside the existing 30-second RPC budget.

    Only a refused connect before any HTTP request is sent may wait. A timeout,
    lost response, HTTP error, or malformed readiness body is never retried.
    """
    source = '''import errno,http.client,json,os,time,signal
from pathlib import Path
def incarnation():
 return Path('/proc/1/stat').read_text().rsplit(')',1)[1].split()[19]
original=incarnation()
token=os.environ.get('PROCESSING_PROTOTYPE_OPERATOR_TOKEN','')
if len(token)<32:raise RuntimeError('operator_credential_unavailable')
if int(os.environ.get('PROCESSING_PORT','8080'))!=8080:raise RuntimeError('reviewed_native_listener_required')
deadline=time.monotonic()+20
def expired(signum,frame):raise TimeoutError('startup_health_deadline')
if signal.getitimer(signal.ITIMER_REAL)!=(0.0,0.0):raise RuntimeError('startup_observer_timer_in_use')
signal.signal(signal.SIGALRM,expired)
signal.setitimer(signal.ITIMER_REAL,20)
while True:
 if incarnation()!=original:raise RuntimeError('startup_incarnation_changed')
 remaining=deadline-time.monotonic()
 if remaining<=0:raise RuntimeError('startup_health_deadline')
 connection=http.client.HTTPConnection('127.0.0.1',8080,timeout=min(5,remaining))
 try:
  try:connection.connect()
  except OSError as error:
   if error.errno!=errno.ECONNREFUSED:raise
   connection.close()
   remaining=deadline-time.monotonic()
   if remaining<=0:raise RuntimeError('startup_health_deadline')
   time.sleep(min(.1,remaining))
   continue
  connection.request('GET','/healthz',headers={'Authorization':'Bearer '+token,'Accept':'application/json','Connection':'close'})
  response=connection.getresponse();raw=response.read(65537)
  if response.status!=200:raise RuntimeError('actual_application_health_required')
  if len(raw)>65536 or response.getheader('Content-Encoding','identity')!='identity' or response.getheader('Content-Type','').split(';')[0]!='application/json':raise RuntimeError('operator_rpc_unavailable')
  body=json.loads(raw)
  if type(body) is not dict or body.get('status')!='ok' or body.get('mode')!='routing_probe':raise RuntimeError('invalid_startup_health_body')
  if incarnation()!=original:raise RuntimeError('startup_incarnation_changed')
  if time.monotonic()>deadline:raise RuntimeError('startup_health_deadline')
  print(json.dumps(dict(status=200,body=body),sort_keys=True,separators=(',',':')))
  break
 finally:connection.close()
'''
    # The tracked wrapper executes this on its main thread in a fresh Linux
    # process. The alarm also bounds slow headers/body/parsing, and is retired
    # before the wrapper writes its own completion/retirement records.
    return ('import signal\ndef observe():\n'+''.join(' '+line+'\n' for line in source.splitlines())+
            'try:\n observe()\nfinally:\n signal.setitimer(signal.ITIMER_REAL,0)\n').encode()


def gate_receipt_script(request):
    """Read exact existing receipt. This never repeats a POST as a lookup."""
    fields = {'requestId', 'planId', 'expectedRevision', 'action', 'startupId'}
    require(type(request) is dict and set(request) == fields, 'closed_gate_request_required')
    return ('''import hashlib,json,os,sqlite3
request=json.loads(''' + repr(canonical(request)) + ''')
wire=json.dumps(request,sort_keys=True,separators=(',',':'))
owner=hashlib.sha256(os.environ['PROCESSING_PROTOTYPE_OPERATOR_TOKEN'].encode('ascii')).hexdigest()
path=os.environ.get('PROCESSING_PROTOTYPE_STATE_DIR','')
if path!='/prototype-state':raise RuntimeError('original_state_path_required')
db=sqlite3.connect('file:/prototype-state/jobs.sqlite3?mode=ro',uri=True,timeout=3)
db.row_factory=sqlite3.Row
try:
 db.execute('PRAGMA query_only=ON');db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH,16384)
 row=db.execute('SELECT * FROM coordinator_freeze_receipts WHERE request_id=?',(request['requestId'],)).fetchone()
 if row is None:print(json.dumps(dict(found=False)))
 else:
  if row['plan_id']!=request['planId'] or row['owner_sha256']!=owner or row['request_json']!=wire or row['request_sha256']!=hashlib.sha256(wire.encode()).hexdigest():raise RuntimeError('gate_receipt_binding_changed')
  print(json.dumps(dict(found=True,ownerSha256=owner,requestSha256=row['request_sha256'],receipt=json.loads(row['receipt_json'])),sort_keys=True,separators=(',',':')))
finally:db.close()
''').encode()


def physical_binding_script(authority_id, lease_directory, *, process_id=None, include_source=False):
    """Independent physical identity; no expected values come from candidate rows."""
    _absolute_path(lease_directory)
    packet = canonical(dict(authorityId=authority_id, leaseDirectory=lease_directory,
                            processId=process_id, includeSource=include_source))
    return ('''import hashlib,json,os,stat
from pathlib import Path
settings=json.loads(''' + repr(packet) + ''')
def key(value):return [value.st_dev,value.st_ino,value.st_mode,value.st_nlink,value.st_uid,value.st_gid]
def regular(path,private=True):
 p=Path(path);s=p.lstat()
 if p.resolve()!=p or not stat.S_ISREG(s.st_mode) or s.st_nlink!=1 or s.st_uid!=10001 or s.st_mode & (0o077 if private else 0o022):raise RuntimeError('original_private_file_required')
 return s
root=Path('/prototype-state');rs=root.lstat();db=root/'jobs.sqlite3';lock=root/'coordinator.lock'
if root.resolve()!=root or not stat.S_ISDIR(rs.st_mode) or rs.st_uid!=10001 or stat.S_IMODE(rs.st_mode) not in (0o700,0o755):raise RuntimeError('original_state_root_required')
ds=regular(db);ls=regular(lock,False);directory=Path(settings['leaseDirectory']);di=directory.lstat();lease=directory/(settings['authorityId']+'.lease');es=regular(lease)
if directory.resolve()!=directory or not stat.S_ISDIR(di.st_mode) or di.st_uid!=10001 or stat.S_IMODE(di.st_mode) not in (0o700,0o711):raise RuntimeError('original_lease_directory_required')
external=dict(authorityId=settings['authorityId'],directory=str(directory),directoryIdentity=[di.st_dev,di.st_ino,di.st_mode,di.st_uid,di.st_gid],path=str(lease),fileIdentity=key(es))
operation=dict(authorityId=settings['authorityId'],database=str(db),databaseIdentity=[ds.st_dev,ds.st_ino],root=str(root),rootIdentity=[rs.st_dev,rs.st_ino],lease=str(lock),leaseIdentity=[ls.st_dev,ls.st_ino,ls.st_mode,ls.st_nlink,ls.st_uid],authorityLease=external)
result=dict(state=dict(root=[rs.st_dev,rs.st_ino,rs.st_mode,rs.st_uid,rs.st_gid],database=key(ds),main=key(ls)),authorityLease=external,operationBinding=operation)
if settings['includeSource']:
 source=root/'source-readers';ss=source.lstat();sp=source/'lease';si=regular(sp)
 if source.resolve()!=source or not stat.S_ISDIR(ss.st_mode) or stat.S_IMODE(ss.st_mode)!=0o700 or ss.st_uid!=10001:raise RuntimeError('source_reader_root_required')
 fd=os.open(sp,os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC)
 try:
  marker=os.pread(fd,65,0)
  if len(marker)!=64 or any(c not in b'0123456789abcdef' for c in marker) or key(os.fstat(fd))!=key(si):raise RuntimeError('source_marker_unavailable')
 finally:os.close(fd)
 binding=dict(authorityId=settings['authorityId'],database=str(db),databaseIdentity=[ds.st_dev,ds.st_ino],root=str(source),rootIdentity=[ss.st_dev,ss.st_ino],leaseIdentity=[si.st_dev,si.st_ino,si.st_size,si.st_mode,si.st_nlink,si.st_uid,si.st_mtime_ns,si.st_ctime_ns],marker=marker.hex())
 pid=settings['processId']
 if type(pid) is not int or pid<=0:raise RuntimeError('actual_runtime_process_required')
 target='/proc/'+str(pid);start=Path(target+'/stat').read_text().rsplit(')',1)[1].split()[19]
 descriptors=[]
 for index,path in enumerate(Path(target+'/fd').iterdir()):
  if index>=512:raise RuntimeError('runtime_descriptor_budget_exceeded')
  try:
   if os.readlink(path)!=str(sp):continue
   if key(path.stat())!=key(si):raise RuntimeError('runtime_source_descriptor_changed')
   text=Path(target+'/fdinfo/'+path.name).read_text()
   locks=[line.split() for line in text.splitlines() if line.startswith('lock:')]
   if len(text)>4096 or len(locks)!=1 or locks[0][2:5]!=['FLOCK','ADVISORY','WRITE']:raise RuntimeError('runtime_source_flock_unavailable')
   descriptors.append(int(path.name))
  except FileNotFoundError:continue
 if len(descriptors)!=1 or start!=Path(target+'/stat').read_text().rsplit(')',1)[1].split()[19]:raise RuntimeError('runtime_source_owner_unavailable')
 result.update(sourceBinding=binding,sourceBindingSha256=hashlib.sha256(json.dumps(binding,sort_keys=True,separators=(',',':')).encode()).hexdigest(),sourceMarkerSha256=hashlib.sha256(marker).hexdigest(),processStartTicks=start)
print(json.dumps(result,sort_keys=True,separators=(',',':')))
''').encode()
