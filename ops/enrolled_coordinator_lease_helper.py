"""Exact original-state lease holder; no Queue, runtime, engine or copy lock.

The reviewed Docker recipe runs this as its non-init PID1, UID10001, network none.
The state volume is read-only: flock on the original existing main descriptor
needs no data writes. Existing AuthorityLease owns the original external lease.
"""
import errno
import json
import os
from pathlib import Path, PurePosixPath
import re
import signal
import socket
import stat
import struct
import threading
import uuid


PROOF = '/tmp/first-cutover-lease-owner.json'


def require(condition):
    if not condition:
        raise RuntimeError('original_lease_holder_unavailable')


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def key(value):
    return [value.st_dev, value.st_ino, value.st_mode, value.st_nlink, value.st_uid, value.st_gid]


def validate_settings(value):
    require(type(value) is dict and set(value) == {'schemaVersion', 'operationId', 'planSha256',
            'authorityId', 'stateRoot', 'leaseDirectory'} and type(value['schemaVersion']) is int
            and value['schemaVersion'] == 1)
    for field in ('operationId', 'authorityId'):
        require(type(value[field]) is str and str(uuid.UUID(value[field])) == value[field])
    require(type(value['planSha256']) is str and re.fullmatch('[a-f0-9]{64}', value['planSha256']))
    for field in ('stateRoot', 'leaseDirectory'):
        path = PurePosixPath(value[field])
        require(path.is_absolute() and str(path) == value[field] and '..' not in path.parts)
    return value


def _start(pid):
    # Parse after the parenthesized comm: spaces in comm cannot shift starttime.
    return Path('/proc/' + str(pid) + '/stat').read_text().rsplit(')', 1)[1].split()[19]


def _serve(listener, settings, ready):
    try: connection, _ = listener.accept()
    except socket.timeout: return
    with connection:
        connection.settimeout(.2)
        raw = connection.recv(4097)
        require(len(raw) <= 4096)
        request = json.loads(raw)
        require(type(request) is dict and set(request) == {'challenge'}
                and type(request['challenge']) is str and re.fullmatch('[a-f0-9]{64}', request['challenge']))
        packet = dict(challenge=request['challenge'], settings=settings, pid=os.getpid(),
                      startTime=_start(os.getpid()), status='ready' if ready else 'held')
        connection.sendall(canonical(packet))


def _namespace_quiet():
    """PID1 may release only after every other live exec process has exited."""
    require(os.getpid() == 1)
    count = 0
    with os.scandir('/proc') as entries:
        for entry in entries:
            if not entry.name.isdecimal() or entry.name == '1': continue
            count += 1; require(count <= 512)
            try: fields = Path(entry.path, 'stat').read_text().rsplit(')', 1)[1].split()
            except FileNotFoundError: continue
            require(len(fields) > 19)
            if fields[0] != 'Z': return False
    return True


def hold(settings, *, proof_path=PROOF, require_pid1=False):
    validate_settings(settings)
    require(os.name == 'posix' and os.geteuid() == 10001)
    require(not require_pid1 or os.getpid() == 1)
    import fcntl
    # The image's exact reviewed package; never CWD/PYTHONPATH-based code.
    from processing.authority_lease import acquire
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    lease = acquire(settings['leaseDirectory'], settings['stateRoot'])
    external = lease._descriptor
    # From successful external acquisition onward, no ordinary error may exit
    # this process and silently release its flock. A lost proof/check is a hold.
    main = None
    main_identity = None
    external_identity = None
    listener = None
    try:
        external_identity = key(os.fstat(external))
        require(lease.authority == settings['authorityId'])
        path = Path(settings['stateRoot']) / 'coordinator.lock'
        before = path.lstat()
        require(path.resolve() == path and stat.S_ISREG(before.st_mode) and before.st_nlink == 1
                and before.st_uid == 10001 and not before.st_mode & 0o022)
        main = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
        main_identity = key(os.fstat(main))
        require(main_identity == key(before) and not os.get_inheritable(main))
        fcntl.flock(main, fcntl.LOCK_EX | fcntl.LOCK_NB)
        require(not os.get_inheritable(external))
        value = dict(schemaVersion=1, settings=settings, pid=os.getpid(), startTime=_start(os.getpid()),
                     mainDescriptor=main, leaseDescriptor=external,
                     mainIdentity=main_identity, leaseIdentity=external_identity)
        proof = Path(proof_path)
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.settimeout(.1)
        listener.bind(str(proof_path) + '.sock')
        os.chmod(str(proof_path) + '.sock', 0o600)
        listener.listen(8)
        descriptor = os.open(proof, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
        try:
            raw = canonical(value)
            require(len(raw) < 4096 and os.write(descriptor, raw) == len(raw))
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        while not stop.is_set():
            lease.check(settings['stateRoot'])
            require(key(os.fstat(main)) == key(path.lstat()) == main_identity)
            _serve(listener, settings, True)
            try:
                while os.waitpid(-1, os.WNOHANG)[0]: pass
            except ChildProcessError:
                pass
    except BaseException:
        # Best-effort status can never become a reason to drop acquired FDs.
        # The observer refuses this marker even if the earlier ready proof exists.
        try:
            held = Path(str(proof_path) + '.held')
            descriptor = os.open(held, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
            try:
                raw = canonical(dict(schemaVersion=1, status='held', settings=settings))
                require(os.write(descriptor, raw) == len(raw))
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        except BaseException:
            pass
        # Only an explicit signal to this exact helper requests release. No
        # timeout, liveness-check failure or proof-publication failure does so.
        while not stop.wait(.1):
            if listener is not None:
                try: _serve(listener, settings, False)
                except BaseException: pass
            try:
                while os.waitpid(-1, os.WNOHANG)[0]: pass
            except BaseException:
                pass
    if require_pid1:
        # An uncertain Docker-exec request can outlive its local CLI. Never
        # release the physical locks while that process may still read state.
        while True:
            reap_verified = True
            try:
                while os.waitpid(-1, os.WNOHANG)[0]: pass
            except ChildProcessError:
                pass
            except BaseException:
                reap_verified = False
            try: quiet = reap_verified and _namespace_quiet()
            except BaseException: quiet = False
            if quiet: break
            threading.Event().wait(.1)
    try:
        if main is not None:
            require(main_identity is not None and key(os.fstat(main)) == main_identity)
            os.close(main)
            try: os.fstat(main)
            except OSError as error: require(error.errno == errno.EBADF)
            else: require(False)
        require(external_identity is not None and key(os.fstat(external)) == external_identity)
        lease.close()
        try: os.fstat(external)
        except OSError as error: require(error.errno == errno.EBADF)
        else: require(False)
    except BaseException:
        # Unknown close is not a release receipt. No FD retry or forced exit.
        threading.Event().wait()
    return 0


def observe(settings, *, expected_pid=1, proof_path=PROOF):
    """Read-only actual-process/descriptor/flock proof, not a readiness boolean."""
    validate_settings(settings)
    require(type(expected_pid) is int and expected_pid > 0)
    require(not os.path.lexists(str(proof_path) + '.held'))
    # Readiness is positive evidence from this actual holder's in-memory loop,
    # not inferred from absence of a best-effort failure file.
    challenge = os.urandom(32).hex()
    endpoint = Path(str(proof_path) + '.sock'); endpoint_info = endpoint.lstat()
    require(stat.S_ISSOCK(endpoint_info.st_mode) and endpoint_info.st_uid == 10001
            and stat.S_IMODE(endpoint_info.st_mode) == 0o600)
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(.5)
        connection.connect(str(endpoint))
        peer_pid, peer_uid, _ = struct.unpack('3i', connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
        require(peer_pid == expected_pid and peer_uid == 10001)
        connection.sendall(canonical(dict(challenge=challenge)))
        raw = connection.recv(4097); require(len(raw) <= 4096)
        answer = json.loads(raw)
    require(type(answer) is dict and set(answer) == {'challenge', 'settings', 'pid', 'startTime', 'status'}
            and answer['challenge'] == challenge and answer['settings'] == settings and answer['pid'] == expected_pid
            and answer['startTime'] == _start(expected_pid) and answer['status'] == 'ready')
    path = Path(proof_path)
    before = path.lstat()
    require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and before.st_uid == 10001
            and stat.S_IMODE(before.st_mode) == 0o600 and before.st_size <= 4096)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        require(key(os.fstat(fd)) == key(before))
        raw = os.read(fd, 4097)
        require(len(raw) == before.st_size and key(os.fstat(fd)) == key(path.lstat()))
    finally: os.close(fd)
    value = json.loads(raw)
    require(type(value) is dict and set(value) == {'schemaVersion', 'settings', 'pid', 'startTime',
        'mainDescriptor', 'leaseDescriptor', 'mainIdentity', 'leaseIdentity'} and type(value['schemaVersion']) is int
        and value['schemaVersion'] == 1 and canonical(value['settings']) == canonical(settings)
        and type(value['pid']) is int and value['pid'] == expected_pid
        and value['startTime'] == _start(expected_pid))
    targets = ((value['mainDescriptor'], Path(settings['stateRoot']) / 'coordinator.lock', value['mainIdentity']),
               (value['leaseDescriptor'], Path(settings['leaseDirectory']) / (settings['authorityId'] + '.lease'), value['leaseIdentity']))
    for descriptor, target, identity in targets:
        require(type(descriptor) is int and descriptor >= 3)
        proc = Path('/proc/' + str(expected_pid))
        require(os.readlink(proc / 'fd' / str(descriptor)) == str(target)
                and key((proc / 'fd' / str(descriptor)).stat()) == key(target.lstat()) == identity)
        text = (proc / 'fdinfo' / str(descriptor)).read_text()
        require(len(text) <= 4096)
        locks = [line.split() for line in text.splitlines() if line.startswith('lock:')]
        require(len(locks) == 1 and locks[0][2:5] == ['FLOCK', 'ADVISORY', 'WRITE'])
        flags = [line.split()[1] for line in text.splitlines() if line.startswith('flags:')]
        require(len(flags) == 1 and int(flags[0], 8) & os.O_CLOEXEC)
    require(value['startTime'] == _start(expected_pid))
    return value


def serializable_source(settings, *, observer=False):
    validate_settings(settings)
    source = Path(__file__).read_text()
    # No private key or token is embedded. Configuration contains only exact
    # physical lease paths and immutable operation identity, never client rows.
    bootstrap = '''import sys,os,importlib.util,pathlib
sys.path.insert(0,"/app")
for name,expected in (("processing","/app/processing/__init__.py"),("processing.authority_lease","/app/processing/authority_lease.py")):
 spec=importlib.util.find_spec(name)
 if spec is None or spec.origin!=expected or pathlib.Path(expected).resolve()!=pathlib.Path(expected) or os.access(expected,os.W_OK):raise RuntimeError("reviewed_readonly_package_required")
'''
    tail = ('print(json.dumps(observe(' + repr(settings) + '),sort_keys=True,separators=(",",":")))'
            if observer else 'raise SystemExit(hold(' + repr(settings) + ',require_pid1=True))')
    return (bootstrap + source + '\n' + tail + '\n').encode()
