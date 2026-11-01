"""INACTIVE bounded engine admission candidate; no listener or live wiring.

This control-body prototype deliberately refuses bodies above 64 KiB. It is NOT
ready for production image uploads or result downloads. The caller must supply
the actual sole-owner Queue on the same trusted host as the canonical NodeGate.
A remote auth_request/boolean check cannot preserve this guard's lifetime.

Only init/upload/commit POSTs are recognized. Cancel/remove/restart and unknown
methods/routes are denied. Reads have a distinct credential and a closed list.
The actual NodeGate lock and SQLite write transaction stay held through one
fixed-loopback HTTP exchange, with an absolute deadline and bounded response.
An uncertain exchange retains its durable host reservation; it is never replayed.
"""
from contextlib import contextmanager
from dataclasses import dataclass, field
import errno
import hashlib
import hmac
import json
import re
import select
import socket
import os
import subprocess
import stat
import sqlite3
from pathlib import Path
import time
from urllib.parse import urlencode
import uuid

from .node_gate import NodeGate, GateError
from .prototype import VIEWER_PROFILE

MAX_BODY = 65536
MAX_HEADERS = 16384
UUID = r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'


class AdmissionError(ValueError):
    """Sanitized refusal; never includes credentials, URLs or response bodies."""


@contextmanager
def _queue_lifecycle(queue, deadline):
    # Take this mutex BEFORE the host gate. Queue._db() takes it recursively;
    # waiting there without a timeout would evade the network/SQLite budget.
    if os.getpid() != queue._creator_pid:
        raise AdmissionError('coordinator process changed')
    remaining = deadline - time.monotonic()
    if remaining <= 0 or not queue._lifecycle_lock.acquire(timeout=remaining):
        raise AdmissionError('bounded coordinator lifecycle deadline exceeded')
    try:
        if queue._lock.closed:
            raise AdmissionError('coordinator lease closed')
        yield
    finally:
        queue._lifecycle_lock.release()


@dataclass(frozen=True)
class OwnerPin:
    """Trusted installation pin, never accepted from HTTP request fields.

    Capturing a pin does not attest which database an operator should select.
    The authentic sole-owner service must supply that already-qualified Queue.
    A cloned database with the same authority UUID is not interchangeable.
    """
    database_path: str
    database_device: int
    database_inode: int
    lock_device: int
    lock_inode: int
    startup_id: str

    @classmethod
    def capture(cls, queue):
        try:
            with _queue_lifecycle(queue, time.monotonic() + 2), queue._db():
                path = Path(queue.path)
                if not path.is_absolute() or path.is_symlink() or path.resolve() != path:
                    raise AdmissionError('canonical owner database required')
                database = path.lstat()
                lease = os.fstat(queue._lock.fileno())
                lease_path = (path.parent / 'coordinator.lock').lstat()
                if (not stat.S_ISREG(database.st_mode) or not stat.S_ISREG(lease.st_mode)
                        or (lease.st_dev, lease.st_ino) != (lease_path.st_dev, lease_path.st_ino)):
                    raise AdmissionError('actual canonical owner lease required')
                return cls(str(path), database.st_dev, database.st_ino, lease.st_dev,
                           lease.st_ino, queue.startup_id)
        except (OSError, RuntimeError, ValueError):
            raise AdmissionError('canonical owner lease unavailable') from None


@dataclass(frozen=True)
class Binding:
    engine_id: str
    node_id: str
    authority_id: str
    coordinator_origin: str
    generation: int
    backend_port: int
    coordinator_secret: str = field(repr=False)
    management_secret: str = field(repr=False)
    backend_secret: str = field(repr=False)
    deadline_seconds: float = 5

    def __post_init__(self):
        if (self.node_id not in ('cluster', 'main') or type(self.generation) is not int
                or self.generation < 1 or type(self.backend_port) is not int
                or not 1024 <= self.backend_port <= 65535
                or type(self.deadline_seconds) not in (int, float)
                or not 0 < self.deadline_seconds <= 10):
            raise AdmissionError('fixed binding and bounded deadline required')
        secrets = (self.coordinator_secret, self.management_secret, self.backend_secret)
        if (any(not isinstance(v, str) or not 32 <= len(v) <= 4096
                or any(ord(c) < 33 or ord(c) > 126 for c in v) for v in secrets)
                or len(set(secrets)) != 3):
            raise AdmissionError('three distinct private credentials required')


def _headers(pairs):
    result = {}
    if not isinstance(pairs, (list, tuple)) or len(pairs) > 32:
        raise AdmissionError('bounded explicit headers required')
    for key, value in pairs:
        if (not isinstance(key, str) or not isinstance(value, str) or not key.isascii()
                or not re.fullmatch(r'[A-Za-z0-9-]{1,64}', key) or len(value) > 4096
                or any(ord(c) < 32 or ord(c) > 126 for c in value)):
            raise AdmissionError('invalid header')
        key = key.lower()
        if key in result:
            raise AdmissionError('duplicate header refused')
        result[key] = value
    return result


def _authenticate(headers, secret):
    supplied = headers.get('authorization', '')
    if not hmac.compare_digest(supplied, 'Bearer ' + secret):
        raise AdmissionError('request credential refused')


def _identifier(value):
    if not isinstance(value, str) or not re.fullmatch(UUID, value):
        raise AdmissionError('canonical identity required')
    return value


def local_engine_identity(deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0: raise AdmissionError('bounded engine probe deadline exceeded')
    environment = {key: value for key, value in os.environ.items()
                   if key.upper() not in ('DOCKER_CONTEXT', 'DOCKER_HOST', 'DOCKER_TLS_VERIFY', 'DOCKER_CERT_PATH')}
    try:
        result = subprocess.run(['/usr/bin/docker', '--host', 'unix:///var/run/docker.sock',
            'info', '--format', '{{.ID}}'], capture_output=True, text=True,
            timeout=min(2, remaining), env=environment)
        if result.returncode or len(result.stdout) > 512: raise ValueError()
        return result.stdout.strip()
    except (OSError, ValueError, subprocess.SubprocessError):
        raise AdmissionError('physical Engine probe unavailable') from None


class EngineIngressGuard:
    def __init__(self, queue, binding, *, owner_pin, engine_probe=local_engine_identity):
        self.queue, self.binding = queue, binding
        self.engine_probe = engine_probe
        self.owner_pin = owner_pin
        if (type(owner_pin) is not OwnerPin or OwnerPin.capture(queue) != owner_pin
                or queue.authority_id != binding.authority_id):
            raise AdmissionError('coordinator authority mismatch')

    def _mutation(self, method, target, headers):
        if method != 'POST' or not isinstance(target, str):
            raise AdmissionError('unrecognized mutation refused')
        if target == '/task/new/init':
            return 'init', _identifier(headers.get('set-uuid'))
        match = re.fullmatch(r'/task/new/(upload|commit)/(' + UUID + ')', target)
        if match:
            return match[1], match[2]
        if target == '/task/cancel':
            # The candidate omits body-form cancellation until an exact-body
            # parser is qualified. Caller flags cannot assert target identity.
            raise AdmissionError('cancel body binding not qualified')
        raise AdmissionError('unrecognized mutation refused')

    @contextmanager
    def _owned(self, headers, engine_uuid, action, deadline):
        binding = self.binding
        job_id = _identifier(headers.get('x-ledgetop-job'))
        token = headers.get('x-ledgetop-claim', '')
        if not 32 <= len(token) <= 128:
            raise AdmissionError('original claim required')
        try:
            with _queue_lifecycle(self.queue, deadline), NodeGate(binding.engine_id) as gate:
                if self.engine_probe(deadline) != binding.engine_id:
                    raise AdmissionError('physical Engine identity changed')
                state = gate.snapshot()
                expected = {'engineId': binding.engine_id, 'nodeId': binding.node_id,
                    'authorityId': self.queue.authority_id,
                    'coordinatorOrigin': binding.coordinator_origin, 'generation': binding.generation}
                if any(state.get(k) != v for k, v in expected.items()):
                    raise AdmissionError('physical ownership binding changed')
                with self.queue._db() as db:
                    if OwnerPin.capture(self.queue) != self.owner_pin:
                        raise AdmissionError('canonical owner lease changed')
                    remaining = deadline - time.monotonic()
                    if remaining <= 0: raise AdmissionError('deadline exceeded')
                    db.execute('PRAGMA busy_timeout=' + str(max(1, int(remaining * 1000))))
                    db.execute('BEGIN IMMEDIATE')
                    self.queue._refresh_nodes(db)
                    row = db.execute('SELECT * FROM jobs WHERE id=?', (job_id,)).fetchone()
                    task = db.execute('SELECT * FROM viewer_tasks WHERE job_id=?', (job_id,)).fetchone()
                    active = db.execute("SELECT id FROM jobs WHERE status IN ('running','held')").fetchall()
                    node = self.queue._node(db, binding.node_id)
                    if (row is None or task is None or len(active) != 1 or active[0]['id'] != job_id
                            or row['status'] != 'running' or row['node_id'] != binding.node_id
                            or row['profile'] != VIEWER_PROFILE or row['worker_id'] != 'viewer-dispatch'
                            or task['node_id'] != binding.node_id or task['engine_uuid'] != engine_uuid
                            or task['claim_token'] != token or not row['claim_hash']
                            or not hmac.compare_digest(hashlib.sha256(token.encode()).hexdigest(), row['claim_hash'])
                            or node['state'] not in ('active', 'scheduled', 'draining')):
                        raise AdmissionError('exact active coordinator claim required')
                    phases = {'init': 'init_pending', 'upload': 'upload_pending', 'commit': 'commit_pending'}
                    if task['phase'] != phases[action]:
                        raise AdmissionError('durable mutation phase mismatch')
                    receipt_id = uuid.UUID(job_id).hex
                    identity = {'id': job_id, 'workerId': row['worker_id'],
                                'nodeId': row['node_id'], 'authorityId': self.queue.authority_id}
                    reservation = state['reservation']
                    if reservation is None:
                        if action != 'init': raise AdmissionError('initial reservation required')
                        gate.reserve(binding.node_id, binding.coordinator_origin, self.queue.authority_id,
                                     binding.generation, receipt_id)
                        gate.bind(receipt_id, identity)
                    elif reservation['receiptId'] != receipt_id or reservation['job'] != identity:
                        raise AdmissionError('different physical reservation exists')
                    # Both ownership mechanisms remain held across the bounded
                    # exchange. Releasing before forwarding would create a race.
                    yield
        except (GateError, sqlite3.Error, RuntimeError):
            raise AdmissionError('physical gate refused request') from None

    def mutate(self, method, target, pairs, body=b'', content_type='application/json'):
        if (type(body) is not bytes or len(body) > MAX_BODY or not isinstance(content_type, str)
                or not re.fullmatch(r'[A-Za-z0-9!#$&^_.+;/= -]{1,200}', content_type)):
            raise AdmissionError('bounded control body required; image streaming unqualified')
        headers = _headers(pairs)
        _authenticate(headers, self.binding.coordinator_secret)
        action, engine_uuid = self._mutation(method, target, headers)
        deadline = time.monotonic() + self.binding.deadline_seconds
        with self._owned(headers, engine_uuid, action, deadline):
            forwarded = {'Content-Type': content_type}
            if action == 'init': forwarded['Set-UUID'] = engine_uuid
            return self._exchange(method, target, body, forwarded, deadline)

    def read(self, method, target, pairs):
        headers = _headers(pairs)
        _authenticate(headers, self.binding.management_secret)
        if method != 'GET' or not (target in ('/info', '/options', '/task/list')
                or re.fullmatch(r'/task/' + UUID + r'/(info|output)', target or '')):
            raise AdmissionError('unrecognized management read refused')
        return self._exchange(method, target, b'', {}, time.monotonic() + self.binding.deadline_seconds)

    def completed(self, pairs):
        """Validate exact success, but REFUSE automatic physical release.

        A durable database success is not independent evidence that an engine's
        processing children stopped. No caller flag can substitute for that
        still-unqualified probe and exclusive writer topology. This candidate
        therefore retains even successful reservations for operator inspection.
        """
        headers = _headers(pairs)
        _authenticate(headers, self.binding.coordinator_secret)
        job_id = _identifier(headers.get('x-ledgetop-job'))
        token = headers.get('x-ledgetop-claim', '')
        deadline = time.monotonic() + self.binding.deadline_seconds
        try:
            with _queue_lifecycle(self.queue, deadline), NodeGate(self.binding.engine_id) as gate:
                if self.engine_probe(deadline) != self.binding.engine_id:
                    raise AdmissionError('physical Engine identity changed')
                state = gate.snapshot()
                if (state['nodeId'] != self.binding.node_id or state['authorityId'] != self.queue.authority_id
                        or state['coordinatorOrigin'] != self.binding.coordinator_origin
                        or state['generation'] != self.binding.generation):
                    raise AdmissionError('physical ownership binding changed')
                with self.queue._db() as db:
                    if OwnerPin.capture(self.queue) != self.owner_pin:
                        raise AdmissionError('canonical owner lease changed')
                    db.execute('PRAGMA busy_timeout=' + str(max(1, int(max(0, deadline-time.monotonic())*1000))))
                    db.execute('BEGIN IMMEDIATE')
                    row = db.execute('SELECT * FROM jobs WHERE id=?', (job_id,)).fetchone()
                    task = db.execute('SELECT * FROM viewer_tasks WHERE job_id=?', (job_id,)).fetchone()
                    if (row is None or task is None or row['status'] != 'succeeded'
                            or row['node_id'] != self.binding.node_id or row['profile'] != VIEWER_PROFILE
                            or task['phase'] != 'complete' or task['claim_token'] != token
                            or not row['claim_hash'] or not hmac.compare_digest(
                                hashlib.sha256(token.encode()).hexdigest(), row['claim_hash'])):
                        raise AdmissionError('exact successful coordinator result required')
                    result = json.loads(row['result'])
                    if result.get('engineTaskId') != task['engine_uuid']:
                        raise AdmissionError('successful engine identity differs')
                    raise AdmissionError('independent engine quiescence and writer fencing unqualified; retain reservation')
        except (GateError, sqlite3.Error, RuntimeError):
            raise AdmissionError('physical gate refused completion') from None

    def _exchange(self, method, target, body, headers, deadline):
        """One nonblocking fixed-loopback exchange; no retries or redirects."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(False)
        def wait(write=False):
            remaining = deadline - time.monotonic()
            if remaining <= 0: raise AdmissionError('bounded upstream deadline exceeded')
            reads, writes, _ = select.select([] if write else [sock], [sock] if write else [], [], remaining)
            if not (writes if write else reads): raise AdmissionError('bounded upstream deadline exceeded')
        try:
            code = sock.connect_ex(('127.0.0.1', self.binding.backend_port))
            if code not in (0, errno.EINPROGRESS, errno.EWOULDBLOCK, errno.EALREADY, 10035):
                raise AdmissionError('upstream unavailable')
            if code:
                wait(True)
                if sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR): raise AdmissionError('upstream unavailable')
            route = target + '?' + urlencode({'token': self.binding.backend_secret})
            request = (f'{method} {route} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n'
                       f'Content-Length: {len(body)}\r\n' + ''.join(f'{k}: {v}\r\n' for k, v in headers.items())
                       + '\r\n').encode('ascii') + body
            offset = 0
            while offset < len(request):
                wait(True)
                try: count = sock.send(request[offset:])
                except BlockingIOError: continue
                if not count: raise AdmissionError('upstream exchange uncertain')
                offset += count
            data = bytearray()
            while b'\r\n\r\n' not in data:
                wait()
                try: chunk = sock.recv(4096)
                except BlockingIOError: continue
                if not chunk: raise AdmissionError('upstream exchange uncertain')
                data.extend(chunk)
                if len(data) > MAX_HEADERS + MAX_BODY: raise AdmissionError('upstream response exceeded bound')
                if b'\r\n\r\n' not in data and len(data) > MAX_HEADERS: raise AdmissionError('upstream headers exceeded bound')
            raw_headers, raw_body = bytes(data).split(b'\r\n\r\n', 1)
            if len(raw_headers) > MAX_HEADERS: raise AdmissionError('upstream headers exceeded bound')
            lines = raw_headers.decode('ascii').split('\r\n')
            match = re.fullmatch(r'HTTP/1\.[01] ([1-5][0-9]{2})[^\r\n]*', lines.pop(0))
            if not match: raise AdmissionError('upstream response invalid')
            received = {}
            for line in lines:
                key, value = line.split(':', 1); key = key.lower(); value = value.strip()
                if key in received: raise AdmissionError('duplicate upstream header')
                received[key] = value
            length = received.get('content-length', '')
            if ('transfer-encoding' in received or not re.fullmatch(r'[0-9]{1,6}', length)
                    or int(length) > MAX_BODY): raise AdmissionError('bounded fixed upstream body required')
            length = int(length)
            result = bytearray(raw_body)
            while len(result) < length:
                wait()
                try: chunk = sock.recv(min(4096, length - len(result)))
                except BlockingIOError: continue
                if not chunk: raise AdmissionError('upstream exchange uncertain')
                result.extend(chunk)
            if len(result) != length: raise AdmissionError('upstream response invalid')
            return int(match[1]), bytes(result)
        except (OSError, UnicodeError, ValueError):
            raise AdmissionError('upstream exchange unconfirmed; retain reservation') from None
        finally:
            sock.close()
