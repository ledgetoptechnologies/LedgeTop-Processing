"""INACTIVE bounded engine admission candidate; no listener or live wiring.

Control bodies are limited to 64 KiB; a separate socket-backed streaming
experiment is not wired to a listener and is not qualified for real uploads.
It is NOT ready for production. The caller must supply
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
MAX_STREAM_BYTES = 2 * 1024 ** 3
STREAM_DDL = '''CREATE TABLE engine_ingress_uploads (upload_id TEXT PRIMARY KEY, job_id TEXT NOT NULL,
 engine_uuid TEXT NOT NULL, byte_count INTEGER NOT NULL, sha256 TEXT NOT NULL, state TEXT NOT NULL)'''
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


def _resume_upload_transaction(db, deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0: raise AdmissionError('deadline exceeded')
    db.execute('PRAGMA busy_timeout=' + str(max(1, int(remaining * 1000))))
    db.execute('BEGIN IMMEDIATE')


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


@dataclass(frozen=True)
class BackendPin:
    """Installer-selected local container identity; not remote authority proof."""
    container_id: str
    image_id: str
    backend_port: int

    def __post_init__(self):
        if (type(self.container_id) is not str or not re.fullmatch(r'[0-9a-f]{64}', self.container_id)
                or type(self.image_id) is not str or not re.fullmatch(r'sha256:[0-9a-f]{64}', self.image_id)
                or type(self.backend_port) is not int or not 1024 <= self.backend_port <= 65535):
            raise AdmissionError('exact local backend identity required')


def _local_docker_json(target, deadline):
    """Bounded HTTP/1.0 read from the fixed local Unix Docker socket only.

    Close-delimited or exact Content-Length responses are accepted. Chunked
    responses are refused. No CLI, subprocess, environment or redirects are used.
    """
    if (not isinstance(target, str) or not target.isascii() or '\r' in target or '\n' in target
            or len(target) > 1024): raise AdmissionError('invalid local Docker request')
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.setblocking(False)
        result = sock.connect_ex('/var/run/docker.sock')
        if result not in (0, errno.EINPROGRESS, errno.EAGAIN, errno.EWOULDBLOCK):
            raise AdmissionError('local Docker socket unavailable')
        def ready(reading):
            left = deadline - time.monotonic()
            if left <= 0: raise AdmissionError('local Docker deadline exceeded')
            readable, writable, _ = select.select([sock] if reading else [], [] if reading else [sock], [], left)
            if not (readable if reading else writable): raise AdmissionError('local Docker deadline exceeded')
        if result:
            ready(False)
            if sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR): raise AdmissionError('local Docker socket unavailable')
        request = ('GET ' + target + ' HTTP/1.0\r\nHost: localhost\r\nConnection: close\r\n\r\n').encode('ascii')
        sent = 0
        while sent < len(request):
            ready(False)
            try: count = sock.send(request[sent:])
            except BlockingIOError: continue
            if count <= 0: raise AdmissionError('local Docker request incomplete')
            sent += count
        data = bytearray()
        while True:
            ready(True)
            try: chunk = sock.recv(8192)
            except BlockingIOError: continue
            if not chunk: break
            data.extend(chunk)
            if len(data) > 48 * 1024: raise AdmissionError('local Docker response exceeds bound')
        header, separator, body = bytes(data).partition(b'\r\n\r\n')
        if not separator or len(header) > 16 * 1024 or len(body) > 32 * 1024:
            raise AdmissionError('local Docker response invalid')
        lines = header.split(b'\r\n')
        if lines[0] not in (b'HTTP/1.0 200 OK', b'HTTP/1.1 200 OK'):
            raise AdmissionError('local Docker response unavailable')
        headers = {}
        for line in lines[1:]:
            name, sep, value = line.partition(b':')
            name = name.strip().lower()
            if not sep or not re.fullmatch(rb'[a-z0-9-]+', name) or name in headers:
                raise AdmissionError('local Docker response invalid')
            headers[name] = value.strip()
        if b'transfer-encoding' in headers:
            raise AdmissionError('local Docker transfer encoding unsupported')
        length = headers.get(b'content-length')
        if length is not None and (not re.fullmatch(rb'[0-9]{1,6}', length) or int(length) != len(body)):
            raise AdmissionError('local Docker response length invalid')
        def unique(items):
            result = {}
            for key, value in items:
                if key in result: raise ValueError('duplicate JSON key')
                result[key] = value
            return result
        return json.loads(body.decode('utf-8'), object_pairs_hook=unique,
                          parse_constant=lambda _: (_ for _ in ()).throw(ValueError('invalid JSON')))
    except (OSError, UnicodeError, ValueError, TypeError, RecursionError):
        raise AdmissionError('local Docker response unavailable') from None
    finally:
        sock.close()


def local_backend_identity(pin, deadline):
    """Read one Docker summary, not full inspect data or container environment."""
    try:
        query = urlencode({'all': '1', 'filters': json.dumps({'id': [pin.container_id]}, separators=(',', ':'))})
        items = _local_docker_json('/containers/json?' + query, deadline)
        if type(items) is not list or len(items) != 1: raise ValueError()
        item = items[0]
        ports = [p for p in item['Ports'] if p.get('PrivatePort') == 3000 and p.get('Type') == 'tcp']
        if (item['Id'] != pin.container_id or item['ImageID'] != pin.image_id
                or item['State'] != 'running' or ports != [
                    {'IP': '127.0.0.1', 'PrivatePort': 3000, 'PublicPort': pin.backend_port, 'Type': 'tcp'}]):
            raise ValueError()
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        raise AdmissionError('local backend identity unavailable') from None


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


def _file_multipart(upload_id, name):
    boundary='processing-'+uuid.UUID(upload_id).hex
    prefix=(f'--{boundary}\r\nContent-Disposition: form-data; name="images"; '
            f'filename="{name}"\r\nContent-Type: application/octet-stream\r\n\r\n').encode('ascii')
    return boundary,prefix,f'\r\n--{boundary}--\r\n'.encode('ascii')


def local_engine_identity(deadline):
    try:
        value = _local_docker_json('/info', deadline)['ID']
        if type(value) is not str or not 1 <= len(value) <= 512: raise ValueError()
        return value
    except (OSError, ValueError, KeyError, TypeError):
        raise AdmissionError('physical Engine probe unavailable') from None


class EngineIngressGuard:
    def __init__(self, queue, binding, *, owner_pin, backend_pin=None,
                 engine_probe=local_engine_identity, backend_probe=local_backend_identity):
        self.queue, self.binding = queue, binding
        self.engine_probe = engine_probe
        self.backend_pin, self.backend_probe = backend_pin, backend_probe
        self.owner_pin = owner_pin
        if (type(owner_pin) is not OwnerPin or OwnerPin.capture(queue) != owner_pin
                or queue.authority_id != binding.authority_id):
            raise AdmissionError('coordinator authority mismatch')

    def _check_backend(self, deadline):
        if type(self.backend_pin) is not BackendPin or self.backend_pin.backend_port != self.binding.backend_port:
            raise AdmissionError('exact local backend pin required')
        if self.engine_probe(deadline) != self.binding.engine_id:
            raise AdmissionError('physical Engine identity changed')
        self.backend_probe(self.backend_pin, deadline)

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

    @staticmethod
    def _bound_file(db, upload_id, job, task, expected_state):
        from .viewer_tasks import TaskStore
        intent=db.execute('SELECT * FROM viewer_engine_upload_intents WHERE upload_id=?',(upload_id,)).fetchone()
        if intent is None:raise AdmissionError('exact file intent required')
        file=db.execute('SELECT * FROM viewer_files WHERE uuid=? AND name_key=?',(task['uuid'],intent['name_key'])).fetchone()
        if (file is None or intent['state']!=expected_state or intent['job_id']!=job['id']
            or intent['task_uuid']!=task['uuid'] or intent['generation']!=task['generation']
            or intent['engine_uuid']!=task['engine_uuid'] or intent['claim_hash']!=job['claim_hash']
            or task['environment']!=job['environment'] or task['node_id']!=job['node_id']
            or task['removed'] or task['stage']!='running'
            or tuple(intent[k] for k in ('name_key','name','path','bytes','sha256')) != tuple(file[k] for k in ('name_key','name','path','bytes','sha256'))
            or TaskStore._filename(intent['name'])!=intent['name_key'] or type(intent['bytes']) is not int
            or not 0<intent['bytes']<=MAX_STREAM_BYTES or not re.fullmatch('[0-9a-f]{64}',intent['sha256'])):
            raise AdmissionError('stored file identity changed')
        return intent

    @contextmanager
    def _owned(self, headers, engine_uuid, action, deadline, permit=None, bound_id=None):
        binding = self.binding
        job_id = _identifier(headers.get('x-ledgetop-job'))
        token = headers.get('x-ledgetop-claim', '')
        if not 32 <= len(token) <= 128:
            raise AdmissionError('original claim required')
        try:
            with _queue_lifecycle(self.queue, deadline), NodeGate(binding.engine_id) as gate:
                self._check_backend(deadline)
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
                    if bound_id is not None:
                        from .transfer_fence import attest
                        attest(db)
                    self.queue._refresh_nodes(db)
                    row = db.execute('SELECT * FROM jobs WHERE id=?', (job_id,)).fetchone()
                    task = db.execute('SELECT * FROM viewer_tasks WHERE job_id=?', (job_id,)).fetchone()
                    active = db.execute("SELECT id FROM jobs WHERE status IN ('running','held')").fetchall()
                    node = self.queue._node(db, binding.node_id)
                    if (row is None or task is None or len(active) != 1 or active[0]['id'] != job_id
                            or db.execute('SELECT COUNT(*) FROM viewer_tasks WHERE job_id=?',(job_id,)).fetchone()[0]!=1
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
                    schema = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='engine_ingress_uploads'").fetchone()
                    from . import engine_upload_contract as file_contract
                    file_schema=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='viewer_engine_upload_intents'").fetchone()
                    if file_schema is not None and file_schema[0]!=file_contract.DDL:
                        raise AdmissionError('file intent schema changed')
                    if file_schema is not None:
                        pending=db.execute("SELECT upload_id FROM viewer_engine_upload_intents WHERE job_id=? AND state!='complete'",(job_id,)).fetchall()
                        if pending and not (bound_id is not None and len(pending)==1 and pending[0]['upload_id']==bound_id):
                            raise AdmissionError('file intent requires inspection')
                        if action=='upload' and bound_id is None:
                            raise AdmissionError('bound file upload required')
                    if bound_id is not None:
                        if file_contract.QUALIFIED is not True or file_schema is None or action!='upload':
                            raise AdmissionError('bound file upload not qualified')
                        record=self._bound_file(db,bound_id,row,task,'prepared')
                        _,prefix,suffix=_file_multipart(bound_id,record['name'])
                        if record['bytes']+len(prefix)+len(suffix)>MAX_STREAM_BYTES:
                            raise AdmissionError('bounded socket upload required')
                        permit=(bound_id,record['bytes'],record['sha256'])
                    if schema is not None and schema[0] != STREAM_DDL:
                        raise AdmissionError('upload journal requires inspection')
                    if schema is not None and db.execute("SELECT 1 FROM engine_ingress_uploads WHERE job_id=? AND state!='complete'", (job_id,)).fetchone():
                        raise AdmissionError('uncertain upload requires inspection')
                    if schema is not None and action == 'upload' and permit is None:
                        raise AdmissionError('journaled upload required')
                    if permit is not None:
                        if schema is None: db.execute(STREAM_DDL)
                        upload_id, byte_count, sha = permit
                        if db.execute('SELECT 1 FROM engine_ingress_uploads WHERE upload_id=?',(upload_id,)).fetchone():
                            raise AdmissionError('upload identity already used')
                        if db.execute('SELECT count(*) FROM engine_ingress_uploads').fetchone()[0] >= 100000:
                            raise AdmissionError('upload journal capacity exhausted')
                        db.execute("INSERT INTO engine_ingress_uploads VALUES(?,?,?,?,?,'started')",(upload_id,job_id,engine_uuid,byte_count,sha))
                        if bound_id is not None:
                            if db.execute("UPDATE viewer_engine_upload_intents SET state='started' WHERE upload_id=? AND state='prepared'",(bound_id,)).rowcount!=1:
                                raise AdmissionError('file intent changed')
                        # Intent commits before any source bytes are consumed or
                        # upstream request is issued; gate and lifecycle remain held.
                        db.commit()
                        _resume_upload_transaction(db, deadline)
                        if bound_id is not None:
                            from .transfer_fence import attest
                            attest(db)
                        if OwnerPin.capture(self.queue) != self.owner_pin:
                            raise AdmissionError('canonical owner lease changed')
                        current = db.execute('SELECT * FROM jobs WHERE id=?',(job_id,)).fetchone()
                        current_task = db.execute('SELECT * FROM viewer_tasks WHERE job_id=?',(job_id,)).fetchone()
                        active_now = db.execute("SELECT id FROM jobs WHERE status IN ('running','held')").fetchall()
                        node_now = self.queue._node(db,binding.node_id)
                        current_gate = gate.snapshot()
                        journal_schema=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='engine_ingress_uploads'").fetchone()
                        journal=db.execute('SELECT job_id,engine_uuid,byte_count,sha256,state FROM engine_ingress_uploads WHERE upload_id=?',(upload_id,)).fetchone() if journal_schema is not None and journal_schema[0]==STREAM_DDL else None
                        if (current is None or current_task is None or len(active_now)!=1 or active_now[0]['id']!=job_id
                            or db.execute('SELECT COUNT(*) FROM viewer_tasks WHERE job_id=?',(job_id,)).fetchone()[0]!=1
                            or current['status']!='running' or current['node_id']!=binding.node_id
                            or current['profile']!=VIEWER_PROFILE or current['worker_id']!='viewer-dispatch'
                            or current_task['node_id']!=binding.node_id or current_task['engine_uuid']!=engine_uuid
                            or current_task['claim_token']!=token or not current['claim_hash']
                            or not hmac.compare_digest(hashlib.sha256(token.encode()).hexdigest(),current['claim_hash'])
                            or current_task['phase']!='upload_pending' or node_now['state'] not in ('active','scheduled','draining')
                            or any(current_gate.get(k)!=v for k,v in expected.items())
                            or current_gate['reservation'] is None or current_gate['reservation']['receiptId']!=receipt_id
                            or current_gate['reservation']['job']!=identity
                            or journal is None or tuple(journal)!=(job_id,engine_uuid,byte_count,sha,'started')
                            ):
                            raise AdmissionError('exact active coordinator claim required')
                        self._check_backend(deadline)
                        if bound_id is not None:
                            current_schema=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='viewer_engine_upload_intents'").fetchone()
                            if current_schema is None or current_schema[0]!=file_contract.DDL:
                                raise AdmissionError('file intent schema changed')
                            self._bound_file(db,bound_id,current,current_task,'started')
                        if deadline-time.monotonic() <= 0:
                            raise AdmissionError('deadline exceeded')
                    # Both ownership mechanisms remain held across the bounded
                    # exchange. Releasing before forwarding would create a race.
                    yield db
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
            result = self._exchange(method, target, body, forwarded, deadline)
            self._check_backend(deadline)
            return result

    def stream_upload(self, target, pairs, source, byte_count, sha256, content_type):
        """One socket-backed upload, guarded and journaled; caller owns source.

        This is unwired experimental code. The caller must supply a dedicated
        unbuffered connected socket whose next byte begins this exact body.
        """
        headers = _headers(pairs)
        _authenticate(headers, self.binding.coordinator_secret)
        action, engine_uuid = self._mutation('POST', target, headers)
        upload_id = _identifier(headers.get('x-ledgetop-upload-id'))
        if (action != 'upload' or type(source) is not socket.socket
                or type(byte_count) is not int or not 0 < byte_count <= MAX_STREAM_BYTES
                or not isinstance(sha256, str) or not re.fullmatch('[0-9a-f]{64}',sha256)
                or not isinstance(content_type,str)
                or not re.fullmatch(r'multipart/form-data; boundary=[A-Za-z0-9_-]{1,70}',content_type)):
            raise AdmissionError('bounded socket upload required')
        deadline=time.monotonic()+self.binding.deadline_seconds
        with self._owned(headers,engine_uuid,action,deadline,(upload_id,byte_count,sha256)) as db:
            result=self._exchange('POST',target,b'',{'Content-Type':content_type},deadline,
                                  source=source,source_length=byte_count,expected_sha=sha256)
            self._check_backend(deadline)
            if result[0] != 200:
                raise AdmissionError('upstream upload unconfirmed; retain reservation')
            try:
                from .reference_protocol import decode_submission
                response=decode_submission(result[1])
                if type(response) is not dict or response.get('success') is not True or 'error' in response:
                    raise ValueError()
            except (ValueError,UnicodeError):
                raise AdmissionError('upstream upload unconfirmed; retain reservation') from None
            remaining=deadline-time.monotonic()
            if remaining<=0: raise AdmissionError('bounded upstream deadline exceeded')
            db.execute('PRAGMA busy_timeout='+str(max(1,int(remaining*1000))))
            changed=db.execute("UPDATE engine_ingress_uploads SET state='complete' WHERE upload_id=? AND job_id=? AND engine_uuid=? AND byte_count=? AND sha256=? AND state='started'",
                (upload_id,_identifier(headers.get('x-ledgetop-job')),engine_uuid,byte_count,sha256)).rowcount
            if changed!=1: raise AdmissionError('upload journal changed')
            db.commit()
            return result

    def stream_bound_upload(self, target, pairs, source, *, completion=None):
        with self.queue.pin_transfer():
            return self._stream_bound_upload(target, pairs, source, completion=completion)

    def _stream_bound_upload(self, target, pairs, source, *, completion=None):
        """Stream raw file bytes using a previously persisted TaskStore intent."""
        from . import engine_upload_contract as contract
        if contract.QUALIFIED is not True:
            raise AdmissionError('bound file upload not qualified')
        headers=_headers(pairs);_authenticate(headers,self.binding.coordinator_secret)
        action,engine_uuid=self._mutation('POST',target,headers)
        upload_id=_identifier(headers.get('x-ledgetop-upload-id'))
        if action!='upload' or type(source) is not socket.socket:
            raise AdmissionError('bounded socket upload required')
        deadline=time.monotonic()+self.binding.deadline_seconds
        with self._owned(headers,engine_uuid,action,deadline,bound_id=upload_id) as db:
            try:
                job=db.execute('SELECT * FROM jobs WHERE id=?',(_identifier(headers.get('x-ledgetop-job')),)).fetchone()
                task=db.execute('SELECT * FROM viewer_tasks WHERE job_id=?',(job['id'],)).fetchone()
                record=self._bound_file(db,upload_id,job,task,'started')
                boundary,prefix,suffix=_file_multipart(upload_id,record['name'])
                result=self._exchange('POST',target,b'',{'Content-Type':'multipart/form-data; boundary='+boundary},deadline,
                                      source=source,source_length=record['bytes'],expected_sha=record['sha256'],
                                      file_prefix=prefix,file_suffix=suffix,forbidden=b'--'+boundary.encode('ascii'))
                self._check_backend(deadline)
                try:
                    from .reference_protocol import decode_submission
                    response=decode_submission(result[1])
                    if result[0]!=200 or type(response) is not dict or response.get('success') is not True or 'error' in response:raise ValueError()
                except (ValueError,UnicodeError):
                    raise AdmissionError('upstream upload unconfirmed; retain reservation') from None
                if completion is not None:completion.verify(deadline)
                if completion is not None:completion.stop()
                if deadline-time.monotonic()<=0:raise AdmissionError('bounded upstream deadline exceeded')
                if db.execute("UPDATE engine_ingress_uploads SET state='complete' WHERE upload_id=? AND state='started'",(upload_id,)).rowcount!=1:
                    raise AdmissionError('upload journal changed')
                if db.execute("UPDATE viewer_engine_upload_intents SET state='complete' WHERE upload_id=? AND state='started'",(upload_id,)).rowcount!=1:
                    raise AdmissionError('file intent changed')
                db.commit()
                return result
            finally:
                if completion is not None:completion.stop()

    def read(self, method, target, pairs):
        headers = _headers(pairs)
        _authenticate(headers, self.binding.management_secret)
        if method != 'GET' or not (target in ('/info', '/options', '/task/list')
                or re.fullmatch(r'/task/' + UUID + r'/(info|output)', target or '')):
            raise AdmissionError('unrecognized management read refused')
        deadline = time.monotonic() + self.binding.deadline_seconds
        self._check_backend(deadline)
        result = self._exchange(method, target, b'', {}, deadline)
        self._check_backend(deadline)
        return result

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
                self._check_backend(deadline)
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

    def _exchange(self, method, target, body, headers, deadline, *, source=None, source_length=None, expected_sha=None, file_prefix=b'', file_suffix=b'', forbidden=None):
        """One nonblocking fixed-loopback exchange; no retries or redirects."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        source_timeout = source.gettimeout() if source is not None else None
        sock.setblocking(False)
        def wait(write=False):
            remaining = deadline - time.monotonic()
            if remaining <= 0: raise AdmissionError('bounded upstream deadline exceeded')
            reads, writes, _ = select.select([sock], [sock] if write else [], [], remaining)
            if write and reads:raise AdmissionError('early upstream response; retain reservation')
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
                       f'Content-Length: {source_length+len(file_prefix)+len(file_suffix) if source is not None else len(body)}\r\n' + ''.join(f'{k}: {v}\r\n' for k, v in headers.items())
                       + '\r\n').encode('ascii') + body
            def send_bytes(data):
                offset=0
                while offset<len(data):
                    wait(True)
                    try:count=sock.send(data[offset:])
                    except BlockingIOError:continue
                    if not count:raise AdmissionError('upstream exchange uncertain')
                    offset+=count
            send_bytes(request)
            if source is not None:
                send_bytes(file_prefix)
                source.setblocking(False)
                remaining_bytes=source_length;digest=hashlib.sha256();tail=b''
                while remaining_bytes:
                    remaining=deadline-time.monotonic()
                    if remaining<=0:raise AdmissionError('bounded source deadline exceeded')
                    readable,_,_=select.select([source],[],[],remaining)
                    if not readable:raise AdmissionError('bounded source deadline exceeded')
                    try: chunk=source.recv(min(65536,remaining_bytes))
                    except BlockingIOError:continue
                    if not chunk:raise AdmissionError('source upload unconfirmed; retain reservation')
                    if forbidden is not None:
                        window=tail+chunk
                        if forbidden in window:raise AdmissionError('multipart boundary collision; retain reservation')
                        tail=window[-(len(forbidden)-1):]
                    digest.update(chunk);remaining_bytes-=len(chunk);offset=0
                    while offset<len(chunk):
                        wait(True)
                        try: count=sock.send(chunk[offset:])
                        except BlockingIOError:continue
                        if not count:raise AdmissionError('upstream exchange uncertain')
                        offset+=count
                if not hmac.compare_digest(digest.hexdigest(),expected_sha):
                    raise AdmissionError('source digest differs; retain reservation')
                send_bytes(file_suffix)
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
            if source is not None:
                try: source.settimeout(source_timeout)
                except OSError: pass
