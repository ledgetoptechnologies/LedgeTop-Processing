"""Explicit local synthetic NodeODM HTTP fixture; performs no processing.

Only the three-byte abc upload and fixed empty ZIP are supported. Mutation
history is FULL SQLite state, so duplicate initialization/commit never reruns.
No request/token logging, external services, shell or container operations.
"""
from contextlib import contextmanager
from email import policy
from email.parser import BytesParser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import ipaddress
import json
import os
from pathlib import Path
import re
import socket
import sqlite3
import threading
from urllib.parse import parse_qs, urlsplit

from processing import reference_protocol as p

MAX_BODY = 8 * 1024
ARCHIVE = b'PK\x05\x06' + bytes(18)
OPTIONS = [
    dict(name='fast-mode', type='bool', value=False, domain='true,false', help='Synthetic boolean mode.'),
    dict(name='numeric-limit', type='int', value=1, domain=dict(min=1, max=8), help='Synthetic numeric limit.'),
]


def _parts(content_type, body):
    if type(content_type) is not str or len(content_type) > 200 or '\r' in content_type or '\n' in content_type:
        raise ValueError('invalid multipart')
    message = BytesParser(policy=policy.default).parsebytes(
        ('Content-Type: ' + content_type + '\r\nMIME-Version: 1.0\r\n\r\n').encode('ascii') + body)
    if message.defects or message.get_content_type() != 'multipart/form-data' or not message.is_multipart():
        raise ValueError('invalid multipart')
    result = []
    for part in message.iter_parts():
        if (part.defects or part.is_multipart() or part.get_content_disposition() != 'form-data'
                or part.get('Content-Transfer-Encoding') is not None):
            raise ValueError('invalid multipart')
        name, filename = part.get_param('name', header='content-disposition'), part.get_filename()
        payload = part.get_payload(decode=True)
        if type(name) is not str or type(payload) is not bytes:
            raise ValueError('invalid multipart')
        result.append((name, filename, payload))
        if len(result) > 4:
            raise ValueError('too many multipart fields')
    return result


class NativeNodeFixture:
    synthetic_fixture = True

    def __init__(self, path, host='127.0.0.1', port=0, engine_version='synthetic-transport-only', *, synthetic_enabled=False, expected_uploads=1):
        if synthetic_enabled is not True:
            raise ValueError('explicit synthetic fixture opt-in required')
        if type(expected_uploads) is not int or not 1 <= expected_uploads <= 50:
            raise ValueError('bounded expected synthetic upload count required')
        self.expected_uploads = expected_uploads
        if not ipaddress.ip_address(host).is_loopback or type(port) is not int or not 0 <= port <= 65535:
            raise ValueError('synthetic fixture requires literal loopback binding')
        if type(engine_version) is not str or not 1 <= len(engine_version) <= 128 or not all(0x21 <= ord(c) <= 0x7e for c in engine_version):
            raise ValueError('invalid synthetic version label')
        self.path = Path(path).absolute()
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if any(part.is_symlink() for part in (self.path, *self.path.parents)):
            raise ValueError('synthetic state must not contain symlinks')
        descriptor = os.open(self.path, os.O_CREAT | os.O_RDWR | getattr(os, 'O_NOFOLLOW', 0), 0o600)
        os.close(descriptor)
        if os.name != 'nt':
            self.path.chmod(0o600)
        self.engine_version = engine_version
        self._thread = None
        self._closed = False
        with self._db() as db:
            policy = db.execute("SELECT 1 FROM sqlite_master WHERE name='synthetic_upload_policy'").fetchone()
            tasks = db.execute("SELECT 1 FROM sqlite_master WHERE name='tasks'").fetchone()
            if expected_uploads == 1 and policy:
                raise ValueError('synthetic fixture upload policy differs')
            if expected_uploads != 1:
                if policy:
                    rows = db.execute('SELECT expected_uploads FROM synthetic_upload_policy').fetchall()
                    if len(rows) != 1 or rows[0][0] != expected_uploads:
                        raise ValueError('synthetic fixture upload policy differs')
                elif tasks:
                    raise ValueError('synthetic fixture upload policy differs')
                else:
                    db.execute('CREATE TABLE synthetic_upload_policy(expected_uploads INTEGER NOT NULL)')
                    db.execute('INSERT INTO synthetic_upload_policy VALUES(?)', (expected_uploads,))
            actions = "'initialize','upload','commit'" if expected_uploads == 1 else "'initialize','commit'"
            db.executescript('''CREATE TABLE IF NOT EXISTS tasks (
              uuid TEXT PRIMARY KEY,name TEXT NOT NULL,options TEXT NOT NULL,
              uploaded INTEGER NOT NULL DEFAULT 0,committed INTEGER NOT NULL DEFAULT 0,status INTEGER NOT NULL DEFAULT 10);
              CREATE TABLE IF NOT EXISTS calls (action TEXT NOT NULL,job TEXT NOT NULL);
              CREATE UNIQUE INDEX IF NOT EXISTS single_mutation ON calls(action,job)
                WHERE action IN ('''+actions+''');''')
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def setup(self):
                self.request.settimeout(2)
                super().setup()

            def log_message(self, *_args):
                pass

            def reply(self, status, value, *, binary=False):
                self.close_connection = True
                body = value if binary else json.dumps(value, separators=(',', ':')).encode()
                try:
                    self.send_response(status)
                    self.send_header('Content-Type', 'application/zip' if binary else 'application/json')
                    self.send_header('Content-Length', str(len(body)))
                    self.send_header('Cache-Control', 'no-store')
                    self.send_header('Connection', 'close')
                    self.end_headers()
                    self.wfile.write(body)
                except OSError:
                    pass

            def send_error(self, code, message=None, explain=None):
                self.reply(code, {'error': 'invalid_synthetic_request'})

            def dispatch(self):
                try:
                    target = urlsplit(self.path)
                    query = parse_qs(target.query, keep_blank_values=True)
                    if target.scheme or target.netloc or target.fragment or set(query) - {'token'} or any(len(v) != 1 for v in query.values()):
                        raise ValueError('invalid target')
                    if self.headers.get_all('Transfer-Encoding') or self.headers.get('Content-Encoding'):
                        raise ValueError('unsupported encoding')
                    for name in ('Content-Length', 'Content-Type', 'set-uuid'):
                        if len(self.headers.get_all(name, [])) > 1:
                            raise ValueError('duplicate header')
                    text = self.headers.get('Content-Length', '0')
                    if not re.fullmatch(r'[0-9]{1,8}', text):
                        raise ValueError('invalid length')
                    length = int(text)
                    if length > MAX_BODY:
                        self.reply(413, {'error': 'synthetic_body_too_large'})
                        return
                    if self.command == 'GET' and length:
                        raise ValueError('unexpected body')
                    body = self.rfile.read(length) if length else b''
                    if len(body) != length:
                        raise ValueError('truncated body')
                    status, value, binary = fixture._dispatch(self.command, target.path, self.headers, body)
                    self.reply(status, value, binary=binary)
                except (ValueError, UnicodeError, sqlite3.IntegrityError):
                    self.reply(400, {'error': 'invalid_synthetic_request'})
                except (OSError, TimeoutError):
                    self.reply(408, {'error': 'synthetic_request_timeout'})
                except Exception:
                    self.reply(500, {'error': 'synthetic_fixture_unavailable'})

            do_GET = dispatch
            do_POST = dispatch

        class Server(ThreadingHTTPServer):
            address_family = socket.AF_INET6 if ':' in host else socket.AF_INET
            daemon_threads = True
            block_on_close = False

            def handle_error(self, *_args):
                pass

        self._server = Server((host, port), Handler)
        actual_host, actual_port = self._server.server_address[:2]
        self.origin = 'http://' + ('[' + actual_host + ']' if ':' in actual_host else actual_host) + ':' + str(actual_port)

    @contextmanager
    def _db(self):
        db = sqlite3.connect(self.path, timeout=5)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA synchronous=FULL')
        try:
            with db:
                db.execute('BEGIN IMMEDIATE')
                yield db
        finally:
            db.close()

    def counts(self):
        result = dict.fromkeys(('initialize','upload','commit','download'), 0)
        with self._db() as db:
            result.update({row[0]: row[1] for row in db.execute('SELECT action,COUNT(*) FROM calls GROUP BY action')})
        return result

    def start(self):
        if self._closed:
            raise ValueError('synthetic fixture is closed')
        if self._thread is None:
            self._thread = threading.Thread(target=self._server.serve_forever, kwargs={'poll_interval': 0.02}, daemon=True)
            self._thread.start()
        return self

    def close(self):
        if self._closed:
            return
        self._closed = True
        if self._thread is not None:
            self._server.shutdown()
            self._thread.join(timeout=2)
        self._server.server_close()

    def _dispatch(self, method, path, headers, body):
        if method == 'GET' and path == '/info':
            with self._db() as db:
                active = db.execute('SELECT COUNT(*) FROM tasks WHERE committed=1 AND status IN (10,20)').fetchone()[0]
            return 200, dict(version='2.4.0', engine='odm', engineVersion=self.engine_version,
                taskQueueCount=active, maxImages=1000, maxParallelTasks=1), False
        if method == 'GET' and path == '/options':
            return 200, OPTIONS, False
        if method == 'GET' and path == '/task/list':
            with self._db() as db:
                return 200, [{'uuid': row[0]} for row in db.execute('SELECT uuid FROM tasks ORDER BY uuid')], False
        if method == 'POST' and path == '/task/remove':
            if headers.get('Content-Type') != 'application/x-www-form-urlencoded':
                raise ValueError('invalid removal form')
            fields = parse_qs(body.decode('ascii'), keep_blank_values=True, strict_parsing=True)
            if set(fields) != {'uuid'} or len(fields['uuid']) != 1:
                raise ValueError('one exact task UUID required')
            identifier = fields['uuid'][0]
            p._uuid(identifier, 'synthetic task UUID')
            with self._db() as db:
                task = db.execute('SELECT status FROM tasks WHERE uuid=?', (identifier,)).fetchone()
                if task is None:
                    return 404, {'error': 'synthetic_not_found'}, False
                if task['status'] != 40:
                    return 409, {'error': 'synthetic_task_not_complete'}, False
                db.execute('DELETE FROM tasks WHERE uuid=?', (identifier,))
                db.execute("INSERT INTO calls VALUES('remove',?)", (identifier,))
            return 200, {'success': True}, False
        if method == 'POST' and path == '/task/new/init':
            identifier = headers.get('set-uuid')
            p._uuid(identifier, 'synthetic task UUID')
            fields = {}
            for name, filename, value in _parts(headers.get('Content-Type'), body):
                if filename is not None or name in fields:
                    raise ValueError('invalid form')
                fields[name] = value.decode('utf-8')
            if set(fields) != {'name','options'}:
                raise ValueError('invalid form fields')
            p._string(fields['name'], 'synthetic name', 128)
            options = p.decode_submission(fields['options'])
            if type(options) is not list or len(options) > len(OPTIONS):
                raise ValueError('invalid synthetic options')
            known = {}
            for option in options:
                p._closed(option, ('name','value'), 'synthetic option')
                name, value = option['name'], option['value']
                if name in known or not ((name == 'fast-mode' and type(value) is bool) or
                        (name == 'numeric-limit' and type(value) is int and 1 <= value <= 8)):
                    raise ValueError('invalid synthetic option value')
                known[name] = value
            with self._db() as db:
                if db.execute('SELECT 1 FROM tasks WHERE uuid=?', (identifier,)).fetchone():
                    return 409, {'error': 'synthetic_task_exists'}, False
                db.execute('INSERT INTO tasks(uuid,name,options) VALUES(?,?,?)', (identifier, fields['name'], p.canonical_json(options).decode()))
                db.execute("INSERT INTO calls VALUES('initialize',?)", (identifier,))
            return 200, {'uuid': identifier}, False
        match = re.fullmatch(r'/task/(?:(new)/(upload|commit)/)?([0-9a-f-]{36})(?:/(info|download/all.zip))?', path)
        if not match:
            return 404, {'error': 'synthetic_not_found'}, False
        _new, action, identifier, read = match.groups()
        p._uuid(identifier, 'synthetic task UUID')
        with self._db() as db:
            task = db.execute('SELECT * FROM tasks WHERE uuid=?', (identifier,)).fetchone()
            if task is None:
                return 404, {'error': 'synthetic_not_found'}, False
            if method == 'POST' and action == 'upload' and read is None:
                parts = _parts(headers.get('Content-Type'), body)
                if len(parts) != 1 or parts[0][0] != 'images' or parts[0][1] is None or parts[0][2] != b'abc':
                    raise ValueError('fixture accepts abc only')
                p._basename(parts[0][1])
                if task['uploaded'] >= self.expected_uploads or task['committed']:
                    return 409, {'error': 'synthetic_upload_exists'}, False
                db.execute('UPDATE tasks SET uploaded=uploaded+1 WHERE uuid=?', (identifier,))
                db.execute("INSERT INTO calls VALUES('upload',?)", (identifier,))
                return 200, {'success': True}, False
            if method == 'POST' and action == 'commit' and read is None:
                if body or task['uploaded'] != self.expected_uploads or task['committed']:
                    return 409, {'error': 'synthetic_commit_refused'}, False
                db.execute('UPDATE tasks SET committed=1,status=40 WHERE uuid=?', (identifier,))
                db.execute("INSERT INTO calls VALUES('commit',?)", (identifier,))
                return 200, {'success': True}, False
            if method == 'GET' and action is None and read == 'info':
                return 200, {'uuid': identifier, 'status': {'code': task['status']}}, False
            if method == 'GET' and action is None and read == 'download/all.zip':
                if not task['committed'] or task['status'] != 40:
                    return 409, {'error': 'synthetic_result_not_ready'}, False
                db.execute("INSERT INTO calls VALUES('download',?)", (identifier,))
                return 200, ARCHIVE, True
        return 404, {'error': 'synthetic_not_found'}, False
