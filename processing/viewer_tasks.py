"""Durable Viewer upload metadata sharing the coordinator's execution database.

``task`` and ``list_tasks`` are PRIVATE broker interfaces: they include filesystem
paths, claim credentials and engine identity. Never return them from an HTTP route.
Native public responses come from ``info``/``output``. Inputs and results survive
``remove``; a reinitialized UUID gets a fresh directory and execution request ID.
"""

import hashlib
import json
import math
import os
from pathlib import Path
import re
import secrets
import shutil
import stat
import threading
import uuid as uuid_module
from datetime import datetime

from .maintenance import Conflict


VIEWER_PROFILE = 'odm-viewer-v1'
PHOTO_SUFFIXES = frozenset(('.jpg', '.jpeg', '.png', '.tif', '.tiff'))
INPUT_SUFFIXES = PHOTO_SUFFIXES | frozenset(('.txt', '.json', '.csv'))
NAME = re.compile(r'[A-Za-z0-9][A-Za-z0-9._ -]{0,179}\Z')
PHASE = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}\Z')
ERROR = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,119}\Z')
STAGES = frozenset(('uploading', 'queued', 'running', 'succeeded', 'failed', 'cancelled', 'held'))
TERMINAL = frozenset(('succeeded', 'failed', 'cancelled'))


class StorageBackpressure(Exception):
    """Retryable rejection before accepting bytes into the durable spool."""


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def checked_uuid(value):
    if not isinstance(value, str):
        raise ValueError('canonical UUID required')
    try:
        identifier = uuid_module.UUID(value)
    except (ValueError, AttributeError):
        raise ValueError('canonical UUID required') from None
    if str(identifier) != value:
        raise ValueError('canonical UUID required')
    return value


def sync_directory(path):
    if os.name != 'nt':
        descriptor = os.open(path, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


class TaskStore:
    def __init__(self, queue, directory, min_free_bytes=0):
        if type(min_free_bytes) is not int or not 0 <= min_free_bytes <= 10 ** 15:
            raise ValueError('nonnegative free-space reserve required')
        self.queue = queue
        self.min_free_bytes = min_free_bytes
        self.root = Path(directory).absolute()
        if self.root.resolve() != self.root:
            raise ValueError('input directory must not contain symlinks')
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        if not self.root.is_dir() or self.root.is_symlink():
            raise ValueError('private input directory required')
        if os.name != 'nt' and self.root.stat().st_mode & 0o077:
            raise ValueError('private input directory required')
        self._lock = threading.RLock()
        # Claim credentials are stored in this shared database. Tighten the
        # exact existing, owned database before creating or updating secret
        # metadata; future SQLite rollback journals inherit its private mode.
        database = Path(queue.path).absolute()
        if database.resolve() != database or database.is_symlink():
            raise ValueError('owned regular queue database required')
        initial = database.lstat()
        if (not stat.S_ISREG(initial.st_mode)
                or (os.name != 'nt' and initial.st_uid != os.geteuid())):
            raise ValueError('owned regular queue database required')
        descriptor = os.open(database, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0)
                             | getattr(os, 'O_NONBLOCK', 0))
        try:
            observed = os.fstat(descriptor)
            current = database.lstat()
            if (not stat.S_ISREG(observed.st_mode)
                    or (observed.st_dev, observed.st_ino) != (current.st_dev, current.st_ino)
                    or (os.name != 'nt' and observed.st_uid != os.geteuid())):
                raise ValueError('owned regular queue database required')
            if os.name != 'nt':
                os.fchmod(descriptor, 0o600)
        finally:
            os.close(descriptor)
        with self.queue._db() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS viewer_tasks (
                uuid TEXT PRIMARY KEY, generation INTEGER NOT NULL DEFAULT 1,
                directory TEXT NOT NULL, name TEXT NOT NULL, options TEXT NOT NULL,
                outputs TEXT NOT NULL, environment TEXT NOT NULL,
                stage TEXT NOT NULL DEFAULT 'uploading', phase TEXT NOT NULL DEFAULT 'uploading',
                job_id TEXT, node_id TEXT, engine_uuid TEXT, claim_token TEXT,
                upload_cursor INTEGER NOT NULL DEFAULT 0,
                error TEXT, progress REAL NOT NULL DEFAULT 0,
                output_lines TEXT NOT NULL DEFAULT '[]', archive_path TEXT, archive_sha256 TEXT,
                removed INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)''')
            db.execute('''CREATE TABLE IF NOT EXISTS viewer_files (
                uuid TEXT NOT NULL, name_key TEXT NOT NULL, name TEXT NOT NULL,
                path TEXT NOT NULL, bytes INTEGER NOT NULL, sha256 TEXT NOT NULL,
                is_photo INTEGER NOT NULL, PRIMARY KEY(uuid,name_key))''')
            db.execute('''CREATE TABLE IF NOT EXISTS viewer_incarnations (
                uuid TEXT NOT NULL, generation INTEGER NOT NULL,
                job_id TEXT, metadata TEXT NOT NULL, files TEXT NOT NULL,
                PRIMARY KEY(uuid,generation))''')
            # Additive migration for candidate databases created before per-file
            # upload progress was recorded. This value is only advanced after
            # NodeODM has acknowledged a complete file upload.
            columns = {row['name'] for row in db.execute('PRAGMA table_info(viewer_tasks)')}
            if 'upload_cursor' not in columns:
                db.execute('ALTER TABLE viewer_tasks ADD COLUMN upload_cursor INTEGER NOT NULL DEFAULT 0')
            if 'archive_sha256' not in columns:
                db.execute('ALTER TABLE viewer_tasks ADD COLUMN archive_sha256 TEXT')
            db.execute('''CREATE TABLE IF NOT EXISTS viewer_delivery_acceptances (
                uuid TEXT NOT NULL, generation INTEGER NOT NULL, event_id TEXT NOT NULL UNIQUE,
                attempt_id TEXT NOT NULL UNIQUE, receipt_sha256 TEXT NOT NULL, receipt_json TEXT NOT NULL,
                accepted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(uuid,generation))''')
            db.execute('''CREATE TABLE IF NOT EXISTS viewer_transfer_pins (
                token TEXT PRIMARY KEY, uuid TEXT NOT NULL, generation INTEGER NOT NULL, kind TEXT NOT NULL)''')
            db.execute('''CREATE TABLE IF NOT EXISTS viewer_cleanup_fences (
                uuid TEXT NOT NULL, generation INTEGER NOT NULL, PRIMARY KEY(uuid,generation))''')
            db.execute('''CREATE UNIQUE INDEX IF NOT EXISTS viewer_engine_uuid
                ON viewer_tasks(engine_uuid) WHERE engine_uuid IS NOT NULL''')

    def acquire_transfer(self, identifier, kind, *, expected_generation=None):
        """Persist an active transfer; orphaned pins stay held across restart."""
        if kind not in ('download', 'upload', 'engine_upload'):
            raise ValueError('invalid transfer kind')
        token = secrets.token_hex(32)
        with self._lock, self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = self._row(db, identifier)
            generation = row['generation']
            if expected_generation is not None and generation != expected_generation:
                raise Conflict('task generation changed')
            if db.execute('SELECT 1 FROM viewer_cleanup_fences WHERE uuid=? AND generation=?',
                          (identifier, generation)).fetchone():
                raise Conflict('task is fenced for cleanup')
            db.execute('INSERT INTO viewer_transfer_pins VALUES(?,?,?,?)',
                       (token, identifier, generation, kind))
        return token

    def transfer_generation(self, token):
        with self.queue._db() as db:
            row = db.execute('SELECT generation FROM viewer_transfer_pins WHERE token=?', (token,)).fetchone()
            if row is None:
                raise Conflict('transfer pin missing')
            return row['generation']

    def _require_upload_pin(self, db, identifier, token, generation):
        row = self._row(db, identifier)
        pin = db.execute('SELECT uuid,generation,kind FROM viewer_transfer_pins WHERE token=?', (token,)).fetchone()
        if (row['generation'] != generation or pin is None or
                (pin['uuid'], pin['generation'], pin['kind']) != (identifier, generation, 'upload') or
                db.execute('SELECT 1 FROM viewer_cleanup_fences WHERE uuid=? AND generation=?',
                           (identifier, generation)).fetchone()):
            raise Conflict('upload transfer identity changed')
        return row

    def release_transfer(self, token):
        with self._lock, self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            if db.execute('DELETE FROM viewer_transfer_pins WHERE token=?', (token,)).rowcount != 1:
                raise Conflict('transfer pin missing')

    def _begin_cleanup_fence(self, identifier, generation):
        """Fence admission only; does not verify retention or authorize deletion."""
        with self._lock, self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = self._row(db, identifier)
            if row['generation'] != generation:
                raise Conflict('task generation changed')
            if db.execute('SELECT 1 FROM viewer_transfer_pins WHERE uuid=? AND generation=?',
                          (identifier, generation)).fetchone():
                raise Conflict('active or uncertain transfers')
            if db.execute('SELECT 1 FROM viewer_cleanup_fences WHERE uuid=? AND generation=?',
                          (identifier, generation)).fetchone():
                raise Conflict('cleanup fence already exists')
            db.execute('INSERT INTO viewer_cleanup_fences VALUES(?,?)', (identifier, generation))

    def ensure_capacity(self, additional_bytes, *, copies=1):
        if (type(additional_bytes) is not int or additional_bytes < 0
                or type(copies) is not int or not 1 <= copies <= 2):
            raise ValueError('invalid storage reservation')
        try:
            available = shutil.disk_usage(self.root).free
        except OSError:
            raise StorageBackpressure() from None
        # Leave room for filesystem metadata and avoid accepting an upload
        # that consumes the configured reserve exactly.
        margin = max(64 * 1024 ** 2, additional_bytes // 100)
        required = additional_bytes * copies + self.min_free_bytes + margin
        if available < required:
            raise StorageBackpressure()
        return available

    def _row(self, db, identifier, *, removed=False):
        checked_uuid(identifier)
        row = db.execute('SELECT * FROM viewer_tasks WHERE uuid=?', (identifier,)).fetchone()
        if row is None or (row['removed'] and not removed):
            raise KeyError('unknown task')
        return row

    def _directory(self, row):
        directory = self.root / row['directory']
        if directory.parent != self.root or directory.resolve() != directory:
            raise Conflict('task directory no longer safe')
        if not directory.is_dir() or directory.is_symlink():
            raise Conflict('task directory unavailable')
        return directory

    @staticmethod
    def _job(db, row):
        if not row['job_id']:
            return None
        job = db.execute('SELECT * FROM jobs WHERE id=?', (row['job_id'],)).fetchone()
        if job is None:
            raise Conflict('task execution record unavailable')
        return job

    def initialize(self, identifier, name='', options=None, outputs=None, environment='production'):
        checked_uuid(identifier)
        if (not isinstance(name, str) or len(name) > 256
                or any(ord(character) < 32 for character in name)):
            raise ValueError('invalid task name')
        if options is None:
            options = []
        if (not isinstance(options, list) or len(options) > 1000
                or any(not isinstance(item, dict) or set(item) != {'name', 'value'}
                       or not isinstance(item['name'], str)
                       or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,119}', item['name'])
                       for item in options)):
            raise ValueError('invalid task options')
        if len({item['name'] for item in options}) != len(options):
            raise ValueError('duplicate option')
        if outputs is not None and (not isinstance(outputs, list) or len(outputs) > 1000
                or any(not isinstance(value, str) or len(value) > 256 for value in outputs)):
            raise ValueError('invalid requested outputs')
        try:
            options_text, outputs_text = canonical(options), canonical(outputs)
        except (ValueError, TypeError, RecursionError):
            raise ValueError('invalid task settings') from None
        if len(options_text.encode()) + len(outputs_text.encode()) > 65536:
            raise ValueError('task settings too large')
        if environment not in ('production', 'staging'):
            raise ValueError('invalid task environment')
        with self._lock, self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT * FROM viewer_tasks WHERE uuid=?', (identifier,)).fetchone()
            if row is not None and row['environment'] != environment:
                # Authorization and task operations use separate reads. Keep
                # the original UUID's credential domain stable even while a
                # removed task is reset into a new private incarnation.
                raise Conflict('task UUID belongs to another environment')
            if row is not None and not row['removed']:
                if (row['name'], row['options'], row['outputs'], row['environment']) != (
                        name, options_text, outputs_text, environment):
                    raise Conflict('task UUID belongs to different settings')
                return {'uuid': identifier}
            if row is None and db.execute('SELECT COUNT(*) FROM viewer_tasks').fetchone()[0] >= 1000:
                raise Conflict('task history limit reached')
            generation = row['generation'] + 1 if row is not None else 1
            if row is not None:
                old_files = [dict(item) for item in db.execute(
                    'SELECT * FROM viewer_files WHERE uuid=? ORDER BY rowid', (identifier,))]
                db.execute('''INSERT INTO viewer_incarnations(uuid,generation,job_id,metadata,files)
                    VALUES(?,?,?,?,?)''', (identifier, row['generation'], row['job_id'],
                                           canonical(dict(row)), canonical(old_files)))
            directory_name = identifier + '-' + str(generation)
            directory = self.root / directory_name
            # No overwrite after an interrupted initialization. Orphan directories
            # stay intact and a distinct incarnation path is chosen.
            if directory.exists() or directory.is_symlink():
                directory_name += '-' + secrets.token_hex(8)
                directory = self.root / directory_name
            directory.mkdir(mode=0o700)
            sync_directory(self.root)
            db.execute('DELETE FROM viewer_files WHERE uuid=?', (identifier,))
            db.execute('''INSERT OR REPLACE INTO viewer_tasks
                (uuid,generation,directory,name,options,outputs,environment)
                VALUES(?,?,?,?,?,?,?)''', (identifier, generation, directory_name, name,
                                          options_text, outputs_text, environment))
            return {'uuid': identifier}

    @staticmethod
    def _filename(name):
        if (not isinstance(name, str) or not NAME.fullmatch(name)
                or Path(name).suffix.lower() not in INPUT_SUFFIXES
                or name.endswith((' ', '.')) or name in ('.', '..')):
            raise ValueError('safe supported input basename required')
        return name.casefold()

    def upload(self, identifier, filename, stream, length, *, transfer=None):
        if transfer is None:
            token = self.acquire_transfer(identifier, 'upload')
            try:
                return self.upload(identifier, filename, stream, length,
                                   transfer=(token, self.transfer_generation(token)))
            finally:
                self.release_transfer(token)
        token, generation = transfer
        key = self._filename(filename)
        if type(length) is not int or length <= 0:
            raise ValueError('invalid input length')
        with self._lock:
            with self.queue._db() as db:
                db.execute('BEGIN IMMEDIATE')
                row = self._require_upload_pin(db, identifier, token, generation)
                if row['stage'] != 'uploading' or row['job_id']:
                    raise Conflict('task is no longer accepting uploads')
                directory = self._directory(row)
                existing = db.execute('SELECT * FROM viewer_files WHERE uuid=? AND name_key=?',
                                      (identifier, key)).fetchone()
                count = db.execute('SELECT COUNT(*) FROM viewer_files WHERE uuid=?',
                                   (identifier,)).fetchone()[0]
                if existing is None and count >= 100000:
                    raise ValueError('too many input files')
            if existing is not None:
                digest = hashlib.sha256()
                remaining = length
                while remaining:
                    chunk = stream.read(min(1024 * 1024, remaining))
                    if not isinstance(chunk, bytes) or not chunk or len(chunk) > remaining:
                        raise ValueError('input stream length differs')
                    digest.update(chunk)
                    remaining -= len(chunk)
                if stream.read(1):
                    raise ValueError('input stream length differs')
                existing_path = directory / existing['path']
                if (digest.hexdigest() != existing['sha256'] or existing['bytes'] != length
                        or existing_path.is_symlink() or not existing_path.is_file()
                        or existing_path.stat().st_size != length):
                    raise Conflict('input basename already contains different or missing bytes')
                with self.queue._db() as db:
                    db.execute('BEGIN IMMEDIATE')
                    self._require_upload_pin(db, identifier, token, generation)
                return {'success': True, 'uuid': identifier}
            # The caller may still hold its multipart temporary copy while this
            # durable temporary file is written. Preserve headroom for both.
            self.ensure_capacity(length, copies=2)
            temporary = directory / ('.incoming-' + secrets.token_hex(16))
            final = directory / ('input-' + secrets.token_hex(16))
            try:
                with self.queue._db() as db:
                    db.execute('BEGIN IMMEDIATE')
                    self._require_upload_pin(db, identifier, token, generation)
                    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                digest = hashlib.sha256()
                with os.fdopen(descriptor, 'wb') as output:
                    remaining = length
                    while remaining:
                        chunk = stream.read(min(1024 * 1024, remaining))
                        if not isinstance(chunk, bytes) or not chunk or len(chunk) > remaining:
                            raise ValueError('input stream length differs')
                        output.write(chunk)
                        digest.update(chunk)
                        remaining -= len(chunk)
                    if stream.read(1):
                        raise ValueError('input stream length differs')
                    output.flush()
                    os.fsync(output.fileno())
                value = digest.hexdigest()
                with self.queue._db() as db:
                    db.execute('BEGIN IMMEDIATE')
                    row = self._require_upload_pin(db, identifier, token, generation)
                    if row['stage'] != 'uploading' or row['job_id']:
                        raise Conflict('task is no longer accepting uploads')
                    existing = db.execute('SELECT * FROM viewer_files WHERE uuid=? AND name_key=?',
                                          (identifier, key)).fetchone()
                    if existing:
                        if existing['sha256'] != value or existing['bytes'] != length:
                            raise Conflict('input basename already contains different bytes')
                        return {'success': True, 'uuid': identifier}
                    total = db.execute('SELECT COALESCE(SUM(bytes),0) FROM viewer_files WHERE uuid=?',
                                       (identifier,)).fetchone()[0]
                    os.replace(temporary, final)
                    sync_directory(directory)
                    db.execute('''INSERT INTO viewer_files
                        (uuid,name_key,name,path,bytes,sha256,is_photo) VALUES(?,?,?,?,?,?,?)''',
                        (identifier, key, filename, final.name, length, value,
                         int(Path(filename).suffix.lower() in PHOTO_SUFFIXES)))
                    db.execute('UPDATE viewer_tasks SET updated_at=CURRENT_TIMESTAMP WHERE uuid=?', (identifier,))
                return {'success': True, 'uuid': identifier}
            finally:
                # Only this invocation's randomly named, explicitly owned tmp file.
                if temporary.exists():
                    temporary.unlink()

    @staticmethod
    def _request_id(row):
        if row['generation'] == 1:
            return row['uuid']
        return str(uuid_module.uuid5(uuid_module.UUID(row['uuid']), 'generation:' + str(row['generation'])))

    def commit(self, identifier):
        with self._lock, self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = self._row(db, identifier)
            if row['job_id']:
                if row['stage'] == 'cancelled':
                    raise Conflict('task is cancelled')
                self._job(db, row)
                return {'success': True, 'uuid': identifier}
            if row['stage'] != 'uploading':
                raise Conflict('task cannot be committed')
            if not db.execute('SELECT 1 FROM viewer_files WHERE uuid=? AND is_photo=1 LIMIT 1',
                              (identifier,)).fetchone():
                raise ValueError('at least one photo required')
            if VIEWER_PROFILE not in self.queue.allowed_profiles:
                raise Conflict('Viewer processing profile is disabled')
            request_id = self._request_id(row)
            old = db.execute('SELECT * FROM jobs WHERE request_id=?', (request_id,)).fetchone()
            if old is not None:
                if old['environment'] != row['environment'] or old['profile'] != VIEWER_PROFILE:
                    raise Conflict('execution identity belongs to another submission')
                owner = db.execute('SELECT uuid FROM viewer_tasks WHERE job_id=? AND uuid<>?',
                                   (old['id'], identifier)).fetchone()
                archived = db.execute('SELECT 1 FROM viewer_incarnations WHERE job_id=? LIMIT 1',
                                      (old['id'],)).fetchone()
                if owner or archived:
                    raise Conflict('execution identity belongs to another incarnation')
                job_id = old['id']
            else:
                if db.execute('SELECT COUNT(*) FROM jobs').fetchone()[0] >= 1000:
                    raise Conflict('execution history limit reached')
                job_id = str(uuid_module.uuid4())
                db.execute('''INSERT INTO jobs(id,request_id,environment,profile,status)
                    VALUES(?,?,?,?,'queued')''', (job_id, request_id, row['environment'], VIEWER_PROFILE))
            db.execute("UPDATE viewer_tasks SET job_id=?,stage='queued',phase='queued',updated_at=CURRENT_TIMESTAMP WHERE uuid=?",
                       (job_id, identifier))
            return {'success': True, 'uuid': identifier}

    def _task(self, db, row):
        directory = self._directory(row)
        value = dict(row)
        value['directory'] = str(directory)
        for field in ('options', 'outputs', 'output_lines'):
            value[field] = json.loads(value[field])
        files = []
        for item in db.execute('SELECT * FROM viewer_files WHERE uuid=? ORDER BY rowid', (row['uuid'],)):
            item = dict(item)
            target = directory / item['path']
            if target.parent != directory or target.resolve() != target:
                raise Conflict('input path no longer safe')
            item['path'] = str(target)
            item['is_photo'] = bool(item['is_photo'])
            files.append(item)
        value['files'] = files
        value['images_count'] = sum(item['is_photo'] for item in files)
        value['input_bytes'] = sum(item['bytes'] for item in files)
        job = self._job(db, row)
        value['job'] = self.queue.public(job) if job is not None else None
        return value

    def task(self, identifier):
        with self._lock, self.queue._db() as db:
            return self._task(db, self._row(db, identifier))

    def list_tasks(self):
        with self._lock, self.queue._db() as db:
            return [self._task(db, row) for row in db.execute(
                'SELECT * FROM viewer_tasks WHERE removed=0 ORDER BY created_at,rowid').fetchall()]

    def _task_state(self, db, row):
        """Scheduler metadata only; never enumerate or resolve every input path."""
        value = dict(row)
        value['directory'] = str(self._directory(row))
        for field in ('options', 'outputs'):
            value[field] = json.loads(value[field])
        counts = db.execute('SELECT COALESCE(SUM(is_photo),0),COALESCE(SUM(bytes),0) '
                            'FROM viewer_files WHERE uuid=?', (row['uuid'],)).fetchone()
        value['images_count'], value['input_bytes'] = counts
        job = self._job(db, row)
        value['job'] = self.queue.public(job) if job is not None else None
        return value

    def task_state(self, identifier):
        with self._lock, self.queue._db() as db:
            return self._task_state(db, self._row(db, identifier))

    def list_task_states(self):
        with self._lock, self.queue._db() as db:
            return [self._task_state(db, row) for row in db.execute(
                'SELECT * FROM viewer_tasks WHERE removed=0 ORDER BY created_at,rowid').fetchall()]

    def update(self, identifier, *, return_task=True, expected_result=None, **fields):
        if type(return_task) is not bool:
            raise ValueError('return_task must be a boolean')
        result_fields = {'generation', 'job_id', 'engine_uuid', 'node_id', 'directory'}
        if expected_result is not None and (
                type(expected_result) is not dict or set(expected_result) != result_fields
                or type(expected_result['generation']) is not int or expected_result['generation'] < 1
                or any(type(expected_result[field]) is not str for field in result_fields - {'generation'})):
            raise ValueError('invalid expected result identity')
        allowed = {'stage', 'phase', 'node_id', 'engine_uuid', 'claim_token', 'error',
                   'progress', 'output_lines', 'archive_path', 'archive_sha256', 'upload_cursor'}
        if not fields or not set(fields) <= allowed:
            raise ValueError('unexpected broker update fields')
        if 'stage' in fields and fields['stage'] not in STAGES:
            raise ValueError('invalid task stage')
        for field in ('phase', 'node_id'):
            if field in fields and (not isinstance(fields[field], str) or not PHASE.fullmatch(fields[field])):
                raise ValueError('invalid broker identity or phase')
        if 'node_id' in fields and fields['node_id'] not in self.queue.node_ids:
            raise ValueError('unknown processing node')
        if 'engine_uuid' in fields and fields['engine_uuid'] is not None:
            checked_uuid(fields['engine_uuid'])
        if 'archive_sha256' in fields and fields['archive_sha256'] is not None and not re.fullmatch(
                r'[0-9a-f]{64}', fields['archive_sha256']):
            raise ValueError('invalid archive digest')
        if 'claim_token' in fields and (not isinstance(fields['claim_token'], str)
                or not 1 <= len(fields['claim_token']) <= 128
                or any(ord(c) < 33 or ord(c) > 126 for c in fields['claim_token'])):
            raise ValueError('invalid claim credential')
        if 'error' in fields and fields['error'] is not None and (
                not isinstance(fields['error'], str) or not ERROR.fullmatch(fields['error'])):
            raise ValueError('error must be a sanitized reason code')
        if 'progress' in fields and (type(fields['progress']) not in (int, float)
                or not math.isfinite(fields['progress']) or not 0 <= fields['progress'] <= 100):
            raise ValueError('invalid task progress')
        if 'upload_cursor' in fields and (type(fields['upload_cursor']) is not int
                or not 0 <= fields['upload_cursor'] <= 100000):
            raise ValueError('invalid upload cursor')
        if 'output_lines' in fields:
            lines = fields['output_lines']
            if (not isinstance(lines, list) or len(lines) > 10000
                    or any(not isinstance(line, str) or len(line) > 4096 for line in lines)):
                raise ValueError('invalid task output')
            fields['output_lines'] = canonical(lines)
            if len(fields['output_lines'].encode()) > 1024 * 1024:
                raise ValueError('task output too large')
        with self._lock, self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = self._row(db, identifier)
            if expected_result is not None:
                current_result = {field: row[field] for field in result_fields - {'directory'}}
                current_result['directory'] = str(self._directory(row))
                if current_result != expected_result:
                    raise Conflict('task result identity changed')
            if not return_task:
                # Preserve the owned-directory fence without rebuilding every
                # file entry for an ignored upload-checkpoint return value.
                self._directory(row)
            if row['engine_uuid'] and fields.get('engine_uuid', row['engine_uuid']) != row['engine_uuid']:
                raise Conflict('engine task identity is immutable')
            if fields.get('engine_uuid') and db.execute(
                    'SELECT 1 FROM viewer_tasks WHERE engine_uuid=? AND uuid<>?',
                    (fields['engine_uuid'], identifier)).fetchone():
                raise Conflict('engine task identity belongs to another task')
            if ('archive_sha256' in fields and row['archive_sha256'] is not None
                    and fields['archive_sha256'] != row['archive_sha256']):
                raise Conflict('retained archive digest is immutable')
            if 'archive_path' in fields and fields['archive_path'] is not None:
                path = Path(fields['archive_path']).absolute()
                directory = self._directory(row)
                if (path.resolve() != path or directory not in path.parents
                        or not path.is_file() or path.is_symlink()):
                    raise ValueError('owned regular result archive required')
                fields['archive_path'] = str(path)
            assignments = ','.join(field + '=?' for field in fields)
            db.execute('UPDATE viewer_tasks SET ' + assignments + ',updated_at=CURRENT_TIMESTAMP WHERE uuid=?',
                       (*fields.values(), identifier))
        return self.task(identifier) if return_task else None

    def accept_viewer_delivery(self, identifier, environment, receipt):
        checked_uuid(identifier)
        required = {'eventId', 'providerTaskId', 'attemptId', 'archiveSha256',
                    'resultModelId', 'resultModelVersionId', 'ingestedAt'}
        if type(receipt) is not dict or set(receipt) != required:
            raise ValueError('invalid Viewer delivery receipt')
        if receipt['providerTaskId'] != identifier:
            raise ValueError('invalid Viewer delivery identity')
        safe_id = re.compile(r'[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z')
        for field in ('eventId', 'attemptId', 'resultModelId', 'resultModelVersionId'):
            if type(receipt[field]) is not str or not safe_id.fullmatch(receipt[field]):
                raise ValueError('invalid Viewer delivery identifier')
        if type(receipt['archiveSha256']) is not str or not re.fullmatch(r'[0-9a-f]{64}', receipt['archiveSha256']):
            raise ValueError('invalid Viewer delivery archive digest')
        ingested = receipt['ingestedAt']
        if type(ingested) is not str or not 1 <= len(ingested) <= 40:
            raise ValueError('invalid Viewer ingestion time')
        try:
            parsed = datetime.fromisoformat(ingested.replace('Z', '+00:00'))
        except ValueError:
            raise ValueError('invalid Viewer ingestion time') from None
        if parsed.utcoffset() is None:
            raise ValueError('Viewer ingestion time must include a timezone')
        encoded = canonical(receipt)
        digest = hashlib.sha256(encoded.encode('utf-8')).hexdigest()
        with self._lock:
            with self.queue._db() as db:
                row = self._row(db, identifier)
                if row['environment'] != environment:
                    raise Conflict('Viewer delivery environment mismatch')
                old = db.execute('''SELECT event_id,attempt_id,receipt_sha256 FROM viewer_delivery_acceptances
                    WHERE uuid=? AND generation=?''', (identifier, row['generation'])).fetchone()
                if old:
                    if (old['event_id'], old['attempt_id'], old['receipt_sha256']) != (
                            receipt['eventId'], receipt['attemptId'], digest):
                        raise Conflict('Viewer delivery receipt already differs')
                    return {'accepted': True, 'replayed': True, 'eventId': receipt['eventId']}
                task = self._task(db, row)
                archive = Path(row['archive_path']) if row['archive_path'] else None
                if (row['stage'] != 'succeeded' or row['phase'] != 'complete' or not task['job']
                        or task['job']['status'] != 'succeeded' or archive is None
                        or row['archive_sha256'] != receipt['archiveSha256']
                        or archive.is_symlink() or not archive.is_file()):
                    raise Conflict('Viewer delivery does not match a retained successful task')
                directory = self._directory(row)
                if archive.parent != directory or archive.resolve() != archive:
                    raise Conflict('retained archive path is invalid')
                generation = row['generation']
                expected_archive_digest = row['archive_sha256']
                archive_identity = archive.stat()

        # Avoid holding the task lock or the queue's SQLite write lock while
        # hashing a potentially large result. The commit below revalidates the
        # captured task generation, path, and digest after this read-only pass.
        actual_archive_digest = self._hash_retained_archive(archive, archive_identity)
        if actual_archive_digest != expected_archive_digest:
            raise Conflict('retained archive bytes differ from the downloaded result')

        with self._lock, self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = self._row(db, identifier)
            if row['environment'] != environment:
                raise Conflict('Viewer delivery environment mismatch')
            old = db.execute('''SELECT event_id,attempt_id,receipt_sha256 FROM viewer_delivery_acceptances
                WHERE uuid=? AND generation=?''', (identifier, row['generation'])).fetchone()
            if old:
                if (old['event_id'], old['attempt_id'], old['receipt_sha256']) != (
                        receipt['eventId'], receipt['attemptId'], digest):
                    raise Conflict('Viewer delivery receipt already differs')
                return {'accepted': True, 'replayed': True, 'eventId': receipt['eventId']}
            task = self._task(db, row)
            current_archive = Path(row['archive_path']) if row['archive_path'] else None
            if (row['generation'] != generation or row['stage'] != 'succeeded'
                    or row['phase'] != 'complete' or not task['job']
                    or task['job']['status'] != 'succeeded'
                    or current_archive != archive
                    or row['archive_sha256'] != expected_archive_digest):
                raise Conflict('Viewer delivery changed while validating retained result')
            try:
                current_identity = current_archive.lstat()
            except OSError:
                raise Conflict('retained archive changed before acceptance') from None
            if (stat.S_ISLNK(current_identity.st_mode)
                    or not stat.S_ISREG(current_identity.st_mode)
                    or (current_identity.st_dev, current_identity.st_ino, current_identity.st_size,
                        current_identity.st_mtime_ns)
                        != (archive_identity.st_dev, archive_identity.st_ino, archive_identity.st_size,
                            archive_identity.st_mtime_ns)):
                raise Conflict('retained archive changed before acceptance')
            duplicate = db.execute('''SELECT uuid,generation FROM viewer_delivery_acceptances
                WHERE event_id=? OR attempt_id=?''', (receipt['eventId'], receipt['attemptId'])).fetchone()
            if duplicate:
                raise Conflict('Viewer delivery identity already belongs to another task')
            db.execute('''INSERT INTO viewer_delivery_acceptances
                (uuid,generation,event_id,attempt_id,receipt_sha256,receipt_json)
                VALUES(?,?,?,?,?,?)''', (identifier, row['generation'], receipt['eventId'],
                    receipt['attemptId'], digest, encoded))
            return {'accepted': True, 'replayed': False, 'eventId': receipt['eventId']}

    @staticmethod
    def _hash_retained_archive(path, expected_stat):
        descriptor = None
        try:
            flags = os.O_RDONLY | getattr(os, 'O_BINARY', 0) | getattr(os, 'O_NOFOLLOW', 0)
            descriptor = os.open(path, flags)
            opened = os.fstat(descriptor)
            named = path.lstat()
            if (not stat.S_ISREG(opened.st_mode) or stat.S_ISLNK(named.st_mode)
                    or (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino)
                    or (opened.st_dev, opened.st_ino, opened.st_size)
                        != (expected_stat.st_dev, expected_stat.st_ino, expected_stat.st_size)):
                raise Conflict('retained archive identity changed')
            digest = hashlib.sha256()
            with os.fdopen(descriptor, 'rb', closefd=False) as source:
                while chunk := source.read(1024 ** 2):
                    digest.update(chunk)
            after = os.fstat(descriptor)
            named_after = path.lstat()
            identity = (opened.st_dev, opened.st_ino, opened.st_size,
                        opened.st_mtime_ns)
            if (identity != (after.st_dev, after.st_ino, after.st_size,
                             after.st_mtime_ns)
                    or identity != (named_after.st_dev, named_after.st_ino, named_after.st_size,
                                    named_after.st_mtime_ns)):
                raise Conflict('retained archive changed while validating')
            return digest.hexdigest()
        except OSError:
            raise Conflict('retained archive is unavailable for verification') from None
        finally:
            if descriptor is not None:
                os.close(descriptor)

    def info(self, identifier):
        task = self.task(identifier)
        job = task['job']
        state = job['status'] if job else task['stage']
        recovering = (job and state == 'held' and job['heldReason'] == 'coordinator_restart'
                      and task['phase'] in ('claimed', 'init_pending', 'initialized', 'uploaded', 'processing',
                                            'commit_pending', 'awaiting_engine_start', 'cancel_pending')
                      and task['engine_uuid'] and task['node_id'] and task['claim_token'])
        cancelled_result = (job and state == 'failed' and isinstance(job['result'], dict)
                            and job['result'].get('cancelled') is True)
        engine_started = (state == 'running'
                          and task['phase'] in ('processing', 'cancel_pending'))
        if task['stage'] == 'cancelled' or cancelled_result:
            code = 50
        elif recovering:
            # Retain the shared slot, but do not tell Viewer this recoverable
            # engine execution failed before its exact UUID is reconciled. A
            # restart does not itself prove NodeODM left its queued state.
            code = 10 if task['phase'] in ('claimed', 'init_pending', 'initialized', 'uploaded',
                                            'commit_pending', 'awaiting_engine_start') else 20
        elif state == 'running' and not engine_started:
            # A durable queue claim owns the task while the coordinator
            # initializes NodeODM and transfers photos. Viewer must keep
            # reporting queued until the engine itself confirms status 20.
            code = 10
        else:
            code = {'uploading': 10, 'queued': 10, 'running': 20, 'held': 30,
                    'failed': 30, 'succeeded': 40}.get(state, 30)
        status = {'code': code}
        if code == 30:
            status['errorMessage'] = task['error'] or ('execution_held' if state == 'held' else 'processing_failed')
        result = {'uuid': identifier, 'name': task['name'], 'status': status,
                  'progress': 100 if code == 40 else task['progress'],
                  'imagesCount': task['images_count'], 'dateCreated': task['created_at']}
        if job and state == 'queued':
            result['ledgetopQueueState'] = 'waiting_for_dispatch'
            if task['phase'] == 'waiting_node':
                result['ledgetopQueueReason'] = ('waiting_node_options'
                    if task['error'] == 'options_unavailable' else 'waiting_node_capacity')
        if job and job.get('nodeId'):
            result['ledgetopNodeId'] = job['nodeId']
        if job and state == 'held':
            result['ledgetopHeld'] = True
        return result

    def output(self, identifier, line=0):
        if type(line) is not int or not 0 <= line <= 2 ** 53 - 1:
            raise ValueError('nonnegative output cursor required')
        return self.task(identifier)['output_lines'][line:]

    def cancel(self, identifier):
        with self._lock, self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = self._row(db, identifier)
            job = self._job(db, row)
            if job and job['status'] in ('running', 'held'):
                raise Conflict('active engine task requires confirmed cancellation')
            if row['stage'] == 'cancelled':
                return {'success': True, 'uuid': identifier}
            if job and job['status'] == 'queued':
                db.execute("UPDATE jobs SET status='failed',result=?,updated_at=CURRENT_TIMESTAMP WHERE id=?",
                           (canonical({'cancelled': True}), job['id']))
            elif job and job['status'] == 'succeeded':
                return {'success': True, 'uuid': identifier}
            db.execute("UPDATE viewer_tasks SET stage='cancelled',phase='cancelled',updated_at=CURRENT_TIMESTAMP WHERE uuid=?", (identifier,))
            return {'success': True, 'uuid': identifier}

    def remove(self, identifier):
        with self._lock, self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = self._row(db, identifier, removed=True)
            job = self._job(db, row)
            if job and job['status'] in ('running', 'held'):
                raise Conflict('active engine task cannot be removed')
            if job and job['status'] == 'queued':
                db.execute("UPDATE jobs SET status='failed',result=?,updated_at=CURRENT_TIMESTAMP WHERE id=?",
                           (canonical({'cancelled': True, 'removed': True}), job['id']))
            db.execute('UPDATE viewer_tasks SET removed=1,updated_at=CURRENT_TIMESTAMP WHERE uuid=?', (identifier,))
            return {'success': True, 'uuid': identifier}
