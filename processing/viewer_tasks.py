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
import stat
import threading
import uuid as uuid_module

from .maintenance import Conflict


VIEWER_PROFILE = 'odm-viewer-v1'
PHOTO_SUFFIXES = frozenset(('.jpg', '.jpeg', '.png', '.tif', '.tiff'))
INPUT_SUFFIXES = PHOTO_SUFFIXES | frozenset(('.txt', '.json', '.csv'))
NAME = re.compile(r'[A-Za-z0-9][A-Za-z0-9._ -]{0,179}\Z')
PHASE = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}\Z')
ERROR = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,119}\Z')
STAGES = frozenset(('uploading', 'queued', 'running', 'succeeded', 'failed', 'cancelled', 'held'))
TERMINAL = frozenset(('succeeded', 'failed', 'cancelled'))


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
    def __init__(self, queue, directory, max_input_bytes=20 * 1024 ** 3):
        if type(max_input_bytes) is not int or not 1 <= max_input_bytes <= 10 ** 15:
            raise ValueError('positive input limit required')
        self.queue = queue
        self.root = Path(directory).absolute()
        if self.root.resolve() != self.root:
            raise ValueError('input directory must not contain symlinks')
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        if not self.root.is_dir() or self.root.is_symlink():
            raise ValueError('private input directory required')
        if os.name != 'nt' and self.root.stat().st_mode & 0o077:
            raise ValueError('private input directory required')
        self.max_input_bytes = max_input_bytes
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
                error TEXT, progress REAL NOT NULL DEFAULT 0,
                output_lines TEXT NOT NULL DEFAULT '[]', archive_path TEXT,
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
            db.execute('''CREATE UNIQUE INDEX IF NOT EXISTS viewer_engine_uuid
                ON viewer_tasks(engine_uuid) WHERE engine_uuid IS NOT NULL''')

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

    def upload(self, identifier, filename, stream, length):
        key = self._filename(filename)
        if type(length) is not int or not 0 < length <= self.max_input_bytes:
            raise ValueError('invalid input length')
        with self._lock:
            with self.queue._db() as db:
                row = self._row(db, identifier)
                if row['stage'] != 'uploading' or row['job_id']:
                    raise Conflict('task is no longer accepting uploads')
                directory = self._directory(row)
                existing = db.execute('SELECT * FROM viewer_files WHERE uuid=? AND name_key=?',
                                      (identifier, key)).fetchone()
                total = db.execute('SELECT COALESCE(SUM(bytes),0) FROM viewer_files WHERE uuid=?',
                                   (identifier,)).fetchone()[0]
                count = db.execute('SELECT COUNT(*) FROM viewer_files WHERE uuid=?',
                                   (identifier,)).fetchone()[0]
                if existing is None and (total + length > self.max_input_bytes or count >= 100000):
                    raise ValueError('task input ceiling exceeded')
            temporary = directory / ('.incoming-' + secrets.token_hex(16))
            final = directory / ('input-' + secrets.token_hex(16))
            try:
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
                    row = self._row(db, identifier)
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
                    if total + length > self.max_input_bytes:
                        raise ValueError('task input ceiling exceeded')
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

    def update(self, identifier, **fields):
        allowed = {'stage', 'phase', 'node_id', 'engine_uuid', 'claim_token', 'error',
                   'progress', 'output_lines', 'archive_path'}
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
            if row['engine_uuid'] and fields.get('engine_uuid', row['engine_uuid']) != row['engine_uuid']:
                raise Conflict('engine task identity is immutable')
            if fields.get('engine_uuid') and db.execute(
                    'SELECT 1 FROM viewer_tasks WHERE engine_uuid=? AND uuid<>?',
                    (fields['engine_uuid'], identifier)).fetchone():
                raise Conflict('engine task identity belongs to another task')
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
        return self.task(identifier)

    def info(self, identifier):
        task = self.task(identifier)
        job = task['job']
        state = job['status'] if job else task['stage']
        recovering = (job and state == 'held' and job['heldReason'] == 'coordinator_restart'
                      and task['phase'] in ('processing', 'commit_pending', 'cancel_pending')
                      and task['engine_uuid'] and task['node_id'] and task['claim_token'])
        cancelled_result = (job and state == 'failed' and isinstance(job['result'], dict)
                            and job['result'].get('cancelled') is True)
        if task['stage'] == 'cancelled' or cancelled_result:
            code = 50
        elif recovering:
            # Retain the shared slot, but do not tell Viewer this recoverable
            # engine execution failed before its exact UUID is reconciled.
            code = 20
        else:
            code = {'uploading': 10, 'queued': 10, 'running': 20, 'held': 30,
                    'failed': 30, 'succeeded': 40}.get(state, 30)
        status = {'code': code}
        if code == 30:
            status['errorMessage'] = task['error'] or ('execution_held' if state == 'held' else 'processing_failed')
        return {'uuid': identifier, 'name': task['name'], 'status': status,
                'progress': 100 if code == 40 else task['progress'],
                'imagesCount': task['images_count'], 'dateCreated': task['created_at']}

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
