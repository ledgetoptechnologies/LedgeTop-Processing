"""Durable Viewer upload metadata sharing the coordinator's execution database.

``task`` and ``list_tasks`` are PRIVATE broker interfaces: they include filesystem
paths, claim credentials and engine identity. Never return them from an HTTP route.
Native public responses come from ``info``/``output``. Inputs and results survive
``remove``; a reinitialized UUID gets a fresh directory and execution request ID.
"""

import base64
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import re
import secrets
import stat
import threading
import uuid as uuid_module
from contextlib import contextmanager, nullcontext
from functools import wraps

from .maintenance import Conflict


class OptionEvidenceConflict(Conflict):
    """A task's admission evidence cannot be trusted."""


class InputLimitExceeded(ValueError):
    """A task's configured cumulative input-byte allowance was exceeded."""


VIEWER_PROFILE = 'odm-viewer-v1'
# Tombstones are retained while any delayed request can replay. This explicit
# finite ceiling bounds authenticated storage use; do not silently purge.
RESERVATION_LIMIT = 100000
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


def task_fenced(method):
    @wraps(method)
    def call(self, identifier, *args, **kwargs):
        # A control request can hold the scheduler's service lock. In the
        # isolated parallel mode it must not wait for a slow upload while
        # holding that lock; callers get a retryable conflict instead.
        blocking = not self.queue.parallel_viewer_uploads or method.__name__ == 'upload'
        with self._task_fence(identifier, blocking=blocking, upload=method.__name__ == 'upload'):
            return method(self, identifier, *args, **kwargs)
    return call


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
        self._lock = queue._viewer_store_lock
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
            if 'option_evidence_required' not in {row['name'] for row in db.execute('PRAGMA table_info(viewer_tasks)')}:
                db.execute('ALTER TABLE viewer_tasks ADD COLUMN option_evidence_required INTEGER NOT NULL DEFAULT 0 CHECK(option_evidence_required IN (0,1))')
            if 'full_option_evidence_required' not in {row['name'] for row in db.execute('PRAGMA table_info(viewer_tasks)')}:
                db.execute('ALTER TABLE viewer_tasks ADD COLUMN full_option_evidence_required INTEGER NOT NULL DEFAULT 0 CHECK(full_option_evidence_required IN (0,1))')
            if 'node_guarded' not in {row['name'] for row in db.execute('PRAGMA table_info(viewer_tasks)')}:
                db.execute('ALTER TABLE viewer_tasks ADD COLUMN node_guarded INTEGER NOT NULL DEFAULT 0 CHECK(node_guarded IN (0,1))')
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
            db.execute('''CREATE TABLE IF NOT EXISTS viewer_reservations (
                uuid TEXT PRIMARY KEY, environment TEXT NOT NULL, capability_hash TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('reserved','cancelled')))''')
            db.execute('''CREATE TABLE IF NOT EXISTS viewer_option_evidence (
                uuid TEXT NOT NULL, generation INTEGER NOT NULL,
                definitions TEXT NOT NULL, sha256 TEXT NOT NULL,
                PRIMARY KEY(uuid,generation))''')
            db.execute('''CREATE TABLE IF NOT EXISTS viewer_full_option_evidence (
                uuid TEXT NOT NULL, generation INTEGER NOT NULL,
                definitions TEXT NOT NULL, sha256 TEXT NOT NULL,
                PRIMARY KEY(uuid,generation))''')
            db.execute('''CREATE TABLE IF NOT EXISTS viewer_guarded_dispatch_checkpoint (
                uuid TEXT PRIMARY KEY, generation INTEGER NOT NULL, job_id TEXT NOT NULL,
                node_id TEXT NOT NULL, engine_uuid TEXT NOT NULL,
                sequence INTEGER NOT NULL, operation TEXT NOT NULL, evidence TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('pending','acknowledged')))''')
            db.execute('''CREATE TABLE IF NOT EXISTS viewer_guarded_dispatch_history (
                uuid TEXT NOT NULL, generation INTEGER NOT NULL, job_id TEXT NOT NULL,
                node_id TEXT NOT NULL, engine_uuid TEXT NOT NULL,
                sequence INTEGER NOT NULL, operation TEXT NOT NULL, evidence TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('pending','acknowledged')),
                PRIMARY KEY(uuid,sequence))''')

    def checkpoint_guarded_dispatch(self, identifier, operation, evidence):
        """Save the exact pending mutation before sending; this is not node proof."""
        if operation not in ('init', 'upload', 'commit') or type(evidence) is not dict:
            raise ValueError('invalid guarded dispatch checkpoint')
        encoded = canonical(evidence)
        if len(encoded.encode('utf-8')) > 4096:
            raise ValueError('guarded dispatch checkpoint too large')
        with self._lock, self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = self._row(db, identifier)
            if row['node_guarded'] != 1 or not all(row[key] for key in ('job_id','node_id','engine_uuid')):
                raise Conflict('guarded task identity unavailable')
            previous = db.execute('SELECT * FROM viewer_guarded_dispatch_checkpoint WHERE uuid=?', (identifier,)).fetchone()
            if previous is not None and previous['state'] != 'acknowledged':
                raise Conflict('guarded mutation requires reconciliation')
            history = db.execute('SELECT * FROM viewer_guarded_dispatch_history WHERE uuid=? ORDER BY sequence DESC LIMIT 1', (identifier,)).fetchone()
            # Do not manufacture a history for checkpoints written by an older
            # binary: earlier operations cannot be recovered from the last row.
            if (previous is None) != (history is None) or (previous is not None and dict(previous) != dict(history)):
                raise Conflict('guarded dispatch history requires reconciliation')
            sequence = previous['sequence'] + 1 if previous is not None else 1
            db.execute('''INSERT INTO viewer_guarded_dispatch_history
                (uuid,generation,job_id,node_id,engine_uuid,sequence,operation,evidence,state)
                VALUES(?,?,?,?,?,?,?,?, 'pending')''', (identifier,row['generation'],row['job_id'],row['node_id'],
                row['engine_uuid'],sequence,operation,encoded))
            db.execute('''INSERT OR REPLACE INTO viewer_guarded_dispatch_checkpoint
                (uuid,generation,job_id,node_id,engine_uuid,sequence,operation,evidence,state)
                VALUES(?,?,?,?,?,?,?,?, 'pending')''', (identifier,row['generation'],row['job_id'],row['node_id'],
                row['engine_uuid'],sequence,operation,encoded))
            return sequence

    def acknowledge_guarded_dispatch(self, identifier, sequence):
        """Record an observed response; recovery must independently verify it."""
        with self._lock, self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = self._row(db, identifier)
            changed = db.execute('''UPDATE viewer_guarded_dispatch_checkpoint SET state='acknowledged'
                WHERE uuid=? AND sequence=? AND state='pending' AND generation=? AND job_id=?
                AND node_id=? AND engine_uuid=?''', (identifier,sequence,row['generation'],row['job_id'],
                row['node_id'],row['engine_uuid'])).rowcount
            if changed != 1:
                raise Conflict('guarded dispatch checkpoint changed')
            changed = db.execute('''UPDATE viewer_guarded_dispatch_history SET state='acknowledged'
                WHERE uuid=? AND sequence=? AND state='pending' AND generation=? AND job_id=?
                AND node_id=? AND engine_uuid=?''', (identifier,sequence,row['generation'],row['job_id'],
                row['node_id'],row['engine_uuid'])).rowcount
            if changed != 1:
                raise Conflict('guarded dispatch history changed')

    def guarded_dispatch_checkpoint(self, identifier):
        with self._lock, self.queue._db() as db:
            row = db.execute('SELECT * FROM viewer_guarded_dispatch_checkpoint WHERE uuid=?', (identifier,)).fetchone()
            return dict(row) if row is not None else None

    def guarded_dispatch_history(self, identifier):
        """Return retained coordinator observations, not proof of node execution."""
        with self._lock, self.queue._db() as db:
            self._row(db, identifier)
            return [dict(row) for row in db.execute(
                'SELECT * FROM viewer_guarded_dispatch_history WHERE uuid=? ORDER BY sequence', (identifier,))]

    @staticmethod
    def _full_option_evidence(db, row):
        evidence = db.execute('SELECT definitions,sha256 FROM viewer_full_option_evidence WHERE uuid=? AND generation=?',
                              (row['uuid'], row['generation'])).fetchone()
        if row['full_option_evidence_required'] not in (0, 1) or bool(row['full_option_evidence_required']) != (evidence is not None):
            raise OptionEvidenceConflict('full option evidence missing or unexpected')
        if evidence is None:
            return None
        try:
            text = evidence['definitions']
            if type(text) is not str or len(text.encode()) > 262144 or hashlib.sha256(text.encode()).hexdigest() != evidence['sha256']:
                raise ValueError()
            value = json.loads(text)
            if (type(value) is not list or not value or canonical(value) != text
                    or any(type(item) is not dict or type(item.get('name')) is not str for item in value)
                    or len({item['name'] for item in value}) != len(value)):
                raise ValueError()
            return value
        except (ValueError, TypeError, KeyError, RecursionError, UnicodeError):
            raise OptionEvidenceConflict('full option evidence changed') from None

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

    @staticmethod
    def _capability_hash(identifier, environment, capability):
        checked_uuid(identifier)
        if environment not in ('production','staging') or not isinstance(capability,str) or not re.fullmatch(r'[A-Za-z0-9_-]{43}',capability):
            raise ValueError('invalid reservation credential')
        try:raw=base64.urlsafe_b64decode(capability+'=')
        except Exception:raise ValueError('invalid reservation credential') from None
        if len(raw)!=32 or base64.urlsafe_b64encode(raw).decode().rstrip('=')!=capability:
            raise ValueError('invalid reservation credential')
        return hashlib.sha256(b'ledgetop-viewer-reservation-v1\0'+environment.encode()+b'\0'+identifier.encode()+b'\0'+raw).hexdigest()

    def _reservation(self, db, identifier, environment, capability, *, allow_cancelled=False):
        row=db.execute('SELECT * FROM viewer_reservations WHERE uuid=?',(identifier,)).fetchone()
        if row is None:
            if capability is not None:raise KeyError('unknown reservation')
            return None
        try:value=self._capability_hash(identifier,environment,capability)
        except ValueError:raise KeyError('unknown reservation') from None
        if row['environment']!=environment or not hmac.compare_digest(row['capability_hash'],value):
            raise KeyError('unknown reservation')
        if row['state']=='cancelled' and not allow_cancelled:raise Conflict('task reservation is cancelled')
        return row

    @contextmanager
    def _task_fence(self, identifier, *, blocking=True, upload=False):
        checked_uuid(identifier)
        queue = self.queue
        if os.getpid() != queue._creator_pid:
            raise RuntimeError('coordinator process changed')
        with queue._viewer_task_locks_guard:
            entry = queue._viewer_task_locks.get(identifier)
            if entry is None:
                entry = [threading.RLock(), 0, 0]
                queue._viewer_task_locks[identifier] = entry
            entry[1] += 1
            if upload:
                entry[2] += 1
            elif not blocking and entry[2]:
                entry[1] -= 1
                raise Conflict('task upload is in progress; retry later')
            # Controls reserve the mutex while holding the registry lock, so
            # an upload cannot register between the count check and acquire.
            preacquired = entry[0].acquire(blocking=False) if not blocking else False
        try:
            acquired = entry[0].acquire() if blocking else preacquired
            if not acquired:
                raise Conflict('task upload is in progress; retry later')
            try:
                # Keep the OS authority lock until even a slow stream finishes.
                with queue._lifecycle_lock:
                    if queue._lock.closed:
                        raise RuntimeError('coordinator ownership closed')
                    queue._database_users += 1
                try:
                    yield
                finally:
                    with queue._lifecycle_lock:
                        queue._database_users -= 1
            finally:
                entry[0].release()
        finally:
            with queue._viewer_task_locks_guard:
                entry[1] -= 1
                if upload:
                    entry[2] -= 1
                if not entry[1]:
                    del queue._viewer_task_locks[identifier]

    @task_fenced
    def reserve(self, identifier, environment, capability):
        digest=self._capability_hash(identifier,environment,capability)
        with self._lock,self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row=db.execute('SELECT * FROM viewer_reservations WHERE uuid=?',(identifier,)).fetchone()
            if row is not None:
                self._reservation(db,identifier,environment,capability,allow_cancelled=True)
                return {'uuid':identifier,'state':row['state']}
            if db.execute('SELECT 1 FROM viewer_tasks WHERE uuid=?',(identifier,)).fetchone():raise Conflict('UUID already belongs to a legacy task')
            if db.execute('SELECT COUNT(*) FROM viewer_reservations').fetchone()[0]>=RESERVATION_LIMIT:raise Conflict('reservation history limit reached')
            db.execute("INSERT INTO viewer_reservations(uuid,environment,capability_hash,state) VALUES (?,?,?,'reserved')",(identifier,environment,digest))
            return {'uuid':identifier,'state':'reserved'}

    def reservation_stats(self):
        with self.queue._db() as db:
            used=db.execute('SELECT COUNT(*) FROM viewer_reservations').fetchone()[0]
        return {'version':1,'capabilityEncoding':'base64url-32','maxRetained':RESERVATION_LIMIT,
                'remaining':max(0,RESERVATION_LIMIT-used),'retention':'indefinite',
                'runningCancellation':'requires_inspection'}

    def authorize_reservation(self,identifier,environment,capability):
        with self.queue._db() as db:self._reservation(db,identifier,environment,capability,allow_cancelled=True)

    @task_fenced
    def cancel_reservation(self, identifier, environment, capability):
        with self._lock,self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            reserved=self._reservation(db,identifier,environment,capability,allow_cancelled=True)
            if reserved is None:raise KeyError('unknown reservation')
            if reserved['state']=='cancelled':return {'uuid':identifier,'state':'cancelled'}
            row=db.execute('SELECT * FROM viewer_tasks WHERE uuid=?',(identifier,)).fetchone()
            if row is not None:
                job=self._job(db,row)
                if (row['removed'] or row['stage'] not in ('uploading','queued')
                        or (row['stage']=='uploading' and job is not None)
                        or (row['stage']=='queued' and (job is None or job['status']!='queued'))):
                    raise Conflict('execution requires inspection')
                if job:
                    db.execute("UPDATE jobs SET status='failed',result=?,updated_at=CURRENT_TIMESTAMP WHERE id=? AND status='queued'",(canonical({'cancelled':True}),job['id']))
                db.execute("UPDATE viewer_tasks SET stage='cancelled',phase='cancelled',updated_at=CURRENT_TIMESTAMP WHERE uuid=?",(identifier,))
            db.execute("UPDATE viewer_reservations SET state='cancelled' WHERE uuid=?",(identifier,))
            return {'uuid':identifier,'state':'cancelled'}

    @task_fenced
    def initialize(self, identifier, name='', options=None, outputs=None, environment='production', capability=None, option_evidence=None, *, full_option_evidence=None, replay_only=False):
        checked_uuid(identifier)
        if type(replay_only) is not bool: raise ValueError('invalid replay mode')
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
        evidence_text = None
        full_evidence_text = None
        if full_option_evidence is not None:
            if (type(full_option_evidence) is not list or not full_option_evidence
                    or any(type(item) is not dict or type(item.get('name')) is not str for item in full_option_evidence)
                    or len({item['name'] for item in full_option_evidence}) != len(full_option_evidence)):
                raise ValueError('full option definitions required')
            try:
                full_evidence_text = canonical(sorted(full_option_evidence, key=lambda item: item['name']))
            except (ValueError, TypeError, RecursionError):
                raise ValueError('invalid full option definitions') from None
            if len(full_evidence_text.encode()) > 262144:
                raise ValueError('full option definitions too large')
        if option_evidence is not None:
            if (type(option_evidence) is not list or len(option_evidence)!=len(options)
                    or any(type(item) is not dict or type(item.get('name')) is not str for item in option_evidence)
                    or sorted(item['name'] for item in option_evidence)!=sorted(item['name'] for item in options)):
                raise ValueError('exact option definitions required')
            try:
                evidence_text=canonical(sorted(option_evidence,key=lambda item:item['name']))
            except (ValueError,TypeError,RecursionError):
                raise ValueError('invalid option definitions') from None
            if len(evidence_text.encode())>262144:raise ValueError('option definitions too large')
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
            reserved=self._reservation(db,identifier,environment,capability)
            row = db.execute('SELECT * FROM viewer_tasks WHERE uuid=?', (identifier,)).fetchone()
            # A read-only exact replay is permitted only for an authenticated
            # reservation. Legacy UUIDs always pass through current catalog checks.
            if replay_only and (reserved is None or row is None): return None
            if reserved is not None and row is not None and (row['removed'] or row['stage']=='cancelled'):
                raise Conflict('reserved UUID cannot be reset')
            if replay_only and row is not None:
                self._directory(row)
                self._job(db,row)
                self._full_option_evidence(db, row)
                evidence=db.execute('SELECT definitions,sha256 FROM viewer_option_evidence WHERE uuid=? AND generation=?',(identifier,row['generation'])).fetchone()
                if row['option_evidence_required'] not in (0,1) or bool(row['option_evidence_required']) != (evidence is not None):
                    raise Conflict('option evidence unavailable')
                if evidence is not None:
                    try:
                        text=evidence['definitions']
                        if type(text) is not str or len(text.encode())>262144 or hashlib.sha256(text.encode()).hexdigest()!=evidence['sha256']:
                            raise ValueError()
                        definitions=json.loads(text)
                        if (type(definitions) is not list or canonical(definitions)!=text
                                or any(type(item) is not dict or type(item.get('name')) is not str for item in definitions)
                                or sorted(item['name'] for item in definitions)!=sorted(item['name'] for item in options)):
                            raise ValueError()
                    except (ValueError,TypeError,KeyError,RecursionError,UnicodeError):
                        raise Conflict('option evidence unavailable') from None
            if row is not None and row['environment'] != environment:
                # Authorization and task operations use separate reads. Keep
                # the original UUID's credential domain stable even while a
                # removed task is reset into a new private incarnation.
                raise Conflict('task UUID belongs to another environment')
            if row is not None and not row['removed']:
                self._full_option_evidence(db, row)
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
                (uuid,generation,directory,name,options,outputs,environment,option_evidence_required,full_option_evidence_required)
                VALUES(?,?,?,?,?,?,?,?,?)''', (identifier, generation, directory_name, name,
                                          options_text, outputs_text, environment, int(evidence_text is not None), int(full_evidence_text is not None)))
            if evidence_text is not None:
                db.execute('INSERT INTO viewer_option_evidence(uuid,generation,definitions,sha256) VALUES(?,?,?,?)',
                    (identifier,generation,evidence_text,hashlib.sha256(evidence_text.encode()).hexdigest()))
            if full_evidence_text is not None:
                db.execute('INSERT INTO viewer_full_option_evidence(uuid,generation,definitions,sha256) VALUES(?,?,?,?)',
                    (identifier,generation,full_evidence_text,hashlib.sha256(full_evidence_text.encode()).hexdigest()))
            return {'uuid': identifier}

    @staticmethod
    def _filename(name):
        if (not isinstance(name, str) or not NAME.fullmatch(name)
                or Path(name).suffix.lower() not in INPUT_SUFFIXES
                or name.endswith((' ', '.')) or name in ('.', '..')):
            raise ValueError('safe supported input basename required')
        return name.casefold()

    @task_fenced
    def upload(self, identifier, filename, stream, length, environment=None, capability=None):
        key = self._filename(filename)
        if type(length) is not int or length <= 0:
            raise ValueError('invalid input length')
        if length > self.max_input_bytes:
            raise InputLimitExceeded('task input ceiling exceeded')
        with (nullcontext() if self.queue.parallel_viewer_uploads else self._lock):
            with self.queue._db() as db:
                self._reservation(db,identifier,environment,capability)
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
                if existing is None and total + length > self.max_input_bytes:
                    raise InputLimitExceeded('task input ceiling exceeded')
                if existing is None and count >= 100000:
                    raise ValueError('task file count ceiling exceeded')
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
                    self._reservation(db,identifier,environment,capability)
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
                        raise InputLimitExceeded('task input ceiling exceeded')
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

    @task_fenced
    def commit(self, identifier, environment=None, capability=None):
        with self._lock, self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            self._reservation(db,identifier,environment,capability)
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
        evidence=db.execute('SELECT definitions,sha256 FROM viewer_option_evidence WHERE uuid=? AND generation=?',
                            (row['uuid'],row['generation'])).fetchone()
        value['option_evidence']=None
        value['full_option_evidence']=self._full_option_evidence(db, row)
        if row['option_evidence_required'] not in (0,1) or bool(row['option_evidence_required']) != (evidence is not None):
            raise OptionEvidenceConflict('option evidence missing or unexpected')
        if evidence is not None:
            text=evidence['definitions']
            if (type(text) is not str or len(text.encode())>262144
                    or hashlib.sha256(text.encode()).hexdigest()!=evidence['sha256']):
                raise OptionEvidenceConflict('option evidence changed')
            try:
                definitions=json.loads(text)
                if (type(definitions) is not list or canonical(definitions)!=text
                        or any(type(item) is not dict or type(item.get('name')) is not str for item in definitions)
                        or sorted(item['name'] for item in definitions)!=sorted(item['name'] for item in value['options'])):
                    raise ValueError()
            except (ValueError,TypeError,KeyError,RecursionError):
                raise OptionEvidenceConflict('option evidence changed') from None
            value['option_evidence']=definitions
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

    def task_environment(self, identifier):
        """Read only the immutable credential domain without parsing evidence."""
        with self._lock, self.queue._db() as db:
            return self._row(db, identifier)['environment']

    def list_tasks(self):
        with self._lock, self.queue._db() as db:
            return [self._task(db, row) for row in db.execute(
                'SELECT * FROM viewer_tasks WHERE removed=0 ORDER BY created_at,rowid').fetchall()]

    def task_for_job(self, job_id):
        """Load only the task selected for dispatch or active reconciliation."""
        with self._lock, self.queue._db() as db:
            rows=db.execute('SELECT * FROM viewer_tasks WHERE removed=0 AND job_id=? LIMIT 2',(job_id,)).fetchall()
            if len(rows)!=1:raise Conflict('execution metadata unavailable')
            return self._task(db,rows[0])

    def incomplete_terminal_job_ids(self):
        with self._lock, self.queue._db() as db:
            return {row[0] for row in db.execute("SELECT t.job_id FROM viewer_tasks t JOIN jobs j ON j.id=t.job_id WHERE t.removed=0 AND (t.phase IN ('processing','commit_pending','cancel_pending') OR (t.node_guarded=1 AND t.phase IN ('init_pending','upload_pending'))) AND j.status IN ('succeeded','failed')")}

    def complete_engine_task(self, task, status, result):
        """Commit the queue terminal state and matching Viewer task together."""
        if status not in ('succeeded','failed'):
            raise ValueError('invalid engine completion')
        with self._lock, self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row=self._row(db,task['uuid'])
            if row['node_guarded'] == 1 and row['phase'] in ('init_pending','upload_pending','commit_pending'):
                raise Conflict('guarded mutation requires reconciliation')
            if (row['job_id']!=task['job_id'] or row['engine_uuid']!=task['engine_uuid']
                    or row['node_id']!=task['node_id'] or row['claim_token']!=task['claim_token']
                    or row['environment']!=task['environment'] or row['phase'] not in ('processing','commit_pending','cancel_pending')):
                raise Conflict('engine completion identity changed')
            job=db.execute('SELECT node_id,environment,profile FROM jobs WHERE id=?',(row['job_id'],)).fetchone()
            if (job is None or job['node_id']!=row['node_id'] or job['environment']!=row['environment']
                    or job['profile']!=VIEWER_PROFILE):
                raise Conflict('engine job binding changed')
            self.queue._complete_in_transaction(db,row['job_id'],
                {'claimToken':row['claim_token'],'status':status,'result':result})
            db.execute("UPDATE viewer_tasks SET stage=?,phase='complete',progress=?,updated_at=CURRENT_TIMESTAMP WHERE uuid=?",
                (status,100 if status=='succeeded' else row['progress'],row['uuid']))

    def hold_reconciliation_error(self, job_id):
        """Hold an unambiguous Viewer job and its task in one transaction."""
        with self._lock, self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            rows = db.execute('SELECT uuid FROM viewer_tasks WHERE removed=0 AND job_id=? LIMIT 2', (job_id,)).fetchall()
            job = db.execute('SELECT status,profile,environment,held_reason FROM jobs WHERE id=?', (job_id,)).fetchone()
            if len(rows) != 1 or job is None or job['profile'] != VIEWER_PROFILE:
                raise Conflict('reconciliation hold identity unavailable')
            if job['status'] != 'running' and not (job['status'] == 'held' and job['held_reason'] == 'coordinator_restart'):
                raise Conflict('reconciliation hold state changed')
            result = json.dumps({'environment': job['environment'], 'operatorAction': 'hold',
                                 'note': 'reconciliation_error', 'workerStopped': False})
            db.execute("UPDATE jobs SET status='held',result=?,held_reason='operator',updated_at=CURRENT_TIMESTAMP WHERE id=?", (result, job_id))
            db.execute("UPDATE viewer_tasks SET stage='held',phase='held',error='reconciliation_error',updated_at=CURRENT_TIMESTAMP WHERE uuid=?", (rows[0]['uuid'],))

    def settle_operator_resolution(self, task):
        """Finish a hold the operator resolved before the Viewer row recorded it.

        An operator hold of a running job, or a crash between the queue hold and
        the Viewer update, leaves the row in an engine phase. Record the same
        held outcome the normal hold path writes; no queue state or bytes change.
        """
        with self._lock, self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row=self._row(db,task['uuid'])
            identity=('job_id','engine_uuid','node_id','claim_token','environment')
            if (any(type(row[k]) is not str or not row[k] or row[k]!=task[k] for k in identity)
                    or row['removed'] or row['phase'] not in ('processing','commit_pending','cancel_pending')
                    or db.execute('SELECT COUNT(*) FROM viewer_tasks WHERE removed=0 AND job_id=?',
                                  (row['job_id'],)).fetchone()[0]!=1):
                raise Conflict('operator resolution identity changed')
            job=db.execute('SELECT status,result,node_id,environment,profile,claim_hash FROM jobs WHERE id=?',(row['job_id'],)).fetchone()
            if (job is None or not job['claim_hash'] or not hmac.compare_digest(
                    job['claim_hash'],hashlib.sha256(row['claim_token'].encode()).hexdigest())):
                raise Conflict('terminal Viewer identity requires inspection')
            try:
                result=json.loads(job['result']) if job is not None and type(job['result']) is str else None
            except ValueError:
                result=None
            if (job is None or job['status']!='failed' or job['profile']!=VIEWER_PROFILE
                    or job['environment']!=row['environment'] or job['node_id']!=row['node_id']
                    or type(result) is not dict or set(result)!={'environment','operatorAction','note','workerStopped'}
                    or result['operatorAction']!='resolve' or result['workerStopped'] is not True
                    or result['environment']!=row['environment']):
                raise Conflict('terminal Viewer identity requires inspection')
            db.execute("UPDATE viewer_tasks SET stage='held',phase='held',error='operator_resolved',updated_at=CURRENT_TIMESTAMP WHERE uuid=?",
                (row['uuid'],))

    @task_fenced
    def update(self, identifier, **fields):
        allowed = {'stage', 'phase', 'node_id', 'engine_uuid', 'claim_token', 'error',
                   'progress', 'output_lines', 'archive_path', 'node_guarded'}
        if not fields or not set(fields) <= allowed:
            raise ValueError('unexpected broker update fields')
        if 'stage' in fields and fields['stage'] not in STAGES:
            raise ValueError('invalid task stage')
        if 'node_guarded' in fields and type(fields['node_guarded']) is not bool:
            raise ValueError('guard marker must be boolean')
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
            if row['node_guarded'] and fields.get('node_guarded', True) is not True:
                raise Conflict('guard marker cannot be cleared')
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
        try:
            task = self.task(identifier)
        except OptionEvidenceConflict:
            # Evidence is never returned; only exact cancellation or a hold.
            with self._lock, self.queue._db() as db:
                row=self._row(db,identifier);job=self._job(db,row)
                count=db.execute('SELECT COUNT(*) FROM viewer_files WHERE uuid=? AND is_photo=1',(identifier,)).fetchone()[0]
                if self._cancelled_never_started(db,row,job):
                    return {'uuid':identifier,'name':row['name'],'status':{'code':50},
                            'progress':row['progress'],'imagesCount':count,'dateCreated':row['created_at']}
                if job is None or job['status'] not in ('queued','running','held'):
                    raise
                # Evidence corruption means observed execution state cannot be
                # trusted; retain the job without claiming processing.
                code=10
                return {'uuid':identifier,'name':row['name'],'status':{'code':code},
                        'progress':row['progress'],'imagesCount':count,'dateCreated':row['created_at'],
                        'ledgetopHeld':True,'ledgetopHoldReason':'option_evidence_invalid'}
        job = task['job']
        state = job['status'] if job else task['stage']
        if (task['node_guarded'] == 1 and task['phase'] in ('init_pending','upload_pending','commit_pending')
                and state in ('succeeded','failed')):
            # A terminal queue row cannot prove that a guarded mutation was
            # acknowledged. Surface the split as held until reconciled.
            state = 'held'
        if state == 'running' and task['stage'] != 'running':
            # A coordinator claim and a successful commit ACK do not establish
            # that the engine has actually begun processing.
            state = 'queued'
        cancelled_result = (job and state == 'failed' and isinstance(job['result'], dict)
                            and job['result'].get('cancelled') is True)
        if state == 'held':
            # Never assert engine processing for a held task: the hold can
            # arise before engine start, and older rows do not preserve proof.
            # The explicit hold marker keeps compatible clients from retrying.
            code = 10
        elif task['stage'] == 'cancelled' or cancelled_result:
            code = 50
        else:
            code = {'uploading': 10, 'queued': 10, 'running': 20,
                    'failed': 30, 'succeeded': 40}.get(state, 30)
        status = {'code': code}
        if code == 30:
            status['errorMessage'] = task['error'] or ('execution_held' if state == 'held' else 'processing_failed')
        info = {'uuid': identifier, 'name': task['name'], 'status': status,
                'progress': 100 if code == 40 else task['progress'],
                'imagesCount': task['images_count'], 'dateCreated': task['created_at']}
        if job and job['status'] == 'queued' and task['node_id'] is None:
            # This is a queue observation, not a diagnosis of why dispatch has
            # not happened. It also covers queued jobs beyond the queue head.
            info['ledgetopQueueState'] = 'waiting_for_dispatch'
        if state == 'held':
            # Fixed marker only: held notes and engine error text can be private.
            info['ledgetopHeld'] = True
        return info

    def output(self, identifier, line=0):
        if type(line) is not int or not 0 <= line <= 2 ** 53 - 1:
            raise ValueError('nonnegative output cursor required')
        return self.task(identifier)['output_lines'][line:]

    @task_fenced
    def cancel(self, identifier):
        with self._lock, self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            self._reservation(db,identifier,None,None)
            row = self._row(db, identifier)
            self._reject_guarded_pending_action(row)
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

    def _never_started_identity(self, db, row, job):
        return bool(job and job['profile'] == VIEWER_PROFILE
            and job['environment'] == row['environment']
            and job['request_id'] == self._request_id(row)
            and all(row[k] is None for k in ('node_id','engine_uuid','claim_token'))
            and all(job[k] is None for k in ('worker_id','claim_hash','node_id','held_reason'))
            and db.execute('SELECT COUNT(*) FROM viewer_tasks WHERE job_id=?', (job['id'],)).fetchone()[0] == 1
            and not db.execute('SELECT 1 FROM viewer_incarnations WHERE job_id=? LIMIT 1', (job['id'],)).fetchone())

    def _cancelled_never_started(self, db, row, job):
        return (self._never_started_identity(db, row, job) and job['status'] == 'failed'
                and job['result'] == canonical({'cancelled':True})
                and row['stage'] == 'cancelled' and row['phase'] == 'cancelled')

    @staticmethod
    def _reject_guarded_pending_action(row):
        if row['node_guarded'] == 1 and row['phase'] in ('init_pending', 'upload_pending', 'commit_pending'):
            raise Conflict('guarded mutation requires reconciliation')

    @task_fenced
    def cancel_never_started(self, identifier):
        """Cancel or acknowledge an unambiguously unclaimed job atomically."""
        with self._lock, self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            self._reservation(db,identifier,None,None)
            row=self._row(db,identifier)
            self._reject_guarded_pending_action(row)
            job=self._job(db,row)
            if self._cancelled_never_started(db, row, job):
                return {'success':True,'uuid':identifier}
            if not job or job['status'] != 'queued':
                return None
            if (not self._never_started_identity(db, row, job) or job['result'] is not None
                    or row['stage'] != 'queued' or row['phase'] not in ('queued','waiting_node')):
                raise Conflict('queued execution identity uncertain')
            db.execute("UPDATE jobs SET status='failed',result=?,updated_at=CURRENT_TIMESTAMP WHERE id=? AND status='queued'",
                       (canonical({'cancelled':True}),job['id']))
            db.execute("UPDATE viewer_tasks SET stage='cancelled',phase='cancelled',updated_at=CURRENT_TIMESTAMP WHERE uuid=?",(identifier,))
            return {'success':True,'uuid':identifier}

    @task_fenced
    def remove(self, identifier):
        with self._lock, self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            self._reservation(db,identifier,None,None)
            row = self._row(db, identifier, removed=True)
            self._reject_guarded_pending_action(row)
            job = self._job(db, row)
            if job and job['status'] in ('running', 'held'):
                raise Conflict('active engine task cannot be removed')
            if job and job['status'] == 'queued':
                db.execute("UPDATE jobs SET status='failed',result=?,updated_at=CURRENT_TIMESTAMP WHERE id=?",
                           (canonical({'cancelled': True, 'removed': True}), job['id']))
            db.execute('UPDATE viewer_tasks SET removed=1,updated_at=CURRENT_TIMESTAMP WHERE uuid=?', (identifier,))
            return {'success': True, 'uuid': identifier}
