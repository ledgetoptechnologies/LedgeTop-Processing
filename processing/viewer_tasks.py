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
import sqlite3

from .maintenance import Conflict


class OptionEvidenceConflict(Conflict):
    """A task's admission evidence cannot be trusted."""


class ArchiveClaimHeld(Conflict):
    """An archive capture claim is already held by an unknown writer."""


# Experimental opt-in only. Qualification requires a real old-writer fence.
INTAKE_JOURNAL_QUALIFIED = False
INTAKE_CENSUS_MAX_FILES = 100000
INTAKE_CENSUS_MAX_ENTRIES = 200000
INTAKE_JOURNAL_DDL = """CREATE TABLE viewer_intake_journal_v1 (
    operation TEXT PRIMARY KEY, uuid TEXT NOT NULL, generation INTEGER NOT NULL,
    name_key TEXT NOT NULL, directory TEXT NOT NULL, temporary TEXT NOT NULL,
    final TEXT NOT NULL, bytes INTEGER NOT NULL, sha256 TEXT,
    state TEXT NOT NULL CHECK(state IN ('prepared','sealed','cleanup','committed')),
    UNIQUE(uuid,generation,name_key), UNIQUE(directory,temporary), UNIQUE(directory,final))"""

INTAKE_INDEX_DDL = "CREATE INDEX viewer_intake_unfinished_v1 ON viewer_intake_journal_v1(uuid,generation) WHERE state!='committed'"

class InputRecoveryRequired(Conflict):
    """Input files and database metadata need offline inspection."""


class InputLimitExceeded(ValueError):
    """A task's configured cumulative input-byte allowance was exceeded."""


VIEWER_PROFILE = 'odm-viewer-v1'
# Tombstones are retained while any delayed request can replay. This explicit
# finite ceiling bounds authenticated storage use; do not silently purge.
RESERVATION_LIMIT = 100000
# Never retire identity evidence automatically. Existing entries remain usable
# even when a previous version has already exceeded this admission ceiling.
ARCHIVE_REGISTRY_LIMIT = 100000
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
    def prepare_guarded_engine_upload(self, job_id, claim_token, name_key, upload_id, *, expected_generation=None):
        """Record exact DB-owned file identity; unwired, no physical fencing."""
        from . import engine_upload_contract as contract
        if contract.QUALIFIED is not True:
            raise Conflict('guarded file upload not qualified')
        checked_uuid(job_id);checked_uuid(upload_id)
        if (type(claim_token) is not str or not 32<=len(claim_token)<=128
            or type(name_key) is not str or self._filename(name_key)!=name_key
            or (expected_generation is not None and (type(expected_generation) is not int or expected_generation<1))):
            raise Conflict('exact upload identity required')
        with self._lock,self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            job=db.execute('SELECT * FROM jobs WHERE id=?',(job_id,)).fetchone()
            task=db.execute('SELECT * FROM viewer_tasks WHERE job_id=?',(job_id,)).fetchone()
            if (job is None or task is None or job['status']!='running' or job['worker_id']!='viewer-dispatch'
                or db.execute('SELECT COUNT(*) FROM viewer_tasks WHERE job_id=?',(job_id,)).fetchone()[0]!=1
                or job['profile']!=VIEWER_PROFILE or not job['claim_hash']
                or not hmac.compare_digest(hashlib.sha256(claim_token.encode()).hexdigest(),job['claim_hash'])
                or task['claim_token']!=claim_token or task['stage']!='running' or task['phase']!='upload_pending'
                or task['environment']!=job['environment'] or task['node_id']!=job['node_id']
                or (expected_generation is not None and task['generation']!=expected_generation)
                or task['removed'] or type(task['generation']) is not int or task['generation']<1):
                raise Conflict('active task claim required')
            checked_uuid(task['uuid']);checked_uuid(task['engine_uuid'])
            file=db.execute('SELECT * FROM viewer_files WHERE uuid=? AND name_key=?',(task['uuid'],name_key)).fetchone()
            if (file is None or self._filename(file['name'])!=name_key or type(file['bytes']) is not int
                or file['bytes']<=0 or type(file['sha256']) is not str or not re.fullmatch('[0-9a-f]{64}',file['sha256'])
                or type(file['path']) is not str or not re.fullmatch('input-[0-9a-f]{32}',file['path'])):
                raise Conflict('stored input identity unavailable')
            self._directory(task)
            schema=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='viewer_engine_upload_intents'").fetchone()
            if schema is None:db.execute(contract.DDL)
            elif schema[0]!=contract.DDL:raise Conflict('file intent schema changed')
            if (db.execute('SELECT 1 FROM viewer_engine_upload_intents WHERE upload_id=? OR (job_id=? AND state!=?)',(upload_id,job_id,'complete')).fetchone()
                or db.execute('SELECT 1 FROM viewer_engine_upload_intents WHERE task_uuid=? AND generation=? AND name_key=?',(task['uuid'],task['generation'],name_key)).fetchone()
                or db.execute('SELECT COUNT(*) FROM viewer_engine_upload_intents').fetchone()[0]>=100000):
                raise Conflict('file upload intent requires inspection')
            db.execute("INSERT INTO viewer_engine_upload_intents VALUES(?,?,?,?,?,?,?,?,?,?,?,'prepared')",
                       (upload_id,job_id,task['uuid'],task['generation'],task['engine_uuid'],job['claim_hash'],
                        file['name_key'],file['name'],file['path'],file['bytes'],file['sha256']))
            return {'state':'prepared'}

    def __init__(self, queue, directory, max_input_bytes=20 * 1024 ** 3, *, input_free_floor=None, intake_journal=False):
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
        if input_free_floor is not None and (type(input_free_floor) is not int or input_free_floor<0):
            raise ValueError('invalid input free-space floor')
        self.input_free_floor=input_free_floor
        if type(intake_journal) is not bool or (intake_journal and
                (not INTAKE_JOURNAL_QUALIFIED or input_free_floor is None
                 or getattr(queue,'_intake_guard_enabled',False) is not True)):
            raise ValueError('intake journal not qualified')
        self.intake_journal=intake_journal
        self._intake_reconciled=set()
        if intake_journal:
            with self.queue._db() as db:
                schema=db.execute("SELECT sql FROM sqlite_master WHERE name='viewer_intake_journal_v1' AND type='table'").fetchone()
                index=db.execute("SELECT sql FROM sqlite_master WHERE name='viewer_intake_unfinished_v1' AND type='index'").fetchone()
                if schema is None or schema[0]!=INTAKE_JOURNAL_DDL or index is None or index[0]!=INTAKE_INDEX_DDL:
                    raise InputRecoveryRequired('intake schema requires offline migration')
        self._lock = threading.RLock()
        self._archive_locks_lock = threading.Lock()
        self._archive_locks = {}
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

    def _check_input_recovery(self, db, row):
        # Prototype: never infer ownership or delete bytes. The authority lease
        # must fence all other writers; this local scan alone is not a fence.
        if self.input_free_floor is None:
            return
        if self.intake_journal:
            schema=db.execute("SELECT sql FROM sqlite_master WHERE name='viewer_intake_journal_v1' AND type='table'").fetchone()
            index=db.execute("SELECT sql FROM sqlite_master WHERE name='viewer_intake_unfinished_v1' AND type='index'").fetchone()
            if schema is None or schema[0]!=INTAKE_JOURNAL_DDL or index is None or index[0]!=INTAKE_INDEX_DDL:
                raise InputRecoveryRequired('intake schema changed')
            if db.execute("SELECT 1 FROM viewer_intake_journal_v1 WHERE uuid=? AND generation=? AND state!='committed' LIMIT 1",(row['uuid'],row['generation'])).fetchone():
                raise InputRecoveryRequired('intake journal needs inspection')
            if (row['uuid'],row['generation']) in self._intake_reconciled:
                return
            if db.execute("SELECT COUNT(*) FROM (SELECT 1 FROM viewer_intake_journal_v1 WHERE uuid=? AND generation=? LIMIT ?)",
                          (row['uuid'],row['generation'],INTAKE_CENSUS_MAX_FILES+1)).fetchone()[0]>INTAKE_CENSUS_MAX_FILES:
                raise InputRecoveryRequired('intake census exceeds bound')
            if db.execute("""SELECT 1 FROM viewer_intake_journal_v1 j
                LEFT JOIN viewer_files f ON f.uuid=j.uuid AND f.name_key=j.name_key
                WHERE j.uuid=? AND j.generation=? AND j.state='committed'
                  AND (f.uuid IS NULL OR typeof(j.final)!='text' OR typeof(j.bytes)!='integer' OR typeof(j.sha256)!='text' OR typeof(f.path)!='text' OR typeof(f.bytes)!='integer' OR typeof(f.sha256)!='text' OR f.path IS NOT j.final OR f.bytes IS NOT j.bytes OR f.sha256 IS NOT j.sha256)
                LIMIT 1""",(row['uuid'],row['generation'])).fetchone():
                raise InputRecoveryRequired('committed intake metadata differs')
        directory = self._directory(row)
        registered = set()
        for file in db.execute('SELECT path FROM viewer_files WHERE uuid=? LIMIT ?',
                               (row['uuid'],INTAKE_CENSUS_MAX_FILES+1)):
            name=file['path']
            if (not isinstance(name,str) or not re.fullmatch(r'input-[0-9a-f]{32}',name)
                    or name in registered or len(registered)>=INTAKE_CENSUS_MAX_FILES):
                raise InputRecoveryRequired('input metadata needs inspection')
            registered.add(name)
        actual = set()
        with os.scandir(directory) as entries:
            for entry_count,entry in enumerate(entries,1):
                if entry_count>INTAKE_CENSUS_MAX_ENTRIES:
                    raise InputRecoveryRequired('input directory census exceeds bound')
                if entry.name.startswith('.incoming-'):
                    raise InputRecoveryRequired('interrupted input needs inspection')
                if entry.name.startswith('input-'):
                    if not entry.is_file(follow_symlinks=False):
                        raise InputRecoveryRequired('input path needs inspection')
                    actual.add(entry.name)
        if actual != registered:
            raise InputRecoveryRequired('input files need inspection')
        if self.intake_journal:
            self._intake_reconciled.add((row['uuid'],row['generation']))

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

    def initialize(self, identifier, name='', options=None, outputs=None, environment='production', capability=None, option_evidence=None, *, replay_only=False):
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
                (uuid,generation,directory,name,options,outputs,environment,option_evidence_required)
                VALUES(?,?,?,?,?,?,?,?)''', (identifier, generation, directory_name, name,
                                          options_text, outputs_text, environment, int(evidence_text is not None)))
            if evidence_text is not None:
                db.execute('INSERT INTO viewer_option_evidence(uuid,generation,definitions,sha256) VALUES(?,?,?,?)',
                    (identifier,generation,evidence_text,hashlib.sha256(evidence_text.encode()).hexdigest()))
            return {'uuid': identifier}

    @staticmethod
    def _filename(name):
        if (not isinstance(name, str) or not NAME.fullmatch(name)
                or Path(name).suffix.lower() not in INPUT_SUFFIXES
                or name.endswith((' ', '.')) or name in ('.', '..')):
            raise ValueError('safe supported input basename required')
        return name.casefold()

    def upload(self, identifier, filename, stream, length, environment=None, capability=None):
        key = self._filename(filename)
        if type(length) is not int or length <= 0:
            raise ValueError('invalid input length')
        if length > self.max_input_bytes:
            raise InputLimitExceeded('task input ceiling exceeded')
        with self._lock:
            with self.queue._db() as db:
                self._reservation(db,identifier,environment,capability)
                row = self._row(db, identifier)
                if row['stage'] != 'uploading' or row['job_id']:
                    raise Conflict('task is no longer accepting uploads')
                directory = self._directory(row)
                self._check_input_recovery(db, row)
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
            if self.intake_journal and existing is not None:
                # Never accept a replay solely from stale metadata. Without an
                # exclusive writer fence this validation still cannot establish
                # continuous file identity, so this remains disabled.
                path=existing['path']
                if not isinstance(path,str) or not re.fullmatch(r'input-[0-9a-f]{32}',path):
                    raise InputRecoveryRequired('input path needs inspection')
                try:
                    fd=os.open(directory/path,os.O_RDONLY|getattr(os,'O_NOFOLLOW',0)|getattr(os,'O_NONBLOCK',0))
                    try:
                        before=os.fstat(fd)
                        if not stat.S_ISREG(before.st_mode) or before.st_size!=existing['bytes']:
                            raise InputRecoveryRequired('stored input needs inspection')
                        saved_hash=hashlib.sha256()
                        while True:
                            chunk=os.read(fd,1024*1024)
                            if not chunk:break
                            saved_hash.update(chunk)
                        after=os.fstat(fd); current=(directory/path).lstat()
                        if ((before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns,before.st_ctime_ns)!=
                            (after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns)
                            or (after.st_dev,after.st_ino)!=(current.st_dev,current.st_ino)
                            or saved_hash.hexdigest()!=existing['sha256']):
                            raise InputRecoveryRequired('stored input changed')
                    finally:os.close(fd)
                except OSError:
                    raise InputRecoveryRequired('stored input unavailable') from None
                if existing['bytes'] != length:
                    raise Conflict('input basename already contains different bytes')
                digest=hashlib.sha256(); remaining=length
                while remaining:
                    chunk=stream.read(min(1024*1024,remaining))
                    if not isinstance(chunk,bytes) or not chunk or len(chunk)>remaining:
                        raise ValueError('input stream length differs')
                    digest.update(chunk);remaining-=len(chunk)
                if stream.read(1):raise ValueError('input stream length differs')
                with self.queue._db() as db:
                    self._reservation(db,identifier,environment,capability)
                    current=self._row(db,identifier)
                    saved=db.execute('SELECT * FROM viewer_files WHERE uuid=? AND name_key=?',(identifier,key)).fetchone()
                    if current['stage']!='uploading' or current['job_id'] or current['generation']!=row['generation'] or saved is None or saved['bytes']!=length or saved['sha256']!=digest.hexdigest():
                        raise Conflict('input basename already contains different bytes')
                return {'success':True,'uuid':identifier}
            temporary = directory / ('.incoming-' + secrets.token_hex(16))
            final = directory / ('input-' + secrets.token_hex(16))
            journal_operation = None
            if self.intake_journal:
                self._intake_reconciled.discard((identifier,row['generation']))
                journal_operation = secrets.token_hex(16)
                with self.queue._db() as db:
                    db.execute('BEGIN IMMEDIATE')
                    current=self._row(db, identifier)
                    self._reservation(db,identifier,environment,capability)
                    if current['stage']!='uploading' or current['job_id'] or current['generation']!=row['generation'] or current['directory']!=row['directory']:
                        raise InputRecoveryRequired('task identity changed')
                    # Existing committed names use the established digest retry path.
                    previous=db.execute('SELECT 1 FROM viewer_files WHERE uuid=? AND name_key=?',(identifier,key)).fetchone()
                    if previous is None:
                        try:
                            db.execute("INSERT INTO viewer_intake_journal_v1 VALUES(?,?,?,?,?,?,?,?,NULL,'prepared')",(journal_operation,identifier,row['generation'],key,row['directory'],temporary.name,final.name,length))
                        except sqlite3.IntegrityError:
                            raise InputRecoveryRequired('input identity requires inspection') from None
                    else:
                        journal_operation=None
            try:
                descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                digest = hashlib.sha256()
                with os.fdopen(descriptor, 'wb') as output:
                    if self.input_free_floor is not None:
                        from .input_space import reserve_input
                        reserve_input(output.fileno(),length,self.input_free_floor)
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
                if journal_operation:
                    sync_directory(directory)
                    with self.queue._db() as db:
                        db.execute('BEGIN IMMEDIATE')
                        if db.execute("UPDATE viewer_intake_journal_v1 SET sha256=?,state='sealed' WHERE operation=? AND state='prepared'",(value,journal_operation)).rowcount!=1:
                            raise InputRecoveryRequired('input intent changed')
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
                    if journal_operation:
                        # Hard link refuses an existing final path. The two names
                        # identify the same bytes until the DB commit completes.
                        os.link(temporary,final,follow_symlinks=False)
                    else:
                        os.replace(temporary, final)
                    sync_directory(directory)
                    db.execute('''INSERT INTO viewer_files
                        (uuid,name_key,name,path,bytes,sha256,is_photo) VALUES(?,?,?,?,?,?,?)''',
                        (identifier, key, filename, final.name, length, value,
                         int(Path(filename).suffix.lower() in PHOTO_SUFFIXES)))
                    db.execute('UPDATE viewer_tasks SET updated_at=CURRENT_TIMESTAMP WHERE uuid=?', (identifier,))
                    if journal_operation and db.execute("UPDATE viewer_intake_journal_v1 SET state='cleanup' WHERE operation=? AND state='sealed' AND sha256=?",(journal_operation,value)).rowcount!=1:
                        raise InputRecoveryRequired('input intent changed')
                if journal_operation:
                    left=temporary.lstat();right=final.lstat()
                    if (left.st_dev,left.st_ino)!=(right.st_dev,right.st_ino):
                        raise InputRecoveryRequired('input publication changed')
                    temporary.unlink()
                    sync_directory(directory)
                    with self.queue._db() as db:
                        db.execute('BEGIN IMMEDIATE')
                        if db.execute("UPDATE viewer_intake_journal_v1 SET state='committed' WHERE operation=? AND state='cleanup' AND sha256=?",(journal_operation,value)).rowcount!=1:
                            raise InputRecoveryRequired('input cleanup intent changed')
                    self._intake_reconciled.add((identifier,row['generation']))
                return {'success': True, 'uuid': identifier}
            finally:
                # Only this invocation's randomly named, explicitly owned tmp file.
                if journal_operation is None and temporary.exists():
                    temporary.unlink()

    @staticmethod
    def _request_id(row):
        if row['generation'] == 1:
            return row['uuid']
        return str(uuid_module.uuid5(uuid_module.UUID(row['uuid']), 'generation:' + str(row['generation'])))

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
            self._check_input_recovery(db, row)
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

    def next_unarchived_success(self, after_rowid=0):
        """Round-robin cursor over durable, nonremoved successful tasks."""
        with self._lock, self.queue._db() as db:
            has_claims = bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='viewer_archive_claims_v1'").fetchone())
            claim_filter = (' AND NOT EXISTS (SELECT 1 FROM viewer_archive_claims_v1 c '
                            'WHERE c.uuid=t.uuid AND c.generation=t.generation)' if has_claims else '')
            for boundary in (after_rowid, 0):
                row = db.execute('''SELECT t.rowid,t.uuid FROM viewer_tasks t JOIN jobs j ON j.id=t.job_id
                    WHERE t.rowid>? AND t.removed=0 AND t.archive_path IS NULL AND t.archive_state='pending'
                      AND t.stage='succeeded' AND t.phase='complete' AND j.status='succeeded' ''' + claim_filter +
                    ' ORDER BY t.rowid LIMIT 1', (boundary,)).fetchone()
                if row is not None:
                    return (row[0], row[1])
                if boundary == 0:
                    break
            return None

    def task_for_job(self, job_id):
        """Load only the task selected for dispatch or active reconciliation."""
        with self._lock, self.queue._db() as db:
            rows=db.execute('SELECT * FROM viewer_tasks WHERE removed=0 AND job_id=? LIMIT 2',(job_id,)).fetchall()
            if len(rows)!=1:raise Conflict('execution metadata unavailable')
            return self._task(db,rows[0])

    def incomplete_terminal_job_ids(self):
        with self._lock, self.queue._db() as db:
            return {row[0] for row in db.execute("SELECT t.job_id FROM viewer_tasks t JOIN jobs j ON j.id=t.job_id WHERE t.removed=0 AND t.phase IN ('processing','commit_pending','cancel_pending') AND j.status IN ('succeeded','failed')")}

    def complete_engine_task(self, task, status, result):
        """Commit the queue terminal state and matching Viewer task together."""
        if status not in ('succeeded','failed'):
            raise ValueError('invalid engine completion')
        with self._lock, self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row=self._row(db,task['uuid'])
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

    _ARCHIVE_REGISTRY_DDL = ('CREATE TABLE viewer_archive_registry ('
        'uuid TEXT NOT NULL, generation INTEGER NOT NULL, job_id TEXT NOT NULL, '
        'node_id TEXT NOT NULL, engine_uuid TEXT NOT NULL, environment TEXT NOT NULL, '
        'PRIMARY KEY(uuid,generation))')

    def enable_archive_registry(self):
        with self._lock, self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='viewer_archive_registry'").fetchone()
            if row is None:
                db.execute(self._ARCHIVE_REGISTRY_DDL)
            elif row[0] != self._ARCHIVE_REGISTRY_DDL:
                raise Conflict('archive registry schema requires inspection')

    def archive_registry_check(self, task, *, register=False):
        """Check exact current identity; commit intent before archive writes."""
        fields=('uuid','generation','job_id','node_id','engine_uuid','environment')
        identity=tuple(task[k] for k in fields)
        if (any(value is None for value in identity) or type(identity[1]) is not int
                or identity[1] < 1):
            raise Conflict('archive registry identity requires inspection')
        with self._lock, self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row=self._row(db,task['uuid'])
            if tuple(row[k] for k in fields) != identity:
                raise Conflict('archive registry identity changed')
            job=self._job(db,row)
            expected={'engineTaskId':row['engine_uuid'],'nodeId':row['node_id'],
                      'environment':row['environment'],'cancelled':False}
            if (job is None or job['status']!='succeeded' or job['profile']!=VIEWER_PROFILE
                    or job['node_id']!=row['node_id'] or job['environment']!=row['environment']
                    or row['stage']!='succeeded' or row['phase']!='complete'
                    or job['result']!=canonical(expected) or not row['claim_token'] or not job['claim_hash']
                    or not hmac.compare_digest(job['claim_hash'],hashlib.sha256(row['claim_token'].encode()).hexdigest())):
                raise Conflict('archive registry job binding requires inspection')
            prior=db.execute('SELECT uuid,generation,job_id,node_id,engine_uuid,environment FROM viewer_archive_registry WHERE uuid=? AND generation=?',identity[:2]).fetchone()
            if prior is not None:
                if tuple(prior) != identity:raise Conflict('archive registry identity changed')
                return True
            if not register:
                return False
            if row['archive_path'] is not None:
                raise Conflict('historical archive format requires inspection')
            try:
                checked_uuid(identity[0]);checked_uuid(identity[2]);checked_uuid(identity[4])
                if (identity[3] not in self.queue.node_ids or identity[5] not in ('production','staging')
                        or identity[1] > 2**63-1):
                    raise ValueError()
            except (TypeError, ValueError):
                raise Conflict('archive registry identity requires inspection') from None
            if db.execute('SELECT COUNT(*) FROM viewer_archive_registry').fetchone()[0] >= ARCHIVE_REGISTRY_LIMIT:
                raise Conflict('archive registry capacity reached')
            db.execute('INSERT INTO viewer_archive_registry VALUES (?,?,?,?,?,?)',identity)
            return True

    @staticmethod
    def _has_exact_archive_status(db):
        rows = db.execute('PRAGMA table_info(viewer_tasks)').fetchall()
        columns = [tuple(row) for row in rows if row[1] in ('archive_state', 'archive_reason')]
        if not columns:
            return False
        if len(columns) != 2:
            raise ValueError('partial archive status schema')
        n = len(rows) - 2
        if columns != [(n, 'archive_state', 'TEXT', 1, "'pending'", 0),
                       (n + 1, 'archive_reason', 'TEXT', 0, None, 0)]:
            raise ValueError('unrecognized archive status schema')
        return True

    def enable_archive_status(self):
        # Both DDL statements and validation must commit as one transaction.
        with self._lock, self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            if not self._has_exact_archive_status(db):
                db.execute("ALTER TABLE viewer_tasks ADD COLUMN archive_state TEXT NOT NULL DEFAULT 'pending'")
                db.execute('ALTER TABLE viewer_tasks ADD COLUMN archive_reason TEXT')
                if not self._has_exact_archive_status(db):
                    raise ValueError('archive status schema not installed')

    def archive_lock(self, identifier):
        """Serialize this store's capture, download and status operations."""
        with self._archive_locks_lock:
            return self._archive_locks.setdefault(identifier, threading.RLock())

    _ARCHIVE_CLAIM_DDL = ('CREATE TABLE viewer_archive_claims_v1 ('
        'uuid TEXT NOT NULL, generation INTEGER NOT NULL, job_id TEXT NOT NULL, '
        'node_id TEXT NOT NULL, engine_uuid TEXT NOT NULL, environment TEXT NOT NULL, '
        'directory TEXT NOT NULL, archive_path TEXT, receipt_sha256 TEXT, operation TEXT NOT NULL '
        "CHECK(operation IN ('capture','restore')), "
        'token_hash TEXT NOT NULL, PRIMARY KEY(uuid,generation))')

    def install_archive_claim_schema(self):
        """Explicit offline migration; never invoked implicitly by service startup."""
        with self._lock, self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            if not self._has_exact_archive_status(db):
                raise Conflict('archive status schema requires explicit migration')
            existing = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='viewer_archive_claims_v1'").fetchone()
            if existing is None:
                db.execute(self._ARCHIVE_CLAIM_DDL)
            elif existing[0] != self._ARCHIVE_CLAIM_DDL:
                raise Conflict('archive claim schema requires inspection')

    def require_archive_claim_schema(self):
        with self.queue._db() as db:
            if not self._has_exact_archive_status(db):
                raise Conflict('archive status schema requires explicit migration')
            row = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='viewer_archive_claims_v1'").fetchone()
            if row is None or row[0] != self._ARCHIVE_CLAIM_DDL:
                raise Conflict('archive claim schema requires explicit migration')

    def archive_claim_held(self, task):
        with self._lock, self.queue._db() as db:
            return bool(db.execute('SELECT 1 FROM viewer_archive_claims_v1 WHERE uuid=? AND generation=?',
                                   (task['uuid'], task['generation'])).fetchone())

    def claim_archive(self, task, *, restore=False, receipt_sha256=None):
        if type(restore) is not bool:
            raise ValueError('explicit archive claim type required')
        if (restore and (type(receipt_sha256) is not str or not re.fullmatch('[a-f0-9]{64}', receipt_sha256))) or (not restore and receipt_sha256 is not None):
            raise ValueError('restore receipt digest required')
        token = uuid_module.uuid4().hex + uuid_module.uuid4().hex
        with self._lock, self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = self._row(db, task['uuid'])
            job = self._job(db, row)
            if (row['generation'] != task['generation'] or row['job_id'] != task['job_id']
                    or any(row[k] != task[k] for k in ('engine_uuid', 'node_id', 'environment'))
                    or self._directory(row) != Path(task['directory'])
                    or (row['archive_path'] != task['archive_path'] if restore else row['archive_path'] is not None)
                    or (restore and not task['archive_path']) or row['removed']
                    or ('archive_state' in row.keys() and row['archive_state'] != 'pending')
                    or job is None or job['status'] != 'succeeded'):
                raise Conflict('archive task changed')
            try:
                db.execute('INSERT INTO viewer_archive_claims_v1 VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                    (task['uuid'], task['generation'], task['job_id'], row['node_id'], row['engine_uuid'],
                     row['environment'], row['directory'], row['archive_path'], receipt_sha256, 'restore' if restore else 'capture',
                     hashlib.sha256(token.encode()).hexdigest()))
            except sqlite3.IntegrityError:
                raise ArchiveClaimHeld('archive requires inspection') from None
        return token

    @staticmethod
    def _archive_claim_matches(claim, row, token, operation):
        return (claim is not None and claim['operation'] == operation
                and all(claim[k] == row[k] for k in ('uuid', 'generation', 'job_id', 'node_id',
                                                       'engine_uuid', 'environment', 'directory', 'archive_path'))
                and hmac.compare_digest(claim['token_hash'], hashlib.sha256(token.encode()).hexdigest()))

    def finish_archive_restore(self, task, token, stream, receipt_sha256):
        """Release a restore claim only after its verified output is published."""
        path = Path(task['archive_path'])
        with self._lock, self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = self._row(db, task['uuid'])
            if (row['removed'] or row['archive_state'] != 'pending'
                    or any(row[k] != task[k] for k in ('generation', 'job_id', 'engine_uuid', 'node_id', 'environment', 'archive_path'))
                    or self._directory(row) != Path(task['directory'])):
                raise Conflict('archive restore identity changed')
            job = self._job(db, row)
            claim = db.execute('SELECT * FROM viewer_archive_claims_v1 WHERE uuid=? AND generation=?',
                               (task['uuid'], row['generation'])).fetchone()
            if (job is None or job['status'] != 'succeeded'
                    or not self._archive_claim_matches(claim, row, token, 'restore')
                    or type(receipt_sha256) is not str or claim['receipt_sha256'] != receipt_sha256):
                raise Conflict('archive restore claim changed')
            opened, current = os.fstat(stream.fileno()), path.lstat()
            if (path.is_symlink() or not stat.S_ISREG(current.st_mode)
                    or (opened.st_dev, opened.st_ino, opened.st_size) != (current.st_dev, current.st_ino, current.st_size)):
                raise Conflict('archive restore output changed')
            db.execute('DELETE FROM viewer_archive_claims_v1 WHERE uuid=? AND generation=?',
                       (task['uuid'], row['generation']))

    def set_archive_status(self, identifier, generation, job_id, state, reason):
        if state not in ('pending', 'inspection') or reason not in ('capture_failed', 'partial_requires_inspection'):
            raise ValueError('invalid archive status')
        with self.archive_lock(identifier), self._lock, self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            return db.execute("UPDATE viewer_tasks SET archive_state=?,archive_reason=? WHERE uuid=? AND generation=? AND job_id=? AND removed=0 AND archive_path IS NULL AND archive_state='pending'",
                              (state, reason, identifier, generation, job_id)).rowcount == 1

    def update(self, identifier, *, expected_archive_task=None, expected_archive_claim=None, **fields):
        allowed = {'stage', 'phase', 'node_id', 'engine_uuid', 'claim_token', 'error',
                   'progress', 'output_lines', 'archive_path', 'archive_state', 'archive_reason'}
        if not fields or not set(fields) <= allowed:
            raise ValueError('unexpected broker update fields')
        if 'archive_state' in fields and fields['archive_state'] not in ('pending', 'inspection'):
            raise ValueError('invalid archive state')
        if 'archive_reason' in fields and fields['archive_reason'] not in (None, 'capture_failed', 'partial_requires_inspection'):
            raise ValueError('invalid archive reason')
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
            if ('archive_state' in row.keys() and row['archive_state'] == 'inspection'
                    and ('archive_state' in fields or 'archive_reason' in fields)):
                raise Conflict('archive requires inspection')
            claim_schema = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='viewer_archive_claims_v1'").fetchone()
            if claim_schema and 'archive_path' in fields and (expected_archive_task is None or expected_archive_claim is None):
                raise Conflict('archive claim required')
            if expected_archive_task is not None:
                if claim_schema and expected_archive_claim is None:
                    raise Conflict('archive claim required')
                task=expected_archive_task
                if (set(fields)!={'archive_path'} or task['uuid']!=identifier or
                        any(row[key]!=task[key] for key in ('generation','job_id','engine_uuid','node_id','environment'))
                        or self._directory(row)!=Path(task['directory'])):
                    raise Conflict('archive task identity changed')
                job=self._job(db,row)
                if not job or job['status']!='succeeded':
                    raise Conflict('archive task no longer complete')
                if row['archive_path'] not in (None,str(Path(fields['archive_path']).absolute())):
                    raise Conflict('competing archive publication')
                if expected_archive_claim is not None:
                    if row['removed'] or ('archive_state' in row.keys() and row['archive_state'] != 'pending'):
                        raise Conflict('archive task requires inspection')
                    claim = db.execute('SELECT * FROM viewer_archive_claims_v1 WHERE uuid=? AND generation=?',
                                       (identifier, row['generation'])).fetchone()
                    if not self._archive_claim_matches(claim, row, expected_archive_claim, 'capture'):
                        raise Conflict('archive claim changed')
                    db.execute('DELETE FROM viewer_archive_claims_v1 WHERE uuid=? AND generation=?', (identifier, row['generation']))
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
                code=10 if job['status']=='queued' else 20
                return {'uuid':identifier,'name':row['name'],'status':{'code':code},
                        'progress':row['progress'],'imagesCount':count,'dateCreated':row['created_at'],
                        'ledgetopHeld':True,'ledgetopHoldReason':'option_evidence_invalid'}
        job = task['job']
        state = job['status'] if job else task['stage']
        cancelled_result = (job and state == 'failed' and isinstance(job['result'], dict)
                            and job['result'].get('cancelled') is True)
        if state == 'held':
            # A hold is nonterminal and retains the shared slot. NodeODM has no
            # held status; code 20 keeps existing clients polling rather than
            # misreporting a failure and potentially starting a new attempt.
            code = 20
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
        if (state == 'queued' and task['phase'] == 'waiting_node'
                and task['error'] in ('option_catalog_unavailable', 'option_definition_changed')):
            # Only our fixed reason codes are exposed; never engine error text.
            info['ledgetopWaitReason'] = task['error']
        if state == 'held' and code == 20:
            # Fixed marker only: held notes and engine error text can be private.
            info['ledgetopHeld'] = True
            if job.get('heldReason') == 'transfer_uncertain':
                info['ledgetopHoldReason'] = 'transfer_uncertain'
            reason='engine_failure_termination_unverified'
            hold=job.get('result') if isinstance(job,dict) else None
            durable_failure_hold=(isinstance(hold,dict)
                and set(hold)=={'environment','operatorAction','note','workerStopped'}
                and hold['environment']==task['environment'] and hold['operatorAction']=='hold'
                and hold['note']==reason and hold['workerStopped'] is False)
            if task['error']==reason or durable_failure_hold:
                info['ledgetopHoldReason']=reason
        return info

    def output(self, identifier, line=0):
        if type(line) is not int or not 0 <= line <= 2 ** 53 - 1:
            raise ValueError('nonnegative output cursor required')
        return self.task(identifier)['output_lines'][line:]

    def cancel(self, identifier):
        with self._lock, self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            self._reservation(db,identifier,None,None)
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

    def cancel_never_started(self, identifier):
        """Cancel or acknowledge an unambiguously unclaimed job atomically."""
        with self._lock, self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            self._reservation(db,identifier,None,None)
            row=self._row(db,identifier)
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

    def remove(self, identifier):
        with self._lock, self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            self._reservation(db,identifier,None,None)
            row = self._row(db, identifier, removed=True)
            job = self._job(db, row)
            if job and job['status'] in ('running', 'held'):
                raise Conflict('active engine task cannot be removed')
            if job and job['status'] == 'queued':
                db.execute("UPDATE jobs SET status='failed',result=?,updated_at=CURRENT_TIMESTAMP WHERE id=?",
                           (canonical({'cancelled': True, 'removed': True}), job['id']))
            db.execute('UPDATE viewer_tasks SET removed=1,updated_at=CURRENT_TIMESTAMP WHERE uuid=?', (identifier,))
            return {'success': True, 'uuid': identifier}
