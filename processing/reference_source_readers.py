"""Coordinator-local Linux lease ownership for NAS source I/O operations.

Read descriptors and synchronous write/audit operations share this registry.
This is a deletion prerequisite, not deletion authority. Fences are permanent,
per job/generation and must only be installed by a future authenticated release
transaction. No timeout retires a reader. Only acquisition of the exact private
lease after its prior owner exits permits retirement of registered old pins.
"""
from contextlib import contextmanager
import json
import hashlib
import os
from pathlib import Path
import secrets
import stat
import sys
import threading

from .maintenance import Conflict
from . import reference_protocol as protocol


class SourceReaderError(Conflict):
    pass


_OWNERS = {}
_OWNERS_LOCK = threading.RLock()
_PROCESS_PID = os.getpid()


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


def _identity(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mode, info.st_nlink,
            info.st_uid, info.st_mtime_ns, info.st_ctime_ns)


def metadata_bytes(db):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE name='source_reader_binding'").fetchone():
        return 0
    used = db.execute('SELECT COALESCE(SUM(length(CAST(binding AS BLOB))+length(epoch)),0) FROM source_reader_binding').fetchone()[0]
    # Reserve each row's longest state so releasing/retiring a reader never
    # needs additional quota merely to represent its terminal state.
    used += db.execute('SELECT COALESCE(SUM(length(epoch)+10),0) FROM source_reader_epochs').fetchone()[0]
    columns = {row[1] for row in db.execute('PRAGMA table_info(source_reader_pins)')}
    operation_bytes = 'length(operation)' if 'operation' in columns else '4'
    used += db.execute('SELECT COALESCE(SUM(length(reference)+length(epoch)+length(job_id)+length(CAST(generation AS TEXT))+length(object_id)+10+' + operation_bytes + '),0) FROM source_reader_pins').fetchone()[0]
    used += db.execute('SELECT COALESCE(SUM(length(job_id)+length(CAST(generation AS TEXT))+length(release_id)),0) FROM source_reader_fences').fetchone()[0]
    return used


class SourceReaders:
    def __init__(self, store):
        self.store, self.queue = store, store.queue
        self.path = Path(self.queue.path).absolute()
        self.root = self.path.parent / 'source-readers'
        self.lock_path = self.root / 'lease'
        self._mutex = threading.RLock()
        self._fd = self._pid = self.epoch = self.binding = None
        self._handles = {}
        self._operations = threading.local()
        self._closed = False

    def _process(self):
        # Reject inherited objects before touching any inherited Python mutex.
        if (_PROCESS_PID != os.getpid()
                or getattr(self.queue, '_creator_pid', os.getpid()) != os.getpid()
                or (self._pid is not None and self._pid != os.getpid())):
            raise SourceReaderError('source_reader_owner_process_required')

    def _paths(self):
        for path in (self.path, self.root, self.lock_path):
            if path.resolve() != path or any(part.is_symlink() for part in (path, *path.parents)):
                raise SourceReaderError('source_reader_owned_paths_required')
        root, database = self.root.stat(), self.path.stat()
        if (not stat.S_ISDIR(root.st_mode) or root.st_uid != os.geteuid() or root.st_mode & 0o077
                or not stat.S_ISREG(database.st_mode) or database.st_nlink != 1
                or database.st_uid != os.geteuid()):
            raise SourceReaderError('source_reader_private_state_required')

    def start(self):
        self._process()
        if not sys.platform.startswith('linux'):
            raise SourceReaderError('linux_source_readers_required')
        import fcntl
        with self._mutex, _OWNERS_LOCK:
            if self._fd is not None or self._closed:
                raise SourceReaderError('source_reader_session_already_used')
            if str(self.lock_path) in _OWNERS:
                raise SourceReaderError('source_reader_session_busy')
            root_created = not self.root.exists()
            self.root.mkdir(mode=0o700, exist_ok=True)
            if root_created:
                parent = os.open(self.path.parent, os.O_DIRECTORY | os.O_RDONLY | os.O_NOFOLLOW)
                try:
                    os.fsync(parent)
                finally:
                    os.close(parent)
            self._paths()
            try:
                created = False
                try:
                    self._fd = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
                    created = True
                except FileExistsError:
                    self._fd = os.open(self.lock_path, os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
                info = os.fstat(self._fd)
                if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                        or info.st_uid != os.geteuid() or info.st_mode & 0o077):
                    raise SourceReaderError('source_reader_private_lease_required')
                os.set_inheritable(self._fd, False)
                try:
                    fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError:
                    raise SourceReaderError('source_reader_session_busy') from None
                if created:
                    os.write(self._fd, secrets.token_hex(32).encode('ascii'))
                    os.fsync(self._fd)
                    parent = os.open(self.root, os.O_DIRECTORY | os.O_RDONLY | os.O_NOFOLLOW)
                    try:
                        os.fsync(parent)
                    finally:
                        os.close(parent)
                marker = os.pread(self._fd, 65, 0)
                if len(marker) != 64 or any(byte not in b'0123456789abcdef' for byte in marker):
                    raise SourceReaderError('source_reader_lease_marker_invalid')
                self._pid = os.getpid()
                self.binding = dict(authorityId=self.queue.authority_id, database=str(self.path),
                    databaseIdentity=list(_identity(self.path.stat())[:2]), root=str(self.root),
                    rootIdentity=list(_identity(self.root.stat())[:2]),
                    leaseIdentity=list(_identity(os.fstat(self._fd))), marker=marker.hex())
                self.epoch = secrets.token_hex(32)
                with self.queue._db() as db:
                    db.execute('BEGIN IMMEDIATE')
                    db.execute('CREATE TABLE IF NOT EXISTS source_reader_binding(id INTEGER PRIMARY KEY CHECK(id=1),binding TEXT NOT NULL,epoch TEXT NOT NULL)')
                    db.execute('CREATE TABLE IF NOT EXISTS source_reader_epochs(epoch TEXT PRIMARY KEY,state TEXT NOT NULL)')
                    db.execute('CREATE TABLE IF NOT EXISTS source_reader_pins(reference TEXT PRIMARY KEY,epoch TEXT NOT NULL,job_id TEXT NOT NULL,generation INTEGER NOT NULL,object_id TEXT NOT NULL,state TEXT NOT NULL,operation TEXT NOT NULL DEFAULT "read")')
                    db.execute('CREATE TABLE IF NOT EXISTS source_reader_fences(job_id TEXT NOT NULL,generation INTEGER NOT NULL,release_id TEXT NOT NULL,PRIMARY KEY(job_id,generation))')
                    columns = {row[1] for row in db.execute('PRAGMA table_info(source_reader_pins)')}
                    if 'operation' not in columns:
                        self.store._quota(db, 0)
                        db.execute("ALTER TABLE source_reader_pins ADD COLUMN operation TEXT NOT NULL DEFAULT 'read'")
                    db.execute('CREATE INDEX IF NOT EXISTS source_reader_job_activity ON source_reader_pins(job_id,generation,state,operation)')
                    old = db.execute('SELECT * FROM source_reader_binding WHERE id=1').fetchone()
                    if old and old['binding'] != _json(self.binding):
                        raise SourceReaderError('source_reader_lease_binding_changed')
                    if not old and (db.execute('SELECT 1 FROM source_reader_epochs LIMIT 1').fetchone()
                            or db.execute('SELECT 1 FROM source_reader_pins LIMIT 1').fetchone()):
                        raise SourceReaderError('source_reader_unbound_ownership')
                    # Flock acquisition proves the old owner no longer holds the
                    # lease. Unknown rows without a registered epoch stay pinned.
                    self.store._quota(db, len(self.epoch) + 10 + (len(_json(self.binding)) + len(self.epoch) if not old else 0))
                    db.execute("UPDATE source_reader_pins SET state='superseded' WHERE state IN ('active','uncertain') AND epoch IN (SELECT epoch FROM source_reader_epochs)")
                    db.execute("UPDATE source_reader_epochs SET state='superseded'")
                    db.execute("INSERT INTO source_reader_epochs VALUES(?,'active')", (self.epoch,))
                    db.execute('INSERT INTO source_reader_binding VALUES(1,?,?) ON CONFLICT(id) DO UPDATE SET epoch=excluded.epoch', (_json(self.binding), self.epoch))
                _OWNERS[str(self.lock_path)] = self
                return self
            except Exception:
                if self._fd is not None:
                    os.close(self._fd)
                self._fd = self._pid = None
                raise

    def check(self):
        self._process()
        if not sys.platform.startswith('linux'):
            raise SourceReaderError('linux_source_readers_required')
        if self._pid != os.getpid() or self._fd is None or self._closed:
            raise SourceReaderError('source_reader_owner_process_required')
        self._paths()
        try:
            valid = (list(_identity(os.fstat(self._fd))) == self.binding['leaseIdentity']
                and _identity(os.fstat(self._fd)) == _identity(self.lock_path.stat())
                and os.pread(self._fd, 65, 0).hex() == self.binding['marker']
                and list(_identity(self.path.stat())[:2]) == self.binding['databaseIdentity']
                and list(_identity(self.root.stat())[:2]) == self.binding['rootIdentity']
                and self.queue.authority_id == self.binding['authorityId']
                and not os.get_inheritable(self._fd))
        except OSError:
            valid = False
        if not valid:
            raise SourceReaderError('source_reader_lease_identity_changed')

    @contextmanager
    def _db(self):
        self.check()
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT binding,epoch FROM source_reader_binding WHERE id=1').fetchone()
            epoch = db.execute('SELECT state FROM source_reader_epochs WHERE epoch=?', (self.epoch,)).fetchone()
            if (not row or row['binding'] != _json(self.binding) or row['epoch'] != self.epoch
                    or not epoch or epoch['state'] != 'active'):
                raise SourceReaderError('source_reader_epoch_changed')
            yield db
            self.check()

    @staticmethod
    def _unfenced(db, job_id, generation):
        if db.execute('SELECT 1 FROM source_reader_fences WHERE job_id=? AND generation=?', (job_id, generation)).fetchone():
            raise SourceReaderError('source_release_fenced')

    def _admit_operation(self, db, job_id, generation, operation):
        owned = tuple(getattr(self._operations, 'handles', ()))
        references = {handle.reference for handle in owned
            if handle.session is self and not handle.closed and not handle.uncertain
            and self._handles.get(handle.reference) is handle}
        for pin in db.execute("SELECT reference,operation FROM source_reader_pins WHERE job_id=? AND generation=? AND state IN ('active','uncertain')", (job_id, generation)):
            if (pin['reference'] in references and operation != 'read'
                    and (pin['operation'] == operation or operation == 'source-receipt')):
                continue
            if operation == 'source-audit' or pin['operation'] == 'source-audit':
                raise SourceReaderError('source_operation_busy')

    @contextmanager
    def operation_scope(self, handle):
        """Only actual synchronous same-thread nesting may reuse audit custody."""
        self._process()
        with self._mutex:
            if (handle.session is not self or handle.closed or handle.uncertain
                    or handle.operation == 'read' or handle.thread_id != threading.get_ident()
                    or self._handles.get(handle.reference) is not handle):
                raise SourceReaderError('source_operation_scope_invalid')
            previous = tuple(getattr(self._operations, 'handles', ()))
            self._operations.handles = previous + (handle,)
        try:
            yield
        finally:
            self._operations.handles = previous

    def reserve(self, job_id, generation, object_id, *, authorize, quota=None, operation='read', _release_id=None):
        with self.queue.coordinator_operation('source-io-admission'):
            return self._reserve(job_id,generation,object_id,authorize=authorize,quota=quota,
                operation=operation,_release_id=_release_id)

    def _reserve(self, job_id, generation, object_id, *, authorize, quota=None, operation='read', _release_id=None):
        self._process()
        for value in (job_id, object_id):
            protocol._slug(value, 'source reader identity')
        protocol._integer(generation, 'source reader generation')
        if operation not in ('read', 'source-put', 'source-receipt', 'source-complete', 'source-audit', 'source-release'):
            raise ValueError('invalid source operation')
        with self.queue._lifecycle_lock, self._mutex:
            self.check()
            reference = (hashlib.sha256(('source-release-v1:'+self.epoch+':'+job_id+':'+str(generation)+':'+str(_release_id)).encode('ascii')).hexdigest()[:48]
                if operation=='source-release' else secrets.token_hex(24))
            if reference in self._handles:
                raise SourceReaderError('source_operation_busy')
            handle = SourceReadHandle(self, reference, job_id, generation, object_id, operation)
            self._handles[reference] = handle
            wrote = False
            try:
                with self._db() as db:
                    if operation == 'source-release':
                        fence=db.execute('SELECT release_id FROM source_reader_fences WHERE job_id=? AND generation=?',(job_id,generation)).fetchone()
                        if not fence or fence['release_id'] != _release_id:
                            raise SourceReaderError('source_release_fence_required')
                        if db.execute("SELECT 1 FROM source_reader_pins WHERE job_id=? AND generation=? AND state IN ('active','uncertain') LIMIT 1",(job_id,generation)).fetchone():
                            raise SourceReaderError('source_operation_busy')
                    else:
                        if _release_id is not None:
                            raise SourceReaderError('source_release_operation_required')
                        self._unfenced(db, job_id, generation)
                        self._admit_operation(db, job_id, generation, operation)
                    authorize(db)
                    old=db.execute('SELECT * FROM source_reader_pins WHERE reference=?',(reference,)).fetchone()
                    if old is not None:
                        if (operation!='source-release' or (old['epoch'],old['job_id'],old['generation'],old['object_id'],old['operation'],old['state'])!=
                                (self.epoch,job_id,generation,object_id,operation,'released')):
                            raise SourceReaderError('source_reader_pin_changed')
                        # Exact same-epoch synchronous release pin reactivation
                        # needs no new metadata; its longest state is prepaid.
                        wrote=True
                        changed=db.execute("UPDATE source_reader_pins SET state='active' WHERE reference=? AND epoch=? AND state='released'",(reference,self.epoch))
                        if changed.rowcount!=1:raise SourceReaderError('source_reader_pin_changed')
                    else:
                        (quota or self.store._quota)(db, sum(len(str(x)) for x in (reference, self.epoch, job_id, generation, object_id, 'superseded', operation)))
                        wrote = True
                        db.execute("INSERT INTO source_reader_pins(reference,epoch,job_id,generation,object_id,state,operation) VALUES(?,?,?,?,?,'active',?)", (reference, self.epoch, job_id, generation, object_id, operation))
                return handle
            except Exception:
                # A failed commit could have published a pin. Retain ownership
                # until exact reconciliation instead of silently unlocking it.
                if wrote:
                    handle.uncertain = True
                else:
                    self._handles.pop(reference, None)
                raise

    def fence(self, job_id, generation, release_id):
        """Internal primitive only; no API exposes release/deletion authority."""
        self._process()
        protocol._slug(job_id, 'source reader job')
        protocol._integer(generation, 'source reader generation')
        protocol._slug(release_id, 'source release identity')
        with self.queue._lifecycle_lock, self._mutex, self._db() as db:
            return self.fence_in_transaction(db, job_id, generation, release_id)

    def fence_in_transaction(self, db, job_id, generation, release_id):
        """Caller owns the queue transaction and verifies its release authority."""
        self.check()
        job = db.execute('SELECT generation FROM reference_jobs WHERE job_id=?', (job_id,)).fetchone()
        if not job or job['generation'] != generation:
            raise SourceReaderError('source_reader_job_changed')
        old = db.execute('SELECT release_id FROM source_reader_fences WHERE job_id=? AND generation=?', (job_id, generation)).fetchone()
        if old:
            if old['release_id'] != release_id:
                raise SourceReaderError('source_release_fence_conflict')
            return
        self.store._quota(db, len(job_id) + len(str(generation)) + len(release_id))
        db.execute('INSERT INTO source_reader_fences VALUES(?,?,?)', (job_id, generation, release_id))

    def reserve_release(self, job_id, generation, release_id, *, authorize):
        """Internal only: exact journal/fence authorizer, after all I/O drains."""
        return self.reserve(job_id,generation,release_id,authorize=authorize,
            operation='source-release',_release_id=release_id)

    def active_count(self, job_id, generation):
        self._process()
        with self.queue._lifecycle_lock, self._mutex, self._db() as db:
            return db.execute("SELECT count(*) FROM source_reader_pins WHERE job_id=? AND generation=? AND state IN ('active','uncertain')", (job_id, generation)).fetchone()[0]

    def close(self):
        self._process()
        with self.queue._lifecycle_lock, self._mutex, _OWNERS_LOCK:
            if self._closed:
                return
            self.check()
            if self._handles:
                raise SourceReaderError('source_readers_still_live_or_uncertain')
            with self.queue._coordinator_freeze.source_readers_shutdown(self):
                with self._db() as db:
                    if db.execute("SELECT 1 FROM source_reader_pins WHERE epoch=? AND state IN ('active','uncertain') LIMIT 1", (self.epoch,)).fetchone():
                        raise SourceReaderError('source_readers_still_live_or_uncertain')
                    db.execute("UPDATE source_reader_epochs SET state='closed' WHERE epoch=?", (self.epoch,))
            os.close(self._fd)
            self._fd = None
            self._closed = True
            _OWNERS.pop(str(self.lock_path), None)


class SourceReadHandle:
    def __init__(self, session, reference, job_id, generation, object_id, operation='read'):
        self.session, self.reference = session, reference
        self.job_id, self.generation, self.object_id = job_id, generation, object_id
        self.operation = operation
        self.thread_id = threading.get_ident()
        self._reader = self.file_identity = None
        self.uncertain = self.closed = self._known_closed = False

    def check_authority(self, *, authorize):
        """Recheck a synchronous operation before effects/acknowledgement.

        A descriptorless pin is valid only while its owning synchronous call
        remains on the stack. No background task or descriptor may escape it.
        """
        self.session._process()
        with self.session.queue._lifecycle_lock, self.session._mutex:
            if self.closed or self.uncertain:
                raise SourceReaderError('source_reader_not_active')
            with self.session._db() as db:
                pin = db.execute('SELECT epoch,state FROM source_reader_pins WHERE reference=?', (self.reference,)).fetchone()
                if not pin or (pin['epoch'], pin['state']) != (self.session.epoch, 'active'):
                    raise SourceReaderError('source_reader_pin_changed')
                self.session._unfenced(db, self.job_id, self.generation)
                self.session._admit_operation(db, self.job_id, self.generation, self.operation)
                authorize(db)

    def attach(self, reader, *, authorize):
        self.session._process()
        with self.session.queue._lifecycle_lock, self.session._mutex:
            if self._reader is not None or self.closed or self.uncertain:
                self.uncertain = True
                raise SourceReaderError('source_reader_already_attached')
            self._reader = reader
            try:
                self.file_identity = _identity(os.fstat(reader.fileno()))
                with self.session._db() as db:
                    pin = db.execute('SELECT epoch,state FROM source_reader_pins WHERE reference=?', (self.reference,)).fetchone()
                    if not pin or (pin['epoch'], pin['state']) != (self.session.epoch, 'active'):
                        raise SourceReaderError('source_reader_pin_changed')
                    self.session._unfenced(db, self.job_id, self.generation)
                    authorize(db)
                return self
            except Exception:
                self.close()
                raise

    def read(self, size=-1):
        self.session.check()
        if self.closed or self.uncertain or self._reader is None:
            raise SourceReaderError('source_reader_not_active')
        return self._reader.read(size)

    def verify_identity(self, expected):
        self.session.check()
        if self._reader is None or self.closed or _identity(os.fstat(self._reader.fileno())) != expected:
            raise SourceReaderError('source_reader_object_changed')

    def close(self):
        self.session._process()
        with self.session.queue._lifecycle_lock, self.session._mutex:
            if self.closed:
                return
            self.session.check()
            if self.uncertain:
                raise SourceReaderError('source_reader_close_uncertain')
            try:
                if self._reader is not None and not self._known_closed:
                    if self.file_identity is None or _identity(os.fstat(self._reader.fileno())) != self.file_identity:
                        raise SourceReaderError('source_reader_descriptor_changed')
                    self._reader.close()
                    self._known_closed = True
                with self.session.queue._coordinator_freeze.source_pin_retire(self):
                    with self.session._db() as db:
                        row = db.execute('SELECT epoch,state FROM source_reader_pins WHERE reference=?', (self.reference,)).fetchone()
                        if not row or row['epoch'] != self.session.epoch or row['state'] not in ('active', 'released'):
                            raise SourceReaderError('source_reader_pin_changed')
                        db.execute("UPDATE source_reader_pins SET state='released' WHERE reference=?", (self.reference,))
                self.closed = True
                self.session._handles.pop(self.reference, None)
            except Exception:
                self.uncertain = True
                raise
