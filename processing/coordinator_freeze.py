"""Single-authority maintenance admission and drain journal.

An operation admitted before intent may finish its synchronous nested work.
Intent closes new work; sealing additionally requires all operation and source
reader pins gone. No lease timeout, claim replay, job cancellation or NAS access
is performed here. A failed operation-pin retirement remains a durable hold.
"""
from contextlib import contextmanager
import hashlib
import json
import os
import sqlite3
import threading
import uuid
import re
import stat
from pathlib import Path
from functools import wraps

from .maintenance import Conflict

OPERATION_KINDS = frozenset(('http', 'viewer-dispatch', 'viewer-transfer',
    'source-io-admission', 'source-io', 'source-release-submit', 'source-release'))
STARTUP_TABLES = frozenset(('coordinator_identity', 'jobs', 'nodes', 'owned_maintenance_schedules',
    'processing_node_inventory', 'reference_acceptances', 'reference_claims', 'reference_jobs',
    'reference_outbox', 'reference_recovery_receipts', 'reference_recovery_startup',
    'reference_source_grant_renewals', 'reference_source_grant_tokens', 'reference_source_intakes',
    'reference_source_object_pins', 'reference_source_object_receipts', 'reference_source_range_requests',
    'reference_source_range_usage', 'reference_terminal_acceptances', 'reference_transfer_grants',
    'reference_viewer_receipts', 'reference_viewer_transfers', 'reference_worker_acceptance_observations',
    'reference_worker_failures', 'reference_worker_receipts', 'source_reader_binding',
    'source_reader_epochs', 'source_reader_fences', 'source_reader_pins', 'sqlite_sequence',
    'viewer_files', 'viewer_incarnations', 'viewer_tasks', 'coordinator_freeze_state',
    'coordinator_freeze_receipts', 'coordinator_operation_pins', 'coordinator_operation_epochs'))


class CoordinatorFrozen(RuntimeError):
    """Retryable service maintenance; producers must retain their outboxes."""


def runtime_constructor(function):
    """Keep the startup authority scope around the real runtime constructor."""
    @wraps(function)
    def guarded(runtime, queue, *args, **kwargs):
        runtime.queue = queue
        with queue._coordinator_freeze.runtime_startup(runtime):
            return function(runtime, queue, *args, **kwargs)
    return guarded


@contextmanager
def request_scope(queue, method, path):
    """Hold admission through handler body/stream lifetime, before body reads."""
    pure = method in ('GET', 'HEAD') and (path in (
        '/healthz', '/v1/prototype/authority', '/v1/prototype/jobs', '/v1/prototype/nodes',
        '/v1/prototype/coordinator-freeze') or re.fullmatch(
            r'/ltds/v[12]/jobs/[A-Za-z0-9][A-Za-z0-9_-]{0,127}', path))
    if queue is None or pure or path == '/v1/prototype/coordinator-freeze':
        yield
    else:
        with queue.coordinator_operation('http'):
            yield


def _wire(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def metadata_bytes(db):
    names = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    used = 0
    for table in ('coordinator_freeze_state', 'coordinator_freeze_receipts', 'coordinator_operation_pins',
                  'coordinator_operation_epochs'):
        if table in names:
            for row in db.execute('SELECT * FROM ' + table):
                used += sum(len(str(value).encode('utf-8')) for value in row if value is not None)
    return used


class FreezeGate:
    def __init__(self, queue):
        self.queue = queue
        self._local = threading.local()
        self._active = {}
        self._pid = os.getpid()
        with queue._db() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS coordinator_freeze_state (
                  singleton INTEGER PRIMARY KEY CHECK(singleton=1), revision INTEGER NOT NULL,
                  plan_id TEXT, phase TEXT NOT NULL, owner_sha256 TEXT, owner_epoch TEXT);
                CREATE TABLE IF NOT EXISTS coordinator_freeze_receipts (
                  request_id TEXT PRIMARY KEY, plan_id TEXT NOT NULL, owner_sha256 TEXT NOT NULL,
                  request_sha256 TEXT NOT NULL, request_json TEXT NOT NULL, receipt_json TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS coordinator_operation_pins (
                  reference TEXT PRIMARY KEY, epoch TEXT NOT NULL, kind TEXT NOT NULL,
                  start_revision INTEGER NOT NULL, state TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS coordinator_operation_epochs (
                  epoch TEXT PRIMARY KEY, binding_json TEXT NOT NULL, binding_sha256 TEXT NOT NULL,
                  state TEXT NOT NULL, retired_by TEXT);
                INSERT OR IGNORE INTO coordinator_freeze_state VALUES(1,0,NULL,'open',NULL,NULL);
            ''')
            row = self._row(db)
            binding = self._binding()
            wire = _wire(binding)
            binding_sha = hashlib.sha256(wire.encode('ascii')).hexdigest()
            epochs = {item['epoch']: item for item in db.execute('SELECT * FROM coordinator_operation_epochs')}
            registered = set()
            uncertain = False
            for epoch, saved in epochs.items():
                try:
                    valid = (str(uuid.UUID(epoch)) == epoch and saved['binding_json'] == wire
                        and saved['binding_sha256'] == binding_sha and saved['state'] in ('active', 'closed', 'retired'))
                    if valid and saved['state'] == 'retired':
                        successor = saved['retired_by']
                        valid = (type(successor) is str and str(uuid.UUID(successor)) == successor
                                 and successor != epoch and successor in epochs)
                    elif valid:
                        valid = saved['retired_by'] is None
                except (ValueError, TypeError, AttributeError):
                    valid = False
                if valid:
                    registered.add(epoch)
                else:
                    uncertain = True
            for epoch in tuple(registered):
                seen = set()
                current = epoch
                while current in registered and epochs[current]['state'] == 'retired':
                    if current in seen:
                        break
                    seen.add(current)
                    current = epochs[current]['retired_by']
                if current not in registered or current in seen:
                    registered.difference_update(seen)
                    uncertain = True
            prior_pins = db.execute('SELECT * FROM coordinator_operation_pins').fetchall()
            retire_pins = []
            for pin in prior_pins:
                try:
                    reference_valid = str(uuid.UUID(pin['reference'])) == pin['reference']
                except (ValueError, TypeError, AttributeError):
                    reference_valid = False
                valid = (reference_valid and pin['epoch'] in registered and pin['kind'] in OPERATION_KINDS
                        and type(pin['start_revision']) is int and 0 <= pin['start_revision'] <= row['revision'])
                if valid and pin['state'] == 'retired':
                    valid = epochs[pin['epoch']]['state'] == 'retired'
                elif valid and pin['state'] == 'active':
                    valid = epochs[pin['epoch']]['state'] == 'active'
                else:
                    valid = False
                if not valid:
                    uncertain = True
                elif pin['state'] == 'active':
                    retire_pins.append(pin)
            charge = sum(len(value) for value in (queue.startup_id, wire, binding_sha, 'active'))
            charge += len(retire_pins) + 37 * sum(epochs[epoch]['state'] == 'active' for epoch in registered)
            if metadata_bytes(db) + charge + 512 > 256 * 1024:
                uncertain = True
            else:
                db.execute('INSERT INTO coordinator_operation_epochs VALUES(?,?,?,?,NULL)',
                           (queue.startup_id, wire, binding_sha, 'active'))
                # Queue acquired this exact original private main lease before
                # constructing the gate. A cloned/restored DB has a different
                # inode/root/lease binding and cannot retire original pins.
                for pin in retire_pins:
                    db.execute("UPDATE coordinator_operation_pins SET state='retired' WHERE reference=? AND epoch=? AND state='active'",
                               (pin['reference'], pin['epoch']))
                for epoch in registered:
                    if not db.execute("SELECT 1 FROM coordinator_operation_pins WHERE epoch=? AND state IN ('active','uncertain') LIMIT 1", (epoch,)).fetchone():
                        db.execute("UPDATE coordinator_operation_epochs SET state='retired',retired_by=? WHERE epoch=? AND state='active'",
                                   (queue.startup_id, epoch))
            if ((row['phase'] in ('intent', 'draining') and row['owner_epoch'] != queue.startup_id)
                    or uncertain):
                db.execute("UPDATE coordinator_freeze_state SET phase='held',revision=revision+1 WHERE singleton=1")

    def _binding(self):
        queue = self.queue
        if queue._creator_pid != os.getpid() or queue._lock.closed:
            raise CoordinatorFrozen('original_coordinator_lease_required')
        database = Path(queue.path).absolute()
        root = database.parent
        lease = root / 'coordinator.lock'
        for path in (database, root, lease):
            if path.resolve() != path or any(parent.is_symlink() for parent in path.parents):
                raise CoordinatorFrozen('original_coordinator_paths_required')
        db_info, root_info, lock_info = database.stat(), root.stat(), lease.stat()
        opened = os.fstat(queue._lock.fileno())
        if (not stat.S_ISDIR(root_info.st_mode) or not stat.S_ISREG(db_info.st_mode)
                or not stat.S_ISREG(lock_info.st_mode) or db_info.st_nlink != 1 or lock_info.st_nlink != 1
                or (opened.st_dev, opened.st_ino) != (lock_info.st_dev, lock_info.st_ino)):
            raise CoordinatorFrozen('original_coordinator_identity_required')
        if os.name == 'posix' and (root_info.st_uid != os.geteuid() or db_info.st_uid != os.geteuid()
                or lock_info.st_uid != os.geteuid() or root_info.st_mode & 0o077 or lock_info.st_mode & 0o022):
            raise CoordinatorFrozen('private_original_coordinator_lease_required')
        return dict(authorityId=queue.authority_id, database=str(database),
            databaseIdentity=[db_info.st_dev, db_info.st_ino], root=str(root),
            rootIdentity=[root_info.st_dev, root_info.st_ino], lease=str(lease),
            leaseIdentity=[lock_info.st_dev, lock_info.st_ino, lock_info.st_mode, lock_info.st_nlink, lock_info.st_uid])

    def _check(self):
        if os.getpid() != self._pid or self.queue._lock.closed:
            raise CoordinatorFrozen('coordinator_owner_unavailable')

    def _row(self, db):
        row = db.execute('SELECT * FROM coordinator_freeze_state WHERE singleton=1').fetchone()
        if row is None or row['phase'] not in ('open', 'intent', 'draining', 'frozen', 'held'):
            raise CoordinatorFrozen('coordinator_freeze_journal_unavailable')
        return row

    def status(self):
        self._check()
        with self.queue._db() as db:
            row = self._row(db)
            active = db.execute("SELECT count(*) FROM coordinator_operation_pins WHERE state IN ('active','uncertain')").fetchone()[0]
            return dict(authorityId=self.queue.authority_id, startupId=self.queue.startup_id,
                        revision=row['revision'], planId=row['plan_id'], phase=row['phase'], operations=active)

    def authorize_connection(self, db):
        """Called under Queue's transaction/lifecycle mutex before yielding DB."""
        self._check()
        row = self._row(db)
        if getattr(self._local, 'control', False):
            return
        token = getattr(self._local, 'operation', None)
        if token is not None and self._active.get(token) == threading.get_ident():
            saved = db.execute('SELECT * FROM coordinator_operation_pins WHERE reference=?', (token,)).fetchone()
            if (saved is None or saved['epoch'] != self.queue.startup_id or saved['state'] != 'active'
                    or saved['start_revision'] > row['revision']):
                raise CoordinatorFrozen('coordinator_operation_ownership_uncertain')
            if row['phase'] in ('open', 'intent', 'draining'):
                return
        if row['phase'] == 'open':
            return
        # Pure SELECTs and transaction bookkeeping still work. SQLite enforces
        # this on every coordinator DB connection, including background writers.
        denied = {sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE,
                  sqlite3.SQLITE_CREATE_INDEX, sqlite3.SQLITE_CREATE_TABLE,
                  sqlite3.SQLITE_CREATE_TRIGGER, sqlite3.SQLITE_CREATE_VIEW,
                  sqlite3.SQLITE_DROP_INDEX, sqlite3.SQLITE_DROP_TABLE,
                  sqlite3.SQLITE_DROP_TRIGGER, sqlite3.SQLITE_DROP_VIEW,
                  sqlite3.SQLITE_ALTER_TABLE, sqlite3.SQLITE_ATTACH, sqlite3.SQLITE_DETACH}
        def authorize(action, first, second, database, source):
            startup = getattr(self._local, 'startup', None)
            if startup is not None and threading.get_ident() == self.queue._startup_thread:
                # A real runtime's constructor/start may add only this reviewed
                # local schema and ownership overlays. It cannot admit work or
                # mutate ordinary job/receipt bodies under a frozen gate.
                tables = startup
                if action == sqlite3.SQLITE_CREATE_TABLE and first in tables:
                    return sqlite3.SQLITE_OK
                if action == sqlite3.SQLITE_CREATE_INDEX and second in tables:
                    return sqlite3.SQLITE_OK
                if action == sqlite3.SQLITE_ALTER_TABLE and second in tables:
                    return sqlite3.SQLITE_OK
                if first == 'sqlite_master' and action in (sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE):
                    return sqlite3.SQLITE_OK
                if action == sqlite3.SQLITE_UPDATE and first == 'jobs' and second in ('status', 'held_reason', 'updated_at'):
                    return sqlite3.SQLITE_OK
                if action in (sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE) and first in (
                    'reference_recovery_startup', 'source_reader_binding', 'source_reader_epochs',
                    'source_reader_pins', 'processing_node_inventory'):
                    return sqlite3.SQLITE_OK
            if (getattr(self._local, 'lifecycle', None) == 'source-reader-close'
                    and action == sqlite3.SQLITE_UPDATE and first == 'source_reader_epochs' and second == 'state'):
                return sqlite3.SQLITE_OK
            if (getattr(self._local, 'lifecycle', None) == 'source-pin-retire'
                    and action == sqlite3.SQLITE_UPDATE and first == 'source_reader_pins' and second == 'state'):
                return sqlite3.SQLITE_OK
            readonly_pragmas = {'table_info', 'table_xinfo', 'index_list', 'index_info',
                                'foreign_key_list', 'quick_check', 'integrity_check'}
            if action in denied or (action == sqlite3.SQLITE_PRAGMA and second is not None and first not in readonly_pragmas):
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK
        db.set_authorizer(authorize)

    @contextmanager
    def runtime_startup(self, runtime):
        """Known real runtime metadata bootstrap, never a user bypass flag."""
        from .reference_runtime import ReferenceRuntime
        if type(runtime) is not ReferenceRuntime or runtime.queue is not self.queue:
            raise CoordinatorFrozen('verified_runtime_startup_required')
        if self.status()['phase'] == 'open':
            yield
            return
        if threading.get_ident() != self.queue._startup_thread:
            raise CoordinatorFrozen('coordinator_startup_thread_required')
        previous = getattr(self._local, 'startup', None)
        self._local.startup = STARTUP_TABLES
        try:
            yield
        finally:
            self._local.startup = previous

    @contextmanager
    def source_readers_shutdown(self, session):
        """Only verified real session sealing can write the closed epoch."""
        from .reference_source_readers import SourceReaders
        if type(session) is not SourceReaders or session.queue is not self.queue:
            raise CoordinatorFrozen('verified_source_reader_shutdown_required')
        session.check()
        if session._handles:
            raise CoordinatorFrozen('source_readers_still_live_or_uncertain')
        previous = getattr(self._local, 'lifecycle', None)
        self._local.lifecycle = 'source-reader-close'
        try:
            yield
        finally:
            self._local.lifecycle = previous

    def require_quiescent_close(self):
        with self.queue._db() as db:
            if db.execute("SELECT 1 FROM coordinator_operation_pins WHERE state IN ('active','uncertain') LIMIT 1").fetchone():
                raise CoordinatorFrozen('coordinator_operations_still_live_or_uncertain')

    def seal_epoch(self):
        self.require_quiescent_close()
        binding = _wire(self._binding())
        with self._control(), self.queue._db() as db:
            row = db.execute('SELECT binding_json,state FROM coordinator_operation_epochs WHERE epoch=?',
                             (self.queue.startup_id,)).fetchone()
            if row is None or row['binding_json'] != binding or row['state'] != 'active':
                raise CoordinatorFrozen('coordinator_epoch_sealing_uncertain')
            db.execute("UPDATE coordinator_operation_epochs SET state='closed' WHERE epoch=?", (self.queue.startup_id,))

    @contextmanager
    def source_pin_retire(self, handle):
        """Retire a real previously admitted descriptor, never a caller ID."""
        from .reference_source_readers import SourceReadHandle
        if type(handle) is not SourceReadHandle or handle.session.queue is not self.queue:
            raise CoordinatorFrozen('verified_source_pin_required')
        session = handle.session
        session.check()
        if (handle.closed or handle.uncertain or session._handles.get(handle.reference) is not handle):
            raise CoordinatorFrozen('verified_source_pin_required')
        with self.queue._db() as db:
            saved = db.execute('SELECT epoch,state FROM source_reader_pins WHERE reference=?', (handle.reference,)).fetchone()
            if not saved or saved['epoch'] != session.epoch or saved['state'] not in ('active', 'released'):
                raise CoordinatorFrozen('source_pin_ownership_changed')
        previous = getattr(self._local, 'lifecycle', None)
        self._local.lifecycle = 'source-pin-retire'
        try:
            yield
        finally:
            self._local.lifecycle = previous

    @contextmanager
    def _control(self):
        # Private gate implementation only; no route accepts a bypass flag.
        previous = getattr(self._local, 'control', False)
        self._local.control = True
        try:
            yield
        finally:
            self._local.control = previous

    def _quota(self, db, extra):
        # Bounded control reserve: exact ACK recovery and maintenance must work
        # even when ordinary payload admission has filled its metadata budget.
        # ReferenceStore still measures these bytes in its total, and cannot
        # allocate payload using this reserve. Census adds this 256KiB ceiling
        # explicitly when deriving the physical DB/headroom budget.
        if metadata_bytes(db) + extra + 512 > 256 * 1024:
            raise CoordinatorFrozen('coordinator_freeze_metadata_full')

    @contextmanager
    def operation(self, kind):
        self._check()
        if kind not in OPERATION_KINDS:
            raise ValueError('closed coordinator operation kind required')
        if getattr(self._local, 'operation', None) is not None:
            token = self._local.operation
            if self._active.get(token) != threading.get_ident():
                raise CoordinatorFrozen('coordinator_operation_owner_changed')
            yield token
            return
        token = str(uuid.uuid4())
        with self.queue._lifecycle_lock, self._control(), self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = self._row(db)
            if row['phase'] != 'open':
                raise CoordinatorFrozen('coordinator_maintenance')
            if db.execute("SELECT count(*) FROM coordinator_operation_pins WHERE state IN ('active','uncertain')").fetchone()[0] >= 128:
                raise CoordinatorFrozen('coordinator_operation_capacity')
            values = (token, self.queue.startup_id, kind, row['revision'], 'active')
            self._quota(db, sum(len(str(value).encode('utf-8')) for value in values))
            db.execute('INSERT INTO coordinator_operation_pins VALUES(?,?,?,?,?)', values)
            self._active[token] = threading.get_ident()
        self._local.operation = token
        try:
            yield token
        finally:
            try:
                with self.queue._lifecycle_lock, self._control(), self.queue._db() as db:
                    db.execute('BEGIN IMMEDIATE')
                    row = db.execute('SELECT epoch,state FROM coordinator_operation_pins WHERE reference=?', (token,)).fetchone()
                    if not row or tuple(row) != (self.queue.startup_id, 'active'):
                        raise CoordinatorFrozen('coordinator_operation_retirement_uncertain')
                    db.execute('DELETE FROM coordinator_operation_pins WHERE reference=?', (token,))
            finally:
                self._active.pop(token, None)
                self._local.operation = None

    def transition(self, request, *, owner_secret, healthy=False):
        """Operator-owned exact retry + CAS; server supplies actual health."""
        self._check()
        if type(request) is not dict or set(request) != {'requestId', 'planId', 'expectedRevision', 'action', 'startupId'}:
            raise ValueError('closed coordinator freeze request required')
        for key in ('requestId', 'planId', 'startupId'):
            if type(request[key]) is not str or str(uuid.UUID(request[key])) != request[key]:
                raise ValueError('canonical freeze identity required')
        if type(request['expectedRevision']) is not int or request['expectedRevision'] < 0:
            raise ValueError('freeze revision required')
        if request['action'] not in ('intent', 'drain', 'seal', 'resume') or not owner_secret:
            raise ValueError('freeze action and owner required')
        owner = hashlib.sha256(owner_secret.encode('ascii')).hexdigest()
        wire = _wire(request)
        digest = hashlib.sha256(wire.encode('ascii')).hexdigest()
        with self.queue._lifecycle_lock, self._control(), self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            prior = db.execute('SELECT * FROM coordinator_freeze_receipts WHERE request_id=?', (request['requestId'],)).fetchone()
            if prior:
                if prior['owner_sha256'] != owner or prior['request_json'] != wire or prior['request_sha256'] != digest:
                    raise Conflict('freeze request identity changed')
                return json.loads(prior['receipt_json'])
            row = self._row(db)
            if row['revision'] != request['expectedRevision'] or request['startupId'] != self.queue.startup_id:
                raise Conflict('freeze revision or startup changed')
            target = {'intent': ('open', 'intent'), 'drain': ('intent', 'draining'),
                      'seal': ('draining', 'frozen'), 'resume': ('frozen', 'open')}[request['action']]
            if row['phase'] != target[0]:
                raise Conflict('freeze phase changed')
            if request['action'] == 'intent':
                if db.execute('SELECT 1 FROM coordinator_freeze_receipts WHERE plan_id=? LIMIT 1', (request['planId'],)).fetchone():
                    raise Conflict('freeze plan identity already used')
                if db.execute("SELECT 1 FROM nodes WHERE state NOT IN ('active','draining') LIMIT 1").fetchone():
                    raise Conflict('node maintenance ownership must resolve first')
                if db.execute("SELECT 1 FROM coordinator_operation_pins WHERE epoch!=? AND state IN ('active','uncertain') LIMIT 1", (self.queue.startup_id,)).fetchone():
                    raise Conflict('previous coordinator operations remain uncertain')
            elif row['plan_id'] != request['planId'] or row['owner_sha256'] != owner:
                raise Conflict('freeze plan owner changed')
            if request['action'] == 'seal':
                if db.execute("SELECT 1 FROM coordinator_operation_pins WHERE state IN ('active','uncertain') LIMIT 1").fetchone():
                    raise Conflict('coordinator operations still draining')
                names = {item[0] for item in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                if 'source_reader_pins' in names and db.execute("SELECT 1 FROM source_reader_pins WHERE state IN ('active','uncertain') LIMIT 1").fetchone():
                    raise Conflict('source IO still draining')
            if request['action'] == 'resume' and healthy is not True:
                raise Conflict('verified candidate health required')
            receipt = dict(authorityId=self.queue.authority_id, startupId=self.queue.startup_id,
                planId=request['planId'], revision=row['revision'] + 1, phase=target[1], requestId=request['requestId'])
            encoded = _wire(receipt)
            self._quota(db, 512 + len(wire) + len(encoded) + len(owner) + len(digest) + len(request['requestId']) + len(request['planId']))
            db.execute('UPDATE coordinator_freeze_state SET revision=?,plan_id=?,phase=?,owner_sha256=?,owner_epoch=? WHERE singleton=1',
                       (receipt['revision'], request['planId'], target[1], owner, self.queue.startup_id))
            db.execute('INSERT INTO coordinator_freeze_receipts VALUES(?,?,?,?,?,?)',
                       (request['requestId'], request['planId'], owner, digest, wire, encoded))
            return receipt

