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
    'source-io-admission', 'source-io', 'source-release-submit', 'source-release', 'legacy-archive'))
CONTROL_LIMIT = 256 * 1024
TRANSITION_RESERVE = 2048
PLAN_FINISH_RESERVE = 8192
STARTUP_TABLES = frozenset(('coordinator_identity', 'jobs', 'nodes', 'owned_maintenance_schedules',
    'processing_node_inventory', 'direct_intake_reservations', 'reference_acceptances', 'reference_claims', 'reference_jobs',
    'reference_outbox', 'reference_recovery_receipts', 'reference_recovery_startup',
    'reference_source_grant_renewals', 'reference_source_grant_tokens', 'reference_source_intakes',
    'reference_source_object_pins', 'reference_source_object_receipts', 'reference_source_range_requests',
    'reference_source_range_usage', 'reference_terminal_acceptances', 'reference_transfer_grants',
    'reference_viewer_receipts', 'reference_viewer_transfers', 'reference_worker_acceptance_observations',
    'reference_worker_failures', 'reference_worker_receipts', 'source_reader_binding',
    'source_reader_epochs', 'source_reader_fences', 'source_reader_pins', 'sqlite_sequence',
    'viewer_files', 'viewer_incarnations', 'viewer_tasks', 'coordinator_freeze_state',
    'coordinator_freeze_receipts', 'coordinator_operation_pins', 'coordinator_operation_epochs',
    'coordinator_authority_lease'))


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
        '/v1/prototype/coordinator-freeze', '/v1/prototype/coordinator-custody') or re.fullmatch(
            r'/ltds/v[12]/jobs/[A-Za-z0-9][A-Za-z0-9_-]{0,127}', path)
        or re.fullmatch(r'/v1/prototype/(?:nodes|jobs)/[A-Za-z0-9][A-Za-z0-9._-]{0,63}', path)
        or re.fullmatch(r'/v1/prototype/nodes/[A-Za-z0-9][A-Za-z0-9._-]{0,63}/schedule-owned/[a-f0-9-]{36}', path))
    if queue is None or pure or path == '/v1/prototype/coordinator-freeze':
        yield
    else:
        with queue.coordinator_operation('http'):
            yield


def _wire(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def _pin_metadata_size(row):
    used = sum(len(str(value).encode('utf-8')) for value in row if value is not None)
    if row[2] == 'legacy-archive':
        # Unknown descriptor close must not borrow capacity to represent itself.
        current = len(row[4].encode('utf-8')) if type(row[4]) is str else 0
        used += max(0, len('uncertain') - current)
    return used


def metadata_bytes(db):
    names = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    used = 0
    for table in ('coordinator_freeze_state', 'coordinator_freeze_receipts', 'coordinator_operation_pins',
                  'coordinator_operation_epochs'):
        if table in names:
            for row in db.execute('SELECT * FROM ' + table):
                used += (_pin_metadata_size(row) if table == 'coordinator_operation_pins' else
                         sum(len(str(value).encode('utf-8')) for value in row if value is not None))
    return used


def active_metadata_bytes(db):
    """Emergency capacity is independent of retained, completed history."""
    names = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if 'coordinator_freeze_state' not in names:
        return 0
    state = db.execute('SELECT * FROM coordinator_freeze_state WHERE singleton=1').fetchone()
    if state is None:
        return 0
    size = lambda row: sum(len(str(value).encode('utf-8')) for value in row if value is not None)
    used = size(state)
    if state['phase'] != 'open' and 'coordinator_freeze_receipts' in names:
        used += sum(size(row) for row in db.execute('SELECT * FROM coordinator_freeze_receipts WHERE plan_id=?', (state['plan_id'],)))
    if 'coordinator_operation_pins' in names:
        used += sum(_pin_metadata_size(row) for row in db.execute("SELECT * FROM coordinator_operation_pins WHERE state IN ('active','uncertain')"))
    if 'coordinator_operation_epochs' in names:
        used += sum(size(row) for row in db.execute("SELECT * FROM coordinator_operation_epochs WHERE state='active'"))
    return used


class FreezeGate:
    def __init__(self, queue):
        self.queue = queue
        self._local = threading.local()
        self._active = {}
        self._legacy_archives = {}
        self._legacy_archive_condition = threading.Condition()
        self._pid = os.getpid()
        with queue._db() as db:
            existing = {item[0] for item in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if existing.intersection(('coordinator_admission', 'coordinator_admission_operations', 'legacy_fence_requirement')):
                raise CoordinatorFrozen('reviewed_legacy_barrier_conversion_required')
            db.executescript('''
                CREATE TABLE IF NOT EXISTS coordinator_freeze_state (
                  singleton INTEGER PRIMARY KEY CHECK(singleton=1), revision INTEGER NOT NULL,
                  plan_id TEXT, phase TEXT NOT NULL, owner_sha256 TEXT, owner_epoch TEXT,
                  reserved_control_bytes INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS coordinator_freeze_receipts (
                  request_id TEXT PRIMARY KEY, plan_id TEXT NOT NULL, owner_sha256 TEXT NOT NULL,
                  request_sha256 TEXT NOT NULL, request_json TEXT NOT NULL, receipt_json TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS coordinator_operation_pins (
                  reference TEXT PRIMARY KEY, epoch TEXT NOT NULL, kind TEXT NOT NULL,
                  start_revision INTEGER NOT NULL, state TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS coordinator_operation_epochs (
                  epoch TEXT PRIMARY KEY, binding_json TEXT NOT NULL, binding_sha256 TEXT NOT NULL,
                  state TEXT NOT NULL, retired_by TEXT);
            ''')
            if 'reserved_control_bytes' not in {item[1] for item in db.execute('PRAGMA table_info(coordinator_freeze_state)')}:
                db.execute('ALTER TABLE coordinator_freeze_state ADD COLUMN reserved_control_bytes INTEGER NOT NULL DEFAULT 0')
            db.execute('''INSERT OR IGNORE INTO coordinator_freeze_state
                (singleton,revision,plan_id,phase,owner_sha256,owner_epoch,reserved_control_bytes)
                VALUES(1,0,NULL,'open',NULL,NULL,0)''')
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
            remaining = row['reserved_control_bytes']
            reserved_after = remaining
            if row['phase'] != 'open' and remaining:
                reserved_after -= charge
                if reserved_after < TRANSITION_RESERVE:
                    uncertain = True
            try:
                self._quota(db, charge, reserved_after=max(0, reserved_after))
            except CoordinatorFrozen:
                uncertain = True
            if not uncertain:
                db.execute('INSERT INTO coordinator_operation_epochs VALUES(?,?,?,?,NULL)',
                           (queue.startup_id, wire, binding_sha, 'active'))
                if reserved_after != remaining:
                    db.execute('UPDATE coordinator_freeze_state SET reserved_control_bytes=? WHERE singleton=1', (reserved_after,))
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
        if os.name == 'posix':
            root_mode = stat.S_IMODE(root_info.st_mode)
            enrolled = getattr(queue, 'authority_lease', None)
            if root_mode == 0o755:
                from .authority_lease import AuthorityLease
                if type(enrolled) is not AuthorityLease or stat.S_IMODE(db_info.st_mode) != 0o600:
                    raise CoordinatorFrozen('exact_enrolled_legacy_state_layout_required')
                enrolled.check(root)
            elif root_mode != 0o700:
                raise CoordinatorFrozen('private_original_coordinator_lease_required')
            if (root_info.st_uid != os.geteuid() or db_info.st_uid != os.geteuid()
                    or lock_info.st_uid != os.geteuid() or lock_info.st_mode & 0o022):
                raise CoordinatorFrozen('private_original_coordinator_lease_required')
        result = dict(authorityId=queue.authority_id, database=str(database),
            databaseIdentity=[db_info.st_dev, db_info.st_ino], root=str(root),
            rootIdentity=[root_info.st_dev, root_info.st_ino], lease=str(lease),
            leaseIdentity=[lock_info.st_dev, lock_info.st_ino, lock_info.st_mode, lock_info.st_nlink, lock_info.st_uid])
        enrolled = getattr(queue, 'authority_lease', None)
        if enrolled is not None:
            result['authorityLease'] = enrolled.snapshot()
        return result

    def _check(self):
        if os.getpid() != self._pid or self.queue._lock.closed:
            raise CoordinatorFrozen('coordinator_owner_unavailable')

    def _row(self, db):
        row = db.execute('SELECT * FROM coordinator_freeze_state WHERE singleton=1').fetchone()
        if (row is None or row['phase'] not in ('open', 'intent', 'draining', 'frozen', 'held')
                or type(row['revision']) is not int or not 0 <= row['revision'] <= 2**53 - 1
                or type(row['reserved_control_bytes']) is not int
                or not 0 <= row['reserved_control_bytes'] <= PLAN_FINISH_RESERVE):
            raise CoordinatorFrozen('coordinator_freeze_journal_unavailable')
        if row['plan_id'] is None:
            if row['phase'] not in ('open', 'held') or row['owner_sha256'] is not None or row['owner_epoch'] is not None:
                raise CoordinatorFrozen('coordinator_freeze_owner_unavailable')
        else:
            try:
                valid = (str(uuid.UUID(row['plan_id'])) == row['plan_id']
                         and str(uuid.UUID(row['owner_epoch'])) == row['owner_epoch']
                         and re.fullmatch('[a-f0-9]{64}', row['owner_sha256']) is not None)
            except (ValueError, TypeError, AttributeError):
                valid = False
            if not valid:
                raise CoordinatorFrozen('coordinator_freeze_owner_unavailable')
        return row

    def status(self):
        self._check()
        with self.queue._db() as db:
            row = self._row(db)
            active = db.execute("SELECT count(*) FROM coordinator_operation_pins WHERE state IN ('active','uncertain')").fetchone()[0]
            return dict(authorityId=self.queue.authority_id, startupId=self.queue.startup_id,
                        revision=row['revision'], planId=row['plan_id'], phase=row['phase'], operations=active,
                        reservedControlBytes=row['reserved_control_bytes'])

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
        denied_by_gate = [False]
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
                if action == sqlite3.SQLITE_UPDATE and first == 'coordinator_freeze_state' and second == 'reserved_control_bytes':
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
                denied_by_gate[0] = True
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK
        db.set_authorizer(authorize)
        return denied_by_gate

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
        with self._legacy_archive_condition:
            if self._legacy_archives:
                raise CoordinatorFrozen('legacy_archives_still_live_or_uncertain')
        with self.queue._db() as db:
            if db.execute("SELECT 1 FROM coordinator_operation_pins WHERE state IN ('active','uncertain') LIMIT 1").fetchone():
                raise CoordinatorFrozen('coordinator_operations_still_live_or_uncertain')

    def register_legacy_archive(self, owner):
        """Only an actual typed descriptor owner may outlive its HTTP pin."""
        from .legacy_viewer_readonly import _ArchiveOwner
        self._check()
        token = getattr(self._local, 'operation', None)
        if (type(owner) is not _ArchiveOwner or owner.queue is not self.queue
                or owner.thread_id != threading.get_ident() or owner.closed or owner.uncertain
                or self._active.get(token) != threading.get_ident()):
            raise CoordinatorFrozen('verified_legacy_archive_owner_required')
        with self.queue._lifecycle_lock, self._legacy_archive_condition, self._control(), self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = self._row(db)
            if row['phase'] != 'open' or owner.reference in self._legacy_archives:
                raise CoordinatorFrozen('coordinator_maintenance')
            if db.execute("SELECT count(*) FROM coordinator_operation_pins WHERE state IN ('active','uncertain')").fetchone()[0] >= 128:
                raise CoordinatorFrozen('coordinator_operation_capacity')
            values = (owner.reference, self.queue.startup_id, 'legacy-archive', row['revision'], 'active')
            self._quota(db, _pin_metadata_size(values))
            db.execute('INSERT INTO coordinator_operation_pins VALUES(?,?,?,?,?)', values)
            self._legacy_archives[owner.reference] = owner

    def finish_legacy_archive(self, owner):
        """Retirement is after verified close, never a finally-side assumption."""
        from .legacy_viewer_readonly import _ArchiveOwner
        self._check()
        if (type(owner) is not _ArchiveOwner or owner.queue is not self.queue
                or owner.process_id != os.getpid() or owner.thread_id != threading.get_ident()
                or not owner.closed or owner.uncertain or owner._fds or owner._iterators):
            raise CoordinatorFrozen('verified_legacy_archive_close_required')
        with self.queue._lifecycle_lock, self._legacy_archive_condition, self._control(), self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            if self._legacy_archives.get(owner.reference) is not owner:
                raise CoordinatorFrozen('legacy_archive_owner_changed')
            changed = db.execute("DELETE FROM coordinator_operation_pins WHERE reference=? AND epoch=? AND kind='legacy-archive' AND state='active'",
                                 (owner.reference, self.queue.startup_id))
            if changed.rowcount != 1:
                raise CoordinatorFrozen('legacy_archive_retirement_uncertain')
        with self._legacy_archive_condition:
            del self._legacy_archives[owner.reference]
            self._legacy_archive_condition.notify_all()

    def uncertain_legacy_archive(self, owner):
        from .legacy_viewer_readonly import _ArchiveOwner
        self._check()
        if (type(owner) is not _ArchiveOwner or owner.queue is not self.queue or not owner.uncertain
                or owner.process_id != os.getpid() or owner.thread_id != threading.get_ident()
                or self._legacy_archives.get(owner.reference) is not owner):
            raise CoordinatorFrozen('verified_legacy_archive_owner_required')
        # An unknown metadata commit retains the actual in-memory typed owner.
        with self._control(), self.queue._db() as db:
            db.execute("UPDATE coordinator_operation_pins SET state='uncertain' WHERE reference=? AND epoch=? AND kind='legacy-archive'",
                       (owner.reference, self.queue.startup_id))

    def drain_legacy_archives(self):
        """Final service shutdown waits; an uncertain close has no retry here."""
        self._check()
        with self._legacy_archive_condition:
            while self._legacy_archives:
                self._legacy_archive_condition.wait()

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

    def _quota(self, db, extra, *, reserved_after=None):
        # Bounded control reserve: exact ACK recovery and maintenance must work
        # even when ordinary payload admission has filled its metadata budget.
        # ReferenceStore still measures these bytes in its total, and cannot
        # allocate payload using this reserve. Census adds this 256KiB ceiling
        # explicitly when deriving the physical DB/headroom budget.
        reserved = self._row(db)['reserved_control_bytes'] if reserved_after is None else reserved_after
        ordinary, ordinary_limit = self._ordinary_usage(db)
        active = active_metadata_bytes(db)
        if (active + extra + reserved + 512 > CONTROL_LIMIT
                or ordinary + active + extra + reserved + 512 > ordinary_limit + CONTROL_LIMIT):
            raise CoordinatorFrozen('coordinator_freeze_metadata_full')

    def _ordinary_usage(self, db):
        store = getattr(self.queue, '_reference_scheduler', None)
        limit = store.max_metadata_bytes if store is not None else 64 * 1024 * 1024
        if store is not None:
            total = store._metadata_used_bytes(db)
        else:
            # Query calculator only: no constructor, Queue, spool, network,
            # reconciliation or lease acquisition. Legacy incomplete schemas
            # use a conservative scalar-row byte census until reviewed upgrade.
            try:
                from .reference_store import ReferenceStore
                total = ReferenceStore._metadata_used_bytes(object.__new__(ReferenceStore), db)
            except sqlite3.OperationalError:
                total = 0
                for table, in db.execute("SELECT name FROM sqlite_master WHERE type='table'"):
                    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', table):
                        raise CoordinatorFrozen('unrecognized_metadata_table')
                    for row in db.execute('SELECT * FROM "' + table + '"'):
                        total += sum(len(value) if type(value) is bytes else len(str(value).encode('utf-8'))
                                     for value in row if value is not None)
        return max(0, total - active_metadata_bytes(db)), limit

    def reserve_startup_metadata(self, db, extra):
        """Borrow only a frozen plan's reserved, verified startup allocation.

        The connection authorizer still denies job/receipt payload mutations;
        only the reviewed real runtime metadata scope can reach this path.
        """
        if (getattr(self._local, 'startup', None) is not STARTUP_TABLES
                or threading.get_ident() != self.queue._startup_thread
                or type(extra) is not int or extra < 0):
            return False
        row = self._row(db)
        if row['phase'] != 'frozen' or row['reserved_control_bytes'] - extra < TRANSITION_RESERVE:
            return False
        after = row['reserved_control_bytes'] - extra
        self._quota(db, extra, reserved_after=after)
        db.execute('UPDATE coordinator_freeze_state SET reserved_control_bytes=? WHERE singleton=1', (after,))
        return True

    def _owned_drains(self, db, plan_id):
        """Only this freeze plan's exact durable node schedule may be adopted."""
        from .maintenance import canonical
        for node in db.execute("SELECT * FROM nodes WHERE state!='active'"):
            if node['state'] != 'draining':
                raise Conflict('node maintenance ownership must resolve first')
            request_id = str(uuid.uuid5(uuid.UUID(plan_id), 'coordinator-freeze:' + node['id']))
            saved = db.execute('SELECT * FROM owned_maintenance_schedules WHERE request_id=? AND node_id=?',
                               (request_id, node['id'])).fetchone()
            if saved is None or saved['authority_id'] != self.queue.authority_id:
                raise Conflict('foreign node drain must resolve first')
            request, receipt = json.loads(saved['request_json']), json.loads(saved['receipt_json'])
            request_sha = hashlib.sha256(canonical(dict(nodeId=node['id'], request=request)).encode()).hexdigest()
            receipt_body = {key: value for key, value in receipt.items() if key != 'receiptSha256'}
            if (saved['request_json'] != canonical(request) or saved['receipt_json'] != canonical(receipt)
                    or request_sha != saved['request_sha256'] or receipt.get('requestSha256') != request_sha
                    or receipt.get('receiptSha256') != hashlib.sha256(canonical(receipt_body).encode()).hexdigest()
                    or request.get('requestId') != request_id or request.get('authorityId') != self.queue.authority_id
                    or receipt.get('requestId') != request_id or receipt.get('authorityId') != self.queue.authority_id
                    or receipt.get('nodeId') != node['id'] or receipt.get('planId') != node['plan_id']
                    or receipt.get('state') != 'draining' or receipt.get('revision') != node['revision']
                    or request.get('expectedRevision', -2) + 1 != node['revision']
                    or any(receipt.get(key) != node[column] for key, column in (
                        ('drainAt', 'drain_at'), ('notBefore', 'not_before'), ('afterJobId', 'after_job_id'),
                        ('targetImage', 'target_image')))):
                raise Conflict('coordinator node drain receipt changed')

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
            self._quota(db, _pin_metadata_size(values))
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
        if type(request['expectedRevision']) is not int or not 0 <= request['expectedRevision'] < 2**53 - 1:
            raise ValueError('freeze revision required')
        if request['action'] not in ('intent', 'drain', 'seal', 'resume', 'abort') or not owner_secret:
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
                expected = dict(authorityId=self.queue.authority_id, startupId=request['startupId'],
                    planId=request['planId'], revision=request['expectedRevision'] + 1,
                    phase={'intent': 'intent', 'drain': 'draining', 'seal': 'frozen',
                           'resume': 'open', 'abort': 'open'}[request['action']], requestId=request['requestId'])
                if prior['plan_id'] != request['planId'] or prior['receipt_json'] != _wire(expected):
                    raise Conflict('freeze stored receipt binding changed')
                return expected
            row = self._row(db)
            if row['revision'] != request['expectedRevision'] or request['startupId'] != self.queue.startup_id:
                raise Conflict('freeze revision or startup changed')
            target = {'intent': ('open', 'intent'), 'drain': ('intent', 'draining'),
                      'seal': ('draining', 'frozen'), 'resume': ('frozen', 'open'),
                      'abort': (('intent', 'draining'), 'open')}[request['action']]
            expected_phases = target[0] if isinstance(target[0], tuple) else (target[0],)
            if row['phase'] not in expected_phases:
                raise Conflict('freeze phase changed')
            if request['action'] == 'intent':
                if db.execute('SELECT 1 FROM coordinator_freeze_receipts WHERE plan_id=? LIMIT 1', (request['planId'],)).fetchone():
                    raise Conflict('freeze plan identity already used')
                self._owned_drains(db, request['planId'])
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
            if request['action'] == 'abort' and row['owner_epoch'] != self.queue.startup_id:
                raise Conflict('only original startup may abort its unfinished freeze')
            if request['action'] in ('resume', 'abort') and healthy is not True:
                raise Conflict('verified candidate health required')
            receipt = dict(authorityId=self.queue.authority_id, startupId=self.queue.startup_id,
                planId=request['planId'], revision=row['revision'] + 1, phase=target[1], requestId=request['requestId'])
            encoded = _wire(receipt)
            charge = 512 + len(wire) + len(encoded) + len(owner) + len(digest) + len(request['requestId']) + len(request['planId'])
            if charge > TRANSITION_RESERVE:
                raise CoordinatorFrozen('freeze_transition_exceeds_reviewed_reservation')
            if request['action'] == 'intent':
                reserved_after = PLAN_FINISH_RESERVE
            else:
                if row['reserved_control_bytes'] < TRANSITION_RESERVE:
                    raise CoordinatorFrozen('freeze_finish_reservation_unavailable')
                reserved_after = (0 if target[1] == 'open'
                                  else row['reserved_control_bytes'] - TRANSITION_RESERVE)
            self._quota(db, charge, reserved_after=reserved_after)
            db.execute('''UPDATE coordinator_freeze_state
                SET revision=?,plan_id=?,phase=?,owner_sha256=?,owner_epoch=?,reserved_control_bytes=? WHERE singleton=1''',
                       (receipt['revision'], request['planId'], target[1], owner, self.queue.startup_id, reserved_after))
            db.execute('INSERT INTO coordinator_freeze_receipts VALUES(?,?,?,?,?,?)',
                       (request['requestId'], request['planId'], owner, digest, wire, encoded))
            return receipt
