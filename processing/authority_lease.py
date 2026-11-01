"""Host-wide exclusive lease on the coordinator authority.

``coordinator.lock`` lives inside the state volume, so it cannot stop two
coordinators that run on *copies* of that volume with the same authority. This
lease lives in a separate host directory that every coordinator container
bind-mounts. It is an OS ``flock`` on ``<authority>.lease``: held for the life
of the process, released by the kernel when the process exits, and never
granted to a second holder. Acquisition happens before the state database is
opened for writing.

It fences only coordinators that run this code with the same lease directory.
It does not fence the d738 image, which has no lease, or a coordinator started
with a different lease directory.
"""

import json
import os
from pathlib import Path
import re
import sqlite3
import time
import threading
import stat
from contextlib import contextmanager

AUTHORITY = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')
MARKER_DDL = ('CREATE TABLE coordinator_authority_lease (singleton INTEGER PRIMARY KEY CHECK(singleton=1), '
              'required INTEGER NOT NULL CHECK(required=1))')


class LeaseUnavailable(RuntimeError):
    pass


def existing_authority(state_dir):
    """Read the stored authority without creating or modifying any state."""
    path = Path(state_dir) / 'jobs.sqlite3'
    if not path.is_file():
        return None
    db = sqlite3.connect('file:' + str(path) + '?mode=ro', uri=True)
    try:
        if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='coordinator_identity'").fetchone():
            return None
        row = db.execute('SELECT authority_id FROM coordinator_identity WHERE singleton=1').fetchone()
        return row[0] if row else None
    finally:
        db.close()


class AuthorityLease:
    def __init__(self, directory, authority):
        if type(authority) is not str or not AUTHORITY.fullmatch(authority):
            raise LeaseUnavailable('authority_lease_invalid_authority')
        directory = Path(directory)
        if directory.is_symlink() or not directory.is_dir():
            raise LeaseUnavailable('authority_lease_directory_unavailable')
        self.authority = authority
        self.path = directory / (authority + '.lease')
        import fcntl
        try:
            descriptor = os.open(self.path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        except OSError:
            raise LeaseUnavailable('authority_lease_unavailable') from None
        try:
            try:fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:raise LeaseUnavailable('authority_lease_held') from None
            self._descriptor = descriptor
            self._pid = os.getpid()
            self._mutex = threading.RLock()
            self._held_thread = None
            info=os.fstat(descriptor);directory_info=directory.lstat()
            self._identity=(info.st_dev,info.st_ino)
            self._directory_identity=(directory_info.st_dev,directory_info.st_ino)
            # Diagnostic only; the flock, not this content, is the lease.
            record = json.dumps({'authorityId': authority, 'pid': os.getpid(), 'host': os.uname().nodename,
                                 'acquiredAt': int(time.time())}).encode() + b'\n'
            os.ftruncate(descriptor, 0)
            os.pwrite(descriptor, record, 0)
            os.fsync(descriptor)
        except BaseException:
            self._descriptor=None
            os.close(descriptor)
            raise

    def close(self):
        with self._mutex:
            if self._held_thread == threading.get_ident():
                raise LeaseUnavailable('authority_lease_operation_active')
            descriptor, self._descriptor = getattr(self, '_descriptor', None), None
            if descriptor is not None:
                os.close(descriptor)

    @contextmanager
    def held(self, deadline):
        """Pin this exact local lease object while a bounded operation runs."""
        remaining=deadline-time.monotonic()
        if os.getpid()!=self._pid or remaining<=0 or not self._mutex.acquire(timeout=remaining):
            raise LeaseUnavailable('authority_lease_unavailable')
        registered=False
        try:
            if self._held_thread is not None: raise LeaseUnavailable('authority_lease_operation_active')
            self._held_thread=threading.get_ident();registered=True
            self.check_held()
            yield
            self.check_held()
        finally:
            if registered:self._held_thread=None
            self._mutex.release()

    def check_held(self):
        if self._held_thread!=threading.get_ident() or os.getpid()!=self._pid:
            raise LeaseUnavailable('authority_lease_operation_required')
        descriptor=self._descriptor
        if descriptor is None:raise LeaseUnavailable('authority_lease_unavailable')
        try:
            directory=self.path.parent.lstat();named=self.path.lstat();opened=os.fstat(descriptor)
            if (self.path.parent.resolve()!=self.path.parent.absolute() or self.path.is_symlink()
                    or not stat.S_ISDIR(directory.st_mode) or not stat.S_ISREG(named.st_mode)
                    or named.st_nlink!=1 or (directory.st_dev,directory.st_ino)!=self._directory_identity
                    or (named.st_dev,named.st_ino)!=self._identity or (opened.st_dev,opened.st_ino)!=self._identity):
                raise LeaseUnavailable('authority_lease_changed')
        except OSError:raise LeaseUnavailable('authority_lease_unavailable') from None


def acquire(directory, state_dir):
    """Lease the authority already stored in ``state_dir``; refuse fresh state."""
    authority = existing_authority(state_dir)
    if authority is None:
        raise LeaseUnavailable('authority_lease_requires_existing_authority')
    return AuthorityLease(directory, authority)


def record_required(queue, lease):
    """Persist that this state needs a lease, after checking the queue's authority."""
    if queue.authority_id != lease.authority:
        raise LeaseUnavailable('authority_lease_authority_changed')
    with queue._db() as db:
        db.execute('BEGIN IMMEDIATE')
        existing = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='coordinator_authority_lease'").fetchone()
        if existing is None:
            db.execute(MARKER_DDL)
            db.execute('INSERT INTO coordinator_authority_lease VALUES(1,1)')
        elif existing[0] != MARKER_DDL or db.execute('SELECT COUNT(*) FROM coordinator_authority_lease').fetchone()[0] != 1:
            raise LeaseUnavailable('authority_lease_marker_changed')


def required(state_dir):
    """Read-only check, before any state write, that this state demands a lease."""
    path = Path(state_dir) / 'jobs.sqlite3'
    if not path.is_file():
        return False
    db = sqlite3.connect('file:' + str(path) + '?mode=ro', uri=True)
    try:
        return db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='coordinator_authority_lease'").fetchone() is not None
    finally:
        db.close()
