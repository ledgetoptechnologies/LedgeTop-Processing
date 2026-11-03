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
import shutil
import sqlite3
import tempfile
import time

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
    def read(db):
        if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='coordinator_identity'").fetchone():
            return None
        row = db.execute('SELECT authority_id FROM coordinator_identity WHERE singleton=1').fetchone()
        return row[0] if row else None
    return _read_committed(path, read)


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
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(descriptor)
            raise LeaseUnavailable('authority_lease_held') from None
        self._descriptor = descriptor
        # Diagnostic only; the flock, not this content, is the lease.
        record = json.dumps({'authorityId': authority, 'pid': os.getpid(), 'host': os.uname().nodename,
                             'acquiredAt': int(time.time())}).encode() + b'\n'
        os.ftruncate(descriptor, 0)
        os.pwrite(descriptor, record, 0)
        os.fsync(descriptor)

    def close(self):
        descriptor, self._descriptor = getattr(self, '_descriptor', None), None
        if descriptor is not None:
            os.close(descriptor)


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
    return _read_committed(path, lambda db: db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='coordinator_authority_lease'").fetchone() is not None)


def _read_committed(path, read):
    """Read the committed state without writing to the state volume.

    A coordinator killed inside a write transaction leaves a hot rollback
    journal, and a read-only connection then refuses to read rather than roll
    it back. The rollback is a state write, so it must wait for the lease and
    the volume lock. Until then, read a private copy that SQLite may roll back,
    and leave the original database and journal untouched.
    """
    try:
        return _query(path, read, 'ro')
    except sqlite3.OperationalError:
        journal = Path(str(path) + '-journal')
        if not journal.is_file() or journal.stat().st_size == 0:
            raise
    stamp = lambda: [(item.stat().st_size, item.stat().st_mtime_ns) for item in (path, journal)]
    try:
        before = stamp()
        with tempfile.TemporaryDirectory(prefix='coordinator-preflight-') as private:
            copy = Path(private) / 'jobs.sqlite3'
            shutil.copyfile(path, copy)
            shutil.copyfile(journal, str(copy) + '-journal')
            if stamp() != before:
                raise OSError('state changed while copying')
            return _query(copy, read, 'rw')
    except (OSError, sqlite3.Error):
        raise LeaseUnavailable('authority_preflight_hot_journal_unreadable') from None


def _query(path, read, mode):
    db = sqlite3.connect('file:' + str(path) + '?mode=' + mode, uri=True)
    try:
        return read(db)
    finally:
        db.close()
