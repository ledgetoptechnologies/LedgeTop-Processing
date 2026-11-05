"""Preserve the enrolled host-wide authority lease before any Queue writes.

Compatible with the required marker and lease filename in installed 693cb7e.
The diagnostic file contents are neither rewritten nor accepted as ownership
proof. The actual private original descriptor/flock is the authority boundary.
"""
import os
from pathlib import Path
import re
import sqlite3
import stat

MARKER_DDL = ('CREATE TABLE coordinator_authority_lease (singleton INTEGER PRIMARY KEY CHECK(singleton=1), '
              'required INTEGER NOT NULL CHECK(required=1))')
AUTHORITY = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z')


class LeaseUnavailable(RuntimeError):
    pass


def _private(path, *, directory=False, lease_directory=False, lease_file=False, state_directory=False):
    path = Path(path).absolute()
    if path.resolve() != path or any(parent.is_symlink() for parent in path.parents):
        raise LeaseUnavailable('authority_lease_original_paths_required')
    info = path.lstat()
    if ((directory and not stat.S_ISDIR(info.st_mode))
            or (not directory and (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1))):
        raise LeaseUnavailable('authority_lease_original_identity_required')
    if os.name == 'posix':
        mode = stat.S_IMODE(info.st_mode)
        allowed = (mode in (0o700, 0o711) if lease_directory else
                   mode in (0o700, 0o755) if state_directory else
                   mode == 0o600 if lease_file else
                   not info.st_mode & (0o077 if directory else 0o022))
        if info.st_uid != os.geteuid() or not allowed:
            raise LeaseUnavailable('authority_lease_private_ownership_required')
    return path, info


def inspect_state(state_dir):
    """Read immutable identity/marker only; no Queue, SQLite sidecars or writes."""
    root = Path(state_dir).absolute()
    database = root / 'jobs.sqlite3'
    if not database.exists():
        return None, False, None
    _, root_info = _private(root, directory=True, state_directory=True)
    database, before = _private(database)
    if any(Path(str(database) + suffix).exists() for suffix in ('-journal', '-wal', '-shm')):
        raise LeaseUnavailable('authority_state_io_not_quiescent')
    db = sqlite3.connect(database.as_uri() + '?mode=ro&immutable=1', uri=True)
    try:
        objects = {row[0]: row[1:] for row in db.execute("SELECT name,type,sql FROM sqlite_master WHERE name IN ('coordinator_identity','coordinator_authority_lease')")}
        authority = None
        if 'coordinator_identity' in objects:
            row = db.execute('SELECT authority_id FROM coordinator_identity WHERE singleton=1').fetchone()
            authority = row[0] if row else None
            if type(authority) is not str or not AUTHORITY.fullmatch(authority):
                raise LeaseUnavailable('authority_identity_invalid')
        required = 'coordinator_authority_lease' in objects
        if required:
            if (objects['coordinator_authority_lease'] != ('table', MARKER_DDL)
                    or db.execute('SELECT singleton,required FROM coordinator_authority_lease').fetchall() != [(1, 1)]
                    or db.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' AND tbl_name='coordinator_authority_lease'").fetchone()
                    or authority is None):
                raise LeaseUnavailable('authority_lease_marker_changed')
        if os.name == 'posix' and stat.S_IMODE(root_info.st_mode) == 0o755:
            if not required or stat.S_IMODE(before.st_mode) != 0o600:
                raise LeaseUnavailable('legacy_public_state_requires_enrollment_and_private_database')
        after = database.stat()
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
            raise LeaseUnavailable('authority_identity_changed_during_read')
        return authority, required, (after.st_dev, after.st_ino)
    except sqlite3.DatabaseError:
        raise LeaseUnavailable('authority_identity_unavailable') from None
    finally:
        db.close()


def required(state_dir):
    return inspect_state(state_dir)[1]


class AuthorityLease:
    def __init__(self, directory, state_dir):
        if os.name != 'posix':
            raise LeaseUnavailable('linux_authority_lease_required')
        authority, enrolled, state_identity = inspect_state(state_dir)
        if authority is None:
            raise LeaseUnavailable('authority_lease_requires_existing_authority')
        if not enrolled:
            raise LeaseUnavailable('new_authority_enrollment_requires_reviewed_migration')
        directory, info = _private(directory, directory=True, lease_directory=True)
        self.authority = authority
        self.directory = directory
        self.path = directory / (authority + '.lease')
        self.state_path = Path(state_dir).absolute() / 'jobs.sqlite3'
        self._state_identity = state_identity
        state_root = self.state_path.parent.stat()
        self._state_root_identity = (state_root.st_dev, state_root.st_ino, state_root.st_mode, state_root.st_uid, state_root.st_gid)
        self._directory_identity = (info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_gid)
        self._pid, self._descriptor = os.getpid(), None
        import fcntl
        descriptor = None
        try:
            flags = os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC
            descriptor = os.open(self.path, flags, 0o600)
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            os.set_inheritable(descriptor, False)
            opened = os.fstat(descriptor)
            self._file_identity = (opened.st_dev, opened.st_ino, opened.st_mode, opened.st_nlink, opened.st_uid, opened.st_gid)
            self._descriptor = descriptor
            self.check(state_dir)
            observed_authority, observed_required, observed_identity = inspect_state(state_dir)
            if (observed_authority, observed_required, observed_identity) != (authority, enrolled, state_identity):
                raise LeaseUnavailable('authority_state_changed_during_acquisition')
        except (OSError, LeaseUnavailable):
            if descriptor is not None:
                os.close(descriptor)
            self._descriptor = None
            raise LeaseUnavailable('authority_lease_unavailable_or_held') from None

    def check(self, state_dir=None):
        if self._pid != os.getpid() or self._descriptor is None:
            raise LeaseUnavailable('authority_lease_owner_required')
        directory, root = _private(self.directory, directory=True, lease_directory=True)
        path, info = _private(self.path, lease_file=True)
        opened = os.fstat(self._descriptor)
        identity = lambda value: (value.st_dev, value.st_ino, value.st_mode, value.st_nlink, value.st_uid, value.st_gid)
        if ((root.st_dev, root.st_ino, root.st_mode, root.st_uid, root.st_gid) != self._directory_identity
                or identity(info) != self._file_identity or identity(opened) != self._file_identity):
            raise LeaseUnavailable('authority_lease_identity_changed')
        if state_dir is not None and Path(state_dir).absolute() / 'jobs.sqlite3' != self.state_path:
            raise LeaseUnavailable('authority_lease_state_binding_changed')
        _, state = _private(self.state_path)
        _, state_root = _private(self.state_path.parent, directory=True, state_directory=True)
        if ((state_root.st_dev, state_root.st_ino, state_root.st_mode, state_root.st_uid, state_root.st_gid) != self._state_root_identity
                or stat.S_IMODE(state_root.st_mode) == 0o755 and stat.S_IMODE(state.st_mode) != 0o600):
            raise LeaseUnavailable('authority_lease_state_root_identity_changed')
        if (state.st_dev, state.st_ino) != self._state_identity:
            raise LeaseUnavailable('authority_lease_state_identity_changed')

    def snapshot(self):
        self.check()
        return dict(authorityId=self.authority, directory=str(self.directory),
                    directoryIdentity=list(self._directory_identity), path=str(self.path),
                    fileIdentity=list(self._file_identity))

    def close(self):
        if self._pid != os.getpid():
            raise LeaseUnavailable('authority_lease_owner_required')
        descriptor = self._descriptor
        if descriptor is None:
            return
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino, opened.st_mode, opened.st_nlink, opened.st_uid, opened.st_gid) != self._file_identity:
            raise LeaseUnavailable('authority_lease_descriptor_changed')
        os.close(descriptor)
        self._descriptor = None


def acquire(directory, state_dir):
    return AuthorityLease(directory, state_dir)


def record_required(queue, lease):
    if type(lease) is not AuthorityLease or queue.authority_id != lease.authority:
        raise LeaseUnavailable('authority_lease_authority_changed')
    lease.check(Path(queue.path).parent)
    with queue._db() as db:
        existing = db.execute("SELECT type,sql FROM sqlite_master WHERE name='coordinator_authority_lease'").fetchone()
        if existing is None:
            raise LeaseUnavailable('authority_lease_marker_missing')
        elif tuple(existing) != ('table', MARKER_DDL) or [tuple(row) for row in db.execute('SELECT singleton,required FROM coordinator_authority_lease')] != [(1, 1)]:
            raise LeaseUnavailable('authority_lease_marker_changed')
