"""Copy and verify a stopped coordinator volume; never starts an update.

The operator must stop every writer, including legacy coordinators and workers,
and provision a destination independent of the source and all its aliases.
Both existing cooperative locks are held throughout; they cannot fence writers
that ignore them. The source mount must be read-only and have no SQLite
sidecars. Failure preserves the incomplete destination for inspection.
"""
import os
import argparse
import json
import sys
from pathlib import Path
import re
import sqlite3
import stat

from ops import state_census


class BackupRefused(RuntimeError):
    pass


def _mount_path(value):
    # mountinfo escapes whitespace and backslashes as octal byte sequences.
    return re.sub(r'\\([0-7]{3})', lambda m: chr(int(m.group(1), 8)), value)


def _require_isolated_mounts(source, destination_parent):
    """Refuse ambiguous topology; a different device is a conservative gate.

    Neither mount inspection nor device identity fences writes via another
    namespace or device mapping. Stopping all writers remains a prerequisite.
    """
    if source.stat().st_dev == destination_parent.stat().st_dev:
        raise BackupRefused('destination_not_separate_filesystem')
    try:
        with open('/proc/self/mountinfo', encoding='utf-8') as stream:
            mountpoints = []
            for line in stream:
                fields = line.split()
                if len(fields) < 10 or '-' not in fields:
                    raise BackupRefused('mountinfo_invalid')
                mountpoints.append(Path(_mount_path(fields[4])))
    except OSError as error:
        raise BackupRefused('mountinfo_unavailable') from error
    if source not in mountpoints:
        raise BackupRefused('source_must_be_mount_root')
    if any(source in mount.parents for mount in mountpoints):
        raise BackupRefused('nested_source_mount_refused')


def _canonical_dir(value):
    path = Path(value)
    if not path.is_absolute() or path.resolve(strict=True) != path or path.is_symlink() or not path.is_dir():
        raise BackupRefused('directory_requires_canonical_path')
    return path


def _lock(path):
    import fcntl
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(fd)
        current = path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or (info.st_dev, info.st_ino) != (current.st_dev, current.st_ino):
            raise BackupRefused('lock_identity_invalid')
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except BaseException:
        os.close(fd)
        raise


def _copy_file(source, target, info):
    source_fd = os.open(source, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    target_fd = None
    try:
        before = os.fstat(source_fd)
        fields = lambda s: (s.st_dev, s.st_ino, s.st_mode, s.st_uid, s.st_gid,
                            s.st_nlink, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or fields(before) != fields(info):
            raise BackupRefused('source_file_changed')
        target_fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        count = 0
        while chunk := os.read(source_fd, 1024 * 1024):
            count += len(chunk)
            if count > before.st_size:
                raise BackupRefused('source_file_changed')
            view = memoryview(chunk)
            while view:
                written = os.write(target_fd, view)
                if written <= 0:
                    raise BackupRefused('copy_failed')
                view = view[written:]
        if count != before.st_size or fields(os.fstat(source_fd)) != fields(before) or fields(source.lstat()) != fields(before):
            raise BackupRefused('source_file_changed')
        os.fchmod(target_fd, stat.S_IMODE(before.st_mode))
        os.utime(target_fd, ns=(before.st_atime_ns, before.st_mtime_ns))
        os.fsync(target_fd)
    finally:
        if target_fd is not None:
            os.close(target_fd)
        os.close(source_fd)


def _require_known_quiescent_state(source):
    # Only the reviewed volume roots are accepted; task contents beneath the
    # two managed directories are retained byte-for-byte, never interpreted.
    allowed = {'jobs.sqlite3', 'coordinator.lock', 'viewer-tasks', 'viewer-spool'}
    with os.scandir(source) as entries:
        if any(entry.name not in allowed for entry in entries):
            raise BackupRefused('unknown_root_path')
    uri = (source / 'jobs.sqlite3').as_uri() + '?mode=ro&immutable=1'
    db = sqlite3.connect(uri, uri=True)
    try:
        if db.execute("SELECT count(*) FROM jobs WHERE status IS NULL OR status NOT IN ('succeeded','failed')").fetchone()[0]:
            raise BackupRefused('source_not_quiescent')
        if db.execute("SELECT count(*) FROM viewer_tasks WHERE removed IS NULL OR removed NOT IN (0,1) OR (removed=0 AND (stage IS NULL OR stage NOT IN ('succeeded','failed','cancelled')))").fetchone()[0]:
            raise BackupRefused('source_not_quiescent')
    finally:
        db.close()


def backup(source, destination, lease_directory, authority_id, key, *, stopped_writers_confirmed=False,
           independent_destination_confirmed=False):
    """Create a new owned destination and compare source/copy/source censuses.

    Returns keyed census documents. Never prints task names, rows or tokens.
    No cleanup or restore is attempted on failure. Linux only.
    """
    if os.name != 'posix' or not hasattr(os, 'O_NOFOLLOW'):
        raise BackupRefused('linux_required')
    if not stopped_writers_confirmed:
        raise BackupRefused('explicit_writer_stop_confirmation_required')
    if not independent_destination_confirmed:
        raise BackupRefused('independent_destination_confirmation_required')
    if not isinstance(key, bytes) or len(key) != 32 or not isinstance(authority_id, str) or not re.fullmatch(r'[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}', authority_id):
        raise BackupRefused('invalid_identity')
    source = _canonical_dir(source)
    leases = _canonical_dir(lease_directory)
    destination = Path(destination)
    if not destination.is_absolute() or destination.parent.resolve(strict=True) != destination.parent or destination.exists() or destination.is_symlink():
        raise BackupRefused('destination_requires_new_path')
    if destination.parent == source or source in destination.parent.parents or destination.parent == leases or leases in destination.parent.parents:
        raise BackupRefused('destination_location_invalid')
    parent_info = destination.parent.stat()
    if parent_info.st_uid != os.geteuid() or stat.S_IMODE(parent_info.st_mode) & 0o077:
        raise BackupRefused('destination_parent_not_private')
    if not os.statvfs(source).f_flag & os.ST_RDONLY:
        raise BackupRefused('source_mount_not_readonly')
    _require_isolated_mounts(source, destination.parent)
    # Open existing files only; don't create/truncate the authority lease.
    authority_fd = _lock(leases / (authority_id + '.lease'))
    queue_fd = None
    try:
        queue_fd = _lock(source / 'coordinator.lock')
        before = state_census.census(source, key)
        db = before.get('database') or {}
        if (any(before['tree']['rejected'].values()) or db.get('quickCheck') != 'ok'
                or db.get('authorityId') != authority_id or db.get('sidecars')
                or db.get('active', {}).get('jobs') != 0 or db.get('active', {}).get('viewerTasks') != 0):
            raise BackupRefused('source_not_quiescent')
        _require_known_quiescent_state(source)
        destination.mkdir(mode=0o700)
        parent_fd = os.open(destination.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
        dirs = [(source, destination, source.lstat())]
        visited = 1
        def walk_error(error):
            raise error
        for directory, names, files in os.walk(source, followlinks=False, onerror=walk_error):
            relative = Path(directory).relative_to(source)
            target_dir = destination / relative
            for name in sorted(names):
                src = Path(directory) / name
                info = src.lstat()
                if not stat.S_ISDIR(info.st_mode):
                    raise BackupRefused('unsupported_source_entry')
                dst = target_dir / name
                dst.mkdir(mode=0o700)
                dirs.append((src, dst, info))
            for name in sorted(files):
                src = Path(directory) / name
                _copy_file(src, target_dir / name, src.lstat())
            visited += len(names) + len(files)
            if visited > state_census.MAX_TREE_ENTRIES:
                raise BackupRefused('entry_limit')
        # Apply directory metadata last after creating children.
        for src, dst, info in reversed(dirs):
            current = src.lstat()
            if (current.st_dev, current.st_ino, current.st_mtime_ns, current.st_ctime_ns) != (info.st_dev, info.st_ino, info.st_mtime_ns, info.st_ctime_ns):
                raise BackupRefused('source_directory_changed')
            os.chmod(dst, stat.S_IMODE(info.st_mode))
            os.utime(dst, ns=(info.st_atime_ns, info.st_mtime_ns))
            fd = os.open(dst, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        copied = state_census.census(destination, key)
        after = state_census.census(source, key)
        if not state_census.compare(before, copied, key)['identical'] or not state_census.compare(before, after, key)['identical']:
            raise BackupRefused('snapshot_verification_failed')
        for fd, path in ((queue_fd, source / 'coordinator.lock'), (authority_fd, leases / (authority_id + '.lease'))):
            held, current = os.fstat(fd), path.lstat()
            if (held.st_dev, held.st_ino) != (current.st_dev, current.st_ino):
                raise BackupRefused('lock_identity_changed')
        return {'source': before, 'snapshot': copied}
    finally:
        if queue_fd is not None:
            os.close(queue_fd)
        os.close(authority_fd)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', required=True)
    parser.add_argument('--destination', required=True)
    parser.add_argument('--lease-directory', required=True)
    parser.add_argument('--authority-id', required=True)
    parser.add_argument('--stopped-writers-confirmed', action='store_true', required=True,
                        help='Operator has stopped and excluded all writers, including legacy instances')
    parser.add_argument('--independent-destination-confirmed', action='store_true', required=True,
                        help='Operator provisioned an independent destination, not an alias of source')
    args = parser.parse_args(argv)
    try:
        key = bytes.fromhex(sys.stdin.readline().strip())
        backup(args.source, args.destination, args.lease_directory, args.authority_id, key,
               stopped_writers_confirmed=args.stopped_writers_confirmed,
               independent_destination_confirmed=args.independent_destination_confirmed)
        print(json.dumps({'status': 'verified', 'scope': 'stopped read-only source; writer exclusion required'}))
        return 0
    except (OSError, ValueError, sqlite3.Error, BackupRefused) as error:
        print(json.dumps({'status': 'refused', 'reason': type(error).__name__}), file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
