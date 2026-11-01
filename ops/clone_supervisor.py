"""Unwired copy-to-mount hold, conditional on a trusted host launcher policy.

This cannot establish that policy, authorize launch, or fence legacy writers.
Failures retain the durable intent and copied files for inspection.
"""
import fcntl
import json
import os
from pathlib import Path
import re
import stat

from ops.authority_handoff import _read_private, _write_once
from ops.outer_authority_guard import OuterAuthorityGuard
from ops.pr197_operation_fence import OperationFence
from ops.pr197_stopped_census import DEFAULT_LIMITS, _identity, _mount, stopped_census
from ops import state_census, stopped_viewer_backup


class CloneHeld(ValueError):
    pass


def _fd_lock(fd):
    """Require this open description to report an exclusive Linux flock."""
    st = os.fstat(fd)
    expected = (os.major(st.st_dev), os.minor(st.st_dev), st.st_ino)
    with open('/proc/self/fdinfo/' + str(fd), encoding='ascii') as stream:
        lines = [line.split() for line in stream if line.startswith('lock:')]
    valid = []
    for parts in lines:
        if len(parts) != 9 or parts[2:5] != ['FLOCK', 'ADVISORY', 'WRITE']:
            continue
        try:
            major, minor, inode = parts[6].split(':')
            valid.append((int(parts[5]), (int(major, 16), int(minor, 16), int(inode))))
        except ValueError:
            continue
    if valid != [(os.getpid(), expected)]:
        raise CloneHeld('exclusive_description_lock_required')


class CloneSupervisor:
    def __init__(self, outer, fence, destination, key, operation_id, *, host_policy_confirmed=False):
        if (host_policy_confirmed is not True or type(outer) is not OuterAuthorityGuard or
                type(fence) is not OperationFence or outer.authority != fence.authority or
                type(key) is not bytes or len(key) != 32 or type(operation_id) is not str or
                not re.fullmatch(r'[0-9a-f]{32}', operation_id)):
            raise CloneHeld('explicit_supervised_clone_required')
        self.outer, self.fence, self.destination = outer, fence, Path(destination)
        self.key, self.operation_id, self.pid = key, operation_id, os.getpid()
        self.fd = None
        self.directory = None
        self.root_identity = None
        self.snapshot = None
        self.identity = None
        self.intent = None

    def _authority(self):
        if self.pid != os.getpid():
            raise CloneHeld('supervisor_owner_changed')
        self.outer.check(); self.fence.check()
        _fd_lock(self.outer.outer)
        for fd in self.fence.fds:
            _fd_lock(fd)
        if self.intent is not None and _read_private(self.outer.pending, 4096) != self.intent:
            raise CloneHeld('clone_intent_changed')

    def copy(self):
        self._authority()
        self.outer._absent()
        if self.snapshot is not None or self.fd is not None:
            raise CloneHeld('clone_already_started')
        if not self.destination.is_absolute() or self.destination.resolve() != self.destination:
            raise CloneHeld('canonical_destination_required')
        record = {'version': 1, 'kind': 'clone_pending', 'operationId': self.operation_id,
                  'authorityId': self.fence.authority, 'destination': str(self.destination)}
        self.intent = json.dumps(record, sort_keys=True, separators=(',', ':')).encode()
        _write_once(self.outer.pending, self.intent)
        try:
            self._authority()
            source = stopped_census(self.fence.root, self.key, operation_fence=self.fence)['census']
            result = stopped_viewer_backup.backup(self.fence.root, self.destination, self.fence.leases,
                self.fence.authority, self.key, stopped_writers_confirmed=True,
                independent_destination_confirmed=True, expected_source_inventory=source,
                operation_fence=self.fence)
            # External host policy must prevent candidate launch throughout copy
            # and until this lock is acquired; private DAC alone is insufficient.
            self.directory = os.open(self.destination, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            self.root_identity = _identity(self.directory)
            self.fd = os.open('coordinator.lock', os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                              dir_fd=self.directory)
            st = os.fstat(self.fd)
            named = (self.destination / 'coordinator.lock').lstat()
            if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1 or (st.st_dev, st.st_ino) != (named.st_dev, named.st_ino):
                raise CloneHeld('clone_lock_identity_changed')
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.identity = (st.st_dev, st.st_ino)
            self.snapshot = result['snapshot']
            self._authority(); self._check_clone()
        except BaseException:
            self.close()
            raise

    def _check_clone(self):
        if self.fd is None or self.identity is None or self.directory is None:
            raise CloneHeld('clone_lock_missing')
        _fd_lock(self.fd)
        named_dir = os.open(self.destination, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            if _identity(named_dir) != self.root_identity or _identity(self.directory) != self.root_identity:
                raise CloneHeld('clone_root_changed')
        finally:
            os.close(named_dir)
        st = os.fstat(self.fd)
        named = os.stat('coordinator.lock', dir_fd=self.directory, follow_symlinks=False)
        if (not stat.S_ISREG(st.st_mode) or st.st_nlink != 1 or
                (st.st_dev, st.st_ino) != self.identity or (named.st_dev, named.st_ino) != self.identity):
            raise CloneHeld('clone_lock_changed')

    def inspect_mount(self, mount):
        """Compare a provisioned readonly alias; does not authorize startup."""
        self._authority(); self._check_clone()
        mount = Path(mount)
        _mount(mount)
        fd = os.open(mount, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            source = self.destination.stat(); alias = os.fstat(fd)
            if (source.st_dev, source.st_ino) != (alias.st_dev, alias.st_ino):
                raise CloneHeld('clone_mount_not_same_root')
            lock = os.stat('coordinator.lock', dir_fd=fd, follow_symlinks=False)
            if (lock.st_dev, lock.st_ino) != self.identity:
                raise CloneHeld('clone_mount_lock_changed')
            mount_identity = _identity(fd)
            observed = state_census.census(mount, self.key, limits=DEFAULT_LIMITS)
            if not state_census.compare(self.snapshot, observed, self.key)['identical']:
                raise CloneHeld('clone_census_changed')
            if observed['database']['authorityId'] != self.fence.authority:
                raise CloneHeld('clone_authority_changed')
            _mount(mount)
            named = os.open(mount, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                if _identity(named) != mount_identity:
                    raise CloneHeld('clone_mount_changed')
            finally:
                os.close(named)
            self._authority(); self._check_clone()
            return {'cloneObserved': True, 'launchAuthorized': False, 'cutoverAuthorized': False}
        finally:
            os.close(fd)

    def close(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None
        if self.directory is not None:
            os.close(self.directory)
            self.directory = None
