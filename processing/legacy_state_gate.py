"""Opt-in legacy-state lock wrapper; not configured by the normal image.

The supervisor must mount the original, retained legacy state at /legacy-state
and ensure all legacy launchers use that same lock. Holds the legacy flock in
the coordinator process through exec, so neither starts while the other owns it.
A missing/moved/replaced volume, uncooperative launch or alternate state copy
requires external fencing. This file does not provide a deployment switch.
"""
import os
from pathlib import Path
import stat
import sys


def acquire(path):
    import fcntl
    path = Path(path)
    if not path.is_absolute() or path.resolve() != path:
        raise RuntimeError('legacy_fence_path_invalid')
    flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK
    try:
        fd = os.open(path, flags)
    except OSError:
        raise RuntimeError('legacy_fence_unavailable') from None
    try:
        opened = os.fstat(fd)
        current = os.stat(path, follow_symlinks=False)
        if not stat.S_ISREG(opened.st_mode) or (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino):
            raise RuntimeError('legacy_fence_identity_changed')
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise RuntimeError('legacy_fence_held') from None
        current = os.stat(path, follow_symlinks=False)
        if (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino):
            raise RuntimeError('legacy_fence_identity_changed')
        return fd
    except BaseException:
        os.close(fd)
        raise


def check_distinct(fd, state):
    if not state or not os.path.isabs(state):
        raise RuntimeError('legacy_fence_candidate_state_invalid')
    try:
        candidate = os.stat(Path(state) / 'coordinator.lock', follow_symlinks=False)
    except FileNotFoundError:
        candidate = None
    if candidate is not None and (candidate.st_dev, candidate.st_ino) == (os.fstat(fd).st_dev, os.fstat(fd).st_ino):
        raise RuntimeError('legacy_fence_same_candidate_lock')


def main():
    fd = acquire('/legacy-state/coordinator.lock')
    try:
        check_distinct(fd, os.environ.get('PROCESSING_PROTOTYPE_STATE_DIR'))
    except BaseException:
        os.close(fd)
        raise
    # Hold the same kernel open-file description through exec. The kernel
    # releases it on process exit; no child process is used as a fence holder.
    os.set_inheritable(fd, True)
    os.execv(sys.executable, [sys.executable, '-m', 'processing.app'])


if __name__ == '__main__':
    main()
