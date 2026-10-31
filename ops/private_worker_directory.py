"""Publish private worker roots before an ownership lease or engine mutation.

No permission repair, deletion or NAS operation. The configured local filesystem
must provide directory fsync; failure prevents factory admission.
"""
import os
from pathlib import Path
import stat


def private_worker_directory(value):
    path = Path(value)
    if not path.is_absolute() or path != Path(os.path.normpath(path)) or '..' in path.parts:
        raise ValueError('literal absolute worker directory required')
    if os.name == 'nt':
        # The owned worker factory is Linux-only. Keep portable synthetic
        # ownership tests usable without claiming Windows directory durability.
        if any(part.is_symlink() for part in (path, *path.parents)):
            raise ValueError('owned private worker directory required')
        path.mkdir(parents=True, mode=0o700, exist_ok=True)
        if path.resolve() != path or not path.is_dir():
            raise ValueError('owned private worker directory required')
        return path
    if os.name != 'posix' or not hasattr(os, 'O_DIRECTORY'):
        raise ValueError('durable local worker directory required')
    opened = []
    try:
        anchor = Path(path.anchor)
        descriptor = os.open(anchor, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        opened.append((anchor, descriptor))
        current = anchor
        for name in path.parts[1:]:
            current = current / name
            try:
                child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                                dir_fd=descriptor)
            except FileNotFoundError:
                # The descriptor pins the real parent; symlink substitution
                # cannot redirect creation or the subsequent no-follow open.
                os.mkdir(name, 0o700, dir_fd=descriptor)
                child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                                dir_fd=descriptor)
            descriptor = child
            opened.append((current, descriptor))
        root = os.fstat(descriptor)
        if (not stat.S_ISDIR(root.st_mode) or root.st_uid != os.geteuid()
                or root.st_mode & 0o077):
            raise ValueError('owned private worker directory required')
        # Publish bottom-up through every ancestor, including preexisting ones:
        # the factory cannot infer which entries another installer synced.
        for current, fd in reversed(opened):
            before, visible = os.fstat(fd), current.lstat()
            if (not stat.S_ISDIR(visible.st_mode) or visible.st_uid != before.st_uid
                    or (visible.st_dev, visible.st_ino) != (before.st_dev, before.st_ino)):
                raise ValueError('worker directory namespace changed')
            os.fsync(fd)
        # Detect a replacement while another directory's sync was in flight.
        for current, fd in opened:
            observed, visible = os.fstat(fd), current.lstat()
            if (not stat.S_ISDIR(visible.st_mode)
                    or (observed.st_dev, observed.st_ino) != (visible.st_dev, visible.st_ino)):
                raise ValueError('worker directory namespace changed')
            if current == path and (observed.st_uid != os.geteuid() or visible.st_uid != os.geteuid()
                    or observed.st_mode & 0o077 or visible.st_mode & 0o077):
                raise ValueError('worker directory privacy changed')
        if path.resolve() != path:
            raise ValueError('worker directory namespace changed')
        return path
    except (OSError, ValueError):
        # Retain any newly created directories; no uncertain publication is
        # permission to remove data or retry engine admission.
        raise ValueError('durable private worker directory unavailable') from None
    finally:
        for _, fd in reversed(opened):
            os.close(fd)


def sync_worker_lease(descriptor, root):
    """Persist the exact already-locked file and its containing entry."""
    if os.name == 'nt':
        return
    os.fsync(descriptor)
    for path in (root, root.parent):
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
