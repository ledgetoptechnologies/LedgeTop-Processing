"""Prepare a new root-only first-worker installer bundle; never run it.

Run only a separately hash-verified, root-owned copy of this file. All input
digests and the expected service UID must come from a separately reviewed plan.
Unknown partial directories are retained and never reused.
"""
import argparse
import hashlib
import io
import os
from pathlib import Path, PurePosixPath
import stat
import tarfile

LIMIT = 256 * 1024 * 1024
MAX_FILES = 8192


def read_pinned(path, digest, owner):
    if not path.is_absolute() or '..' in path.parts or path == Path('/'):
        raise ValueError('input_path_invalid')
    opened = []
    lineage = []
    signature = lambda s: (s.st_dev, s.st_ino, s.st_mode, s.st_uid, s.st_nlink,
                           s.st_size, s.st_mtime_ns, s.st_ctime_ns)
    try:
        parent = os.open('/', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        opened.append(parent)
        for component in path.parts[1:-1]:
            before_dir = os.stat(component, dir_fd=parent, follow_symlinks=False)
            if not stat.S_ISDIR(before_dir.st_mode):
                raise ValueError('input_path_invalid')
            child = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                            dir_fd=parent)
            opened.append(child)
            if (before_dir.st_dev, before_dir.st_ino) != (os.fstat(child).st_dev, os.fstat(child).st_ino):
                raise ValueError('input_path_changed')
            lineage.append((parent, component, child))
            parent = child
        name = path.parts[-1]
        before = os.stat(name, dir_fd=parent, follow_symlinks=False)
        if (not stat.S_ISREG(before.st_mode) or before.st_uid != owner or before.st_nlink != 1
                or stat.S_IMODE(before.st_mode) != 0o600 or before.st_size > LIMIT):
            raise ValueError('input_metadata_invalid')
        fd = os.open(name, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent)
        opened.append(fd)
        if signature(before) != signature(os.fstat(fd)):
            raise ValueError('input_changed')
        with os.fdopen(fd, 'rb', closefd=False) as stream:
            data = stream.read(LIMIT + 1)
        after = os.fstat(fd)
        current = os.stat(name, dir_fd=parent, follow_symlinks=False)
        if (signature(before) != signature(after) or signature(before) != signature(current)
                or len(data) != before.st_size or hashlib.sha256(data).hexdigest() != digest):
            raise ValueError('input_digest_mismatch')
        for containing, component, child in lineage:
            current_dir = os.stat(component, dir_fd=containing, follow_symlinks=False)
            opened_dir = os.fstat(child)
            if not stat.S_ISDIR(current_dir.st_mode) or (current_dir.st_dev, current_dir.st_ino) != (opened_dir.st_dev, opened_dir.st_ino):
                raise ValueError('input_path_changed')
        return data
    finally:
        for descriptor in reversed(opened):
            os.close(descriptor)


def validate_archive(data):
    names = set()
    total = 0
    with tarfile.open(fileobj=io.BytesIO(data), mode='r:') as archive:
        entries = []
        for entry in archive:
            if len(entries) >= MAX_FILES:
                raise ValueError('archive_count_invalid')
            entries.append(entry)
            if (not entry.name or entry.name.startswith('/') or '\\' in entry.name
                    or any(x in ('', '.', '..') for x in entry.name.rstrip('/').split('/'))
                    or not (entry.isfile() or entry.isdir()) or entry.name.rstrip('/') in names
                    or len(entry.name.encode()) > 512):
                raise ValueError('archive_member_invalid')
            names.add(entry.name.rstrip('/'))
            if entry.isfile():
                total += entry.size
                if entry.size < 0 or entry.size > 64 * 1024 * 1024 or total > LIMIT:
                    raise ValueError('archive_size_invalid')
        if not entries:
            raise ValueError('archive_count_invalid')
        return entries


def unpack(data, target):
    entries = validate_archive(data)
    target.mkdir(mode=0o700)
    with tarfile.open(fileobj=io.BytesIO(data), mode='r:') as archive:
        for entry in entries:
            dest = target.joinpath(*PurePosixPath(entry.name).parts)
            if entry.isdir():
                dest.mkdir(mode=0o700, parents=True, exist_ok=True)
                continue
            dest.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            fd = os.open(dest, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o700 if entry.mode & 0o111 else 0o600)
            try:
                with archive.extractfile(entry) as source, os.fdopen(fd, 'wb', closefd=False) as output:
                    while True:
                        block = source.read(65536)
                        if not block: break
                        output.write(block)
                    output.flush()
                os.fsync(fd)
            finally:
                os.close(fd)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--target', required=True, type=Path)
    p.add_argument('--owner', required=True, type=int)
    for name in ('source', 'dependencies', 'manifest'):
        p.add_argument('--' + name, required=True, type=Path)
        p.add_argument('--' + name + '-sha256', required=True)
    args = p.parse_args()
    if os.name != 'posix' or os.geteuid() != 0:
        raise ValueError('root_required')
    target = args.target
    if (not target.is_absolute() or target.parent != Path('/opt') or target.parent.resolve() != target.parent or target.exists()
            or target.is_symlink() or target.parent.stat().st_uid != 0
            or target.parent.stat().st_mode & 0o022):
        raise ValueError('new_root_target_required')
    data = {name: read_pinned(getattr(args, name), getattr(args, name + '_sha256'), args.owner)
            for name in ('source', 'dependencies', 'manifest')}
    validate_archive(data['source']); validate_archive(data['dependencies'])
    target.mkdir(mode=0o700)
    unpack(data['source'], target / 'source'); unpack(data['dependencies'], target / 'dependencies')
    fd = os.open(target / 'manifest.json', os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(fd, 'wb', closefd=False) as output:
            output.write(data['manifest']); output.flush()
        os.fsync(fd)
    finally: os.close(fd)
    print('root-only installer bundle prepared; no installation or service operation performed')


if __name__ == '__main__':
    main()
