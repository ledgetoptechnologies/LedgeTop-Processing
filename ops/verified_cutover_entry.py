"""Run one controller action only from an independently pinned source archive.

Invoke this file directly with ``python -I``; invoking it as an ``ops`` module
would import unverified package code before the archive is checked.  The caller
must independently verify this small launcher and provide the reviewed archive
SHA-256.  The archive is preparation input, not cutover authorization.
"""
import argparse
import hashlib
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile

MAX_ARCHIVE = 64 * 1024 * 1024
MAX_FILES = 10000
MODULES = {'coordinator': 'ops.enrolled_coordinator_driver',
           'worker-activation': 'ops.first_worker_activation',
           'legacy-observe': 'ops.capture_legacy_delivery'}


def _regular(path):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > MAX_ARCHIVE:
        raise ValueError('regular_file_required')
    return info


def _digest(path):
    with path.open('rb') as stream:
        digest = hashlib.sha256()
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
        return digest.hexdigest()


def verify_and_extract(archive_path, source_path, expected_sha256, destination):
    """Verify the archive and matching source tree, then extract a private copy."""
    archive_path, source_path, destination = map(Path, (archive_path, source_path, destination))
    if not re.fullmatch('[a-f0-9]{64}', expected_sha256):
        raise ValueError('reviewed_archive_digest_required')
    before = _regular(archive_path)
    if _digest(archive_path) != expected_sha256:
        raise ValueError('reviewed_archive_changed')
    if not source_path.is_dir() or source_path.is_symlink() or not destination.is_dir():
        raise ValueError('source_or_destination_invalid')
    expected, total = set(), 0
    with tarfile.open(archive_path, 'r:') as archive:
        for member in archive:
            path = PurePosixPath(member.name)
            if (path.is_absolute() or not path.parts or path.parts[0] != 'source' or
                    any(part in ('.', '..') for part in path.parts) or
                    (not member.isfile() and not member.isdir())):
                raise ValueError('unsafe_archive_member')
            relative = PurePosixPath(*path.parts[1:])
            if not relative.parts:
                if not member.isdir(): raise ValueError('unsafe_archive_root')
                continue
            if str(relative) in expected: raise ValueError('duplicate_archive_member')
            expected.add(str(relative))
            if len(expected) > MAX_FILES: raise ValueError('archive_file_limit')
            source = source_path.joinpath(*relative.parts)
            output = destination.joinpath(*relative.parts)
            if member.isdir():
                if source.is_symlink() or not source.is_dir(): raise ValueError('source_tree_changed')
                output.mkdir(mode=0o700, parents=True, exist_ok=True)
                continue
            total += member.size
            if member.size < 0 or total > MAX_ARCHIVE: raise ValueError('archive_size_limit')
            _regular(source)
            stream = archive.extractfile(member)
            if stream is None: raise ValueError('archive_member_unreadable')
            output.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            with output.open('xb') as target:
                shutil.copyfileobj(stream, target, length=1024 * 1024)
            if output.stat().st_size != member.size or _digest(output) != _digest(source):
                raise ValueError('source_tree_changed')
    for root, dirs, files in os.walk(source_path, followlinks=False):
        for name in dirs + files:
            item = Path(root) / name
            relative = item.relative_to(source_path).as_posix()
            if item.is_symlink() or relative not in expected:
                raise ValueError('source_tree_changed')
    after = _regular(archive_path)
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
            after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
        raise ValueError('reviewed_archive_changed')
    if _digest(archive_path) != expected_sha256:
        raise ValueError('reviewed_archive_changed')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive', required=True)
    parser.add_argument('--source', required=True)
    parser.add_argument('--sha256', required=True)
    parser.add_argument('--controller', required=True, choices=tuple(MODULES))
    parser.add_argument('controller_args', nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    if not args.controller_args or args.controller_args[0] != '--':
        parser.error('controller arguments must follow --')
    try:
        with tempfile.TemporaryDirectory(prefix='ltds-reviewed-controller-') as temp:
            verify_and_extract(args.archive, args.source, args.sha256, temp)
            # -I prevents import-path/environment injection; only the verified
            # private extraction is added to the child's Python search path.
            code = ('import importlib,sys;sys.path.insert(0,sys.argv.pop(1));'
                    'module=importlib.import_module(sys.argv.pop(1));'
                    'raise SystemExit(module.main(sys.argv[1:]))')
            return subprocess.run([sys.executable, '-I', '-c', code, temp,
                                   MODULES[args.controller], *args.controller_args[1:]], check=False).returncode
    except (OSError, ValueError, tarfile.TarError):
        print('Reviewed controller source held; inspect the private inputs.', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
