"""Crash-safe writes to an explicitly mounted, private source-spool volume.

The spool never creates or repairs its root or mount marker. A missing or
changed mount fails closed, and source filenames are never used as paths.
Database receipts are written by ReferenceStore only after this module has
fsynced and atomically published the complete, digest-verified object.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import re
import shutil
import stat
import threading

from . import reference_protocol as protocol


class SourceSpoolError(ValueError):
    """A bounded safe error suitable for mapping to a protocol response."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


class SourceSpoolCloseUncertain(SourceSpoolError):
    """An owned close did not prove descriptor retirement; never retry its FD."""
    def __init__(self):
        super().__init__('spool_descriptor_close_uncertain')


def _close_owned_fd(fd):
    try:
        os.close(fd)
    except OSError:
        raise SourceSpoolCloseUncertain() from None


class SourceSpool:
    def __init__(self, root: str | os.PathLike, *, mount_id: str,
                 mount_path: str | os.PathLike | None = None,
                 marker_name: str = ".ltds-source-spool-id",
                 safety_floor_bytes: int = 10 * 1024**3,
                 max_object_bytes: int = 20 * 1024**3,
                 chunk_bytes: int = 1024**2,
                 require_mount: bool = True):
        if not isinstance(root, (str, os.PathLike)):
            raise SourceSpoolError("invalid_spool_root")
        candidate = Path(root)
        if (not candidate.is_absolute() or candidate != Path(os.path.normpath(candidate))
                or candidate.resolve(strict=True) != candidate):
            raise SourceSpoolError("invalid_spool_root")
        mounted = Path(mount_path) if mount_path is not None else candidate
        if (not mounted.is_absolute() or mounted != Path(os.path.normpath(mounted))
                or mounted.resolve(strict=True) != mounted or candidate != mounted and mounted not in candidate.parents):
            raise SourceSpoolError("invalid_spool_mount_path")
        if type(mount_id) is not str or not re.fullmatch(r"[a-f0-9]{32,64}", mount_id):
            raise SourceSpoolError("invalid_spool_mount_id")
        if (type(marker_name) is not str or not re.fullmatch(r"\.[a-z0-9][a-z0-9.-]{0,63}", marker_name)):
            raise SourceSpoolError("invalid_spool_marker")
        protocol._integer(safety_floor_bytes, "source spool safety floor", allow_zero=True)
        for value in (max_object_bytes, chunk_bytes):
            protocol._integer(value, "source spool limit")
        if safety_floor_bytes < 0 or max_object_bytes < 1 or chunk_bytes > 16 * 1024**2:
            raise SourceSpoolError("invalid_spool_limits")
        if type(require_mount) is not bool:
            raise SourceSpoolError("invalid_mount_policy")
        self.root = candidate
        self.mount_path = mounted
        self.mount_id = mount_id
        self.marker = mounted / marker_name
        self.safety_floor_bytes = safety_floor_bytes
        self.max_object_bytes = max_object_bytes
        self.chunk_bytes = chunk_bytes
        self.require_mount = require_mount
        self._object_locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()
        self._root_identity, self._mount_identity, self._marker_identity = self._check_mount(capture=True)

    @staticmethod
    def _identity(path: Path, *, directory: bool, owner_required=True, private=True):
        try:
            info = path.lstat()
        except OSError:
            raise SourceSpoolError("spool_mount_unavailable") from None
        if stat.S_ISLNK(info.st_mode) or (directory and not stat.S_ISDIR(info.st_mode)):
            raise SourceSpoolError("spool_mount_unavailable")
        if not directory and (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1):
            raise SourceSpoolError("spool_marker_invalid")
        if os.name != "nt":
            if owner_required and info.st_uid != os.geteuid():
                raise SourceSpoolError("spool_owner_mismatch")
            mode = stat.S_IMODE(info.st_mode)
            if private and mode & 0o077:
                raise SourceSpoolError("spool_permissions_invalid")
        return info

    def _check_mount(self, *, capture=False):
        root_info = self._identity(self.root, directory=True)
        mount_info = self._identity(self.mount_path, directory=True, owner_required=False, private=False)
        marker_info = self._identity(self.marker, directory=False)
        if self.require_mount and not os.path.ismount(self.mount_path):
            raise SourceSpoolError("spool_mount_unavailable")
        if root_info.st_dev != marker_info.st_dev or mount_info.st_dev != marker_info.st_dev:
            raise SourceSpoolError("spool_mount_changed")
        try:
            fd = os.open(self.marker, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
            try:
                observed = os.fstat(fd)
                value = os.read(fd, 129)
            finally:
                _close_owned_fd(fd)
        except OSError:
            raise SourceSpoolError("spool_marker_invalid") from None
        if ((observed.st_dev, observed.st_ino) != (marker_info.st_dev, marker_info.st_ino)
                or value != self.mount_id.encode("ascii")):
            raise SourceSpoolError("spool_mount_changed")
        root_identity = (root_info.st_dev, root_info.st_ino)
        mount_identity = (mount_info.st_dev, mount_info.st_ino)
        marker_identity = (marker_info.st_dev, marker_info.st_ino)
        if not capture and (root_identity != self._root_identity
                            or mount_identity != self._mount_identity
                            or marker_identity != self._marker_identity):
            raise SourceSpoolError("spool_mount_changed")
        return root_identity, mount_identity, marker_identity

    def _assert_mount(self):
        self._check_mount()

    def available_bytes(self) -> int:
        self._assert_mount()
        try:
            return shutil.disk_usage(self.root).free
        except OSError:
            raise SourceSpoolError("spool_mount_unavailable") from None

    def check_capacity(self, required_bytes: int, *, reserved_bytes: int = 0):
        protocol._integer(required_bytes, "source reservation", allow_zero=True)
        protocol._integer(reserved_bytes, "existing source reservations", allow_zero=True)
        available = self.available_bytes()
        if available - self.safety_floor_bytes - reserved_bytes < required_bytes:
            raise SourceSpoolError("spool_capacity_unavailable")

    @staticmethod
    def storage_key(job_id: str, generation: int, object_id: str) -> str:
        return SourceSpool._key(job_id, generation, object_id)

    @staticmethod
    def _key(job_id: str, generation: int, object_id: str) -> str:
        protocol._slug(job_id, "job ID")
        protocol._integer(generation, "generation")
        protocol._slug(object_id, "source object ID")
        return f"{job_id}/{generation}/{hashlib.sha256(object_id.encode('ascii')).hexdigest()}.blob"

    def _directory(self, job_id: str, generation: int) -> Path:
        base = self.root / job_id
        for path in (base, base / str(generation)):
            created = False
            try:
                path.mkdir(mode=0o700)
                created = True
            except FileExistsError:
                pass
            except OSError:
                raise SourceSpoolError("spool_directory_unavailable") from None
            info = self._identity(path, directory=True)
            if info.st_dev != self._mount_identity[0]:
                raise SourceSpoolError("spool_mount_changed")
            if created:
                self._fsync_directory(path.parent)
        return base / str(generation)

    def _lock_for(self, storage_key: str) -> threading.Lock:
        with self._locks_guard:
            return self._object_locks.setdefault(storage_key, threading.Lock())

    def _hash_file(self, path: Path, expected_size: int, expected_sha256: str,
                   *, expected_links: int = 1) -> bool:
        try:
            fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
                         | getattr(os, "O_BINARY", 0))
            try:
                info = os.fstat(fd)
                if (not stat.S_ISREG(info.st_mode) or info.st_nlink != expected_links
                        or (os.name != "nt" and info.st_uid != os.geteuid())
                        or info.st_size != expected_size):
                    return False
                digest = hashlib.sha256()
                count = 0
                while True:
                    chunk = os.read(fd, self.chunk_bytes)
                    if not chunk:
                        break
                    count += len(chunk)
                    if count > expected_size:
                        return False
                    digest.update(chunk)
                after = os.fstat(fd)
                fields = ('st_dev', 'st_ino', 'st_size', 'st_mode', 'st_nlink',
                          'st_uid', 'st_mtime_ns', 'st_ctime_ns')
                return (count == expected_size and digest.hexdigest() == expected_sha256
                    and all(getattr(info, field) == getattr(after, field) for field in fields))
            finally:
                _close_owned_fd(fd)
        except OSError:
            return False

    def _recover_published_partial(self, directory: Path, target: Path,
                                   temporary: Path, byte_size: int,
                                   sha256: str) -> None:
        """Remove only a proven second name from our interrupted publication link."""
        try:
            target_before, partial_before = target.lstat(), temporary.lstat()
        except OSError:
            raise SourceSpoolError("source_object_conflict") from None
        same_identity = (target_before.st_dev, target_before.st_ino) == (
            partial_before.st_dev, partial_before.st_ino)
        if (not stat.S_ISREG(target_before.st_mode)
                or not stat.S_ISREG(partial_before.st_mode)
                or not same_identity
                or target_before.st_nlink != 2 or partial_before.st_nlink != 2
                or target_before.st_dev != self._mount_identity[0]
                or target_before.st_size != byte_size or partial_before.st_size != byte_size
                or (os.name != "nt" and target_before.st_uid != os.geteuid())):
            raise SourceSpoolError("source_object_conflict")
        if not self._hash_file(target, byte_size, sha256, expected_links=2):
            raise SourceSpoolError("source_object_conflict")
        try:
            target_after, partial_after = target.lstat(), temporary.lstat()
        except OSError:
            raise SourceSpoolError("source_object_conflict") from None
        if ((target_after.st_dev, target_after.st_ino, target_after.st_nlink)
                != (target_before.st_dev, target_before.st_ino, 2)
                or (partial_after.st_dev, partial_after.st_ino, partial_after.st_nlink)
                != (target_before.st_dev, target_before.st_ino, 2)):
            raise SourceSpoolError("source_object_conflict")
        try:
            temporary.unlink()
            self._fsync_directory(directory)
        except OSError:
            raise SourceSpoolError("spool_write_failed") from None
        if not self._hash_file(target, byte_size, sha256):
            raise SourceSpoolError("source_object_conflict")

    def verify_object(self, *, job_id: str, generation: int, object_id: str,
                      byte_size: int, sha256: str) -> dict:
        protocol._integer(byte_size, "source object size", allow_zero=True)
        protocol._hash(sha256, "source object digest")
        self._assert_mount()
        storage_key = self._key(job_id, generation, object_id)
        target = self.root / storage_key
        if self._directory(job_id, generation) != target.parent:
            raise SourceSpoolError("spool_directory_unavailable")
        if not self._hash_file(target, byte_size, sha256):
            raise SourceSpoolError("source_object_receipt_mismatch")
        return {"schemaVersion": 1, "jobId": job_id, "generation": generation,
                "objectId": object_id, "storageKey": storage_key,
                "byteSize": byte_size, "sha256": sha256, "mountId": self.mount_id}

    def open_verified_object(self, *, job_id: str, generation: int, object_id: str,
                             byte_size: int, sha256: str):
        """Open and hash-check a pinned spool inode before serving it to a worker."""
        protocol._integer(byte_size, "source object size", allow_zero=True)
        protocol._hash(sha256, "source object digest")
        self._assert_mount()
        storage_key = self._key(job_id, generation, object_id)
        target = self.root / storage_key
        if self._directory(job_id, generation) != target.parent:
            raise SourceSpoolError("spool_directory_unavailable")
        expected = {"schemaVersion": 1, "jobId": job_id, "generation": generation,
                    "objectId": object_id, "storageKey": storage_key,
                    "byteSize": byte_size, "sha256": sha256, "mountId": self.mount_id}
        try:
            fd = os.open(target, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
                         | getattr(os, "O_BINARY", 0))
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                    or (os.name != "nt" and info.st_uid != os.geteuid())
                    or info.st_size != byte_size or info.st_dev != self._mount_identity[0]):
                raise SourceSpoolError("source_object_receipt_mismatch")
            digest = hashlib.sha256()
            count = 0
            while True:
                chunk = os.read(fd, self.chunk_bytes)
                if not chunk:
                    break
                count += len(chunk)
                if count > byte_size:
                    raise SourceSpoolError("source_object_receipt_mismatch")
                digest.update(chunk)
            if count != byte_size or digest.hexdigest() != sha256:
                raise SourceSpoolError("source_object_receipt_mismatch")
            os.lseek(fd, 0, os.SEEK_SET)
            return os.fdopen(fd, "rb"), expected
        except SourceSpoolError:
            if "fd" in locals():
                _close_owned_fd(fd)
            raise
        except OSError:
            if "fd" in locals():
                _close_owned_fd(fd)
            raise SourceSpoolError("source_object_receipt_mismatch") from None

    def audit_root(self, expected_job_ids):
        """Refuse unknown root entries; never clean them automatically."""
        self._assert_mount()
        expected = set(expected_job_ids)
        for name in expected:
            protocol._slug(name, "job ID")
        try:
            entries = list(self.root.iterdir())
        except OSError:
            raise SourceSpoolError("spool_mount_unavailable") from None
        for entry in entries:
            if self.marker.parent == self.root and entry == self.marker:
                continue
            if entry.name not in expected:
                raise SourceSpoolError("spool_unknown_entry")
            info = self._identity(entry, directory=True)
            if info.st_dev != self._mount_identity[0]:
                raise SourceSpoolError("spool_mount_changed")

    def audit_job(self, *, job_id, generation, expected_objects):
        """Verify closed manifest files; retain partials pending writer reconciliation."""
        self._assert_mount()
        protocol._slug(job_id, "job ID")
        protocol._integer(generation, "generation")
        if type(expected_objects) is not list:
            raise SourceSpoolError("invalid_source_manifest")
        expected = {}
        for obj in expected_objects:
            if type(obj) is not dict or set(obj) != {"id", "byteSize", "sha256"}:
                raise SourceSpoolError("invalid_source_manifest")
            protocol._slug(obj["id"], "source object ID")
            protocol._integer(obj["byteSize"], "source object size", allow_zero=True)
            protocol._hash(obj["sha256"], "source object digest")
            if obj["id"] in expected:
                raise SourceSpoolError("invalid_source_manifest")
            expected[obj["id"]] = obj
        job_dir = self.root / job_id
        generation_dir = job_dir / str(generation)
        try:
            job_info = job_dir.lstat()
        except FileNotFoundError:
            return {}
        job_info = self._identity(job_dir, directory=True)
        if job_info.st_dev != self._mount_identity[0]:
            raise SourceSpoolError("spool_mount_changed")
        try:
            generation_info = generation_dir.lstat()
        except FileNotFoundError:
            return {}
        generation_info = self._identity(generation_dir, directory=True)
        if generation_info.st_dev != self._mount_identity[0]:
            raise SourceSpoolError("spool_mount_changed")
        known = {}
        for object_id in expected:
            key = Path(self._key(job_id, generation, object_id)).name
            known[key] = object_id
            known["." + key + ".part"] = object_id
        found = {}
        # An audit cannot establish that another process has stopped writing.
        # Even a valid hard-link pair may be an in-progress publication.
        for object_id, obj in expected.items():
            key = Path(self._key(job_id, generation, object_id)).name
            target, temporary = generation_dir / key, generation_dir / ("." + key + ".part")
            if (target.exists() or target.is_symlink()) and (temporary.exists() or temporary.is_symlink()):
                raise SourceSpoolError("source_object_conflict")
        try:
            entries = list(generation_dir.iterdir())
        except OSError:
            raise SourceSpoolError("spool_directory_unavailable") from None
        for entry in entries:
            if entry.name not in known:
                raise SourceSpoolError("spool_unknown_entry")
            object_id = known[entry.name]
            if entry.name.startswith(".") and entry.name.endswith(".part"):
                info = self._identity(entry, directory=False)
                if info.st_dev != self._mount_identity[0]:
                    raise SourceSpoolError("spool_mount_changed")
                raise SourceSpoolError("source_object_conflict")
            obj = expected[object_id]
            found[object_id] = self.verify_object(job_id=job_id, generation=generation,
                object_id=object_id, byte_size=obj["byteSize"], sha256=obj["sha256"])
            # A reverified inode is not durable publication evidence until the
            # directory entry itself crosses the filesystem sync barrier.
            self._fsync_directory(generation_dir)
        return found

    def receive_object(self, *, job_id: str, generation: int, object_id: str,
                       byte_size: int, sha256: str, reader,
                       reserved_bytes: int = 0) -> tuple[dict, bool]:
        """Stream exactly one manifest object; return its durable receipt and replay flag."""
        protocol._integer(byte_size, "source object size", allow_zero=True)
        protocol._hash(sha256, "source object digest")
        if byte_size > self.max_object_bytes:
            raise SourceSpoolError("source_object_too_large")
        if not hasattr(reader, "read"):
            raise SourceSpoolError("invalid_source_stream")
        storage_key = self._key(job_id, generation, object_id)
        lock = self._lock_for(storage_key)
        with lock:
            self._assert_mount()
            try:
                target = self.root / storage_key
                target.parent.parent.mkdir(mode=0o700, exist_ok=True)
                directory = self._directory(job_id, generation)
            except SourceSpoolCloseUncertain:
                raise
            except SourceSpoolError:
                raise
            except OSError:
                raise SourceSpoolError("spool_directory_unavailable") from None
            if target.exists() or target.is_symlink():
                temporary = target.parent / ("." + target.name + ".part")
                if temporary.exists() or temporary.is_symlink():
                    self._recover_published_partial(target.parent, target, temporary,
                        byte_size, sha256)
                if self._hash_file(target, byte_size, sha256):
                    # Never turn a prior failed directory sync into an ACK just
                    # because the file contents survived a process restart.
                    self._fsync_directory(directory)
                    self._remove_owned_partial(directory / ("." + target.name + ".part"))
                    return self.verify_object(job_id=job_id, generation=generation,
                        object_id=object_id, byte_size=byte_size, sha256=sha256), True
                raise SourceSpoolError("source_object_conflict")
            temporary = directory / ("." + target.name + ".part")
            try:
                existing = temporary.lstat()
            except FileNotFoundError:
                existing = None
            if existing is not None:
                info = self._identity(temporary, directory=False)
                if info.st_dev != self._mount_identity[0]:
                    raise SourceSpoolError("spool_mount_changed")
                temporary.unlink()
                self._fsync_directory(directory)
            self.check_capacity(byte_size, reserved_bytes=reserved_bytes)
            created = False
            fd = None
            try:
                flags = (os.O_WRONLY | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
                         | getattr(os, "O_BINARY", 0))
                flags |= os.O_EXCL
                fd = os.open(temporary, flags, 0o600)
                created = True
                info = os.fstat(fd)
                if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                        or (os.name != "nt" and info.st_uid != os.geteuid())):
                    raise SourceSpoolError("spool_temporary_file_invalid")
                digest = hashlib.sha256()
                remaining = byte_size
                while remaining:
                    chunk = reader.read(min(remaining, self.chunk_bytes))
                    if type(chunk) is not bytes or not chunk:
                        raise SourceSpoolError("source_stream_truncated")
                    if len(chunk) > remaining:
                        raise SourceSpoolError("source_stream_invalid")
                    view = memoryview(chunk)
                    while view:
                        written = os.write(fd, view)
                        if written <= 0:
                            raise OSError("short file write")
                        view = view[written:]
                    digest.update(chunk)
                    remaining -= len(chunk)
                if digest.hexdigest() != sha256:
                    raise SourceSpoolError("source_digest_mismatch")
                os.fsync(fd)
                _close_owned_fd(fd)
                fd = None
                self._assert_mount()
                try:
                    os.link(temporary, target, follow_symlinks=False)
                except FileExistsError:
                    if not self._hash_file(target, byte_size, sha256):
                        raise SourceSpoolError("source_object_conflict") from None
                os.unlink(temporary)
                self._fsync_directory(directory)
                receipt = self.verify_object(job_id=job_id, generation=generation,
                    object_id=object_id, byte_size=byte_size, sha256=sha256)
                return receipt, False
            except SourceSpoolCloseUncertain:
                raise
            except SourceSpoolError:
                if fd is not None:
                    _close_owned_fd(fd)
                if created:
                    self._remove_owned_partial(temporary)
                raise
            except OSError:
                if fd is not None:
                    _close_owned_fd(fd)
                if created:
                    self._remove_owned_partial(temporary)
                raise SourceSpoolError("spool_write_failed") from None
            except Exception:
                if fd is not None:
                    _close_owned_fd(fd)
                if created:
                    self._remove_owned_partial(temporary)
                raise SourceSpoolError("source_stream_failed") from None

    @staticmethod
    def _fsync_directory(path: Path):
        if os.name == "nt":
            return
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
                     | getattr(os, "O_NOFOLLOW", 0))
        try:
            os.fsync(fd)
        finally:
            _close_owned_fd(fd)

    def _remove_owned_partial(self, path: Path):
        try:
            info = self._identity(path, directory=False)
            if info.st_dev == self._mount_identity[0]:
                path.unlink()
                self._fsync_directory(path.parent)
        except FileNotFoundError:
            pass
        except SourceSpoolCloseUncertain:
            raise
        except (OSError, SourceSpoolError):
            # An uncertain partial is left in place; cleanup cannot authorize
            # an object receipt or delete an unexpected filesystem entry.
            return
