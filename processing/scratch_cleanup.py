"""Linux-only exact-object cleanup with durable quarantine and ledger fencing.

This executor performs no recursive operations. Its private same-filesystem
quarantine closes the source-name replacement race: a substituted file is retained,
never unlinked. The storage tree and journal must be owned by the worker account;
other processes with that account's privileges remain outside this trust boundary.
SQLite FULL and directory fsync request filesystem durability, not hardware proof.
"""
from contextlib import contextmanager
import ctypes
import errno
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import sys


class CleanupRefused(ValueError):
    """Identity or filesystem safety cannot be established; retain all evidence."""


class CleanupUnsupported(RuntimeError):
    """Required Linux dirfd, no-replace rename, or directory durability unavailable."""


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _identity(info):
    return dict(device=info.st_dev, inode=info.st_ino, bytes=info.st_size,
                mtime=info.st_mtime_ns)


class ScratchCleanupExecutor:
    def __init__(self, ledger):
        self.ledger = ledger
        self.root = Path(ledger.root)

    def _supported(self):
        if not sys.platform.startswith("linux"):
            raise CleanupUnsupported("cleanup requires Linux dirfds and directory fsync")
        needed = {"open", "stat", "unlink", "mkdir"}
        if not needed <= {call.__name__ for call in os.supports_dir_fd} or not hasattr(os, "O_NOFOLLOW"):
            raise CleanupUnsupported("required descriptor-relative operations unavailable")
        libc = ctypes.CDLL(None, use_errno=True)
        try:
            rename = libc.renameat2
        except AttributeError:
            raise CleanupUnsupported("renameat2(RENAME_NOREPLACE) unavailable") from None
        rename.argtypes = (ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint)
        rename.restype = ctypes.c_int
        return rename

    def _safe_directory(self, descriptor):
        info = os.fstat(descriptor)
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o022:
            raise CleanupRefused("directory must be worker-owned without other writers")

    def _directory(self, parent, name):
        descriptor = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
        try:
            self._safe_directory(descriptor)
        except Exception:
            os.close(descriptor)
            raise
        return descriptor

    @contextmanager
    def _root_descriptor(self):
        descriptor = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
        try:
            for component in self.root.parts[1:]:
                next_fd = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=descriptor)
                os.close(descriptor)
                descriptor = next_fd
            self._safe_directory(descriptor)
            yield descriptor
        finally:
            os.close(descriptor)

    @contextmanager
    def _parent_descriptor(self, root_fd, relative_path):
        descriptor = os.dup(root_fd)
        try:
            for component in relative_path.split("/")[:-1]:
                next_fd = self._directory(descriptor, component)
                os.close(descriptor)
                descriptor = next_fd
            yield descriptor
        finally:
            os.close(descriptor)

    def _sync(self, *descriptors):
        try:
            for descriptor in descriptors:
                os.fsync(descriptor)
        except OSError as exc:
            raise CleanupUnsupported("directory fsync failed; cleanup retained for reconciliation") from exc

    @contextmanager
    def _journal(self, root_fd):
        import fcntl
        name = ".cleanup-journal"
        try:
            os.mkdir(name, 0o700, dir_fd=root_fd)
        except FileExistsError:
            pass
        directory = self._directory(root_fd, name)
        lock_fd = None
        db = None
        try:
            if os.fstat(directory).st_mode & 0o077:
                raise CleanupRefused("cleanup journal must be private")
            lock_fd = os.open("executor.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600, dir_fd=directory)
            self._private_file(lock_fd)
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            db_fd = os.open("journal.sqlite", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600, dir_fd=directory)
            try:
                self._private_file(db_fd)
            finally:
                os.close(db_fd)
            db = sqlite3.connect(f"/proc/self/fd/{directory}/journal.sqlite", isolation_level=None, timeout=30)
            db.execute("PRAGMA synchronous=FULL")
            db.execute("CREATE TABLE IF NOT EXISTS deletions (token TEXT PRIMARY KEY,binding TEXT NOT NULL,verified TEXT NOT NULL,phase TEXT NOT NULL)")
            self._sync(root_fd, directory)
            yield directory, db
        finally:
            if db is not None:
                db.close()
            if lock_fd is not None:
                os.close(lock_fd)
            os.close(directory)

    def _private_file(self, descriptor):
        info = os.fstat(descriptor)
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.geteuid()
                or info.st_mode & 0o077):
            raise CleanupRefused("journal file must be private, regular, and unlinked elsewhere")

    def _load(self, db, token):
        return db.execute("SELECT binding,verified,phase FROM deletions WHERE token=?", (token,)).fetchone()

    def _persist(self, db, directory, token, binding, verified, phase):
        db.execute("BEGIN IMMEDIATE")
        try:
            old = self._load(db, token)
            if old and (old[0] != _json(binding) or old[1] != _json(verified)):
                raise CleanupRefused("journal immutable identity changed")
            db.execute("INSERT INTO deletions VALUES(?,?,?,?) ON CONFLICT(token) DO UPDATE SET phase=excluded.phase",
                       (token, _json(binding), _json(verified), phase))
            db.commit()
        except Exception:
            db.rollback()
            raise
        self._sync(directory)

    def _open_verified(self, directory, name, binding, expected=None):
        descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        try:
            before = os.fstat(descriptor)
            if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_size != binding["bytes"]:
                raise CleanupRefused("cleanup requires the exact single-link regular file size")
            digest = hashlib.sha256()
            while chunk := os.read(descriptor, 1024 * 1024):
                digest.update(chunk)
            after = os.fstat(descriptor)
            identity = _identity(after)
            if (_identity(before) != identity or before.st_ctime_ns != after.st_ctime_ns
                    or after.st_nlink != 1 or digest.hexdigest() != binding["sha256"]
                    or (expected is not None and identity != expected)):
                raise CleanupRefused("file identity or digest changed")
            return descriptor, identity
        except Exception:
            os.close(descriptor)
            raise

    def execute(self, reservation_id, object_id, *, generation, sha256, byte_size,
                relative_path, intent_token=None):
        """Fence one exact accepted object, verify, quarantine, unlink, fsync, ACK.

        Supply intent_token to resume an existing fence. Missing source alone never
        proves cleanup: reconciliation requires a matching durable verified fact
        and an unlinking/removed journal phase. No data or journal records expire.
        """
        return self._execute(None, reservation_id, object_id, generation=generation, sha256=sha256,
            byte_size=byte_size, relative_path=relative_path, intent_token=intent_token)

    def execute_input_copy(self, reservation_id, object_id, *, generation, sha256, byte_size,
                           relative_path, intent_token=None):
        """The same exact procedure for one released processing-input copy.

        Its ledger fence and journal binding use the separate input-copy namespace,
        so an input and a result object with the same ID can never share an intent.
        """
        return self._execute("input-copy", reservation_id, object_id, generation=generation, sha256=sha256,
            byte_size=byte_size, relative_path=relative_path, intent_token=intent_token)

    def _execute(self, namespace, reservation_id, object_id, *, generation, sha256, byte_size,
                 relative_path, intent_token):
        rename = self._supported()  # Fail closed on Windows before creating a fence.
        self.ledger._owned_path(reservation_id, relative_path)
        inputs = namespace == "input-copy"
        begin = self.ledger.begin_input_cleanup if inputs else self.ledger.begin_cleanup
        recover = self.ledger.input_cleanup_intent if inputs else self.ledger.cleanup_intent
        confirm = self.ledger.mark_input_cleaned if inputs else self.ledger.mark_cleaned
        if intent_token is None:
            intent = begin(reservation_id, object_id, generation=generation, sha256=sha256)
            intent_token = intent["intent_token"]
        else:
            intent = recover(reservation_id, object_id)
        if (type(generation) is not int or type(byte_size) is not int
                or (intent["generation"], intent["sha"], intent["bytes"], intent["path"], intent["intent_token"])
                != (generation, sha256, byte_size, relative_path, intent_token)):
            raise CleanupRefused("cleanup request does not match durable intent")
        binding = dict(reservationId=reservation_id, objectId=object_id, generation=generation,
                       sha256=sha256, bytes=byte_size, path=relative_path, intentToken=intent_token)
        # Journal rows are keyed by token. Input rows use their own durable key
        # namespace; result keys and bindings stay byte-identical.
        key = intent_token
        if inputs:
            binding["namespace"] = namespace
            key = namespace + ":" + intent_token
        quarantine = intent_token + (".input" if inputs else ".object")
        with self._root_descriptor() as root_fd, self._parent_descriptor(root_fd, relative_path) as parent_fd:
            with self._journal(root_fd) as (journal_fd, db):
                self._sync(parent_fd, journal_fd)  # Capability preflight before any rename/unlink.
                binding["rootIdentity"] = dict(device=os.fstat(root_fd).st_dev, inode=os.fstat(root_fd).st_ino)
                binding["parentIdentity"] = dict(device=os.fstat(parent_fd).st_dev, inode=os.fstat(parent_fd).st_ino)
                entry = self._load(db, key)
                if entry and entry[0] != _json(binding):
                    raise CleanupRefused("journal binding mismatch")
                verified = json.loads(entry[1]) if entry else None
                phase = entry[2] if entry else None
                if entry and (not isinstance(verified, dict)
                              or set(verified) != {"device", "inode", "bytes", "mtime"}
                              or any(type(v) is not int for v in verified.values())
                              or verified["device"] < 0 or verified["inode"] <= 0
                              or verified["bytes"] != byte_size):
                    raise CleanupRefused("journal lacks a valid formerly verified file identity")
                if phase not in (None, "verified", "quarantined", "unlinking", "removed"):
                    raise CleanupRefused("unknown journal phase")
                try:
                    quarantined_fd, identity = self._open_verified(journal_fd, quarantine, binding, verified)
                except FileNotFoundError:
                    quarantined_fd = None
                else:
                    os.close(quarantined_fd)
                    if not entry:
                        raise CleanupRefused("quarantined file lacks durable verification")
                if quarantined_fd is None and phase not in ("unlinking", "removed"):
                    source_fd, identity = self._open_verified(parent_fd, relative_path.split("/")[-1], binding, verified)
                    try:
                        if os.fstat(source_fd).st_dev != os.fstat(journal_fd).st_dev:
                            raise CleanupRefused("quarantine must share source filesystem")
                        verified = identity
                        self._persist(db, journal_fd, key, binding, verified, "verified")
                        if rename(parent_fd, os.fsencode(relative_path.split("/")[-1]), journal_fd, os.fsencode(quarantine), 1):
                            code = ctypes.get_errno()
                            if code in (errno.ENOSYS, errno.EINVAL, errno.EOPNOTSUPP):
                                raise CleanupUnsupported("no-replace quarantine rename unsupported")
                            raise OSError(code, os.strerror(code))
                        # A source-name swap can move an unrelated file, but cannot
                        # cause its deletion: compare the actual quarantined inode.
                        check_fd, _ = self._open_verified(journal_fd, quarantine, binding, verified)
                        os.close(check_fd)
                        self._sync(parent_fd, journal_fd)
                        self._persist(db, journal_fd, key, binding, verified, "quarantined")
                    finally:
                        os.close(source_fd)
                    quarantined_fd = True
                if quarantined_fd is not None:
                    self._persist(db, journal_fd, key, binding, verified, "unlinking")
                    os.unlink(quarantine, dir_fd=journal_fd)
                elif phase not in ("unlinking", "removed"):
                    raise CleanupRefused("missing file without verified deletion phase")
                self._sync(parent_fd, journal_fd)
                self._persist(db, journal_fd, key, binding, verified, "removed")
                confirm(reservation_id, object_id, generation=generation,
                    sha256=sha256, intent_token=intent_token, executor_attested=True)
        return dict(reservationId=reservation_id, objectId=object_id, intentToken=intent_token, state="cleaned")
