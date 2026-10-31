"""Linux same-inode output allocation primitive; not an admission or custody API.

Caller supplies a separately trusted private root identity. A filesystem's
successful fallocate and st_blocks do not attest its physical backing, power
loss behaviour, quota policy, or exclusion of privileged/uncooperative writers.
"""
import fcntl
import json
import os
from pathlib import Path
import re
import stat
import threading

from . import reference_protocol as p


class SlotHeld(RuntimeError):
    pass


def _mount_id(fd):
    try:
        with open('/proc/self/fdinfo/' + str(fd), encoding='ascii') as stream:
            fields = [line.split(':', 1)[1].strip() for line in stream if line.startswith('mnt_id:')]
        if len(fields) != 1 or not fields[0].isdecimal(): raise ValueError()
        return int(fields[0])
    except (OSError, ValueError, IndexError):
        raise SlotHeld('mount_identity_unavailable') from None


def _identity(fd):
    info = os.fstat(fd)
    return [info.st_dev, info.st_ino, _mount_id(fd)]


def _file(fd, root_identity, *, size=None):
    info = os.fstat(fd)
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1
            or info.st_dev != root_identity[0] or _mount_id(fd) != root_identity[2]):
        raise SlotHeld('slot_file_identity_invalid')
    if size is not None and (info.st_size != size or info.st_blocks * 512 < size):
        raise SlotHeld('slot_allocation_unverified')
    return info


class OutputSlot:
    """Exclusive, bounded pwrite access to one existing allocated inode."""
    def __init__(self, root_path, root_fd, file_fd, meta_fd, root_identity, file_identity, name, meta_name, metadata, budget):
        self._root_path = Path(root_path)
        self._root, self._fd = root_fd, file_fd
        self._meta, self._meta_name, self._metadata = meta_fd, meta_name, metadata
        self._meta_identity = _identity(meta_fd)
        self._root_identity, self._file_identity = root_identity, file_identity
        self._name, self.budget, self._pid = name, budget, os.getpid()
        self._operation_lock = threading.RLock()
        self._held = False

    def _check(self):
        if self._fd is None or os.getpid() != self._pid: raise SlotHeld('slot_not_owned')
        if self._held: raise SlotHeld('slot_write_requires_inspection')
        if _identity(self._root) != self._root_identity: raise SlotHeld('root_identity_changed')
        root_info = os.fstat(self._root)
        if root_info.st_uid != os.geteuid() or stat.S_IMODE(root_info.st_mode) != 0o700:
            raise SlotHeld('root_identity_changed')
        # Reopen the name, rather than comparing only st_dev/st_ino: a bind
        # mount of the same inode can have a different Linux mount identity.
        named_root = _open_root(self._root_path, self._root_identity)
        os.close(named_root)
        info = _file(self._fd, self._root_identity, size=self.budget)
        meta = _file(self._meta,self._root_identity)
        try:
            named = os.stat(self._name, dir_fd=self._root, follow_symlinks=False)
            named_meta = os.stat(self._meta_name, dir_fd=self._root, follow_symlinks=False)
        except OSError: raise SlotHeld('slot_path_missing') from None
        if ([info.st_dev, info.st_ino, _mount_id(self._fd)] != self._file_identity
                or (named.st_dev, named.st_ino) != (info.st_dev, info.st_ino)
                or _identity(self._meta)!=self._meta_identity
                or (named_meta.st_dev,named_meta.st_ino)!=(meta.st_dev,meta.st_ino)
                or meta.st_size!=len(self._metadata)
                or os.pread(self._meta,len(self._metadata)+1,0)!=self._metadata):
            raise SlotHeld('slot_path_changed')

    def write_at(self, offset, data):
        if os.getpid() != self._pid: raise SlotHeld('slot_not_owned')
        if (type(offset) is not int or offset < 0 or type(data) is not bytes
                or offset + len(data) > self.budget): raise SlotHeld('slot_write_out_of_bounds')
        with self._operation_lock:
            self._check()
            try:
                written = os.pwrite(self._fd, data, offset)
                if written != len(data): raise SlotHeld('slot_write_incomplete')
                self._check()
            except OSError:
                self._held = True
                raise SlotHeld('slot_write_uncertain') from None
            except BaseException:
                self._held = True
                raise

    def sync(self):
        if os.getpid() != self._pid: raise SlotHeld('slot_not_owned')
        with self._operation_lock:
            self._check()
            try:
                os.fsync(self._fd); self._check()
            except OSError:
                self._held = True
                raise SlotHeld('slot_sync_uncertain') from None
            except BaseException:
                self._held = True
                raise

    def close(self):
        if os.getpid() != self._pid: raise SlotHeld('slot_not_owned')
        with self._operation_lock:
            if self._fd is not None:
                os.close(self._fd); self._fd = None
                os.close(self._meta); self._meta = None
                os.close(self._root); self._root = None


def _open_root(path, expected):
    if os.name != 'posix' or not hasattr(os, 'posix_fallocate'):
        raise SlotHeld('linux_allocation_required')
    path = Path(path)
    if not path.is_absolute() or path.resolve() != path: raise SlotHeld('root_path_invalid')
    if (type(expected) is not list or len(expected) != 3
            or any(type(x) is not int or x < 0 for x in expected)): raise SlotHeld('root_binding_invalid')
    try:
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    except OSError: raise SlotHeld('root_unavailable') from None
    try:
        info = os.fstat(fd)
        if (info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700
                or _identity(fd) != expected): raise SlotHeld('root_identity_changed')
        current = os.stat(path, follow_symlinks=False)
        if (info.st_dev,info.st_ino) != (current.st_dev,current.st_ino): raise SlotHeld('root_identity_changed')
        return fd
    except BaseException:
        os.close(fd); raise


def open_slot(root, expected_root, binding_digest, budget, *, max_budget, create=False):
    """Allocate or reopen a slot; never overwrite, truncate, unlink or clean up.

    Existing incomplete/ambiguous files are held. A complete metadata record
    is revalidated and re-fsynced before reuse, including after a lost response.
    This operation proves only local filesystem allocation under its documented
    trust assumptions; it does not authorize dispatch.
    """
    if type(binding_digest) is not str or not re.fullmatch('[a-f0-9]{64}', binding_digest):
        raise SlotHeld('binding_digest_invalid')
    if (type(max_budget) is not int or not 1 <= max_budget <= p.MAX_SAFE_INTEGER
            or type(budget) is not int or not 1 <= budget <= max_budget or type(create) is not bool):
        raise SlotHeld('slot_budget_invalid')
    expected_root = list(expected_root) if type(expected_root) is list else expected_root
    root_fd = _open_root(root, expected_root); fd = None; meta_fd = None
    name = 'slot-' + binding_digest + '.alloc'; meta_name = 'slot-' + binding_digest + '.json'
    flags = os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK
    try:
        if create:
            try: fd = os.open(name, flags | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=root_fd)
            except OSError: raise SlotHeld('slot_already_exists_or_unavailable') from None
            os.fsync(root_fd)
        else:
            try: fd = os.open(name, flags, dir_fd=root_fd)
            except OSError: raise SlotHeld('slot_unavailable') from None
        _file(fd, expected_root)
        try: fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError: raise SlotHeld('slot_busy') from None
        if create:
            os.posix_fallocate(fd, 0, budget)
            _file(fd, expected_root, size=budget); os.fsync(fd)
            record = dict(schemaVersion=1,bindingSha256=binding_digest,budget=budget,
                          root=expected_root,file=_identity(fd))
            data = p.canonical_json(record)
            try: meta_fd = os.open(meta_name, flags | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=root_fd)
            except OSError: raise SlotHeld('slot_metadata_unavailable') from None
            if os.write(meta_fd, data) != len(data): raise SlotHeld('slot_metadata_incomplete')
            os.fsync(meta_fd); os.fsync(root_fd)
        else:
            try: meta_fd = os.open(meta_name, flags, dir_fd=root_fd)
            except OSError: raise SlotHeld('slot_metadata_unavailable') from None
            info = _file(meta_fd, expected_root)
            if info.st_size > 4096: raise SlotHeld('slot_metadata_invalid')
            data = os.read(meta_fd, 4097)
            try: record = json.loads(data)
            except (ValueError,UnicodeError): raise SlotHeld('slot_metadata_invalid') from None
            expected_record = dict(schemaVersion=1,bindingSha256=binding_digest,budget=budget,
                                   root=expected_root,file=_identity(fd))
            if record != expected_record or data != p.canonical_json(expected_record):
                raise SlotHeld('slot_metadata_invalid')
            _file(fd, expected_root, size=budget)
            os.fsync(fd); os.fsync(meta_fd); os.fsync(root_fd)
        slot = OutputSlot(root,root_fd,fd,meta_fd,expected_root,_identity(fd),name,meta_name,
                          p.canonical_json(record),budget)
        slot._check()
        meta_fd=None; root_fd=None; fd=None
        return slot
    except SlotHeld: raise
    except (OSError,ValueError): raise SlotHeld('slot_operation_uncertain') from None
    finally:
        if meta_fd is not None: os.close(meta_fd)
        if fd is not None: os.close(fd)
        if root_fd is not None: os.close(root_fd)
