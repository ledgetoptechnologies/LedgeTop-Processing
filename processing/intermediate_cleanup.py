"""Exact rebuildable-cache pruning under an existing STOPPED physical-node fence.

No originals, results, camera/pose/tracks/sparse metadata or OpenMVS tree is
selected. This executor never schedules/stops/starts a node. Host root/Docker
administrators remain trusted; private quarantine excludes other writers.
"""
from contextlib import closing, contextmanager
import ctypes
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import sqlite3
import stat
import sys
import uuid
import zipfile

from .intermediate_dependencies import DependencyHeld, resource_references

ALLOWLIST = ('opensfm/features', 'opensfm/matches', 'opensfm/undistorted/images')


class CleanupHeld(ValueError):
    pass


def require(ok, code):
    if not ok:
        raise CleanupHeld(code)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def relative(value):
    require(type(value) is str and value and '\\' not in value and ':' not in value, 'unsafe_relative_path')
    require(not PurePosixPath(value).is_absolute() and str(PurePosixPath(value)) == value
            and all(x not in ('', '.', '..') for x in value.split('/')), 'unsafe_relative_path')
    return value


def identity(info):
    base = dict(device=info.st_dev, inode=info.st_ino, mode=info.st_mode, uid=info.st_uid, gid=info.st_gid)
    if stat.S_ISREG(info.st_mode):
        base.update(bytes=info.st_size, mtime=info.st_mtime_ns, links=info.st_nlink)
    return base


def sha_fd(fd):
    os.lseek(fd, 0, os.SEEK_SET)
    h = hashlib.sha256()
    while True:
        part = os.read(fd, 1024 * 1024)
        if not part:
            return h.hexdigest()
        h.update(part)


class CacheCleanup:
    def __init__(self, task_root, authorization, observe, *, checkpoint=lambda *_: None):
        self.root, self.auth, self.observe, self.checkpoint = Path(task_root).absolute(), authorization, observe, checkpoint
        require(sys.platform.startswith('linux') and hasattr(os, 'O_NOFOLLOW'), 'linux_dirfd_required')
        require(str(uuid.UUID(authorization['operationId'])) == authorization['operationId'], 'invalid_operation')
        require(self.root.name == authorization['engineTaskId'] and
                str(uuid.UUID(self.root.name)) == self.root.name, 'task_identity_changed')
        require(authorization.get('schemaVersion') == 1 and authorization.get('nativeStatus') == 40
                and authorization.get('coordinatorArchiveDurable') is True
                and authorization.get('finalInventoryComplete') is True
                and authorization.get('futureCacheConsumers') == [], 'archive_or_dependency_authorization_missing')
        require(type(authorization.get('generation')) is int and authorization['generation'] >= 1, 'invalid_generation')
        for field in ('authorityId','viewerTaskId','jobId','maintenancePlanId'):
            require(type(authorization.get(field)) is str and str(uuid.UUID(authorization[field]))==authorization[field],
                    'noncanonical_terminal_identity')
        self.selected = authorization['selectedCaches']
        require(isinstance(self.selected, list) and self.selected and len(set(self.selected)) == len(self.selected)
                and set(self.selected) <= set(ALLOWLIST), 'cache_allowlist_changed')
        self.archive, self.inventory = relative(authorization['archiveRelativePath']), authorization['finalInventory']
        require(isinstance(self.inventory, list) and self.inventory, 'complete_final_inventory_required')
        require(self.root.resolve() == self.root and not any(p.is_symlink() for p in (self.root, *self.root.parents)),
                'task_ancestor_symlink')
        self.root_id = identity(self.root.stat())
        require(stat.S_ISDIR(self.root_id['mode']) and self.root_id['uid'] == os.geteuid()
                and not self.root_id['mode'] & 0o022, 'unsafe_task_ownership')

    def fence(self):
        actual = self.observe(self.auth)
        expected = {k: self.auth[k] for k in ('authorityId', 'nodeId', 'engineId', 'containerId', 'imageId',
                    'viewerTaskId', 'generation', 'jobId', 'engineTaskId', 'maintenancePlanId')}
        require(actual == expected, 'fresh_fence_identity_changed')
        require(identity(self.root.stat()) == self.root_id, 'task_root_replaced')

    @contextmanager
    def root_fd(self):
        fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            require(identity(os.fstat(fd)) == self.root_id, 'task_root_replaced')
            yield fd
        finally:
            os.close(fd)

    def safe_dir(self, fd):
        info = os.fstat(fd)
        require(stat.S_ISDIR(info.st_mode) and info.st_dev == self.root_id['device']
                and info.st_uid == os.geteuid() and not info.st_mode & 0o022, 'unsafe_cache_directory')

    @contextmanager
    def parent(self, root, name):
        relative(name)
        fd = os.dup(root)
        try:
            for part in name.split('/')[:-1]:
                nxt = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                os.close(fd)
                fd = nxt
                self.safe_dir(fd)
            yield fd, name.split('/')[-1]
        finally:
            os.close(fd)

    @contextmanager
    def file_fd(self, root, name):
        with self.parent(root, name) as (parent, leaf):
            fd = os.open(leaf, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
            try:
                info = os.fstat(fd)
                require(stat.S_ISREG(info.st_mode) and info.st_dev == self.root_id['device']
                        and identity(info) == identity(os.stat(leaf, dir_fd=parent, follow_symlinks=False)),
                        'unsafe_or_replaced_file')
                yield fd
            finally:
                os.close(fd)

    def scan(self, root, name):
        observed_name = False
        try:
            with self.parent(root, name) as (parent, leaf):
                info = os.stat(leaf, dir_fd=parent, follow_symlinks=False)
                observed_name = True
                require(stat.S_ISDIR(info.st_mode), 'cache_not_regular_directory')
                fd = os.open(leaf, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
                try:
                    require(identity(os.fstat(fd)) == identity(info), 'cache_directory_replaced')
                    found = {}
                    def walk(current, prefix):
                        self.safe_dir(current)
                        found[prefix] = identity(os.fstat(current))
                        for entry in sorted(os.listdir(current)):
                            child = prefix + '/' + entry if prefix else entry
                            info = os.stat(entry, dir_fd=current, follow_symlinks=False)
                            require(info.st_dev == self.root_id['device'], 'nested_mount_or_device')
                            if stat.S_ISDIR(info.st_mode):
                                sub = os.open(entry, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=current)
                                try:
                                    require(identity(os.fstat(sub)) == identity(info), 'cache_directory_replaced')
                                    walk(sub, child)
                                finally:
                                    os.close(sub)
                            else:
                                require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and
                                        info.st_uid == os.geteuid(), 'cache_symlink_hardlink_or_special_file')
                                found[child] = identity(info)
                    walk(fd, '')
                    return found
                finally:
                    os.close(fd)
        except FileNotFoundError:
            require(not observed_name, 'cache_changed_during_scan')
            return None

    def protected(self, root):
        records = {}
        for item in self.inventory:
            local, archived = relative(item['relativePath']), relative(item.get('archivePath', item['relativePath']))
            require(archived not in records and not any(local == p or local.startswith(p + '/') for p in self.selected),
                    'final_inventory_overlap_or_duplicate')
            require(type(item['byteSize']) is int and item['byteSize'] >= 0 and type(item['sha256']) is str
                    and len(item['sha256']) == 64 and all(c in '0123456789abcdef' for c in item['sha256']),
                    'invalid_final_inventory')
            records[archived] = item
            with self.file_fd(root, local) as fd:
                before = identity(os.fstat(fd))
                require(before['bytes'] == item['byteSize'] and sha_fd(fd) == item['sha256']
                        and identity(os.fstat(fd)) == before, 'final_output_changed')
        local_names = {item['relativePath'] for item in self.inventory}
        require(self.archive not in local_names and not any(self.archive == p or self.archive.startswith(p + '/')
                for p in self.selected), 'archive_in_cleanup_set')
        references, pending, buffer_targets, other_targets = {}, set(), set(), set()
        for local in local_names:
            with self.file_fd(root, local) as fd:
                try:references[local] = resource_references(fd, local)
                except DependencyHeld as error:
                    if str(error) != 'untyped_binary_resource':raise
                    pending.add(local)
        def check_references(local, refs):
            for ref in refs:
                buffer_ref = ref in getattr(refs,'buffers',())
                if ref.startswith('data:'):
                    continue
                native_prefix='/var/www/data/'+self.auth['engineTaskId']+'/'
                absolute_native=ref.startswith(native_prefix)
                if absolute_native:
                    ref=ref[len(native_prefix):]
                require(not any(c in ref for c in (':', '\\', '%', '?', '#')) and not ref.startswith('/'),
                        'external_output_dependency')
                parts = [] if absolute_native else list(PurePosixPath(local).parent.parts)
                for part in ref.split('/'):
                    if part == '..':
                        require(parts, 'output_dependency_escape')
                        parts.pop()
                    elif part not in ('', '.'):
                        parts.append(part)
                target = '/'.join(parts)
                require(target in local_names and not any(target == p or target.startswith(p + '/') for p in self.selected),
                        'output_depends_on_cache_or_missing_asset')
                if buffer_ref:buffer_targets.add(target)
                if not buffer_ref or ref in getattr(refs,'nonbuffers',()):other_targets.add(target)
        for local,refs in references.items():check_references(local,refs)
        for local in pending:
            # A raw buffer cannot simultaneously be a tile/content resource.
            # Renamed known containers were already inspected by their magic.
            with self.file_fd(root,local) as fd:
                refs=resource_references(fd,local,opaque_buffer=(local in buffer_targets and local not in other_targets))
            check_references(local,refs)
        return records

    def verify_archive(self, root):
        records = self.protected(root)
        with self.file_fd(root, self.archive) as fd:
            before = identity(os.fstat(fd))
            require(before['links'] == 1 and sha_fd(fd) == self.auth['archiveSha256'], 'archive_digest_changed')
            os.lseek(fd, 0, os.SEEK_SET)
            with os.fdopen(os.dup(fd), 'rb') as stream, zipfile.ZipFile(stream) as archive:
                seen = set()
                for item in archive.infolist():
                    name = relative(item.filename.rstrip('/'))
                    require(not stat.S_ISLNK(item.external_attr >> 16), 'archive_symlink')
                    if item.is_dir():
                        continue
                    require(name not in seen and name in records, 'archive_inventory_incomplete_or_duplicate')
                    seen.add(name)
                    expected, h = records[name], hashlib.sha256()
                    require(item.file_size == expected['byteSize'], 'archive_member_size_changed')
                    with archive.open(item) as member:
                        while True:
                            data = member.read(1024 * 1024)  # ZipExtFile validates full member CRC at EOF.
                            if not data:
                                break
                            h.update(data)
                    require(item.file_size == expected['byteSize'] and h.hexdigest() == expected['sha256'],
                            'archive_member_integrity_failed')
                require(seen == set(records), 'archive_missing_final_output')
            require(identity(os.fstat(fd)) == before, 'archive_replaced_during_verification')
        return before

    def plan(self):
        self.fence()
        with self.root_fd() as root:
            archive = self.verify_archive(root)
            caches = {name: self.scan(root, name) for name in self.selected}
        self.fence()
        return {'schemaVersion': 1, 'authorizationSha256': digest(self.auth), 'taskRootIdentity': self.root_id,
                'archiveIdentity': archive, 'caches': caches,
                'reclaimBytes': sum(x['bytes'] for tree in caches.values() if tree for x in tree.values() if 'bytes' in x)}

    @staticmethod
    def rename_noreplace(src, name, dest, target):
        libc = ctypes.CDLL(None, use_errno=True)
        try:
            fn = libc.renameat2
        except AttributeError:
            raise CleanupHeld('rename_noreplace_unavailable') from None
        fn.argtypes = (ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint)
        fn.restype = ctypes.c_int
        require(fn(src, os.fsencode(name), dest, os.fsencode(target), 1) == 0, 'quarantine_rename_held')

    def private_dir(self, parent, name):
        try:
            os.mkdir(name, 0o700, dir_fd=parent)
            os.fsync(parent)
        except FileExistsError:
            pass
        fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
        try:
            self.safe_dir(fd)
            require(stat.S_IMODE(os.fstat(fd).st_mode) == 0o700, 'quarantine_not_private')
            return fd
        except BaseException:
            os.close(fd)
            raise

    def erase(self, root, name, expected):
        observed = self.scan(root, name)
        if observed is None:
            return
        require(all(path in expected and value == expected[path] for path, value in observed.items()),
                'quarantined_object_changed')
        for path in sorted(observed, key=lambda p: (p.count('/'), p), reverse=True):
            if not path:
                continue
            current = name + '/' + path
            self.checkpoint('before_unlink', current)
            with self.parent(root, current) as (parent, leaf):
                info = os.stat(leaf, dir_fd=parent, follow_symlinks=False)
                require(identity(info) == expected[path], 'quarantine_entry_replaced')
                if stat.S_ISDIR(info.st_mode):
                    os.rmdir(leaf, dir_fd=parent)
                else:
                    require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1, 'unsafe_unlink_object')
                    os.unlink(leaf, dir_fd=parent)
                os.fsync(parent)
            self.checkpoint('after_unlink', current)
        with self.parent(root, name) as (parent, leaf):
            require(identity(os.stat(leaf, dir_fd=parent, follow_symlinks=False)) == expected[''],
                    'quarantine_root_replaced')
            os.rmdir(leaf, dir_fd=parent)
            os.fsync(parent)

    @staticmethod
    def private_regular(info):
        require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_uid == os.geteuid()
                and stat.S_IMODE(info.st_mode) == 0o600, 'unsafe_private_journal')

    def apply(self, prepared, receipt_root):
        require(prepared['authorizationSha256'] == digest(self.auth)
                and prepared['taskRootIdentity'] == self.root_id, 'prepared_authorization_changed')
        receipt = Path(receipt_root).absolute()
        require(receipt.resolve() == receipt and not receipt.is_symlink()
                and receipt.stat().st_uid == os.geteuid()
                and stat.S_IMODE(receipt.stat().st_mode) == 0o700, 'private_receipt_root_required')
        self.fence()
        with self.root_fd() as root:
            require(self.verify_archive(root) == prepared['archiveIdentity'], 'prepared_archive_changed')
            journal_path = receipt / 'cleanup.sqlite'
            if journal_path.exists():
                self.private_regular(journal_path.lstat())
            else:
                fd = os.open(journal_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
                os.close(fd)
            import fcntl
            lock = os.open(receipt / 'cleanup.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
            try:
                self.private_regular(os.fstat(lock))
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                # Connection.__exit__ preserves commit/rollback semantics but
                # does not close the connection. Close on every exit path.
                with closing(sqlite3.connect(journal_path)) as db, db:
                    db.execute('PRAGMA synchronous=FULL')
                    tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                    require(tables <= {'binding', 'phases'}, 'foreign_cleanup_journal')
                    db.execute('CREATE TABLE IF NOT EXISTS binding (singleton INTEGER PRIMARY KEY,value TEXT NOT NULL)')
                    db.execute('CREATE TABLE IF NOT EXISTS phases (path TEXT PRIMARY KEY,phase TEXT NOT NULL)')
                    bound = canonical({'operationId': self.auth['operationId'], 'planSha256': digest(prepared)}).decode()
                    old = db.execute('SELECT value FROM binding WHERE singleton=1').fetchone()
                    require(old is None or old[0] == bound, 'cleanup_journal_binding_changed')
                    db.execute('INSERT OR IGNORE INTO binding VALUES(1,?)', (bound,))
                    db.commit()
                    meta = os.open(receipt, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                    try:
                        os.fsync(meta)
                    finally:
                        os.close(meta)
                    def phase(name, value):
                        db.execute('INSERT INTO phases VALUES(?,?) ON CONFLICT(path) DO UPDATE SET phase=excluded.phase',
                                   (name, value))
                        db.commit()
                        self.checkpoint(value, name)
                    base = self.private_dir(root, '.lt-intermediate-cleanup')
                    try:
                        quarantine = self.private_dir(base, self.auth['operationId'])
                        try:
                            for number, name in enumerate(self.selected):
                                self.fence()
                                tree = prepared['caches'][name]
                                row = db.execute('SELECT phase FROM phases WHERE path=?', (name,)).fetchone()
                                state = row[0] if row else 'new'
                                require(state in ('new', 'intent', 'quarantined', 'erasing', 'done'), 'unknown_cleanup_phase')
                                target = 'cache-' + str(number)
                                src, dst = self.scan(root, name), self.scan(quarantine, target)
                                if tree is None:
                                    require(src is None and dst is None, 'previously_missing_cache_appeared')
                                    phase(name, 'done')
                                    continue
                                if state == 'done':
                                    require(src is None and dst is None, 'cleaned_cache_reappeared')
                                    continue
                                if state in ('new', 'intent'):
                                    if src is not None and dst is None:
                                        require(src == tree, 'cache_changed_after_plan')
                                        phase(name, 'intent')
                                        with self.parent(root, name) as (parent, leaf):
                                            self.rename_noreplace(parent, leaf, quarantine, target)
                                            os.fsync(parent)
                                            os.fsync(quarantine)
                                        self.checkpoint('after_rename', name)
                                        dst = self.scan(quarantine, target)
                                    require(self.scan(root, name) is None and dst == tree, 'uncertain_quarantine_identity')
                                    phase(name, 'quarantined')
                                require(self.scan(root, name) is None, 'new_cache_writer_appeared')
                                if state == 'quarantined':
                                    require(self.scan(quarantine, target) == tree, 'quarantine_changed')
                                phase(name, 'erasing')
                                self.fence()
                                self.erase(quarantine, target, tree)
                                phase(name, 'done')
                        finally:
                            os.close(quarantine)
                    finally:
                        os.close(base)
            finally:
                os.close(lock)
        self.fence()
        return {'status': 'complete', 'operationId': self.auth['operationId'],
                'reclaimedLogicalBytes': prepared['reclaimBytes'], 'originalsAndResultsRemoved': False}
