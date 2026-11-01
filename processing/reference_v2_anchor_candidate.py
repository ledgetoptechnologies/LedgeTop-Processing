"""Disabled local monotonic registry checkpoint, not hardware anti-rollback."""
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
from contextlib import contextmanager
from . import reference_v2_registry_candidate as registry

QUALIFIED=False
DDL=('CREATE TABLE anchor (singleton INTEGER PRIMARY KEY CHECK(singleton=1), root_sha TEXT NOT NULL, '
     'epoch TEXT NOT NULL, revision INTEGER NOT NULL, digest TEXT NOT NULL)')

class AnchorHeld(ValueError):pass

class LocalAnchor:
    def __init__(self,path,*,owner,root_key,registry_path,enabled=False,bootstrap=None):
        from ops.reference_worker_daemon import DaemonStateLock
        if QUALIFIED is not True or enabled is not True or os.name!='posix' or type(owner) is not DaemonStateLock:
            raise AnchorHeld('local anchor is not qualified')
        if type(root_key) is not bytes or len(root_key)!=32:raise AnchorHeld('pinned root required')
        self.owner=owner;self.pid=os.getpid();self.path=Path(path).absolute();self.registry_path=Path(registry_path).absolute()
        owner.check(owner.root)
        if self.path.parent!=owner.root or self.path.resolve()!=self.path or self.registry_path.is_symlink():raise AnchorHeld('invalid anchor path')
        source=self.registry_path.stat()
        if (not stat.S_ISREG(source.st_mode) or source.st_uid!=os.geteuid() or source.st_nlink!=1
                or stat.S_IMODE(source.st_mode)&0o077 or source.st_dev==owner.root.stat().st_dev):
            raise AnchorHeld('private registry on separate filesystem required')
        self.registry_identity=(source.st_dev,source.st_ino);self.root_sha=hashlib.sha256(root_key).hexdigest()
        self.root_key=root_key
        if bootstrap is not None and (type(bootstrap) is not tuple or len(bootstrap)!=3 or type(bootstrap[0]) is not str
                or not registry.envelope.ID.fullmatch(bootstrap[0]) or type(bootstrap[1]) is not int
                or not 1<=bootstrap[1]<=2**53-1 or type(bootstrap[2]) is not str
                or not registry.envelope.HEX.fullmatch(bootstrap[2])):raise AnchorHeld('invalid offline bootstrap')
        if bootstrap is None and not self.path.exists():raise AnchorHeld('anchor missing: offline enrollment required')
        flags=os.O_RDWR|os.O_NOFOLLOW|os.O_CLOEXEC
        if bootstrap is not None:flags|=os.O_CREAT|os.O_EXCL
        try:fd=os.open(self.path,flags,0o600)
        except OSError:raise AnchorHeld('anchor unavailable') from None
        try:
            info=os.fstat(fd);named=self.path.lstat()
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_uid!=os.geteuid()
                    or stat.S_IMODE(info.st_mode)!=0o600 or (info.st_dev,info.st_ino)!=(named.st_dev,named.st_ino)):
                raise AnchorHeld('owned anchor required')
            self.identity=(info.st_dev,info.st_ino)
        finally:os.close(fd)
        if bootstrap is not None:
            with self._tx() as db:
                if db.execute("SELECT 1 FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' LIMIT 1").fetchone():raise AnchorHeld('empty anchor required')
                db.execute(DDL);db.execute('INSERT INTO anchor VALUES(1,?,?,?,?)',(self.root_sha,*bootstrap))
            directory=os.open(owner.root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
            try:os.fsync(directory)
            finally:os.close(directory)
        else:
            with self._tx() as db:self._row(db)

    def _check(self):
        if QUALIFIED is not True or self.pid!=os.getpid():raise AnchorHeld('anchor owner changed')
        self.owner.check(self.owner.root);info=self.path.lstat();source=self.registry_path.lstat()
        if (not stat.S_ISREG(info.st_mode) or (info.st_dev,info.st_ino)!=self.identity
                or not stat.S_ISREG(source.st_mode) or (source.st_dev,source.st_ino)!=self.registry_identity):
            raise AnchorHeld('anchor or registry identity changed')

    @contextmanager
    def _tx(self):
        self._check();db=sqlite3.connect(self.path,timeout=5)
        try:
            db.execute('PRAGMA synchronous=FULL');db.execute('BEGIN IMMEDIATE');yield db
            self._check();db.commit()
        except BaseException:db.rollback();raise
        finally:db.close()

    def _row(self,db):
        schema=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='anchor'").fetchone()
        row=db.execute('SELECT root_sha,epoch,revision,digest FROM anchor WHERE singleton=1').fetchone()
        if schema is None or schema[0]!=DDL or row is None or row[0]!=self.root_sha:raise AnchorHeld('anchor schema or root changed')
        return row

    def verify_and_advance(self,raw,signature,*,now,role_policy):
        """Verify first, then CAS the local checkpoint; no remote authority grant."""
        with self._tx() as db:
            fd=os.open(self.registry_path,os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC)
            try:
                opened=os.fstat(fd)
                if (opened.st_dev,opened.st_ino)!=self.registry_identity or opened.st_size>65536:raise AnchorHeld('registry file changed')
                content=b''
                while len(content)<=65536:
                    block=os.read(fd,65537-len(content))
                    if not block:break
                    content+=block
                after=os.fstat(fd)
                if (after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns)!=(opened.st_dev,opened.st_ino,opened.st_size,opened.st_mtime_ns) or content!=raw:
                    raise AnchorHeld('registry file changed')
            finally:os.close(fd)
            old=self._row(db);epoch,revision,digest=old[1:]
            snapshot=registry.load_signed(raw,signature,root_public_key=self.root_key,expected_epoch=epoch,
                minimum_revision=revision,now=now,role_policy=role_policy)
            parsed=json.loads(raw);new_revision=parsed['revision'];new_digest=hashlib.sha256(raw).hexdigest()
            if new_revision==revision:
                if new_digest!=digest:raise AnchorHeld('conflicting registry revision')
            elif new_revision==revision+1:
                if db.execute('UPDATE anchor SET revision=?,digest=? WHERE singleton=1 AND revision=? AND digest=?',
                              (new_revision,new_digest,revision,digest)).rowcount!=1:raise AnchorHeld('anchor compare-and-swap failed')
            else:raise AnchorHeld('noncontiguous registry revision')
            return snapshot
