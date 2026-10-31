"""Linux process-lease ownership of explicitly registered result-reader pins.

The fixed flock, not time, PID liveness, or a generic daemon lock, proves the prior
reader process no longer holds its lease. Only exact registered read-pin mappings
are retired. Global strong ownership intentionally retains uncertain sessions even
if callers lose references; no finalizer unlocks a lease behind a live descriptor.
"""
from contextlib import contextmanager
import errno
import json
import os
from pathlib import Path
import secrets
import sqlite3
import stat
import sys
import threading


class ReaderSessionError(ValueError):
    """Bounded ownership failure; keep the pin/lease when outcome is uncertain."""


_OWNERS = {}
_OWNERS_LOCK = threading.RLock()
_PROCESS_PID=os.getpid()


def _json(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'))


def _regular(info, *, private=True):
    return (stat.S_ISREG(info.st_mode) and info.st_nlink==1 and info.st_uid==os.geteuid()
            and (not private or info.st_mode & 0o077==0))


def _lock_identity(info):
    return (info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns,info.st_ctime_ns)


class ReaderSession:
    def __init__(self,ledger):
        self.ledger=ledger
        self.path=Path(ledger.path).absolute()
        self.root=Path(ledger.root).absolute()
        self.lock_path=self.path.with_name(self.path.name+'.readers.lock')
        self._mutex=threading.RLock()
        self._fd=None
        self._pid=None
        self._binding=None
        self.epoch=None
        self._handles={}
        self._closed=False

    def _paths(self):
        for path in (self.path,self.root,self.lock_path.parent):
            if any(part.is_symlink() for part in (path,*path.parents)) or path.resolve()!=path:
                raise ReaderSessionError('reader_owned_paths_required')
        parent=self.path.parent.stat()
        root=self.root.stat()
        if (not stat.S_ISDIR(parent.st_mode) or parent.st_uid!=os.geteuid() or parent.st_mode & 0o077
                or not stat.S_ISDIR(root.st_mode) or root.st_uid!=os.geteuid() or root.st_mode & 0o022
                or not _regular(self.path.lstat())):
            raise ReaderSessionError('reader_owned_private_ledger_required')

    def __enter__(self):
        if os.getpid()!=_PROCESS_PID:
            raise ReaderSessionError('reader_fork_requires_fresh_process')
        if not sys.platform.startswith('linux'):
            raise ReaderSessionError('linux_reader_session_required')
        import fcntl
        with self._mutex,_OWNERS_LOCK:
            if self._fd is not None or self._closed:
                raise ReaderSessionError('reader_session_already_used')
            self._paths()
            key=str(self.lock_path)
            if key in _OWNERS:
                raise ReaderSessionError('reader_session_busy')
            created=False
            try:
                try:
                    fd=os.open(self.lock_path,os.O_CREAT|os.O_EXCL|os.O_RDWR|os.O_NOFOLLOW|os.O_CLOEXEC,0o600)
                    created=True
                except FileExistsError:
                    fd=os.open(self.lock_path,os.O_RDWR|os.O_NOFOLLOW|os.O_CLOEXEC|os.O_NONBLOCK)
                self._fd=fd
                os.set_inheritable(fd,False)
                if not _regular(os.fstat(fd)):
                    raise ReaderSessionError('reader_private_single_link_lease_required')
                try:
                    fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
                except OSError:
                    raise ReaderSessionError('reader_session_busy') from None
                if created:
                    os.write(fd,secrets.token_hex(32).encode('ascii'))
                    os.fsync(fd)
                    parent_fd=os.open(self.path.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
                    try:os.fsync(parent_fd)
                    finally:os.close(parent_fd)
                lock=os.fstat(fd)
                marker=os.pread(fd,65,0)
                marker_valid=len(marker)==64 and all(byte in b'0123456789abcdef' for byte in marker)
                current=self.lock_path.lstat()
                if _lock_identity(lock)!=_lock_identity(current):
                    raise ReaderSessionError('reader_lease_identity_changed')
                ledger,root=self.path.lstat(),self.root.stat()
                self._binding=dict(ledgerPath=str(self.path),storageRoot=str(self.root),lockPath=key,
                    ledgerIdentity=[ledger.st_dev,ledger.st_ino],rootIdentity=[root.st_dev,root.st_ino],
                    lockIdentity=list(_lock_identity(lock)),marker=marker.hex())
                self._pid=os.getpid()
                epoch=secrets.token_hex(32)
                with self._db(startup=True) as db:
                    for sql in (
                        'CREATE TABLE IF NOT EXISTS reader_session_binding (id INTEGER PRIMARY KEY CHECK(id=1),binding TEXT NOT NULL,current_epoch TEXT NOT NULL)',
                        'CREATE TABLE IF NOT EXISTS reader_session_epochs (epoch TEXT PRIMARY KEY,state TEXT NOT NULL,successor TEXT)',
                        '''CREATE TABLE IF NOT EXISTS reader_owned_pins (
                            reference TEXT PRIMARY KEY,reservation TEXT NOT NULL,object TEXT NOT NULL,
                            epoch TEXT NOT NULL,state TEXT NOT NULL,superseded_by TEXT)'''):
                        db.execute(sql)
                    old=db.execute('SELECT * FROM reader_session_binding WHERE id=1').fetchone()
                    if old:
                        if not marker_valid or old['binding']!=_json(self._binding):
                            raise ReaderSessionError('reader_lease_binding_changed')
                    else:
                        if (db.execute('SELECT 1 FROM reader_owned_pins LIMIT 1').fetchone()
                                or db.execute('SELECT 1 FROM reader_session_epochs LIMIT 1').fetchone()):
                            raise ReaderSessionError('unbound_reader_ownership_requires_review')
                        # A crash before the first generation commit has no
                        # registered owners. Under exclusive flock it is safe to
                        # finish a partial marker; anonymous pins remain intact.
                        if not marker_valid:
                            marker=secrets.token_hex(32).encode('ascii')
                            os.ftruncate(fd,0)
                            os.pwrite(fd,marker,0)
                        os.fsync(fd)
                        parent_fd=os.open(self.path.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
                        try:os.fsync(parent_fd)
                        finally:os.close(parent_fd)
                        self._binding['lockIdentity']=list(_lock_identity(os.fstat(fd)))
                        self._binding['marker']=marker.hex()
                    encoded=_json(self._binding)
                    # Mark only exact prior registered active/uncertain read pins.
                    # Worker/downstream/foreign/legacy pins never match this join.
                    db.execute("""UPDATE reader_owned_pins SET state='superseded',superseded_by=?
                        WHERE epoch!=? AND state IN ('active','uncertain')
                        AND EXISTS(SELECT 1 FROM reader_session_epochs e WHERE e.epoch=reader_owned_pins.epoch)
                        AND EXISTS(
                        SELECT 1 FROM pins p WHERE p.reservation=reader_owned_pins.reservation
                        AND p.object=reader_owned_pins.object AND p.reference=reader_owned_pins.reference
                        AND p.kind='read' AND p.state IN ('active','uncertain'))""",(epoch,epoch))
                    db.execute("""UPDATE pins SET state='released' WHERE kind='read' AND state IN ('active','uncertain')
                        AND EXISTS(SELECT 1 FROM reader_owned_pins o WHERE o.reservation=pins.reservation
                        AND o.object=pins.object AND o.reference=pins.reference
                        AND o.state='superseded' AND o.superseded_by=?)""",(epoch,))
                    db.execute("UPDATE reader_session_epochs SET state='superseded',successor=? WHERE state!='superseded'",(epoch,))
                    db.execute("INSERT INTO reader_session_epochs VALUES(?,'active',NULL)",(epoch,))
                    db.execute('INSERT INTO reader_session_binding VALUES(1,?,?) ON CONFLICT(id) DO UPDATE SET current_epoch=excluded.current_epoch',(encoded,epoch))
                self.epoch=epoch
                _OWNERS[key]=self
                return self
            except Exception:
                if self._fd is not None:
                    os.close(self._fd)
                    self._fd=None
                self._pid=None
                raise

    def _commit(self,db):
        """Fault-injection seam; a raised/unknown commit keeps handle ownership."""
        db.commit()

    @contextmanager
    def _db(self,*,startup=False,closing=False):
        self._check_files()
        db=sqlite3.connect(self.path,timeout=20,isolation_level=None)
        db.row_factory=sqlite3.Row
        try:
            db.execute('PRAGMA synchronous=FULL')
            db.execute('BEGIN IMMEDIATE')
            if not startup:
                row=db.execute('SELECT * FROM reader_session_binding WHERE id=1').fetchone()
                if not row or row['binding']!=_json(self._binding) or row['current_epoch']!=self.epoch:
                    raise ReaderSessionError('reader_epoch_not_current')
                epoch=db.execute('SELECT state FROM reader_session_epochs WHERE epoch=?',(self.epoch,)).fetchone()
                if not epoch or epoch['state'] not in (('active','closed') if closing else ('active',)):
                    raise ReaderSessionError('reader_epoch_not_active')
            yield db
            self._check_files()
            self._commit(db)
        except Exception:
            db.rollback()
            raise
        finally:db.close()

    def _check_files(self):
        if self._pid!=os.getpid() or self._fd is None:
            raise ReaderSessionError('reader_owner_process_required')
        self._paths()
        try:
            observed,current=os.fstat(self._fd),self.lock_path.lstat()
            ledger,root=self.path.lstat(),self.root.stat()
            valid=(_regular(observed) and _regular(current)
                and list(_lock_identity(observed))==self._binding['lockIdentity']
                and _lock_identity(observed)==_lock_identity(current)
                and os.pread(self._fd,65,0).hex()==self._binding['marker']
                and [ledger.st_dev,ledger.st_ino]==self._binding['ledgerIdentity']
                and [root.st_dev,root.st_ino]==self._binding['rootIdentity']
                and not os.get_inheritable(self._fd))
        except (OSError,UnicodeError):valid=False
        if not valid:
            raise ReaderSessionError('reader_lease_identity_changed')

    def _process(self):
        # Check before any inherited Python mutex: another parent thread may
        # have held it at fork and cannot unlock the child's copied mutex.
        if self._pid!=os.getpid():
            raise ReaderSessionError('reader_owner_process_required')

    def check(self):
        self._process()
        with self._mutex:
            if self._closed:
                raise ReaderSessionError('reader_session_closed')
            self._check_files()

    @property
    def active_count(self):
        self._process()
        with self._mutex:
            self.check()
            return len(self._handles)

    def pin(self,reservation,object_id):
        self._process()
        with self._mutex:
            self.check()
            reference='reader-'+secrets.token_hex(24)
            handle=ReadPinHandle(self,reservation,object_id,reference)
            self._handles[reference]=handle
            writes=False
            try:
                with self._db() as db:
                    obj=db.execute('SELECT * FROM objects WHERE reservation=? AND id=?',(reservation,object_id)).fetchone()
                    if (obj is None or obj['cleaned'] is not None or
                            db.execute('SELECT 1 FROM cleanup_intents WHERE reservation=? AND object=?',(reservation,object_id)).fetchone()):
                        raise ReaderSessionError('reader_object_unavailable')
                    writes=True
                    db.execute("INSERT INTO pins VALUES(?,?,?,'read','active')",(reservation,object_id,reference))
                    db.execute("INSERT INTO reader_owned_pins VALUES(?,?,?,?,'active',NULL)",(reference,reservation,object_id,self.epoch))
                return handle
            except Exception:
                if writes:handle._uncertain=True
                else:self._handles.pop(reference,None)
                raise

    def _release(self,handle,*,unopened=False):
        self._process()
        with self._mutex:
            self.check()
            if handle._session is not self or handle._epoch!=self.epoch or handle._tainted:
                raise ReaderSessionError('reader_handle_not_current')
            if unopened:
                if handle._attached:
                    raise ReaderSessionError('reader_handle_already_attached')
            else:
                if not handle._attached:
                    raise ReaderSessionError('reader_descriptor_not_attached')
                if not handle._known_closed:
                    try:os.fstat(handle._descriptor)
                    except OSError as error:
                        if error.errno!=errno.EBADF:
                            handle._uncertain=True
                            raise ReaderSessionError('reader_descriptor_close_uncertain') from None
                        handle._known_closed=True
                    else:
                        handle._uncertain=True
                        raise ReaderSessionError('reader_descriptor_still_open_or_reused')
            try:
                with self._db() as db:
                    owner=db.execute('SELECT * FROM reader_owned_pins WHERE reference=?',(handle._reference,)).fetchone()
                    pin=db.execute('SELECT * FROM pins WHERE reservation=? AND object=? AND reference=?',
                                   (handle._reservation,handle._object,handle._reference)).fetchone()
                    if (not owner or not pin or (owner['epoch'],owner['reservation'],owner['object'])!=(self.epoch,handle._reservation,handle._object)
                            or pin['kind']!='read' or owner['state'] not in ('active','uncertain','released')
                            or pin['state'] not in ('active','uncertain','released')):
                        raise ReaderSessionError('reader_pin_ownership_changed')
                    db.execute("UPDATE pins SET state='released' WHERE reservation=? AND object=? AND reference=?",
                               (handle._reservation,handle._object,handle._reference))
                    db.execute("UPDATE reader_owned_pins SET state='released' WHERE reference=?",(handle._reference,))
            except Exception:
                handle._uncertain=True
                raise
            handle._uncertain=False
            handle._released=True
            self._handles.pop(handle._reference,None)

    def close(self):
        self._process()
        with self._mutex,_OWNERS_LOCK:
            self.check()
            if self._handles:
                raise ReaderSessionError('reader_handles_still_live_or_uncertain')
            with self._db(closing=True) as db:
                if db.execute("""SELECT 1 FROM reader_owned_pins o WHERE o.epoch=?
                    AND (o.state!='released' OR NOT EXISTS(SELECT 1 FROM pins p
                    WHERE p.reservation=o.reservation AND p.object=o.object AND p.reference=o.reference
                    AND p.kind='read' AND p.state='released')) LIMIT 1""",(self.epoch,)).fetchone():
                    raise ReaderSessionError('durable_reader_handles_still_live_or_uncertain')
                db.execute("UPDATE reader_session_epochs SET state='closed' WHERE epoch=?",(self.epoch,))
            # Closing the sole non-inheritable lease FD releases flock. No
            # explicit LOCK_UN is ever issued, especially in a forked child.
            try:os.close(self._fd)
            except OSError:
                raise ReaderSessionError('reader_lease_close_uncertain') from None
            self._fd=None
            self._closed=True
            _OWNERS.pop(str(self.lock_path),None)

    def __exit__(self,*args):
        self.close()


class ReadPinHandle:
    def __init__(self,session,reservation,object_id,reference):
        self._session,self._epoch=session,session.epoch
        self._reservation,self._object,self._reference=reservation,object_id,reference
        self._attached=self._released=self._uncertain=self._tainted=False
        self._known_closed=False
        self._descriptor=None
        self._fd_identity=None

    def check(self):
        self._session.check()
        if self._released or self._epoch!=self._session.epoch:
            raise ReaderSessionError('reader_handle_not_active')

    def attach_fd(self,descriptor):
        self._session._process()
        with self._session._mutex:
            self.check()
            if self._attached:
                self._tainted=self._uncertain=True
                raise ReaderSessionError('reader_descriptor_already_attached')
            self._attached=True
            self._descriptor=descriptor
            try:
                if type(descriptor) is not int or descriptor<0:
                    raise ReaderSessionError('reader_descriptor_invalid')
                observed=os.fstat(descriptor)
                self._fd_identity=(observed.st_dev,observed.st_ino)
                with self._session._db() as db:
                    obj=db.execute('SELECT path FROM objects WHERE reservation=? AND id=?',(self._reservation,self._object)).fetchone()
                    self._session.ledger._owned_path(self._reservation,obj['path'])
                    current=(self._session.root/obj['path']).lstat()
                    if (not stat.S_ISREG(observed.st_mode) or observed.st_nlink!=1
                            or (observed.st_dev,observed.st_ino)!=(current.st_dev,current.st_ino)):
                        raise ReaderSessionError('reader_descriptor_identity_changed')
            except Exception:
                self._uncertain=True
                raise

    def close_fd(self,descriptor=None):
        """Close the attached FD and remember success before its number is reused.

        Optional descriptor must equal the attached FD. Unknown outcomes retain
        the pin/lease. This method does not release the durable pin itself.
        """
        self._session._process()
        with self._session._mutex:
            self.check()
            if (not self._attached or self._tainted or self._fd_identity is None
                    or (descriptor is not None and (type(descriptor) is not int or descriptor!=self._descriptor))):
                self._uncertain=True
                raise ReaderSessionError('reader_descriptor_ownership_unknown')
            if self._known_closed:
                return
            try:
                current=os.fstat(self._descriptor)
                if (current.st_dev,current.st_ino)!=self._fd_identity:
                    raise ReaderSessionError('reader_descriptor_reused')
                os.close(self._descriptor)
            except Exception:
                self._uncertain=True
                raise ReaderSessionError('reader_descriptor_close_uncertain') from None
            self._known_closed=True

    def release(self):
        self._session._release(self)

    def abort_unopened(self):
        self._session._release(self,unopened=True)
