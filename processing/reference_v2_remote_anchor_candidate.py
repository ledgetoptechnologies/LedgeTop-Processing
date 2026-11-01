"""Disabled, scoped mTLS + durable CAS experiment; not independent authority."""
import hashlib
import os
from pathlib import Path
import sqlite3
import stat
import time
from contextlib import contextmanager
from . import node_authority_session as wire, reference_v2_envelope_candidate as envelope

QUALIFIED=False
# No coordinator-owned claim/pin + ledger-event authorization exists yet.
# Tests may patch this only to exercise synthetic transport, never for runtime.
AUTHORITY_QUALIFIED=False
DDL_STATE=('CREATE TABLE checkpoint (singleton INTEGER PRIMARY KEY CHECK(singleton=1), domain TEXT NOT NULL, '
           'ledger TEXT NOT NULL, epoch TEXT NOT NULL, revision INTEGER NOT NULL, digest TEXT NOT NULL)')
DDL_OP=('CREATE TABLE operations (operation TEXT PRIMARY KEY, previous_revision INTEGER NOT NULL, '
        'previous_digest TEXT NOT NULL, next_revision INTEGER NOT NULL, next_digest TEXT NOT NULL)')
class AnchorHeld(ValueError):pass

class Store:
    def __init__(self,path,*,owner,domain,ledger,epoch,enabled=False,bootstrap=None):
        from ops.reference_worker_daemon import DaemonStateLock
        if QUALIFIED is not True or enabled is not True or os.name!='posix' or type(owner) is not DaemonStateLock:raise AnchorHeld('unqualified')
        if any(type(x) is not str or not envelope.ID.fullmatch(x) for x in (domain,ledger,epoch)):raise AnchorHeld('invalid binding')
        self.owner=owner;self.pid=os.getpid();self.path=Path(path).absolute();self.binding=(domain,ledger,epoch)
        owner.check(owner.root)
        if self.path.parent!=owner.root or self.path.resolve()!=self.path:raise AnchorHeld('invalid path')
        if bootstrap is not None and (type(bootstrap) is not tuple or len(bootstrap)!=2 or type(bootstrap[0]) is not int or not 0<=bootstrap[0]<2**53-1 or type(bootstrap[1]) is not str or not envelope.HEX.fullmatch(bootstrap[1])):raise AnchorHeld('invalid bootstrap')
        if bootstrap is None and not self.path.exists():raise AnchorHeld('missing state')
        try:fd=os.open(self.path,os.O_RDWR|os.O_NOFOLLOW|os.O_CLOEXEC|((os.O_CREAT|os.O_EXCL) if bootstrap is not None else 0),0o600)
        except OSError:raise AnchorHeld('state unavailable') from None
        try:
            info=os.fstat(fd);named=self.path.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_uid!=os.geteuid() or stat.S_IMODE(info.st_mode)!=0o600 or (info.st_dev,info.st_ino)!=(named.st_dev,named.st_ino):raise AnchorHeld('invalid state')
            self.identity=(info.st_dev,info.st_ino)
        finally:os.close(fd)
        with self._tx() as db:
            if bootstrap is not None:
                if db.execute("SELECT 1 FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' LIMIT 1").fetchone():raise AnchorHeld('nonempty state')
                db.execute(DDL_STATE);db.execute(DDL_OP);db.execute('INSERT INTO checkpoint VALUES(1,?,?,?,?,?)',(*self.binding,*bootstrap))
            else:self._state(db)
        if bootstrap is not None:
            fd=os.open(owner.root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
            try:os.fsync(fd)
            finally:os.close(fd)

    def _check(self):
        if QUALIFIED is not True or self.pid!=os.getpid():raise AnchorHeld('owner changed')
        self.owner.check(self.owner.root);info=self.path.lstat()
        if not stat.S_ISREG(info.st_mode) or (info.st_dev,info.st_ino)!=self.identity:raise AnchorHeld('state changed')

    @contextmanager
    def _tx(self):
        self._check();db=sqlite3.connect(self.path,timeout=5)
        try:
            db.execute('PRAGMA synchronous=FULL');db.execute('BEGIN IMMEDIATE');yield db;self._check();db.commit()
        except BaseException:db.rollback();raise
        finally:db.close()

    def _state(self,db):
        for name,ddl in (('checkpoint',DDL_STATE),('operations',DDL_OP)):
            row=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?",(name,)).fetchone()
            if row is None or row[0]!=ddl:raise AnchorHeld('schema changed')
        row=db.execute('SELECT domain,ledger,epoch,revision,digest FROM checkpoint WHERE singleton=1').fetchone()
        if row is None or row[:3]!=self.binding:raise AnchorHeld('binding changed')
        return row[3:]

    def cas(self,request):
        if type(request) is not dict or set(request)!={'domain','ledger','epoch','operation','previousRevision','previousDigest','nextRevision','nextDigest'}:raise AnchorHeld('invalid request')
        if tuple(request[k] for k in ('domain','ledger','epoch'))!=self.binding:raise AnchorHeld('binding changed')
        operation=request['operation'];previous=request['previousRevision'];next_revision=request['nextRevision'];before=request['previousDigest'];after=request['nextDigest']
        if (type(operation) is not str or not envelope.ID.fullmatch(operation) or type(previous) is not int
                or not 0<=previous<2**53-1 or type(next_revision) is not int or next_revision!=previous+1
                or type(before) is not str or not envelope.HEX.fullmatch(before) or type(after) is not str or not envelope.HEX.fullmatch(after)):
            raise AnchorHeld('invalid transition')
        with self._tx() as db:
            current=self._state(db)
            old=db.execute('SELECT previous_revision,previous_digest,next_revision,next_digest FROM operations WHERE operation=?',(operation,)).fetchone()
            transition=(previous,before,next_revision,after)
            if old is not None:
                if old!=transition or current!=(next_revision,after):raise AnchorHeld('conflicting or superseded replay')
            else:
                if current!=(previous,before):raise AnchorHeld('stale checkpoint')
                if db.execute('UPDATE checkpoint SET revision=?,digest=? WHERE singleton=1 AND revision=? AND digest=?',(next_revision,after,previous,before)).rowcount!=1:raise AnchorHeld('CAS failed')
                db.execute('INSERT INTO operations VALUES(?,?,?,?,?)',(operation,*transition))
            return dict(domain=self.binding[0],ledger=self.binding[1],epoch=self.binding[2],operation=operation,revision=next_revision,digest=after)

def serve(sock,*,store,peer_sha256,timeout=5):
    """One authenticated session only, caller owns listener and socket lifecycle."""
    if QUALIFIED is not True or AUTHORITY_QUALIFIED is not True or type(store) is not Store or type(timeout) not in (int,float) or not 0<timeout<=10:raise AnchorHeld('unqualified session')
    wire._peer(sock,peer_sha256,True);deadline=time.monotonic()+timeout
    request=wire._read(sock,deadline)
    if type(request) is not dict or set(request)!={'nonce','transition'} or type(request['nonce']) is not str or len(request['nonce'])!=64 or not envelope.HEX.fullmatch(request['nonce']):raise AnchorHeld('invalid nonce')
    receipt=store.cas(request['transition'])
    wire._write(sock,dict(nonce=request['nonce'],receipt=receipt),deadline)

def exchange(sock,*,request,peer_sha256,nonce,timeout=5):
    """One mTLS exchange; loss is ambiguous and must retain original operation."""
    if QUALIFIED is not True or type(timeout) not in (int,float) or not 0<timeout<=10:raise AnchorHeld('unqualified session')
    wire._peer(sock,peer_sha256,False)
    if not sock.context.check_hostname or type(nonce) is not str or not envelope.HEX.fullmatch(nonce):raise AnchorHeld('invalid session')
    deadline=time.monotonic()+timeout;wire._write(sock,dict(nonce=nonce,transition=request),deadline)
    value=wire._read(sock,deadline)
    expected=dict(domain=request['domain'],ledger=request['ledger'],epoch=request['epoch'],operation=request['operation'],revision=request['nextRevision'],digest=request['nextDigest'])
    if type(value) is not dict or value!=dict(nonce=nonce,receipt=expected):raise AnchorHeld('receipt changed')
    return value['receipt']
