"""Opt-in conservative coordinator spool admission, never cleanup authority.

Every attempted request keeps its immutable retained-byte upper bound, even a
duplicate or failed upload. A crash keeps the larger active promise as well.
No TTL, remove, restart or caller JSON releases a charge. This deliberately
bounded first profile cannot provide sustained quota reclamation.
"""
from contextlib import closing, contextmanager
from contextvars import ContextVar
from dataclasses import asdict, dataclass
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import stat
import threading
import uuid
import weakref

from .maintenance import Conflict

PAGE_CAP=1024
CONTROL_ALLOWANCE=3*PAGE_CAP*4096+65536
_CURRENT=ContextVar('viewer_spool_capacity_request',default=None)
_LEASES=weakref.WeakKeyDictionary()
_LOCKS={}
_LOCKS_LOCK=threading.Lock()
DDL=(
 "CREATE TABLE spool_binding(id INTEGER PRIMARY KEY CHECK(id=1),value TEXT NOT NULL CHECK(length(CAST(value AS BLOB))<=8192))",
 "CREATE TABLE spool_operations(id TEXT PRIMARY KEY,task TEXT NOT NULL,generation INTEGER NOT NULL,kind TEXT NOT NULL CHECK(kind IN ('upload','archive','form')),charge INTEGER NOT NULL CHECK(charge>0),state TEXT NOT NULL CHECK(state IN ('active','retained')))",
 "CREATE TRIGGER spool_binding_update BEFORE UPDATE ON spool_binding BEGIN SELECT RAISE(ABORT,'immutable spool policy'); END",
 "CREATE TRIGGER spool_binding_delete BEFORE DELETE ON spool_binding BEGIN SELECT RAISE(ABORT,'immutable spool policy'); END",
 "CREATE TRIGGER spool_operations_identity BEFORE UPDATE OF id,task,generation,kind,charge ON spool_operations BEGIN SELECT RAISE(ABORT,'immutable spool charge'); END",
 "CREATE TRIGGER spool_operations_delete BEFORE DELETE ON spool_operations BEGIN SELECT RAISE(ABORT,'retained spool charge'); END",
 "CREATE TRIGGER spool_operations_state BEFORE UPDATE OF state ON spool_operations WHEN OLD.state!='active' OR NEW.state!='retained' BEGIN SELECT RAISE(ABORT,'invalid spool settlement'); END",
)

def require(value,reason='spool_admission_unavailable'):
    if not value:raise Conflict(reason)

def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)

def _integer(value,minimum=1,maximum=10**15):
    require(type(value) is int and minimum<=value<=maximum,'invalid_spool_policy')

@dataclass(frozen=True)
class SpoolPolicy:
    retained_budget_bytes:int
    safety_floor_bytes:int
    max_concurrent_requests:int
    max_records:int
    def __post_init__(self):
        _integer(self.retained_budget_bytes);_integer(self.safety_floor_bytes)
        _integer(self.max_concurrent_requests,maximum=64);_integer(self.max_records,maximum=4096)
    @classmethod
    def from_payload(cls,value):
        require(type(value) is dict and set(value)=={'schemaVersion','retainedBudgetBytes','safetyFloorBytes','maxConcurrentRequests','maxRecords'})
        require(type(value['schemaVersion']) is int and value['schemaVersion']==1)
        return cls(value['retainedBudgetBytes'],value['safetyFloorBytes'],value['maxConcurrentRequests'],value['maxRecords'])
    def payload(self):
        return dict(schemaVersion=1,retainedBudgetBytes=self.retained_budget_bytes,
            safetyFloorBytes=self.safety_floor_bytes,maxConcurrentRequests=self.max_concurrent_requests,maxRecords=self.max_records)

class _Lease:pass

def _facts(path,*,private):
    path=Path(path).absolute()
    require(path.resolve()==path and not any(p.is_symlink() for p in (path,*path.parents)))
    s=path.lstat()
    require(stat.S_ISDIR(s.st_mode) and (os.name=='nt' or s.st_uid==os.geteuid()))
    require(not private or os.name=='nt' or stat.S_IMODE(s.st_mode)==0o700)
    return dict(path=str(path),device=s.st_dev,inode=s.st_ino,uid=s.st_uid)

def filesystem_space(path):
    if os.name=='nt':
        value=shutil.disk_usage(path);return value.total,value.free
    value=os.statvfs(path);return value.f_blocks*value.f_frsize,value.f_bavail*value.f_frsize

def _schema(db):return tuple(db.execute("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"))

def _expected_schema():
    with closing(sqlite3.connect(':memory:')) as db:
        for sql in DDL:db.execute(sql)
        return _schema(db)

def _read_private(path,cap):
    path=Path(path);require(path.resolve()==path and not path.is_symlink())
    s=path.lstat();require(stat.S_ISREG(s.st_mode) and s.st_nlink==1 and 0<s.st_size<=cap
        and (os.name=='nt' or s.st_uid==os.geteuid() and stat.S_IMODE(s.st_mode)==0o600))
    fd=os.open(path,os.O_RDONLY|getattr(os,'O_NOFOLLOW',0)|getattr(os,'O_NONBLOCK',0))
    try:
        require((os.fstat(fd).st_dev,os.fstat(fd).st_ino)==(s.st_dev,s.st_ino))
        with os.fdopen(fd,'rb',closefd=False) as f:value=f.read(cap+1)
        after=path.lstat()
        require((s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns)==
            (after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns))
        return value,(s.st_dev,s.st_ino)
    finally:os.close(fd)

def startup_guard(state_root,payload):
    """Read-only latch BEFORE Queue/TaskStore constructors or clock/schema IO.

    Unmarked absent policy retains legacy defaults. This cannot make already
    built pre-feature binaries honor a new latch; downgrade control is separate.
    """
    root=Path(state_root).absolute();dbpath=root/'viewer-spool-capacity.sqlite';marker=root/'viewer-spool-capacity.json'
    dbexists,marked=os.path.lexists(dbpath),os.path.lexists(marker)
    if not dbexists and not marked:
        if payload is not None:SpoolPolicy.from_payload(payload)
        return
    require(dbexists and marked and payload is not None,'latched_spool_policy_required')
    policy=SpoolPolicy.from_payload(payload)
    _facts(root,private=False)
    require(not any(os.path.lexists(str(dbpath)+s) for s in ('-journal','-wal','-shm')),'spool_reconciliation_required')
    raw,identity=_read_private(dbpath,PAGE_CAP*4096)
    markerraw,_=_read_private(marker,4096);value=json.loads(markerraw)
    require(encoded(value).encode()==markerraw and set(value)=={'schemaVersion','databaseIdentity','bindingSha256'}
        and type(value['schemaVersion']) is int and value['schemaVersion']==1 and value['databaseIdentity']==list(identity))
    with closing(sqlite3.connect(dbpath.as_uri()+'?mode=ro&immutable=1',uri=True)) as db:
        require(_schema(db)==_expected_schema(),'spool_schema_changed')
        require(db.execute('PRAGMA journal_mode').fetchone()[0]=='delete' and db.execute('PRAGMA page_size').fetchone()[0]==4096
            and db.execute('PRAGMA page_count').fetchone()[0]<=PAGE_CAP)
        row=db.execute('SELECT value FROM spool_binding WHERE id=1').fetchone();require(row is not None)
        require(hashlib.sha256(row[0].encode()).hexdigest()==value['bindingSha256'])
        binding=json.loads(row[0]);require(encoded(binding)==row[0] and binding['policy']==asdict(policy))
        require(encoded(binding['roots'][0])==encoded(_facts(root,private=False)))
        for item in binding['roots'][1:]:require(encoded(item)==encoded(_facts(item['path'],private=True)))
    require(_read_private(dbpath,PAGE_CAP*4096)==(raw,identity),'spool_changed_during_startup_check')

class SpoolCapacity:
    def __init__(self,queue,state_root,spool_root,tasks_root,policy,*,probe=filesystem_space):
        from .prototype import Queue
        require(type(queue) is Queue and type(policy) is SpoolPolicy)
        self.queue,self.policy,self.probe=queue,policy,probe
        self.state,self.spool,self.tasks=map(lambda p:Path(p).absolute(),(state_root,spool_root,tasks_root))
        require(Path(queue.path).absolute().parent==self.state)
        self.roots=(_facts(self.state,private=False),_facts(self.spool,private=True),_facts(self.tasks,private=True))
        require(len({r['device'] for r in self.roots})==1,'spool_requires_one_accounted_filesystem')
        self.path=self.state/'viewer-spool-capacity.sqlite'
        self.marker=self.state/'viewer-spool-capacity.json'
        startup_guard(self.state,policy.payload())
        with _LOCKS_LOCK:self.lock=_LOCKS.setdefault(str(self.path),threading.RLock())
        self.owner_check()
        with self.lock:
            fresh=not os.path.lexists(self.path)
            if fresh:
                fd=os.open(self.path,os.O_RDWR|os.O_CREAT|os.O_EXCL|getattr(os,'O_NOFOLLOW',0),0o600);os.close(fd)
            self.file_identity=self._file()
            with self._db(initialize=fresh) as db:
                if fresh:
                    total,available=self._space()
                    # An upper bound for ALL preexisting spool bytes, without
                    # reading client files. Includes unrelated FS usage too.
                    initial=max(0,total-available)
                    value=dict(schemaVersion=1,roots=self.roots,policy=asdict(policy),initialRetainedUpperBound=initial)
                    db.execute('INSERT INTO spool_binding VALUES(1,?)',(encoded(value),))
                raw=db.execute('SELECT value FROM spool_binding WHERE id=1').fetchone()
                require(raw is not None and len(raw[0].encode())<=8192)
                value=json.loads(raw[0]);require(encoded(value)==raw[0] and set(value)=={'schemaVersion','roots','policy','initialRetainedUpperBound'})
                require(value['schemaVersion']==1 and encoded(value['roots'])==encoded(self.roots) and value['policy']==asdict(policy))
                _integer(value['initialRetainedUpperBound'],minimum=0)
                self.binding=raw[0];self.initial=value['initialRetainedUpperBound']
            if fresh:
                marker=encoded(dict(schemaVersion=1,databaseIdentity=list(self.file_identity),
                    bindingSha256=hashlib.sha256(self.binding.encode()).hexdigest())).encode()
                fd=os.open(self.marker,os.O_WRONLY|os.O_CREAT|os.O_EXCL|getattr(os,'O_NOFOLLOW',0),0o600)
                with os.fdopen(fd,'wb') as f:f.write(marker);f.flush();os.fsync(f.fileno())
        self._sync_root()

    def _sync_root(self):
        if os.name!='nt':
            fd=os.open(self.state,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
            try:os.fsync(fd)
            finally:os.close(fd)

    def _file(self):
        require(self.path.resolve()==self.path and not self.path.is_symlink())
        s=self.path.lstat();require(stat.S_ISREG(s.st_mode) and s.st_nlink==1
            and s.st_size<=PAGE_CAP*4096 and (os.name=='nt' or s.st_uid==os.geteuid() and stat.S_IMODE(s.st_mode)==0o600))
        return (s.st_dev,s.st_ino)

    def owner_check(self):
        require(self.queue._creator_pid==os.getpid() and not self.queue._lock.closed,'spool_owner_closed')
        require(self.roots==(_facts(self.state,private=False),_facts(self.spool,private=True),_facts(self.tasks,private=True)))

    def _space(self):
        total,available=self.probe(self.state)
        _integer(total);_integer(available,minimum=0);require(available<=total)
        return total,available

    def _operations(self,db):
        rows=db.execute('SELECT id,task,generation,kind,charge,state FROM spool_operations LIMIT ?',
            (self.policy.max_records+1,)).fetchall()
        require(len(rows)<=self.policy.max_records,'spool_history_bound')
        for identifier,task,generation,kind,charge,state in rows:
            require(type(identifier) is str and str(uuid.UUID(identifier))==identifier
                and type(task) is str and str(uuid.UUID(task))==task)
            _integer(generation);_integer(charge)
            require(kind in ('upload','archive','form') and state in ('active','retained'))
        return rows

    @contextmanager
    def _db(self,*,initialize=False):
        with self.lock:
            self.owner_check();require(self._file()==self.file_identity)
            require(not any(os.path.lexists(str(self.path)+s) for s in ('-journal','-wal','-shm')),'spool_reconciliation_required')
            with closing(sqlite3.connect(self.path,timeout=5)) as db:
                db.execute('PRAGMA synchronous=FULL')
                if initialize:
                    db.execute('PRAGMA page_size=4096');db.execute('PRAGMA journal_mode=DELETE')
                require(db.execute('PRAGMA page_size').fetchone()[0]==4096 and db.execute('PRAGMA journal_mode').fetchone()[0]=='delete')
                require(db.execute('PRAGMA max_page_count='+str(PAGE_CAP)).fetchone()[0]==PAGE_CAP)
                db.execute('BEGIN IMMEDIATE')
                try:
                    if initialize:
                        for sql in DDL:db.execute(sql)
                    require(_schema(db)==_expected_schema(),'spool_schema_changed')
                    if hasattr(self,'binding'):require(db.execute('SELECT value FROM spool_binding WHERE id=1').fetchone()==(self.binding,))
                    self._operations(db)
                    yield db
                    self.owner_check();require(self._file()==self.file_identity)
                    require(_schema(db)==_expected_schema(),'spool_schema_changed')
                    self._operations(db)
                    db.commit()
                except sqlite3.Error:
                    db.rollback();raise Conflict('spool_reconciliation_required') from None
                except BaseException:db.rollback();raise

    def _check(self,lease,*,kind=None,task=None,generation=None):
        self.owner_check();require(type(lease) is _Lease)
        record=_LEASES.get(lease)
        require(record is not None and record['capacity'] is self and record['pid']==os.getpid()
            and record['thread']==threading.get_ident() and _CURRENT.get() is lease,'actual_spool_window_required')
        require((kind is None or record['kind']==kind) and (task is None or record['task']==task)
            and (generation is None or record['generation']==generation))
        return record

    @contextmanager
    def window(self,kind,task,generation,byte_bound):
        require(kind in ('upload','archive','form') and _CURRENT.get() is None)
        require(type(task) is str and str(uuid.UUID(task))==task);_integer(generation);_integer(byte_bound)
        copies=2 if kind=='upload' else 1;operation=str(uuid.uuid4())
        with self._db() as db:
            count,charge,active,promise=db.execute("SELECT COUNT(*),COALESCE(SUM(charge),0),COALESCE(SUM(state='active'),0),COALESCE(SUM(CASE WHEN state='active' THEN charge*(CASE kind WHEN 'upload' THEN 2 ELSE 1 END) ELSE 0 END),0) FROM spool_operations").fetchone()
            require(count<self.policy.max_records and active<self.policy.max_concurrent_requests,'spool_concurrency_or_history_full')
            require(self.initial+charge+byte_bound<=self.policy.retained_budget_bytes,'spool_retained_budget_full')
            require(self._space()[1]>=self.policy.safety_floor_bytes+CONTROL_ALLOWANCE+promise+copies*byte_bound,'spool_disk_capacity_unavailable')
            db.execute('INSERT INTO spool_operations VALUES(?,?,?,?,?,?)',(operation,task,generation,kind,byte_bound,'active'))
        lease=_Lease();_LEASES[lease]=dict(capacity=self,pid=os.getpid(),thread=threading.get_ident(),operation=operation,
            task=task,generation=generation,kind=kind,bound=byte_bound,used=0)
        token=_CURRENT.set(lease)
        try:yield lease
        finally:
            try:
                self._check(lease)
                with self._db() as db:
                    row=db.execute('SELECT task,generation,kind,charge,state FROM spool_operations WHERE id=?',(operation,)).fetchone()
                    require(row==(task,generation,kind,byte_bound,'active'))
                    # Caller must close synchronous writers/temporary multipart
                    # generators BEFORE this boundary. Retained upper bound
                    # remains immutable even on error or exact duplicate.
                    db.execute("UPDATE spool_operations SET state='retained' WHERE id=?",(operation,))
            finally:_CURRENT.reset(token);_LEASES.pop(lease,None)

    def consume(self,kind,task,generation,byte_count):
        _integer(byte_count,minimum=0)
        record=self._check(_CURRENT.get(),kind=kind,task=task,generation=generation)
        require(record['used']+byte_count<=record['bound'],'spool_stream_bound_exceeded')
        require(self._space()[1]>=self.policy.safety_floor_bytes+CONTROL_ALLOWANCE,'spool_disk_floor_reached')
        record['used']+=byte_count

    def check_floor(self):
        self._check(_CURRENT.get())
        require(self._space()[1]>=self.policy.safety_floor_bytes+CONTROL_ALLOWANCE,'spool_disk_floor_reached')
