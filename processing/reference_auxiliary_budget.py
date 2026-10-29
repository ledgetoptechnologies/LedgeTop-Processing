"""Inactive fresh-v4 metadata reservations; never source/file cleanup authority.

The numeric descriptor is a candidate fuse/reservation policy, NOT a proven
upper bound for all legal workloads. Normal daemon v4 admission stays disabled.
Reservations and reader history are permanent: no TTL, release, or compaction.
"""
from contextlib import contextmanager, closing
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import stat
import threading

from . import reference_indexed_fetch as indexed
from .reference_protocol import canonical_json

MIB = 1024**2
CONTROL_CAP = 4*MIB
JOBS = 32
PINS = 8192
EPOCHS = 64
NAMES = ('worker.sqlite', 'scratch.sqlite', 'outbox.sqlite', 'failure-outbox.sqlite')
PER_JOB = dict(zip(NAMES, (16*MIB, 32*MIB, 16*MIB, 16*MIB)))
EMERGENCY = 16*MIB
READER_BYTES = 64*MIB
CAPS = {name: 4*MIB+JOBS*PER_JOB[name]+EMERGENCY+(READER_BYTES if name=='scratch.sqlite' else 0)
        for name in NAMES}
SCHEMA = '''
CREATE TABLE latch(id INTEGER PRIMARY KEY CHECK(id=1),value TEXT NOT NULL);
CREATE TABLE job_holds(job TEXT PRIMARY KEY CHECK(typeof(job)='text' AND instr(job,char(0))=0 AND length(CAST(job AS BLOB)) BETWEEN 1 AND 128 AND job NOT GLOB '*[^A-Za-z0-9_-]*' AND substr(job,1,1) GLOB '[A-Za-z0-9]'),claim TEXT NOT NULL UNIQUE CHECK(typeof(claim)='text' AND instr(claim,char(0))=0 AND length(CAST(claim AS BLOB)) BETWEEN 1 AND 128 AND claim NOT GLOB '*[^A-Za-z0-9_-]*' AND substr(claim,1,1) GLOB '[A-Za-z0-9]'));
CREATE TABLE reader_holds(reference TEXT PRIMARY KEY CHECK(typeof(reference)='text' AND instr(reference,char(0))=0 AND length(CAST(reference AS BLOB)) BETWEEN 1 AND 128 AND reference NOT GLOB '*[^A-Za-z0-9_-]*' AND substr(reference,1,1) GLOB '[A-Za-z0-9]'),kind TEXT NOT NULL CHECK(kind IN ('pin','epoch')));
CREATE INDEX reader_kind ON reader_holds(kind);
CREATE TRIGGER latch_update BEFORE UPDATE ON latch BEGIN SELECT RAISE(ABORT,'immutable budget latch'); END;
CREATE TRIGGER latch_delete BEFORE DELETE ON latch BEGIN SELECT RAISE(ABORT,'immutable budget latch'); END;
CREATE TRIGGER job_update BEFORE UPDATE ON job_holds BEGIN SELECT RAISE(ABORT,'retained job hold'); END;
CREATE TRIGGER job_delete BEFORE DELETE ON job_holds BEGIN SELECT RAISE(ABORT,'retained job hold'); END;
CREATE TRIGGER reader_update BEFORE UPDATE ON reader_holds BEGIN SELECT RAISE(ABORT,'retained reader hold'); END;
CREATE TRIGGER reader_delete BEFORE DELETE ON reader_holds BEGIN SELECT RAISE(ABORT,'retained reader hold'); END;
'''


class AuxiliaryHeld(ValueError):
    pass


def require(value, message='auxiliary_budget_held'):
    if not value: raise AuxiliaryHeld(message)


def candidate_descriptor():
    return dict(schemaVersion=1,policyId='v4-auxiliary-budget-candidate-v1',
        qualifiedForDaemon=False,jobs=JOBS,pins=PINS,epochs=EPOCHS,
        perJobBytes=dict(PER_JOB),emergencyBytesPerStore=EMERGENCY,
        readerHistoryBytes=READER_BYTES,storeCaps=dict(CAPS),controlCap=CONTROL_CAP)


def _fact(path, cap):
    require(path.is_absolute() and path.resolve()==path and not any(p.is_symlink() for p in (path,*path.parents)),
            'auxiliary_path_changed')
    info=path.lstat()
    require(stat.S_ISREG(info.st_mode) and info.st_nlink==1 and info.st_size<=cap
            and (os.name=='nt' or info.st_uid==os.geteuid() and stat.S_IMODE(info.st_mode)==0o600),
            'auxiliary_file_changed')
    require(not any(os.path.lexists(str(path)+suffix) for suffix in ('-wal','-shm')),
            'auxiliary_wal_held')
    return [info.st_dev, info.st_ino]


def _id(value):
    require(type(value) is str and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,127}',value),
            'auxiliary_identity_required')
    return value


def reject_unbudgeted(path, budget):
    """New-source default writer fence; installed old images remain unqualified."""
    if budget is None:
        require(not os.path.lexists(Path(path).absolute().parent/'auxiliary-budget.sqlite'),
                'auxiliary_latched_budget_required')
    else:
        require(type(budget) is AuxiliaryBudget,'actual_auxiliary_budget_required')
        budget.check_path(path)


@contextmanager
def writer_scope(path,budget):
    reject_unbudgeted(path,budget)
    if budget is None:
        yield lambda db: None
    else:
        with budget.writer(path) as configure:yield configure


class AuxiliaryBudget:
    def __init__(self, *args, **kwargs):
        raise AuxiliaryHeld('explicit_auxiliary_factory_required')

    def _identity(self):
        require(self.pid==os.getpid(),'auxiliary_process_changed')
        root,identity=indexed._root(self.root,self.owner)
        require(identity==self.context.root_identity and
                _fact(Path(self.context.driver_path),self.context.policy.database_cap)==list(self.context.driver_identity),
                'auxiliary_profile_identity_changed')
        require(_fact(self.path,CONTROL_CAP)==self.control_identity,'auxiliary_control_identity_changed')
        for name, identity in self.store_identities.items():
            require(_fact(root/name,CAPS[name])==identity,'auxiliary_store_identity_changed')
        binding=json.loads(self.context.binding_json);scratch=Path(binding['scratchRoot'])
        require(scratch.is_absolute() and scratch.resolve()==scratch and
                not any(v.is_symlink() for v in (scratch,*scratch.parents)), 'auxiliary_scratch_changed')
        info=scratch.stat()
        require(stat.S_ISDIR(info.st_mode) and [info.st_dev,info.st_ino]==binding['scratchRootIdentity'],
                'auxiliary_scratch_changed')
        require(os.name=='nt' or info.st_uid==os.geteuid() and stat.S_IMODE(info.st_mode)==0o700,
                'auxiliary_scratch_changed')

    def check_path(self,path):
        self._identity();path=Path(path).absolute()
        require(path.parent==self.root and path.name in NAMES,'auxiliary_store_not_bound')
        return path.name

    @contextmanager
    def _transaction(self):
        # A single retained owner plus this shared lock serializes pre-claim,
        # pre-reader and all four actual writers. Never acquire ReaderSession's
        # mutex from inside this lock.
        require(self.pid==os.getpid(),'auxiliary_process_changed')
        with self.mutex:
            self._identity()
            require(not os.path.lexists(str(self.path)+'-journal'),'auxiliary_control_recovery_required')
            with closing(sqlite3.connect(self.path, isolation_level=None,timeout=5)) as db:
                _connection(db,CONTROL_CAP)
                db.execute('BEGIN IMMEDIATE')
                try:
                    self._validate(db)
                    yield db
                    self._validate(db);self._identity();db.commit()
                except BaseException:
                    db.rollback();raise

    def _validate(self,db,*,full=False):
        require(_catalog(db)==self.catalog,'auxiliary_control_schema_changed')
        row=db.execute('SELECT value FROM latch WHERE id=1').fetchall()
        require(row==[(self.latch,)],'auxiliary_latch_changed')
        require(db.execute('SELECT COUNT(*) FROM job_holds').fetchone()[0]<=JOBS,
                'auxiliary_job_limit')
        counts=dict(db.execute('SELECT kind,COUNT(*) FROM reader_holds GROUP BY kind'))
        require(set(counts)<= {'pin','epoch'} and counts.get('pin',0)<=PINS and counts.get('epoch',0)<=EPOCHS,
                'auxiliary_reader_limit')
        if full:
            # Cold-once full row audit. Exact immutable CHECKs reject malformed
            # future inserts; no per-object Python re-materialization of the
            # entire reader history on normal writer bookkeeping.
            require(db.execute("SELECT 1 FROM job_holds WHERE typeof(job)!='text' OR typeof(claim)!='text' "
                "OR length(CAST(job AS BLOB))>128 OR length(CAST(claim AS BLOB))>128 LIMIT 1").fetchone() is None,
                'auxiliary_job_identity_changed')
            for job,claim in db.execute('SELECT job,claim FROM job_holds LIMIT 33'):
                _id(job);_id(claim)
            require(db.execute("SELECT 1 FROM reader_holds WHERE typeof(reference)!='text' OR typeof(kind)!='text' "
                "OR length(CAST(reference AS BLOB))>128 LIMIT 1").fetchone() is None,
                'auxiliary_reader_identity_changed')
            for reference,kind in db.execute('SELECT reference,kind FROM reader_holds LIMIT '+str(PINS+EPOCHS+1)):
                _id(reference);require(kind in ('pin','epoch'),'auxiliary_reader_kind')

    def reserve_job(self,job,claim):
        job,claim=_id(job),_id(claim)
        with self._transaction() as db:
            old=db.execute('SELECT claim FROM job_holds WHERE job=?',(job,)).fetchone()
            if old:
                require(old==(claim,),'auxiliary_claim_changed');return
            require(db.execute('SELECT COUNT(*) FROM job_holds').fetchone()[0]<JOBS,'auxiliary_job_limit')
            self.check_capacity()
            db.execute('INSERT INTO job_holds VALUES(?,?)',(job,claim))

    def require_lifecycle_qualification(self):
        # A durable reservation is not a proof that every legal manifest and
        # retry/control path fits. Keep the actual pre-claim seam closed until
        # the full-life-cycle stress/failure budget has independent acceptance.
        raise AuxiliaryHeld('auxiliary_lifecycle_bounds_unqualified')

    def reserve_reader(self,reference,kind):
        reference=_id(reference);require(kind in ('pin','epoch'),'auxiliary_reader_kind')
        with self._transaction() as db:
            old=db.execute('SELECT kind FROM reader_holds WHERE reference=?',(reference,)).fetchone()
            if old:
                require(old==(kind,),'auxiliary_reader_changed');return
            limit=PINS if kind=='pin' else EPOCHS
            require(db.execute('SELECT COUNT(*) FROM reader_holds WHERE kind=?',(kind,)).fetchone()[0]<limit,
                    'auxiliary_reader_limit')
            self.check_capacity()
            with closing(sqlite3.connect((self.root/'scratch.sqlite').as_uri()+'?mode=ro&immutable=1',uri=True)) as scratch:
                allocated=scratch.execute('PRAGMA page_count').fetchone()[0]*4096
                require(allocated+MIB<=CAPS['scratch.sqlite']-EMERGENCY,'auxiliary_reader_emergency_held')
            db.execute('INSERT INTO reader_holds VALUES(?,?)',(reference,kind))

    def remaining_physical(self):
        self._identity();allocated=0
        for path,cap in [(self.path,CONTROL_CAP)]+[(self.root/n,CAPS[n]) for n in NAMES]:
            allocated+=getattr(path.stat(),'st_blocks',0)*512
            journal=Path(str(path)+'-journal')
            if os.path.lexists(journal):
                _fact(journal,2*cap);allocated+=getattr(journal.stat(),'st_blocks',0)*512
        # Allocated pages include freelist. Current free bytes have already
        # paid allocated files/journals, so subtract each future hold once.
        return max(0,3*(CONTROL_CAP+sum(CAPS.values()))-allocated)

    def check_capacity(self):
        """Actual device-grouped free space, retained ledger holds, no clock writes.

        The driver and five-store future physical allowances are global holds,
        not per-job additions. Existing blocks are deducted exactly once.
        """
        self._identity();binding=json.loads(self.context.binding_json)
        with closing(sqlite3.connect((self.root/'scratch.sqlite').as_uri()+'?mode=ro&immutable=1',uri=True)) as db:
            rows=db.execute('SELECT remaining FROM reservations LIMIT 100001').fetchall()
            rows+=db.execute('SELECT bytes FROM control_charges LIMIT 100001').fetchall()
            require(len(rows)<=100000,'auxiliary_ledger_bound')
            require(all(type(v[0]) is int and 0<=v[0]<=2**53-1 for v in rows),'auxiliary_ledger_hold_changed')
            outstanding=sum(v[0] for v in rows)
            require(outstanding<=2**53-1,'auxiliary_ledger_hold_changed')
        driver=Path(self.context.driver_path);allocated=0
        for path in (driver,Path(str(driver)+'-journal')):
            if os.path.lexists(path):
                _fact(path,2*self.context.policy.database_cap)
                allocated+=getattr(path.stat(),'st_blocks',0)*512
        driver_remaining=max(0,self.context.policy.physical_allowance-allocated)
        floor=binding.get('policy',{}).get('safetyFloor',0)
        require(type(floor) is int and 0<=floor<=2**53-1,'auxiliary_floor_changed')
        scratch=Path(binding['scratchRoot']);shared=scratch.stat().st_dev==self.root.stat().st_dev
        state_free=shutil.disk_usage(self.root).free
        require(type(state_free) is int and state_free>=0,'auxiliary_free_unavailable')
        require(state_free>=self.remaining_physical()+driver_remaining+floor+(outstanding if shared else 0),
                'auxiliary_physical_capacity_held')
        if not shared:
            scratch_free=shutil.disk_usage(scratch).free
            require(type(scratch_free) is int and scratch_free>=outstanding+floor,'auxiliary_physical_capacity_held')

    @contextmanager
    def writer(self,path):
        name=self.check_path(path)
        with self._transaction():
            require(not os.path.lexists(str(path)+'-journal'),'auxiliary_store_recovery_required')
            self.check_capacity()
            yield lambda db: _connection(db,CAPS[name])


def _connection(db,cap):
    require(db.execute('PRAGMA journal_mode').fetchone()[0]=='delete','auxiliary_delete_required')
    require(db.execute('PRAGMA page_size').fetchone()[0]==4096,'auxiliary_page_size_changed')
    require(db.execute('PRAGMA page_count').fetchone()[0]<=cap//4096,'auxiliary_page_cap')
    require(db.execute('PRAGMA max_page_count='+str(cap//4096)).fetchone()[0]==cap//4096,'auxiliary_page_cap')
    db.execute('PRAGMA synchronous=FULL')


def _catalog(db):
    require(db.execute('SELECT COUNT(*) FROM main.sqlite_master').fetchone()[0]<=32,'auxiliary_schema_bound')
    require(not db.execute('SELECT 1 FROM temp.sqlite_master LIMIT 1').fetchone(),'auxiliary_temp_held')
    require([row[1] for row in db.execute('PRAGMA database_list')] in (['main'],['main','temp']),
            'auxiliary_attached_held')
    return list(db.execute('SELECT type,name,tbl_name,sql FROM main.sqlite_master ORDER BY type,name'))


def open_budget(context,owner):
    """Read existing completed latch; never initialize or adopt an orphan DB."""
    require(type(context) is indexed.IndexedFetchContext and context.policy.policy_id==indexed.RETIRING_POLICY_ID,
            'fresh_v4_auxiliary_profile_required')
    binding=json.loads(context.binding_json)
    current=indexed.read_only_profile(context.driver_path,bootstrap_id=context.bootstrap_id,binding=binding,owner=owner)
    require(current==context,'auxiliary_profile_changed')
    root,_=indexed._root(Path(context.driver_path).parent,owner);path=root/'auxiliary-budget.sqlite'
    control_identity=_fact(path,CONTROL_CAP)
    require(not os.path.lexists(str(path)+'-journal'),'auxiliary_control_recovery_required')
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro&immutable=1',uri=True)) as db:
        catalog=_catalog(db)
        with closing(sqlite3.connect(':memory:')) as expected:
            expected.executescript(SCHEMA);require(catalog==_catalog(expected),'auxiliary_control_schema_changed')
        row=db.execute('SELECT value FROM latch WHERE id=1').fetchone();require(row is not None,'auxiliary_latch_missing')
        latch=json.loads(row[0]);require(canonical_json(latch).decode()==row[0],'auxiliary_latch_changed')
    stores={n:_fact(root/n,CAPS[n]) for n in NAMES}
    expected=dict(schemaVersion=1,descriptor=candidate_descriptor(),bootstrapId=context.bootstrap_id,
        bindingSha256=context.binding_sha256,rootIdentity=list(context.root_identity),driverIdentity=list(context.driver_identity),
        controlIdentity=control_identity,stores=stores)
    require(canonical_json(latch)==canonical_json(expected),'auxiliary_latch_changed')
    value=object.__new__(AuxiliaryBudget);value.root=root;value.path=path;value.owner=owner;value.context=context
    value.pid=os.getpid();value.mutex=threading.RLock();value.control_identity=control_identity
    value.store_identities=stores;value.latch=row[0];value.catalog=catalog
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro&immutable=1',uri=True)) as db:value._validate(db,full=True)
    with value._transaction():pass
    return value


def initialize_fresh_budget(context,owner):
    """Explicit empty-state latch. A crash leaving an unknown file stays held.

    This prospective bootstrap primitive is not wired into the normal daemon.
    Existing schema/config is never migrated; marker creation isn't authority
    to release physical files or to adopt unknown prior SQLite state.
    """
    require(type(context) is indexed.IndexedFetchContext and context.policy.policy_id==indexed.RETIRING_POLICY_ID,
            'fresh_v4_auxiliary_profile_required')
    binding=json.loads(context.binding_json)
    require(indexed.read_only_profile(context.driver_path,bootstrap_id=context.bootstrap_id,binding=binding,owner=owner)==context,
            'auxiliary_profile_changed')
    root,_=indexed._root(Path(context.driver_path).parent,owner);path=root/'auxiliary-budget.sqlite'
    stores={n:_fact(root/n,CAPS[n]) for n in NAMES}
    # Schemas may be constructed already, but no business rows may be adopted.
    # Only constructor configuration/input-accounting markers are exempt.
    exempt={'metadata','input_accounting','reader_session_binding'}
    for name in NAMES:
        with closing(sqlite3.connect((root/name).as_uri()+'?mode=ro&immutable=1',uri=True)) as db:
            require(not os.path.lexists(str(root/name)+'-journal'),'auxiliary_store_recovery_required')
            require(db.execute('PRAGMA journal_mode').fetchone()[0]=='delete','auxiliary_delete_required')
            tables=db.execute("SELECT name FROM main.sqlite_master WHERE type='table'").fetchall()
            require(len(tables)<=64,'auxiliary_schema_bound')
            for (table,) in tables:
                require(re.fullmatch('[A-Za-z_][A-Za-z0-9_]*',table),'auxiliary_table_changed')
                if table not in exempt:
                    require(db.execute('SELECT 1 FROM "'+table+'" LIMIT 1').fetchone() is None,'auxiliary_existing_business_held')
    with closing(sqlite3.connect(Path(context.driver_path).as_uri()+'?mode=ro&immutable=1',uri=True)) as db:
        require(db.execute('SELECT 1 FROM driver_jobs LIMIT 1').fetchone() is None,'auxiliary_existing_business_held')
        for (table,) in db.execute("SELECT name FROM main.sqlite_master WHERE type='table' AND name LIKE 'indexed_%'"):
            if table!='indexed_profile':
                require(re.fullmatch('[A-Za-z_][A-Za-z0-9_]*',table),'auxiliary_table_changed')
                require(db.execute('SELECT 1 FROM "'+table+'" LIMIT 1').fetchone() is None,'auxiliary_existing_business_held')
    fd=os.open(path,os.O_CREAT|os.O_EXCL|os.O_RDWR|getattr(os,'O_NOFOLLOW',0),0o600)
    try:os.fsync(fd)
    finally:os.close(fd)
    identity=_fact(path,CONTROL_CAP)
    prospective=object.__new__(AuxiliaryBudget)
    prospective.root=root;prospective.path=path;prospective.context=context;prospective.owner=owner
    prospective.pid=os.getpid();prospective.control_identity=identity;prospective.store_identities=stores
    prospective.check_capacity()
    latch=dict(schemaVersion=1,descriptor=candidate_descriptor(),bootstrapId=context.bootstrap_id,
        bindingSha256=context.binding_sha256,rootIdentity=list(context.root_identity),driverIdentity=list(context.driver_identity),
        controlIdentity=identity,stores=stores)
    with closing(sqlite3.connect(path,isolation_level=None)) as db:
        _connection(db,CONTROL_CAP);db.executescript('BEGIN IMMEDIATE;'+SCHEMA)
        db.execute('INSERT INTO latch VALUES(1,?)',(canonical_json(latch).decode(),));db.commit()
    fd=os.open(root,os.O_RDONLY|getattr(os,'O_DIRECTORY',0))
    try:os.fsync(fd)
    finally:os.close(fd)
    return open_budget(context,owner)
