"""Fresh, explicit indexed fetch journal. No engine, network or cleanup actions.

Only authenticated, fully validated claims/grants supplied by trusted driver
composition may be sealed. Stored hashes are integrity checks, not custody or
release authority. Default/v1 drivers never create these tables.
"""
from contextlib import closing
from dataclasses import dataclass, field
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import sqlite3
import stat
import time
import uuid

from . import reference_protocol as p

PROTOCOL = 'ltds-indexed-worker-fetch-v1'
POLICY_ID = 'indexed-worker-fetch-v1'
PAGE_SIZE = 4096
PAGE_CAP = 16384
DB_CAP = PAGE_SIZE * PAGE_CAP
JOB_CHARGE = 32 * 1024**2
PHYSICAL_ALLOWANCE = 3 * DB_CAP
NON_JOB_ALLOWANCE = 4 * 1024**2
LIMITS = dict(maxObjects=10000, databasePageCap=PAGE_CAP, pageSize=PAGE_SIZE,
    claimBytes=p.MAX_BODY_BYTES, grantBytes=2*1024**2, objectRowBytes=2048,
    progressBytes=8192, perObjectAttempts=3, totalAttempts=10010,
    retainedJobs=64, jobControlBytes=JOB_CHARGE, nonJobControlBytes=NON_JOB_ALLOWANCE,
    physicalAllowanceBytes=PHYSICAL_ALLOWANCE)

BASE_SCHEMA = (
    "CREATE TABLE driver_jobs (created INTEGER PRIMARY KEY AUTOINCREMENT, job TEXT UNIQUE NOT NULL, claim_request TEXT UNIQUE NOT NULL, stage TEXT NOT NULL, data TEXT NOT NULL CHECK(typeof(data)='text' AND length(CAST(data AS BLOB))<=8192), background_checked REAL NOT NULL DEFAULT 0)",
    "CREATE TABLE readiness_tasks (uuid TEXT PRIMARY KEY)",
    "CREATE TABLE driver_binding (singleton INTEGER PRIMARY KEY,binding TEXT NOT NULL)",
    "CREATE TABLE driver_engine_pin (singleton INTEGER PRIMARY KEY,pin TEXT NOT NULL)",
)
INDEXED_SCHEMA = (
    "CREATE TABLE indexed_profile (singleton INTEGER PRIMARY KEY CHECK(singleton=1),record TEXT NOT NULL CHECK(length(CAST(record AS BLOB))<=16384))",
    "CREATE TABLE indexed_quota (job TEXT PRIMARY KEY,charge INTEGER NOT NULL CHECK(charge>0))",
    "CREATE TABLE indexed_claims (job TEXT PRIMARY KEY,header TEXT NOT NULL CHECK(length(CAST(header AS BLOB))<=8192),envelope TEXT NOT NULL CHECK(length(CAST(envelope AS BLOB))<=4194304),sha TEXT NOT NULL,count INTEGER NOT NULL CHECK(count BETWEEN 1 AND 10000),bytes INTEGER NOT NULL,closure TEXT NOT NULL)",
    "CREATE TABLE indexed_objects (job TEXT NOT NULL,seq INTEGER NOT NULL CHECK(seq BETWEEN 0 AND 9999),id TEXT NOT NULL,descriptor TEXT NOT NULL CHECK(length(CAST(descriptor AS BLOB))<=2048),PRIMARY KEY(job,seq),UNIQUE(job,id))",
    "CREATE TABLE indexed_seals (job TEXT PRIMARY KEY,sha TEXT NOT NULL)",
    "CREATE TABLE indexed_reservations (job TEXT PRIMARY KEY,record TEXT NOT NULL CHECK(length(CAST(record AS BLOB))<=4096))",
    "CREATE TABLE indexed_grants (job TEXT PRIMARY KEY,record TEXT NOT NULL CHECK(length(CAST(record AS BLOB))<=2097152),sha TEXT NOT NULL)",
    "CREATE TABLE indexed_fetch (job TEXT PRIMARY KEY,next INTEGER NOT NULL CHECK(next BETWEEN 0 AND 10000),bytes INTEGER NOT NULL CHECK(bytes>=0),attempts INTEGER NOT NULL CHECK(attempts BETWEEN 0 AND 10010),revision INTEGER NOT NULL CHECK(revision>=0),clock REAL NOT NULL)",
    "CREATE TABLE indexed_attempts (job TEXT NOT NULL,seq INTEGER NOT NULL,attempt INTEGER NOT NULL CHECK(attempt BETWEEN 1 AND 3),intent TEXT NOT NULL UNIQUE CHECK(typeof(intent)='text' AND length(CAST(intent AS BLOB))=32 AND intent NOT GLOB '*[^0-9a-f]*'),state TEXT NOT NULL CHECK(state IN ('started','confirmed')),PRIMARY KEY(job,seq,attempt))",
)
TRIGGERS = []
for _table in ('indexed_profile','indexed_claims','indexed_objects','indexed_seals','indexed_reservations','indexed_grants','indexed_quota'):
    for _action in ('UPDATE','DELETE'):
        TRIGGERS.append(f"CREATE TRIGGER {_table}_{_action.lower()} BEFORE {_action} ON {_table} BEGIN SELECT RAISE(ABORT,'immutable indexed identity'); END")
TRIGGERS.append("CREATE TRIGGER indexed_objects_sealed_insert BEFORE INSERT ON indexed_objects WHEN EXISTS(SELECT 1 FROM indexed_seals WHERE job=NEW.job) BEGIN SELECT RAISE(ABORT,'sealed object closure'); END")
TRIGGERS.append("CREATE TRIGGER indexed_attempt_identity_update BEFORE UPDATE OF job,seq,attempt,intent ON indexed_attempts BEGIN SELECT RAISE(ABORT,'immutable attempt identity'); END")
TRIGGERS.append("CREATE TRIGGER indexed_attempt_identity_delete BEFORE DELETE ON indexed_attempts BEGIN SELECT RAISE(ABORT,'immutable attempt identity'); END")
SCHEMA = BASE_SCHEMA + INDEXED_SCHEMA + tuple(TRIGGERS)


class IndexedFetchHeld(ValueError):
    pass


def require(condition, reason='indexed_fetch_unavailable'):
    if not condition:
        raise IndexedFetchHeld(reason)


def encoded(value):
    return p.canonical_json(value)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def compiled_profile_descriptor():
    schema_sha = p.digest(SCHEMA)
    policy = dict(schemaVersion=1, protocol=PROTOCOL, policyId=POLICY_ID, schemaSha256=schema_sha, limits=dict(LIMITS))
    return dict(policy, policySha256=p.digest(policy))


@dataclass(frozen=True)
class IndexedFetchContext:
    bootstrap_id: str
    binding_sha256: str
    driver_identity: tuple
    root_identity: tuple
    binding_json: bytes = field(repr=False)
    driver_path: str = field(default='',repr=False)


class IndexedCapacityProbe:
    """Trusted v2 composition: one remaining metadata hold on a shared device."""
    def __init__(self,context,owner,scratch_root):
        require(type(context) is IndexedFetchContext)
        self.context=context;self.owner=owner;self.scratch_root=Path(scratch_root).absolute()
        self.root=Path(json.loads(context.binding_json)['stateRoot'])
        self.path=Path(context.driver_path).absolute()
        require(self.path.parent==self.root,'indexed_capacity_identity_changed')
        self._scratch_identity=_identity(self.scratch_root.stat())
        self._check()

    def _check(self):
        root,root_id=_root(self.root,self.owner)
        require(root_id==self.context.root_identity and _identity(_file(self.path))==self.context.driver_identity,
                'indexed_capacity_identity_changed')
        require(self.scratch_root.resolve()==self.scratch_root
                and not any(v.is_symlink() for v in (self.scratch_root,*self.scratch_root.parents)))
        info=self.scratch_root.stat();binding=json.loads(self.context.binding_json)
        require(stat.S_ISDIR(info.st_mode) and _identity(info)==self._scratch_identity
                and str(self.scratch_root)==binding['scratchRoot']
                and (os.name=='nt' or info.st_uid==os.geteuid() and stat.S_IMODE(info.st_mode)==0o700),
                'indexed_capacity_identity_changed')
        if 'scratchRootIdentity' in binding:require(tuple(binding['scratchRootIdentity'])==_identity(info))
        return root.stat().st_dev==info.st_dev

    def remaining(self):
        self._check();allocated=0
        for path in (self.path,Path(str(self.path)+'-journal')):
            if os.path.lexists(path):
                info=path.lstat();allocated+=getattr(info,'st_blocks',0)*512
        return max(0,PHYSICAL_ALLOWANCE-allocated)

    def __call__(self,root):
        require(Path(root).absolute()==self.scratch_root,'indexed_capacity_root_changed')
        shared=self._check();free=shutil.disk_usage(self.scratch_root).free
        require(type(free) is int and free>=0,'indexed_capacity_probe_unavailable')
        return max(0,free-self.remaining()) if shared else free


def indexed_capacity_probe(context,owner,scratch_root):
    return IndexedCapacityProbe(context,owner,scratch_root)


def _identity(info):
    return (info.st_dev, info.st_ino)


def _root(path, owner):
    from ops.reference_worker_daemon import DaemonStateLock
    require(type(owner) is DaemonStateLock,'actual_indexed_state_owner_required')
    root = Path(path).absolute()
    owner.check(root)
    require(root.resolve()==root and not any(v.is_symlink() for v in (root,*root.parents)))
    info=root.stat()
    require(stat.S_ISDIR(info.st_mode) and (os.name=='nt' or info.st_uid==os.geteuid() and stat.S_IMODE(info.st_mode)==0o700))
    return root,_identity(info)


def _file(path):
    path=Path(path).absolute();require(path.resolve()==path and not any(v.is_symlink() for v in (path,*path.parents)))
    info=path.lstat()
    require(stat.S_ISREG(info.st_mode) and info.st_nlink==1 and info.st_size<=DB_CAP
            and (os.name=='nt' or info.st_uid==os.geteuid() and stat.S_IMODE(info.st_mode)==0o600))
    require(not any(os.path.lexists(str(path)+v) for v in ('-wal','-shm')))
    rollback=Path(str(path)+'-journal')
    if os.path.lexists(rollback):
        item=rollback.lstat()
        require(stat.S_ISREG(item.st_mode) and item.st_nlink==1 and item.st_size<=2*DB_CAP
                and (os.name=='nt' or item.st_uid==os.geteuid() and stat.S_IMODE(item.st_mode)==0o600))
    return info


def _schema(db):
    require(db.execute('SELECT COUNT(*) FROM sqlite_master').fetchone()[0]<=128,'indexed_schema_bound')
    require(not db.execute('SELECT 1 FROM sqlite_master WHERE length(CAST(sql AS BLOB))>16384 LIMIT 1').fetchone(),'indexed_schema_bound')
    return [tuple(row) for row in db.execute("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name LIMIT 128")]


def _expected_schema():
    with closing(sqlite3.connect(':memory:')) as db:
        for statement in SCHEMA:db.execute(statement)
        return _schema(db)


def _profile(bootstrap_id,binding,root_identity,driver_identity):
    require(str(uuid.UUID(bootstrap_id))==bootstrap_id)
    raw=encoded(binding);require(len(raw)<=16384)
    return dict(schemaVersion=1,protocol=PROTOCOL,bootstrapId=bootstrap_id,bindingSha256=sha(raw),
                rootIdentity=list(root_identity),driverIdentity=list(driver_identity),descriptor=compiled_profile_descriptor())


def initialize_fresh_profile(driver_path, *, bootstrap_id, binding, owner):
    path=Path(driver_path).absolute();root,root_id=_root(path.parent,owner)
    require(not os.path.lexists(path),'indexed_existing_database_requires_reconciliation')
    fd=os.open(path,os.O_CREAT|os.O_EXCL|os.O_RDWR|getattr(os,'O_NOFOLLOW',0),0o600)
    try:identity=_identity(os.fstat(fd));os.fsync(fd)
    finally:os.close(fd)
    with closing(sqlite3.connect(path)) as db:
        db.execute('PRAGMA journal_mode=DELETE');db.execute('PRAGMA synchronous=FULL')
        require(db.execute('PRAGMA page_size').fetchone()[0]==PAGE_SIZE)
        require(db.execute('PRAGMA max_page_count='+str(PAGE_CAP)).fetchone()[0]==PAGE_CAP)
        db.execute('BEGIN IMMEDIATE')
        for statement in SCHEMA:db.execute(statement)
        db.execute('INSERT INTO indexed_profile VALUES(1,?)',(encoded(_profile(bootstrap_id,binding,root_id,identity)).decode(),))
        db.commit()
    fd=os.open(root,os.O_RDONLY|getattr(os,'O_DIRECTORY',0))
    try:os.fsync(fd)
    finally:os.close(fd)
    return read_only_profile(path,bootstrap_id=bootstrap_id,binding=binding,owner=owner)


def validate_profile_connection(db, *, bootstrap_id, binding, root_identity, driver_identity):
    """Logical verification only; never grants a live physical context."""
    expected=_profile(bootstrap_id,binding,root_identity,driver_identity)
    deadline=time.monotonic()+3;db.set_progress_handler(lambda:int(time.monotonic()>deadline),1000)
    require(db.execute('PRAGMA page_size').fetchone()[0]==PAGE_SIZE and db.execute('PRAGMA page_count').fetchone()[0]<=PAGE_CAP)
    require(db.execute('PRAGMA journal_mode').fetchone()[0]=='delete')
    require(_schema(db)==_expected_schema(),'indexed_schema_changed')
    row=db.execute('SELECT record FROM indexed_profile WHERE singleton=1 AND length(CAST(record AS BLOB))<=16384').fetchone()
    require(row is not None and row[0]==encoded(expected).decode(),'indexed_profile_changed')
    require(not db.execute('SELECT 1 FROM driver_binding WHERE typeof(binding)!=\'text\' OR length(CAST(binding AS BLOB))>16384 LIMIT 1').fetchone(),'indexed_driver_binding_bound')
    driver_binding=db.execute('SELECT binding FROM driver_binding WHERE singleton=1').fetchone()
    if driver_binding is None:
        for table in ('driver_jobs','readiness_tasks','driver_engine_pin','indexed_quota','indexed_claims','indexed_objects',
                      'indexed_seals','indexed_reservations','indexed_grants','indexed_fetch','indexed_attempts'):
            require(db.execute('SELECT 1 FROM '+table+' LIMIT 1').fetchone() is None,'unbound_indexed_history')
    else:
        require(type(driver_binding[0]) is str and len(driver_binding[0].encode())<=16384)
        settings=json.loads(driver_binding[0])
        require(encoded(settings.get('inputCopyAccounting'))==encoded(dict(schemaVersion=1))
                and encoded(settings.get('indexedFetchJournal'))==encoded(compiled_profile_descriptor()),'indexed_driver_binding_changed')


def read_only_profile(driver_path, *, bootstrap_id, binding, owner):
    path=Path(driver_path).absolute();_,root_id=_root(path.parent,owner);info=_file(path)
    require(not os.path.lexists(str(path)+'-journal'),'indexed_database_recovery_required')
    expected=_profile(bootstrap_id,binding,root_id,_identity(info))
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro&immutable=1',uri=True)) as db:
        validate_profile_connection(db,bootstrap_id=bootstrap_id,binding=binding,
            root_identity=root_id,driver_identity=_identity(info))
    after=_file(path);require((info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns,info.st_ctime_ns)==
        (after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns))
    return IndexedFetchContext(bootstrap_id,expected['bindingSha256'],_identity(info),root_id,encoded(binding),str(path))


def reject_indexed_footprint(path):
    """Read-only disabled-profile guard; never creates an SQLite file."""
    path=Path(path)
    require(path.is_absolute() and path.resolve()==path and not any(v.is_symlink() for v in (path,*path.parents)),
            'driver_footprint_identity_unavailable')
    if not os.path.lexists(path):return
    parent=path.parent.stat()
    require(stat.S_ISDIR(parent.st_mode) and (os.name=='nt' or parent.st_uid==os.geteuid() and stat.S_IMODE(parent.st_mode)==0o700),
            'driver_footprint_identity_unavailable')
    before=path.lstat()
    require(stat.S_ISREG(before.st_mode) and before.st_nlink==1 and before.st_size<=1024**3
            and (os.name=='nt' or before.st_uid==os.geteuid() and stat.S_IMODE(before.st_mode)==0o600)
            and not any(os.path.lexists(str(path)+suffix) for suffix in ('-wal','-shm','-journal')),
            'driver_footprint_identity_unavailable')
    facts=lambda value:(value.st_dev,value.st_ino,value.st_mode,value.st_nlink,value.st_uid,
                         value.st_size,value.st_mtime_ns,value.st_ctime_ns)
    descriptor=os.open(path,os.O_RDONLY|getattr(os,'O_BINARY',0)|getattr(os,'O_NOFOLLOW',0)|getattr(os,'O_NONBLOCK',0))
    try:
        require(facts(os.fstat(descriptor))==facts(before),'driver_footprint_identity_changed')
        if before.st_size:
            require(before.st_size>=100 and os.read(descriptor,16)==b'SQLite format 3\0','driver_footprint_format_unavailable')
            with closing(sqlite3.connect(path.as_uri()+'?mode=ro&immutable=1',uri=True)) as db:
                deadline=time.monotonic()+3;db.set_progress_handler(lambda:int(time.monotonic()>deadline),1000)
                require(not db.execute("SELECT 1 FROM sqlite_master WHERE lower(name) GLOB 'indexed_*' LIMIT 1").fetchone(),
                        'indexed_profile_required')
                table=db.execute("SELECT type FROM sqlite_master WHERE lower(name)='driver_binding' LIMIT 2").fetchall()
                if table:
                    require(table==[('table',)] and db.execute('SELECT COUNT(*) FROM driver_binding').fetchone()[0]<=1,
                            'driver_footprint_binding_unavailable')
                    require(not db.execute("SELECT 1 FROM driver_binding WHERE typeof(binding)!='text' OR length(CAST(binding AS BLOB))>16384 LIMIT 1").fetchone())
                    for row in db.execute('SELECT binding FROM driver_binding LIMIT 1'):
                        value=json.loads(row[0]);require(type(value) is dict)
                        require('indexedFetchJournal' not in value,'indexed_profile_required')
        require(facts(os.fstat(descriptor))==facts(path.lstat())==facts(before),'driver_footprint_identity_changed')
    finally:os.close(descriptor)


class IndexedFetchJournal:
    def __init__(self,path,context,owner,*,clock=time.time):
        require(type(context) is IndexedFetchContext)
        self.path=Path(path).absolute();self.owner=owner;self.context=context;self.clock=clock
        actual=read_only_profile(self.path,bootstrap_id=context.bootstrap_id,binding=json.loads(context.binding_json),owner=owner)
        require(actual==context,'indexed_context_changed')
        with closing(sqlite3.connect(self.path)) as db:
            self.schema_version=db.execute('PRAGMA schema_version').fetchone()[0]
            row=db.execute('SELECT binding FROM driver_binding WHERE singleton=1').fetchone()
            self.driver_binding=None if row is None else row[0]

    def check(self,db):
        _,identity=_root(self.path.parent,self.owner);require(identity==self.context.root_identity)
        require(_identity(_file(self.path))==self.context.driver_identity,'indexed_file_replaced')
        require(db.execute('PRAGMA schema_version').fetchone()[0]==self.schema_version,'indexed_schema_changed')
        require(db.execute('PRAGMA page_size').fetchone()[0]==PAGE_SIZE and db.execute('PRAGMA page_count').fetchone()[0]<=PAGE_CAP)
        require(db.execute('PRAGMA max_page_count='+str(PAGE_CAP)).fetchone()[0]==PAGE_CAP)
        if self.driver_binding is not None:
            row=db.execute('SELECT binding FROM driver_binding WHERE singleton=1').fetchone()
            require(row is not None and row[0]==self.driver_binding,'indexed_driver_binding_changed')

    def reserve_slot(self,db,job,*,ledger=None):
        self.check(db);p._slug(job,'indexed job')
        old=db.execute('SELECT charge FROM indexed_quota WHERE job=?',(job,)).fetchone()
        if old:require(old[0]==JOB_CHARGE);return
        count,total=db.execute('SELECT COUNT(*),COALESCE(SUM(charge),0) FROM indexed_quota').fetchone()
        require(count<LIMITS['retainedJobs'] and total+JOB_CHARGE+NON_JOB_ALLOWANCE<=DB_CAP,'indexed_metadata_quota_full')
        allocated=sum(getattr(v.lstat(),'st_blocks',0)*512 for v in (self.path,Path(str(self.path)+'-journal')) if os.path.lexists(v))
        outstanding=0
        if ledger is not None:
            require(type(ledger.probe) is IndexedCapacityProbe and ledger.probe.context==self.context
                    and ledger.probe.owner is self.owner,'indexed_capacity_composition_required')
            if ledger.probe._check():
                with ledger._transaction() as scratch:
                    outstanding=scratch.execute('SELECT COALESCE(SUM(remaining),0) FROM reservations').fetchone()[0]
                    outstanding+=scratch.execute('SELECT COALESCE(SUM(bytes),0) FROM control_charges').fetchone()[0]
        free=shutil.disk_usage(self.path.parent).free
        require(type(free) is int and free>=0,'indexed_capacity_probe_unavailable')
        floor=json.loads(self.context.binding_json).get('policy',{}).get('safetyFloor',0)
        require(free-outstanding-floor>=max(0,PHYSICAL_ALLOWANCE-allocated),'indexed_metadata_disk_unavailable')
        db.execute('INSERT INTO indexed_quota VALUES(?,?)',(job,JOB_CHARGE))

    def seal_claim(self,db,job,claim_request,claim):
        self.check(db);self.reserve_slot(db,job)
        raw=encoded(claim);require(len(raw)<=LIMITS['claimBytes'],'indexed_claim_bound')
        old=db.execute('SELECT envelope FROM indexed_claims WHERE job=?',(job,)).fetchone()
        if old:require(old[0]==raw.decode(),'indexed_original_claim_changed');return
        require(claim['jobId']==job and claim['status']=='running')
        submission=claim['submission'];objects=submission['manifest']['objects']
        require(type(objects) is list and 0<len(objects)<=p.MAX_OBJECTS)
        header={key:claim[key] for key in ('jobId','generation','environment','nodeId','manifestSha256','pipelineDigest','claimToken')}
        header['claimRequestId']=claim_request
        require(len(encoded(header))<=8192)
        closure=p.digest(objects)
        db.execute('INSERT INTO indexed_claims VALUES(?,?,?,?,?,?,?)',(job,encoded(header).decode(),raw.decode(),sha(raw),len(objects),submission['manifest']['totalBytes'],closure))
        for index,obj in enumerate(objects):
            descriptor=encoded(obj);require(len(descriptor)<=LIMITS['objectRowBytes'])
            db.execute('INSERT INTO indexed_objects VALUES(?,?,?,?)',(job,index,obj['id'],descriptor.decode()))
        db.execute('INSERT INTO indexed_seals VALUES(?,?)',(job,closure))

    def claim(self,db,job):
        row=db.execute("SELECT envelope,sha FROM indexed_claims WHERE job=? AND typeof(envelope)='text' AND length(CAST(envelope AS BLOB))<=4194304",(job,)).fetchone()
        require(row is not None and len(row[0].encode())<=LIMITS['claimBytes'] and sha(row[0].encode())==row[1])
        return json.loads(row[0])

    def header(self,db,job):
        row=db.execute("SELECT header FROM indexed_claims WHERE job=? AND typeof(header)='text' AND length(CAST(header AS BLOB))<=8192",(job,)).fetchone()
        require(row is not None and len(row[0].encode())<=8192)
        return json.loads(row[0])

    def seal_reservation(self,db,job,request):
        raw=encoded(request);require(len(raw)<=4096)
        old=db.execute('SELECT record FROM indexed_reservations WHERE job=?',(job,)).fetchone()
        if old:require(old[0]==raw.decode(),'indexed_reservation_changed')
        else:db.execute('INSERT INTO indexed_reservations VALUES(?,?)',(job,raw.decode()))

    def seal_grant(self,db,job,grant):
        raw=encoded(grant);require(len(raw)<=LIMITS['grantBytes'])
        old=db.execute('SELECT record FROM indexed_grants WHERE job=?',(job,)).fetchone()
        if old:require(old[0]==raw.decode(),'indexed_grant_changed')
        else:
            db.execute('INSERT INTO indexed_grants VALUES(?,?,?)',(job,raw.decode(),sha(raw)))
            now=self.clock();require(type(now) in (int,float) and math.isfinite(now))
            db.execute('INSERT INTO indexed_fetch VALUES(?,0,0,0,0,?)',(job,now))

    def grant(self,db,job):
        row=db.execute("SELECT record,sha FROM indexed_grants WHERE job=? AND typeof(record)='text' AND length(CAST(record AS BLOB))<=2097152",(job,)).fetchone()
        require(row is not None and len(row[0].encode())<=LIMITS['grantBytes'] and sha(row[0].encode())==row[1])
        return json.loads(row[0])

    def object(self,db,job,index):
        row=db.execute("SELECT id,descriptor FROM indexed_objects WHERE job=? AND seq=? AND typeof(descriptor)='text' AND length(CAST(descriptor AS BLOB))<=2048",(job,index)).fetchone()
        require(row is not None and len(row[1].encode())<=LIMITS['objectRowBytes'],'indexed_object_missing')
        obj=json.loads(row[1]);require(obj['id']==row[0]);return obj

    def progress(self,db,job):
        row=db.execute('SELECT next,bytes,attempts,revision,clock FROM indexed_fetch WHERE job=?',(job,)).fetchone()
        require(row is not None,'indexed_progress_missing')
        return dict(zip(('next','bytes','attempts','revision','clock'),row))

    def validate_cold(self,db,job,claim,*,grant=None):
        """One bounded original-envelope/object/progress census, never a hot scan."""
        self.check(db)
        original=self.claim(db,job);header=self.header(db,job)
        row=db.execute('SELECT claim_request FROM driver_jobs WHERE job=?',(job,)).fetchone()
        require(row is not None and row[0]==header['claimRequestId'],'indexed_claim_request_changed')
        require(all(type(claim[key]) is type(original[key]) and claim[key]==original[key]
            for key in ('jobId','generation','environment','nodeId','manifestSha256','pipelineDigest','claimToken')),
            'indexed_original_claim_changed')
        require(p.canonical_json(claim['submission'])==p.canonical_json(original['submission']))
        for key in ('jobId','generation','environment','nodeId','manifestSha256','pipelineDigest','claimToken'):
            require(type(header[key]) is type(original[key]) and header[key]==original[key])
        require(not db.execute('SELECT 1 FROM indexed_claims c LEFT JOIN driver_jobs d ON d.job=c.job LEFT JOIN indexed_quota q ON q.job=c.job LEFT JOIN indexed_seals s ON s.job=c.job WHERE d.job IS NULL OR q.job IS NULL OR s.job IS NULL LIMIT 1').fetchone(),'indexed_orphan_claim')
        require(not db.execute('SELECT 1 FROM indexed_quota q LEFT JOIN driver_jobs d ON d.job=q.job WHERE d.job IS NULL LIMIT 1').fetchone(),'indexed_orphan_quota')
        for table in ('indexed_objects','indexed_reservations','indexed_grants','indexed_fetch','indexed_attempts','indexed_seals'):
            require(not db.execute('SELECT 1 FROM '+table+' x LEFT JOIN indexed_claims c ON c.job=x.job WHERE c.job IS NULL LIMIT 1').fetchone(),'indexed_orphan_record')
        objects=claim['submission']['manifest']['objects'];require(0<len(objects)<=p.MAX_OBJECTS)
        reservation=db.execute("SELECT record FROM indexed_reservations WHERE job=? AND typeof(record)='text' AND length(CAST(record AS BLOB))<=4096",(job,)).fetchone()
        require(reservation is not None and reservation[0]==encoded(claim.get('transferRequest')).decode(),'indexed_reservation_changed')
        if grant is None:grant=self.grant(db,job)
        grant_sha=db.execute('SELECT sha FROM indexed_grants WHERE job=?',(job,)).fetchone()
        require(grant_sha is not None and p.digest(grant)==grant_sha[0],'indexed_grant_changed')
        record=db.execute('SELECT count,bytes,closure FROM indexed_claims WHERE job=?',(job,)).fetchone()
        require(tuple(record)==(len(objects),sum(obj['byteSize'] for obj in objects),p.digest(objects)))
        require(db.execute('SELECT sha FROM indexed_seals WHERE job=?',(job,)).fetchone()[0]==record[2])
        seen=0
        require(not db.execute("SELECT 1 FROM indexed_objects WHERE job=? AND (typeof(descriptor)!='text' OR length(CAST(descriptor AS BLOB))>2048) LIMIT 1",(job,)).fetchone(),'indexed_object_bound')
        for row in db.execute('SELECT seq,id,descriptor FROM indexed_objects WHERE job=? ORDER BY seq LIMIT 10001',(job,)):
            require(seen<10000 and seen<len(objects),'indexed_object_count')
            require(row[0]==seen and row[1]==objects[seen]['id'] and len(row[2].encode())<=2048
                    and row[2]==encoded(objects[seen]).decode(),'indexed_object_changed')
            seen+=1
        require(seen==len(objects),'indexed_object_count')
        progress=self.progress(db,job)
        require(type(progress['next']) is int and 0<=progress['next']<=len(objects)
                and progress['bytes']==sum(obj['byteSize'] for obj in objects[:progress['next']])
                and type(progress['clock']) in (int,float) and math.isfinite(progress['clock']),'indexed_prefix_changed')
        self.validate_attempt_closure(db,job,len(objects),progress)

    def validate_attempt_closure(self,db,job,count,progress):
        """One bounded phase/cold audit; never invoked for every object."""
        self.check(db)
        require(type(count) is int and 0<count<=10000 and self.progress(db,job)==progress,'indexed_cursor_changed')
        require(db.execute('SELECT count FROM indexed_claims WHERE job=?',(job,)).fetchone()[0]==count)
        total=db.execute('SELECT COUNT(*) FROM indexed_attempts WHERE job=?',(job,)).fetchone()[0]
        require(total==progress['attempts'] and total<=min(3*count,count+10),'indexed_attempt_history_changed')
        require(not db.execute('SELECT 1 FROM indexed_attempts WHERE job=? AND (seq<0 OR seq>=?) LIMIT 1',(job,count)).fetchone(),'indexed_attempt_object_changed')
        require(not db.execute("SELECT 1 FROM indexed_attempts WHERE job=? AND (typeof(intent)!='text' OR length(CAST(intent AS BLOB))!=32 OR intent GLOB '*[^0-9a-f]*') LIMIT 1",(job,)).fetchone(),'indexed_attempt_identity_changed')
        require(not db.execute("SELECT 1 FROM indexed_attempts WHERE job=? AND (seq>? OR (seq=? AND state='confirmed') OR (seq<? AND state!='confirmed' AND attempt=(SELECT MAX(y.attempt) FROM indexed_attempts y WHERE y.job=indexed_attempts.job AND y.seq=indexed_attempts.seq))) LIMIT 1",
            (job,progress['next'],progress['next'],progress['next'])).fetchone(),'indexed_attempt_prefix_changed')
        require(not db.execute("SELECT 1 FROM indexed_attempts WHERE job=? AND state='confirmed' AND attempt!=(SELECT MAX(y.attempt) FROM indexed_attempts y WHERE y.job=indexed_attempts.job AND y.seq=indexed_attempts.seq) LIMIT 1",(job,)).fetchone(),'indexed_attempt_prefix_changed')
        require(not db.execute('SELECT 1 FROM indexed_attempts WHERE job=? GROUP BY seq HAVING MIN(attempt)!=1 OR COUNT(*)!=MAX(attempt) LIMIT 1',(job,)).fetchone(),'indexed_attempt_history_changed')
        require(db.execute('SELECT COUNT(DISTINCT seq) FROM indexed_attempts WHERE job=? AND seq<?',(job,progress['next'])).fetchone()[0]==progress['next'],'indexed_attempt_prefix_missing')

    def begin_attempt(self,db,job,index,expected,*,previous_attempts=()):
        self.check(db);current=self.progress(db,job);require(current==expected,'indexed_cursor_changed')
        require(index==current['next'])
        actual=self.attempts_for(db,job,index)
        require(actual==list(previous_attempts),'indexed_attempt_predecessor_changed')
        count=len(actual)
        total=db.execute('SELECT count FROM indexed_claims WHERE job=?',(job,)).fetchone()[0]
        require(count<3 and current['attempts']<min(3*total,total+10),'indexed_attempt_budget_exhausted')
        now=self.clock();require(type(now) in (int,float) and math.isfinite(now) and now>=current['clock'],'indexed_clock_rollback')
        intent=uuid.uuid4().hex
        db.execute("INSERT INTO indexed_attempts VALUES(?,?,?,?,'started')",(job,index,count+1,intent))
        db.execute('UPDATE indexed_fetch SET attempts=attempts+1,revision=revision+1,clock=? WHERE job=?',(now,job))
        return self.progress(db,job),intent

    def latest_attempt(self,db,job,index):
        rows=self.attempts_for(db,job,index)
        return None if not rows else dict(intent=rows[-1]['intent'],state=rows[-1]['state'])

    def attempts_for(self,db,job,index):
        require(not db.execute("SELECT 1 FROM indexed_attempts WHERE job=? AND seq=? AND (typeof(intent)!='text' OR length(CAST(intent AS BLOB))!=32 OR intent GLOB '*[^0-9a-f]*') LIMIT 1",(job,index)).fetchone(),'indexed_attempt_identity_changed')
        result=[]
        for row in db.execute('SELECT attempt,intent,state FROM indexed_attempts WHERE job=? AND seq=? ORDER BY attempt LIMIT 4',(job,index)):
            require(len(result)<3 and type(row[0]) is int and row[0]==len(result)+1
                    and row[2] in ('started','confirmed'),'indexed_attempt_identity_changed')
            result.append(dict(attempt=row[0],intent=row[1],state=row[2]))
        return result

    def advance(self,db,job,index,byte_size,expected,intent):
        self.check(db);require(self.progress(db,job)==expected and index==expected['next'],'indexed_cursor_changed')
        attempt=self.latest_attempt(db,job,index);require(attempt is not None and attempt['intent']==intent)
        now=self.clock();require(type(now) in (int,float) and math.isfinite(now) and now>=expected['clock'],'indexed_clock_rollback')
        db.execute("UPDATE indexed_attempts SET state='confirmed' WHERE job=? AND seq=? AND intent=?",(job,index,intent))
        db.execute('UPDATE indexed_fetch SET next=next+1,bytes=bytes+?,revision=revision+1,clock=? WHERE job=?',(byte_size,now,job))
        return self.progress(db,job)
