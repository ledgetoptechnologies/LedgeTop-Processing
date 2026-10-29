"""Fresh-profile metadata partitioning primitives; no file/source release.

The archive is in the SAME SQLite database as the hot journal.  Its immutable
rows and tombstone are committed with hot-row removal in one FULL transaction.
This module never creates a live profile, commits a caller's transaction, or
turns a saved JSON proof into execution/cleanup authority.  Callers must validate
the original physical profile and own its transaction before using projections.
"""
from contextlib import contextmanager, closing
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import stat
import threading
import time
from weakref import WeakKeyDictionary

from . import reference_protocol as p

POLICY_ID = 'indexed-worker-fetch-retiring-v1'
MAX_ARCHIVES = 32
MAX_ARCHIVE_BYTES = 32 * 1024**2
MAX_PROOF_BYTES = 16384
COPY_HEADROOM = 32 * 1024**2
_PARTITIONS = WeakKeyDictionary()
_PROJECTIONS = {}


class VerifiedPartition:
    """Opaque, scoped logical-snapshot cache. Never physical authority."""
    def __init__(self):
        raise RetirementHeld('retirement_partition_factory_required')
TABLES = {
    'driver_jobs': ('created','job','claim_request','stage','data','background_checked'),
    'indexed_quota': ('job','charge'),
    'indexed_claims': ('job','header','envelope','sha','count','bytes','closure'),
    'indexed_objects': ('job','seq','id','descriptor'),
    'indexed_seals': ('job','sha'),
    'indexed_reservations': ('job','record'),
    'indexed_grants': ('job','record','sha'),
    'indexed_fetch': ('job','next','bytes','attempts','revision','clock'),
    'indexed_attempts': ('job','seq','attempt','intent','state'),
}
ORDER = {t: ','.join(c for c in columns if c in ('job','seq','attempt')) for t,columns in TABLES.items()}
TYPES = {
    'driver_jobs': (int,str,str,str,str,float),
    'indexed_quota': (str,int),
    'indexed_claims': (str,str,str,str,int,int,str),
    'indexed_objects': (str,int,str,str),
    'indexed_seals': (str,str),
    'indexed_reservations': (str,str),
    'indexed_grants': (str,str,str),
    'indexed_fetch': (str,int,int,int,int,float),
    'indexed_attempts': (str,int,int,str,str),
}
PROOF_FIELDS = ('schemaVersion','purpose','jobId','claimRequestId','generation',
    'environment','inputManifestSha256','pipelineDigest','resultManifestSha256',
    'coordinatorReceiptSha256','viewerReceiptsSha256','profileSha256',
    'workerClosureSha256','scratchClosureSha256')


class RetirementHeld(ValueError):
    pass


def require(value, reason='retirement_metadata_unavailable'):
    if not value:
        raise RetirementHeld(reason)


def encoded(value):
    return p.canonical_json(value)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def decode(raw, maximum):
    require(type(raw) is str and len(raw.encode()) <= maximum, 'retirement_record_bound')
    def pairs(items):
        out = {}
        for key,value in items:
            require(key not in out, 'retirement_duplicate_key')
            out[key] = value
        return out
    try:
        value = json.loads(raw, object_pairs_hook=pairs)
        require(encoded(value).decode() == raw, 'retirement_noncanonical_record')
        return value
    except (ValueError, TypeError, RecursionError) as error:
        raise RetirementHeld('retirement_record_invalid') from error


def schema(base):
    """Only a NEW v4 profile may select this DDL; legacy DDL stays literal."""
    removed = {f'{table}_delete' for table in TABLES if table != 'driver_jobs'}
    removed.add('indexed_attempt_identity_delete')
    statements = tuple(s for s in base if not any(s.startswith('CREATE TRIGGER '+name+' ') for name in removed))
    statements += (
        "CREATE TABLE indexed_archives (job TEXT PRIMARY KEY,rows TEXT NOT NULL CHECK(typeof(rows)='text' AND length(CAST(rows AS BLOB))<=33554432),sha TEXT NOT NULL)",
        "CREATE TABLE indexed_retired (job TEXT PRIMARY KEY,claim_request TEXT UNIQUE NOT NULL,archive_sha TEXT NOT NULL,proof TEXT NOT NULL CHECK(typeof(proof)='text' AND length(CAST(proof AS BLOB))<=16384),proof_sha TEXT NOT NULL)",
    )
    for table in ('indexed_archives','indexed_retired'):
        for action in ('UPDATE','DELETE'):
            statements += (f"CREATE TRIGGER {table}_{action.lower()} BEFORE {action} ON {table} BEGIN SELECT RAISE(ABORT,'immutable retirement history'); END",)
    for table in TABLES:
        if table == 'driver_jobs':
            continue
        statements += (
            f"CREATE TRIGGER {table}_retirement_delete BEFORE DELETE ON {table} WHEN NOT EXISTS(SELECT 1 FROM indexed_retired r JOIN indexed_archives a ON a.job=r.job WHERE r.job=OLD.job AND r.archive_sha=a.sha) BEGIN SELECT RAISE(ABORT,'sealed retirement required'); END",
            f"CREATE TRIGGER {table}_retired_insert BEFORE INSERT ON {table} WHEN EXISTS(SELECT 1 FROM indexed_retired WHERE job=NEW.job) BEGIN SELECT RAISE(ABORT,'retired job replay'); END",
        )
    return statements


def _rows(db, table, job):
    columns = TABLES[table]
    # Bounds apply BEFORE materialization, including malicious oversized TEXT.
    maximum = 10010 if table == 'indexed_attempts' else 10000 if table == 'indexed_objects' else 1
    require(db.execute('SELECT COUNT(*) FROM '+table+' WHERE job=?',(job,)).fetchone()[0] <= maximum)
    total = '+'.join('COALESCE(length(CAST("'+c+'" AS BLOB)),0)' for c in columns)
    require(db.execute('SELECT COALESCE(SUM('+total+'),0) FROM '+table+' WHERE job=?',(job,)).fetchone()[0] <= MAX_ARCHIVE_BYTES)
    return [list(row) for row in db.execute('SELECT '+','.join(columns)+' FROM '+table+' WHERE job=? ORDER BY '+ORDER[table],(job,))]


def snapshot(db, job):
    p._uuid(job,'retirement job')
    value = dict(schemaVersion=1, jobId=job, tables={table:_rows(db,table,job) for table in TABLES})
    raw = encoded(value)
    require(len(raw) <= MAX_ARCHIVE_BYTES, 'retirement_archive_bound')
    return raw.decode()


def _proof(raw, job, request):
    value = decode(raw, MAX_PROOF_BYTES)
    require(type(value) is dict and set(value) == set(PROOF_FIELDS))
    require(type(value['schemaVersion']) is int and value['schemaVersion']==1
            and value['purpose']=='metadata-partition-only'
            and value['jobId']==job and value['claimRequestId']==request)
    p._uuid(job,'retirement job');p._slug(request,'retirement claim request')
    p._integer(value['generation'],'retirement generation')
    p._environment(value['environment'])
    for key in PROOF_FIELDS:
        if key.endswith('Sha256') or key.endswith('Digest'):
            p._hash(value[key],key)
    return value


def _archive(db, job):
    row = db.execute("SELECT rows,sha FROM indexed_archives WHERE job=? AND typeof(rows)='text' AND length(CAST(rows AS BLOB))<=?",(job,MAX_ARCHIVE_BYTES)).fetchone()
    tomb = db.execute("SELECT claim_request,archive_sha,proof,proof_sha FROM indexed_retired WHERE job=? AND typeof(proof)='text' AND length(CAST(proof AS BLOB))<=?",(job,MAX_PROOF_BYTES)).fetchone()
    require(row is not None and tomb is not None, 'retirement_missing_partition')
    require(type(row[1]) is str and digest(row[0].encode())==row[1]==tomb[1])
    require(digest(tomb[2].encode())==tomb[3])
    proof = _proof(tomb[2],job,tomb[0])
    profile=db.execute('SELECT record FROM indexed_profile WHERE singleton=1').fetchone()
    require(profile is not None and proof['profileSha256']==digest(profile[0].encode()),'retirement_profile_proof_changed')
    value = decode(row[0],MAX_ARCHIVE_BYTES)
    require(type(value) is dict and set(value)=={'schemaVersion','jobId','tables'}
            and type(value['schemaVersion']) is int and value['schemaVersion']==1
            and value['jobId']==job and type(value['tables']) is dict and set(value['tables'])==set(TABLES))
    for table,columns in TABLES.items():
        rows=value['tables'][table]
        maximum=10010 if table=='indexed_attempts' else 10000 if table=='indexed_objects' else 1
        require(type(rows) is list and len(rows)<=maximum)
        for record in rows:
            require(type(record) is list and len(record)==len(columns) and record[columns.index('job')]==job)
            require(all(type(v) is expected for v,expected in zip(record,TYPES[table]))
                    and all(type(v) is not float or math.isfinite(v) for v in record),'retirement_row_type_changed')
        key_indices=[columns.index(k) for k in ORDER[table].split(',')]
        keys=[tuple(row[i] for i in key_indices) for row in rows]
        require(keys==sorted(set(keys)),'retirement_row_order_or_duplicate')
    original=value['tables']['driver_jobs']
    require(len(original)==1 and original[0][2]==tomb[0] and original[0][3]=='completed')
    require(encoded(_rows(db,'driver_jobs',job))==encoded(original),'retirement_completed_row_changed')
    require(all(len(value['tables'][table])==1 for table in TABLES if table not in ('indexed_objects','indexed_attempts')))
    require(value['tables']['indexed_quota'][0][1]==32*1024**2)
    claim=decode(value['tables']['indexed_claims'][0][2],4*1024**2)
    require(all(claim.get(k)==proof[v] for k,v in (('jobId','jobId'),('generation','generation'),('environment','environment'),('manifestSha256','inputManifestSha256'),('pipelineDigest','pipelineDigest'))))
    projection=decode(original[0][4],8192)
    require('failure' not in projection,'retirement_success_only')
    pipeline=claim['submission']['pipeline']
    require(pipeline['odm']['requested'] is True and pipeline['treedar']['requested'] is False
            and pipeline['gaussian_splat']['requested'] is False,'retirement_odm_only')
    return value,proof


def require_clean_sql_context(db):
    """No TEMP shadow objects or attached namespace may redirect a query."""
    databases=tuple(tuple(row) for row in db.execute('PRAGMA database_list'))
    require(len(databases)<=2 and sum(row[1]=='main' for row in databases)==1
            and all(row[1] in ('main','temp') for row in databases),'retirement_attached_database')
    require(db.execute('SELECT 1 FROM temp.sqlite_master LIMIT 1').fetchone() is None,'retirement_temp_shadow')
    databases=tuple(tuple(row) for row in db.execute('PRAGMA database_list'))
    require(len(databases)<=2 and {row[1] for row in databases}=={'main','temp'}
            and all(row[1]=='main' or row[2]=='' for row in databases),'retirement_attached_database')
    return databases,db.execute('PRAGMA temp.schema_version').fetchone()[0]


def _schema_fingerprint(db):
    require(db.execute('SELECT COUNT(*) FROM main.sqlite_master').fetchone()[0]<=128)
    require(not db.execute('SELECT 1 FROM main.sqlite_master WHERE length(CAST(sql AS BLOB))>16384 LIMIT 1').fetchone())
    return digest(encoded([list(row) for row in db.execute('SELECT type,name,tbl_name,sql FROM main.sqlite_master ORDER BY type,name')]))


def retired_jobs(db):
    """Check ORIGINAL complete partition before any projection/filtering.

    Physical original schema/root/owner/profile and stable snapshot validation
    are mandatory caller preconditions. Hashes do not grant that authority.
    """
    require(db.in_transaction, 'retirement_snapshot_required')
    require_clean_sql_context(db)
    count=db.execute('SELECT COUNT(*) FROM indexed_retired').fetchone()[0]
    require(type(count) is int and 0<=count<=MAX_ARCHIVES)
    require(db.execute('SELECT COUNT(*) FROM indexed_archives').fetchone()[0]==count)
    jobs=tuple(row[0] for row in db.execute('SELECT job FROM indexed_retired ORDER BY job LIMIT 33'))
    require(not db.execute('SELECT 1 FROM indexed_archives a LEFT JOIN indexed_retired r ON r.job=a.job WHERE r.job IS NULL LIMIT 1').fetchone())
    require(db.execute('SELECT COUNT(*) FROM driver_jobs').fetchone()[0] <= MAX_ARCHIVES+4)
    for job in jobs:
        _archive(db,job)
        for table in TABLES:
            if table!='driver_jobs':
                require(db.execute('SELECT 1 FROM '+table+' WHERE job=? LIMIT 1',(job,)).fetchone() is None,'retirement_double_partition')
    # Never hide orphan hot rows by selecting only known driver jobs.
    for table in TABLES:
        if table!='driver_jobs':
            require(not db.execute('SELECT 1 FROM '+table+' x LEFT JOIN driver_jobs d ON d.job=x.job WHERE d.job IS NULL LIMIT 1').fetchone(),'retirement_orphan_hot_row')
    require(not db.execute("SELECT 1 FROM driver_jobs d LEFT JOIN indexed_claims c ON c.job=d.job LEFT JOIN indexed_retired r ON r.job=d.job WHERE c.job IS NULL AND r.job IS NULL AND d.stage!='claim_pending' LIMIT 1").fetchone(),'retirement_missing_job_partition')
    return jobs


def _partition_jobs(db,partition):
    if partition is None:return retired_jobs(db)
    require(type(partition) is VerifiedPartition)
    record=_PARTITIONS.get(partition)
    require(record is not None)
    connection,pid,thread,changes,version,schema_version,sql_context,schema_fingerprint,jobs=record
    require(connection is db and pid==os.getpid() and thread==threading.get_ident()
            and db.in_transaction and db.total_changes==changes
            and db.execute('PRAGMA data_version').fetchone()[0]==version
            and db.execute('PRAGMA schema_version').fetchone()[0]==schema_version
            and require_clean_sql_context(db)==sql_context
            and _schema_fingerprint(db)==schema_fingerprint,'retirement_partition_snapshot_changed')
    return jobs


@contextmanager
def validated_partition(db):
    """One exhaustive original partition pass, scoped to this SQL snapshot."""
    jobs=retired_jobs(db)
    token=object.__new__(VerifiedPartition)
    _PARTITIONS[token]=(db,os.getpid(),threading.get_ident(),db.total_changes,
        db.execute('PRAGMA data_version').fetchone()[0],db.execute('PRAGMA schema_version').fetchone()[0],
        require_clean_sql_context(db),_schema_fingerprint(db),jobs)
    try:
        yield token
        _partition_jobs(db,token)
    finally:_PARTITIONS.pop(token,None)


partition_jobs = _partition_jobs


def archive_history(db,*,partition=None):
    jobs=_partition_jobs(db,partition)
    rows=[list(row) for row in db.execute('SELECT job,claim_request,archive_sha,proof_sha FROM indexed_retired ORDER BY job')]
    return dict(schemaVersion=1,jobs=len(jobs),sha256=digest(encoded(rows)))


normalized_history = archive_history


@contextmanager
def logical_projection(db, *, job=None, partition=None):
    """Bounded BUSINESS input only: never physical/profile/owner authority.

    No actual file, clock, TTL, descriptor, original root or inode is changed.
    A projection cannot authorize startup, recovery, engine work or cleanup.
    """
    from . import reference_indexed_fetch as j
    jobs=_partition_jobs(db,partition)
    require(job is None or job in jobs)
    values=None if job is None else _archive(db,job)[0]
    with closing(sqlite3.connect(':memory:')) as memory:
        memory.execute('PRAGMA max_page_count=32768')
        for statement in schema(j.SCHEMA):memory.execute(statement)
        # Original immutable records copied verbatim, never rewritten/rebound.
        for table in ('indexed_profile','driver_binding','driver_engine_pin'):
            for row in db.execute('SELECT * FROM '+table):
                memory.execute('INSERT INTO '+table+' VALUES('+','.join('?' for _ in row)+')',tuple(row))
        selected=(job,) if job is not None else tuple(row[0] for row in db.execute('SELECT job FROM driver_jobs ORDER BY created') if row[0] not in jobs)
        for identifier in selected:
            for table,columns in TABLES.items():
                rows=values['tables'][table] if values is not None else _rows(db,table,identifier)
                for row in rows:memory.execute('INSERT INTO '+table+' VALUES('+','.join('?' for _ in columns)+')',row)
                require(encoded(_rows(memory,table,identifier))==encoded(rows),'retirement_projection_affinity_changed')
        memory.commit()
        memory.execute('PRAGMA query_only=ON');memory.execute('BEGIN')
        _PROJECTIONS[id(memory)]=(db,memory,partition,job,os.getpid(),threading.get_ident(),
            memory.total_changes,memory.execute('PRAGMA schema_version').fetchone()[0],
            require_clean_sql_context(memory),_schema_fingerprint(memory))
        try:
            yield memory
            _partition_jobs(db,partition)
        finally:
            _PROJECTIONS.pop(id(memory),None)
            memory.rollback()


def check_projection(original_db, projected_db, *, partition, job=None):
    """Prove exact scoped BUSINESS projection, never physical DB authority."""
    require(type(partition) is VerifiedPartition,'retirement_verified_partition_required')
    _partition_jobs(original_db,partition)
    record=_PROJECTIONS.get(id(projected_db))
    require(record is not None,'retirement_registered_projection_required')
    original,projected,token,identifier,pid,thread,changes,schema_version,sql_context,schema_fingerprint=record
    require(original is original_db and projected is projected_db and token is partition
            and identifier==job and pid==os.getpid() and thread==threading.get_ident()
            and projected_db.in_transaction and projected_db.total_changes==changes
            and projected_db.execute('PRAGMA query_only').fetchone()[0]==1
            and projected_db.execute('PRAGMA schema_version').fetchone()[0]==schema_version
            and require_clean_sql_context(projected_db)==sql_context
            and _schema_fingerprint(projected_db)==schema_fingerprint,
            'retirement_projection_context_changed')
    return True


def reserve_admission(db):
    """V4 only, inside the same live BEGIN IMMEDIATE as quota insertion."""
    from . import reference_indexed_fetch as j
    require(len(retired_jobs(db)) < MAX_ARCHIVES, 'retirement_history_full')
    pages=db.execute('PRAGMA page_count').fetchone()[0]
    page_size=db.execute('PRAGMA page_size').fetchone()[0]
    require(type(pages) is int and pages>=0 and page_size==4096)
    count,charge=db.execute('SELECT COUNT(*),COALESCE(SUM(charge),0) FROM indexed_quota').fetchone()
    require(type(count) is int and 0<=count<3 and type(charge) is int and charge==count*j.JOB_CHARGE)
    # Allocated pages INCLUDE freelist. Never assume VACUUM/truncation or future
    # deletions will return bytes; reserve simultaneous hot+archive copy now.
    require(pages*page_size+charge+j.JOB_CHARGE+COPY_HEADROOM+j.NON_JOB_ALLOWANCE <= 128*1024**2,'retirement_metadata_headroom')


def _store_validated(db, job, proof):
    """Internal storage primitive; ONLY an actual-driver auditor may call it.

    This is not a public approval/JSON/CLI interface.  It supplies no physical
    owner or terminal authority. The caller retains its actual DriverStepLease,
    reader-session barrier and original-file checks until the outer commit.
    """
    from . import reference_indexed_fetch as j
    require(db.in_transaction, 'retirement_transaction_required')
    require(j._schema(db)==j._expected_schema(j._RETIRING_POLICY),'retirement_fresh_schema_required')
    profile=db.execute('SELECT record FROM indexed_profile WHERE singleton=1').fetchone()
    require(profile is not None and json.loads(profile[0])['descriptor']['policyId']==POLICY_ID)
    jobs=retired_jobs(db)
    if job in jobs:
        old=_archive(db,job)[1]
        require(encoded(old)==encoded(proof),'retirement_proof_replay_changed')
        return False
    require(len(jobs)<MAX_ARCHIVES,'retirement_history_full')
    row=db.execute("SELECT claim_request,stage FROM driver_jobs WHERE job=?",(job,)).fetchone()
    require(row is not None and row[1]=='completed','retirement_success_not_terminal')
    raw_proof=encoded(proof).decode();_proof(raw_proof,job,row[0])
    require(proof['profileSha256']==digest(profile[0].encode()),'retirement_profile_proof_changed')
    raw=snapshot(db,job)
    page_count=db.execute('PRAGMA page_count').fetchone()[0]
    require(page_count*4096+COPY_HEADROOM+4*1024**2<=128*1024**2,'retirement_copy_headroom')
    # Do not add REPLACE/IGNORE or a second committing journal. A crash before
    # the caller's FULL commit leaves all hot rows; after it all archived rows.
    db.execute('INSERT INTO indexed_archives VALUES(?,?,?)',(job,raw,digest(raw.encode())))
    db.execute('INSERT INTO indexed_retired VALUES(?,?,?,?,?)',(job,row[0],digest(raw.encode()),raw_proof,digest(raw_proof.encode())))
    for table in reversed(tuple(TABLES)):
        if table!='driver_jobs':db.execute('DELETE FROM '+table+' WHERE job=?',(job,))
    retired_jobs(db)
    return True


def _facts(info):
    return (info.st_dev,info.st_ino,info.st_mode,info.st_uid,info.st_nlink,
            info.st_size,info.st_mtime_ns,info.st_ctime_ns)


def _directory(path,identity):
    path=Path(path).absolute()
    require(path.resolve()==path and not any(p.is_symlink() for p in (path,*path.parents)))
    info=path.lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid==os.geteuid() and not info.st_mode&0o077
            and [info.st_dev,info.st_ino]==identity,'retirement_bound_root_changed')
    return path


@contextmanager
def _durable_store(path):
    """Existing private DELETE database only; no recovery/clock/constructor."""
    path=Path(path)
    require(path.is_absolute() and path.resolve()==path and not any(p.is_symlink() for p in (path,*path.parents)))
    before=path.lstat()
    # This existing read ceiling is NOT an approved v4 physical/admission cap.
    require(stat.S_ISREG(before.st_mode) and before.st_uid==os.geteuid()
            and before.st_nlink==1 and stat.S_IMODE(before.st_mode)==0o600
            and 0<before.st_size<=1024**3,'retirement_store_unavailable')
    require(not any(os.path.lexists(str(path)+suffix) for suffix in ('-journal','-wal','-shm')),'retirement_store_requires_reconciliation')
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
        require(_facts(os.fstat(fd))==_facts(before))
        with closing(sqlite3.connect(path.as_uri()+'?mode=ro&immutable=1',uri=True)) as db:
            db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON');db.execute('PRAGMA trusted_schema=OFF');db.execute('BEGIN')
            deadline=time.monotonic()+3;db.set_progress_handler(lambda:int(time.monotonic()>deadline),1000)
            require(db.execute('PRAGMA journal_mode').fetchone()[0]=='delete')
            yield db
        require(_facts(path.lstat())==_facts(os.fstat(fd))==_facts(before),'retirement_store_changed_during_audit')
    finally:os.close(fd)


def _one(db,table,key,value):
    expected={
        'executions':('job','identity','receipt','phase','result','outbox_id','archive_size','archive_sha','part_dev','part_ino','archive_expected_size'),
        'completions':('id','job','generation','environment','payload','digest','state','attempts','next_at','claim_token','lease_until','receipt','local_metadata'),
        'durable_receipts':('id','storage_id','outbox','payload'),
        'reservations':('id','settings','job','generation','environment','expires','state','remaining','allocated','terminal_at'),
        'results':('reservation','identity'),
        'input_bindings':('reservation','identity'),
    }[table]
    actual=db.execute("SELECT type,sql FROM sqlite_master WHERE name=?",(table,)).fetchall()
    require(len(actual)==1 and actual[0][0]=='table' and type(actual[0][1]) is str
            and not actual[0][1].upper().startswith('CREATE VIRTUAL'))
    require(tuple(row[1] for row in db.execute('PRAGMA table_info('+table+')'))==expected,'retirement_store_columns_changed')
    total='+'.join('COALESCE(length(CAST("'+name+'" AS BLOB)),0)' for name in expected)
    require(db.execute('SELECT COUNT(*) FROM '+table+' WHERE '+key+'=?',(value,)).fetchone()[0]==1)
    require(not db.execute('SELECT 1 FROM '+table+' WHERE '+key+'=? AND ('+total+')>? LIMIT 1',(value,MAX_ARCHIVE_BYTES)).fetchone(),'retirement_durable_row_bound')
    rows=db.execute('SELECT * FROM '+table+' WHERE '+key+'=? LIMIT 2',(value,)).fetchall()
    require(len(rows)==1,'retirement_durable_closure_missing')
    result=dict(rows[0]);require(len(encoded(result))<=MAX_ARCHIVE_BYTES)
    return result


def terminal_proof(db, rows, *, state_root, binding, actual_scratch_root=None, eligibility=False):
    """Recompute commitments from actual retained stores, never saved proof.

    Caller must first verify actual original physical profile/schema/owner and
    original SQL snapshot. `state_root` is explicit for recovery copies and host
    mount mappings. No absolute archive paths or source-release receipts exist.
    Mutable clock, allocation and read-pin records are preserved, not hashed as
    immutable completion identity. Producer eligibility additionally holds on
    active pins; cold audit does not erase/reconcile any historical reader pin.
    """
    from .reference_sender import decode_receipt
    root=_directory(state_root,binding['stateRootIdentity'])
    scratch_root=_directory(binding['scratchRoot'] if actual_scratch_root is None else actual_scratch_root,binding['scratchRootIdentity'])
    require(type(eligibility) is bool)
    job=rows['jobId'];table=rows['tables'];driver=table['driver_jobs'][0]
    require(driver[3]=='completed')
    claim=decode(table['indexed_claims'][0][2],4*1024**2)
    projection=decode(driver[4],8192)
    require('failure' not in projection and claim['submission']['pipeline']['odm']['requested'] is True
            and claim['submission']['pipeline']['treedar']['requested'] is False
            and claim['submission']['pipeline']['gaussian_splat']['requested'] is False,'retirement_success_only')
    reservation=decode(table['indexed_reservations'][0][1],4096)['reservationId']
    profile=db.execute('SELECT record FROM indexed_profile WHERE singleton=1').fetchone()
    require(profile is not None)
    with _durable_store(root/'worker.sqlite') as worker, _durable_store(root/'outbox.sqlite') as outbox, _durable_store(root/'scratch.sqlite') as scratch:
        execution=_one(worker,'executions','job',job)
        require(execution['phase']=='delivered' and execution['outbox_id']==projection.get('outboxId'),'retirement_worker_not_delivered')
        identity=decode(execution['identity'],4*1024**2+4*1024**2)
        fixed=('jobId','generation','environment','nodeId','manifestSha256','pipelineDigest','claimToken')
        require(identity['reservation']==reservation and all(identity['job'][k]==claim[k] for k in fixed))
        require(encoded(identity['job']['submission'])==encoded(claim['submission']))
        result=p.validate_result_manifest(decode(execution['result'],4*1024**2),job_id=job,generation=claim['generation'],environment=claim['environment'],pipeline_digest=claim['pipelineDigest'])
        require(len(result['objects'])==1 and result['objects'][0]['role']=='odm_archive' and set(result['engineVersions'])=={'odm'})
        completion=_one(outbox,'completions','id',execution['outbox_id'])
        require(completion['state']=='acknowledged' and completion['claim_token'] is None and completion['lease_until'] is None and completion['attempts']>0,'retirement_outbox_not_acknowledged')
        payload=decode(completion['payload'],4*1024**2)
        required=dict(jobId=job,generation=claim['generation'],environment=claim['environment'],inputManifestSha256=claim['manifestSha256'],pipelineDigest=claim['pipelineDigest'],resultManifestSha256=result['resultManifestSha256'])
        p._closed(payload,(*required,'terminalState','resultManifest'),'retirement immutable result payload')
        require(completion['id']==p.digest(required) and all(payload.get(k)==v for k,v in required.items()) and payload.get('terminalState')=='succeeded'
                and encoded(payload.get('resultManifest'))==encoded(result) and p.digest(payload)==completion['digest'])
        local=decode(completion['local_metadata'],MAX_PROOF_BYTES)
        require(local==dict(ownedPrefix='jobs/'+reservation,localObjects={result['objects'][0]['id']:'jobs/'+reservation+'/all.zip'}),'retirement_outbox_paths_changed')
        receipt=decode_receipt(completion['receipt'].encode())
        p._closed(receipt,('type','durable','receiptId','storageReceiptId','outboxId','payloadSha256','acceptedObjectIds',*required),'retirement coordinator receipt')
        require(receipt['type']=='durable-result-receipt' and receipt['durable'] is True
                and receipt['outboxId']==completion['id'] and receipt['payloadSha256']==completion['digest']
                and all(receipt[k]==v for k,v in required.items())
                and receipt['acceptedObjectIds']==[result['objects'][0]['id']])
        stored=_one(outbox,'durable_receipts','outbox',completion['id'])
        require(stored['id']==receipt['receiptId'] and stored['storage_id']==receipt['storageReceiptId'] and stored['payload']==encoded(receipt).decode())
        reserved=_one(scratch,'reservations','id',reservation)
        require((reserved['job'],reserved['generation'],reserved['environment'],reserved['state'])==(job,claim['generation'],claim['environment'],'terminal'))
        registered=_one(scratch,'results','reservation',reservation)
        registered_value=decode(registered['identity'],4*1024**2)
        mapping={'job_id':'jobId','generation':'generation','manifest_sha256':'inputManifestSha256','pipeline_sha256':'pipelineDigest','result_manifest_sha256':'resultManifestSha256'}
        require(all(registered_value.get(k)==required[v] for k,v in mapping.items()))
        objects=[dict(row) for row in scratch.execute('SELECT * FROM objects WHERE reservation=? ORDER BY id LIMIT 2',(reservation,))]
        require(len(objects)==1 and objects[0]['id']==result['objects'][0]['id'] and objects[0]['bytes']==result['objects'][0]['byteSize']
                and objects[0]['sha']==result['objects'][0]['sha256'] and objects[0]['accepted'] is not None and objects[0]['cleaned'] is None
                and objects[0]['path']=='jobs/'+reservation+'/all.zip')
        require(registered_value['objects']==[dict(object_id=objects[0]['id'],bytes=objects[0]['bytes'],sha256=objects[0]['sha'],relative_path=objects[0]['path'])])
        require(scratch.execute('SELECT COUNT(*) FROM receipts').fetchone()[0]<=MAX_ARCHIVES+4,'retirement_receipt_history_bound')
        viewers=[dict(row) for row in scratch.execute("SELECT * FROM receipts WHERE json_extract(payload,'$.reservation_id')=? LIMIT 2",(reservation,))]
        require(len(viewers)==1,'retirement_single_result_receipt_required')
        viewer=decode(viewers[0]['payload'],MAX_PROOF_BYTES)
        p._closed(viewer,('reservation_id','receipt_id','storage_receipt_id','job_id','generation','manifest_sha256','pipeline_sha256','result_manifest_sha256','accepted_object_ids'),'retirement normalized viewer receipt')
        require(viewer['reservation_id']==reservation and viewer['receipt_id']==viewers[0]['id'] and viewer['storage_receipt_id']==viewers[0]['storage_id']
                and all(viewer[k]==required[v] for k,v in mapping.items())
                and viewer['accepted_object_ids']==[result['objects'][0]['id']])
        input_binding=_one(scratch,'input_bindings','reservation',reservation)
        require(input_binding['identity']==encoded(projection['inputAccounting']['binding']).decode())
        inputs=[dict(row) for row in scratch.execute('SELECT * FROM input_copies WHERE reservation=? ORDER BY id LIMIT 10001',(reservation,))]
        expected=sorted(claim['submission']['manifest']['objects'],key=lambda o:o['id'])
        require(len(inputs)==len(expected))
        for row,obj in zip(inputs,expected):
            require(row['id']==obj['id'] and row['bytes']==obj['byteSize'] and row['sha']==obj['sha256']
                    and row['path']=='jobs/'+reservation+'/'+obj['id']+'.input'
                    and row['fetched'] is not None and row['cleaned'] is None)
        require(identity['paths']=={obj['id']:'jobs/'+reservation+'/'+obj['id']+'.input' for obj in expected},'retirement_worker_input_paths_changed')
        for name in ('cleanup_intents','input_cleanup_intents','input_releases'):
            require(scratch.execute('SELECT 1 FROM '+name+' WHERE reservation=? LIMIT 1',(reservation,)).fetchone() is None,'retirement_cleanup_or_release_present')
        if eligibility:
            for name in ('pins','input_pins'):
                require(scratch.execute("SELECT 1 FROM "+name+" WHERE reservation=? AND state!='released' LIMIT 1",(reservation,)).fetchone() is None,'retirement_active_pin')
        file_facts=[]
        for row in inputs+objects:
            path=scratch_root/row['path']
            require(path.resolve()==path and not any(v.is_symlink() for v in (path,*path.parents)))
            info=path.lstat()
            require(stat.S_ISREG(info.st_mode) and info.st_uid==os.geteuid() and not info.st_mode&0o077 and info.st_size==row['bytes'],'retirement_retained_file_missing')
            file_facts.append(dict(path=row['path'],dev=info.st_dev,ino=info.st_ino,uid=info.st_uid,
                gid=info.st_gid,mode=stat.S_IMODE(info.st_mode),bytes=info.st_size,mtimeNs=info.st_mtime_ns))
        scratch_closure=dict(reservation={k:reserved[k] for k in ('id','settings','job','generation','environment','state')},result=registered,objects=objects,inputBinding=input_binding,inputs=inputs,retainedFiles=file_facts)
        return dict(schemaVersion=1,purpose='metadata-partition-only',jobId=job,claimRequestId=driver[2],generation=claim['generation'],environment=claim['environment'],inputManifestSha256=claim['manifestSha256'],pipelineDigest=claim['pipelineDigest'],resultManifestSha256=result['resultManifestSha256'],coordinatorReceiptSha256=digest(encoded(receipt)),viewerReceiptsSha256=digest(encoded(viewers)),profileSha256=digest(profile[0].encode()),workerClosureSha256=digest(encoded(dict(execution=execution,completion=completion,durableReceipt=stored))),scratchClosureSha256=digest(encoded(scratch_closure)))


def validate_archived_closures(db, *, state_root, binding, partition=None, actual_scratch_root=None):
    """Resolve every immutable commitment against ORIGINAL durable stores."""
    jobs=_partition_jobs(db,partition)
    for job in jobs:
        rows,expected=_archive(db,job)
        actual=terminal_proof(db,rows,state_root=state_root,binding=binding,actual_scratch_root=actual_scratch_root)
        require(encoded(actual)==encoded(expected),'retirement_durable_proof_changed')
    _partition_jobs(db,partition)
    return archive_history(db,partition=partition)

