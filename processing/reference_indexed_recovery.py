"""Completed-v2 driver-only rollback recovery. No engine, migration or release authority.

SQLite alone performs rollback. A validated private copy precedes original writes.
One fixed capsule bounds retained recovery material; unknown state always holds.
"""
from contextlib import closing
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

from . import reference_indexed_fetch as indexed

DIRECTORY = 'indexed-driver-recovery'
MAGIC = bytes.fromhex('d9d505f920a163d7')
FILES = {'state.json','state.tmp','copy.sqlite','copy.sqlite-journal'}
CONTROL_ALLOWANCE = 64*1024
# Exact ODM stages emitted/consumed by ReferenceDriver._step and its background
# acceptance handler. Combined preparation is forbidden by the indexed ABI.
INDEXED_ODM_STAGES = frozenset((
    'claim_pending','claimed','reservation_pending','awaiting_grant',
    'fetching_pending','fetching','processing_pending','execution_pending',
    'executing','delivering','awaiting_acceptance','failure_delivering',
    'awaiting_failure_acceptance','completed'))
PRE_GRANT_STAGES = frozenset(('claimed','reservation_pending','awaiting_grant'))
POST_FETCH_STAGES = frozenset(('processing_pending','execution_pending','executing','delivering',
    'awaiting_acceptance','failure_delivering','awaiting_failure_acceptance','completed'))


def require(value):
    if not value:raise indexed.IndexedFetchHeld('indexed_recovery_held')


def facts(info):
    return [info.st_dev,info.st_ino,info.st_mode,info.st_nlink,info.st_uid,
            info.st_size,info.st_mtime_ns,info.st_ctime_ns]


def file_facts(path,cap):
    path=Path(path)
    require(path.resolve()==path and not any(v.is_symlink() for v in (path,*path.parents)))
    info=path.lstat()
    require(stat.S_ISREG(info.st_mode) and info.st_nlink==1 and info.st_size<=cap
            and info.st_uid==os.geteuid() and stat.S_IMODE(info.st_mode)==0o600)
    return facts(info)


def fingerprint(path,cap):
    before=file_facts(path,cap)
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
        require(facts(os.fstat(fd))==before)
        digest=hashlib.sha256()
        while True:
            block=os.read(fd,1024*1024)
            if not block:break
            digest.update(block)
        require(facts(os.fstat(fd))==before==file_facts(path,cap))
    finally:os.close(fd)
    return dict(facts=before,sha256=digest.hexdigest())


def sync(directory):
    fd=os.open(directory,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:os.fsync(fd)
    finally:os.close(fd)


def directory_identity(directory):
    require(directory.resolve()==directory and not any(p.is_symlink() for p in (directory,*directory.parents)))
    info=directory.lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid==os.geteuid() and stat.S_IMODE(info.st_mode)==0o700)
    require(set(v.name for v in directory.iterdir())<=FILES)
    return [info.st_dev,info.st_ino]


def state_read(directory,*,policy=None):
    policy=indexed.validated_policy(policy)
    path=directory/'state.json';before=file_facts(path,16384)
    raw=path.read_bytes();require(file_facts(path,16384)==before)
    value=json.loads(raw);require(indexed.encoded(value)==raw)
    require(set(value)=={'schemaVersion','purpose','operationId','directoryIdentity','bootstrapId',
        'bindingSha256','driverIdentity','rootIdentity','original','journal','phase','recoveredSha256'})
    require(type(value['schemaVersion']) is int and value['schemaVersion']==1
            and value['purpose']=='completed-v2-driver-rollback'
            and str(uuid.UUID(value['operationId']))==value['operationId']
            and value['phase'] in ('copying','validated','original-started','complete'))
    for key in ('directoryIdentity','rootIdentity','driverIdentity'):
        require(type(value[key]) is list and len(value[key])==2
                and all(type(v) is int and v>=0 for v in value[key]))
    def digest(v):return type(v) is str and len(v)==64 and all(c in '0123456789abcdef' for c in v)
    require(str(uuid.UUID(value['bootstrapId']))==value['bootstrapId'] and digest(value['bindingSha256']))
    for key,cap in (('original',policy.database_cap),('journal',2*policy.database_cap)):
        record=value[key]
        require(type(record) is dict and set(record)=={'facts','sha256'} and digest(record['sha256']))
        f=record['facts'];require(type(f) is list and len(f)==8 and all(type(v) is int and v>=0 for v in f)
            and stat.S_ISREG(f[2]) and stat.S_IMODE(f[2])==0o600 and f[3]==1 and f[4]==os.geteuid() and f[5]<=cap)
    require((value['phase']=='copying' and value['recoveredSha256'] is None)
            or value['phase']!='copying' and digest(value['recoveredSha256']))
    require(value['directoryIdentity']==directory_identity(directory))
    return value


def inspect_completed_recovery_capsule(state_root, *, bootstrap_id, binding_sha256, root_identity, driver_identity, policy=None):
    """Read-only stopped census seam; never authorizes recovery or current jobs."""
    root=Path(state_root).absolute();require(root.resolve()==root)
    info=root.stat();require([info.st_dev,info.st_ino]==list(root_identity))
    directory=root/DIRECTORY
    state=state_read(directory,policy=policy)
    require(state['phase']=='complete' and state['bootstrapId']==bootstrap_id
            and state['bindingSha256']==binding_sha256
            and state['rootIdentity']==list(root_identity) and state['driverIdentity']==list(driver_identity)
            and set(p.name for p in directory.iterdir())=={'state.json'})
    return dict(schemaVersion=1,operationId=state['operationId'],phase='complete',
                stateSha256=fingerprint(directory/'state.json',16384)['sha256'])


def state_write(directory,value):
    # state.tmp is exclusively this capsule's bounded replace buffer, never a DB.
    require(value['directoryIdentity']==directory_identity(directory))
    path=directory/'state.tmp'
    if os.path.lexists(path):file_facts(path,16384)
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_TRUNC|os.O_NOFOLLOW,0o600)
    try:
        raw=indexed.encoded(value);require(len(raw)<=16384)
        require(os.write(fd,raw)==len(raw));os.fsync(fd)
    finally:os.close(fd)
    os.replace(path,directory/'state.json');sync(directory)


def clean_copy(directory,identity,*,policy=None):
    policy=indexed.validated_policy(policy)
    # Only fixed, regular, single-link files created inside this owned capsule.
    require(directory_identity(directory)==identity)
    for name,cap in (('copy.sqlite-journal',2*policy.database_cap),('copy.sqlite',policy.database_cap)):
        path=directory/name
        if os.path.lexists(path):file_facts(path,cap);path.unlink()
    sync(directory)


def copied_file(source,target,cap):
    before=file_facts(source,cap)
    if os.path.lexists(target):file_facts(target,cap)
    src=os.open(source,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    dst=None
    try:
        dst=os.open(target,os.O_WRONLY|os.O_CREAT|os.O_TRUNC|os.O_NOFOLLOW,0o600)
        require(facts(os.fstat(src))==before)
        while True:
            raw=os.read(src,1024*1024)
            if not raw:break
            view=memoryview(raw)
            while view:
                count=os.write(dst,view);require(count>0);view=view[count:]
        os.fsync(dst);require(facts(os.fstat(src))==before==file_facts(source,cap))
    finally:
        os.close(src)
        if dst is not None:os.close(dst)


def capacity(config,intent,path,directory,*,copy_required=True):
    """Read-only ledger commitments plus existing global metadata hold, once."""
    from ops import bootstrap_worker_input_accounting as bootstrap
    root=path.parent;binding=intent['binding'];policy=indexed.policy_from_binding(binding);ledger=root/'scratch.sqlite'
    created=bootstrap._file(root/bootstrap.DIRECTORY/'ledger-created.json')[0]
    bootstrap._ledger(ledger,binding,expected_identity=created['ledgerIdentity'])
    before=fingerprint(ledger,1024**3)
    with closing(sqlite3.connect(ledger.as_uri()+'?mode=ro&immutable=1',uri=True)) as db:
        require(not db.execute("SELECT 1 FROM reservations WHERE typeof(remaining)!='integer' OR remaining<0 LIMIT 1").fetchone())
        require(not db.execute("SELECT 1 FROM control_charges WHERE typeof(bytes)!='integer' OR bytes<0 LIMIT 1").fetchone())
        remaining=db.execute('SELECT COALESCE(SUM(remaining),0) FROM reservations').fetchone()[0]
        control=db.execute('SELECT COALESCE(SUM(bytes),0) FROM control_charges').fetchone()[0]
    require(before==fingerprint(ledger,1024**3))
    held=remaining+control if root.stat().st_dev==Path(binding['scratchRoot']).stat().st_dev else 0
    allocated=sum(getattr(p.lstat(),'st_blocks',0)*512 for p in (path,Path(str(path)+'-journal')) if os.path.lexists(p))
    materialized=sum(getattr(p.lstat(),'st_blocks',0)*512 for p in directory.iterdir() if p.name.startswith('copy.'))
    extra=(3*policy.database_cap+CONTROL_ALLOWANCE) if copy_required else CONTROL_ALLOWANCE
    free=shutil.disk_usage(root).free
    require(type(free) is int and free>=held+binding['policy']['safetyFloor']
            +max(0,policy.physical_allowance-allocated)+max(0,extra-materialized))


def validate_business_connection(db,binding,*,state_root=None):
    """Audit the original partition before checking any logical projection.

    Recovery's caller has already checked the actual profile/schema and original
    root/inode binding. A memory projection is business input, never authority
    to open or repair an original file.
    """
    from . import reference_metadata_retirement as retirement
    policy=indexed.policy_from_binding(binding)
    if policy.policy_id!=retirement.POLICY_ID:
        return _validate_hot_business_connection(db,binding)
    # This verifies every original archive/tombstone and the disjoint complete
    # partition BEFORE projecting anything away. Unknown/incomplete state holds.
    with retirement.validated_partition(db) as partition:
        jobs=retirement.partition_jobs(db,partition)
        validate_retirement_readiness(db,policy,len(jobs))
        if jobs:
            # The stored proof's syntax/hash is not independently resolved
            # receipt evidence. An incomplete integration remains held.
            resolver=getattr(retirement,'validate_archived_closures',None)
            require(state_root is not None and callable(resolver))
            resolver(db,state_root=state_root,binding=binding,partition=partition)
        with retirement.logical_projection(db,partition=partition) as active:
            _validate_hot_business_connection(active,binding)
        for job in jobs:
            with retirement.logical_projection(db,job=job,partition=partition) as archived:
                _validate_hot_business_connection(archived,binding)
        retirement.partition_jobs(db,partition)


def validate_retirement_readiness(db,policy,retired_count):
    """Account ORIGINAL terminal-native cache, never a filtered projection."""
    from . import reference_metadata_retirement as retirement
    require(type(retired_count) is int and 0<=retired_count<=retirement.MAX_ARCHIVES)
    maximum=policy.capacity+retired_count
    cached=db.execute('SELECT uuid FROM readiness_tasks LIMIT ?',(maximum+1,)).fetchall()
    require(len(cached)<=maximum)
    completed={row[0] for row in db.execute("SELECT job FROM driver_jobs WHERE stage='completed'")}
    for entry in cached:
        indexed.p._uuid(entry[0],'cached native task')
        require(entry[0] in completed)


def _validate_hot_business_connection(db,binding):
    """Bounded read-only claim/object/grant/attempt closure, no clock or TTL writes."""
    p=indexed.p;policy=indexed.policy_from_binding(binding)
    from .reference_driver import _INDEXED_FIXED_CLAIM,_INDEXED_MUTABLE_CLAIM,validate_indexed_fetch_projection
    fixed=_INDEXED_FIXED_CLAIM
    permitted_projection=set(fixed+_INDEXED_MUTABLE_CLAIM)
    require(db.execute('SELECT COUNT(*) FROM driver_jobs').fetchone()[0]<=policy.retained_jobs+1)
    require(not db.execute('SELECT 1 FROM indexed_quota WHERE typeof(charge)!=\'integer\' OR charge!=? LIMIT 1',(indexed.JOB_CHARGE,)).fetchone())
    require(db.execute('SELECT COALESCE(SUM(charge),0) FROM indexed_quota').fetchone()[0]+indexed.NON_JOB_ALLOWANCE<=policy.database_cap)
    for table in ('indexed_quota','indexed_claims'):
        require(not db.execute('SELECT 1 FROM '+table+' x LEFT JOIN driver_jobs d ON d.job=x.job WHERE d.job IS NULL LIMIT 1').fetchone())
    for table in ('indexed_objects','indexed_seals','indexed_reservations','indexed_grants','indexed_fetch','indexed_attempts'):
        require(not db.execute('SELECT 1 FROM '+table+' x LEFT JOIN indexed_claims c ON c.job=x.job WHERE c.job IS NULL LIMIT 1').fetchone())
    require(not db.execute('SELECT 1 FROM indexed_claims c LEFT JOIN indexed_quota q ON q.job=c.job LEFT JOIN indexed_seals s ON s.job=c.job WHERE q.job IS NULL OR s.job IS NULL LIMIT 1').fetchone())
    for job,request,stage,data in db.execute('SELECT job,claim_request,stage,data FROM driver_jobs'):
        require(type(stage) is str and stage in INDEXED_ODM_STAGES)
        require(type(data) is str and len(data.encode())<=8192)
        small=json.loads(data);require(type(small) is dict)
        row=db.execute('SELECT header,envelope,sha,count,bytes,closure FROM indexed_claims WHERE job=?',(job,)).fetchone()
        if row is None:
            require(stage=='claim_pending' and not set(small)-{'execution'})
            continue
        require(stage!='claim_pending')
        header,raw,digest,count,byte_count,closure=row
        require(type(raw) is str and len(raw.encode())<=indexed.LIMITS['claimBytes'] and indexed.sha(raw.encode())==digest)
        require(type(header) is str and len(header.encode())<=8192)
        claim=json.loads(raw);head=json.loads(header)
        require(claim['jobId']==job and head['claimRequestId']==request
                and claim['nodeId']==binding['physical']['nodeId']
                and claim['environment'] in binding['policy']['environments'])
        require(all(type(head[k]) is type(claim[k]) and head[k]==claim[k] for k in fixed))
        submission=claim['submission'];objects=submission['manifest']['objects']
        payload={k:submission[k] for k in ('schemaVersion','submissionId','manifest','pipeline','capabilityFingerprint')}
        normalized=p.validate_submission(payload,environment=claim['environment'],capability_fingerprint=submission['capabilityFingerprint'])
        require(p.canonical_json(normalized)==p.canonical_json(submission))
        require(claim['manifestSha256']==submission['manifestSha256'] and claim['pipelineDigest']==submission['pipelineDigest'])
        require(type(count) is int and 0<count<=10000 and count==len(objects)
                and byte_count==sum(o['byteSize'] for o in objects) and closure==p.digest(objects))
        require(db.execute('SELECT sha FROM indexed_seals WHERE job=?',(job,)).fetchone()==(closure,))
        seen=0
        for seq,object_id,descriptor in db.execute('SELECT seq,id,descriptor FROM indexed_objects WHERE job=? ORDER BY seq LIMIT 10001',(job,)):
            require(seen<count and seq==seen and object_id==objects[seen]['id']
                    and type(descriptor) is str and len(descriptor.encode())<=2048 and descriptor==indexed.encoded(objects[seen]).decode())
            seen+=1
        require(seen==count)
        projection=small.get('claim')
        require(type(projection) is dict and set(fixed)<=set(projection)
                and not set(projection)-permitted_projection
                and all(type(projection[k]) is type(head[k]) and projection[k]==head[k] for k in fixed))
        reservation=db.execute('SELECT record FROM indexed_reservations WHERE job=?',(job,)).fetchone()
        transfer=projection.get('transferRequest')
        if reservation is None:
            require(stage in ('claimed','reservation_pending') and 'reservation' not in small and transfer is None)
        else:
            require(type(reservation[0]) is str and len(reservation[0].encode())<=4096
                    and reservation[0]==indexed.encoded(small.get('reservation')).decode())
            # The local intent is saved before the coordinator accepts it. The
            # actual driver permits an absent transfer only in these two stages.
            require((transfer is None and stage in ('claimed','reservation_pending'))
                    or reservation[0]==indexed.encoded(transfer).decode())
        input_identity=None
        if reservation is not None:
            local_reservation=json.loads(reservation[0]);reservation_id=local_reservation['reservationId']
            normalized_inputs=[dict(object_id=o['id'],byte_size=o['byteSize'],sha256=o['sha256'],
                relative_path='jobs/'+reservation_id+'/'+o['id']+'.input') for o in objects]
            # Same ASCII manifest-derived closure serialized by ScratchLedger;
            # no paths or closure hashes are accepted from mutable JSON.
            closure_bytes=json.dumps(sorted(normalized_inputs,key=lambda o:o['object_id']),sort_keys=True,separators=(',',':')).encode()
            input_identity=dict(jobId=job,generation=claim['generation'],environment=claim['environment'],
                inputManifestSha256=claim['manifestSha256'],pipelineDigest=claim['pipelineDigest'],
                inputsSha256=hashlib.sha256(closure_bytes).hexdigest())
        checkpoint=small.get('inputAccounting')
        if checkpoint is not None:
            require(input_identity is not None and type(checkpoint) is dict
                    and set(checkpoint)=={'schemaVersion','binding','state','next','byteCount'}
                    and type(checkpoint['schemaVersion']) is int and checkpoint['schemaVersion']==1
                    and indexed.encoded(checkpoint['binding'])==indexed.encoded(input_identity)
                    and type(checkpoint['next']) is int and type(checkpoint['byteCount']) is int)
            if stage=='reservation_pending':
                require(checkpoint['state'] in ('binding_pending','bound') and checkpoint['next']==checkpoint['byteCount']==0)
            else:require(checkpoint['state']=='bound')
        if stage not in ('claimed','reservation_pending'):
            require(checkpoint is not None)
        grant=db.execute('SELECT record,sha FROM indexed_grants WHERE job=?',(job,)).fetchone()
        progress=db.execute('SELECT next,bytes,attempts,revision,clock FROM indexed_fetch WHERE job=?',(job,)).fetchone()
        if grant is None:
            require(stage in PRE_GRANT_STAGES and not small.get('indexedGrant') and progress is None
                    and db.execute('SELECT 1 FROM indexed_attempts WHERE job=? LIMIT 1',(job,)).fetchone() is None)
            continue
        require(stage not in PRE_GRANT_STAGES and small.get('indexedGrant') is True and reservation is not None
                and type(grant[0]) is str and len(grant[0].encode())<=indexed.LIMITS['grantBytes']
                and indexed.sha(grant[0].encode())==grant[1])
        grant_value=json.loads(grant[0]);require(indexed.encoded(grant_value).decode()==grant[0])
        require(progress is not None)
        next_index,byte_prefix,attempt_count,revision,clock=progress
        require(all(type(v) is int for v in (next_index,byte_prefix,attempt_count,revision))
                and 0<=next_index<=count and byte_prefix==sum(o['byteSize'] for o in objects[:next_index])
                and type(clock) in (float,int) and math.isfinite(clock) and revision>=attempt_count>=0)
        if stage=='fetching':
            validate_indexed_fetch_projection(small,reservation=local_reservation,identity=input_identity)
        elif stage=='fetching_pending':
            require(next_index==0 and attempt_count==0 and checkpoint['next']==checkpoint['byteCount']==0)
        elif stage in POST_FETCH_STAGES:
            require(next_index==count and checkpoint['next']==count and checkpoint['byteCount']==byte_count)
        # Historical closure only, not a current-time transfer capability. The
        # normal driver still checks real time/TTL before any transfer resumes.
        from .reference_transfer import validate_grant
        validate_grant(grant_value,job={**claim,'transferRequest':json.loads(reservation[0])},clock=lambda:clock)
        seen_attempts=0;by_seq={} 
        for seq,attempt,intent,state in db.execute('SELECT seq,attempt,intent,state FROM indexed_attempts WHERE job=? ORDER BY seq,attempt LIMIT 10011',(job,)):
            require(type(seq) is int and 0<=seq<count and type(attempt) is int and 1<=attempt<=3
                    and type(intent) is str and len(intent)==32 and all(c in '0123456789abcdef' for c in intent)
                    and state in ('started','confirmed'))
            previous=by_seq.setdefault(seq,[]);require(attempt==len(previous)+1);previous.append(state);seen_attempts+=1
        require(seen_attempts==attempt_count<=min(3*count,count+10))
        require(set(by_seq)==set(range(next_index))|({next_index} if next_index in by_seq else set()))
        for seq,states in by_seq.items():
            require(states[-1]==('confirmed' if seq<next_index else 'started') and 'confirmed' not in states[:-1])


def recovered_validate(path,intent,identity,root_identity):
    """Opening rw may recover this path; callers must authorize writes FIRST."""
    from ops import bootstrap_worker_input_accounting as bootstrap
    with closing(sqlite3.connect(path.as_uri()+'?mode=rw',uri=True,timeout=1)) as db:
        db.execute('PRAGMA synchronous=FULL')
        deadline=time.monotonic()+10
        db.set_progress_handler(lambda:int(time.monotonic()>deadline),1000)
        from . import reference_metadata_retirement as retirement
        if indexed.policy_from_binding(intent['binding']).policy_id==retirement.POLICY_ID:
            # Pin the ORIGINAL recovered snapshot before schema/profile checks
            # as well as before any later business projection.
            db.execute('BEGIN')
        require(db.execute('PRAGMA quick_check').fetchall()==[('ok',)])
        indexed.validate_profile_connection(db,bootstrap_id=intent['bootstrapId'],binding=intent['binding'],
            root_identity=root_identity,driver_identity=identity)
        require(db.execute('SELECT COUNT(*) FROM driver_binding').fetchone()[0]<=1)
        validate_business_connection(db,intent['binding'],state_root=intent['binding'].get('stateRoot'))
        row=db.execute('SELECT binding FROM driver_binding WHERE singleton=1').fetchone()
        if row is not None:bootstrap._driver_settings(json.loads(row[0]),intent['binding'],indexed=True)
    require(not any(os.path.lexists(str(path)+s) for s in ('-journal','-wal','-shm')))
    return fingerprint(path,indexed.policy_from_binding(intent['binding']).database_cap)['sha256']


def recover_indexed_driver(config,owner):
    """Recover only a completed v2 driver; default/v1 never enter this function."""
    from ops import bootstrap_worker_input_accounting as bootstrap
    require(bootstrap.indexed_profile(config))
    root=Path(config['stateRoot']);owner.check(root)
    path=root/'driver.sqlite';journal=Path(str(path)+'-journal');directory=root/DIRECTORY
    if not os.path.lexists(journal) and not os.path.lexists(directory):return False
    intent=bootstrap.completed_indexed_recovery_authority(config,owner)
    policy=indexed.policy_from_binding(intent['binding'])
    info=indexed._file(path,policy=policy);identity=[info.st_dev,info.st_ino];root_id=bootstrap._directory(root)
    if not os.path.lexists(directory):directory.mkdir(mode=0o700);sync(root)
    directory_id=directory_identity(directory)
    if os.path.lexists(directory/'state.json'):
        state=state_read(directory,policy=policy)
        require(state['bootstrapId']==intent['bootstrapId'] and state['bindingSha256']==intent['bindingSha256']
                and state['driverIdentity']==identity and state['rootIdentity']==root_id)
        if state['phase']=='complete':
            clean_copy(directory,state['directoryIdentity'],policy=policy)
            if not os.path.lexists(journal):return False
            state=None
    else:
        # Before the first atomic rename no SQLite copy can exist. A bounded
        # partial replace buffer is scratch material, never startup authority.
        require(set(p.name for p in directory.iterdir())<={'state.tmp'})
        if os.path.lexists(directory/'state.tmp'):file_facts(directory/'state.tmp',16384)
        state=None
    if state is None:
        require(os.path.lexists(journal))
        original=fingerprint(path,policy.database_cap);rollback=fingerprint(journal,2*policy.database_cap)
        # Multi-database super-journals are outside this single-file protocol.
        with journal.open('rb') as stream:
            require(stream.read(8)==MAGIC and rollback['facts'][5]>512)
            stream.seek(-8,2);require(stream.read(8)!=MAGIC)
        state=dict(schemaVersion=1,purpose='completed-v2-driver-rollback',operationId=str(uuid.uuid4()),
            directoryIdentity=directory_id,bootstrapId=intent['bootstrapId'],bindingSha256=intent['bindingSha256'],
            driverIdentity=identity,rootIdentity=root_id,original=original,journal=rollback,
            phase='copying',recoveredSha256=None)
        capacity(config,intent,path,directory);state_write(directory,state)
    owner.check(root)
    if state['phase'] in ('copying','validated'):
        require(fingerprint(path,policy.database_cap)==state['original']
                and fingerprint(journal,2*policy.database_cap)==state['journal'])
        capacity(config,intent,path,directory)
        if state['phase']=='copying':
            copied_file(path,directory/'copy.sqlite',policy.database_cap)
            copied_file(journal,directory/'copy.sqlite-journal',2*policy.database_cap);sync(directory)
            state['recoveredSha256']=recovered_validate(directory/'copy.sqlite',intent,identity,root_id)
            require(fingerprint(path,policy.database_cap)==state['original']
                    and fingerprint(journal,2*policy.database_cap)==state['journal'])
            state['phase']='validated';state_write(directory,state)
        require(fingerprint(directory/'copy.sqlite',policy.database_cap)['sha256']==state['recoveredSha256'])
        capacity(config,intent,path,directory,copy_required=False)
        require(fingerprint(path,policy.database_cap)==state['original']
                and fingerprint(journal,2*policy.database_cap)==state['journal'])
        owner.check(root);state['phase']='original-started';state_write(directory,state)
    if state['phase']=='original-started':
        capacity(config,intent,path,directory,copy_required=False)
        require([indexed._file(path,policy=policy).st_dev,indexed._file(path,policy=policy).st_ino]==identity)
        if os.path.lexists(journal):require(fingerprint(journal,2*policy.database_cap)==state['journal'])
        else:require(fingerprint(path,policy.database_cap)['sha256']==state['recoveredSha256'])
        actual=recovered_validate(path,intent,identity,root_id)
        require(actual==state['recoveredSha256'])
        owner.check(root)
        state['phase']='complete';state_write(directory,state)
        clean_copy(directory,state['directoryIdentity'],policy=policy)
    return True
