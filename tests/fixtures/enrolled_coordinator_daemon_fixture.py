"""Standalone synthetic693 original data for task-owned Docker qualification.

No import has effects. Requires pre-owned EMPTY test mounts; never imports the
application/test package, changes an owner, reconciles a spool, or contacts a
backend. The outer harness exclusively owns resource creation and cleanup.
"""
import base64
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import uuid
import zipfile

SCHEMA_SHA256='4368b50f2ba34bb969e9750b49fdd85478f88183276a1aa164744a5cc2188642'
SOURCE='693cb7eca32295da959a4f42399f317577ef8a8a'
EXPECTED_COUNTS={'jobs':22,'nodes':2,'viewer_tasks':7,'viewer_reservations':0,
 'viewer_option_evidence':7,'viewer_files':155,'owned_maintenance_schedules':16,'reference_jobs':0}


def require(condition,reason):
    if not condition:raise RuntimeError(reason)


def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()


def identity(authority,label):return str(uuid.uuid5(uuid.UUID(authority),'synthetic693:'+label))


def _root(value,mode):
    path=Path(value)
    require(path.is_absolute() and path.resolve()==path and not any(p.is_symlink() for p in path.parents),
            'original_synthetic_mount_required')
    info=path.lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid==10001 and info.st_gid==10001 and stat.S_IMODE(info.st_mode)==mode,
            'preowned_synthetic_mount_required')
    require(next(path.iterdir(),None) is None,'empty_synthetic_mount_required')
    return path


def _write(path,data,mode=0o600):
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC,mode)
    try:
        cursor=0
        while cursor<len(data):
            count=os.write(fd,data[cursor:]);require(count>0,'synthetic_write_unknown');cursor+=count
        os.fsync(fd)
    finally:os.close(fd)


def build(*,state_root,lease_directory,authority_id,schema_bytes,include_opaque_pilots=True):
    require(os.name=='posix' and os.geteuid()==10001 and os.getegid()==10001,'synthetic_uid10001_required')
    require(type(authority_id) is str and str(uuid.UUID(authority_id))==authority_id,'canonical_synthetic_authority_required')
    require(type(schema_bytes) is bytes and len(schema_bytes)<=65536,'bounded_frozen_schema_required')
    profile=json.loads(schema_bytes)
    require(hashlib.sha256(canonical(profile)).hexdigest()==SCHEMA_SHA256 and
            profile['sourceCommit']==SOURCE,'exact_frozen693_schema_required')
    require(type(include_opaque_pilots) is bool,'explicit_pilot_fixture_choice_required')
    root=_root(state_root,0o755);external=_root(lease_directory,0o711)
    require(root!=external and root not in external.parents and external not in root.parents,
            'separate_original_mounts_required')
    for name in ('viewer-tasks','viewer-spool'):(root/name).mkdir(mode=0o700)
    database=root/'jobs.sqlite3'
    # Set private mode before SQLite opens its existing empty synthetic file.
    _write(database,b'')
    db=sqlite3.connect(database)
    try:
        for kind,name,_,sql in profile['objects']:
            if kind=='table' and name!='sqlite_sequence':db.execute(sql)
        for kind,_,_,sql in profile['objects']:
            if kind=='index' and sql:db.execute(sql)
        db.execute('INSERT INTO coordinator_identity VALUES(1,?)',(authority_id,))
        db.execute('INSERT INTO coordinator_authority_lease VALUES(1,1)')
        for node in ('cluster','main'):db.execute('INSERT INTO nodes(id) VALUES(?)',(node,))
        for index in range(22):
            job=identity(authority_id,'job:'+str(index));task=identity(authority_id,'task:'+str(index))
            environment='production' if index<12 else 'staging'
            if index>=7:
                db.execute("INSERT INTO jobs(id,request_id,environment,profile,status,result) VALUES(?,?,?,'treedar-synthetic-v1',?,'{}')",
                    (job,'synthetic-'+str(index),environment,'failed' if index>=20 else 'succeeded'))
                continue
            node='cluster' if index%2 else 'main';token='synthetic-claim-'+str(index)
            engine=identity(authority_id,'engine:'+str(index));directory=root/'viewer-tasks'/(task+'-1')
            directory.mkdir(mode=0o700)
            db.execute("INSERT INTO jobs(id,request_id,environment,profile,status,node_id,claim_hash,result) VALUES(?,?,?,'odm-viewer-v1','succeeded',?,?,'{}')",
                (job,task,environment,node,hashlib.sha256(token.encode()).hexdigest()))
            db.execute("INSERT INTO viewer_tasks(uuid,directory,name,options,outputs,environment,stage,phase,job_id,node_id,engine_uuid,claim_token,option_evidence_required) VALUES(?,?,?,'{}','[]',?,'succeeded','complete',?,?,?,?,1)",
                (task,directory.name,'synthetic',environment,job,node,engine,token))
            definitions='[{"name":"pc-ept","type":"bool"}]'
            db.execute('INSERT INTO viewer_option_evidence VALUES(?,1,?,?)',(task,definitions,hashlib.sha256(definitions.encode()).hexdigest()))
            for number in range(23 if index==0 else 22):
                name='photo_'+str(number)+'.jpg';storage='input-'+uuid.UUID(identity(authority_id,'input:'+str(index)+':'+str(number))).hex
                data=('synthetic-photo-'+str(index)+'-'+str(number)).encode();_write(directory/storage,data)
                db.execute('INSERT INTO viewer_files VALUES(?,?,?,?,?,?,1)',
                    (task,name.casefold(),name,storage,len(data),hashlib.sha256(data).hexdigest()))
            archive=directory/'all.zip'
            with zipfile.ZipFile(archive,'x') as output:
                entry=zipfile.ZipInfo('synthetic/result.txt',date_time=(2026,11,1,0,0,0))
                output.writestr(entry,b'synthetic-engine-result')
            # The ZIP is a NEW synthetic file created by this UID, not shared data.
            archive.chmod(0o600)
            raw=archive.read_bytes();binding=dict(version=1,task=task,generation=1,job=job,node=node,engine=engine)
            _write(directory/'all.zip.intent',canonical(binding))
            _write(directory/'all.zip.receipt',canonical(dict(binding,bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest())))
        for index in range(16):
            node='cluster' if index%2 else 'main';revision=(index//2)*2
            request=dict(schemaVersion=1,requestId=identity(authority_id,'maintenance:'+str(index)),authorityId=authority_id,
                expectedRevision=revision,drainAt='2026-11-01T00:00:00Z',notBefore='2026-11-01T00:00:00Z',
                afterJobId=None,targetImage='sha256:'+str(index%10)*64)
            request_sha=hashlib.sha256(canonical(dict(nodeId=node,request=request))).hexdigest()
            receipt=dict(schemaVersion=1,requestId=request['requestId'],authorityId=authority_id,nodeId=node,
                requestSha256=request_sha,expectedRevision=revision,revision=revision+1,
                planId=identity(authority_id,'owned-plan:'+str(index)),state='draining',drainAt=request['drainAt'],
                notBefore=request['notBefore'],afterJobId=None,targetImage=request['targetImage'])
            receipt['receiptSha256']=hashlib.sha256(canonical(receipt)).hexdigest()
            db.execute('INSERT INTO owned_maintenance_schedules VALUES(?,?,?,?,?,?)',
                (request['requestId'],node,authority_id,request_sha,canonical(request).decode(),canonical(receipt).decode()))
            db.execute("UPDATE nodes SET state='active',revision=?,plan_id=?,drain_at=?,not_before=?,target_image=? WHERE id=?",
                (revision+2,receipt['planId'],receipt['drainAt'],receipt['notBefore'],receipt['targetImage'],node))
        # Exact old direct schedule/cancel retained-metadata shape. No new owned
        # receipt is invented for the unowned direct closed plan.
        for node in ('cluster','main'):
            db.execute("UPDATE nodes SET state='active',revision=revision+2,plan_id=?,target_image=?,result=NULL,update_hash=NULL,finish_status=NULL WHERE id=?",
                (identity(authority_id,'direct-closed:'+node),'sha256:'+'e'*64,node))
        db.commit()
        counts={name:db.execute('SELECT COUNT(*) FROM "'+name+'"').fetchone()[0] for name in EXPECTED_COUNTS}
        require(counts==EXPECTED_COUNTS,'synthetic_fixture_counts_changed')
    finally:db.close()
    _write(root/'coordinator.lock',b'',0o644)
    if include_opaque_pilots:
        pilot=root/'pilot-journals';pilot.mkdir(mode=0o700)
        for number in range(3):
            directory=pilot/('lt-aa5-'+uuid.UUID(identity(authority_id,'opaque:'+str(number))).hex)
            directory.mkdir(mode=0o700)
            for node in ('cluster','main'):
                for suffix in ('.json','.json.cancel-intent','.child-start.json','.child-exit.json'):
                    _write(directory/(node+suffix),canonical(dict(synthetic=True,opaqueRetainedEvidence=True)))
    _write(external/(authority_id+'.lease'),b'synthetic-original-external-lease-marker-v1')
    for directory in (root,external):
        fd=os.open(directory,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try:os.fsync(fd)
        finally:os.close(fd)
    return dict(schemaVersion=1,profile='synthetic-installed-693-fixture-v1',authorityId=authority_id,
        sourceCommit=SOURCE,expectedCounts=dict(EXPECTED_COUNTS),expectedFileCount=202 if include_opaque_pilots else 178,
        expectedDirectoryCount=13 if include_opaque_pilots else 9,
        signedViewerAcceptanceProven=False,pilotClosureProven=False,containsRealData=False)


def serializable_source(*,state_root='/prototype-state',lease_directory,authority_id,schema_bytes,include_opaque_pilots=True):
    require(type(schema_bytes) is bytes and len(schema_bytes)<=65536,'bounded_frozen_schema_required')
    request=dict(state_root=state_root,lease_directory=lease_directory,authority_id=authority_id,
                 include_opaque_pilots=include_opaque_pilots)
    source=Path(__file__).read_bytes()
    return source+b'\nrequest=json.loads('+repr(canonical(request)).encode()+b')\nrequest["schema_bytes"]=base64.b64decode('+repr(base64.b64encode(schema_bytes)).encode()+b')\nprint(json.dumps(build(**request),sort_keys=True,separators=(",",":")))\n'
