"""Read-only stopped-worker indexed census. No repair, release or clock writes."""
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import time
import weakref

from processing import reference_protocol as p
from processing import reference_indexed_fetch as j
from ops import bootstrap_worker_input_accounting as bootstrap

_PROOFS=weakref.WeakKeyDictionary()

class IndexedMaintenanceHeld(ValueError):pass

def require(value,reason='indexed_maintenance_identity_changed'):
    if not value:raise IndexedMaintenanceHeld(reason)

def _same(a,b):return p.canonical_json(a)==p.canonical_json(b)
def _id(path):
    info=Path(path).lstat();return [info.st_dev,info.st_ino]

def observe_running_mounts(worker):
    """Mint proof by issuing the actual trusted CLI observation ourselves."""
    from ops.reference_worker_maintenance import WorkerMaintenance,private
    require(type(worker) is WorkerMaintenance and worker.inspect()['running'])
    config,view=worker.config()
    gate_target='/var/lib/ledgetop-gates/LedgeTop-Processing/node-gates/'+hashlib.sha256(worker.policy.engine_id.encode()).hexdigest()
    pairs=[(worker.policy.config_directory,worker.policy.container_config_directory),(worker.policy.state_root,config['stateRoot']),
        (worker.policy.scratch_root,config['scratchRoot']),(worker.policy.gate_directory,gate_target),
        (str(Path(worker.policy.config_directory)/'worker.json'),str(Path(worker.policy.container_config_directory)/'worker.json'))]
    for key in ('caFile','serverCertFile','serverKeyFile'):
        pairs.append((str(Path(worker.policy.config_directory)/Path(config[key]).relative_to(worker.policy.container_config_directory)),config[key]))
    code='import os,stat,json,sys; print(json.dumps([[s.st_dev,s.st_ino,s.st_uid,s.st_gid,stat.S_IMODE(s.st_mode)] for p in json.loads(sys.argv[1]) for s in [os.lstat(p)]]))'
    actual=p.decode_submission(worker.docker(['exec',worker.policy.container_id,'python','-c',code,json.dumps([target for source,target in pairs])]))
    wanted=[]
    for source,target in pairs:
        _,info=private(source,directory=Path(source).is_dir())
        wanted.append([info.st_dev,info.st_ino,info.st_uid,info.st_gid,info.st_mode&0o777])
    require(_same(actual,wanted),'indexed_runtime_mount_identity_changed')
    require(worker.config()[1]==view,'indexed_runtime_config_changed')
    old=_PROOFS.get(worker)
    if old is not None:
        require(old['policy']==worker.policy.sha256 and old['base']==p.digest({k:v for k,v in config.items() if k!='engineVersion'}))
        return
    _PROOFS[worker]=dict(pid=os.getpid(),container=worker.policy.container_id,engine=worker.policy.engine_id,
        policy=worker.policy.sha256,config=view,base=p.digest({k:v for k,v in config.items() if k!='engineVersion'}),
        pairs=tuple(tuple(v) for v in pairs),facts=tuple(tuple(v) for v in wanted))

def observe_owned_rebind(worker):
    """Only the controller calls this after exact atomic config/publication CAS."""
    proof=_PROOFS.get(worker);require(proof is not None,'indexed_stopped_mapping_unproven')
    worker._owner.check(Path(worker.policy.state_root))
    config,view=worker.config()
    require(proof['base']==p.digest({k:v for k,v in config.items() if k!='engineVersion'}))
    worker.publication_read(view)
    proof['config']=view


class IndexedMaintenanceContext:
    __slots__=('__weakref__',)
    def __init__(self,*args):raise TypeError('trusted stopped-worker proof required')

_CONTEXTS=weakref.WeakKeyDictionary()

def stopped_context(worker):
    from ops.reference_worker_maintenance import WorkerMaintenance,private
    from ops.reference_worker_daemon import DaemonStateLock
    require(type(worker) is WorkerMaintenance and type(worker._owner) is DaemonStateLock,'indexed_maintenance_owner_required')
    proof=_PROOFS.get(worker);require(proof is not None,'indexed_stopped_mapping_unproven')
    require(not worker.inspect()['running'] and worker.inspect()['pid']==0,'indexed_worker_not_stopped')
    config,view=worker.config();worker._owner.check(Path(worker.policy.state_root))
    require(proof['pid']==os.getpid() and proof['container']==worker.policy.container_id and proof['engine']==worker.policy.engine_id
        and proof['policy']==worker.policy.sha256 and proof['base']==p.digest({k:v for k,v in config.items() if k!='engineVersion'})
        and view==proof['config'])
    facts=[]
    for source,target in proof['pairs']:
        if source==str(Path(worker.policy.config_directory)/'worker.json'):
            facts.append(proof['facts'][len(facts)]);continue
        _,s=private(source,directory=Path(source).is_dir());facts.append((s.st_dev,s.st_ino,s.st_uid,s.st_gid,s.st_mode&0o777))
    require(tuple(facts)==proof['facts'],'indexed_mount_identity_changed')
    context=object.__new__(IndexedMaintenanceContext)
    _CONTEXTS[context]=(weakref.ref(worker),proof,worker._owner)
    return context

def _check(context,root,*,running=False):
    require(type(context) is IndexedMaintenanceContext and context in _CONTEXTS,'indexed_maintenance_context_required')
    worker_ref,proof,owner=_CONTEXTS[context];worker=worker_ref()
    require(worker is not None and _PROOFS.get(worker) is proof)
    require(Path(root)==Path(worker.policy.state_root))
    # After restart the daemon owns the lock; this observer cannot acquire it.
    if not running:
        require(worker._owner is owner,'indexed_maintenance_owner_changed')
        owner.check(Path(root))
    require(proof['pid']==os.getpid() and proof['policy']==worker.policy.sha256)
    config,view=worker.config()
    require(proof['base']==p.digest({k:v for k,v in config.items() if k!='engineVersion'}) and view==proof['config'],'indexed_configuration_identity_changed')
    for (source,target),facts in zip(proof['pairs'],proof['facts']):
        if source==str(Path(worker.policy.config_directory)/'worker.json'):continue
        info=Path(source).lstat()
        require((info.st_dev,info.st_ino,info.st_uid,info.st_gid,info.st_mode&0o777)==facts,'indexed_mount_identity_changed')
    return worker,config

def _authority(context,root,*,running=False):
    worker,config=_check(context,root,running=running);root=Path(root);scratch=Path(worker.policy.scratch_root)
    directory=root/bootstrap.DIRECTORY
    bootstrap._directory(directory)
    require({x.name for x in directory.iterdir()}=={'intent.json','ledger-created.json','indexed-created.json','completed.json'},'indexed_bootstrap_inventory_changed')
    intent,_,intent_sha=bootstrap._file(directory/'intent.json')
    mapped=dict(config,stateRoot=str(root),scratchRoot=str(scratch))
    expected=bootstrap._binding(mapped)
    expected.update(stateRoot=config['stateRoot'],scratchRoot=config['scratchRoot'])
    require(_same(intent,dict(schemaVersion=1,purpose='fresh-worker-input-copy-accounting',bootstrapId=intent.get('bootstrapId'),binding=expected,bindingSha256=p.digest(expected))),
        'indexed_bootstrap_binding_changed')
    binding=intent['binding'];bootstrap._read_intent(directory,binding)
    claim,claim_id,claim_sha=bootstrap._file(scratch/bootstrap.CLAIM)
    require(_same(claim,bootstrap._claim_value(intent,binding)),'indexed_scratch_claim_changed')
    created,_,created_sha=bootstrap._file(directory/'ledger-created.json')
    require(_same(created,dict(schemaVersion=1,bootstrapId=intent['bootstrapId'],intentSha256=intent_sha,claimSha256=claim_sha,
        claimIdentity=claim_id,ledgerIdentity=_id(root/'scratch.sqlite'))),'indexed_ledger_receipt_changed')
    bootstrap._ledger(root/'scratch.sqlite',binding,expected_identity=created['ledgerIdentity'])
    indexed,_,indexed_sha=bootstrap._file(directory/'indexed-created.json')
    require(_same(indexed,dict(schemaVersion=1,bootstrapId=intent['bootstrapId'],intentSha256=intent_sha,ledgerCreatedSha256=created_sha,
        descriptorSha256=p.digest(binding['indexedFetchJournal']),driverIdentity=_id(root/'driver.sqlite'),rootIdentity=_id(root),bindingSha256=intent['bindingSha256'])),
        'indexed_creation_receipt_changed')
    completed,_,completed_sha=bootstrap._file(directory/'completed.json')
    require(_same(completed,dict(schemaVersion=2,bootstrapId=intent['bootstrapId'],intentSha256=intent_sha,createdSha256=created_sha,
        inputCopyAccountingVersion=1,indexedCreatedSha256=indexed_sha)),'indexed_completion_receipt_changed')
    receipts=dict(intent=intent_sha,ledger=created_sha,indexed=indexed_sha,completed=completed_sha,claim=claim_sha)
    if os.path.lexists(root/'indexed-driver-recovery'):
        from processing.reference_indexed_recovery import inspect_completed_recovery_capsule
        receipts['recovery']=inspect_completed_recovery_capsule(root,bootstrap_id=intent['bootstrapId'],binding_sha256=intent['bindingSha256'],
            root_identity=_id(root),driver_identity=_id(root/'driver.sqlite'))
    return intent,receipts

class _ReadOnlyJournal:
    """Reuse logical checks without constructing a writer or observing walltime."""
    claim=j.IndexedFetchJournal.claim
    header=j.IndexedFetchJournal.header
    grant=j.IndexedFetchJournal.grant
    progress=j.IndexedFetchJournal.progress
    validate_cold=j.IndexedFetchJournal.validate_cold
    validate_attempt_closure=j.IndexedFetchJournal.validate_attempt_closure
    def __init__(self,intent,root,driver_path=None):
        self.intent,self.root=intent,Path(root)
        self.driver_path=Path(driver_path) if driver_path is not None else self.root/'driver.sqlite'
    def check(self,db):
        j.validate_profile_connection(db,bootstrap_id=self.intent['bootstrapId'],binding=self.intent['binding'],
            root_identity=_id(self.root),driver_identity=_id(self.driver_path))


def _hash_rows(db,table,limit):
    require(db.execute('SELECT COUNT(*) FROM "'+table+'"').fetchone()[0]<=limit,'indexed_maintenance_row_bound')
    columns=list(db.execute('PRAGMA table_info("'+table+'")'))
    keys=[r['name'] for r in sorted(columns,key=lambda r:r['pk']) if r['pk']]
    require(keys,'indexed_maintenance_primary_key_required')
    order=','.join('"'+key+'"' for key in keys)
    digest=hashlib.sha256();digest.update(p.canonical_json(dict(table=table,columns=[r['name'] for r in columns])))
    total=' + '.join('COALESCE(length(CAST("'+r['name']+'" AS BLOB)),0)' for r in columns)
    require(not db.execute('SELECT 1 FROM "'+table+'" WHERE ('+total+')>? LIMIT 1',(j.LIMITS['claimBytes']+16384,)).fetchone(),'indexed_maintenance_row_byte_bound')
    for row in db.execute('SELECT * FROM "'+table+'" ORDER BY '+order):
        value=dict(row)
        if table=='driver_jobs':value.pop('background_checked',None)
        raw=p.canonical_json(value);require(len(raw)<=j.LIMITS['claimBytes']+16384,'indexed_maintenance_row_byte_bound')
        digest.update(len(raw).to_bytes(8,'big'));digest.update(raw)
    return digest.hexdigest()


def audit_terminal_job(db,ro,row,scratch):
    require(row['stage']=='completed','worker_pending_driver_or_acceptance')
    claim=ro.claim(db,row['job']);projection=json.loads(row['data'])
    from processing.reference_driver import _INDEXED_FIXED_CLAIM,_INDEXED_MUTABLE_CLAIM
    require(type(projection.get('claim')) is dict and set(_INDEXED_FIXED_CLAIM)<=set(projection['claim'])
        and not set(projection['claim'])-set(_INDEXED_FIXED_CLAIM+_INDEXED_MUTABLE_CLAIM),'indexed_maintenance_projection_changed')
    for key in ('jobId','generation','environment','nodeId','manifestSha256','pipelineDigest','claimToken'):
        require(_same(projection['claim'].get(key),claim[key]),'indexed_maintenance_projection_changed')
    claim['transferRequest']=projection.get('reservation')
    ro.validate_cold(db,row['job'],claim)
    progress=ro.progress(db,row['job']);objects=claim['submission']['manifest']['objects']
    # Historical evidence is checked at the durable fetch highwater, never at
    # current wall time and never converted into a live download capability.
    from processing.reference_transfer import validate_grant
    validate_grant(ro.grant(db,row['job']),job=claim,clock=lambda:progress['clock'])
    require(progress['next']==len(objects),'indexed_maintenance_fetch_incomplete')
    reservation=claim['transferRequest']['reservationId']
    own=scratch.execute('SELECT job,generation,environment,state FROM reservations WHERE id=?',(reservation,)).fetchone()
    require(own is not None and tuple(own)==(row['job'],claim['generation'],claim['environment'],'terminal'),'indexed_terminal_reservation_changed')
    if 'failure' in projection:
        from processing.reference_terminal import validate_terminal_acceptance
        failure=projection['failure']
        for key,value in dict(jobId=row['job'],generation=claim['generation'],environment=claim['environment'],inputManifestSha256=claim['manifestSha256'],pipelineDigest=claim['pipelineDigest']).items():
            require(_same(failure.get(key),value),'indexed_terminal_failure_changed')
        validate_terminal_acceptance(projection.get('terminalAcceptance'),failure=failure)
    else:
        result=scratch.execute('SELECT identity FROM results WHERE reservation=?',(reservation,)).fetchone()
        require(result is not None,'indexed_terminal_result_missing')
        result=json.loads(result[0])
        require(all(_same(result.get(key),value) for key,value in dict(job_id=row['job'],generation=claim['generation'],manifest_sha256=claim['manifestSha256'],pipeline_sha256=claim['pipelineDigest']).items()),'indexed_terminal_result_changed')
        result_rows=list(scratch.execute('SELECT id,accepted FROM objects WHERE reservation=? LIMIT 10001',(reservation,)))
        ids={o['object_id'] for o in result['objects']}
        require(0<len(ids)<=10000 and len(result_rows)==len(ids) and {r[0] for r in result_rows}==ids and all(r[1] is not None for r in result_rows),'indexed_viewer_acceptance_missing')
        accepted=set()
        for receipt in scratch.execute('SELECT payload FROM receipts LIMIT 10001'):
            value=json.loads(receipt[0])
            if value.get('reservation_id')!=reservation:continue
            require(all(_same(value.get(k),result[k]) for k in ('job_id','generation','manifest_sha256','pipeline_sha256','result_manifest_sha256')),'indexed_viewer_receipt_changed')
            accepted.update(value['accepted_object_ids'])
        require(accepted==ids,'indexed_viewer_receipt_missing')
    expected=[dict(object_id=o['id'],byte_size=o['byteSize'],sha256=o['sha256'],relative_path='jobs/'+reservation+'/'+o['id']+'.input') for o in objects]
    expected.sort(key=lambda o:o['object_id'])
    binding=dict(jobId=row['job'],generation=claim['generation'],environment=claim['environment'],
        inputManifestSha256=claim['manifestSha256'],pipelineDigest=claim['pipelineDigest'],inputsSha256=p.digest(expected))
    stored=scratch.execute('SELECT identity FROM input_bindings WHERE reservation=?',(reservation,)).fetchone()
    require(stored is not None and stored[0]==p.canonical_json(binding).decode(),'indexed_scratch_binding_changed')
    require(_same(projection.get('inputAccounting'),dict(schemaVersion=1,binding=binding,state='bound',next=len(objects),byteCount=sum(o['byteSize'] for o in objects))),'indexed_input_projection_changed')
    from processing.scratch_policy import _path_key
    count=0
    for actual in scratch.execute('SELECT id,bytes,sha,path,path_key,fetched FROM input_copies WHERE reservation=? ORDER BY id LIMIT 10001',(reservation,)):
        require(count<len(expected),'indexed_scratch_closure_changed')
        e=expected[count]
        require(tuple(actual[:5])==(e['object_id'],e['byte_size'],e['sha256'],e['relative_path'],_path_key(e['relative_path']))
            and actual[5] is not None,'indexed_scratch_closure_changed')
        count+=1
    require(count==len(expected),'indexed_scratch_closure_changed')


def inspect_driver(db,root,context,*,running=False):
    """Full terminal audit only at maintenance boundaries, never health polling."""
    intent,receipts=_authority(context,root,running=running);ro=_ReadOnlyJournal(intent,root);ro.check(db)
    tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
    require(not db.execute("SELECT 1 FROM driver_jobs WHERE typeof(data)!='text' OR length(CAST(data AS BLOB))>8192 LIMIT 1").fetchone(),'indexed_maintenance_projection_bound')
    jobs=list(db.execute('SELECT job,claim_request,stage,data FROM driver_jobs LIMIT 65'))
    require(len(jobs)<=j.LIMITS['retainedJobs'],'indexed_maintenance_job_bound')
    require(not db.execute('SELECT 1 FROM readiness_tasks LIMIT 1').fetchone(),'indexed_readiness_task_unresolved')
    require(len(jobs)==db.execute('SELECT COUNT(*) FROM indexed_claims').fetchone()[0]==db.execute('SELECT COUNT(*) FROM indexed_quota').fetchone()[0],
        'indexed_maintenance_job_closure')
    require(not db.execute('SELECT 1 FROM indexed_quota WHERE charge!=? LIMIT 1',(j.JOB_CHARGE,)).fetchone(),'indexed_quota_changed')
    require(db.execute('SELECT COALESCE(SUM(charge),0) FROM indexed_quota').fetchone()[0]+j.NON_JOB_ALLOWANCE<=j.DB_CAP,'indexed_quota_changed')
    for table in ('indexed_seals','indexed_reservations','indexed_grants','indexed_fetch'):
        require(db.execute('SELECT COUNT(*) FROM '+table).fetchone()[0]==len(jobs),'indexed_terminal_history_missing')
    for table in ('indexed_objects','indexed_seals','indexed_reservations','indexed_grants','indexed_fetch','indexed_attempts'):
        require(not db.execute('SELECT 1 FROM '+table+' x LEFT JOIN indexed_claims c ON c.job=x.job WHERE c.job IS NULL LIMIT 1').fetchone(),'indexed_orphan_record')
    scratch_path=Path(root)/'scratch.sqlite'
    before=scratch_path.lstat()
    with closing(sqlite3.connect(scratch_path.as_uri()+'?mode=ro&immutable=1',uri=True)) as scratch:
        scratch.row_factory=sqlite3.Row;scratch.execute('BEGIN')
        deadline=time.monotonic()+3
        scratch.set_progress_handler(lambda:int(time.monotonic()>deadline),1000)
        for row in jobs:audit_terminal_job(db,ro,row,scratch)
    after=scratch_path.lstat()
    require((before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns,before.st_ctime_ns)==
        (after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns),'indexed_scratch_changed_during_audit')
    settings=db.execute('SELECT binding FROM driver_binding WHERE singleton=1').fetchone()
    if settings:bootstrap._driver_settings(json.loads(settings[0]),intent['binding'],indexed=True)
    limits={name:j.LIMITS['retainedJobs'] for name in tables}
    limits.update(driver_binding=1,driver_engine_pin=1,readiness_tasks=0,indexed_profile=1,indexed_objects=10000,indexed_attempts=10010)
    hashes={name:_hash_rows(db,name,limits[name]) for name in sorted(tables)}
    # Engine pin legitimately advances on owned native version admission; it is
    # fenced separately by gate/backend/config and omitted only from retained hash.
    history={name:value for name,value in hashes.items() if name!='driver_engine_pin'}
    return hashes,dict(schemaVersion=1,profile='indexed-worker-fetch-v1',tables=history,bootstrap=receipts)
