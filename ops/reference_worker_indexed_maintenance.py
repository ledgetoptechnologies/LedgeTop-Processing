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
    facts=list(proof['facts']);position=next(i for i,pair in enumerate(proof['pairs']) if pair[0]==str(Path(worker.policy.config_directory)/'worker.json'))
    info=Path(proof['pairs'][position][0]).lstat()
    facts[position]=(info.st_dev,info.st_ino,info.st_uid,info.st_gid,info.st_mode&0o777)
    proof['facts']=tuple(facts)


# Prospective receipt, not a generic serialized context. Only the trusted live
# namespace observer may issue it, and restoration also needs actual stopped
# Docker identity, a held daemon lease, owned update/schedule receipts and the
# unchanged host inodes. Old mount digests are deliberately insufficient.
_WITNESS_FIELDS=('schemaVersion','purpose','receiptRoot','receiptIdentity','policySha256',
    'planId','ownedScheduleRequestId','reservedRevision','operationBindingSha256',
    'oldImage','targetImage','engineId','containerId','imageId','recipeSha256',
    'configBaseSha256','beforeConfig','pairs','facts')

def _receipt(worker):
    from ops.reference_worker_maintenance import private
    path=getattr(worker,'_operation_receipt',None)
    require(path is not None,'indexed_mapping_receipt_required')
    return private(path,directory=True)

def _operation(worker,state):
    from ops.reference_worker_maintenance import WorkerMaintenance,read_json
    require(type(worker) is WorkerMaintenance,'indexed_maintenance_owner_required')
    path,info=_receipt(worker)
    p._uuid(state['planId'],'owned mapping plan')
    p._uuid(worker.owned_schedule_request_id,'owned mapping request')
    own=state['worker'];p._integer(own['reservedRevision'],'owned mapping revision')
    require(own['profileSha256']==worker.policy.sha256
        and state['binding'].get('workerProfileSha256')==worker.policy.sha256
        and state['binding'].get('ownedScheduleRequestId')==worker.owned_schedule_request_id,
        'indexed_mapping_operation_changed')
    require(worker.operator is not None and worker.operator.origin==worker.api.origin,'indexed_mapping_operator_required')
    wire=worker.operator.json('GET','/v1/prototype/nodes/'+worker.policy.node_id+'/schedule-owned/'+worker.owned_schedule_request_id)
    require(type(wire) is dict and set(wire)=={'receipt'},'indexed_mapping_schedule_changed')
    require(all(wire['receipt'].get(k)==v for k,v in dict(requestId=worker.owned_schedule_request_id,
        authorityId=worker.policy.authority_id,nodeId=worker.policy.node_id,planId=state['planId'],targetImage=state['targetImage']).items()),
        'indexed_mapping_schedule_changed')
    update=read_json(path/'update.json')
    require(all(update.get(k)==v for k,v in dict(nodeId=worker.policy.node_id,planId=state['planId'],
        oldImage=state['oldImage'],targetImage=state['targetImage'],reservedRevision=own['reservedRevision']).items()),
        'indexed_mapping_update_changed')
    return dict(receiptRoot=str(path),receiptIdentity=[info.st_dev,info.st_ino],policySha256=worker.policy.sha256,
        planId=state['planId'],ownedScheduleRequestId=worker.owned_schedule_request_id,reservedRevision=own['reservedRevision'],
        operationBindingSha256=p.digest(state['binding']),oldImage=state['oldImage'],targetImage=state['targetImage'],
        engineId=worker.policy.engine_id,containerId=worker.policy.container_id,imageId=worker.policy.image_id,
        recipeSha256=worker.policy.recipe_sha256,configBaseSha256=worker.policy.config_base_sha256)

def capture_mapping_witness(worker,state):
    """Return the bounded witness for the SAME durable stop_pending write."""
    from ops.reference_worker_maintenance import private
    proof=_PROOFS.get(worker);require(proof is not None,'indexed_stopped_mapping_unproven')
    require(proof['pid']==os.getpid() and proof['policy']==worker.policy.sha256,'indexed_mapping_observer_changed')
    worker.barrier(state)
    require(worker.docker(['info','--format','{{.ID}}']).strip()==worker.policy.engine_id,'indexed_mapping_engine_changed')
    item=worker.inspect();require(item['running'] and item['pid']>0,'indexed_mapping_live_worker_required')
    config,view=worker.config()
    require(view==proof['config'] and proof['base']==p.digest({k:v for k,v in config.items() if k!='engineVersion'}),'indexed_mapping_config_changed')
    facts=[]
    for source,target in proof['pairs']:
        _,info=private(source,directory=Path(source).is_dir())
        facts.append([info.st_dev,info.st_ino,info.st_uid,info.st_gid,info.st_mode&0o777])
    require(_same(facts,proof['facts']),'indexed_mount_identity_changed')
    value=dict(schemaVersion=1,purpose='prospective-indexed-stop-mapping',**_operation(worker,state),
        beforeConfig=view,pairs=[list(pair) for pair in proof['pairs']],facts=facts)
    require(len(p.canonical_json(value))<=16384,'indexed_mapping_witness_bound')
    return value

def _config_at_checkpoint(worker,state,witness):
    """Only the existing atomic rebind's exact inode/bytes may advance config."""
    own=state['worker'];phase=own['phase'];before=witness['beforeConfig']
    require(_same(own['beforeConfig'],before),'indexed_mapping_before_config_changed')
    config,view=worker.config()
    if _same(view,before):
        require(phase in ('stop_pending','stopped','quiesced','rebind_pending'),'indexed_mapping_config_phase_changed')
        require(_same(worker.publication_read(before),own['beforePublication']),'indexed_mapping_publication_changed')
    else:
        require(phase in ('rebind_pending','publication_pending','rebound','start_pending','verified','finish_pending'),
            'indexed_mapping_config_phase_changed')
        after=own.get('afterConfig')
        require(type(after) is dict and set(after)=={'sha256','identity','engineVersion'},'indexed_mapping_rebind_required')
        # rename changes ctime only; existing rebind() records that final ctime
        # before advancing publication. No new temp or config is constructed.
        require(view['sha256']==after['sha256'] and view['engineVersion']==after['engineVersion']
            and view['identity'][:4]==after['identity'][:4] and view['identity'][5:]==after['identity'][5:]
            and (phase=='rebind_pending' or _same(view,after)),'indexed_mapping_rebind_changed')
        if phase=='rebind_pending':
            require(_same(worker.publication_read(before),own['beforePublication']),'indexed_mapping_publication_changed')
        else:
            expected=worker.publication_value(view,state['planId'])
            require(_same(own.get('afterPublication'),expected)
                and _same(worker.publication_read(view),expected),'indexed_mapping_publication_changed')
    return config,view

def restore_mapping_context(worker,state,*,running=False):
    """Revalidate a prospective owned receipt; never adopt old JSON or cold roots."""
    from ops.reference_worker_maintenance import read_file,private
    from ops.reference_worker_daemon import DaemonStateLock
    require(type(running) is bool,'indexed_mapping_mode_required')
    root,_=_receipt(worker);journal=root/'managed-update.json'
    raw,record_identity=read_file(journal)
    update_raw,update_identity=read_file(root/'update.json')
    update=p.decode_submission(update_raw)
    stored=p.decode_submission(raw)
    require(_same(stored,state),'indexed_mapping_journal_changed')
    own=stored['worker'];witness=own.get('mappingWitness')
    require(type(witness) is dict,'indexed_stopped_mapping_unproven')
    p._closed(witness,_WITNESS_FIELDS,'prospective mapping witness')
    require(type(witness['schemaVersion']) is int and witness['schemaVersion']==1
        and witness['purpose']=='prospective-indexed-stop-mapping'
        and len(p.canonical_json(witness))<=16384,'indexed_mapping_witness_changed')
    expected=_operation(worker,stored)
    require(all(type(witness[k]) is type(v) and _same(witness[k],v) for k,v in expected.items()),'indexed_mapping_operation_changed')
    require(type(witness['pairs']) is list and len(witness['pairs'])==8
        and all(type(v) is list and len(v)==2 and all(type(x) is str for x in v) for v in witness['pairs'])
        and type(witness['facts']) is list and len(witness['facts'])==8
        and all(type(v) is list and len(v)==5 and all(type(x) is int and x>=0 for x in v) for v in witness['facts']),
        'indexed_mapping_facts_changed')
    # Only definite backend success is resumable here. An uncertain execute()
    # receipt still requires separate reconciliation, never replacement replay.
    require(update.get('status')=='succeeded' and update.get('stage') in ('finalization_pending','finish_pending','completed')
        and type(update.get('result')) is dict and update['result'].get('image')==stored['targetImage']
        and update['result'].get('healthy') is True,'indexed_mapping_backend_success_required')
    worker.barrier(stored)
    require(worker.docker(['info','--format','{{.ID}}']).strip()==worker.policy.engine_id,'indexed_mapping_engine_changed')
    item=worker.inspect()
    if running:require(item['running'] and item['pid']>0,'indexed_mapping_running_required')
    else:
        require(not item['running'] and item['pid']==0 and item['exitCode']==0,'indexed_worker_not_stopped')
        require(type(worker._owner) is DaemonStateLock,'indexed_maintenance_owner_required')
        worker._owner.check(Path(worker.policy.state_root))
    config,view=_config_at_checkpoint(worker,stored,witness)
    # Reconstruct the exact eight expected path pairs, not arbitrary paths from
    # the receipt. The seven stable identities never acquire rebind exceptions.
    gate='/var/lib/ledgetop-gates/LedgeTop-Processing/node-gates/'+hashlib.sha256(worker.policy.engine_id.encode()).hexdigest()
    pairs=[(worker.policy.config_directory,worker.policy.container_config_directory),(worker.policy.state_root,config['stateRoot']),
        (worker.policy.scratch_root,config['scratchRoot']),(worker.policy.gate_directory,gate),
        (str(Path(worker.policy.config_directory)/'worker.json'),str(Path(worker.policy.container_config_directory)/'worker.json'))]
    for key in ('caFile','serverCertFile','serverKeyFile'):
        pairs.append((str(Path(worker.policy.config_directory)/Path(config[key]).relative_to(worker.policy.container_config_directory)),config[key]))
    require(_same(pairs,witness['pairs']),'indexed_mapping_paths_changed')
    facts=[]
    for index,(source,target) in enumerate(pairs):
        _,info=private(source,directory=index<4)
        fact=[info.st_dev,info.st_ino,info.st_uid,info.st_gid,info.st_mode&0o777]
        if index!=4:require(fact==witness['facts'][index],'indexed_mount_identity_changed')
        else:
            before=witness['beforeConfig']['identity']
            require(witness['facts'][index]==[before[0],before[1],before[5],before[6],before[7]],'indexed_mapping_config_fact_changed')
            require(fact==[view['identity'][0],view['identity'][1],view['identity'][5],view['identity'][6],view['identity'][7]],'indexed_mapping_config_fact_changed')
        facts.append(tuple(fact))
    if running:
        # Actual trusted exec is mandatory again after a cold controller start.
        observe_running_mounts(worker)
        live=_PROOFS[worker]
        require(_same(live['pairs'],pairs) and _same(live['facts'],facts),'indexed_runtime_mount_identity_changed')
    else:
        _PROOFS[worker]=dict(pid=os.getpid(),container=worker.policy.container_id,engine=worker.policy.engine_id,
            policy=worker.policy.sha256,config=view,base=p.digest({k:v for k,v in config.items() if k!='engineVersion'}),
            pairs=tuple(pairs),facts=tuple(facts))
    try:
        if running:
            context=object.__new__(IndexedMaintenanceContext)
            _CONTEXTS[context]=(weakref.ref(worker),_PROOFS[worker],None)
        else:context=stopped_context(worker)
        _authority(context,worker.policy.state_root,running=running)
        again,again_identity=read_file(journal)
        require(raw==again and record_identity==again_identity,'indexed_mapping_journal_changed')
        update_again,update_after=read_file(root/'update.json')
        require(update_raw==update_again and update_identity==update_after,'indexed_mapping_update_changed')
        worker.barrier(stored)
        return context
    except BaseException:
        _PROOFS.pop(worker,None)
        raise


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
        from ops.reference_worker_daemon import DaemonStateLock
        require(type(owner) is DaemonStateLock and worker._owner is owner,'indexed_maintenance_owner_changed')
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
            root_identity=_id(root),driver_identity=_id(root/'driver.sqlite'),policy=j.policy_from_binding(binding))
    return intent,receipts

def indexed_database_cap(context,root,*,running=False):
    intent,_=_authority(context,root,running=running)
    return j.policy_from_binding(intent['binding']).database_cap


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
    intent,receipts=_authority(context,root,running=running);policy=j.policy_from_binding(intent['binding']);ro=_ReadOnlyJournal(intent,root);ro.check(db)
    tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
    require(not db.execute("SELECT 1 FROM driver_jobs WHERE typeof(data)!='text' OR length(CAST(data AS BLOB))>8192 LIMIT 1").fetchone(),'indexed_maintenance_projection_bound')
    jobs=list(db.execute('SELECT job,claim_request,stage,data FROM driver_jobs LIMIT 65'))
    require(len(jobs)<=policy.retained_jobs,'indexed_maintenance_job_bound')
    require(len(jobs)==db.execute('SELECT COUNT(*) FROM indexed_claims').fetchone()[0]==db.execute('SELECT COUNT(*) FROM indexed_quota').fetchone()[0],
        'indexed_maintenance_job_closure')
    require(not db.execute("SELECT 1 FROM indexed_quota WHERE typeof(charge)!='integer' OR charge!=? LIMIT 1",(j.JOB_CHARGE,)).fetchone(),'indexed_quota_changed')
    require(db.execute('SELECT COALESCE(SUM(charge),0) FROM indexed_quota').fetchone()[0]+j.NON_JOB_ALLOWANCE<=policy.database_cap,'indexed_quota_changed')
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
    # Readiness is a terminal-native UUID cache, not pending work. It may survive
    # only for these independently audited completed jobs; no foreign cache,
    # malformed UUID or excess selected-profile slot is adopted or deleted.
    cached=db.execute('SELECT uuid FROM readiness_tasks LIMIT ?',(policy.capacity+1,)).fetchall()
    require(len(cached)<=policy.capacity,'indexed_readiness_task_unresolved')
    completed={row['job'] for row in jobs}
    for entry in cached:
        try:p._uuid(entry[0],'cached native task')
        except (TypeError,ValueError):raise IndexedMaintenanceHeld('indexed_readiness_task_unresolved') from None
        require(entry[0] in completed,'indexed_readiness_task_unresolved')
    after=scratch_path.lstat()
    require((before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns,before.st_ctime_ns)==
        (after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns),'indexed_scratch_changed_during_audit')
    settings=db.execute('SELECT binding FROM driver_binding WHERE singleton=1').fetchone()
    if settings:bootstrap._driver_settings(json.loads(settings[0]),intent['binding'],indexed=True)
    limits={name:policy.retained_jobs for name in tables}
    limits.update(driver_binding=1,driver_engine_pin=1,readiness_tasks=policy.capacity,indexed_profile=1,indexed_objects=10000*policy.capacity,indexed_attempts=10010*policy.capacity)
    hashes={name:_hash_rows(db,name,limits[name]) for name in sorted(tables)}
    # Engine pin legitimately advances on owned native version admission; it is
    # fenced separately by gate/backend/config and omitted only from retained hash.
    history={name:value for name,value in hashes.items() if name!='driver_engine_pin'}
    return hashes,dict(schemaVersion=1,profile='indexed-worker-fetch-v1',tables=history,bootstrap=receipts)
