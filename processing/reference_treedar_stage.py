"""Inactive native TreeDar attempt journal; intentionally not a v1 result adapter.

Only begin mutates Docker, once per issued parent intent. tick observes Docker
and checkpoints retained native evidence. read_evidence is SQL-only. No parent
completion, outbox, reservation release, cancellation or cleanup exists here.
"""
from contextlib import contextmanager,closing
from dataclasses import asdict,dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import threading

from . import reference_protocol as p,reference_pipeline as pipeline
from .node_gate import NodeGate
from .reference_odm_stage import NativeODMStage
from .reference_parent_guard import _readonly_sqlite,_private,_private_json,exact
from .scratch_policy import ScratchLedger
from .treedar_admission import AdmissionPolicy
from .treedar_engine import TreeDarConfig,SOURCE_FILES,source_digest,_file,_sync_directory,_write_new
from .update_registry import _run


class TreeStageHeld(ValueError):pass


def encoded(value):return p.canonical_json(value).decode()
def detached(value):return json.loads(p.canonical_json(value))


def environment(values):
    if type(values) not in (tuple,list):raise TreeStageHeld('closed_image_environment_required')
    result={}
    for value in values:
        if type(value) is not str or '=' not in value or any(c in value for c in '\0\n\r'):raise TreeStageHeld('fixed_image_environment_required')
        key,content=value.split('=',1)
        if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',key) or key in result:raise TreeStageHeld('unique_image_environment_keys_required')
        result[key]=content
    return result


def image_labels(values):
    if type(values) is not tuple:raise TreeStageHeld('immutable_image_labels_required')
    result={}
    for pair in values:
        if type(pair) is not tuple or len(pair)!=2:raise TreeStageHeld('closed_image_labels_required')
        key,value=pair
        if (type(key) is not str or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.:/-]{0,255}',key) or key in result
                or type(value) is not str or len(value)>4096 or any(c in value for c in '\0\n\r')):raise TreeStageHeld('fixed_unique_image_labels_required')
        result[key]=value
    return result


@dataclass(frozen=True)
class TreeStagePolicy:
    config:TreeDarConfig
    admission:AdmissionPolicy=AdmissionPolicy()
    control_budget:int=65536
    native_proof_budget:int=65536
    max_raw_bytes:int=64*1024**2
    max_files:int=64
    image_environment:tuple[str,...]=()
    image_labels:tuple[tuple[str,str],...]=()

    def __post_init__(self):
        if type(self.config) is not TreeDarConfig or type(self.admission) is not AdmissionPolicy:raise TreeStageHeld('fixed_tree_configuration_required')
        for value in (self.control_budget,self.native_proof_budget,self.max_raw_bytes,self.max_files):p._integer(value,'native evidence bound')
        if self.max_files>64 or self.control_budget>1024**2 or self.native_proof_budget>1024**2:raise TreeStageHeld('bounded_native_evidence_required')
        if type(self.image_environment) is not tuple:raise TreeStageHeld('fixed_image_environment_required')
        environment(self.image_environment)
        image_labels(self.image_labels)
    @property
    def sha256(self):return p.digest(dict(protocol='treedar-native-stage-runtime-v1',policy=asdict(self)))


SCHEMA=(
 'CREATE TABLE tree_stage_binding(id INTEGER PRIMARY KEY CHECK(id=1),value TEXT NOT NULL)',
 '''CREATE TABLE tree_stage_attempts(job TEXT PRIMARY KEY,attempt TEXT UNIQUE NOT NULL,intent TEXT NOT NULL,
    preparation TEXT NOT NULL,phase TEXT NOT NULL,container TEXT,evidence TEXT)''')
PHASES=('prepare_pending','prepared','create_pending','created','start_pending','processing','terminal_evidence')


class TreeDarNativeStage:
    """No start/lookup/verify adapter methods: v1 scientific closure is disabled."""
    def __init__(self,path,policy,*,journal,ledger,parent_guard,state_owner,input_resolver,provenance_reader,prior_odm,runner=None):
        from ops.reference_worker_daemon import DaemonStateLock
        if (os.name!='posix' or type(policy) is not TreeStagePolicy or type(journal) is not pipeline.PipelineJournal
                or type(ledger) is not ScratchLedger or type(state_owner) is not DaemonStateLock or type(prior_odm) is not NativeODMStage):raise TreeStageHeld('actual_tree_dependencies_required')
        self.path=Path(path).absolute();self.root=self.path.parent;_private(self.root,directory=True);state_owner.check(self.root)
        c=policy.config;binding=journal.adapter_binding('treedar')
        if (journal.root!=self.root or Path(ledger.path).absolute().parent!=self.root or os.geteuid()!=c.uid or os.getegid()!=c.gid
                or policy.sha256!=journal.policy.treedar_runtime_sha256 or c.engine_id!=journal.policy.engine_id or c.engine_version!=journal.policy.treedar_version
                or (c.authority_id,c.worker_id,c.coordinator_origin,c.gate_generation)!=(journal.policy.authority_id,journal.policy.worker_id,journal.policy.coordinator_origin,journal.policy.gate_generation)
                or getattr(parent_guard,'binding',None)!=journal.guard_binding() or not callable(getattr(parent_guard,'check',None))
                or any(getattr(reader,'binding',None)!=binding or not callable(getattr(reader,method,None)) for reader,method in ((input_resolver,'resolve'),(provenance_reader,'read')))
                or prior_odm.journal is not journal or prior_odm.ledger is not ledger or prior_odm.owner is not state_owner
                or prior_odm.binding!=journal.adapter_binding('odm')):raise TreeStageHeld('tree_dependency_binding_changed')
        self.policy,self.journal,self.ledger,self.guard,self.owner=policy,journal,ledger,parent_guard,state_owner
        self.inputs,self.provenance,self.prior,self.runner=input_resolver,provenance_reader,prior_odm,runner
        self._dependencies=(journal,ledger,parent_guard,state_owner,input_resolver,provenance_reader,prior_odm,runner)
        self._pid=os.getpid();self._mutex=threading.RLock();self._busy=False;self.binding=binding
        self.lock_path=self.path.with_name(self.path.name+'.lock')
        known=(journal.path,journal.lock_path,Path(ledger.path).absolute(),state_owner.root/'worker-daemon.lock',prior_odm.path,prior_odm.lock_path)
        if any(path in known or path in {Path(str(k)+s) for k in known for s in ('-journal','-wal','-shm','.lock')} for path in (self.path,self.lock_path)):raise TreeStageHeld('dedicated_tree_stage_paths_required')
        self._root_identity=_private(self.root,directory=True)[1];self._files={}
        for path in (self.path,self.lock_path):
            if path.resolve()!=path:raise TreeStageHeld('private_tree_journal_required')
            fd=os.open(path,os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW|os.O_NONBLOCK,0o600)
            try:
                self._files[path]=_private(path)[1]
                if (os.fstat(fd).st_dev,os.fstat(fd).st_ino)!=self._files[path]:raise TreeStageHeld('tree_file_identity_changed')
            finally:os.close(fd)
        self._ledger_identity=_private(Path(ledger.path).absolute())[1]
        self._binding=encoded(dict(protocol='treedar-native-attempt-v1',runtimeSha256=policy.sha256,pipelineBinding=journal._binding,
            root=str(self.root),rootIdentity=self._root_identity,files={str(k):v for k,v in self._files.items()},ledgerIdentity=self._ledger_identity))
        with self._operation(),self._db() as db:
            existing=list(db.execute("SELECT name FROM sqlite_master WHERE name NOT GLOB 'sqlite_*'"))
            if not existing:
                for sql in SCHEMA:db.execute(sql)
            self._schema(db)
            row=db.execute('SELECT value FROM tree_stage_binding WHERE id=1').fetchone()
            if row and row[0]!=self._binding or not row and db.execute('SELECT 1 FROM tree_stage_attempts').fetchone():raise TreeStageHeld('tree_journal_binding_changed')
            db.execute('INSERT OR IGNORE INTO tree_stage_binding VALUES(1,?)',(self._binding,))
        _sync_directory(self.root);_sync_directory(self.root.parent);self._initialized=True

    def _check(self):
        if self._pid!=os.getpid():raise TreeStageHeld('tree_owner_process_changed')
        self.owner.check(self.root);self.journal._check()
        if (any(a is not b for a,b in zip((self.journal,self.ledger,self.guard,self.owner,self.inputs,self.provenance,self.prior,self.runner),self._dependencies))
                or self.policy.sha256!=self.journal.policy.treedar_runtime_sha256 or self.binding!=self.journal.adapter_binding('treedar')
                or self.guard.binding!=self.journal.guard_binding() or self.inputs.binding!=self.binding or self.provenance.binding!=self.binding
                or _private(self.root,directory=True)[1]!=self._root_identity
                or any(_private(path)[1]!=identity for path,identity in self._files.items())
                or _private(Path(self.ledger.path).absolute())[1]!=self._ledger_identity):raise TreeStageHeld('tree_owner_or_configuration_changed')

    @staticmethod
    def _schema(db):
        with closing(sqlite3.connect(':memory:')) as expected:
            for sql in SCHEMA:expected.execute(sql)
            if NativeODMStage._inventory(expected)!=NativeODMStage._inventory(db):raise TreeStageHeld('tree_journal_schema_changed')

    @contextmanager
    def _operation(self):
        self._check()
        with self._mutex:
            if self._busy:raise TreeStageHeld('tree_operation_active')
            fd=os.open(self.lock_path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
            try:
                import fcntl
                if (os.fstat(fd).st_dev,os.fstat(fd).st_ino)!=self._files[self.lock_path]:raise TreeStageHeld('tree_lock_changed')
                try:fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
                except BlockingIOError:raise TreeStageHeld('tree_operation_active') from None
                self._busy=True;yield;self._check()
            finally:self._busy=False;os.close(fd)

    @contextmanager
    def _db(self,*,readonly=False):
        self._check()
        # Empty journal is allowed only during the first constructor transaction.
        if self.path.stat().st_size:
            with _readonly_sqlite(self.path):pass
        db=sqlite3.connect(self.path.as_uri()+'?mode=ro',uri=True) if readonly else sqlite3.connect(self.path,timeout=5)
        db.row_factory=sqlite3.Row
        try:
            db.execute('BEGIN' if readonly else 'PRAGMA synchronous=FULL')
            if not readonly:db.execute('BEGIN IMMEDIATE')
            if getattr(self,'_initialized',False):
                self._schema(db);row=db.execute('SELECT value FROM tree_stage_binding WHERE id=1').fetchone()
                if not row or row[0]!=self._binding:raise TreeStageHeld('tree_journal_binding_changed')
            yield db;self._check()
            if not readonly:db.commit()
        except BaseException:
            if not readonly:db.rollback()
            raise
        finally:db.close()

    def _row(self,job):
        with self._db(readonly=True) as db:row=db.execute('SELECT * FROM tree_stage_attempts WHERE job=?',(job,)).fetchone()
        if row is None:return None
        row=dict(row)
        if row['phase'] not in PHASES or (row['phase']=='terminal_evidence')!=(row['evidence'] is not None):raise TreeStageHeld('tree_checkpoint_changed')
        return row

    def _phase(self,job,phase,*,container=None,evidence=None):
        if phase not in PHASES:raise TreeStageHeld('tree_checkpoint_invalid')
        with self._db() as db:db.execute('UPDATE tree_stage_attempts SET phase=?,container=COALESCE(?,container),evidence=? WHERE job=?',(phase,container,encoded(evidence) if evidence else None,job))

    def _intent(self,intent):
        intent=detached(intent);parent=intent['parentBinding'];job=parent['jobId'];p._uuid(job,'parent job')
        with _readonly_sqlite(self.journal.path) as db:
            bound=db.execute('SELECT value FROM pipeline_binding WHERE id=1').fetchone()
            row=db.execute('SELECT * FROM pipeline_parents WHERE job=?',(job,)).fetchone()
            stages={r['stage']:dict(r) for r in db.execute('SELECT * FROM pipeline_stages WHERE job=?',(job,))}
        if not bound or bound[0]!=self.journal._binding or not row or set(stages)!=set(('odm','treedar')):raise TreeStageHeld('actual_combined_parent_required')
        submission=pipeline._submission(json.loads(row['submission']),parent['environment']);plan=pipeline._plan(self.journal.policy,parent,submission)
        if row['identity']!=encoded(parent) or row['plan']!=encoded(plan) or row['state']!='executing' or row['result'] is not None or row['failure'] is not None or stages['odm']['state']!='succeeded' or stages['treedar']['state']!='issued' or stages['treedar']['intent']!=encoded(intent) or stages['treedar']['proof'] is not None:raise TreeStageHeld('exact_issued_tree_intent_required')
        prior_intent=json.loads(stages['odm']['intent']);self.journal._intent(prior_intent,plan,'odm',None)
        prior_proof=pipeline.validate_stage_proof(json.loads(stages['odm']['proof']),intent=prior_intent,policy=self.journal.policy)
        if prior_proof['outcome']!='succeeded':raise TreeStageHeld('verified_odm_success_required')
        self.journal._intent(intent,plan,'treedar',prior_proof['proofSha256'])
        if len(intent['stage']['inputObjects'])!=1 or intent['stage']['inputObjects'][0]['kind']!='lidar':raise TreeStageHeld('original_lidar_stage_required')
        return intent,plan,prior_intent,prior_proof

    def _guard(self,parent,mutation):
        if self.guard.check(detached(parent),mutation=mutation) is not None:raise TreeStageHeld('actual_parent_authorization_required')

    def _provenance(self,intent,plan):
        receipt=self.provenance.read(detached(intent))
        if not exact(receipt,intent['provenance']):raise TreeStageHeld('fresh_original_provenance_required')
        return self.journal._provenance(plan,receipt)

    def _local(self,parent):
        # ParentGuard must run outside this gate; no nested flock callback.
        with NodeGate(self.policy.config.engine_id) as gate:NativeODMStage._gate(self,gate,parent)
        return self._reservation(parent)

    def _reservation(self,parent):
        with _readonly_sqlite(Path(self.ledger.path).absolute()) as db:
            row=db.execute('SELECT * FROM reservations WHERE id=?',(parent['reservationId'],)).fetchone()
        if row is None:raise TreeStageHeld('whole_parent_reservation_required')
        row=dict(row);settings=json.loads(row['settings'])
        expected=dict(job_id=parent['jobId'],generation=parent['generation'],reservation_id=parent['reservationId'],manifest_sha256=parent['manifestSha256'],input_bytes=parent['inputBytes'],output_budget=parent['outputBudget'],environment=parent['environment'])
        if (not exact({k:settings[k] for k in expected},expected) or row['state'] not in ('engine_started','uncertain') or row['terminal_at'] is not None
                or row['allocated']<parent['inputBytes'] or row['allocated']+row['remaining']!=parent['inputBytes']+parent['outputBudget']):raise TreeStageHeld('retained_parent_reservation_changed')
        return row

    def _docker(self,args):
        c=self.policy.config;command=[c.docker_executable,'--host','unix://'+c.docker_socket,*args]
        try:
            if self.runner:result=self.runner(command,timeout=20)
            else:result=_run(command,timeout=20,max_bytes=262144)
            output=result.stdout.decode() if isinstance(result.stdout,bytes) else result.stdout
            if result.returncode!=0 or type(output) is not str or len(output)>262144:raise ValueError()
            return output.strip()
        except Exception:raise TreeStageHeld('tree_docker_outcome_unavailable') from None

    def _host(self):
        c=self.policy.config;socket=Path(c.docker_socket)
        if socket.resolve()!=socket or not stat.S_ISSOCK(socket.stat().st_mode):raise TreeStageHeld('local_docker_socket_required')
        if self._docker(['info','--format','{{.ID}}'])!=c.engine_id or self._docker(['image','inspect','--format','{{.Id}}',c.image_id])!=c.image_id:raise TreeStageHeld('tree_runtime_image_changed')
        if source_digest(c.source_root)!=c.source_sha256:raise TreeStageHeld('tree_bootstrap_source_changed')
        for name in SOURCE_FILES:
            path=Path(c.source_root)/'processing'/name;info=path.lstat()
            if path.resolve()!=path or any(x.is_symlink() for x in path.parents) or not stat.S_ISREG(info.st_mode) or info.st_mode&0o022 or info.st_nlink!=1:raise TreeStageHeld('fixed_bootstrap_source_required')

    def _mounts(self,preparation):
        values=[(preparation['input'],'/input/cloud.las',True),(preparation['output'],'/output',False),(preparation['control'],'/control/task.json',True)]
        values += [(str(Path(self.policy.config.source_root)/'processing'/name),'/worker/processing/'+name,True) for name in SOURCE_FILES]
        return values

    def _inspect(self,row):
        prep=json.loads(row['preparation']);c=self.policy.config
        values=_private_json(self._docker(['inspect',row['container'] or prep['name']]))
        if type(values) is not list or len(values)!=1:raise TreeStageHeld('one_exact_tree_container_required')
        value=values[0];config=value['Config'];host=value['HostConfig'];state=value['State']
        expected_environment=environment(self.policy.image_environment)
        expected_environment.update(PYTHONDONTWRITEBYTECODE='1',PYTHONPATH='/worker')
        expected_labels=image_labels(self.policy.image_labels)
        expected_labels['ltds.treedar.stage']=p.digest(prep)
        mounts=sorted((x['Source'],x['Destination'],not x['RW']) for x in value['Mounts'] if x['Type']=='bind')
        if (type(value['Id']) is not str or not re.fullmatch('[a-f0-9]{64}',value['Id']) or row['container'] and row['container']!=value['Id']
                or value['Name']!='/'+prep['name'] or value['Image']!=c.image_id or config['Image']!=c.image_id
                or config['User']!=f'{c.uid}:{c.gid}' or config['Entrypoint']!=['/usr/local/bin/python'] or config['Cmd']!=['-B','-m','processing.treedar_engine','--inside']
                or not exact(config['Labels'],expected_labels) or config['WorkingDir']!='/'
                or environment(config['Env'])!=expected_environment
                or host['NetworkMode']!='none' or host['ReadonlyRootfs'] is not True or host['Privileged'] is not False
                or host['CapDrop']!=['ALL'] or host.get('CapAdd') or host['SecurityOpt']!=['no-new-privileges']
                or host['LogConfig']['Type']!='none' or host.get('Devices') or host.get('DeviceRequests') or host.get('Binds')
                or host.get('PidMode','') or host.get('IpcMode','private') not in ('private','') or host.get('PortBindings')
                or host.get('RestartPolicy',{}).get('Name')!='no' or host.get('AutoRemove') is not False
                or host.get('Memory',0)!=0 or host.get('NanoCpus',0)!=0 or host.get('CpuQuota',0)!=0 or host.get('PidsLimit') not in (None,0)
                or host['Tmpfs']!={'/tmp':'rw,nosuid,nodev,mode=1777'} or len(value['Mounts'])!=len(self._mounts(prep))
                or mounts!=sorted(self._mounts(prep)) or type(state['Running']) is not bool or type(state['ExitCode']) is not int or not 0<=state['ExitCode']<=255
                or state['Status'] not in ('created','running','exited') or state['Running']!=(state['Status']=='running')):raise TreeStageHeld('tree_container_configuration_changed')
        return value

    def _charge(self,parent,baseline,size):
        current=self._local(parent);amount=baseline+size
        if size>self.journal.policy.treedar_output_budget or amount>parent['inputBytes']+parent['outputBudget']:raise TreeStageHeld('tree_coexistence_budget_exceeded')
        self.ledger.record_allocated(parent['reservationId'],max(current['allocated'],amount))

    def begin(self,intent):
        with self._operation():
            intent,plan,odm_intent,odm_proof=self._intent(intent);parent=intent['parentBinding'];job=parent['jobId']
            if self._row(job) is not None:raise TreeStageHeld('tree_attempt_never_replayed')
            self._guard(parent,True);self.prior.verify(odm_intent,odm_proof);self._provenance(intent,plan);self._host()
            supplied=self.inputs.resolve(detached(intent));obj=intent['stage']['inputObjects'][0]
            if type(supplied) is not dict or set(supplied)!={obj['id']}:raise TreeStageHeld('exact_original_lidar_input_required')
            relative=supplied[obj['id']];self.ledger._owned_path(parent['reservationId'],relative);source=self.ledger.root/relative
            _file(source,obj['byteSize'],expected_bytes=obj['byteSize'],expected_sha=obj['sha256'])
            prior_paths=self.prior.artifact_paths(odm_intent);retained=0
            for artifact in odm_proof['objects']:
                path=self.ledger.root/prior_paths[artifact['id']];self.ledger._owned_path(parent['reservationId'],prior_paths[artifact['id']])
                retained+=_file(path,artifact['byteSize'],expected_bytes=artifact['byteSize'],expected_sha=artifact['sha256'])[0]
            baseline=self._local(parent)['allocated']
            if baseline<parent['inputBytes']+retained:raise TreeStageHeld('retained_odm_allocation_required')
            directory=self.ledger.root/('jobs/'+parent['reservationId']+'/tree-stage-'+intent['attemptId'])
            self.ledger._owned_path(parent['reservationId'],str(directory.relative_to(self.ledger.root)))
            prep=dict(name='ltds-tree-stage-'+intent['attemptId'],input=str(source),output=str(directory),control=str(directory.parent/('tree-'+intent['attemptId']+'.json')),baseline=baseline)
            for path in (prep['input'],prep['output'],prep['control'],self.policy.config.source_root):
                if any(c in path for c in ',\n\r\0'):raise TreeStageHeld('literal_tree_mount_required')
            task=dict(job={k:parent[k] for k in ('jobId','generation','environment','manifestSha256','pipelineDigest')},reservationId=parent['reservationId'],
                inputObject=obj,inputRelativePath=relative,outputRelativePath=str(directory.relative_to(self.ledger.root)),provenance=intent['provenance'],allowSynthetic=False,
                policy=asdict(self.policy.admission),outputBudget=self.policy.max_raw_bytes,sourceSha256=self.policy.config.source_sha256,
                engineVersion=self.policy.config.engine_version,timeoutSeconds=self.policy.config.timeout_seconds,maxFiles=self.policy.max_files)
            if len(p.canonical_json(task))>self.policy.control_budget:raise TreeStageHeld('tree_control_budget_exceeded')
            with self._db() as db:db.execute('INSERT INTO tree_stage_attempts VALUES(?,?,?,?,?,NULL,NULL)',(job,intent['attemptId'],encoded(intent),encoded(prep),'prepare_pending'))
            directory.mkdir(mode=0o700)
            # TreeDar's mkdir(exist_ok=True) preserves this private RAW root;
            # publication need not chmod or copy terminal engine outputs later.
            raw=directory/'run';raw.mkdir(mode=0o700)
            _sync_directory(raw);_sync_directory(directory);_sync_directory(directory.parent)
            _write_new(Path(prep['control']),task)
            self._phase(job,'prepared');self._charge(parent,baseline,Path(prep['control']).stat().st_size)
            # Hashing/provenance can take time. Recheck outside Gate; then hold
            # the physical lease across final runtime checks and each mutation.
            self._check();self.prior.verify(odm_intent,odm_proof);self._provenance(intent,plan);self._check();self._guard(parent,True)
            with NodeGate(self.policy.config.engine_id) as gate:
                NativeODMStage._gate(self,gate,parent);self._reservation(parent);self._host();self._check()
                args=['create','--pull','never','--name',prep['name'],'--label','ltds.treedar.stage='+p.digest(prep),'--network','none','--read-only','--cap-drop','ALL',
                    '--security-opt','no-new-privileges','--user',f'{self.policy.config.uid}:{self.policy.config.gid}','--log-driver','none','--tmpfs','/tmp:rw,nosuid,nodev,mode=1777',
                    '--workdir','/','--restart','no','--env','PYTHONDONTWRITEBYTECODE=1','--env','PYTHONPATH=/worker']
                for src,dest,readonly in self._mounts(prep):args+=['--mount',f'type=bind,source={src},target={dest}'+(',readonly' if readonly else '')]
                args+=['--entrypoint','/usr/local/bin/python',self.policy.config.image_id,'-B','-m','processing.treedar_engine','--inside']
                self._phase(job,'create_pending');container=self._docker(args)
                if not re.fullmatch('[a-f0-9]{64}',container):raise TreeStageHeld('tree_create_identity_uncertain')
                self._phase(job,'created',container=container)
            self._check();self._provenance(intent,plan);self._check();self._guard(parent,True)
            with NodeGate(self.policy.config.engine_id) as gate:
                NativeODMStage._gate(self,gate,parent);self._reservation(parent);self._host();self._check();observed=self._inspect(self._row(job))
                if observed['State']['Status']!='created':raise TreeStageHeld('fresh_created_tree_container_required')
                self._phase(job,'start_pending')
                if self._docker(['start',container])!=container:raise TreeStageHeld('tree_start_outcome_uncertain')
                self._phase(job,'processing')
            return dict(phase='processing',adapterIntegration='held_pending_outer_v2')

    def tick(self,job):
        with self._operation():
            row=self._row(job)
            if row is None:raise TreeStageHeld('unknown_tree_attempt')
            intent,plan,_,_=self._intent(json.loads(row['intent']));parent=intent['parentBinding'];prep=json.loads(row['preparation'])
            self._guard(parent,False);self._local(parent);self._host();observed=self._inspect(row)
            self._charge_observed(parent,prep)
            # Never start a discovered created container, even if create was
            # successful but its reply/checkpoint was lost.
            if observed['State']['Status']!='exited':return dict(phase=row['phase'],observed=observed['State']['Status'],adapterIntegration='held_pending_outer_v2')
            if row['evidence']:
                evidence=self._sealed(row,intent)
                if evidence['containerId']!=observed['Id'] or evidence['exitCode']!=observed['State']['ExitCode']:raise TreeStageHeld('tree_terminal_observation_changed')
                for obj in evidence['objects']:
                    _file(Path(prep['output'])/obj['relativePath'],obj['byteSize'],expected_bytes=obj['byteSize'],expected_sha=obj['sha256'])
                return evidence
            directory=Path(prep['output']);_private(directory,directory=True)
            objects=[];total=Path(prep['control']).stat().st_size;pending=[directory]
            while pending:
                current=pending.pop()
                with os.scandir(current) as stream:
                    for entry in stream:
                        if len(objects)+len(pending)>=self.policy.max_files+2:raise TreeStageHeld('bounded_tree_output_inventory_required')
                        path=Path(entry.path);info=path.lstat()
                        if stat.S_ISDIR(info.st_mode):
                            if path.name!='run' or current!=directory:raise TreeStageHeld('unexpected_tree_output_directory')
                            if info.st_uid!=os.geteuid() or info.st_mode&0o022:raise TreeStageHeld('owned_output_directory_required')
                            pending.append(path);continue
                        relative=str(path.relative_to(directory));maximum=self.policy.native_proof_budget if relative=='proof.json' else self.policy.max_raw_bytes
                        size,sha=_file(path,maximum);total+=size
                        objects.append(dict(relativePath=relative,byteSize=size,sha256=sha))
                        if total>self.policy.max_raw_bytes+self.policy.control_budget+self.policy.native_proof_budget:raise TreeStageHeld('bounded_raw_tree_outputs_required')
                        self._charge(parent,prep['baseline'],total)
            evidence=dict(schemaVersion=1,protocol='ltds-treedar-native-execution-v1',intentSha256=intent['intentSha256'],precedingProofSha256=intent['precedingProofSha256'],
                runtimeSha256=self.policy.sha256,containerId=observed['Id'],imageId=self.policy.config.image_id,engineId=self.policy.config.engine_id,
                engineVersion=self.policy.config.engine_version,exitCode=observed['State']['ExitCode'],objects=sorted(objects,key=lambda o:o['relativePath']),
                nativeStatus='exited_unvalidated',adapterIntegration='held_pending_outer_v2')
            self._objects(evidence['objects'])
            evidence['evidenceSha256']=p.digest(evidence)
            if len(p.canonical_json(evidence))>self.policy.native_proof_budget:raise TreeStageHeld('bounded_native_evidence_required')
            # Seal and read must have exactly one acceptance boundary. A Docker
            # observation is not sufficient to publish an unreadable checkpoint.
            evidence=self._sealed(dict(row,evidence=encoded(evidence)),intent)
            self._charge(parent,prep['baseline'],total+len(p.canonical_json(evidence)))
            self._phase(job,'terminal_evidence',evidence=evidence)
            return evidence

    def _charge_observed(self,parent,prep):
        """Count growing raw/control files on every tick, without hashing writers."""
        total=0;raw=0;raw_count=0;count=0;pending=[Path(prep['output'])]
        control=Path(prep['control']);_private(control)
        total+=control.stat().st_size
        if total>self.policy.control_budget:raise TreeStageHeld('tree_control_budget_changed')
        while pending:
            current=pending.pop()
            if current.resolve()!=current:raise TreeStageHeld('owned_native_output_required')
            with os.scandir(current) as stream:
                for item in stream:
                    count+=1
                    if count>self.policy.max_files+2:raise TreeStageHeld('bounded_raw_inventory_required')
                    path=Path(item.path);info=path.lstat()
                    if info.st_uid!=os.geteuid() or info.st_mode&0o022:raise TreeStageHeld('owned_native_output_required')
                    if stat.S_ISDIR(info.st_mode):
                        if current!=Path(prep['output']) or path.name!='run':raise TreeStageHeld('fixed_native_output_layout_required')
                        pending.append(path)
                    elif stat.S_ISREG(info.st_mode) and info.st_nlink==1:
                        total+=info.st_size
                        if current==Path(prep['output']) and path.name=='proof.json':
                            if info.st_size>self.policy.native_proof_budget:raise TreeStageHeld('native_proof_budget_exceeded')
                        else:
                            if current==Path(prep['output']):raise TreeStageHeld('fixed_native_output_layout_required')
                            raw+=info.st_size;raw_count+=1
                            if raw_count>self.policy.max_files:raise TreeStageHeld('bounded_raw_inventory_required')
                    else:raise TreeStageHeld('owned_native_output_required')
                    if raw>self.policy.max_raw_bytes or total>self.policy.max_raw_bytes+self.policy.control_budget+self.policy.native_proof_budget:raise TreeStageHeld('bounded_native_disk_required')
        self._charge(parent,prep['baseline'],total)

    def _sealed(self,row,intent):
        value=json.loads(row['evidence']);sha=value.pop('evidenceSha256')
        p._closed(value,('schemaVersion','protocol','intentSha256','precedingProofSha256','runtimeSha256','containerId','imageId','engineId','engineVersion','exitCode','objects','nativeStatus','adapterIntegration'),'native tree evidence')
        c=self.policy.config
        if (sha!=p.digest(value) or type(value['schemaVersion']) is not int or value['schemaVersion']!=1 or value['protocol']!='ltds-treedar-native-execution-v1'
                or value['intentSha256']!=intent['intentSha256'] or value['precedingProofSha256']!=intent['precedingProofSha256'] or value['runtimeSha256']!=self.policy.sha256
                or value['imageId']!=c.image_id or value['engineId']!=c.engine_id or value['engineVersion']!=c.engine_version
                or type(value['containerId']) is not str or not re.fullmatch('[a-f0-9]{64}',value['containerId']) or row['container'] and row['container']!=value['containerId']
                or type(value['exitCode']) is not int or not 0<=value['exitCode']<=255 or value['nativeStatus']!='exited_unvalidated' or value['adapterIntegration']!='held_pending_outer_v2'
                or type(value['objects']) is not list):raise TreeStageHeld('sealed_tree_evidence_changed')
        self._objects(value['objects'])
        value['evidenceSha256']=sha;return value

    def _objects(self,objects):
        """One shared seal/read bound: raw files plus optional native proof."""
        if type(objects) is not list or len(objects)>self.policy.max_files+1:raise TreeStageHeld('bounded_native_evidence_inventory_required')
        seen=set();raw_count=0;proof_count=0;raw_bytes=0
        for obj in objects:
            p._closed(obj,('relativePath','byteSize','sha256'),'native artifact');name=obj['relativePath']
            if type(name) is not str or not re.fullmatch(r'(proof\.json|run/[A-Za-z0-9_.-]+)',name) or name in seen:raise TreeStageHeld('closed_native_artifact_paths_required')
            seen.add(name);p._integer(obj['byteSize'],'native artifact bytes',allow_zero=True);p._hash(obj['sha256'],'native artifact SHA')
            if name=='proof.json':
                proof_count+=1
                if obj['byteSize']>self.policy.native_proof_budget:raise TreeStageHeld('native_proof_budget_exceeded')
            else:raw_count+=1;raw_bytes+=obj['byteSize']
        if raw_count>self.policy.max_files or proof_count>1 or raw_bytes>self.policy.max_raw_bytes:raise TreeStageHeld('bounded_native_evidence_inventory_required')

    def read_evidence(self,intent):
        """SQL-only sealed evidence read. Does not authorize, inspect or advance."""
        with self._operation():
            row=self._row(intent['parentBinding']['jobId'])
            if row is None:return None
            if row['intent']!=encoded(intent):raise TreeStageHeld('tree_attempt_rebound')
            if row['evidence'] is None:return None
            return self._sealed(row,intent)
