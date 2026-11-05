"""Host-owned intermediate NativeEngine attempt; never completes a parent job.

start issues each native mutation once. tick reads engine state and owns only
its archive cache/proof checkpoints. lookup reads sealed SQL evidence only.
The parent already owns its claim, scratch reservation and physical gate.
"""
from contextlib import contextmanager,closing
from dataclasses import asdict,dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
import threading
import zipfile
from urllib.parse import urlsplit

from . import reference_protocol as p,reference_native_archive as archive
from .node_gate import NodeGate,ENGINE
from .reference_pipeline import PipelineJournal,seal_stage_proof,validate_stage_proof
from .scratch_policy import ScratchLedger
from .update_registry import _run
from .viewer_service import NativeEngine
from .options import _validate_options
from .reference_runtime import _UNSAFE


class NativeStageHeld(ValueError):pass


def encoded(value):return p.canonical_json(value).decode()
def detached(value):return json.loads(p.canonical_json(value))


def absolute(value):
    if type(value) is not str or not value or not Path(value).is_absolute() or str(Path(value))!=value or '..' in Path(value).parts or any(c in value for c in '\0\n\r'):
        raise NativeStageHeld('literal_absolute_runtime_path_required')
    return Path(value)


@dataclass(frozen=True)
class NativeRuntimePolicy:
    docker_executable:str
    docker_endpoint:str
    engine_id:str
    compose_file:str
    compose_base_sha256:str
    compose_project:str
    compose_service:str
    backend_name:str
    image_id:str
    engine_origin:str
    engine_family:str
    engine_version:str
    container_port:int=3000
    probe_timeout_seconds:int=15
    archive_deadline_seconds:int=300
    max_census_tasks:int=100

    def __post_init__(self):
        absolute(self.docker_executable);absolute(self.compose_file)
        if type(self.docker_endpoint) is not str or not re.fullmatch(r'unix:///[A-Za-z0-9_./-]+',self.docker_endpoint):raise NativeStageHeld('local_docker_socket_required')
        absolute(self.docker_endpoint[7:])
        if type(self.engine_id) is not str or not ENGINE.fullmatch(self.engine_id):raise NativeStageHeld('fixed_engine_id_required')
        for value in (self.compose_project,self.compose_service,self.backend_name):p._slug(value,'fixed backend identity')
        p._hash(self.compose_base_sha256,'Compose base digest')
        if type(self.image_id) is not str or not re.fullmatch(r'sha256:[a-f0-9]{64}',self.image_id):raise NativeStageHeld('immutable_backend_image_required')
        try:
            endpoint=urlsplit(self.engine_origin)
            if (endpoint.scheme!='http' or endpoint.hostname!='127.0.0.1' or endpoint.username or endpoint.password
                    or endpoint.path or endpoint.query or endpoint.fragment or endpoint.port is None
                    or self.engine_origin!='http://127.0.0.1:'+str(endpoint.port)):raise ValueError()
        except (ValueError,TypeError):raise NativeStageHeld('fixed_private_loopback_origin_required') from None
        if self.engine_family not in ('odm','odx'):raise NativeStageHeld('fixed_native_engine_family_required')
        if type(self.engine_version) is not str or not 1<=len(self.engine_version)<=128 or not all(0x21<=ord(c)<=0x7e for c in self.engine_version):raise NativeStageHeld('fixed_native_engine_version_required')
        if type(self.container_port) is not int or not 1<=self.container_port<=65535 or type(self.probe_timeout_seconds) is not int or not 1<=self.probe_timeout_seconds<=60:raise NativeStageHeld('bounded_runtime_probe_required')
        if type(self.archive_deadline_seconds) is not int or not 60<=self.archive_deadline_seconds<=3600:raise NativeStageHeld('bounded_archive_transfer_required')
        if type(self.max_census_tasks) is not int or not 1<=self.max_census_tasks<=10000:raise NativeStageHeld('bounded_native_census_required')

    @property
    def sha256(self):return p.digest(dict(protocol='native-odm-stage-runtime-v1',policy=asdict(self)))


class NativeRuntimeProbe:
    """Concrete read-only local Docker/Compose census; runner injection is test-only."""
    def __init__(self,policy,*,runner=None):
        if type(policy) is not NativeRuntimePolicy:raise NativeStageHeld('fixed_runtime_policy_required')
        self.policy,self.runner=policy,runner or _run
        self._policy_sha256=policy.sha256

    def _command(self,args):
        try:
            result=self.runner([self.policy.docker_executable,'--host',self.policy.docker_endpoint,*args],timeout=self.policy.probe_timeout_seconds,max_bytes=262144)
            if result.returncode!=0 or len(result.stdout)>262144 or len(result.stderr)>262144:raise ValueError()
            return result.stdout.decode() if isinstance(result.stdout,bytes) else result.stdout
        except Exception:raise NativeStageHeld('native_runtime_probe_unavailable') from None

    def check(self):
        try:
            policy=self.policy
            if policy.sha256!=self._policy_sha256:raise ValueError()
            socket=Path(policy.docker_endpoint[7:])
            if socket.resolve()!=socket or not stat.S_ISSOCK(socket.stat().st_mode):raise ValueError()
            path=Path(policy.compose_file)
            info=path.lstat()
            if (path.resolve()!=path or any(p.is_symlink() for p in (path,*path.parents)) or not stat.S_ISREG(info.st_mode)
                    or info.st_uid!=os.geteuid() or info.st_nlink!=1 or info.st_mode&0o077 or info.st_size>131072):raise ValueError()
            source=json.loads(path.read_bytes())
            if source['services'][policy.compose_service]['image']!=policy.image_id:raise ValueError()
            source=detached(source);source['services'][policy.compose_service].pop('image')
            if p.digest(source)!=policy.compose_base_sha256:raise ValueError()
            if self._command(['info','--format','{{.ID}}']).strip()!=policy.engine_id:raise ValueError()
            template='{"id":{{json .Id}},"name":{{json .Name}},"image":{{json .Image}},"running":{{json .State.Running}},"labels":{{json .Config.Labels}},"ports":{{json .NetworkSettings.Ports}}}'
            value=json.loads(self._command(['container','inspect','--format',template,policy.backend_name]))
            p._closed(value,('id','name','image','running','labels','ports'),'native backend census')
            labels=value['labels']
            if (type(value['id']) is not str or not re.fullmatch(r'[a-f0-9]{64}',value['id']) or value['name']!='/'+policy.backend_name
                    or value['image']!=policy.image_id or value['running'] is not True
                    or labels.get('com.docker.compose.project')!=policy.compose_project or labels.get('com.docker.compose.service')!=policy.compose_service
                    or labels.get('com.docker.compose.project.config_files')!=policy.compose_file
                    or value['ports'].get(str(policy.container_port)+'/tcp')!=[dict(HostIp='127.0.0.1',HostPort=str(urlsplit(policy.engine_origin).port))]):raise ValueError()
            after=path.lstat()
            if (info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns)!=(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns):raise ValueError()
            return dict(engineId=policy.engine_id,containerId=value['id'],imageId=policy.image_id,composeBaseSha256=policy.compose_base_sha256)
        except NativeStageHeld:raise
        except Exception:raise NativeStageHeld('native_runtime_binding_changed') from None


SCHEMA=(
    'CREATE TABLE native_stage_binding(id INTEGER PRIMARY KEY CHECK(id=1),value TEXT NOT NULL)',
    '''CREATE TABLE native_stage_executions(job TEXT PRIMARY KEY,attempt TEXT UNIQUE NOT NULL,intent TEXT NOT NULL,
       paths TEXT NOT NULL,runtime_container TEXT NOT NULL,phase TEXT NOT NULL,part_dev TEXT,part_ino TEXT,
       archive_expected_size TEXT,archive_size INTEGER,archive_sha TEXT,proof TEXT,
       allocation_base INTEGER NOT NULL)''')
PHASES=('prepared','initialize_pending','upload_pending','commit_pending','processing','archive_open_pending','archive_pending','archive_ready','terminal_proof')


class NativeODMStage:
    def __init__(self,path,policy,*,journal,ledger,engine,parent_guard,state_owner,input_resolver,runtime_probe=None):
        from ops.reference_worker_daemon import DaemonStateLock
        if (os.name!='posix' or type(policy) is not NativeRuntimePolicy or not isinstance(journal,PipelineJournal)
                or not isinstance(ledger,ScratchLedger) or type(engine) is not NativeEngine or type(state_owner) is not DaemonStateLock):
            raise NativeStageHeld('actual_linux_stage_dependencies_required')
        self.path=Path(path).absolute();self.root=self.path.parent
        if self.path.resolve()!=self.path or any(p.is_symlink() for p in (self.path,*self.path.parents)):raise NativeStageHeld('private_stage_root_required')
        try:info=self.root.stat()
        except OSError:raise NativeStageHeld('preexisting_private_stage_root_required') from None
        if not stat.S_ISDIR(info.st_mode) or info.st_uid!=os.geteuid() or stat.S_IMODE(info.st_mode)!=0o700:raise NativeStageHeld('private_stage_root_required')
        state_owner.check(self.root)
        if journal.root!=self.root or Path(ledger.path).absolute().parent!=self.root:raise NativeStageHeld('shared_state_owner_required')
        expected=journal.adapter_binding('odm')
        if (policy.sha256!=journal.policy.odm_runtime_sha256 or policy.engine_id!=journal.policy.engine_id
                or policy.engine_version!=journal.policy.odm_version or engine.origin!=policy.engine_origin or engine.client.origin!=policy.engine_origin
                or getattr(parent_guard,'binding',None)!=journal.guard_binding() or not callable(getattr(parent_guard,'check',None))
                or getattr(input_resolver,'binding',None)!=expected or not callable(getattr(input_resolver,'resolve',None))):raise NativeStageHeld('stage_dependency_binding_changed')
        self.policy,self.journal,self.ledger,self.engine=policy,journal,ledger,engine
        self.guard,self.owner,self.inputs=parent_guard,state_owner,input_resolver
        self.probe=runtime_probe or NativeRuntimeProbe(policy)
        if type(self.probe) is not NativeRuntimeProbe or self.probe.policy!=policy:raise NativeStageHeld('actual_runtime_probe_required')
        self.binding=expected;self._engine_token=engine.token;self._pid=os.getpid();self._mutex=threading.RLock();self._busy=False
        self._client=engine.client
        self._dependencies=(journal,ledger,engine,parent_guard,state_owner,input_resolver,self.probe)
        self.lock_path=self.path.with_name(self.path.name+'.lock')
        known=(journal.path,journal.lock_path,Path(ledger.path).absolute(),self.root/'worker-daemon.lock')
        if self.path in known or self.lock_path in known or self.path in {Path(str(p)+suffix) for p in known for suffix in ('-journal','-wal','-shm','.lock')}:
            raise NativeStageHeld('dedicated_stage_paths_required')
        self._root_identity=(info.st_dev,info.st_ino)
        self._files={p:self._create(p) for p in (self.path,self.lock_path)}
        self._ledger_identity=self._file(Path(ledger.path).absolute())
        self._binding=encoded(dict(protocol='native-odm-stage-v1',runtime=asdict(policy),pipelineBinding=journal._binding,
            ledgerPath=str(Path(ledger.path).absolute()),ledgerIdentity=self._ledger_identity,scratchRoot=str(ledger.root),
            path=str(self.path),rootIdentity=self._root_identity,files={str(k):v for k,v in self._files.items()}))
        self._expected_schema=self._schema_template()
        with self._operation(),self._db() as db:
            objects=db.execute("SELECT name FROM sqlite_master WHERE name NOT GLOB 'sqlite_*'").fetchall()
            if any(row[0] not in ('native_stage_binding','native_stage_executions') for row in objects):raise NativeStageHeld('dedicated_native_stage_journal_required')
            if not objects:
                for sql in SCHEMA:db.execute(sql)
            self._schema(db)
            old=db.execute('SELECT value FROM native_stage_binding WHERE id=1').fetchone()
            if old and old[0]!=self._binding:raise NativeStageHeld('native_stage_binding_changed')
            if not old and db.execute('SELECT 1 FROM native_stage_executions').fetchone():raise NativeStageHeld('unbound_native_stage_rows')
            db.execute('INSERT OR IGNORE INTO native_stage_binding VALUES(1,?)',(self._binding,))
        for directory in (self.root,self.root.parent):self._sync(directory)
        self._initialized=True

    @staticmethod
    def _sync(path):
        fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try:os.fsync(fd)
        finally:os.close(fd)

    def _file(self,path,fd=None):
        before=path.lstat();observed=os.fstat(fd) if fd is not None else before
        if (not stat.S_ISREG(before.st_mode) or before.st_nlink!=1 or before.st_uid!=os.geteuid() or stat.S_IMODE(before.st_mode)!=0o600
                or (before.st_dev,before.st_ino)!=(observed.st_dev,observed.st_ino)):raise NativeStageHeld('owned_native_stage_file_required')
        return before.st_dev,before.st_ino

    def _create(self,path):
        fd=os.open(path,os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW|os.O_NONBLOCK,0o600)
        try:return self._file(path,fd)
        finally:os.close(fd)

    def _check(self):
        if self._pid!=os.getpid():raise NativeStageHeld('native_stage_process_changed')
        self.owner.check(self.root);self.journal._check();info=self.root.stat()
        if ((info.st_dev,info.st_ino)!=self._root_identity or self.root.resolve()!=self.root or stat.S_IMODE(info.st_mode)!=0o700
                or any(self._file(path)!=identity for path,identity in self._files.items())
                or self._file(Path(self.ledger.path).absolute())!=self._ledger_identity
                or self.policy.sha256!=self.journal.policy.odm_runtime_sha256 or self.engine.origin!=self.policy.engine_origin
                or self.engine.client is not self._client or self._client.origin!=self.policy.engine_origin
                or any(current is not original for current,original in zip((self.journal,self.ledger,self.engine,self.guard,self.owner,self.inputs,self.probe),self._dependencies))
                or self.engine.token!=self._engine_token or self.binding!=self.journal.adapter_binding('odm')
                or self.guard.binding!=self.journal.guard_binding() or self.inputs.binding!=self.binding):raise NativeStageHeld('native_stage_owner_or_policy_changed')
        descriptor=os.open(self.path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
        try:
            info=os.fstat(descriptor);header=os.read(descriptor,20)
            if ((info.st_dev,info.st_ino)!=self._files[self.path]
                    or info.st_size and (len(header)!=20 or header[:16]!=b'SQLite format 3\0' or header[18:20]!=b'\1\1')):
                raise NativeStageHeld('rollback_native_stage_journal_required')
        finally:os.close(descriptor)

    @contextmanager
    def _operation(self):
        self._check()
        with self._mutex:
            if self._busy:raise NativeStageHeld('native_stage_operation_active')
            fd=os.open(self.lock_path,os.O_RDWR|os.O_NOFOLLOW|os.O_NONBLOCK)
            try:
                import fcntl
                if self._file(self.lock_path,fd)!=self._files[self.lock_path]:raise NativeStageHeld('native_stage_lock_changed')
                try:fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
                except BlockingIOError:raise NativeStageHeld('native_stage_operation_active') from None
                self._busy=True;yield;self._check()
            finally:self._busy=False;os.close(fd)

    @contextmanager
    def _db(self,*,readonly=False):
        self._check()
        db=sqlite3.connect(self.path.as_uri()+'?mode=ro',uri=True) if readonly else sqlite3.connect(self.path,timeout=5)
        db.row_factory=sqlite3.Row
        try:
            if not readonly:db.execute('PRAGMA synchronous=FULL');db.execute('BEGIN IMMEDIATE')
            if getattr(self,'_initialized',False):
                self._schema(db)
                binding=db.execute('SELECT value FROM native_stage_binding WHERE id=1').fetchone()
                if not binding or binding[0]!=self._binding:raise NativeStageHeld('native_stage_binding_changed')
            yield db;self._check()
            if not readonly:self._commit(db)
        except BaseException:
            if not readonly:db.rollback()
            raise
        finally:db.close()

    def _commit(self,db):db.commit()

    @staticmethod
    def _inventory(connection):return [[*r[:3],re.sub(r'\s+','',r[3]).lower() if r[3] else None] for r in connection.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name')]

    @classmethod
    def _schema_template(cls):
        memory=sqlite3.connect(':memory:')
        try:
            for sql in SCHEMA:memory.execute(sql)
            return cls._inventory(memory)
        finally:memory.close()

    def _schema(self,db):
        if self._inventory(db)!=self._expected_schema:raise NativeStageHeld('dedicated_native_stage_schema_required')

    def _intent(self,intent):
        try:
            intent=detached(intent);parent=intent['parentBinding'];p._uuid(parent['jobId'],'job')
            with closing(sqlite3.connect(self.journal.path.as_uri()+'?mode=ro',uri=True)) as db:
                row=db.execute('SELECT state,intent FROM pipeline_stages WHERE job=? AND stage=\'odm\'',(parent['jobId'],)).fetchone()
            if not row or row[0]!='issued' or row[1]!=encoded(intent) or intent['stage']['stageId']!='odm':raise ValueError()
            self.journal._intent(intent,self.journal._read(parent['jobId'])[1],'odm',None)
            return intent
        except Exception:raise NativeStageHeld('actual_issued_odm_intent_required') from None

    def _guard(self,parent,*,mutation=False):
        try:
            if self.guard.check(detached(parent),mutation=mutation) is not None:raise ValueError()
        except Exception:raise NativeStageHeld('native_parent_fence_unavailable') from None

    def _gate(self,gate,parent):
        state=gate.snapshot();held=state['reservation']
        if (state['engineId']!=parent['engineId'] or state['generation']!=parent['gateGeneration']
                or state['nodeId']!=parent['nodeId'] or state['authorityId']!=parent['authorityId'] or state['coordinatorOrigin']!=parent['coordinatorOrigin']
                or not held or held['receiptId']!=parent['gateReceiptId'] or held['completionDigest'] is not None
                or held['job']!=dict(id=parent['jobId'],workerId=parent['workerId'],nodeId=parent['nodeId'],authorityId=parent['authorityId'])):
            raise NativeStageHeld('exact_parent_gate_receipt_required')

    def _runtime(self,row=None):
        actual=self.probe.check();info=self.engine.read('/info',limit=262144)
        if (actual['engineId']!=self.policy.engine_id or actual['imageId']!=self.policy.image_id or actual['composeBaseSha256']!=self.policy.compose_base_sha256
                or type(info) is not dict or info.get('engine')!=self.policy.engine_family or info.get('engineVersion')!=self.policy.engine_version
                or row and actual['containerId']!=row['runtime_container']):raise NativeStageHeld('native_runtime_changed')
        return actual

    def _reservation(self,parent,*,mutation=False):
        with self.ledger._transaction() as db:
            row=self.ledger._reservation(db,parent['reservationId']);settings=json.loads(row['settings'])
        if ((row['job'],row['generation'],row['environment'],settings['manifest_sha256'],settings['input_bytes'],settings['output_budget'])!=
                (parent['jobId'],parent['generation'],parent['environment'],parent['manifestSha256'],parent['inputBytes'],parent['outputBudget'])
                or row['state'] not in ('reserved','engine_started','uncertain') or mutation and row['state']=='uncertain'
                or row['allocated']<parent['inputBytes']):raise NativeStageHeld('whole_parent_scratch_required')
        return row

    def _options(self,intent):
        try:
            catalog=_validate_options(self.engine.read('/options',limit=262144))
            definitions={o['name']:o for o in catalog if p.valid_odm_option_name(o['name'])
                and o.get('type') in ('bool','boolean','int','integer','float','double','number')
                and not _UNSAFE.search(p.canonical_json(o).decode())}
            for name,value in intent['stage']['options']['options'].items():
                kind=definitions[name]['type']
                if not ((kind in ('bool','boolean') and type(value) is bool)
                        or (kind in ('int','integer') and type(value) is int and abs(value)<=p.MAX_SAFE_INTEGER)
                        or (kind in ('float','double','number') and type(value) in (int,float) and abs(value)<=p.MAX_SAFE_INTEGER and math.isfinite(value))):raise ValueError()
        except Exception:raise NativeStageHeld('current_native_options_required') from None

    def _admit(self,job):
        info=self.engine.read('/info',limit=262144);tasks=self.engine.read('/task/list')
        if (type(info) is not dict or type(info.get('taskQueueCount')) is not int or info['taskQueueCount']!=0
                or type(tasks) is not list or len(tasks)>self.policy.max_census_tasks):raise NativeStageHeld('native_engine_not_idle')
        seen=set()
        for item in tasks:
            if type(item) is not dict:raise NativeStageHeld('native_task_census_unavailable')
            identifier=item.get('uuid');p._uuid(identifier,'native task UUID')
            if identifier in seen or identifier==job:raise NativeStageHeld('existing_native_task_requires_reconciliation')
            seen.add(identifier);current=self.engine.read('/task/'+identifier+'/info')
            status=current.get('status') if type(current) is dict else None
            if (type(current) is not dict or current.get('uuid')!=identifier or type(status) is not dict
                    or type(status.get('code')) is not int or status['code'] not in (30,40,50)):
                raise NativeStageHeld('native_engine_has_active_or_uncertain_work')
        if self.engine.read('/task/'+job+'/info',missing=True) is not None:raise NativeStageHeld('existing_native_task_requires_reconciliation')

    def _row(self,job):
        with self._db(readonly=True) as db:row=db.execute('SELECT * FROM native_stage_executions WHERE job=?',(job,)).fetchone()
        if row is None:raise NativeStageHeld('unknown_native_stage_attempt')
        row=dict(row)
        if row['phase'] not in PHASES or (row['phase']=='terminal_proof')!=(row['proof'] is not None):raise NativeStageHeld('native_stage_phase_changed')
        parent=json.loads(row['intent'])['parentBinding'];base=row['allocation_base']
        if type(base) is not int or not parent['inputBytes']<=base<=parent['inputBytes']+parent['outputBudget']:
            raise NativeStageHeld('native_allocation_baseline_changed')
        return row

    def _phase(self,job,phase,**values):
        if phase not in PHASES or set(values)-{'archive_size','archive_sha','proof'}:raise NativeStageHeld('closed_native_checkpoint_required')
        with self._db() as db:db.execute('UPDATE native_stage_executions SET '+','.join(['phase=?']+[key+'=?' for key in values])+' WHERE job=?',(phase,*values.values(),job))

    def _cache(self,job,device,inode,expected):
        with self._db() as db:db.execute("UPDATE native_stage_executions SET phase='archive_pending',part_dev=?,part_ino=?,archive_expected_size=? WHERE job=?",(device,inode,expected,job))

    def _allocated(self,parent,size):
        current=self._reservation(parent)
        # Freeze all earlier retained allocations before this attempt writes.
        # Using inputBytes here would let archive growth consume control bytes.
        baseline=self._row(parent['jobId'])['allocation_base']
        if current['allocated']<baseline:raise NativeStageHeld('native_allocation_baseline_changed')
        self.ledger.record_allocated(parent['reservationId'],max(current['allocated'],baseline+size))

    def _paths(self,parent):
        base='jobs/'+parent['reservationId']+'/odm-stage'
        return base+'/all.zip',base+'/.archive.part'

    def start(self,intent):
        with self._operation():
            intent=self._intent(intent);parent=intent['parentBinding'];job=parent['jobId']
            self._guard(parent,mutation=True);self._reservation(parent,mutation=True);runtime=self._runtime()
            try:old=self._row(job)
            except NativeStageHeld as error:
                if str(error)!='unknown_native_stage_attempt':raise
            else:
                if old['intent']!=encoded(intent):raise NativeStageHeld('native_attempt_rebound')
                raise NativeStageHeld('existing_native_attempt_requires_tick')
            self._options(intent)
            self._admit(job)
            paths=self.inputs.resolve(detached(intent))
            objects=intent['stage']['inputObjects']
            if (type(paths) is not dict or set(paths)!={obj['id'] for obj in objects} or any(obj['kind']!='image' for obj in objects)):
                raise NativeStageHeld('exact_original_image_paths_required')
            inputs=[]
            for obj in objects:
                path,_,_=archive.owned_file(self.ledger,parent['reservationId'],paths[obj['id']],obj['byteSize'],obj['sha256'])
                inputs.append(dict(path=str(path),bytes=obj['byteSize'],sha256=obj['sha256'],name=obj['name']))
            relative,partial=self._paths(parent)
            for path in (relative,partial):self.ledger._owned_path(parent['reservationId'],path)
            directory=(self.ledger.root/relative).parent
            source=directory.parent
            if (not source.is_dir() or source.resolve()!=source or source.stat().st_uid!=os.geteuid() or source.stat().st_mode&0o077
                    or os.path.lexists(directory) or set(paths.values())&{relative,partial}):raise NativeStageHeld('durably_provisioned_parent_scratch_required')
            # Resolver, original-file hashing and admission reads may be slow.
            # Refresh the live parent outside the gate because a concrete guard
            # may itself acquire that gate; then recheck local ownership/runtime
            # under the physical OS lease before any engine mutation.
            self._check();self._guard(parent,mutation=True);self._check()
            with NodeGate(self.policy.engine_id) as gate:
                self._check();self._gate(gate,parent);self._reservation(parent,mutation=True)
                if self._runtime()!=runtime:raise NativeStageHeld('native_runtime_changed_during_admission')
                baseline=self._reservation(parent,mutation=True)['allocated']
                with self._db() as db:db.execute("INSERT INTO native_stage_executions VALUES(?,?,?,?,?,'prepared',NULL,NULL,NULL,NULL,NULL,NULL,?)",(job,intent['attemptId'],encoded(intent),encoded(paths),runtime['containerId'],baseline))
                directory.mkdir(mode=0o700);self._sync(directory);self._sync(source)
                self.ledger.set_engine_state(parent['reservationId'],'engine_started')
                try:
                    self._phase(job,'initialize_pending')
                    self._check();self._gate(gate,parent);self._reservation(parent,mutation=True)
                    self._options(intent);self._runtime(self._row(job))
                    self._check();self._gate(gate,parent);self._reservation(parent,mutation=True)
                    self.engine.initialize(dict(name=job,engine_uuid=job,outputs=None,options=[dict(name=k,value=v) for k,v in intent['stage']['options']['options'].items()]))
                    for item in inputs:
                        self._phase(job,'upload_pending');self._check();self._gate(gate,parent);self.engine.upload(job,item)
                    self._phase(job,'commit_pending');self._check();self._gate(gate,parent);self.engine.action('commit',job)
                    self._phase(job,'processing')
                except Exception:raise NativeStageHeld('native_mutation_outcome_unknown') from None
            return None

    def _seal(self,row,intent,*,outcome,objects,error_code):
        parent=intent['parentBinding'];stage=intent['stage']
        proof=seal_stage_proof(dict(schemaVersion=1,planSha256=intent['planSha256'],intentSha256=intent['intentSha256'],jobId=parent['jobId'],generation=parent['generation'],environment=parent['environment'],inputManifestSha256=parent['manifestSha256'],pipelineDigest=parent['pipelineDigest'],stageId='odm',engineVersion=stage['engineVersion'],runtimeSha256=stage['runtimeSha256'],inputObjectIds=[o['id'] for o in stage['inputObjects']],engineTerminal=True,outcome=outcome,objects=objects,errorCode=error_code))
        checked=validate_stage_proof(proof,intent=intent,policy=self.journal.policy)
        self._phase(row['job'],'terminal_proof',proof=encoded(checked));return checked

    def tick(self,job):
        with self._operation():
            row=self._row(job);intent=json.loads(row['intent']);parent=intent['parentBinding']
            self._guard(parent);self._reservation(parent)
            with NodeGate(self.policy.engine_id) as gate:
                self._gate(gate,parent);self._runtime(row)
                if row['proof']:return validate_stage_proof(json.loads(row['proof']),intent=intent,policy=self.journal.policy)
                if row['phase'] not in ('commit_pending','processing','archive_open_pending','archive_pending','archive_ready'):
                    raise NativeStageHeld('native_precommit_outcome_requires_reconciliation')
                if row['phase']!='archive_ready':
                    info=self.engine.read('/task/'+job+'/info')
                    if type(info) is not dict or info.get('uuid')!=job or type(info.get('status')) is not dict:raise NativeStageHeld('exact_native_terminal_status_required')
                    code=info['status'].get('code')
                    if type(code) is not int or code not in (10,20,30,40,50):raise NativeStageHeld('exact_native_terminal_status_required')
                    if code in (10,20):return None
                    if code in (30,50):return self._seal(row,intent,outcome='failed' if code==30 else 'cancelled',objects=[],error_code='native_odm_failed' if code==30 else 'native_odm_cancelled')
                relative,partial=self._paths(parent);target=self.ledger.root/relative;part=self.ledger.root/partial
                budget=intent['stage']['outputBudget'];reservation=parent['reservationId']
                try:
                    if row['phase']!='archive_ready':
                        if target.exists():raise NativeStageHeld('unbound_native_archive_requires_reconciliation')
                        size,sha=archive.download_archive(self.engine,row,reservation,part,budget,relative=partial,
                            file=lambda *args:archive.owned_file(self.ledger,*args),phase=self._phase,checkpoint=self._cache,read_row=self._row,allocated=lambda n:self._allocated(parent,n),deadline_seconds=self.policy.archive_deadline_seconds)
                        self._phase(job,'archive_ready',archive_size=size,archive_sha=sha);row=self._row(job)
                    if part.exists() and target.exists():
                        if not os.path.samefile(part,target):raise NativeStageHeld('native_archive_publication_changed')
                        part.unlink()
                    elif not target.exists():
                        archive.owned_file(self.ledger,reservation,partial,row['archive_size'],row['archive_sha'])
                        os.link(part,target,follow_symlinks=False);part.unlink()
                    self._sync(target.parent)
                    _,size,sha=archive.owned_file(self.ledger,reservation,relative,row['archive_size'],row['archive_sha'])
                    if not size or size>budget or not zipfile.is_zipfile(target):raise NativeStageHeld('invalid_native_archive')
                    self._allocated(parent,size)
                    return self._seal(row,intent,outcome='succeeded',objects=[dict(id='odm-archive',role='odm_archive',byteSize=size,sha256=sha)],error_code=None)
                except NativeStageHeld:raise
                except Exception:raise NativeStageHeld('native_archive_or_proof_reconciliation_required') from None

    def lookup(self,intent):
        """Only SQL reads plus ownership checks; no engine/probe/ledger mutation."""
        with self._operation():
            try:row=self._row(intent['parentBinding']['jobId'])
            except NativeStageHeld as error:
                if str(error)=='unknown_native_stage_attempt':return None
                raise
            if row['intent']!=encoded(intent):raise NativeStageHeld('native_attempt_rebound')
            return validate_stage_proof(json.loads(row['proof']),intent=intent,policy=self.journal.policy) if row['proof'] else None

    def verify(self,intent,proof):
        with self._operation():
            row=self._row(intent['parentBinding']['jobId']);parent=intent['parentBinding']
            if row['intent']!=encoded(intent) or row['proof']!=encoded(proof):raise NativeStageHeld('exact_sealed_native_proof_required')
            checked=validate_stage_proof(proof,intent=intent,policy=self.journal.policy)
            self._guard(parent);self._reservation(parent)
            with NodeGate(self.policy.engine_id) as gate:
                self._gate(gate,parent);self._runtime(row)
                terminal=self.engine.read('/task/'+parent['jobId']+'/info')
                expected={'succeeded':40,'failed':30,'cancelled':50}[checked['outcome']]
                status=terminal.get('status') if type(terminal) is dict else None
                if (type(terminal) is not dict or terminal.get('uuid')!=parent['jobId'] or type(status) is not dict
                        or type(status.get('code')) is not int or status['code']!=expected):raise NativeStageHeld('native_terminal_proof_changed')
                if checked['outcome']=='succeeded':
                    obj=checked['objects'][0];relative,_=self._paths(parent)
                    archive.owned_file(self.ledger,parent['reservationId'],relative,obj['byteSize'],obj['sha256'])

    def artifact_paths(self,intent):
        with self._operation():
            row=self._row(intent['parentBinding']['jobId'])
            if row['intent']!=encoded(intent) or not row['proof']:raise NativeStageHeld('sealed_native_artifact_required')
            proof=validate_stage_proof(json.loads(row['proof']),intent=intent,policy=self.journal.policy)
            return {'odm-archive':self._paths(intent['parentBinding'])[0]} if proof['outcome']=='succeeded' else {}
