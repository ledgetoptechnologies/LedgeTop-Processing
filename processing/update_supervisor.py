"""Source-only single-host update state machine; no loop or installed adapters.

Every mutating external call has a FULL committed intent first. A lost response
permits only exact read-only reconciliation, never retrying qualification,
scheduling, replacement, rollback, or a registry pull. Adapters are trusted local
composition dependencies, not network-submitted code or another coordinator.
"""
from contextlib import contextmanager
from dataclasses import asdict,dataclass
import os
from pathlib import Path
import re
import sqlite3
import stat
import threading
from typing import Protocol
import uuid

from .update_registry import DockerRegistry,RegistryError,_json,_sha,_load,_digest,_integer


class SupervisorError(ValueError):pass
class SupervisorHeld(SupervisorError):pass


def _hash(value):
    if type(value) is not str or not re.fullmatch(r'[a-f0-9]{64}',value):raise SupervisorError('invalid_binding_digest')


def _slug(value):
    if type(value) is not str or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,79}',value):raise SupervisorError('invalid_fixed_identity')


def _uuid(value):
    try:
        if type(value) is not str or str(uuid.UUID(value))!=value:raise ValueError()
    except Exception:raise SupervisorError('invalid_authority_or_plan') from None


@dataclass(frozen=True)
class SupervisorPolicy:
    node_id:str
    channel:str
    current_image_id:str
    authority_id:str
    qualifier_id:str
    qualification_policy_sha256:str
    maintenance_adapter_id:str
    capacity_bytes:int

    def __post_init__(self):
        _slug(self.node_id)
        for value in (self.channel,self.qualifier_id,self.maintenance_adapter_id):_slug(value)
        _digest(self.current_image_id);_uuid(self.authority_id);_hash(self.qualification_policy_sha256)
        if _integer(self.capacity_bytes)<1:raise SupervisorError('positive_capacity_required')


class Qualifier(Protocol):
    binding:dict
    def qualify(self,request:dict)->dict:...
    def lookup(self,request:dict)->dict|None:...


class MaintenanceAdapter(Protocol):
    binding:dict
    def inspect_host(self)->dict:...
    def schedule(self,request:dict)->dict:...
    def lookup_schedule(self,request:dict)->dict|None:...
    def ready_to_run(self,request:dict)->bool:...
    def run(self,request:dict)->dict:...
    def lookup_completion(self,request:dict)->dict|None:...


def seal_receipt(value,field):
    """Adapter helper: canonical SHA256 of all receipt fields except itself."""
    if not isinstance(value,dict) or field in value:raise SupervisorError('unsealed_receipt_required')
    return dict(value,**{field:_sha(value)})


class UpdateSupervisor:
    def __init__(self,path,policy,*,registry,qualifier,maintenance):
        if type(policy) is not SupervisorPolicy or not isinstance(registry,DockerRegistry):
            raise SupervisorError('fixed_policy_and_actual_registry_required')
        if (policy.channel not in registry.channels or policy.capacity_bytes!=registry.policy.staging_reservation_bytes):
            raise SupervisorError('registry_channel_or_capacity_changed')
        self.path=Path(path).absolute();self.policy=policy
        self.registry,self.qualifier,self.maintenance=registry,qualifier,maintenance
        self.engine_id=registry.policy.engine_id
        self.policy_sha256=_sha(dict(policy=asdict(policy),registryPolicy=asdict(registry.policy),registryPath=str(registry.path)))
        self._pid=os.getpid();self._lease=None;self._lease_identity=None;self._root_identity=None
        self._mutex=threading.RLock()
        self._busy=False
        self._bindings()

    def _bindings(self):
        expected=dict(qualifierId=self.policy.qualifier_id,policySha256=self.policy.qualification_policy_sha256)
        adapter=dict(adapterId=self.policy.maintenance_adapter_id,authorityId=self.policy.authority_id,
                     nodeId=self.policy.node_id,engineId=self.engine_id)
        if (getattr(self.qualifier,'binding',None)!=expected or getattr(self.maintenance,'binding',None)!=adapter
                or any(not callable(getattr(self.qualifier,name,None)) for name in ('qualify','lookup'))
                or any(not callable(getattr(self.maintenance,name,None)) for name in
                       ('inspect_host','schedule','lookup_schedule','ready_to_run','run','lookup_completion'))):
            raise SupervisorError('fixed_adapter_binding_required')

    @property
    def lock_path(self):return self.path.with_name(self.path.name+'.lock')

    def __enter__(self):
        if self._pid!=os.getpid():raise SupervisorError('supervisor_process_changed')
        with self._mutex:
            if self._lease is not None:raise SupervisorError('supervisor_already_entered')
            self.path.parent.mkdir(parents=True,mode=0o700,exist_ok=True)
            if any(part.is_symlink() for part in (self.path,*self.path.parents,self.lock_path)):
                raise SupervisorError('private_supervisor_state_required')
            root=self.path.parent.stat()
            if os.name!='nt' and (root.st_uid!=os.geteuid() or root.st_mode & 0o077):
                raise SupervisorError('private_supervisor_state_required')
            fd=os.open(self.lock_path,os.O_CREAT|os.O_RDWR|getattr(os,'O_NOFOLLOW',0),0o600)
            try:
                info,current=os.fstat(fd),self.lock_path.lstat()
                if (not stat.S_ISREG(info.st_mode) or info.st_nlink!=1
                        or (info.st_dev,info.st_ino)!=(current.st_dev,current.st_ino)
                        or (os.name!='nt' and (info.st_uid!=os.geteuid() or info.st_mode & 0o077))):
                    raise SupervisorError('private_supervisor_lease_required')
                if os.name=='nt':
                    import msvcrt
                    if info.st_size==0:os.write(fd,b'0');os.fsync(fd)
                    os.lseek(fd,0,os.SEEK_SET);msvcrt.locking(fd,msvcrt.LK_NBLCK,1)
                else:
                    import fcntl
                    fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
                self._lease=fd;fd=None
                self._lease_identity=(info.st_dev,info.st_ino);self._root_identity=(root.st_dev,root.st_ino)
                self._sync_directory(self.path.parent)
                self._init()
                self.check()
                return self
            except Exception:
                if self._lease is not None:os.close(self._lease);self._lease=None
                raise SupervisorError('supervisor_lease_or_binding_unavailable') from None
            finally:
                if fd is not None:os.close(fd)

    @staticmethod
    def _sync_directory(path):
        if os.name!='nt':
            fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
            try:os.fsync(fd)
            finally:os.close(fd)

    def check(self):
        # Fork children must not acquire the inherited mutex or unlock the lease.
        if self._pid!=os.getpid() or self._lease is None:raise SupervisorError('supervisor_owner_unavailable')
        root=self.path.parent.stat();info,current=os.fstat(self._lease),self.lock_path.lstat()
        if (self.path.parent.resolve()!=self.path.parent or self.lock_path.is_symlink()
                or (root.st_dev,root.st_ino)!=self._root_identity
                or not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or current.st_nlink!=1
                or (info.st_dev,info.st_ino)!=self._lease_identity or (current.st_dev,current.st_ino)!=self._lease_identity
                or os.get_inheritable(self._lease)
                or (os.name!='nt' and (root.st_uid!=os.geteuid() or root.st_mode & 0o077
                    or info.st_uid!=os.geteuid() or info.st_mode & 0o077))):
            raise SupervisorError('supervisor_lease_identity_changed')
        self._bindings()

    def close(self):
        self.check()
        with self._mutex:
            if self._busy:raise SupervisorHeld('supervisor_operation_in_progress')
            self.check();os.close(self._lease);self._lease=None

    def __exit__(self,*_):self.close()

    @contextmanager
    def _operation(self):
        self.check()
        with self._mutex:
            if self._busy:raise SupervisorHeld('supervisor_operation_in_progress')
            self._busy=True
            try:yield
            finally:self._busy=False

    def _init(self):
        fd=os.open(self.path,os.O_CREAT|os.O_RDWR|getattr(os,'O_NOFOLLOW',0),0o600);os.close(fd)
        with self._db() as db:
            for sql in (
                'CREATE TABLE IF NOT EXISTS supervisor_binding (id INTEGER PRIMARY KEY CHECK(id=1),value TEXT NOT NULL)',
                'CREATE TABLE IF NOT EXISTS supervisor_current (id INTEGER PRIMARY KEY CHECK(id=1),image TEXT NOT NULL,generation INTEGER NOT NULL)',
                '''CREATE TABLE IF NOT EXISTS supervisor_operations (
                    id TEXT PRIMARY KEY,node TEXT NOT NULL,target TEXT NOT NULL UNIQUE,state TEXT NOT NULL,
                    candidate TEXT NOT NULL,base_image TEXT NOT NULL,generation INTEGER NOT NULL,
                    staged TEXT,qualification_request TEXT,qualification_receipt TEXT,
                    schedule_request TEXT,plan TEXT,run_request TEXT,completion TEXT)''',
                "CREATE UNIQUE INDEX IF NOT EXISTS one_open_update ON supervisor_operations(node) WHERE state!='completed'"):
                db.execute(sql)
            columns={row['name'] for row in db.execute('PRAGMA table_info(supervisor_operations)')}
            for name in ('drain_request','drain_receipt'):
                if name not in columns:db.execute('ALTER TABLE supervisor_operations ADD COLUMN '+name+' TEXT')
            binding=_json(dict(policySha256=self.policy_sha256,lockPath=str(self.lock_path),lockIdentity=self._lease_identity,
                rootPath=str(self.path.parent),rootIdentity=self._root_identity))
            old=db.execute('SELECT value FROM supervisor_binding WHERE id=1').fetchone()
            if old and old[0]!=binding:raise SupervisorError('supervisor_binding_changed')
            if not old and db.execute('SELECT 1 FROM supervisor_operations').fetchone():raise SupervisorError('unbound_supervisor_operations')
            db.execute('INSERT OR IGNORE INTO supervisor_binding VALUES(1,?)',(binding,))
            db.execute('INSERT OR IGNORE INTO supervisor_current VALUES(1,?,0)',(self.policy.current_image_id,))
        self._sync_directory(self.path.parent)

    @contextmanager
    def _db(self):
        self.check()
        info=self.path.lstat()
        if (self.path.is_symlink() or not stat.S_ISREG(info.st_mode) or info.st_nlink!=1
                or (os.name!='nt' and (info.st_uid!=os.geteuid() or info.st_mode & 0o077))):
            raise SupervisorError('private_supervisor_journal_required')
        db=sqlite3.connect(self.path,timeout=10);db.row_factory=sqlite3.Row;db.execute('PRAGMA synchronous=FULL')
        try:
            db.execute('BEGIN IMMEDIATE');yield db;self.check();self._commit(db)
        except BaseException:db.rollback();raise
        finally:db.close()

    def _commit(self,db):db.commit()

    def _row(self,identifier):
        with self._db() as db:row=db.execute('SELECT * FROM supervisor_operations WHERE id=?',(identifier,)).fetchone()
        if row is None:raise SupervisorError('unknown_update_operation')
        row=dict(row);self._validate_row(row)
        return row

    def _validate_row(self,row):
        """Continuation never trusts a state label without its sealed evidence."""
        try:
            states=('discovered','staging_intent','staged','qualification_intent','qualified',
                    'schedule_intent','scheduled','run_intent','completed')
            rank=0 if row['state'] in ('drain_intent','draining') else states.index(row['state']);candidate=_load(row['candidate'])
            self.registry._candidate(candidate)
            target=_sha(dict(nodeId=self.policy.node_id,engineId=self.engine_id,repository=candidate['repository'],
                manifestDigest=candidate['manifestDigest'],configDigest=candidate['configDigest'],
                previousImageId=row['base_image'],hostGeneration=row['generation']))
            _digest(row['base_image']);_integer(row['generation'])
            if (row['target']!=target or row['node']!=self.policy.node_id
                    or row['id']!=_sha(dict(target=target,policySha256=self.policy_sha256))):raise ValueError()
            if row['drain_request'] is not None:
                request=_load(row['drain_request'])
                self._validate_request(row,candidate,request,'drain',row['base_image'])
                if row['state']!='drain_intent':self._receipt(_load(row['drain_receipt']),request,'drain')
            elif row['state'] in ('drain_intent','draining'):raise ValueError()
            if rank>=2:
                staged=_load(row['staged'])
                expected=dict(candidateId=candidate['candidateId'],repository=candidate['repository'],manifestDigest=candidate['manifestDigest'],
                    configDigest=candidate['configDigest'],platform='linux/amd64',engineId=self.engine_id)
                if set(staged)!=set(expected)|{'localSize','imageId','imageIdKind'} or any(staged[key]!=value for key,value in expected.items()):raise ValueError()
                if _integer(staged['localSize'])>self.policy.capacity_bytes:raise ValueError()
                _digest(staged['imageId'])
                if row['drain_request'] is not None and staged['imageId']!=candidate['configDigest']:raise ValueError()
                if (staged['imageIdKind'] not in ('config','manifest')
                        or staged['imageId']!=candidate[{'config':'configDigest','manifest':'manifestDigest'}[staged['imageIdKind']]]):raise ValueError()
            previous=row['base_image']
            for kind,minimum in (('qualification',3),('schedule',5),('run',7)):
                if rank<minimum:continue
                request=_load(row[{'qualification':'qualification_request','schedule':'schedule_request','run':'run_request'}[kind]])
                self._validate_request(row,candidate,request,kind,previous)
                previous=request['previousImageId']
                if rank>minimum:
                    receipt=_load(row[{'qualification':'qualification_receipt','schedule':'plan','run':'completion'}[kind]])
                    self._receipt(receipt,request,kind)
        except Exception:raise SupervisorHeld('durable_update_evidence_changed') from None

    def _validate_request(self,row,candidate,request,kind,previous):
        expected=dict(schemaVersion=1,operationId=row['id'],nodeId=self.policy.node_id,engineId=self.engine_id,
            authorityId=self.policy.authority_id,candidateId=candidate['candidateId'],repository=candidate['repository'],
            manifestDigest=candidate['manifestDigest'],configDigest=candidate['configDigest'],localImageId=candidate['configDigest'] if kind=='drain' else _load(row['staged'])['imageId'],platform='linux/amd64',
            previousImageId=request.get('previousImageId'),supervisorPolicySha256=self.policy_sha256,
            hostGeneration=row['generation'],
            qualifierId=self.policy.qualifier_id,qualificationPolicySha256=self.policy.qualification_policy_sha256,
            receiptDirectory=str(self.path.parent/'receipts'/row['id']/kind))
        _digest(expected['previousImageId'])
        if previous is not None and previous!=expected['previousImageId']:raise ValueError()
        if kind not in ('qualification','drain'):
            qualification=_load(row['qualification_receipt'])
            expected['qualificationSha256']=qualification['qualificationSha256']
            # Exact old records remain readable. They cannot advance/reconcile
            # an unbound schedule or run into new execution authority.
            if 'targetEngineVersion' in request:
                expected['targetEngineVersion']=qualification['engineVersion']
        if kind=='run':expected['plan']=_load(row['plan'])
        expected['requestSha256']=_sha(expected)
        if request!=expected or type(request['schemaVersion']) is not int:raise ValueError()

    def _checkpoint(self,identifier,expected,state,**values):
        allowed={'staged','qualification_request','qualification_receipt','schedule_request','plan','run_request','completion','drain_request','drain_receipt'}
        if set(values)-allowed:raise SupervisorError('invalid_checkpoint')
        with self._db() as db:
            row=db.execute('SELECT state FROM supervisor_operations WHERE id=?',(identifier,)).fetchone()
            if row is None or row[0]!=expected:raise SupervisorHeld('update_state_changed')
            assignments=['state=?']+[key+'=?' for key in values]
            db.execute('UPDATE supervisor_operations SET '+','.join(assignments)+' WHERE id=?',
                (state,*(_json(value) for value in values.values()),identifier))

    def _current(self):
        return self._current_state()[0]

    def _current_state(self):
        with self._db() as db:
            row=db.execute('SELECT image,generation FROM supervisor_current WHERE id=1').fetchone()
            _digest(row['image']);_integer(row['generation'])
            return row['image'],row['generation']

    def _host(self,expected):
        try:host=self.maintenance.inspect_host()
        except Exception:raise SupervisorHeld('host_identity_unavailable') from None
        wanted=dict(nodeId=self.policy.node_id,authorityId=self.policy.authority_id,engineId=self.engine_id,imageId=expected)
        if host!=wanted:raise SupervisorHeld('host_identity_or_image_changed')

    def _directory(self,identifier,kind):
        root=self.path.parent
        for part in ('receipts',identifier,kind):
            directory=root/part
            if directory.is_symlink():raise SupervisorError('owned_receipt_directory_required')
            if not directory.exists():directory.mkdir(mode=0o700);self._sync_directory(root)
            info=directory.stat()
            if not stat.S_ISDIR(info.st_mode) or (os.name!='nt' and (info.st_uid!=os.geteuid() or info.st_mode & 0o077)):
                raise SupervisorError('owned_receipt_directory_required')
            self._sync_directory(directory)
            root=directory
        self._sync_directory(self.path.parent)
        return str(root)

    def status(self,identifier):
        self.check()
        with self._mutex:
            row=self._row(identifier)
            result=dict(operationId=identifier,nodeId=row['node'],state=row['state'],candidate=_load(row['candidate']),
                plan=_load(row['plan']) if row['plan'] else None,completion=_load(row['completion']) if row['completion'] else None)
            if row['drain_receipt'] is not None:result['drain']=_load(row['drain_receipt'])
            return result

    @property
    def early_drain_enabled(self):
        value=getattr(self.maintenance,'early_drain_enabled',False)
        if type(value) is not bool:raise SupervisorHeld('explicit_drain_policy_required')
        if value and any(not callable(getattr(self.maintenance,name,None)) for name in ('prepare_drain','lookup_drain')):
            raise SupervisorHeld('owned_drain_adapter_required')
        return value

    def pending(self):
        """Read one validated unresolved operation under the existing owner lease."""
        self.check()
        with self._mutex:
            with self._db() as db:
                rows=db.execute("SELECT id FROM supervisor_operations WHERE state!='completed' LIMIT 2").fetchall()
            if len(rows)>1:raise SupervisorHeld('ambiguous_pending_update_operations')
            return self.status(rows[0]['id']) if rows else None

    def discover(self):
        with self._operation():
            with self._db() as db:
                if db.execute("SELECT 1 FROM supervisor_operations WHERE state!='completed'").fetchone():
                    raise SupervisorHeld('prior_update_requires_reconciliation')
            current,generation=self._current_state();self._host(current)
            candidate=self.registry.discover(self.policy.channel)
            try:matches=self.registry.matches_local_image(candidate,current)
            except Exception:raise SupervisorHeld('cached_local_image_identity_unavailable') from None
            if type(matches) is not bool:raise SupervisorHeld('explicit_local_image_match_required')
            if matches:return None
            self._host(current)
            target=_sha(dict(nodeId=self.policy.node_id,engineId=self.engine_id,repository=candidate['repository'],
                manifestDigest=candidate['manifestDigest'],configDigest=candidate['configDigest'],
                previousImageId=current,hostGeneration=generation))
            identifier=_sha(dict(target=target,policySha256=self.policy_sha256))
            with self._db() as db:
                old=db.execute('SELECT id FROM supervisor_operations WHERE target=?',(target,)).fetchone()
                if old:identifier=old[0]
                else:db.execute("INSERT INTO supervisor_operations(id,node,target,state,candidate,base_image,generation) VALUES(?,?,?,'discovered',?,?,?)",
                    (identifier,self.policy.node_id,target,_json(candidate),current,generation))
            return self.status(identifier)

    def _request(self,row,kind):
        candidate=_load(row['candidate'])
        request=dict(schemaVersion=1,operationId=row['id'],nodeId=self.policy.node_id,engineId=self.engine_id,
            authorityId=self.policy.authority_id,candidateId=candidate['candidateId'],repository=candidate['repository'],
            manifestDigest=candidate['manifestDigest'],configDigest=candidate['configDigest'],localImageId=candidate['configDigest'] if kind=='drain' else _load(row['staged'])['imageId'],platform=candidate['platform'],
            previousImageId=row['base_image'],hostGeneration=row['generation'],supervisorPolicySha256=self.policy_sha256,
            qualifierId=self.policy.qualifier_id,qualificationPolicySha256=self.policy.qualification_policy_sha256,
            receiptDirectory=self._directory(row['id'],kind))
        if kind not in ('qualification','drain'):
            qualification=_load(row['qualification_receipt'])
            request['qualificationSha256']=qualification['qualificationSha256']
            request['targetEngineVersion']=qualification['engineVersion']
        if kind=='run':request['plan']=_load(row['plan'])
        request['requestSha256']=_sha(request)
        return request

    @staticmethod
    def _receipt(value,request,kind):
        common=('operationId','nodeId','engineId','authorityId','candidateId','manifestDigest','configDigest','localImageId','supervisorPolicySha256','hostGeneration')
        base={'schemaVersion','requestSha256',*common}
        extra={'drain':{'planId','previousImageId','targetImageId','drainRequestId','drainRequestSha256','drainAt','notBefore','afterJobId','drainSha256'},
            'qualification':{'qualifierId','qualificationPolicySha256','passed','engineVersion','evidenceSha256','qualificationSha256'},
            'schedule':{'planId','previousImageId','targetImageId','qualificationSha256','planSha256'},
            'run':{'planId','planSha256','previousImageId','targetImageId','qualificationSha256','imageId','healthy',
                   'hostPinDurable','ingressOpen','coordinatorFinished','completionSha256'}}[kind]
        if kind!='qualification' and 'targetEngineVersion' in request:
            extra=extra|({'targetEngineVersion'} if kind=='schedule' else {'engineVersion'})
        field={'drain':'drainSha256','qualification':'qualificationSha256','schedule':'planSha256','run':'completionSha256'}[kind]
        try:
            if type(value) is not dict or set(value)!=base|extra:raise ValueError()
            _digest(request['localImageId'])
            value=_load(_json(value))
            if (type(value['schemaVersion']) is not int or value['schemaVersion']!=1
                    or type(value['hostGeneration']) is not int
                    or value['requestSha256']!=request['requestSha256'] or any(value[key]!=request[key] for key in common)
                    or value[field]!=_sha({key:item for key,item in value.items() if key!=field})):raise ValueError()
            if kind=='qualification':
                if (value['passed'] is not True or value['qualifierId']!=request['qualifierId']
                        or value['qualificationPolicySha256']!=request['qualificationPolicySha256']):raise ValueError()
                _slug(value['engineVersion']);_hash(value['evidenceSha256'])
            elif kind=='drain':
                _uuid(value['planId']);_uuid(value['drainRequestId']);_hash(value['drainRequestSha256'])
                if value['afterJobId'] is not None:_uuid(value['afterJobId'])
                from .maintenance import utc_stamp
                if utc_stamp(value['notBefore'])[0]<utc_stamp(value['drainAt'])[0]:raise ValueError()
                if value['previousImageId']!=request['previousImageId'] or value['targetImageId']!=request['localImageId']:
                    raise ValueError()
            else:
                _uuid(value['planId'])
                if (value['previousImageId']!=request['previousImageId'] or value['targetImageId']!=request['localImageId']
                        or value['qualificationSha256']!=request['qualificationSha256']):raise ValueError()
                if 'targetEngineVersion' in request:
                    _slug(request['targetEngineVersion'])
                    if value['targetEngineVersion' if kind=='schedule' else 'engineVersion']!=request['targetEngineVersion']:
                        raise ValueError()
                    if kind=='run' and request['plan'].get('targetEngineVersion')!=request['targetEngineVersion']:
                        raise ValueError()
                if kind=='run' and (value['planId']!=request['plan']['planId'] or value['planSha256']!=request['plan']['planSha256']
                        or value['imageId']!=request['localImageId']
                        or any(value[key] is not True for key in ('healthy','hostPinDurable','ingressOpen','coordinatorFinished'))):raise ValueError()
            return value
        except Exception:raise SupervisorHeld('exact_bound_'+kind+'_receipt_required') from None

    def _complete(self,row,receipt):
        request=_load(row['run_request']);checked=self._receipt(receipt,request,'run')
        self._host(checked['imageId'])
        with self._db() as db:
            current=db.execute('SELECT image,generation FROM supervisor_current WHERE id=1').fetchone()
            if (current['image'],current['generation'])!=(request['previousImageId'],request['hostGeneration']):
                raise SupervisorHeld('supervisor_current_image_changed')
            db.execute("UPDATE supervisor_operations SET state='completed',completion=? WHERE id=? AND state IN ('run_intent','run_held')",
                (_json(checked),row['id']))
            if db.execute('SELECT changes()').fetchone()[0]!=1:raise SupervisorHeld('update_state_changed')
            db.execute('UPDATE supervisor_current SET image=?,generation=generation+1 WHERE id=1',(checked['imageId'],))

    def advance(self,identifier):
        """Attempt exactly one next mutation. Intent/held states never replay."""
        with self._operation():
            row=self._row(identifier);state=row['state']
            if state=='completed':return self.status(identifier)
            if state not in ('discovered','draining','staged','qualified','scheduled'):raise SupervisorHeld('update_requires_read_only_reconciliation')
            if state=='scheduled' and 'targetEngineVersion' not in _load(row['plan']):
                raise SupervisorHeld('target_engine_version_unbound')
            if self._current_state()!=(row['base_image'],row['generation']):raise SupervisorHeld('supervisor_current_image_changed')
            self._host(row['base_image'])
            kind='drain' if state=='discovered' and self.early_drain_enabled else {'discovered':'staging','draining':'staging','staged':'qualification','qualified':'schedule','scheduled':'run'}[state]
            if kind=='staging':
                self._checkpoint(identifier,state,'staging_intent')
                try:
                    metadata=self.registry.stage(_load(row['candidate']))
                    self._checkpoint(identifier,'staging_intent','staged',staged=metadata)
                except Exception:
                    raise SupervisorHeld('staging_outcome_requires_reconciliation') from None
            else:
                request=self._request(row,kind)
                if kind=='run':
                    # Future windows and draining jobs are a normal wait, not an
                    # uncertain issued update. This probe cannot issue actions.
                    try:ready=self.maintenance.ready_to_run(_load(_json(request)))
                    except Exception:raise SupervisorHeld('run_readiness_unavailable') from None
                    self.check()
                    if type(ready) is not bool:raise SupervisorHeld('explicit_run_readiness_required')
                    if not ready:return self.status(identifier)
                    self._host(row['base_image'])
                column={'drain':'drain_request','qualification':'qualification_request','schedule':'schedule_request','run':'run_request'}[kind]
                self._checkpoint(identifier,state,kind+'_intent',**{column:request})
                try:
                    self.check()
                    call={'qualification':self.qualifier.qualify,'schedule':self.maintenance.schedule,'run':self.maintenance.run,
                        **({'drain':self.maintenance.prepare_drain} if kind=='drain' else {})}[kind]
                    receipt=self._receipt(call(_load(_json(request))),request,kind)
                    self.check()
                    if kind=='run':self._complete(self._row(identifier),receipt)
                    else:self._checkpoint(identifier,kind+'_intent',{'drain':'draining','qualification':'qualified','schedule':'scheduled'}[kind],
                        **{{'drain':'drain_receipt','qualification':'qualification_receipt','schedule':'plan'}[kind]:receipt})
                except Exception:
                    # The original intent itself is the durable hold. Never
                    # overwrite a possibly committed success after a lost ACK.
                    raise SupervisorHeld(kind+'_outcome_requires_reconciliation') from None
            return self.status(identifier)

    def reconcile(self,identifier):
        """Only registry inspection or adapter read-only receipt lookup."""
        with self._operation():
            row=self._row(identifier);state=row['state']
            if state=='completed':return self.status(identifier)
            if state=='staging_intent':
                try:metadata=self.registry.reconcile(_load(row['candidate']))
                except Exception:raise SupervisorHeld('staging_outcome_requires_reconciliation') from None
                self._checkpoint(identifier,state,'staged',staged=metadata)
            elif state in ('drain_intent','qualification_intent','schedule_intent','run_intent'):
                kind=state.removesuffix('_intent')
                column={'drain':'drain_request','qualification':'qualification_request','schedule':'schedule_request','run':'run_request'}[kind]
                request=_load(row[column])
                if kind not in ('qualification','drain') and 'targetEngineVersion' not in request:
                    raise SupervisorHeld('target_engine_version_unbound')
                try:
                    lookup={'qualification':self.qualifier.lookup,'schedule':self.maintenance.lookup_schedule,
                            'run':self.maintenance.lookup_completion,
                            **({'drain':self.maintenance.lookup_drain} if kind=='drain' else {})}[kind]
                    receipt=self._receipt(lookup(_load(_json(request))),request,kind)
                    self.check()
                    if kind=='run':self._complete(row,receipt)
                    else:
                        self._host(self._current())
                        self._checkpoint(identifier,state,{'drain':'draining','qualification':'qualified','schedule':'scheduled'}[kind],
                            **{{'drain':'drain_receipt','qualification':'qualification_receipt','schedule':'plan'}[kind]:receipt})
                except Exception:raise SupervisorHeld(kind+'_outcome_requires_reconciliation') from None
            else:raise SupervisorHeld('no_uncertain_intent_to_reconcile')
            return self.status(identifier)
