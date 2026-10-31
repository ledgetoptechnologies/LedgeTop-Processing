"""Real, one-shot maintenance bridge for UpdateSupervisor; no installed loop.

Unknown schedule/run calls are never repeated. Lookup methods only read local
evidence and authoritative HTTP/Docker state; they cannot finalize an update.
"""
from contextlib import contextmanager
from dataclasses import asdict,dataclass
from datetime import datetime,timedelta,timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import threading
from types import MappingProxyType
import uuid

from processing.container_update import ComposeNode,loopback,finish_matches
from processing.node_gate import NodeGate,GateBusy
from processing.maintenance_authority import maintenance_authority,MaintenanceAuthorityBusy
from processing.odx_smoke_worker import Client
from processing.update_supervisor import seal_receipt,UpdateSupervisor
from ops.node_ingress import Ingress
from ops import node_maintenance as managed


COMMON=('operationId','nodeId','engineId','authorityId','candidateId','manifestDigest',
        'configDigest','localImageId','supervisorPolicySha256','hostGeneration')
REQUEST={'schemaVersion',*COMMON,'repository','platform','previousImageId','qualifierId',
         'qualificationPolicySha256','receiptDirectory','qualificationSha256','requestSha256'}


class MaintenanceAdapterHeld(ValueError):pass


def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)
def digest(value):return hashlib.sha256(canonical(value).encode()).hexdigest()


def identity(value):
    if type(value) is not str or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,79}',value):raise MaintenanceAdapterHeld('invalid_fixed_identity')


def sha(value,prefix=False):
    if type(value) is not str or not re.fullmatch(('sha256:' if prefix else '')+r'[a-f0-9]{64}',value):raise MaintenanceAdapterHeld('invalid_fixed_digest')


def directory(value,*,create=False):
    path=Path(value).absolute()
    if path.resolve()!=path or any(part.is_symlink() for part in (path,*path.parents)):
        raise MaintenanceAdapterHeld('private_directory_required')
    if create and not path.exists():
        directory(path.parent);path.mkdir(mode=0o700)
    info=path.stat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid!=os.geteuid() or info.st_mode & 0o077:
        raise MaintenanceAdapterHeld('private_directory_required')
    return path


def read_json(path):
    path=Path(path)
    info=path.lstat()
    if (path.is_symlink() or not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_uid!=os.geteuid()
            or info.st_mode & 0o077 or info.st_size>131072):raise MaintenanceAdapterHeld('private_receipt_required')
    return json.loads(path.read_text())


@dataclass(frozen=True)
class ScheduleWindow:
    drain_after_seconds:int
    not_before_after_seconds:int
    wait_seconds:int
    drain_seconds:int

    def __post_init__(self):
        for value in asdict(self).values():
            if type(value) is not int or not 0<=value<=86400:raise ValueError('invalid_schedule_window')
        if self.not_before_after_seconds<self.drain_after_seconds or self.drain_seconds<1:
            raise ValueError('invalid_schedule_window')


class UpdateMaintenanceAdapter:
    def __init__(self,managed_root,*,state_path,receipt_root,api,operator,node_id,authority_id,engine_id,adapter_id,
                 window,clock=lambda:datetime.now(timezone.utc)):
        try:
            with maintenance_authority(engine_id):
                self._initialize(managed_root,state_path=state_path,receipt_root=receipt_root,api=api,operator=operator,
                    node_id=node_id,authority_id=authority_id,engine_id=engine_id,adapter_id=adapter_id,window=window,clock=clock)
        except MaintenanceAuthorityBusy:
            raise MaintenanceAdapterHeld('maintenance_authority_unavailable') from None

    def _initialize(self,managed_root,*,state_path,receipt_root,api,operator,node_id,authority_id,engine_id,adapter_id,
                    window,clock):
        if not sys_linux():raise MaintenanceAdapterHeld('linux_maintenance_adapter_required')
        if type(window) is not ScheduleWindow or not isinstance(api,Client) or not isinstance(operator,Client):
            raise MaintenanceAdapterHeld('fixed_clients_and_window_required')
        if node_id not in ('main','cluster'):raise MaintenanceAdapterHeld('fixed_node_required')
        identity(engine_id);identity(adapter_id)
        if type(authority_id) is not str or str(uuid.UUID(authority_id))!=authority_id:
            raise MaintenanceAdapterHeld('canonical_authority_required')
        if (api.origin!=operator.origin or loopback(api.origin)!=api.origin or api.node or operator.node
                or api.token==operator.token or any(type(client.token) is not str or not 1<=len(client.token)<=4096
                    or not all(0x21<=ord(char)<=0x7e for char in client.token) for client in (api,operator))):
            raise MaintenanceAdapterHeld('distinct_fixed_control_credentials_required')
        self.root=directory(managed_root);self.receipt_root=directory(receipt_root,create=True)
        self.path=Path(state_path).absolute();directory(self.path.parent)
        if self.path.resolve()!=self.path or self.path.is_symlink():raise MaintenanceAdapterHeld('private_adapter_journal_required')
        self.api,self.operator,self.window,self.clock=api,operator,window,clock
        self.binding=MappingProxyType(dict(adapterId=adapter_id,authorityId=authority_id,nodeId=node_id,engineId=engine_id))
        self._clients=(api.origin,operator.origin,digest(api.token),digest(operator.token))
        self._pid=os.getpid();self._mutex=threading.RLock();self._busy=False
        self._config=self._fingerprints()
        descriptor=os.open(self.path,os.O_CREAT|os.O_RDWR|getattr(os,'O_NOFOLLOW',0)|getattr(os,'O_NONBLOCK',0),0o600)
        try:
            info=os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_uid!=os.geteuid() or info.st_mode & 0o077:
                raise MaintenanceAdapterHeld('private_adapter_journal_required')
            self._file_identity=(info.st_dev,info.st_ino)
        finally:os.close(descriptor)
        frozen=dict(schemaVersion=1,binding=dict(self.binding),managedRoot=str(self.root),receiptRoot=str(self.receipt_root),
            statePath=str(self.path),stateIdentity=self._file_identity,coordinator=api.origin,config=self._config,window=asdict(window))
        with self._db(write=True) as db:
            names={row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
            if names-{'adapter_binding','schedule_intents','run_intents'}:raise MaintenanceAdapterHeld('dedicated_adapter_journal_required')
            db.execute('CREATE TABLE IF NOT EXISTS adapter_binding(id INTEGER PRIMARY KEY CHECK(id=1),value TEXT NOT NULL)')
            db.execute('''CREATE TABLE IF NOT EXISTS schedule_intents(request_sha TEXT PRIMARY KEY,operation TEXT NOT NULL,
                generation INTEGER NOT NULL,request TEXT NOT NULL,wire TEXT NOT NULL,receipt TEXT,
                UNIQUE(operation,generation))''')
            db.execute('''CREATE TABLE IF NOT EXISTS run_intents(request_sha TEXT PRIMARY KEY,operation TEXT NOT NULL,
                generation INTEGER NOT NULL,request TEXT NOT NULL,state TEXT NOT NULL,receipt TEXT,
                UNIQUE(operation,generation))''')
            old=db.execute('SELECT value FROM adapter_binding WHERE id=1').fetchone()
            if old and old[0]!=canonical(frozen):raise MaintenanceAdapterHeld('adapter_binding_changed')
            if not old and any(db.execute('SELECT 1 FROM '+table+' LIMIT 1').fetchone() for table in ('schedule_intents','run_intents')):
                raise MaintenanceAdapterHeld('unbound_adapter_intents')
            db.execute('INSERT OR IGNORE INTO adapter_binding VALUES(1,?)',(canonical(frozen),))
        fd=os.open(self.path.parent,os.O_RDONLY|os.O_DIRECTORY)
        try:os.fsync(fd)
        finally:os.close(fd)

    def _fingerprints(self):
        settings=read_json(self.root/'maintenance.json');gateway=read_json(self.root/'ingress'/'gateway.json')
        source=read_json(self.root/'compose.json')
        if (settings['nodeId']!=self.binding['nodeId'] or settings.get('coordinator')!=self.api.origin
                or gateway['engineId']!=self.binding['engineId']):raise MaintenanceAdapterHeld('managed_root_binding_changed')
        return dict(settings=digest(settings),ingress=digest(gateway),composeBase=managed.compose_base(source,settings['service']))

    def _check(self):
        if self._pid!=os.getpid():raise MaintenanceAdapterHeld('adapter_owner_process_changed')
        directory(self.root);directory(self.receipt_root);directory(self.path.parent)
        info=self.path.lstat()
        if (self.path.is_symlink() or not stat.S_ISREG(info.st_mode) or info.st_nlink!=1
                or (info.st_dev,info.st_ino)!=self._file_identity or info.st_uid!=os.geteuid() or info.st_mode & 0o077
                or self._clients!=(self.api.origin,self.operator.origin,digest(self.api.token),digest(self.operator.token))
                or self._config!=self._fingerprints()):raise MaintenanceAdapterHeld('adapter_binding_changed')

    @contextmanager
    def _operation(self):
        try:
            with maintenance_authority(self.binding['engineId']):
                self._check()
                with self._mutex:
                    if self._busy:raise MaintenanceAdapterHeld('adapter_operation_active')
                    self._busy=True
                    try:self._check();yield
                    except MaintenanceAdapterHeld:raise
                    except Exception:raise MaintenanceAdapterHeld('maintenance_evidence_unavailable') from None
                    finally:self._busy=False
        except MaintenanceAuthorityBusy:
            raise MaintenanceAdapterHeld('maintenance_authority_unavailable') from None

    @contextmanager
    def _db(self,*,write=False):
        self._check()
        db=sqlite3.connect(self.path if write else self.path.as_uri()+'?mode=ro',uri=not write,timeout=10)
        db.row_factory=sqlite3.Row
        try:
            if write:db.execute('PRAGMA synchronous=FULL');db.execute('BEGIN IMMEDIATE')
            yield db
            self._check()
            if write:db.commit()
        except BaseException:
            if write:db.rollback()
            raise
        finally:db.close()

    def _request(self,request,kind):
        expected=REQUEST|({'plan'} if kind=='run' else set())
        if type(request) is not dict or set(request)!=expected or request.get('schemaVersion')!=1 or type(request['schemaVersion']) is not int:
            raise MaintenanceAdapterHeld('closed_supervisor_request_required')
        if request['requestSha256']!=digest({key:value for key,value in request.items() if key!='requestSha256'}):
            raise MaintenanceAdapterHeld('supervisor_request_changed')
        for key in ('operationId','candidateId','supervisorPolicySha256','qualificationPolicySha256','qualificationSha256','requestSha256'):sha(request[key])
        for key in ('manifestDigest','configDigest','localImageId','previousImageId'):sha(request[key],True)
        identity(request['qualifierId'])
        if (any(request[key]!=self.binding[key] for key in ('nodeId','authorityId','engineId'))
                or type(request['hostGeneration']) is not int or not 0<=request['hostGeneration']<=2**53-1
                or request['platform']!='linux/amd64' or type(request['repository']) is not str
                or not re.fullmatch(r'[a-z0-9][a-z0-9.:-]*(?:/[a-z0-9][a-z0-9._-]*)+',request['repository'])):
            raise MaintenanceAdapterHeld('supervisor_host_binding_changed')
        expected_dir=self.receipt_root/request['operationId']/kind
        if type(request['receiptDirectory']) is not str or Path(request['receiptDirectory'])!=expected_dir:
            raise MaintenanceAdapterHeld('owned_operation_directory_required')
        directory(expected_dir);directory(expected_dir.parent)
        return json.loads(canonical(request))

    def _context(self,work):
        self._check()
        settings=read_json(self.root/'maintenance.json');ingress=Ingress(self.root/'ingress')
        if digest(ingress.config)!=self._config['ingress']:raise MaintenanceAdapterHeld('ingress_binding_changed')
        args=managed.adapter_args(self.root,settings,ingress)
        return settings,ingress,ComposeNode(args,Path(work))

    def _host(self):
        authority=self.api.json('GET','/v1/prototype/authority')
        if authority!={'authorityId':self.binding['authorityId']}:raise MaintenanceAdapterHeld('coordinator_authority_changed')
        settings,ingress,adapter=self._context(self.path.parent)
        item=adapter.inspect();sha(item['Image'],True)
        backend=json.loads(ingress.docker(['inspect',ingress.config['backendName']]))
        source=read_json(self.root/'compose.json')
        if (len(backend)!=1 or backend[0]['Id']!=item['Id'] or source['services'][settings['service']]['image']!=item['Image']
                or not adapter.serving(item['Image'])):raise MaintenanceAdapterHeld('private_backend_identity_changed')
        ingress.inspect()
        return dict(nodeId=self.binding['nodeId'],authorityId=self.binding['authorityId'],engineId=self.binding['engineId'],imageId=item['Image'])

    def inspect_host(self):
        with self._operation():return self._host()

    def _schedule_row(self,request):
        with self._db() as db:
            row=db.execute('SELECT * FROM schedule_intents WHERE operation=? AND generation=?',
                (request['operationId'],request['hostGeneration'])).fetchone()
        return dict(row) if row else None

    def _plan(self,row,*,current=True):
        request=json.loads(row['request']);wire=json.loads(row['wire'])
        response=self.operator.json('GET','/v1/prototype/nodes/'+self.binding['nodeId']+'/schedule-owned/'+wire['requestId'])
        if type(response) is not dict or set(response)!={'receipt'}:raise MaintenanceAdapterHeld('owned_plan_receipt_required')
        receipt=response['receipt']
        fields={'schemaVersion','requestId','authorityId','nodeId','requestSha256','expectedRevision','revision','planId','state',
                'drainAt','notBefore','afterJobId','targetImage','receiptSha256'}
        if (type(receipt) is not dict or set(receipt)!=fields or type(receipt['schemaVersion']) is not int or receipt['schemaVersion']!=1
                or receipt['receiptSha256']!=digest({key:value for key,value in receipt.items() if key!='receiptSha256'})
                or receipt['requestSha256']!=digest(dict(nodeId=self.binding['nodeId'],request=wire))
                or any(receipt[key]!=wire[key] for key in ('requestId','authorityId','expectedRevision','afterJobId','targetImage'))
                or receipt['nodeId']!=self.binding['nodeId'] or type(receipt['revision']) is not int
                or receipt['revision']!=wire['expectedRevision']+1):raise MaintenanceAdapterHeld('owned_plan_binding_changed')
        for key in ('drainAt','notBefore'):
            if stamp(receipt[key])!=stamp(wire[key]):raise MaintenanceAdapterHeld('owned_plan_window_changed')
        if str(uuid.UUID(receipt['planId']))!=receipt['planId']:raise MaintenanceAdapterHeld('owned_plan_identity_changed')
        self._authority()
        if current:
            node=self.api.json('GET','/v1/prototype/nodes/'+self.binding['nodeId'])['node']
            if (node.get('id')!=self.binding['nodeId'] or node['planId']!=receipt['planId'] or node['targetImage']!=wire['targetImage']
                    or type(node.get('revision')) is not int or node['revision']<receipt['revision']
                    or any(node.get(key)!=receipt[key] for key in ('drainAt','notBefore','afterJobId'))):
                raise MaintenanceAdapterHeld('owned_plan_is_no_longer_current')
        value=dict(schemaVersion=1,requestSha256=request['requestSha256'],**{key:request[key] for key in COMMON},
            planId=receipt['planId'],previousImageId=request['previousImageId'],targetImageId=request['localImageId'],
            qualificationSha256=request['qualificationSha256'])
        value=UpdateSupervisor._receipt(seal_receipt(value,'planSha256'),request,'schedule')
        if row['receipt'] is not None and row['receipt']!=canonical(value):raise MaintenanceAdapterHeld('sealed_plan_changed')
        return value,receipt

    def _authority(self):
        if self.api.json('GET','/v1/prototype/authority')!={'authorityId':self.binding['authorityId']}:
            raise MaintenanceAdapterHeld('coordinator_authority_changed')

    def schedule(self,request):
        with self._operation():
            request=self._request(request,'schedule');row=self._schedule_row(request)
            if row:
                if row['request']!=canonical(request):raise MaintenanceAdapterHeld('schedule_cycle_already_bound')
                return self._plan(row)[0]
            if self._host()['imageId']!=request['previousImageId']:raise MaintenanceAdapterHeld('previous_image_changed')
            _,_,adapter=self._context(request['receiptDirectory']);adapter.image(request['localImageId'])
            node=self.api.json('GET','/v1/prototype/nodes/'+self.binding['nodeId'])['node']
            if node['state']!='active':raise MaintenanceAdapterHeld('node_already_has_maintenance')
            now=aware(self.clock())
            wire=dict(schemaVersion=1,requestId=str(uuid.uuid4()),authorityId=self.binding['authorityId'],expectedRevision=node['revision'],
                drainAt=(now+timedelta(seconds=self.window.drain_after_seconds)).isoformat().replace('+00:00','Z'),
                notBefore=(now+timedelta(seconds=self.window.not_before_after_seconds)).isoformat().replace('+00:00','Z'),
                afterJobId=None,targetImage=request['localImageId'])
            with self._db(write=True) as db:
                existing=db.execute('SELECT * FROM schedule_intents WHERE operation=? AND generation=?',
                    (request['operationId'],request['hostGeneration'])).fetchone()
                if existing:
                    if existing['request']!=canonical(request):raise MaintenanceAdapterHeld('schedule_cycle_already_bound')
                    row=dict(existing)
                else:
                    db.execute('INSERT INTO schedule_intents VALUES(?,?,?,?,?,NULL)',
                        (request['requestSha256'],request['operationId'],request['hostGeneration'],canonical(request),canonical(wire)))
            if existing:return self._plan(row)[0]
            self._check()
            try:self.operator.json('POST','/v1/prototype/nodes/'+self.binding['nodeId']+'/schedule-owned',wire)
            except Exception:raise MaintenanceAdapterHeld('schedule_requires_owned_receipt_lookup') from None
            row=self._schedule_row(request);plan,_=self._plan(row)
            with self._db(write=True) as db:db.execute('UPDATE schedule_intents SET receipt=? WHERE request_sha=?',(canonical(plan),request['requestSha256']))
            return plan

    def lookup_schedule(self,request):
        with self._operation():
            request=self._request(request,'schedule');row=self._schedule_row(request)
            if row is None:return None
            if row['request']!=canonical(request):raise MaintenanceAdapterHeld('schedule_cycle_already_bound')
            return self._plan(row)[0]

    def _run_plan(self,request):
        row=self._schedule_row(request)
        if row is None:raise MaintenanceAdapterHeld('owned_schedule_intent_required')
        prior=json.loads(row['request'])
        left={key:value for key,value in request.items() if key not in ('plan','requestSha256','receiptDirectory')}
        right={key:value for key,value in prior.items() if key not in ('requestSha256','receiptDirectory')}
        plan,_=self._plan(row)
        if left!=right or request['plan']!=plan:raise MaintenanceAdapterHeld('frozen_run_plan_changed')
        return plan

    def _ready(self,request):
        plan=self._run_plan(request)
        if self._host()['imageId']!=request['previousImageId']:raise MaintenanceAdapterHeld('previous_image_changed')
        node=self.api.json('GET','/v1/prototype/nodes/'+self.binding['nodeId'])['node']
        if (node.get('id')!=self.binding['nodeId'] or node.get('planId')!=plan['planId']
                or node.get('targetImage')!=request['localImageId'] or node['state'] not in ('scheduled','draining')):
            raise MaintenanceAdapterHeld('frozen_plan_not_eligible')
        if node['state']!='draining' or stamp(node['notBefore'])>aware(self.clock()):return False
        jobs=self.api.json('GET','/v1/prototype/jobs')['jobs']
        if any(job.get('nodeId')==self.binding['nodeId'] and job['status'] in ('running','held') for job in jobs):return False
        try:
            with NodeGate(self.binding['engineId']) as gate:
                state=gate.snapshot()
                if (state['nodeId'],state['authorityId'],state['coordinatorOrigin'])!=(self.binding['nodeId'],self.binding['authorityId'],self.api.origin):
                    raise MaintenanceAdapterHeld('physical_gate_binding_changed')
                if state['reservation'] is not None:return False
                _,_,adapter=self._context(request['receiptDirectory'])
                return adapter.idle() is True
        except GateBusy:return False

    def ready_to_run(self,request):
        with self._operation():return self._ready(self._request(request,'run'))

    def run(self,request):
        with self._operation():
            request=self._request(request,'run')
            with self._db() as db:row=db.execute('SELECT * FROM run_intents WHERE operation=? AND generation=?',
                (request['operationId'],request['hostGeneration'])).fetchone()
            if row:
                if row['request']!=canonical(request):raise MaintenanceAdapterHeld('run_cycle_already_bound')
                value=self._completion(request)
                if value is None:raise MaintenanceAdapterHeld('issued_run_requires_read_only_reconciliation')
                return value
            if not self._ready(request):raise MaintenanceAdapterHeld('maintenance_not_ready')
            with self._db(write=True) as db:
                old=db.execute('SELECT request FROM run_intents WHERE operation=? AND generation=?',
                    (request['operationId'],request['hostGeneration'])).fetchone()
                if old:raise MaintenanceAdapterHeld('issued_run_requires_read_only_reconciliation')
                db.execute("INSERT INTO run_intents VALUES(?,?,?,?,'intent',NULL)",
                    (request['requestSha256'],request['operationId'],request['hostGeneration'],canonical(request)))
            try:
                result=managed._run_locked(self.root,self.api,self.operator,Path(request['receiptDirectory']),self.window.wait_seconds,
                    self.window.drain_seconds,engine_id=self.binding['engineId'],expected_plan_id=request['plan']['planId'],expected_target=request['localImageId'])
            except Exception:raise MaintenanceAdapterHeld('managed_run_requires_read_only_reconciliation') from None
            if result.get('status')!='succeeded':raise MaintenanceAdapterHeld('issued_run_has_no_successful_completion')
            value=self._completion(request)
            if value is None:raise MaintenanceAdapterHeld('exact_managed_completion_required')
            with self._db(write=True) as db:db.execute("UPDATE run_intents SET state='completed',receipt=? WHERE request_sha=?",(canonical(value),request['requestSha256']))
            return value

    def _completion(self,request):
        with self._db() as db:row=db.execute('SELECT * FROM run_intents WHERE request_sha=?',(request['requestSha256'],)).fetchone()
        if row is None:return None
        if row['request']!=canonical(request):raise MaintenanceAdapterHeld('run_request_changed')
        self._run_plan(request)
        root=Path(request['receiptDirectory'])
        if not (root/'managed-update.json').exists() or not (root/'update.json').exists():return None
        state=read_json(root/'managed-update.json');private=read_json(root/'update.json')
        if state.get('stage')!='completed' or private.get('stage')!='completed':return None
        settings,ingress,adapter=self._context(root)
        expected=dict(healthy=True,image=request['localImageId'],previousImage=request['previousImageId'],
            composePinUpdateRequired=False,persistentComposePin=request['localImageId'],ingress='open')
        if (state.get('binding')!=managed.operation_binding(self.api,settings,ingress,adapter.args)
                or state.get('planId')!=request['plan']['planId'] or state.get('targetImage')!=request['localImageId']
                or state.get('oldImage')!=request['previousImageId'] or state.get('persistentComposePin')!=request['localImageId']
                or private.get('planId')!=request['plan']['planId'] or private.get('targetImage')!=request['localImageId']
                or private.get('nodeId')!=self.binding['nodeId'] or private.get('oldImage')!=request['previousImageId']
                or private.get('status')!='succeeded' or private.get('finishResult')!=expected
                or state.get('result')!=dict(status='succeeded',nodeId=self.binding['nodeId'],**expected)):
            raise MaintenanceAdapterHeld('completed_receipt_binding_changed')
        self._authority();node=self.api.json('GET','/v1/prototype/nodes/'+self.binding['nodeId'])['node']
        if not finish_matches(node,private,expected):raise MaintenanceAdapterHeld('coordinator_finish_not_proven')
        source=read_json(self.root/'compose.json')
        if (source['services'][settings['service']]['image']!=request['localImageId']
                or managed.compose_base(source,settings['service'])!=state['composeBaseSha256']
                or self._host()['imageId']!=request['localImageId'] or not adapter.serving(request['localImageId'])):
            raise MaintenanceAdapterHeld('durable_host_pin_not_proven')
        managed.owned_open_proof(ingress,state)
        value=dict(schemaVersion=1,requestSha256=request['requestSha256'],**{key:request[key] for key in COMMON},
            planId=request['plan']['planId'],planSha256=request['plan']['planSha256'],previousImageId=request['previousImageId'],
            targetImageId=request['localImageId'],qualificationSha256=request['qualificationSha256'],imageId=request['localImageId'],
            healthy=True,hostPinDurable=True,ingressOpen=True,coordinatorFinished=True)
        value=UpdateSupervisor._receipt(seal_receipt(value,'completionSha256'),request,'run')
        if row['receipt'] is not None and row['receipt']!=canonical(value):raise MaintenanceAdapterHeld('sealed_completion_changed')
        return value

    def lookup_completion(self,request):
        with self._operation():return self._completion(self._request(request,'run'))


def aware(value):
    if not isinstance(value,datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise MaintenanceAdapterHeld('aware_UTC_clock_required')
    return value.astimezone(timezone.utc)


def stamp(value):return aware(datetime.fromisoformat(value.replace('Z','+00:00')))


def sys_linux():
    import sys
    return sys.platform.startswith('linux')
