"""Fresh authenticated parent observations plus read-only local execution fences.

No claim, grant, reservation, gate or journal is created or advanced here. A
configured policy/hash is not authority: every check reads the real coordinator,
private driver claim, whole scratch reservation and existing physical OS fence.
"""
from contextlib import contextmanager,closing
from dataclasses import asdict,dataclass
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import ssl
import stat
import threading
import time
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler,HTTPSHandler,ProxyHandler,Request,build_opener

from . import reference_protocol as p
from .node_gate import NodeGate
from .reference_pipeline import PipelineJournal,PipelineOperationLease
from .reference_sender import decode_receipt
from .scratch_policy import ScratchLedger


class ParentGuardHeld(ValueError):pass


CLAIM_FIELDS=('schemaVersion','protocol','authorityId','jobId','generation','environment','nodeId','workerId',
    'workerInstanceId','claimRequestId','claimTokenSha256','manifestSha256','pipelineDigest','status','executionPhase','heldReason')
PARENT_FIELDS=('jobId','generation','environment','nodeId','workerId','workerInstanceId','authorityId','engineId',
    'coordinatorOrigin','gateGeneration','manifestSha256','pipelineDigest','claimRequestId','claimTokenSha256',
    'reservationId','gateReceiptId','inputBytes','outputBudget')


def exact(left,right):return p.canonical_json(left)==p.canonical_json(right)


def _credential(value):
    if type(value) is not str or not 16<=len(value)<=4096 or not all(0x21<=ord(c)<=0x7e for c in value):raise ParentGuardHeld('fixed_worker_credential_required')
    return value


class WorkerCredential:
    """Immutable in-memory role credential; no serializable policy secret field."""
    __slots__=('_role','_secret')
    def __init__(self,role,secret):
        p._slug(role,'worker credential role')
        object.__setattr__(self,'_role',role);object.__setattr__(self,'_secret',_credential(secret))
    def __setattr__(self,*_):raise ParentGuardHeld('worker_credential_is_immutable')
    @property
    def role(self):return self._role
    def __repr__(self):return 'WorkerCredential(role='+repr(self._role)+')'


def tls_trust_sha256(context):
    if (not isinstance(context,ssl.SSLContext) or context.protocol!=ssl.PROTOCOL_TLS_CLIENT or not context.check_hostname
            or context.verify_mode!=ssl.CERT_REQUIRED or context.minimum_version<ssl.TLSVersion.TLSv1_2):raise ParentGuardHeld('verified_worker_tls_required')
    roots=context.get_ca_certs(binary_form=True)
    if not roots:raise ParentGuardHeld('configured_worker_trust_required')
    return p.digest(sorted(hashlib.sha256(cert).hexdigest() for cert in roots))


@dataclass(frozen=True)
class ClaimStatePolicy:
    origin:str
    authority_id:str
    node_id:str
    worker_id:str
    worker_instance_id:str
    credential_role:str
    tls_trust_sha256:str
    timeout_seconds:int=10
    max_response_bytes:int=16384

    def __post_init__(self):
        try:
            origin=urlsplit(self.origin)
            if (type(self.origin) is not str or not self.origin.isascii() or origin.scheme!='https' or not origin.hostname
                    or origin.username or origin.password or origin.path or origin.query or origin.fragment
                    or '\\' in self.origin or any(ord(c)<=32 for c in self.origin) or origin.port==0):raise ValueError()
        except (TypeError,ValueError):raise ParentGuardHeld('fixed_https_worker_origin_required') from None
        p._uuid(self.authority_id,'expected coordinator authority')
        if self.node_id not in ('cluster','main'):raise ParentGuardHeld('fixed_worker_node_required')
        for value in (self.worker_id,self.worker_instance_id,self.credential_role):p._slug(value,'fixed worker identity')
        p._hash(self.tls_trust_sha256,'worker TLS trust digest')
        if type(self.timeout_seconds) is not int or not 1<=self.timeout_seconds<=30 or type(self.max_response_bytes) is not int or not 1024<=self.max_response_bytes<=65536:raise ParentGuardHeld('bounded_claim_observation_required')

    @property
    def sha256(self):return p.digest(dict(protocol='parent-guard-transport-v1',policy=asdict(self)))


class _NoRedirects(HTTPRedirectHandler):
    def redirect_request(self,request,response,*_):
        response.close();raise ParentGuardHeld('claim_observation_redirect_refused')


class ClaimStateClient:
    """Only the authenticated GET claim-state route is available; no fallback/cache."""
    def __init__(self,policy,credential,*,ssl_context):
        if type(policy) is not ClaimStatePolicy or type(credential) is not WorkerCredential or credential.role!=policy.credential_role:
            raise ParentGuardHeld('configured_worker_transport_required')
        if tls_trust_sha256(ssl_context)!=policy.tls_trust_sha256:raise ParentGuardHeld('worker_tls_policy_changed')
        self.policy,self.credential,self.context=policy,credential,ssl_context
        self._initial=(policy,credential,ssl_context);self._policy_sha256=policy.sha256;self._pid=os.getpid()
        self._open=build_opener(ProxyHandler({}),_NoRedirects(),HTTPSHandler(context=ssl_context)).open
        self._initial_open=self._open

    def check(self):
        if (self._pid!=os.getpid() or any(a is not b for a,b in zip((self.policy,self.credential,self.context),self._initial))
                or self._open is not self._initial_open or self.policy.sha256!=self._policy_sha256
                or tls_trust_sha256(self.context)!=self.policy.tls_trust_sha256):raise ParentGuardHeld('worker_transport_policy_changed')

    def snapshot(self,job_id,claim_token):
        self.check();p._uuid(job_id,'observed parent job')
        if type(claim_token) is not str or not 32<=len(claim_token)<=128 or not re.fullmatch(r'[A-Za-z0-9_-]+',claim_token):raise ParentGuardHeld('private_claim_credential_required')
        url=self.policy.origin+'/ltds/worker/v1/jobs/'+job_id+'/claim-state'
        request=Request(url,method='GET',headers={'Authorization':'Bearer '+self.credential._secret,'X-Claim-Token':claim_token,
            'Accept':'application/json','Accept-Encoding':'identity','Cache-Control':'no-store','Pragma':'no-cache'})
        response=None;deadline=time.monotonic()+self.policy.timeout_seconds
        try:
            response=self._open(request,timeout=self.policy.timeout_seconds)
            if (response.status!=200 or response.geturl()!=url or response.headers.get('Content-Type','').split(';')[0].strip().lower()!='application/json'
                    or response.headers.get('Content-Encoding','identity').lower()!='identity' or response.headers.get('Cache-Control','').lower()!='no-store'):
                raise ParentGuardHeld('claim_observation_response_unavailable')
            lengths=response.headers.get_all('Content-Length',[])
            if len(lengths)>1:raise ParentGuardHeld('claim_observation_response_unavailable')
            length=lengths[0] if lengths else None
            if length is not None and (not length.isascii() or not length.isdecimal() or int(length)>self.policy.max_response_bytes):raise ParentGuardHeld('bounded_claim_observation_required')
            chunks=[];total=0;read=getattr(response,'read1',response.read)
            while True:
                remaining=deadline-time.monotonic()
                if remaining<=0:raise ParentGuardHeld('claim_observation_deadline')
                socket=getattr(getattr(getattr(response,'fp',None),'raw',None),'_sock',None)
                if socket is not None:socket.settimeout(remaining)
                elif not response.isclosed():raise ParentGuardHeld('bounded_claim_observation_required')
                chunk=read(min(16384,self.policy.max_response_bytes-total+1))
                if not chunk:break
                chunks.append(chunk);total+=len(chunk)
                if total>self.policy.max_response_bytes:raise ParentGuardHeld('bounded_claim_observation_required')
                if length is not None and total==int(length):break
            if time.monotonic()>=deadline or length is not None and total!=int(length):raise ParentGuardHeld('claim_observation_response_unavailable')
            value=decode_receipt(b''.join(chunks));p._closed(value,CLAIM_FIELDS,'claim observation')
            if type(value['schemaVersion']) is not int or value['schemaVersion']!=1 or value['protocol']!='ltds-parent-claim-v1':raise ParentGuardHeld('claim_observation_version_unavailable')
            p._uuid(value['authorityId'],'observed authority');p._uuid(value['jobId'],'observed job');p._integer(value['generation'],'observed generation');p._environment(value['environment'])
            for key in ('nodeId','workerId','workerInstanceId','claimRequestId','executionPhase'):p._slug(value[key],'observed identity')
            for key in ('claimTokenSha256','manifestSha256','pipelineDigest'):p._hash(value[key],'observed digest')
            if (value['status'] not in ('running','held','succeeded','failed','cancelled')
                    or value['heldReason'] is not None and (type(value['heldReason']) is not str or not re.fullmatch(r'[a-z][a-z0-9_]{0,79}',value['heldReason']))
                    or value['status']!='held' and value['heldReason'] is not None):raise ParentGuardHeld('claim_observation_state_unavailable')
            self.check();return value
        except HTTPError as error:
            error.close();raise ParentGuardHeld('claim_observation_rejected') from None
        except ParentGuardHeld:raise
        except Exception:raise ParentGuardHeld('claim_observation_unavailable') from None
        finally:
            if response is not None:response.close()


def _private(path,*,directory=False):
    path=Path(path).absolute()
    if path.resolve()!=path or any(p.is_symlink() for p in (path,*path.parents)):raise ParentGuardHeld('private_guard_state_required')
    info=path.lstat()
    if (info.st_uid!=os.geteuid() or stat.S_IMODE(info.st_mode)!=(0o700 if directory else 0o600)
            or (not stat.S_ISDIR(info.st_mode) if directory else not stat.S_ISREG(info.st_mode) or info.st_nlink!=1)):
        raise ParentGuardHeld('private_guard_state_required')
    return path,(info.st_dev,info.st_ino)


@contextmanager
def _readonly_sqlite(path):
    path,identity=_private(path)
    descriptor=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
        info=os.fstat(descriptor);header=os.read(descriptor,20)
        if (info.st_dev,info.st_ino)!=identity or header[:16]!=b'SQLite format 3\0' or header[18:20]!=b'\1\1':raise ParentGuardHeld('rollback_guard_database_required')
    finally:os.close(descriptor)
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=5)) as db:
        db.row_factory=sqlite3.Row;db.execute('BEGIN')
        yield db
        if _private(path)[1]!=identity:raise ParentGuardHeld('guard_database_identity_changed')


def _private_json(raw):
    if type(raw) is not str or len(raw.encode())>4*p.MAX_BODY_BYTES:raise ParentGuardHeld('bounded_private_claim_required')
    def pairs(items):
        result={}
        for key,value in items:
            if key in result:raise ParentGuardHeld('duplicate_private_claim_field')
            result[key]=value
        return result
    value=json.loads(raw,object_pairs_hook=pairs,parse_constant=lambda _:(_ for _ in ()).throw(ParentGuardHeld('finite_private_claim_required')))
    p.canonical_json(value);return value


class DriverClaimTokens:
    """Read existing private driver claims; create or modify no credential state."""
    def __init__(self,path,*,journal,policy,state_owner):
        from ops.reference_worker_daemon import DaemonStateLock
        if type(state_owner) is not DaemonStateLock or not isinstance(journal,PipelineJournal) or type(policy) is not ClaimStatePolicy:raise ParentGuardHeld('actual_private_claim_source_required')
        self.path,self._identity=_private(path);self.root=self.path.parent
        self._initial_path=self.path
        if self.root!=journal.root or self.path in (journal.path,journal.lock_path,state_owner.root/'worker-daemon.lock'):raise ParentGuardHeld('shared_private_claim_root_required')
        state_owner.check(self.root);self.journal,self.policy,self.owner=journal,policy,state_owner
        self._pid=os.getpid();self._binding=journal.guard_binding();self._initial=(journal,policy,state_owner)

    @property
    def binding(self):return dict(self._binding)

    def check(self):
        if (self._pid!=os.getpid() or any(a is not b for a,b in zip((self.journal,self.policy,self.owner),self._initial))
                or self.path!=self._initial_path or self.journal.guard_binding()!=self._binding or _private(self.path)[1]!=self._identity):raise ParentGuardHeld('private_claim_source_changed')
        self.owner.check(self.root);self.journal._check()

    def token(self,parent):
        self.check()
        try:
            with _readonly_sqlite(self.path) as db:
                binding=db.execute('SELECT binding FROM driver_binding WHERE singleton=1').fetchone()
                row=db.execute('SELECT claim_request,data,length(CAST(data AS BLOB)) AS size FROM driver_jobs WHERE job=?',(parent['jobId'],)).fetchone()
                if not binding or row is None or row['size']>4*p.MAX_BODY_BYTES:raise ValueError()
                configured=_private_json(binding[0]);data=_private_json(row['data'])
            policy=self.journal.policy
            expected=dict(origin=self.policy.origin,nodeId=policy.node_id,workerId=policy.worker_id,workerInstanceId=policy.worker_instance_id,
                authorityId=policy.authority_id,engineId=policy.engine_id,gateOrigin=policy.coordinator_origin,gateGeneration=policy.gate_generation,
                outputBudget=policy.output_budget,environments=sorted(policy.allowed_environments))
            if any(not exact(configured[key],value) for key,value in expected.items()) or row['claim_request']!=parent['claimRequestId']:raise ValueError()
            job=data['claim'];token=job['claimToken']
            if type(token) is not str or not 32<=len(token)<=128 or not re.fullmatch(r'[A-Za-z0-9_-]+',token):raise ValueError()
            expected={key:parent[key] for key in ('jobId','generation','environment','nodeId','manifestSha256','pipelineDigest')}
            if (any(not exact(job[key],value) for key,value in expected.items())
                    or not hmac.compare_digest(hashlib.sha256(token.encode()).hexdigest(),parent['claimTokenSha256'])
                    or data['reservation']['reservationId']!=parent['reservationId']
                    or p.manifest_sha256(job['submission']['manifest'])!=parent['manifestSha256']
                    or p.digest(job['submission']['pipeline'])!=parent['pipelineDigest']
                    or job['submission']['manifest']['totalBytes']!=parent['inputBytes']):raise ValueError()
            self.check();return token
        except ParentGuardHeld:raise
        except Exception:raise ParentGuardHeld('private_parent_claim_unavailable') from None


@contextmanager
def _existing_gate(engine_id):
    """Acquire the real existing flock without NodeGate's mkdir/chmod/create path."""
    gate=NodeGate(engine_id);root,root_identity=_private(gate.root,directory=True)
    lock,identity=_private(root/'gate.lock');_private(root/'gate.json')
    descriptor=os.open(lock,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
        import fcntl
        if (os.fstat(descriptor).st_dev,os.fstat(descriptor).st_ino)!=identity:raise ParentGuardHeld('physical_gate_identity_changed')
        try:fcntl.flock(descriptor,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:raise ParentGuardHeld('physical_gate_lease_busy') from None
        gate._stream=os.fdopen(descriptor,'rb');descriptor=None
        yield gate
        if _private(root,directory=True)[1]!=root_identity or _private(lock)[1]!=identity:raise ParentGuardHeld('physical_gate_identity_changed')
    finally:
        if gate._stream is not None:gate._stream.close();gate._stream=None
        if descriptor is not None:os.close(descriptor)


class ProductionParentGuard:
    def __init__(self,*,journal,ledger,client,tokens,state_owner):
        from ops.reference_worker_daemon import DaemonStateLock
        if (not isinstance(journal,PipelineJournal) or not isinstance(ledger,ScratchLedger) or type(client) is not ClaimStateClient
                or type(tokens) is not DriverClaimTokens or type(state_owner) is not DaemonStateLock):raise ParentGuardHeld('actual_parent_guard_dependencies_required')
        policy=journal.policy;transport=client.policy
        if (journal.root!=Path(ledger.path).absolute().parent or state_owner is not tokens.owner or tokens.journal is not journal
                or tokens.policy!=transport or any(getattr(transport,key)!=getattr(policy,key) for key in ('authority_id','node_id','worker_id','worker_instance_id'))):raise ParentGuardHeld('parent_guard_dependency_binding_changed')
        self.journal,self.ledger,self.client,self.tokens,self.owner=journal,ledger,client,tokens,state_owner
        self.root=journal.root;self._pid=os.getpid();self._mutex=threading.Lock();self._binding=journal.guard_binding()
        self._initial=(journal,ledger,client,tokens,state_owner);self._ledger_identity=_private(Path(ledger.path).absolute())[1]
        self._check()

    @property
    def binding(self):return dict(self._binding)

    def _check(self):
        if (self._pid!=os.getpid() or any(a is not b for a,b in zip((self.journal,self.ledger,self.client,self.tokens,self.owner),self._initial))
                or self.journal.guard_binding()!=self._binding or self.tokens.binding!=self._binding
                or _private(Path(self.ledger.path).absolute())[1]!=self._ledger_identity):raise ParentGuardHeld('parent_guard_owner_changed')
        self.owner.check(self.root);self.journal._check();self.tokens.check();self.client.check()

    def _parent(self,parent):
        p._closed(parent,PARENT_FIELDS,'parent fence binding');parent=json.loads(p.canonical_json(parent));policy=self.journal.policy
        expected=dict(nodeId=policy.node_id,workerId=policy.worker_id,workerInstanceId=policy.worker_instance_id,authorityId=policy.authority_id,
            engineId=policy.engine_id,coordinatorOrigin=policy.coordinator_origin,gateGeneration=policy.gate_generation,
            outputBudget=policy.output_budget)
        if any(not exact(parent[key],value) for key,value in expected.items()) or parent['environment'] not in policy.allowed_environments:raise ParentGuardHeld('parent_guard_binding_changed')
        p._uuid(parent['jobId'],'parent job');p._integer(parent['generation'],'parent generation');p._integer(parent['inputBytes'],'parent input bytes');p._environment(parent['environment'])
        for key in ('claimRequestId','reservationId'):p._slug(parent[key],'parent identity')
        for key in ('manifestSha256','pipelineDigest','claimTokenSha256'):p._hash(parent[key],'parent digest')
        if type(parent['gateReceiptId']) is not str or not re.fullmatch(r'[a-f0-9]{32}',parent['gateReceiptId']):raise ParentGuardHeld('parent_gate_receipt_required')
        return parent

    def _reservation(self,parent,*,mutation,recovering=False):
        with _readonly_sqlite(Path(self.ledger.path).absolute()) as db:
            row=db.execute('SELECT * FROM reservations WHERE id=?',(parent['reservationId'],)).fetchone()
            metadata={r['key']:r['value'] for r in db.execute("SELECT key,value FROM metadata WHERE key IN ('clock','config')")}
            if row is None:raise ParentGuardHeld('parent_reservation_unavailable')
            row=dict(row);settings=_private_json(row['settings']);configured=_private_json(metadata['config'])
        if (not exact(configured,dict(root=str(self.ledger.root),floor=self.ledger.floor,grace=self.ledger.grace))
                or not exact({k:settings[k] for k in ('job_id','generation','reservation_id','manifest_sha256','input_bytes','output_budget','environment')},
                    dict(job_id=parent['jobId'],generation=parent['generation'],reservation_id=parent['reservationId'],manifest_sha256=parent['manifestSha256'],input_bytes=parent['inputBytes'],output_budget=parent['outputBudget'],environment=parent['environment']))
                or not exact([row['job'],row['generation'],row['environment']],[parent['jobId'],parent['generation'],parent['environment']])):raise ParentGuardHeld('parent_reservation_binding_changed')
        for key in ('allocated','remaining'):p._integer(row[key],'parent allocation',allow_zero=True)
        if (row['allocated']<parent['inputBytes'] or row['allocated']+row['remaining']!=parent['inputBytes']+parent['outputBudget']
                or row['state'] not in ('reserved','engine_started','uncertain') or row['terminal_at'] is not None
                or mutation and row['state']=='uncertain' or recovering and row['state']=='reserved'):raise ParentGuardHeld('parent_reservation_not_executable')
        now=self.ledger.clock();prior=float(metadata['clock'])
        if any(type(v) not in (int,float) or not math.isfinite(v) or v<0 for v in (now,prior,row['expires'])):raise ParentGuardHeld('parent_reservation_clock_unavailable')
        if row['state']=='reserved' and row['expires']<=max(now,prior):raise ParentGuardHeld('parent_reservation_expired')

    @staticmethod
    def _fence(gate,parent):
        state=gate.snapshot();held=state['reservation']
        if (not exact({key:state[key] for key in ('engineId','nodeId','authorityId','coordinatorOrigin','generation')},
                dict(engineId=parent['engineId'],nodeId=parent['nodeId'],authorityId=parent['authorityId'],coordinatorOrigin=parent['coordinatorOrigin'],generation=parent['gateGeneration']))
                or not held or held['receiptId']!=parent['gateReceiptId'] or held['completionDigest'] is not None
                or held['job']!=dict(id=parent['jobId'],workerId=parent['workerId'],nodeId=parent['nodeId'],authorityId=parent['authorityId'])):raise ParentGuardHeld('parent_physical_fence_changed')

    def check(self,parent_binding,*,mutation):
        if type(mutation) is not bool:raise ParentGuardHeld('explicit_parent_check_mode_required')
        try:self._check()
        except ParentGuardHeld:raise
        except Exception:raise ParentGuardHeld('parent_guard_owner_unavailable') from None
        if not self._mutex.acquire(blocking=False):raise ParentGuardHeld('parent_guard_check_active')
        try:
            parent=self._parent(parent_binding)
            with _existing_gate(parent['engineId']) as gate:
                self._fence(gate,parent);self._reservation(parent,mutation=mutation)
                if mutation and self.journal.policy.recovery_control_budget:
                    control=self.ledger.read_control_charge(parent['reservationId'])
                    if (control is None or control['parentBindingSha256']!=p.digest(parent) or control['allowanceBytes']!=self.journal.policy.recovery_control_budget
                            or control['allocatedBefore']!=parent['inputBytes'] or control['publicationSha256'] is None):raise ParentGuardHeld('published_original_recovery_control_required')
                    from .reference_worker_recovery import validate_published_control
                    validate_published_control(self,parent,control)
                token=self.tokens.token(parent);observation=self.client.snapshot(parent['jobId'],token)
                expected=dict(schemaVersion=1,protocol='ltds-parent-claim-v1',**{key:parent[key] for key in
                    ('authorityId','jobId','generation','environment','nodeId','workerId','workerInstanceId','claimRequestId','claimTokenSha256','manifestSha256','pipelineDigest')})
                if any(not exact(observation[key],value) for key,value in expected.items()) or observation['executionPhase']!='processing':raise ParentGuardHeld('live_parent_claim_binding_changed')
                running=observation['status']=='running' and observation['heldReason'] is None
                recovering=not mutation and observation['status']=='held' and observation['heldReason']=='coordinator_restart'
                if not running and not recovering:raise ParentGuardHeld('live_parent_claim_not_executable')
                self._check();self._fence(gate,parent);self._reservation(parent,mutation=mutation,recovering=recovering)
            return None
        except ParentGuardHeld:raise
        except Exception:raise ParentGuardHeld('parent_guard_evidence_unavailable') from None
        finally:self._mutex.release()

    def check_preexecution_recovery(self,parent_binding,*,lease):
        """Setup/claim continuation only; never grants an engine mutation.

        A reserved job whose controls were charged before any stage can survive
        restart without pretending that an engine was already started.
        """
        if type(lease) is not PipelineOperationLease:raise ParentGuardHeld('actual_preexecution_lease_required')
        try:
            lease.check(self.journal);self._check();parent=self._parent(parent_binding)
            with _readonly_sqlite(self.journal.path) as db:
                row=db.execute('SELECT identity,state,result,failure FROM pipeline_parents WHERE job=?',(parent['jobId'],)).fetchone()
                stages=db.execute('SELECT stage,state,intent,proof FROM pipeline_stages WHERE job=?',(parent['jobId'],)).fetchall()
                binding=db.execute('SELECT value FROM pipeline_binding WHERE id=1').fetchone()
            if (row is None or row['identity']!=p.canonical_json(parent).decode() or row['state']!='bound' or row['result'] is not None or row['failure'] is not None
                    or binding is None or binding[0]!=self.journal._binding or len(stages)!=2 or {stage['stage'] for stage in stages}!={'odm','treedar'}
                    or any(stage['state']!='new' or stage['intent'] is not None or stage['proof'] is not None for stage in stages)):raise ParentGuardHeld('unissued_original_parent_required')
            receipt=self.ledger.read_control_charge(parent['reservationId'])
            if receipt is not None and (receipt['parentBindingSha256']!=p.digest(parent) or receipt['allocatedBefore']!=parent['inputBytes']
                    or receipt['allowanceBytes']!=self.journal.policy.recovery_control_budget):raise ParentGuardHeld('typed_preexecution_control_required')
            if not self.journal.policy.recovery_control_budget:raise ParentGuardHeld('explicit_preexecution_control_policy_required')
            with _existing_gate(parent['engineId']) as gate:
                self._fence(gate,parent);self._reservation(parent,mutation=True)
                with _readonly_sqlite(Path(self.ledger.path).absolute()) as db:
                    reservation=db.execute('SELECT state,allocated FROM reservations WHERE id=?',(parent['reservationId'],)).fetchone()
                    results=db.execute('SELECT COUNT(*) FROM results WHERE reservation=?',(parent['reservationId'],)).fetchone()[0]
                expected=receipt['allocatedAfter'] if receipt is not None else parent['inputBytes']
                if reservation['state']!='reserved' or reservation['allocated']!=expected or results:raise ParentGuardHeld('preexecution_only_reservation_required')
                self._original_input_files(parent)
                token=self.tokens.token(parent);value=self.client.snapshot(parent['jobId'],token)
                expected=dict(schemaVersion=1,protocol='ltds-parent-claim-v1',**{key:parent[key] for key in ('authorityId','jobId','generation','environment','nodeId','workerId','workerInstanceId','claimRequestId','claimTokenSha256','manifestSha256','pipelineDigest')})
                if any(not exact(value[key],item) for key,item in expected.items()) or value['executionPhase']!='processing' or not (
                    value['status']=='running' and value['heldReason'] is None or receipt is not None and value['status']=='held' and value['heldReason']=='coordinator_restart'):raise ParentGuardHeld('current_original_preexecution_claim_required')
                lease.check(self.journal);self._check();self._fence(gate,parent);self._reservation(parent,mutation=True)
            return None
        except ParentGuardHeld:raise
        except Exception:raise ParentGuardHeld('preexecution_recovery_evidence_unavailable') from None

    def _original_input_files(self,parent):
        self.tokens.token(parent)
        with _readonly_sqlite(self.tokens.path) as db:
            row=db.execute('SELECT data FROM driver_jobs WHERE job=?',(parent['jobId'],)).fetchone();data=_private_json(row[0])
        objects=data['claim']['submission']['manifest']['objects'];paths=data['inputPaths']
        if type(paths) is not dict or set(paths)!={obj['id'] for obj in objects}:raise ParentGuardHeld('complete_original_input_paths_required')
        total=0
        for obj in objects:
            relative=paths[obj['id']];self.ledger._owned_path(parent['reservationId'],relative);path,identity=_private(self.ledger.root/relative)
            fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
            try:
                before=os.fstat(fd)
                if (before.st_dev,before.st_ino)!=identity or before.st_size!=obj['byteSize']:raise ParentGuardHeld('original_input_binding_changed')
                digest=hashlib.sha256();count=0
                while chunk:=os.read(fd,min(1024**2,obj['byteSize']-count+1)):
                    count+=len(chunk)
                    if count>obj['byteSize']:raise ParentGuardHeld('original_input_byte_bound_changed')
                    digest.update(chunk)
                after=os.fstat(fd);current=path.stat()
                signature=lambda info:(info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns,info.st_ctime_ns)
                if count!=obj['byteSize'] or digest.hexdigest()!=obj['sha256'] or signature(before)!=signature(after) or signature(after)!=signature(current):raise ParentGuardHeld('original_input_integrity_changed')
                total+=count
            finally:os.close(fd)
        if total!=parent['inputBytes']:raise ParentGuardHeld('whole_original_input_bytes_required')
