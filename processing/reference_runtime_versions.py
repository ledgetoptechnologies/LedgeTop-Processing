"""Owned-maintenance runtime metadata publication; never Docker attestation.

Only a qualified operator's existing begin-update token can publish a changed
configured runtime binding. Native/Reader HTTP is freshly observed, never
silently adopted. Historical receipts are read-only and confer no new permit.
"""
from contextlib import nullcontext
from dataclasses import dataclass
import hashlib
import hmac
import ipaddress
import json
import math
import re
import threading
import time
from types import MappingProxyType
from urllib.parse import urlsplit,urlencode

from . import reference_protocol as p
from .maintenance import Conflict,canonical
from .prototype import Queue
from .config import _node_origin
from .reference_catalog_transport import read_json
from .reference_parent_guard import tls_trust_sha256
from .reference_routing import RoutingNodeBinding,worker_observation,HEALTH_PATH
from .options import _validate_info

REQUEST_FIELDS=frozenset(('schemaVersion','requestId','authorityId','planId','expectedRevision','ownedScheduleRequestId','expectedVersion','newVersion','targetImage','engineId','gateGeneration'))
RECEIPT_FIELDS=frozenset(('schemaVersion','requestId','requestSha256','authorityId','nodeId','planId','expectedRevision','ownedScheduleRequestId','engineId','gateGeneration','expectedVersion','newVersion','targetImage','epoch','bindingsSha256','verificationScope'))
CURRENT_FIELDS=frozenset(('schemaVersion','authorityId','nodeId','engineId','gateGeneration','engineVersion','targetImage','epoch','nodeEpoch','bindingsSha256','scope','publicationRequestId'))
MAX_RECEIPTS=256
MAX_ROW=16384
_SCHEMA={
 'reference_runtime_versions':'CREATE TABLE reference_runtime_versions(singleton INTEGER PRIMARY KEY CHECK(singleton=1),payload TEXT NOT NULL,digest TEXT NOT NULL)',
 'reference_runtime_version_receipts':'CREATE TABLE reference_runtime_version_receipts(request_id TEXT PRIMARY KEY,node_id TEXT NOT NULL,request_sha TEXT NOT NULL,request_json TEXT NOT NULL,receipt_json TEXT NOT NULL,receipt_sha TEXT NOT NULL,update_token_sha TEXT NOT NULL,epoch INTEGER UNIQUE NOT NULL,row_sha TEXT NOT NULL)',
}
for _op in ('INSERT','UPDATE','DELETE'):
    _SCHEMA['reference_runtime_fence_'+_op.lower()]=f"CREATE TRIGGER reference_runtime_fence_{_op.lower()} BEFORE {_op} ON jobs BEGIN SELECT CASE WHEN reference_runtime_writer_v1()!=1 THEN RAISE(ABORT,'runtime writer required') END; END"

class RuntimeVersionHeld(ValueError):pass

@dataclass(frozen=True)
class RuntimeVersionPublicationPolicy:
    schema_version:int=1
    def __post_init__(self):
        if type(self.schema_version) is not int or self.schema_version!=1:raise RuntimeVersionHeld('runtime_version_policy_required')

@dataclass(frozen=True)
class RuntimeBindingSnapshot:
    epoch:int
    bindings_sha256:str
    bindings:object
    target_images:object
    node_epochs:object
    publication_requests:object

def _version(value):
    if type(value) is not str or re.fullmatch('[A-Za-z0-9][A-Za-z0-9_.+-]{0,127}',value) is None:raise RuntimeVersionHeld('bounded_runtime_version_required')

def _image(value):
    if type(value) is not str or re.fullmatch('sha256:[a-f0-9]{64}',value) is None:raise RuntimeVersionHeld('immutable_runtime_image_required')

def validate_version_request(value):
    p._closed(value,REQUEST_FIELDS,'runtime publication')
    if type(value['schemaVersion']) is not int or value['schemaVersion']!=1:raise RuntimeVersionHeld('runtime_publication_version_required')
    for field in ('requestId','authorityId','planId','ownedScheduleRequestId'):p._uuid(value[field],field)
    p._integer(value['expectedRevision'],'expectedRevision',allow_zero=True)
    _version(value['expectedVersion']);_version(value['newVersion']);_image(value['targetImage'])
    RoutingNodeBinding(value['engineId'],value['gateGeneration'],value['newVersion'])
    return value

def request_sha256(node_id,value):
    validate_version_request(value);p._slug(node_id,'nodeId')
    return p.digest(dict(nodeId=node_id,request=value))

def validate_version_receipt(value,request,node_id):
    validate_version_request(request);p._closed(value,RECEIPT_FIELDS,'runtime publication receipt')
    expected={field:request[field] for field in REQUEST_FIELDS}
    expected.update(nodeId=node_id,requestSha256=request_sha256(node_id,request),verificationScope='configured_worker_and_native_http')
    for key,item in expected.items():
        if type(value[key]) is not type(item) or value[key]!=item:raise RuntimeVersionHeld('runtime_receipt_binding_changed')
    p._integer(value['epoch'],'runtime epoch');p._hash(value['bindingsSha256'],'runtime bindings')
    return value

def validate_current_version(value,request,node_id,receipt):
    validate_version_receipt(receipt,request,node_id)
    p._closed(value,CURRENT_FIELDS,'current runtime')
    expected=dict(schemaVersion=1,authorityId=request['authorityId'],nodeId=node_id,engineId=request['engineId'],gateGeneration=request['gateGeneration'],
        engineVersion=request['newVersion'],targetImage=request['targetImage'],scope='configured_runtime',publicationRequestId=request['requestId'],nodeEpoch=receipt['epoch'])
    for key,item in expected.items():
        if type(value[key]) is not type(item) or value[key]!=item:raise RuntimeVersionHeld('current_runtime_binding_changed')
    p._integer(value['epoch'],'current epoch');p._hash(value['bindingsSha256'],'current runtime bindings')
    if value['epoch']<receipt['epoch'] or value['epoch']==receipt['epoch'] and value['bindingsSha256']!=receipt['bindingsSha256']:
        raise RuntimeVersionHeld('current_runtime_epoch_changed')
    return value

def _read(raw):
    if type(raw) is not str or len(raw.encode())>MAX_ROW:raise RuntimeVersionHeld('runtime_row_budget')
    value=p.decode_submission(raw.encode())
    if p.canonical_json(value).decode()!=raw:raise RuntimeVersionHeld('runtime_row_changed')
    return value

def _encode(value):
    raw=p.canonical_json(value).decode()
    if len(raw.encode())>MAX_ROW:raise RuntimeVersionHeld('runtime_row_budget')
    return raw

def _row_digest(row):
    return p.digest({key:row[key] for key in ('request_id','node_id','request_sha','request_json','receipt_json','receipt_sha','update_token_sha','epoch')})

def validate_runtime_version_abi(db):
    actual={r['name']:r['sql'] for r in db.execute("SELECT name,sql FROM sqlite_master WHERE name LIKE 'reference_runtime_%'")}
    if actual!=_SCHEMA:raise RuntimeVersionHeld('runtime_registry_schema_changed')
    rows=db.execute('SELECT singleton,length(CAST(payload AS BLOB)),length(digest) FROM reference_runtime_versions').fetchall()
    if len(rows)!=1 or rows[0][0]!=1 or not 1<=rows[0][1]<=MAX_ROW or rows[0][2]!=64:raise RuntimeVersionHeld('runtime_registry_marker_changed')
    row=db.execute('SELECT * FROM reference_runtime_versions WHERE singleton=1').fetchone();state=_read(row['payload'])
    p._closed(state,('schemaVersion','baseScope','authorityId','epoch','nodes'),'runtime registry state')
    if type(state['schemaVersion']) is not int or state['schemaVersion']!=1 or row['digest']!=p.digest(state):raise RuntimeVersionHeld('runtime_registry_marker_changed')
    p._hash(state['baseScope'],'runtime base scope');p._uuid(state['authorityId'],'runtime authority');p._integer(state['epoch'],'runtime epoch',allow_zero=True)
    if type(state['nodes']) is not dict or set(state['nodes'])!=set(('cluster','main')):raise RuntimeVersionHeld('runtime_registry_coverage_changed')
    for node,value in state['nodes'].items():
        p._closed(value,('engineId','gateGeneration','engineVersion','targetImage','epoch','requestId'),'runtime node')
        RoutingNodeBinding(value['engineId'],value['gateGeneration'],value['engineVersion'])
        p._integer(value['epoch'],'node runtime epoch',allow_zero=True)
        if value['epoch']>state['epoch']:raise RuntimeVersionHeld('runtime_epoch_changed')
        if value['epoch']==0:
            if value['requestId'] is not None or value['targetImage'] is not None:raise RuntimeVersionHeld('runtime_bootstrap_changed')
        else:_image(value['targetImage']);p._uuid(value['requestId'],'runtime request')
    stats=db.execute('SELECT COUNT(*),COALESCE(MAX(epoch),0),COALESCE(SUM(length(CAST(request_json AS BLOB))+length(CAST(receipt_json AS BLOB))),0),COALESCE(MAX(length(CAST(request_json AS BLOB))),0),COALESCE(MAX(length(CAST(receipt_json AS BLOB))),0) FROM reference_runtime_version_receipts').fetchone()
    if stats[0]>MAX_RECEIPTS or stats[1]!=state['epoch'] or stats[0]!=state['epoch'] or stats[2]>2*MAX_ROW*MAX_RECEIPTS or max(stats[3],stats[4])>MAX_ROW:
        raise RuntimeVersionHeld('runtime_history_budget_or_epoch_changed')
    return state

class RuntimeVersionRegistry:
    def __init__(self,queue,config,settings,*,ssl_context):
        if (type(queue) is not Queue or type(settings.runtime_version_publication) is not RuntimeVersionPublicationPolicy
                or queue.runtime_version_publication is not True or queue.catalog_admission is not True
                or settings.catalog_admission is None or type(settings.routing_bindings) is not dict
                or set(settings.routing_bindings)!=set(('cluster','main')) or any(type(v) is not RoutingNodeBinding for v in settings.routing_bindings.values())
                or set(queue.node_ids)!=set(('cluster','main')) or set(settings.workers)!=set(('cluster','main'))
                or not config.prototype_operator_token):
            raise RuntimeVersionHeld('explicit_paired_runtime_registry_required')
        self.queue,self.config,self.settings,self.context=queue,config,settings,ssl_context
        self._anchors=MappingProxyType(dict(settings.routing_bindings));self._trust=self._tls_identity()
        self._nodes={}
        for node,origin,token in (('cluster',config.local_node_url,config.local_node_token),('main',config.main_node_url,config.main_node_token)):
            worker=settings.workers[node]
            origin=_node_origin(origin);worker_origin=_node_origin(worker['resultOrigin'])
            for address in (origin,worker_origin):
                parsed=urlsplit(address)
                if '%' in (parsed.hostname or ''):raise RuntimeVersionHeld('literal_runtime_origins_required')
                ipaddress.ip_address(parsed.hostname)
            if not worker_origin.startswith('https://'):raise RuntimeVersionHeld('verified_worker_origin_required')
            self._nodes[node]=(origin.rstrip('/'),token,worker_origin,worker['resultBearer'],worker['workerId'],worker['instanceId'])
        self._nodes=MappingProxyType(self._nodes)
        self._base_scope=p.digest(dict(schemaVersion=1,authorityId=queue.authority_id,anchors={n:vars(b) for n,b in self._anchors.items()},
            privateConfigurationSha256=p.digest(dict(config=vars(config),workers=settings.workers,listener=settings.listener,trust=self._trust))))
        self._identity=(queue,config,settings,ssl_context,self._nodes,self._anchors,self._base_scope)
        self._lock=threading.Lock();self._catalog=None
        with queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            present=db.execute("SELECT 1 FROM sqlite_master WHERE name LIKE 'reference_runtime_%' LIMIT 1").fetchone()
            if not present:
                if db.execute("SELECT 1 FROM jobs WHERE status IN ('running','held') LIMIT 1").fetchone():raise RuntimeVersionHeld('runtime_registry_migration_requires_drain')
                for sql in _SCHEMA.values():db.execute(sql)
                state=dict(schemaVersion=1,baseScope=self._base_scope,authorityId=queue.authority_id,epoch=0,
                    nodes={n:dict(engineId=b.engine_id,gateGeneration=b.gate_generation,engineVersion=b.engine_version,targetImage=None,epoch=0,requestId=None) for n,b in self._anchors.items()})
                db.execute('INSERT INTO reference_runtime_versions VALUES(1,?,?)',(_encode(state),p.digest(state)))
            self._state(db)

    def _tls_identity(self):
        c=self.context
        return (tls_trust_sha256(c),c.minimum_version,c.maximum_version,c.options,c.verify_flags,c.hostname_checks_common_name,
            tuple((v['name'],v['protocol'],v['strength_bits']) for v in c.get_ciphers()))

    def _check(self):
        if ((self.queue,self.config,self.settings,self.context,self._nodes,self._anchors,self._base_scope)!=self._identity or self._tls_identity()!=self._trust
                or self.queue.runtime_version_publication is not True):raise RuntimeVersionHeld('runtime_registry_configuration_changed')

    def attach_catalog(self,catalog):
        from .reference_catalog_cache import CatalogAdmission
        if type(catalog) is not CatalogAdmission or catalog.queue is not self.queue or catalog.config is not self.config or catalog.settings is not self.settings or self._catalog is not None:
            raise RuntimeVersionHeld('concrete_original_catalog_required')
        self._catalog=catalog
        with self.queue._db() as db:catalog.store._quota(db)

    def _state(self,db):
        self._check();state=validate_runtime_version_abi(db)
        if state['baseScope']!=self._base_scope or state['authorityId']!=self.queue.authority_id:raise RuntimeVersionHeld('runtime_registry_scope_migration_required')
        for node,b in self._anchors.items():
            item=state['nodes'][node]
            if (item['engineId'],item['gateGeneration'])!=(b.engine_id,b.gate_generation):raise RuntimeVersionHeld('fixed_runtime_identity_changed')
            if item['epoch']==0 and item['engineVersion']!=b.engine_version:raise RuntimeVersionHeld('runtime_bootstrap_changed')
        replay=dict(schemaVersion=1,baseScope=self._base_scope,authorityId=self.queue.authority_id,epoch=0,
            nodes={n:dict(engineId=b.engine_id,gateGeneration=b.gate_generation,engineVersion=b.engine_version,targetImage=None,epoch=0,requestId=None) for n,b in self._anchors.items()})
        for row in db.execute('SELECT * FROM reference_runtime_version_receipts ORDER BY epoch'):
            request,receipt=self._validate_row(row)
            if row['epoch']!=replay['epoch']+1 or row['node_id'] not in replay['nodes']:raise RuntimeVersionHeld('runtime_history_order_changed')
            node=row['node_id'];previous=replay['nodes'][node]
            if (request['authorityId']!=self.queue.authority_id or request['expectedVersion']!=previous['engineVersion']
                    or (request['engineId'],request['gateGeneration'])!=(previous['engineId'],previous['gateGeneration'])):
                raise RuntimeVersionHeld('runtime_history_identity_changed')
            replay['epoch']=row['epoch'];previous.update(engineVersion=request['newVersion'],targetImage=request['targetImage'],epoch=row['epoch'],requestId=row['request_id'])
            if receipt['bindingsSha256']!=self._snapshot(replay).bindings_sha256:raise RuntimeVersionHeld('runtime_history_digest_changed')
        if p.canonical_json(replay)!=p.canonical_json(state):raise RuntimeVersionHeld('runtime_current_history_changed')
        return state

    @staticmethod
    def _validate_row(row):
        request,receipt=_read(row['request_json']),_read(row['receipt_json'])
        validate_version_receipt(receipt,request,row['node_id'])
        if (row['request_id']!=request['requestId'] or row['request_sha']!=request_sha256(row['node_id'],request)
                or row['receipt_sha']!=p.digest(receipt) or type(row['epoch']) is not int or row['epoch']!=receipt['epoch']
                or row['row_sha']!=_row_digest(row)):raise RuntimeVersionHeld('runtime receipt changed')
        p._hash(row['update_token_sha'],'private maintenance claim')
        return request,receipt

    @staticmethod
    def _snapshot(state):
        bindings={n:RoutingNodeBinding(v['engineId'],v['gateGeneration'],v['engineVersion']) for n,v in state['nodes'].items()}
        digest=p.digest(dict(authorityId=state['authorityId'],epoch=state['epoch'],nodes=state['nodes']))
        return RuntimeBindingSnapshot(state['epoch'],digest,MappingProxyType(bindings),
            MappingProxyType({n:v['targetImage'] for n,v in state['nodes'].items()}),
            MappingProxyType({n:v['epoch'] for n,v in state['nodes'].items()}),
            MappingProxyType({n:v['requestId'] for n,v in state['nodes'].items()}))

    def snapshot(self,db=None):
        with self.queue._db() if db is None else nullcontext(db) as current:return self._snapshot(self._state(current))

    def current_bindings(self,db=None):return self.snapshot(db).bindings

    def current(self,node_id):
        self.queue._known_node(node_id);snap=self.snapshot();binding=snap.bindings[node_id]
        return dict(schemaVersion=1,authorityId=self.queue.authority_id,nodeId=node_id,engineId=binding.engine_id,gateGeneration=binding.gate_generation,
            engineVersion=binding.engine_version,targetImage=snap.target_images[node_id],epoch=snap.epoch,nodeEpoch=snap.node_epochs[node_id],
            bindingsSha256=snap.bindings_sha256,scope='configured_runtime',publicationRequestId=snap.publication_requests[node_id])

    def _receipt(self,db,node_id,request_id):
        self._state(db)
        row=db.execute('SELECT *,length(CAST(request_json AS BLOB)) AS request_bytes,length(CAST(receipt_json AS BLOB)) AS receipt_bytes FROM reference_runtime_version_receipts WHERE request_id=?',(request_id,)).fetchone()
        if row is None:return None
        if row['node_id']!=node_id or max(row['request_bytes'],row['receipt_bytes'])>MAX_ROW:raise Conflict('runtime request ownership changed')
        request,receipt=self._validate_row(row)
        return row,request,receipt

    def lookup(self,node_id,request_id):
        self.queue._known_node(node_id);p._uuid(request_id,'runtime request')
        with self.queue._db() as db:
            db.execute('BEGIN');row=self._receipt(db,node_id,request_id)
            return row[2] if row else None

    def _owned(self,db,node,payload,token_sha):
        state=self._state(db);current=state['nodes'][node];anchor=self._anchors[node]
        if (payload['authorityId']!=self.queue.authority_id or payload['expectedVersion']!=current['engineVersion']
                or (payload['engineId'],payload['gateGeneration'])!=(anchor.engine_id,anchor.gate_generation)):raise Conflict('runtime previous binding changed')
        maintenance=self.queue._node(db,node)
        if (maintenance['state']!='updating' or maintenance['plan_id']!=payload['planId'] or maintenance['revision']!=payload['expectedRevision']
                or maintenance['target_image']!=payload['targetImage'] or type(maintenance['update_hash']) is not str
                or not hmac.compare_digest(maintenance['update_hash'],token_sha)):raise Conflict('owned runtime update changed')
        if db.execute("SELECT 1 FROM jobs WHERE node_id=? AND status IN ('running','held') LIMIT 1",(node,)).fetchone():raise Conflict('runtime node still owns execution')
        owned=db.execute('SELECT * FROM owned_maintenance_schedules WHERE request_id=? AND node_id=?',(payload['ownedScheduleRequestId'],node)).fetchone()
        if owned is None or owned['authority_id']!=self.queue.authority_id:raise Conflict('original owned maintenance receipt required')
        request,receipt=_read(owned['request_json']),_read(owned['receipt_json'])
        p._closed(request,('schemaVersion','requestId','authorityId','expectedRevision','drainAt','notBefore','afterJobId','targetImage'),'original owned schedule request')
        p._closed(receipt,('schemaVersion','requestId','authorityId','nodeId','requestSha256','expectedRevision','revision','planId','state','drainAt','notBefore','afterJobId','targetImage','receiptSha256'),'original owned schedule receipt')
        if type(request['schemaVersion']) is not int or request['schemaVersion']!=1 or type(receipt['schemaVersion']) is not int or receipt['schemaVersion']!=1:
            raise Conflict('original owned maintenance version changed')
        p._integer(request['expectedRevision'],'original revision',allow_zero=True);p._integer(receipt['expectedRevision'],'owned expected revision',allow_zero=True);p._integer(receipt['revision'],'owned revision')
        validated=self.queue._validate_schedule(request)
        request_sha=hashlib.sha256(canonical(dict(nodeId=node,request=request)).encode()).hexdigest()
        original=dict(receipt);checksum=original.pop('receiptSha256',None)
        if (owned['request_sha256']!=request_sha or receipt.get('requestSha256')!=request_sha
                or checksum!=hashlib.sha256(canonical(original).encode()).hexdigest()
                or receipt.get('requestId')!=payload['ownedScheduleRequestId'] or request.get('requestId')!=payload['ownedScheduleRequestId']
                or receipt.get('authorityId')!=self.queue.authority_id or request.get('authorityId')!=self.queue.authority_id
                or receipt.get('nodeId')!=node or receipt.get('planId')!=payload['planId']
                or receipt.get('targetImage')!=payload['targetImage'] or request.get('targetImage')!=payload['targetImage']
                or receipt['expectedRevision']!=request['expectedRevision'] or receipt['revision']!=request['expectedRevision']+1
                or receipt['drainAt']!=validated[1] or receipt['notBefore']!=validated[2] or receipt['afterJobId']!=request['afterJobId']
                or maintenance['drain_at']!=receipt['drainAt'] or maintenance['not_before']!=receipt['notBefore'] or maintenance['after_job_id']!=receipt['afterJobId']
                or receipt['state'] not in ('scheduled','draining') or maintenance['revision']<=receipt['revision']):raise Conflict('original owned maintenance binding changed')
        return state

    def _probe(self,node,new_version):
        native,token,origin,reader,worker,instance=self._nodes[node];anchor=self._anchors[node]
        binding=RoutingNodeBinding(anchor.engine_id,anchor.gate_generation,new_version)
        deadline=time.monotonic()+6
        while True:
            remaining=deadline-time.monotonic()
            if remaining<=0:raise RuntimeVersionHeld('new_worker_runtime_not_qualified')
            code,body=read_json(origin+HEALTH_PATH,{'Authorization':'Bearer '+reader,'Accept':'application/json','Accept-Encoding':'identity'},context=self.context,timeout=min(3,remaining))
            observation=worker_observation(code,body,node=node,authority_id=self.queue.authority_id,worker_id=worker,instance_id=instance,binding=binding)
            if observation=='ready' and body['awaitingAcceptance']==0:break
            if observation!='busy':raise RuntimeVersionHeld('new_worker_runtime_not_qualified')
            time.sleep(min(.025,max(0,deadline-time.monotonic())))
        query='?'+urlencode({'token':token}) if token else ''
        remaining=deadline-time.monotonic()
        if remaining<=0:raise RuntimeVersionHeld('new_native_runtime_not_qualified')
        code,info=read_json(native+'/info'+query,{'Accept':'application/json','Accept-Encoding':'identity'},context=self.context,timeout=min(3,remaining))
        if (code!=200 or _validate_info(info)['engineVersion']!=new_version or type(info.get('taskQueueCount')) is not int or info['taskQueueCount']!=0):
            raise RuntimeVersionHeld('new_native_runtime_not_qualified')
        return deadline

    def publish(self,node_id,payload,*,update_token):
        self.queue._known_node(node_id);validate_version_request(payload)
        if type(update_token) is not str or not 1<=len(update_token)<=128 or not update_token.isascii():raise Conflict('original maintenance claim required')
        token_sha=hashlib.sha256(update_token.encode()).hexdigest();encoded=_encode(payload);request_sha=request_sha256(node_id,payload)
        if self._catalog is None:raise RuntimeVersionHeld('paired_catalog_not_attached')
        if not self._lock.acquire(timeout=1):raise RuntimeVersionHeld('runtime_publication_busy')
        try:
            with self.queue._db() as db:
                db.execute('BEGIN');old=self._receipt(db,node_id,payload['requestId'])
                if old:
                    row,request,receipt=old
                    if row['request_json']!=encoded or row['request_sha']!=request_sha or not hmac.compare_digest(row['update_token_sha'],token_sha):raise Conflict('runtime request already bound')
                    return receipt
                initial=self._owned(db,node_id,payload,token_sha)
            probe_deadline=self._probe(node_id,payload['newVersion']);self._check()
            with self.queue._db() as db:
                db.execute('BEGIN IMMEDIATE');appeared=self._receipt(db,node_id,payload['requestId'])
                if appeared:
                    row,request,receipt=appeared
                    if row['request_json']!=encoded or row['request_sha']!=request_sha or not hmac.compare_digest(row['update_token_sha'],token_sha):raise Conflict('runtime request already bound')
                    return receipt
                state=self._owned(db,node_id,payload,token_sha)
                if p.digest(initial)!=p.digest(state):raise Conflict('runtime epoch changed during verification')
                if type(probe_deadline) not in (int,float) or not math.isfinite(probe_deadline) or time.monotonic()>=probe_deadline:
                    raise RuntimeVersionHeld('runtime_readback_expired')
                if state['epoch']>=MAX_RECEIPTS:raise RuntimeVersionHeld('runtime_history_capacity_reached')
                self._catalog.invalidate_for_runtime(db)
                state['epoch']+=1
                state['nodes'][node_id].update(engineVersion=payload['newVersion'],targetImage=payload['targetImage'],epoch=state['epoch'],requestId=payload['requestId'])
                snap=self._snapshot(state)
                receipt={key:payload[key] for key in REQUEST_FIELDS}
                receipt.update(nodeId=node_id,requestSha256=request_sha,epoch=state['epoch'],bindingsSha256=snap.bindings_sha256,verificationScope='configured_worker_and_native_http')
                validate_version_receipt(receipt,payload,node_id)
                old_length=db.execute('SELECT length(CAST(payload AS BLOB)) FROM reference_runtime_versions').fetchone()[0]
                self._catalog.store._quota(db,len(_encode(state).encode())-old_length+len(encoded.encode())+len(_encode(receipt).encode())+512)
                db.execute('UPDATE reference_runtime_versions SET payload=?,digest=? WHERE singleton=1',(_encode(state),p.digest(state)))
                row=dict(request_id=payload['requestId'],node_id=node_id,request_sha=request_sha,request_json=encoded,
                    receipt_json=_encode(receipt),receipt_sha=p.digest(receipt),update_token_sha=token_sha,epoch=state['epoch'])
                db.execute('INSERT INTO reference_runtime_version_receipts VALUES(?,?,?,?,?,?,?,?,?)',
                    (*row.values(),_row_digest(row)))
                self._state(db)
                if time.monotonic()>=probe_deadline:raise RuntimeVersionHeld('runtime_readback_expired')
            return receipt
        finally:self._lock.release()
