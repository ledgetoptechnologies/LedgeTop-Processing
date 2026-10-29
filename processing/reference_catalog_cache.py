"""Opt-in metadata admission evidence; never execution or source authority.

State and job evidence live in the authoritative Queue FULL transaction. No
unavailable observation refreshes a timestamp, and no caller supplies a cache.
"""
from dataclasses import dataclass
import ipaddress
import json
import math
import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlsplit,urlencode

from . import reference_protocol as p
from .options import _validate_info,_validate_options,_contains_secret,_sha,_canonical
from .reference_catalog_transport import read_json
from .reference_parent_guard import tls_trust_sha256

MAX_CACHE=1048576
MAX_EVIDENCE=262144
MAX_PAGES=32768
WRITER_HEADER='X-LTDS-Option-Evidence'
CLAIM_HEADER='X-LTDS-Claim-Token'
_SCHEMA={
    'reference_catalog_state':'CREATE TABLE reference_catalog_state(singleton INTEGER PRIMARY KEY CHECK(singleton=1),payload TEXT NOT NULL,digest TEXT NOT NULL,invalidated INTEGER NOT NULL CHECK(invalidated IN (0,1)),pending TEXT NOT NULL CHECK(length(pending)=32),high_water TEXT NOT NULL CHECK(length(high_water)=64),control_sha256 TEXT NOT NULL CHECK(length(control_sha256)=64))',
    'reference_option_evidence':'CREATE TABLE reference_option_evidence(job_id TEXT PRIMARY KEY REFERENCES reference_jobs(job_id),payload TEXT NOT NULL,digest TEXT NOT NULL)',
}
for _op in ('INSERT','UPDATE','DELETE'):
    _SCHEMA['reference_catalog_fence_'+_op.lower()]=f"CREATE TRIGGER reference_catalog_fence_{_op.lower()} BEFORE {_op} ON jobs BEGIN SELECT CASE WHEN reference_catalog_writer_v1()!=1 THEN RAISE(ABORT,'catalog writer required') END; END"

class CatalogHeld(ValueError):pass

_EMPTY_PENDING='0'*32

def _clock_text(value):
    if type(value) not in (int,float) or not math.isfinite(value) or not 0<=value<=p.MAX_SAFE_INTEGER:
        raise CatalogHeld('catalog_clock_unavailable')
    return float(value).hex().ljust(64)

def _control_digest(invalidated,pending,high_water,payload_digest,scope):
    return p.digest(dict(schemaVersion=1,invalidated=invalidated,pending=pending,highWater=high_water,payloadDigest=payload_digest,scope=scope))

def _controls(row,scope):
    if (type(row['invalidated']) is not int or row['invalidated'] not in (0,1)
            or type(row['pending']) is not str or re.fullmatch('[a-f0-9]{32}',row['pending']) is None
            or type(row['high_water']) is not str or len(row['high_water'])!=64):raise CatalogHeld('catalog_control_changed')
    try:high=float.fromhex(row['high_water'].rstrip())
    except ValueError:raise CatalogHeld('catalog_control_changed') from None
    if _clock_text(high)!=row['high_water']:raise CatalogHeld('catalog_control_changed')
    if (type(row['control_sha256']) is not str or row['control_sha256']!=_control_digest(row['invalidated'],row['pending'],row['high_water'],row['digest'],scope)):
        raise CatalogHeld('catalog_control_changed')
    return high

def validate_catalog_abi(db):
    CatalogAdmission._schema(db)
    rows=db.execute('SELECT singleton,length(CAST(payload AS BLOB)),length(digest) FROM reference_catalog_state').fetchall()
    if len(rows)!=1 or tuple(rows[0])[:1]!=(1,) or not 1<=rows[0][1]<=MAX_CACHE+4096 or rows[0][2]!=64:
        raise CatalogHeld('catalog_marker_missing_or_changed')
    row=db.execute('SELECT * FROM reference_catalog_state WHERE singleton=1').fetchone()
    state=_read(row['payload'],MAX_CACHE+4096)
    if type(state.get('schemaVersion')) is not int or state['schemaVersion']!=1 or p.digest(state)!=row['digest']:
        raise CatalogHeld('catalog_marker_changed')
    _controls(row,state.get('scope'))

@dataclass(frozen=True)
class CatalogAdmissionPolicy:
    max_age_seconds:int
    def __post_init__(self):
        if type(self.max_age_seconds) is not int or not 1<=self.max_age_seconds<=86400:
            raise CatalogHeld('bounded_catalog_age_required')

def _encoded(value,maximum):
    raw=p.canonical_json(value)
    if len(raw)>maximum:raise CatalogHeld('catalog_metadata_budget')
    return raw.decode()

def _read(raw,maximum):
    if type(raw) is not str or len(raw.encode())>maximum:raise CatalogHeld('catalog_metadata_budget')
    value=p.decode_submission(raw.encode())
    if _encoded(value,maximum)!=raw:raise CatalogHeld('noncanonical_catalog')
    return value

def _snapshot(value,bindings):
    p._closed(value,('ready','reason','nodes','commonOptions','commonOptionsSha256','conflictingNames','unsupportedByNode'),'cached catalog')
    if value['ready'] is not True or value['reason'] is not None or type(value['nodes']) is not list or len(value['nodes'])!=2:
        raise CatalogHeld('complete_fleet_catalog_required')
    nodes={}
    for node in value['nodes']:
        p._closed(node,('role','configured','available','reason','options','optionsSha256','capabilitiesSha256','version','engine','engineVersion'),'catalog node')
        role=node['role']
        if role not in ('local','main') or role in nodes or node['configured'] is not True or node['available'] is not True or node['reason'] is not None:
            raise CatalogHeld('complete_fleet_catalog_required')
        info=_validate_info(node);definitions=_validate_options(node['options'])
        _encoded(definitions,MAX_EVIDENCE)
        if node['engineVersion']!=bindings['cluster' if role=='local' else role].engine_version:
            raise CatalogHeld('configured_engine_version_changed')
        if node['optionsSha256']!=_sha(sorted(definitions,key=lambda o:o['name'])) or node['capabilitiesSha256']!=_sha({**info,'optionsSha256':node['optionsSha256']}):
            raise CatalogHeld('catalog_digest_changed')
        nodes[role]={o['name']:o for o in definitions}
    common=set(nodes['local'])&set(nodes['main'])
    conflicting=sorted(n for n in common if _canonical(nodes['local'][n])!=_canonical(nodes['main'][n]))
    definitions=[nodes['local'][n] for n in sorted(common-set(conflicting))]
    union=set(nodes['local'])|set(nodes['main'])
    unsupported={role:sorted(union-set(items)) for role,items in nodes.items()}
    if (value['commonOptions']!=definitions or value['commonOptionsSha256']!=_sha(definitions)
            or value['conflictingNames']!=conflicting or value['unsupportedByNode']!=unsupported):
        raise CatalogHeld('catalog_intersection_changed')
    _encoded(value,MAX_CACHE)
    return value

class CatalogAdmission:
    def __init__(self,queue,config,settings,*,ssl_context,store,clock=time.time,runtime_versions=None):
        from .reference_runtime import discovered_policy
        from .reference_routing import RoutingNodeBinding
        policy=settings.catalog_admission
        from .reference_store import ReferenceStore
        if (type(store) is not ReferenceStore or store.queue is not queue or type(policy) is not CatalogAdmissionPolicy or queue.catalog_admission is not True
                or type(settings.routing_bindings) is not dict or set(settings.routing_bindings)!=set(('cluster','main'))
                or any(type(b) is not RoutingNodeBinding for b in settings.routing_bindings.values())):
            raise CatalogHeld('explicit_catalog_binding_required')
        endpoints=[]
        for role,origin,token in (('local',config.local_node_url,config.local_node_token),('main',config.main_node_url,config.main_node_token)):
            parsed=urlsplit(origin)
            if parsed.scheme not in ('http','https') or '%' in (parsed.hostname or ''):raise CatalogHeld('fixed_catalog_origin_required')
            ipaddress.ip_address(parsed.hostname)
            endpoints.append((role,origin.rstrip('/'),token))
        self.queue,self.config,self.settings=queue,config,settings
        if runtime_versions is not None:
            from .reference_runtime_versions import RuntimeVersionRegistry
            if type(runtime_versions) is not RuntimeVersionRegistry or runtime_versions.queue is not queue or runtime_versions.config is not config or runtime_versions.settings is not settings:
                raise CatalogHeld('concrete_original_runtime_registry_required')
        self.runtime_versions=runtime_versions
        self.store=store
        self.policy,self.bindings,self.context,self.clock=policy,dict(settings.routing_bindings),ssl_context,clock
        self.endpoints=tuple(endpoints);self._trust=tls_trust_sha256(ssl_context)
        self._tls_policy=(ssl_context.minimum_version,ssl_context.maximum_version,ssl_context.options,ssl_context.verify_flags,ssl_context.hostname_checks_common_name,
            tuple((c['name'],c['protocol'],c['strength_bits']) for c in ssl_context.get_ciphers()))
        self._bindings_sha=p.digest({n:vars(b) for n,b in self.bindings.items()})
        self._identity=(queue,config,settings,policy,self.endpoints,ssl_context,clock,store,runtime_versions)
        self.scope=p.digest(dict(schemaVersion=1,authorityId=queue.authority_id,
            origins=[(r,o) for r,o,_ in endpoints],bindings={n:vars(b) for n,b in self.bindings.items()},
            workers={n:{k:v for k,v in w.items() if k not in ('bearer','resultBearer')} for n,w in settings.workers.items()},
            privateConfigurationSha256=p.digest(dict(config=vars(config),listener=settings.listener,
                viewerBearers=settings.viewer_bearers,workers=settings.workers,streamBudget=settings.stream_budget,
                tlsRoots=self._trust,tlsPolicy=self._tls_policy)),
            maxAge=policy.max_age_seconds,localMaxPhotos=config.local_max_photos,localMaxInputBytes=config.local_max_input_bytes))
        self._lock=threading.Lock();self._discovered_policy=discovered_policy
        with queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            self._budget(db)
            present=db.execute("SELECT name FROM sqlite_master WHERE name LIKE 'reference_catalog_%' OR name='reference_option_evidence'").fetchall()
            if not present:
                if db.execute("SELECT 1 FROM jobs WHERE status IN ('queued','running','held') LIMIT 1").fetchone():raise CatalogHeld('catalog_migration_requires_drained_queue')
                for sql in _SCHEMA.values():db.execute(sql)
                self._write(db,dict(schemaVersion=1,scope=self.scope,state='invalidated',observedAt=0,highWater=0,snapshot=None,
                    capabilityFingerprint=None,optionsFingerprint=None,allowed=[],catalogSha256=None))
            self._schema(db)
            validate_catalog_abi(db)
            old=self._state(db)
            if old is None or old['scope']!=self.scope:raise CatalogHeld('catalog_scope_migration_required')

    def _check(self):
        if ((self.queue,self.config,self.settings,self.policy,self.endpoints,self.context,self.clock,self.store,self.runtime_versions)!=self._identity
                or tls_trust_sha256(self.context)!=self._trust
                or p.digest({n:vars(b) for n,b in self.bindings.items()})!=self._bindings_sha
                or (self.context.minimum_version,self.context.maximum_version,self.context.options,self.context.verify_flags,self.context.hostname_checks_common_name,
                    tuple((c['name'],c['protocol'],c['strength_bits']) for c in self.context.get_ciphers()))!=self._tls_policy):raise CatalogHeld('catalog_policy_changed')

    @staticmethod
    def _budget(db):
        if db.execute('PRAGMA page_size').fetchone()[0]!=4096 or db.execute('PRAGMA page_count').fetchone()[0]>MAX_PAGES:
            raise CatalogHeld('catalog_database_budget')
        db.execute(f'PRAGMA max_page_count={MAX_PAGES}')

    @staticmethod
    def _schema(db):
        actual={r['name']:r['sql'] for r in db.execute("SELECT name,sql FROM sqlite_master WHERE name LIKE 'reference_catalog_%' OR name='reference_option_evidence'")}
        if actual!=_SCHEMA:raise CatalogHeld('catalog_schema_changed')

    def _state(self,db,owned_pending=None):
        self._schema(db)
        size=db.execute('SELECT length(CAST(payload AS BLOB)) FROM reference_catalog_state WHERE singleton=1').fetchone()
        if size is not None and (type(size[0]) is not int or size[0]>MAX_CACHE+4096):raise CatalogHeld('catalog_metadata_budget')
        row=db.execute('SELECT * FROM reference_catalog_state WHERE singleton=1').fetchone()
        if row is None:return None
        state=_read(row['payload'],MAX_CACHE+4096)
        p._closed(state,('schemaVersion','scope','state','observedAt','highWater','snapshot','capabilityFingerprint','optionsFingerprint','allowed','catalogSha256'),'catalog state')
        if (type(state['schemaVersion']) is not int or state['schemaVersion']!=1 or row['digest']!=p.digest(state) or state['scope']!=self.scope
                or state['state'] not in ('valid','invalidated') or any(type(state[k]) not in (int,float) or not math.isfinite(state[k]) or state[k]<0 for k in ('observedAt','highWater'))
                or state['highWater']<state['observedAt']):raise CatalogHeld('catalog_state_changed')
        high=_controls(row,state['scope'])
        if high<state['highWater']:raise CatalogHeld('catalog_control_changed')
        state=dict(state,highWater=high)
        if row['invalidated'] or row['pending'] not in (_EMPTY_PENDING,owned_pending):state['state']='invalidated'
        if state['state']=='valid':
            _snapshot(state['snapshot'],self._effective_bindings(db))
            cap,opt,allowed=self._discovered_policy(state['snapshot'],self.config)
            if (state['capabilityFingerprint'],state['optionsFingerprint'],state['allowed'],state['catalogSha256'])!=(cap,opt,sorted(allowed),p.digest(state['snapshot'])):
                raise CatalogHeld('catalog_policy_changed')
        return state

    def _write(self,db,value):
        raw=_encoded(value,MAX_CACHE+4096)
        self._budget(db)
        old=db.execute('SELECT length(CAST(payload AS BLOB))+length(digest)+176 FROM reference_catalog_state WHERE singleton=1').fetchone()
        self.store._quota(db,len(raw.encode())+64+176-(old[0] if old else 0))
        # Cache has a single bounded row; job evidence is separately quota charged.
        digest=p.digest(value);invalidated=int(value['state']!='valid');high_water=_clock_text(value['highWater'])
        control=_control_digest(invalidated,_EMPTY_PENDING,high_water,digest,self.scope)
        db.execute('INSERT INTO reference_catalog_state VALUES(1,?,?,?,?,?,?) ON CONFLICT(singleton) DO UPDATE SET payload=excluded.payload,digest=excluded.digest,invalidated=excluded.invalidated,pending=excluded.pending,high_water=excluded.high_water,control_sha256=excluded.control_sha256',
            (raw,digest,invalidated,_EMPTY_PENDING,high_water,control))

    def _update_controls(self,db,*,invalidated,pending,high_water):
        digest=db.execute('SELECT digest FROM reference_catalog_state WHERE singleton=1').fetchone()[0]
        encoded=_clock_text(high_water)
        control=_control_digest(invalidated,pending,encoded,digest,self.scope)
        db.execute('UPDATE reference_catalog_state SET invalidated=?,pending=?,high_water=?,control_sha256=? WHERE singleton=1',
            (invalidated,pending,encoded,control))

    def _effective_bindings(self,db=None):
        return self.runtime_versions.current_bindings(db) if self.runtime_versions is not None else self.bindings

    def _discover_node(self,endpoint,bindings):
        role,origin,token=endpoint;observed=dict(role=role,info=None,options=None)
        query='?'+urlencode({'token':token}) if token else ''
        for route,limit in (('/info',65536),('/options',MAX_EVIDENCE)):
            code,value=read_json(origin+route+query,{'Accept':'application/json','Accept-Encoding':'identity'},context=self.context,timeout=3,max_bytes=limit)
            if code!=200:return ('unavailable' if code=='unavailable' else 'unknown'),None,observed
            try:
                if _contains_secret(value,token):raise CatalogHeld('credential_reflected')
                if route=='/info':
                    info=_validate_info(value);observed['info']=info
                    if info['engineVersion']!=bindings['cluster' if role=='local' else role].engine_version:raise CatalogHeld('engine_version_changed')
                else:
                    definitions=_validate_options(value);_encoded(definitions,MAX_EVIDENCE);observed['options']=definitions
            except (ValueError,TypeError):return 'unknown',None,observed
        option_hash=_sha(sorted(definitions,key=lambda o:o['name']))
        return 'fresh',dict(role=role,configured=True,available=True,reason=None,options=definitions,
            optionsSha256=option_hash,capabilitiesSha256=_sha({**info,'optionsSha256':option_hash}),**info),observed

    def _discover(self,bindings):
        with ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(lambda endpoint:self._discover_node(endpoint,bindings),self.endpoints))
        failures=[outcome for outcome,_,_ in results if outcome!='fresh']
        observed=[item for _,_,item in results]
        if failures:return ('unknown' if 'unknown' in failures else 'unavailable'),None,observed
        nodes=[node for _,node,_ in results]
        byrole={n['role']:{o['name']:o for o in n['options']} for n in nodes}
        common=set(byrole['local'])&set(byrole['main']);union=set(byrole['local'])|set(byrole['main'])
        conflicting=sorted(n for n in common if _canonical(byrole['local'][n])!=_canonical(byrole['main'][n]))
        definitions=[byrole['local'][n] for n in sorted(common-set(conflicting))]
        return 'fresh',_snapshot(dict(ready=True,reason=None,nodes=nodes,commonOptions=definitions,commonOptionsSha256=_sha(definitions),conflictingNames=conflicting,unsupportedByNode={r:sorted(union-set(v)) for r,v in byrole.items()}),bindings),observed

    @staticmethod
    def _known_drift(old,observed):
        if old is None or old['snapshot'] is None:return False
        prior={node['role']:node for node in old['snapshot']['nodes']}
        for item in observed:
            node=prior[item['role']]
            if item['info'] is not None and item['info']!=_validate_info(node):return True
            if item['options'] is not None and _canonical(sorted(item['options'],key=lambda o:o['name']))!=_canonical(sorted(node['options'],key=lambda o:o['name'])):return True
        return False

    def observe(self):
        self._check()
        if not self._lock.acquire(timeout=1):raise CatalogHeld('catalog_observation_busy')
        try:
            pending=uuid.uuid4().hex;before=self.clock();_clock_text(before)
            runtime=self.runtime_versions.snapshot() if self.runtime_versions is not None else None
            bindings=runtime.bindings if runtime else self.bindings
            # Durable constant-size intent BEFORE native I/O. A crash or any
            # later unknown commit leaves a pending marker that closes fallback.
            with self.queue._db() as db:
                db.execute('BEGIN IMMEDIATE');prior=self._state(db)
                if runtime is not None and self.runtime_versions.snapshot(db)!=runtime:raise CatalogHeld('runtime_epoch_changed')
                if prior is None:raise CatalogHeld('catalog_marker_missing')
                self._update_controls(db,invalidated=int(prior['state']!='valid' or before<prior['highWater']),pending=pending,high_water=max(before,prior['highWater']))
            outcome,snapshot,observed=self._discover(bindings);self._check();now=self.clock();_clock_text(now)
            held=None;result=None
            with self.queue._db() as db:
                db.execute('BEGIN IMMEDIATE');old=self._state(db,owned_pending=pending)
                if runtime is not None and self.runtime_versions.snapshot(db)!=runtime:raise CatalogHeld('runtime_epoch_changed')
                actual=db.execute('SELECT pending FROM reference_catalog_state WHERE singleton=1').fetchone()
                if actual is None or actual[0]!=pending:raise CatalogHeld('catalog_observation_ownership_changed')
                if old is not None and now<old['highWater']:outcome='unknown'
                if outcome=='unavailable' and self._known_drift(old,observed):outcome='unknown'
                if outcome=='fresh':
                    cap,opt,allowed=self._discovered_policy(snapshot,self.config)
                    result=dict(schemaVersion=1,scope=self.scope,state='valid',observedAt=now,highWater=now,snapshot=snapshot,
                        capabilityFingerprint=cap,optionsFingerprint=opt,allowed=sorted(allowed),catalogSha256=p.digest(snapshot))
                    self._write(db,result)
                elif old is not None:
                    result=dict(old,highWater=max(now,old['highWater']))
                    if outcome!='unavailable' or old['state']!='valid' or now<old['observedAt'] or now-old['observedAt']>self.policy.max_age_seconds:
                        result['state']='invalidated';held='catalog_invalidated'
                    # These preallocated controls never enlarge the row or
                    # require logical quota headroom to close admission.
                    self._update_controls(db,invalidated=int(result['state']!='valid'),pending=_EMPTY_PENDING,high_water=result['highWater'])
                else:held='verified_catalog_unavailable'
            if held:raise CatalogHeld(held)
            return result,('fresh' if outcome=='fresh' else 'cached')
        finally:self._lock.release()

    def evidence(self,state,submission):
        if state['scope']!=self.scope or state['state']!='valid':raise CatalogHeld('valid_catalog_required')
        expected=submission['pipeline']['odm']
        if (submission['capabilityFingerprint'],expected['optionsFingerprint'])!=(state['capabilityFingerprint'],state['optionsFingerprint']):raise CatalogHeld('stale_catalog')
        definitions={o['name']:o for o in state['snapshot']['commonOptions']}
        selected=[definitions[name] for name in sorted(expected['options'])]
        _encoded(selected,MAX_EVIDENCE)
        return dict(schemaVersion=1,authorityId=self.queue.authority_id,scope=self.scope,
            capabilityFingerprint=state['capabilityFingerprint'],optionsFingerprint=state['optionsFingerprint'],
            catalogSha256=state['catalogSha256'],selectedDefinitions=selected)

    def admit(self,db,submission,catalog_sha):
        self._check();state=self._state(db);now=self.clock()
        if (state is None or state['state']!='valid' or catalog_sha!=state['catalogSha256']
                or type(now) not in (int,float) or not math.isfinite(now) or now<state['highWater']
                or now-state['observedAt']>self.policy.max_age_seconds):raise CatalogHeld('fresh_or_valid_cached_admission_required')
        return self.evidence(state,submission)

    def invalidate_for_runtime(self,db):
        """Close the current catalog inside a qualified publication transaction.

        The registry calls this BEFORE changing accepted versions. It also
        cancels any old observation intent, fencing a late HTTP response.
        """
        self._check();state=self._state(db)
        if state is None:raise CatalogHeld('catalog_marker_missing')
        self._update_controls(db,invalidated=1,pending=_EMPTY_PENDING,high_water=state['highWater'])

    def insert_evidence(self,db,job_id,evidence):
        raw=_encoded(evidence,MAX_EVIDENCE+4096)
        db.execute('INSERT INTO reference_option_evidence VALUES(?,?,?)',(job_id,raw,p.digest(evidence)))

    def read_evidence(self,db,job_id):
        self._schema(db)
        size=db.execute('SELECT length(CAST(payload AS BLOB)) FROM reference_option_evidence WHERE job_id=?',(job_id,)).fetchone()
        if size is None or type(size[0]) is not int or size[0]>MAX_EVIDENCE+4096:raise CatalogHeld('original_option_evidence_budget')
        row=db.execute('SELECT payload,digest FROM reference_option_evidence WHERE job_id=?',(job_id,)).fetchone()
        if row is None:raise CatalogHeld('original_option_evidence_missing')
        value=_read(row['payload'],MAX_EVIDENCE+4096)
        p._closed(value,('schemaVersion','authorityId','scope','capabilityFingerprint','optionsFingerprint','catalogSha256','selectedDefinitions'),'original option evidence')
        if type(value['schemaVersion']) is not int or value['schemaVersion']!=1 or value['scope']!=self.scope or value['authorityId']!=self.queue.authority_id or p.digest(value)!=row['digest']:
            raise CatalogHeld('original_option_evidence_changed')
        for k in ('capabilityFingerprint','optionsFingerprint','catalogSha256'):p._hash(value[k],k)
        _validate_options(value['selectedDefinitions']) if value['selectedDefinitions'] else None
        return value
