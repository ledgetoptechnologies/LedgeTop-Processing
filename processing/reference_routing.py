"""Fresh fixed-origin scheduling observations, never worker/gate admission.

Only explicit unavailable/maintenance may overflow a small job. Unknown TLS,
authentication, malformed state and busy work hold rather than invent idleness.
Actual workers must still acquire their physical gate and verify local admission.
"""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass,field
import errno
import ipaddress
import math
import re
import socket
import select
import ssl
import time
import threading
from types import MappingProxyType
from urllib.parse import urlencode,urlsplit

from .config import Config,_node_origin
from .prototype import Queue
from .reference_parent_guard import tls_trust_sha256
from .reference_protocol import decode_submission,_uuid,_slug

HEALTH_PATH='/ltds/worker/v1/health'
_HEALTH_FIELDS={'schemaVersion','profile','scope','nodeId','state','reason','driverAlive','listenerAlive','workState','retainedJobs','pendingJobs','awaitingAcceptance','identityScope','authorityId','workerId','workerInstanceId','engineId','gateGeneration','engineVersion'}

class RoutingHeld(ValueError):pass
@dataclass(frozen=True)
class RoutingNodeBinding:
    engine_id:str
    gate_generation:int
    engine_version:str
    def __post_init__(self):
        if (type(self.engine_id) is not str or re.fullmatch('[A-Za-z0-9][A-Za-z0-9:._-]{0,255}',self.engine_id) is None
                or type(self.gate_generation) is not int or not 1<=self.gate_generation<=2**53-1
                or type(self.engine_version) is not str or re.fullmatch('[A-Za-z0-9][A-Za-z0-9_.+-]{0,127}',self.engine_version) is None):
            raise RoutingHeld('fixed_configured_routing_identity_required')

@dataclass(frozen=True)
class Observation:
    state:str
    max_images:int|None=None

@dataclass(frozen=True,repr=False)
class _Endpoint:
    native:str
    native_token:str=field(repr=False)
    worker:str
    reader:str=field(repr=False)
    worker_id:str
    instance_id:str
    binding:RoutingNodeBinding

@dataclass(frozen=True)
class _Sample:
    owner:object=field(repr=False)
    at:float
    nodes:object
    runtime_epoch:int|None=None
    runtime_digest:str|None=None

def worker_observation(code,body,*,node,authority_id,worker_id,instance_id,binding):
    if code=='unavailable':return 'unavailable'
    if (code not in (200,503) or type(body) is not dict or set(body)!=_HEALTH_FIELDS
            or type(body['schemaVersion']) is not int or body['schemaVersion']!=1
            or (body['nodeId'],body['profile'],body['scope'])!=(node,'odm_only','service_liveness')
            or (body['identityScope'],body['authorityId'],body['workerId'],body['workerInstanceId'],body['engineId'],body['gateGeneration'],body['engineVersion'])!=('configured_service',authority_id,worker_id,instance_id,binding.engine_id,binding.gate_generation,binding.engine_version)
            or type(body['gateGeneration']) is not int
            or body['state'] not in ('starting','ready','failed','draining','stopped')
            or body['reason'] is not None and (type(body['reason']) is not str or re.fullmatch('[a-z][a-z0-9_]{0,79}',body['reason']) is None)
            or any(type(body[k]) is not bool for k in ('driverAlive','listenerAlive'))):return 'unknown'
    counts=[body[k] for k in ('retainedJobs','pendingJobs','awaitingAcceptance')]
    if body['workState']=='busy':return 'busy' if counts==[None,None,None] else 'unknown'
    if (any(type(n) is not int or not 0<=n<=1000000 for n in counts) or counts[0]<counts[1]+counts[2]):return 'unknown'
    retained,pending,awaiting=counts
    expected='empty' if not retained else 'pending_work' if pending else 'awaiting_acceptance' if awaiting else 'idle'
    if body['workState']!=expected:return 'unknown'
    ready=body['state']=='ready' and body['driverAlive'] and body['listenerAlive']
    if (code==200)!=ready:return 'unknown'
    if pending:return 'busy'
    return 'ready' if ready else 'unavailable'

class ReferenceRouting:
    def __init__(self,config,workers,*,authority_id,bindings,ssl_context,timeout=2,runtime_versions=None):
        if (type(config) is not Config or config.reference_main_fallback is not True
                or set(workers)!=set(('cluster','main')) or set(config.prototype_node_ids)!=set(workers)):
            raise RoutingHeld('explicit_two_node_routing_policy_required')
        if (not isinstance(ssl_context,ssl.SSLContext) or not ssl_context.check_hostname
                or ssl_context.verify_mode!=ssl.CERT_REQUIRED or type(timeout) not in (int,float)
                or not math.isfinite(timeout) or not 0<timeout<=3):raise RoutingHeld('verified_bounded_worker_transport_required')
        _uuid(authority_id,'routing authority')
        if type(bindings) is not dict or set(bindings)!=set(('cluster','main')):raise RoutingHeld('independent_routing_bindings_required')
        nodes={}
        for node,origin,token in (('cluster',config.local_node_url,config.local_node_token),('main',config.main_node_url,config.main_node_token)):
            worker=workers[node];native=_node_origin(origin);remote=_node_origin(worker['resultOrigin']);reader=worker['resultBearer']
            if not native or not remote.startswith('https://'):raise RoutingHeld('fixed_node_origins_required')
            try:
                for origin in (native,remote):
                    parsed=urlsplit(origin)
                    if '%' in parsed.hostname:raise ValueError()
                    ipaddress.ip_address(parsed.hostname)
            except ValueError:raise RoutingHeld('literal_ip_routing_origins_required') from None
            if (type(reader) is not str or not 16<=len(reader)<=4096 or any(ord(c)<33 or ord(c)>126 for c in reader)
                    or type(token) is not str or len(token)>4096 or any(ord(c)<33 or ord(c)>126 for c in token)):
                raise RoutingHeld('fixed_read_credentials_required')
            binding=bindings[node]
            if type(binding) is not RoutingNodeBinding:raise RoutingHeld('concrete_routing_binding_required')
            RoutingNodeBinding(**vars(binding))
            Queue.slug(worker['workerId']);_slug(worker['instanceId'],'routing worker instance')
            nodes[node]=_Endpoint(native,token,remote,reader,worker['workerId'],worker['instanceId'],binding)
        self._nodes=MappingProxyType(nodes);self.timeout=timeout;self._tls=ssl_context;self.authority_id=authority_id
        if runtime_versions is not None:
            from .reference_runtime_versions import RuntimeVersionRegistry
            if type(runtime_versions) is not RuntimeVersionRegistry or runtime_versions.queue.authority_id!=authority_id:
                raise RoutingHeld('concrete_original_runtime_registry_required')
        self.runtime_versions=runtime_versions
        self._trust=self._tls_identity()
        self._identity=(self._nodes,self.timeout,self._tls,self.authority_id,self.runtime_versions)
        self._seal=object();self._sampling=threading.BoundedSemaphore(1)

    def _tls_identity(self):
        context=self._tls
        try:
            return (tls_trust_sha256(context),context.minimum_version,context.maximum_version,
                context.options,context.verify_flags,context.hostname_checks_common_name,
                tuple((c['name'],c['protocol'],c['strength_bits']) for c in context.get_ciphers()))
        except Exception:raise RoutingHeld('routing_tls_policy_changed') from None

    def _check(self):
        if ((self._nodes,self.timeout,self._tls,self.authority_id,self.runtime_versions)!=self._identity
                or self._tls_identity()!=self._trust):raise RoutingHeld('routing_transport_changed')

    def _json(self,url,headers):
        from .reference_catalog_transport import read_json
        return read_json(url,headers,context=self._tls,timeout=self.timeout)

    def _worker(self,node,endpoint,binding=None):
        code,body=self._json(endpoint.worker+HEALTH_PATH,{'Authorization':'Bearer '+endpoint.reader,'Accept':'application/json','Accept-Encoding':'identity'})
        return worker_observation(code,body,node=node,authority_id=self.authority_id,worker_id=endpoint.worker_id,instance_id=endpoint.instance_id,binding=binding or endpoint.binding)

    def _native(self,node,binding=None):
        endpoint=self._nodes[node]
        query='?'+urlencode({'token':endpoint.native_token}) if endpoint.native_token else ''
        code,info=self._json(endpoint.native+'/info'+query,{'Accept':'application/json','Accept-Encoding':'identity'})
        if code=='unavailable':return Observation('unavailable')
        if (code!=200 or type(info) is not dict or type(info.get('taskQueueCount')) is not int or info['taskQueueCount']<0
                or any(type(info.get(k)) is not str or not 1<=len(info[k])<=128 or any(ord(c)<33 or ord(c)>126 for c in info[k]) for k in ('engine','engineVersion','version'))
                or 'maxImages' not in info or info['maxImages'] is not None and (type(info['maxImages']) is not int or not 1<=info['maxImages']<=2**53-1)):
            return Observation('unknown')
        if info['engineVersion']!=(binding or endpoint.binding).engine_version:return Observation('unknown')
        return Observation('busy' if info['taskQueueCount'] else 'ready',info['maxImages'])

    def _node(self,node,binding=None):
        # Both independent channels must be observed. A stopped daemon is not
        # evidence that its still-running engine is idle or correctly bound.
        worker=self._worker(node,self._nodes[node],binding);native=self._native(node,binding)
        states=(worker,native.state)
        for state in ('unknown','busy','unavailable'):
            if state in states:return Observation(state,native.max_images)
        return Observation('ready',native.max_images)

    def sample(self):
        self._check()
        if not self._sampling.acquire(timeout=2*self.timeout+1):raise RoutingHeld('routing_sample_capacity_unavailable')
        try:
            at=time.monotonic()
            runtime=self.runtime_versions.snapshot() if self.runtime_versions is not None else None
            with ThreadPoolExecutor(max_workers=2) as pool:
                values=dict(zip(('cluster','main'),pool.map(lambda node:self._node(node,runtime.bindings[node] if runtime else None),('cluster','main'))))
            self._check()
            return _Sample(self._seal,at,MappingProxyType(values),runtime.epoch if runtime else None,runtime.bindings_sha256 if runtime else None)
        finally:self._sampling.release()

    def choose(self,submission,states,sample,*,local_max_photos,local_max_input_bytes,main_node_id,db=None):
        self._check()
        if (type(sample) is not _Sample or sample.owner is not self._seal or not 0<=time.monotonic()-sample.at<=4*self.timeout+1):
            raise RoutingHeld('fresh_fixed_route_observation_required')
        if self.runtime_versions is not None:
            runtime=self.runtime_versions.snapshot(db)
            if (sample.runtime_epoch,sample.runtime_digest)!=(runtime.epoch,runtime.bindings_sha256):raise RoutingHeld('runtime_epoch_changed')
        if submission['pipeline']['treedar']['requested']:
            # This liveness profile is ODM-only; retain existing main-only TreeDar
            # routing. Its separate adapter still controls actual admission.
            return main_node_id
        count=submission['manifest']['inputCounts']['images'];size=submission['manifest']['totalBytes']
        def state(node):return states[node] in ('active','scheduled')
        def eligible(node):
            observation=sample.nodes[node]
            return state(node) and observation.state=='ready' and (observation.max_images is None or count<=observation.max_images)
        if count>local_max_photos or size>local_max_input_bytes:
            return main_node_id if eligible('main') else None
        if eligible('cluster'):return 'cluster'
        local=sample.nodes['cluster']
        if local.state in ('unknown','busy'):return None
        unavailable=(not state('cluster') or local.state=='unavailable'
            or local.state=='ready' and local.max_images is not None and count>local.max_images)
        return main_node_id if unavailable and eligible('main') else None
