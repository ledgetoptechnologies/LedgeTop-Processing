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

class ReferenceRouting:
    def __init__(self,config,workers,*,authority_id,bindings,ssl_context,timeout=2):
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
        self._trust=self._tls_identity()
        self._identity=(self._nodes,self.timeout,self._tls,self.authority_id)
        self._seal=object();self._sampling=threading.BoundedSemaphore(1)

    def _tls_identity(self):
        context=self._tls
        try:
            return (tls_trust_sha256(context),context.minimum_version,context.maximum_version,
                context.options,context.verify_flags,context.hostname_checks_common_name,
                tuple((c['name'],c['protocol'],c['strength_bits']) for c in context.get_ciphers()))
        except Exception:raise RoutingHeld('routing_tls_policy_changed') from None

    def _check(self):
        if ((self._nodes,self.timeout,self._tls,self.authority_id)!=self._identity
                or self._tls_identity()!=self._trust):raise RoutingHeld('routing_transport_changed')

    def _json(self,url,headers):
        """Literal-IP nonblocking TCP/TLS/HTTP under ONE absolute deadline.

        Like the existing gate bridge's fixed local HTTP parser, bounds cover
        headers as well as body; verified TLS and fixed IP remove DNS ambiguity.
        No timeout thread, redirects, proxies, decompression or chunked replay.
        """
        connection=None;connected=False;deadline=time.monotonic()+self.timeout
        try:
            parsed=urlsplit(url);address=ipaddress.ip_address(parsed.hostname)
            connection=socket.socket(socket.AF_INET6 if address.version==6 else socket.AF_INET,socket.SOCK_STREAM)
            connection.setblocking(False)
            def wait(read):
                remaining=deadline-time.monotonic()
                if remaining<=0:raise TimeoutError()
                readable,writable,_=select.select([connection] if read else [],[] if read else [connection],[],remaining)
                if not readable and not writable or time.monotonic()>=deadline:raise TimeoutError()
            port=parsed.port or (443 if parsed.scheme=='https' else 80)
            result=connection.connect_ex((str(address),port))
            if result not in (0,errno.EISCONN):
                if result not in (errno.EINPROGRESS,errno.EWOULDBLOCK,errno.EALREADY):raise OSError(result,'connect refused')
                wait(False);result=connection.getsockopt(socket.SOL_SOCKET,socket.SO_ERROR)
                if result:raise OSError(result,'connect refused')
            connected=True
            def io(operation,read):
                while True:
                    if time.monotonic()>=deadline:raise TimeoutError()
                    try:return operation()
                    except ssl.SSLWantReadError:wait(True)
                    except ssl.SSLWantWriteError:wait(False)
                    except BlockingIOError:wait(read)
            if parsed.scheme=='https':
                connection=self._tls.wrap_socket(connection,server_hostname=str(address),do_handshake_on_connect=False)
                connection.setblocking(False);io(connection.do_handshake,True)
            host='['+str(address)+']' if address.version==6 else str(address)
            route=parsed.path or '/'
            if parsed.query:route+='?'+parsed.query
            lines=['GET '+route+' HTTP/1.1','Host: '+host+':'+str(port),'Connection: close']
            lines.extend(key+': '+value for key,value in headers.items())
            request=('\r\n'.join(lines)+'\r\n\r\n').encode('ascii')
            while request:
                sent=io(lambda:connection.send(request),False)
                if sent<=0:raise ValueError()
                request=request[sent:]
            data=bytearray()
            while b'\r\n\r\n' not in data:
                chunk=io(lambda:connection.recv(1024),True)
                if not chunk:raise ValueError()
                data.extend(chunk)
                if len(data)>8192+16384 or b'\r\n\r\n' not in data and len(data)>8192:raise ValueError()
            head,body=bytes(data).split(b'\r\n\r\n',1)
            if len(head)>8192:raise ValueError()
            rows=head.decode('ascii').split('\r\n')
            if re.fullmatch(r'HTTP/1\.[01] [1-5][0-9]{2}(?: [^\r\n]*)?',rows[0]) is None:raise ValueError()
            code=int(rows[0].split(' ')[1]);fields={}
            for row in rows[1:]:
                key,value=row.split(':',1);key=key.lower()
                if key in fields or re.fullmatch('[a-z0-9-]+',key) is None:raise ValueError()
                fields[key]=value.strip()
            if (fields.get('content-type','').split(';')[0].strip().lower()!='application/json'
                    or fields.get('content-encoding','identity').lower()!='identity' or 'transfer-encoding' in fields
                    or re.fullmatch('[0-9]{1,8}',fields.get('content-length','')) is None):raise ValueError()
            length=int(fields['content-length'])
            if length>16384 or len(body)>length:raise ValueError()
            while len(body)<length:
                chunk=io(lambda:connection.recv(min(4096,length-len(body))),True)
                if not chunk:raise ValueError()
                body+=chunk
            if time.monotonic()>=deadline:raise TimeoutError()
            return code,decode_submission(body)
        except (ssl.SSLError,ValueError):return 'unknown',None
        except OSError as error:
            unavailable=not connected and (isinstance(error,TimeoutError) or error.errno in
                (errno.ECONNREFUSED,errno.EHOSTUNREACH,errno.ENETUNREACH,errno.ECONNRESET,errno.ETIMEDOUT))
            return ('unavailable' if unavailable else 'unknown'),None
        except Exception:return 'unknown',None
        finally:
            if connection is not None:connection.close()

    def _worker(self,node,endpoint):
        code,body=self._json(endpoint.worker+HEALTH_PATH,{'Authorization':'Bearer '+endpoint.reader,'Accept':'application/json','Accept-Encoding':'identity'})
        if code=='unavailable':return 'unavailable'
        if (code not in (200,503) or type(body) is not dict or set(body)!=_HEALTH_FIELDS
                or type(body['schemaVersion']) is not int or body['schemaVersion']!=1
                or (body['nodeId'],body['profile'],body['scope'])!=(node,'odm_only','service_liveness')
                or (body['identityScope'],body['authorityId'],body['workerId'],body['workerInstanceId'],body['engineId'],body['gateGeneration'],body['engineVersion'])!=('configured_service',self.authority_id,endpoint.worker_id,endpoint.instance_id,endpoint.binding.engine_id,endpoint.binding.gate_generation,endpoint.binding.engine_version)
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

    def _native(self,node):
        endpoint=self._nodes[node]
        query='?'+urlencode({'token':endpoint.native_token}) if endpoint.native_token else ''
        code,info=self._json(endpoint.native+'/info'+query,{'Accept':'application/json','Accept-Encoding':'identity'})
        if code=='unavailable':return Observation('unavailable')
        if (code!=200 or type(info) is not dict or type(info.get('taskQueueCount')) is not int or info['taskQueueCount']<0
                or any(type(info.get(k)) is not str or not 1<=len(info[k])<=128 or any(ord(c)<33 or ord(c)>126 for c in info[k]) for k in ('engine','engineVersion','version'))
                or 'maxImages' not in info or info['maxImages'] is not None and (type(info['maxImages']) is not int or not 1<=info['maxImages']<=2**53-1)):
            return Observation('unknown')
        if info['engineVersion']!=endpoint.binding.engine_version:return Observation('unknown')
        return Observation('busy' if info['taskQueueCount'] else 'ready',info['maxImages'])

    def _node(self,node):
        # Both independent channels must be observed. A stopped daemon is not
        # evidence that its still-running engine is idle or correctly bound.
        worker=self._worker(node,self._nodes[node]);native=self._native(node)
        states=(worker,native.state)
        for state in ('unknown','busy','unavailable'):
            if state in states:return Observation(state,native.max_images)
        return Observation('ready',native.max_images)

    def sample(self):
        self._check()
        if not self._sampling.acquire(timeout=2*self.timeout+1):raise RoutingHeld('routing_sample_capacity_unavailable')
        try:
            at=time.monotonic()
            with ThreadPoolExecutor(max_workers=2) as pool:
                values=dict(zip(('cluster','main'),pool.map(self._node,('cluster','main'))))
            self._check()
            return _Sample(self._seal,at,MappingProxyType(values))
        finally:self._sampling.release()

    def choose(self,submission,states,sample,*,local_max_photos,local_max_input_bytes,main_node_id):
        self._check()
        if (type(sample) is not _Sample or sample.owner is not self._seal or not 0<=time.monotonic()-sample.at<=4*self.timeout+1):
            raise RoutingHeld('fresh_fixed_route_observation_required')
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
