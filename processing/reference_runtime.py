"""Explicit optional TLS reference listener sharing the native queue authority.

Configuration is a private JSON file, never environment-serialized credentials.
No certificates, credentials, worker daemons, or cleanup are created here. The
initial discovered allowlist admits numeric/boolean options only; string enums
and CRS/EPSG string settings require a later typed option-value contract.
"""
from contextlib import contextmanager
from dataclasses import dataclass
import os
from pathlib import Path
import re
import ssl
import stat
import threading

from . import reference_protocol as p
from .options import catalog
from .reference_api import ReferenceAPI, Response
from .reference_control import ReferenceControlAPI
from .reference_http import create_reference_http_server
from .reference_result_proxy import ResultProxy
from .reference_store import ReferenceStore
from .reference_tls import verified_client_context
from .reference_recovery import RecoveryNodeBinding

MAX_CONFIG_BYTES = 64 * 1024
# This draft schema has not been qualified for upgrade, rollback, or restore.
# Tests may patch this in an isolated fixture; production must remain closed.
STAGED_ACK_SCHEMA_QUALIFIED = False
VIEWER_ARCHIVE_SPOOL_SCHEMA_QUALIFIED = False
VIEWER_RUNTIME_PROJECTION_QUALIFIED = False
SYNTHETIC_CHUNK_TLS_QUALIFIED = False
REFERENCE_CONNECTION_LIMIT_QUALIFIED = False
_UNSAFE = re.compile(r"\b(file|filename|path|directory|folder|url|uri|endpoint|host|hostname|"
                     r"token|credential|secret|password|key|script|command|upload|download)\b", re.I)


class RuntimeConfigurationError(ValueError):
    """Bounded startup error, without private configuration or file contents."""


@dataclass(frozen=True, repr=False)
class ReferenceSettings:
    listener: dict
    viewer_bearers: dict
    workers: dict
    stream_budget: dict
    recovery_bindings: dict | None = None
    staged_ack_bearers: dict | None = None
    viewer_archive_spool: dict | None = None
    viewer_projection_environments: tuple | None = None


def _spool_policy(value):
    p._closed(value,('root','rootIdentity','maxArchiveBytes'),'Viewer archive spool')
    if type(value['root']) is not str:raise RuntimeConfigurationError('explicit spool root required')
    p._integer(value['maxArchiveBytes'],'spool archive limit')
    from .direct_output_slot import _open_root
    fd=_open_root(value['root'],value['rootIdentity'])
    try:return p.decode_submission(p.canonical_json(value))
    finally:os.close(fd)


def _preflight_spool_settings(settings,config,queue,policy):
    """Validate direct settings and existing spool state without schema writes."""
    if settings.recovery_bindings is not None:
        raise RuntimeConfigurationError('archive schema cannot combine recovery policies')
    if settings.staged_ack_bearers is not None and not STAGED_ACK_SCHEMA_QUALIFIED:
        raise RuntimeConfigurationError('staged acknowledgment schema is not qualified')
    listener=settings.listener
    p._closed(listener,('host','port','certificate','privateKey','clientCA'),'TLS listener')
    if (type(listener['host']) is not str or not listener['host'] or any(ord(c)<=32 for c in listener['host'])
            or type(listener['port']) is not int or not 0<=listener['port']<=65535):
        raise RuntimeConfigurationError('invalid TLS listener address')
    for field in ('certificate','privateKey','clientCA'):_file(listener[field],private=field=='privateKey')
    viewers=settings.viewer_bearers;workers=settings.workers;budget=settings.stream_budget
    if type(viewers) is not dict or not viewers or type(workers) is not dict or set(workers)!=set(queue.node_ids):
        raise RuntimeConfigurationError('credential coverage invalid')
    credentials=[]
    for token,env in viewers.items():
        p._environment(env);credentials.append(token)
    from .reference_result_proxy import _origin
    for node,worker in workers.items():
        p._closed(worker,('bearer','workerId','instanceId','resultOrigin','resultBearer'),'reference worker')
        queue._known_node(node);queue.slug(worker['workerId']);p._slug(worker['instanceId'],'worker instance ID')
        _origin(worker['resultOrigin']);credentials.extend((worker['bearer'],worker['resultBearer']))
    if settings.staged_ack_bearers is not None:
        import base64
        principals=settings.staged_ack_bearers
        if type(principals) is not dict or not 1<=len(principals)<=32 or not config.viewer_enabled:
            raise RuntimeConfigurationError('staged principals invalid')
        environments={}
        for token,binding in principals.items():
            p._closed(binding,('environment','principalId'),'staged principal')
            env,principal=binding['environment'],binding['principalId']
            p._environment(env);p._slug(principal,'staged principal')
            if env not in viewers.values() or (principal in environments and environments[principal]!=env):
                raise RuntimeConfigurationError('staged principal environment changed')
            environments[principal]=env
            if type(token) is not str or not re.fullmatch(r'[A-Za-z0-9_-]{43}',token):
                raise RuntimeConfigurationError('invalid staged credential')
            raw=base64.urlsafe_b64decode(token+'=')
            if len(raw)!=32 or base64.urlsafe_b64encode(raw).decode().rstrip('=')!=token:
                raise RuntimeConfigurationError('invalid staged credential')
            credentials.append(token)
        with queue._db() as db:
            from .viewer_staged_ack_schema import validate_staged_ack_schema
            validate_staged_ack_schema(db)
    legacy={secret for secret in (config.api_token,config.prototype_operator_token,config.prototype_observer_token,
            config.viewer_token,config.viewer_staging_token,config.local_node_token,config.main_node_token) if secret}
    if (any(type(token) is not str or not 32<=len(token)<=4096 or any(ord(c)<33 or ord(c)>126 for c in token)
            for token in credentials) or len(set(credentials))!=len(credentials) or set(credentials)&legacy):
        raise RuntimeConfigurationError('credential scope invalid')
    p._closed(budget,('maxConcurrent','maxObjectBytes'),'stream budget')
    if type(budget['maxConcurrent']) is not int or not 1<=budget['maxConcurrent']<=16:
        raise RuntimeConfigurationError('stream budget invalid')
    p._integer(budget['maxObjectBytes'],'stream object budget')
    if 'reference-pipeline-v1' not in queue.allowed_profiles or 'main' not in queue.node_ids:
        raise RuntimeConfigurationError('reference queue unavailable')
    with queue._db() as db:
        names={row['name'] for row in db.execute("SELECT name FROM sqlite_master WHERE name LIKE 'viewer_worker_%'")}
        if names:
            from .viewer_worker_claim import ViewerWorkerClaims
            ViewerWorkerClaims._schema(db)
            old=db.execute('SELECT policy FROM viewer_worker_spool_config WHERE singleton=1').fetchone()
            value=p.canonical_json(dict(root=policy['root'],rootIdentity=policy['rootIdentity'],maxArchiveBytes=policy['maxArchiveBytes'])).decode()
            if old and old['policy']!=value:raise RuntimeConfigurationError('spool policy changed')
            if not old and db.execute('SELECT 1 FROM viewer_worker_spool LIMIT 1').fetchone():
                raise RuntimeConfigurationError('spool policy missing')


def _file(path, *, private):
    if type(path) is not str or not path or not Path(path).is_absolute():
        raise RuntimeConfigurationError("reference files require explicit absolute paths")
    candidate = Path(path).absolute()
    if candidate.resolve() != candidate or any(part.is_symlink() for part in (candidate, *candidate.parents)):
        raise RuntimeConfigurationError("reference file paths cannot contain symlinks")
    info = candidate.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise RuntimeConfigurationError("reference files must be single-link regular files")
    if private and os.name != "nt" and (info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) & 0o077):
        raise RuntimeConfigurationError("reference configuration and key must be owner-private")
    # Windows ACL privacy is the provisioner's responsibility; no ACL is changed.
    return candidate, info


def load_reference_settings(path, config):
    """Read and validate the explicit opt-in, before opening the shared Queue."""
    try:
        candidate, initial = _file(path, private=True)
        descriptor = os.open(candidate, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            observed = os.fstat(descriptor)
            if (observed.st_dev, observed.st_ino) != (initial.st_dev, initial.st_ino):
                raise RuntimeConfigurationError("reference configuration identity changed")
            with os.fdopen(descriptor, "rb", closefd=False) as source:
                raw = source.read(MAX_CONFIG_BYTES + 1)
        finally:
            os.close(descriptor)
        if len(raw) > MAX_CONFIG_BYTES:
            raise RuntimeConfigurationError("reference configuration exceeds size limit")
        value = p.decode_submission(raw)
        if type(value.get("schemaVersion")) is not int or value["schemaVersion"] not in (1, 2, 3, 4, 5, 6):
            raise RuntimeConfigurationError("unsupported runtime configuration version")
        fields=("schemaVersion", "listener", "viewerBearers", "workers", "streamBudget")
        has_recovery=value['schemaVersion']==2 or (value['schemaVersion']==3 and 'recoveryBindings' in value)
        p._closed(value, fields + (("recoveryBindings",) if has_recovery else ()) + (("stagedAckPrincipals",) if value["schemaVersion"] in (3,5,6) else ()) + (("viewerArchiveSpool",) if value["schemaVersion"] in (4,5,6) else ()) + (("viewerProjectionEnvironments",) if value["schemaVersion"]==6 else ()), "runtime configuration")
        listener = value["listener"]
        p._closed(listener, ("host", "port", "certificate", "privateKey", "clientCA"), "TLS listener")
        if (type(listener["host"]) is not str or not listener["host"] or
                any(ord(c) <= 32 for c in listener["host"]) or type(listener["port"]) is not int
                or not 1 <= listener["port"] <= 65535):
            raise RuntimeConfigurationError("invalid TLS listener address")
        for field in ("certificate", "privateKey", "clientCA"):
            _file(listener[field], private=field == "privateKey")
        if (not config.prototype_state_dir or not config.local_node_url or not config.main_node_url
                or "main" not in config.prototype_node_ids):
            raise RuntimeConfigurationError("reference runtime requires persistent shared state and both configured nodes")
        viewers = value["viewerBearers"]
        workers = value["workers"]
        if type(viewers) is not dict or not viewers or not set(viewers) <= {"production", "staging"}:
            raise RuntimeConfigurationError("explicit scoped reference Viewer credentials required")
        if type(workers) is not dict or set(workers) != set(config.prototype_node_ids):
            raise RuntimeConfigurationError("reference workers must exactly cover configured queue nodes")
        legacy = {secret for secret in (config.api_token, config.prototype_operator_token,
            config.prototype_observer_token, config.viewer_token, config.viewer_staging_token,
            config.local_node_token, config.main_node_token) if secret}
        credentials = []
        for token in viewers.values():
            credentials.append(token)
        for node, worker in workers.items():
            p._closed(worker, ("bearer", "workerId", "instanceId", "resultOrigin", "resultBearer"), "reference worker")
            config_node = node
            if config_node not in config.prototype_node_ids:
                raise RuntimeConfigurationError("unknown reference worker node")
            # Match each downstream boundary's own identifier grammar.
            from .prototype import Queue
            Queue.slug(worker["workerId"])
            p._slug(worker["instanceId"], "worker instance ID")
            from .reference_result_proxy import _origin
            _origin(worker["resultOrigin"])
            credentials.extend((worker["bearer"], worker["resultBearer"]))
        for token in credentials:
            if (type(token) is not str or not 32 <= len(token) <= 4096
                    or any(ord(c) < 33 or ord(c) > 126 for c in token)):
                raise RuntimeConfigurationError("reference credentials must be separate bounded printable secrets")
        staged=None
        if value['schemaVersion'] in (3,5,6):
            import base64
            staged_config=value['stagedAckPrincipals']
            if type(staged_config) is not list or not 1<=len(staged_config)<=32:
                raise RuntimeConfigurationError('bounded staged principals required')
            staged={}
            principal_env={}
            for entry in staged_config:
                p._closed(entry,('principalId','environment','bearer'),'staged principal')
                env,token,principal=entry['environment'],entry['bearer'],entry['principalId']
                p._slug(principal,'staged principal');p._environment(env)
                if env not in viewers or (principal in principal_env and principal_env[principal]!=env):
                    raise RuntimeConfigurationError('staged principal environment changed')
                principal_env[principal]=env
                if type(token) is not str or not re.fullmatch(r'[A-Za-z0-9_-]{43}',token):raise RuntimeConfigurationError('invalid staged credential')
                raw=base64.urlsafe_b64decode(token+'=')
                if len(raw)!=32 or base64.urlsafe_b64encode(raw).decode().rstrip('=')!=token:raise RuntimeConfigurationError('invalid staged credential')
                staged[token]={'environment':env,'principalId':principal};credentials.append(token)
            if not config.viewer_enabled:raise RuntimeConfigurationError('native Viewer service required')
        if len(set(credentials)) != len(credentials) or set(credentials) & legacy:
            raise RuntimeConfigurationError("reference credentials must be disjoint from existing credentials")
        budget = value["streamBudget"]
        p._closed(budget, ("maxConcurrent", "maxObjectBytes"), "stream budget")
        if type(budget["maxConcurrent"]) is not int or not 1 <= budget["maxConcurrent"] <= 16:
            raise RuntimeConfigurationError("invalid stream concurrency budget")
        p._integer(budget["maxObjectBytes"], "stream object budget")
        recovery = None
        if has_recovery:
            configured=value["recoveryBindings"]
            if type(configured) is not dict or set(configured)!=set(config.prototype_node_ids):
                raise RuntimeConfigurationError("recovery bindings must exactly cover configured nodes")
            recovery={}
            for node,physical in configured.items():
                p._closed(physical,("engineId","authorityId","coordinatorOrigin","gateGeneration","outputBudget"),"trusted recovery binding")
                recovery[node]=RecoveryNodeBinding(node,physical["engineId"],physical["authorityId"],
                    physical["coordinatorOrigin"],physical["gateGeneration"],physical["outputBudget"])
            if len({binding.authority_id for binding in recovery.values()})!=1:
                raise RuntimeConfigurationError("recovery nodes must share one configured authority")
        spool=None
        if value['schemaVersion'] in (4,5,6):
            if not config.viewer_worker_claim_enabled:raise RuntimeConfigurationError('Viewer worker claims required')
            spool=_spool_policy(value['viewerArchiveSpool'])
        projection=None
        if value['schemaVersion']==6:
            if value['viewerProjectionEnvironments']!=['staging'] or 'staging' not in viewers:
                raise RuntimeConfigurationError('staging projection scope required')
            projection=('staging',)
        return ReferenceSettings(dict(listener), {token: env for env, token in viewers.items()},
                                 {node: dict(worker) for node, worker in workers.items()}, dict(budget), recovery, staged, spool, projection)
    except Exception:
        raise RuntimeConfigurationError("invalid private reference runtime configuration") from None


def discovered_policy(snapshot, config):
    """Build opaque identities from actual fleet definitions and routing policy."""
    if type(snapshot) is not dict or snapshot.get("ready") is not True:
        raise RuntimeConfigurationError("configured engine catalog unavailable")
    options = []
    for option in snapshot["commonOptions"]:
        name = option.get("name")
        if (type(name) is not str or not re.fullmatch(r"[a-z][a-z0-9-]{0,79}", name)
                or option.get("type") not in ("bool", "boolean", "int", "integer", "float", "double", "number")
                or _UNSAFE.search(p.canonical_json(option).decode("utf-8"))):
            continue
        options.append(option)
    options.sort(key=lambda option: option["name"])
    options_fingerprint = p.digest(dict(protocol="reference-options-v1", definitions=options))
    capability = p.digest(dict(protocol="reference-capabilities-v1", options=options_fingerprint,
        nodes=[{key: node.get(key) for key in ("role", "available", "capabilitiesSha256", "engine", "engineVersion")}
               for node in sorted(snapshot["nodes"], key=lambda node: node["role"])],
        localMaxPhotos=config.local_max_photos, localMaxInputBytes=config.local_max_input_bytes,
        treedar=False, gaussian_splat=False))
    return capability, options_fingerprint, frozenset(option["name"] for option in options)


class _DynamicAPI:
    def __init__(self, control, config, settings, discover, staged_ack=None):
        self.control, self.config, self.settings, self.discover, self.staged_ack = control, config, settings, discover, staged_ack

    def handle_chunk(self,*args,**kwargs):
        return self.control.handle_chunk(*args,**kwargs)

    def handle_chunk_v2(self,*args,**kwargs):
        return self.control.handle_chunk_v2(*args,**kwargs)

    def handle_archive(self,path,headers,length,chunks,*,protected_transport=False):
        return self.control.handle_archive(path,headers,length,chunks,protected_transport=protected_transport)

    def handle(self, method, path, headers, body=b"", *, protected_transport=False):
        from .viewer_staged_ack import ROUTE
        if path.startswith(ROUTE):
            if self.staged_ack is None:return Response(404,{'error':'not_found'})
            return self.staged_ack.handle(method,path,headers,body,protected_transport=protected_transport)
        # Authenticate before engine discovery; ordinary status and worker actions
        # never depend on live catalog reads. Each admission sees a fresh snapshot.
        if path in (p.PREFIX + '/options', p.PREFIX + "/capabilities", p.PREFIX + "/jobs") and method in ("GET", "POST"):
            if protected_transport is not True:
                return Response(503, {"error": "protected_transport_required"})
            environment = self.control.viewer._authenticate(headers)
            if environment is None:
                return Response(401, {"error": "unauthorized"})
            try:
                snapshot = self.discover()
                capability, options, allowed = discovered_policy(snapshot, self.config)
                if path == p.PREFIX + '/options' and method == 'GET':
                    definitions = sorted((option for option in snapshot['commonOptions'] if option['name'] in allowed), key=lambda option: option['name'])
                    return Response(200, p.decode_submission(p.canonical_json(dict(schemaVersion=1,
                        environment=environment, capabilityFingerprint=capability,
                        optionsFingerprint=options, options=definitions))))
                api = ReferenceAPI(self.control.viewer.store, bearer_tokens=self.settings.viewer_bearers,
                    capability_fingerprint=capability, options_fingerprint=options,
                    allowed_odm_options=allowed, treedar_available=False,
                    result_proxy=self.control.viewer.result_proxy)
            except Exception:
                return Response(503, {"error": "engine_catalog_unavailable"})
            if path == p.PREFIX + '/jobs' and method == 'POST':
                from .admission_drain import AdmissionClosed
                gate = getattr(self.control.viewer.store.queue, 'admission_drain', None)
                if gate is not None:
                    try:
                        with gate.admit():
                            return api.handle(method, path, headers, body, protected_transport=protected_transport)
                    except AdmissionClosed:
                        return Response(503, {'error': 'admission_draining'})
            return api.handle(method, path, headers, body, protected_transport=protected_transport)
        return self.control.handle(method, path, headers, body, protected_transport=protected_transport)


class ReferenceRuntime:
    def __init__(self, queue, config, settings, *, discover=None, server_factory=None, viewer_service=None, synthetic_chunk_tls=False, max_connections=None):
        """Construct services around the existing authority; never create a Queue."""
        self.queue, self.config, self.settings = queue, config, settings
        self._server_factory = server_factory or create_reference_http_server
        self._server, self._thread = None, None
        self._projection_owner=None;self._projection_binding=None;self._closed=False
        self._lifecycle_lock=threading.RLock()
        try:
            if max_connections is not None and (not REFERENCE_CONNECTION_LIMIT_QUALIFIED or type(max_connections) is not int or not 1 <= max_connections <= 256):
                raise RuntimeConfigurationError('reference connection limit not qualified')
            self._max_connections=max_connections
            if type(synthetic_chunk_tls) is not bool or (synthetic_chunk_tls and (not SYNTHETIC_CHUNK_TLS_QUALIFIED or settings.viewer_archive_spool is None)):
                raise RuntimeConfigurationError('synthetic chunk TLS is not qualified')
            projection=settings.viewer_projection_environments
            if projection is not None:
                from .viewer_service import ViewerService
                if (not VIEWER_RUNTIME_PROJECTION_QUALIFIED or projection!=('staging',)
                        or settings.viewer_archive_spool is None or type(settings.staged_ack_bearers) is not dict
                        or not any(type(v) is dict and v.get('environment')=='staging' for v in settings.staged_ack_bearers.values())
                        or 'staging' not in settings.viewer_bearers.values()
                        or type(viewer_service) is not ViewerService or viewer_service.queue is not queue
                        or viewer_service.viewer_spool is not None or viewer_service.viewer_completion_projection
                        or viewer_service._runtime_projection is not None):
                    raise RuntimeConfigurationError('Viewer projection is not qualified or scoped')
            recovery=settings.recovery_bindings
            if recovery is not None:
                if (type(recovery) is not dict or set(recovery)!=set(config.prototype_node_ids)
                        or set(recovery)!=set(queue.node_ids)):
                    raise RuntimeConfigurationError("recovery binding coverage changed")
                for node,physical in recovery.items():
                    if (type(physical) is not RecoveryNodeBinding or physical.node_id!=node
                            or physical.authority_id!=queue.authority_id):
                        raise RuntimeConfigurationError("recovery queue authority mismatch")
                    # Recheck even direct settings callers before any listener
                    # or feature startup can expose the configured authority.
                    RecoveryNodeBinding(**vars(physical))
                recovery=dict(recovery)
            # Validate every spool prerequisite before ReferenceStore or
            # ViewerWorkerClaims can create any persistent schema markers.
            spool_policy=None
            if settings.viewer_archive_spool is not None:
                if not VIEWER_ARCHIVE_SPOOL_SCHEMA_QUALIFIED:
                    raise RuntimeConfigurationError('Viewer archive spool schema is not qualified')
                if (not config.viewer_worker_claim_enabled or viewer_service is None
                        or viewer_service.queue is not queue or getattr(getattr(viewer_service,'store',None),'queue',None) is not queue):
                    raise RuntimeConfigurationError('Viewer worker claims require shared Viewer authority')
                environments=dict(config.viewer_worker_claim_env_policy)
                if (set(environments)!=set(queue.node_ids) or any(type(values) not in (tuple,list) or not values
                    or len(set(values))!=len(values) or not set(values)<={'production','staging'}
                    for values in environments.values()) or type(config.local_max_photos) is not int
                    or config.local_max_photos<1 or type(config.local_max_input_bytes) is not int or config.local_max_input_bytes<1):
                    raise RuntimeConfigurationError('invalid Viewer claim policy')
                spool_policy=_spool_policy(settings.viewer_archive_spool)
                _preflight_spool_settings(settings,config,queue,spool_policy)
            server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            server_context.minimum_version = ssl.TLSVersion.TLSv1_2
            server_context.load_cert_chain(settings.listener["certificate"], settings.listener["privateKey"])
            worker_context = verified_client_context(settings.listener["clientCA"])
            self._server_context = server_context
            self.store = ReferenceStore(queue, local_max_photos=config.local_max_photos,
                local_max_input_bytes=config.local_max_input_bytes, main_node_id="main")
            self._streams = threading.BoundedSemaphore(settings.stream_budget["maxConcurrent"])
            proxy = ResultProxy({node: worker["resultOrigin"] for node, worker in settings.workers.items()},
                worker_bearers={node: worker["resultBearer"] for node, worker in settings.workers.items()},
                admit_read=self._admit_read, ssl_context=worker_context)
            workers = {worker["bearer"]: dict(node_id=node, worker_id=worker["workerId"],
                worker_instance_id=worker["instanceId"]) for node, worker in settings.workers.items()}
            viewer_claims=None
            if config.viewer_worker_claim_enabled:
                if viewer_service is None or viewer_service.queue is not queue:
                    raise RuntimeConfigurationError('Viewer worker claims require shared Viewer authority')
                from .viewer_worker_claim import ViewerWorkerClaims
                viewer_claims=ViewerWorkerClaims(queue,viewer_service.store,
                    local_max_photos=config.local_max_photos,local_max_input_bytes=config.local_max_input_bytes,
                    node_environments=dict(config.viewer_worker_claim_env_policy))
            viewer_spool=None
            if spool_policy is not None:
                if viewer_claims is None:raise RuntimeConfigurationError('Viewer worker claims required')
                from .viewer_archive_spool import ViewerArchiveSpool
                viewer_spool=ViewerArchiveSpool(viewer_claims,root=spool_policy['root'],root_identity=spool_policy['rootIdentity'],
                                                max_archive_bytes=spool_policy['maxArchiveBytes'],enabled=True)
            placeholder = p.digest({"runtime": "discovery_required"})
            self.control = ReferenceControlAPI(self.store, viewer_bearers=settings.viewer_bearers,
                worker_bearers=workers, capability_fingerprint=placeholder,
                options_fingerprint=placeholder, result_proxy=proxy, treedar_available=False,
                recovery_bindings=recovery,viewer_claims=viewer_claims,viewer_spool=viewer_spool)
            if synthetic_chunk_tls:
                from .viewer_chunk_spool import SyntheticChunkSpool
                self.control.worker.viewer_chunks=SyntheticChunkSpool(viewer_spool,synthetic_fixture=True)
            discover = discover or (lambda: catalog([
                ("local", config.local_node_url, config.local_node_token),
                ("main", config.main_node_url, config.main_node_token)]))
            staged=None
            if settings.staged_ack_bearers is not None:
                if not STAGED_ACK_SCHEMA_QUALIFIED:
                    raise RuntimeConfigurationError('staged acknowledgment schema is not qualified')
                from .viewer_staged_ack import ViewerStagedAck
                if viewer_service is None or viewer_service.queue is not queue:
                    raise RuntimeConfigurationError('staged acknowledgment requires shared Viewer authority')
                staged_tokens=set(settings.staged_ack_bearers)
                existing=set(settings.viewer_bearers)
                existing.update(worker[key] for worker in settings.workers.values() for key in ('bearer','resultBearer'))
                existing.update(secret for secret in (config.api_token,config.prototype_operator_token,
                    config.prototype_observer_token,config.viewer_token,config.viewer_staging_token,
                    config.local_node_token,config.main_node_token) if secret)
                if staged_tokens & existing or not {binding['environment'] for binding in settings.staged_ack_bearers.values()} <= set(settings.viewer_bearers.values()):
                    raise RuntimeConfigurationError('staged credential scope or separation changed')
                staged=ViewerStagedAck(viewer_service,settings.staged_ack_bearers,spool=viewer_spool)
            self.api = _DynamicAPI(self.control, config, settings, discover, staged)
            if projection is not None:
                # Keep pending until the listener has successfully started.
                self._projection_binding=(viewer_spool,projection)
                self._projection_owner=viewer_service
        except Exception:
            raise RuntimeConfigurationError("reference TLS runtime initialization failed") from None

    @contextmanager
    def _admit_read(self, binding):
        if binding["byteSize"] > self.settings.stream_budget["maxObjectBytes"] or not self._streams.acquire(blocking=False):
            raise RuntimeConfigurationError("result stream capacity unavailable")
        try:
            yield
        finally:
            self._streams.release()

    def start(self):
        with self._lifecycle_lock:return self._start_locked()

    def _start_locked(self):
        if self._closed:raise RuntimeConfigurationError("reference listener is closed")
        if self._server is not None:
            return
        listener = self.settings.listener
        server=None;thread=None;started=False
        try:
            kwargs={"ssl_context":self._server_context}
            if self._max_connections is not None:kwargs["max_connections"]=self._max_connections
            server = self._server_factory(self.api, listener["host"], listener["port"], **kwargs)
            # Join active requests before releasing the shared queue's authority lock.
            server.daemon_threads, server.block_on_close = False, True
            thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.1},
                                      name="processing-reference-tls", daemon=True)
            thread.start();started=True
            if self._projection_owner is not None:
                with self._projection_owner._lock:
                    if self._projection_owner._runtime_projection is not None:
                        raise RuntimeConfigurationError('Viewer projection already bound')
                    self._projection_owner._runtime_projection=self._projection_binding
            self._server, self._thread = server, thread
        except Exception:
            if server is not None:
                if started:server.shutdown()
                server.server_close()
                if started:thread.join()
            self.close()
            raise RuntimeConfigurationError("reference listener start failed") from None

    def close(self):
        with self._lifecycle_lock:return self._close_locked()

    def _close_locked(self):
        self._closed=True
        if self._server is not None:
            server, thread = self._server, self._thread
            self._server, self._thread = None, None
            server.shutdown()
            server.server_close()
            thread.join()
        if self._projection_owner is not None:
            with self._projection_owner._lock:
                if self._projection_owner._runtime_projection is self._projection_binding:
                    self._projection_owner._runtime_projection=None
            self._projection_owner=None;self._projection_binding=None
