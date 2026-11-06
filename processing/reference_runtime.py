"""Explicit optional TLS reference listener sharing the native queue authority.

Configuration is a private JSON file, never environment-serialized credentials.
No certificates, credentials, worker daemons, or cleanup are created here. The
discovered allowlist admits typed booleans, numbers, bounded enum choices and
bounded free text. Path/file options remain closed until a registered file
workflow exists.
"""
from contextlib import contextmanager
from dataclasses import dataclass, field
import os
import re
from pathlib import Path
import ssl
import stat
import threading

from . import reference_protocol as p
from .reference_api import ReferenceAPI, Response
from .reference_control import ReferenceControlAPI
from .reference_http import create_reference_http_server
from .reference_result_proxy import ResultProxy
from .reference_store import ReferenceStore
from .reference_tls import verified_client_context
from .reference_recovery import RecoveryNodeBinding
from .maintenance import Conflict
from .config import normalized_node_origin, node_image_limit
from .reference_source_intake import SOURCE_PREFIX
from .reference_limits import V2_SOURCE_INTAKE_BYTES

MAX_CONFIG_BYTES = 64 * 1024
from .reference_option_policy import (
    _UNSAFE, _FILE_OPTIONS, _OPTION_BOUND, _OPTION_RANGE, _option_domain,
    _value_matches_domain, safe_option_definitions, advertised_option_definitions, validate_option_values,
)


class RuntimeConfigurationError(ValueError):
    """Bounded startup error, without private configuration or file contents."""


def _validate_node_configuration(config, queue_node_ids=None):
    """Validate ordered node identity and syntactic endpoint uniqueness.

    Normalized URL identity catches duplicate spellings only; the configured
    Engine IDs are daemon identities and are not physical-host attestation.
    """
    try:
        node_ids = tuple(config.prototype_node_ids)
        endpoints = tuple(config.node_endpoints)
        endpoint_ids = tuple(node.id for node in endpoints)
        origins = tuple(normalized_node_origin(node.url) for node in endpoints)
    except Exception:
        raise RuntimeConfigurationError("ordered processing node configuration is invalid") from None
    if (not node_ids or len(node_ids) != len(set(node_ids)) or endpoint_ids != node_ids
            or len(origins) != len(set(origins))):
        raise RuntimeConfigurationError("ordered processing nodes must have unique IDs and origins")
    if queue_node_ids is not None and tuple(queue_node_ids) != node_ids:
        raise RuntimeConfigurationError("processing node order must match the queue authority")
    return node_ids


@dataclass(frozen=True, repr=False)
class DisabledDirectWorkerPreparation:
    """References for offline preparation; never an active capability."""
    nodes: dict
    enabled: bool = field(default=False, init=False)

    def __bool__(self):
        return False


@dataclass(frozen=True, repr=False)
class ReferenceSettings:
    listener: dict
    viewer_bearers: dict
    workers: dict
    stream_budget: dict
    recovery_bindings: dict | None = None
    viewer_receipt_keys: dict | None = None
    viewer_acceptance_targets: dict | None = None
    source_spool: object | None = None
    source_attestation_keys: object | None = None
    source_release: object | None = None
    source_bearer_principals: dict | None = None
    direct_worker_preparation: DisabledDirectWorkerPreparation | None = None


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


def load_reference_settings(path, config, *, validate_disabled_direct_files=False):
    """Read and validate the explicit opt-in, before opening the shared Queue."""
    try:
        if type(validate_disabled_direct_files) is not bool:
            raise RuntimeConfigurationError('invalid offline direct validation option')
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
        node_ids = _validate_node_configuration(config)
        if len(node_ids) > 1 and value["schemaVersion"] == 1:
            raise RuntimeConfigurationError(
                "multi-node dispatch requires daemon identity bindings; these do not prove physical-host identity")
        fields=("schemaVersion", "listener", "viewerBearers", "workers", "streamBudget")
        if value["schemaVersion"] in (3, 4, 5, 6):
            required = set(fields) | {"viewerReceiptKeys"}
            if value["schemaVersion"] in (4,5,6):
                required.add("viewerAcceptance")
            allowed = required | {"recoveryBindings"}
            if value["schemaVersion"] in (5,6):allowed.add("sourceSpool")
            if value["schemaVersion"]==6:
                required.add("sourceSpool")
                required.add("sourceAttestationKeys")
                allowed.add("sourceAttestationKeys")
                allowed.add("sourceRelease")
                allowed.add("sourceBearerPrincipals")
                allowed.add("directWorkers")
            if not required <= set(value) or set(value) - allowed:
                raise RuntimeConfigurationError("runtime configuration fields differ")
        else:
            p._closed(value, fields + (("recoveryBindings",) if value["schemaVersion"]==2 else ()),
                "runtime configuration")
        listener = value["listener"]
        p._closed(listener, ("host", "port", "certificate", "privateKey", "clientCA"), "TLS listener")
        if (type(listener["host"]) is not str or not listener["host"] or
                any(ord(c) <= 32 for c in listener["host"]) or type(listener["port"]) is not int
                or not 1 <= listener["port"] <= 65535):
            raise RuntimeConfigurationError("invalid TLS listener address")
        for field in ("certificate", "privateKey", "clientCA"):
            _file(listener[field], private=field == "privateKey")
        if not config.prototype_state_dir:
            raise RuntimeConfigurationError("reference runtime requires persistent shared state")
        viewers = value["viewerBearers"]
        workers = value["workers"]
        if type(viewers) is not dict or not viewers or not set(viewers) <= {"production", "staging"}:
            raise RuntimeConfigurationError("explicit scoped reference Viewer credentials required")
        if type(workers) is not dict or set(workers) != set(config.prototype_node_ids):
            raise RuntimeConfigurationError("reference workers must exactly cover configured queue nodes")
        legacy = {secret for secret in (config.api_token, config.prototype_operator_token,
            config.prototype_observer_token, config.viewer_token, config.viewer_staging_token,
            config.local_node_token, config.main_node_token) if secret}
        legacy.update(node.token for node in config.node_endpoints if node.token)
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
        if len(set(credentials)) != len(credentials) or set(credentials) & legacy:
            raise RuntimeConfigurationError("reference credentials must be disjoint from existing credentials")
        budget = value["streamBudget"]
        p._closed(budget, ("maxConcurrent", "maxObjectBytes"), "stream budget")
        if type(budget["maxConcurrent"]) is not int or not 1 <= budget["maxConcurrent"] <= 16:
            raise RuntimeConfigurationError("invalid stream concurrency budget")
        p._integer(budget["maxObjectBytes"], "stream object budget")
        recovery = None
        if value["schemaVersion"] == 2 or "recoveryBindings" in value:
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
            if len({binding.engine_id for binding in recovery.values()}) != len(recovery):
                raise RuntimeConfigurationError("each configured node must bind a unique Docker Engine daemon")
        if len(node_ids) > 1 and recovery is None:
            raise RuntimeConfigurationError("multi-node runtime requires daemon identity bindings")
        viewer_keys = {}
        if value["schemaVersion"] in (3, 4, 5, 6):
            from .viewer_acceptance_receipt import validate_trusted_viewer_receipt_keys
            viewer_keys = validate_trusted_viewer_receipt_keys(value["viewerReceiptKeys"])
        viewer_targets = {}
        if value["schemaVersion"] in (4,5,6):
            configured = value["viewerAcceptance"]
            # Schema 6 can use Viewer's authenticated pull/copy-ACK path without
            # starting the legacy push sender. Receipt pins remain mandatory
            # below, including when this explicit target map is empty.
            if (type(configured) is not dict
                    or (not configured and value["schemaVersion"] != 6)
                    or not set(configured) <= {"production", "staging"}):
                raise RuntimeConfigurationError("explicit environment-scoped Viewer targets required")
            target_secrets = []
            from .reference_result_proxy import _origin as validate_https_origin
            for environment, target in configured.items():
                p._environment(environment)
                p._closed(target, ("origin", "keyId", "secretFile"), "Viewer acceptance target")
                origin = validate_https_origin(target["origin"])
                key_id = target["keyId"]
                p._slug(key_id, "Viewer service key ID")
                secret_path, initial_secret = _file(target["secretFile"], private=True)
                descriptor = os.open(secret_path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
                try:
                    observed = os.fstat(descriptor)
                    if (observed.st_dev, observed.st_ino) != (initial_secret.st_dev, initial_secret.st_ino):
                        raise RuntimeConfigurationError("Viewer service key identity changed")
                    with os.fdopen(descriptor, "rb", closefd=False) as source:
                        secret_bytes = source.read(4097)
                finally:
                    os.close(descriptor)
                if secret_bytes.endswith(b"\r\n"):
                    secret_bytes = secret_bytes[:-2]
                elif secret_bytes.endswith(b"\n"):
                    secret_bytes = secret_bytes[:-1]
                try:
                    secret = secret_bytes.decode("ascii")
                except UnicodeError:
                    raise RuntimeConfigurationError("invalid Viewer service key file") from None
                if (not 32 <= len(secret) <= 4096 or any(ord(char) < 33 or ord(char) > 126 for char in secret)
                        or secret in credentials):
                    raise RuntimeConfigurationError("invalid Viewer service key file")
                credentials.append(secret)
                target_secrets.append(secret)
                viewer_targets[environment] = {"origin": origin, "keyId": key_id, "secret": secret}
            if len(set(target_secrets)) != len(target_secrets) or not viewer_keys:
                raise RuntimeConfigurationError("distinct Viewer service keys and receipt pins are required")
        direct_workers=None
        if 'directWorkers' in value:
            direct=value['directWorkers']
            p._closed(direct,('enabled','nodes'),'direct worker preparation')
            if direct['enabled'] is not False:
                raise RuntimeConfigurationError('direct worker runtime not implemented')
            if type(direct['nodes']) is not dict or set(direct['nodes'])!=set(node_ids):
                raise RuntimeConfigurationError('direct worker nodes must exactly cover configured nodes')
            from .reference_result_proxy import _origin
            from cryptography.hazmat.primitives import serialization
            from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
            import ipaddress
            direct_workers={}
            direct_key_bytes=set();direct_endpoints=set()
            for node,entry in direct['nodes'].items():
                p._closed(entry,('origin','connectIp','caFile','clientCertFile','clientKeyFile',
                    'bearerFile','issuerId','keyId','workerPublicKeyPem'),'direct worker node')
                origin=_origin(entry['origin'])
                if type(entry['connectIp']) is not str or '%' in entry['connectIp']:
                    raise RuntimeConfigurationError('direct worker address invalid')
                ipaddress.ip_address(entry['connectIp'])
                for field in ('caFile','clientCertFile','clientKeyFile','bearerFile'):
                    if type(entry[field]) is not str or not Path(entry[field]).is_absolute():
                        raise RuntimeConfigurationError('absolute direct file reference required')
                p._slug(entry['issuerId'],'issuer');p._slug(entry['keyId'],'key')
                if type(entry['workerPublicKeyPem']) is not str or len(entry['workerPublicKeyPem'])>4096:
                    raise RuntimeConfigurationError('worker public key invalid')
                key=serialization.load_pem_public_key(entry['workerPublicKeyPem'].encode('ascii'))
                if not isinstance(key,Ed25519PublicKey):raise RuntimeConfigurationError('worker public key invalid')
                raw_key=key.public_bytes(serialization.Encoding.Raw,serialization.PublicFormat.Raw)
                if raw_key in direct_key_bytes or origin in direct_endpoints:
                    raise RuntimeConfigurationError('direct worker pins and origins must be distinct')
                direct_key_bytes.add(raw_key);direct_endpoints.add(origin)
                if validate_disabled_direct_files:
                    for field in ('caFile','clientCertFile','clientKeyFile','bearerFile'):
                        _file(entry[field],private=field in ('clientKeyFile','bearerFile'))
                    context=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                    context.minimum_version=ssl.TLSVersion.TLSv1_2
                    context.load_verify_locations(cafile=entry['caFile'])
                    context.load_cert_chain(entry['clientCertFile'],entry['clientKeyFile'])
                    secret_path,initial=_file(entry['bearerFile'],private=True)
                    fd=os.open(secret_path,os.O_RDONLY|getattr(os,'O_NOFOLLOW',0))
                    try:
                        observed=os.fstat(fd)
                        if (observed.st_dev,observed.st_ino)!=(initial.st_dev,initial.st_ino):
                            raise RuntimeConfigurationError('direct bearer identity changed')
                        with os.fdopen(fd,'rb',closefd=False) as source: raw_secret=source.read(4097)
                    finally:os.close(fd)
                    if len(raw_secret)>4096:raise RuntimeConfigurationError('direct bearer invalid')
                    secret=raw_secret.decode('ascii').rstrip('\r\n')
                    if (not 32<=len(secret)<=4096 or any(ord(c)<33 or ord(c)>126 for c in secret)
                            or secret in credentials or secret in legacy):
                        raise RuntimeConfigurationError('direct bearer invalid')
                    credentials.append(secret)
                # Preparation only: retain the protected path, never an
                # additional in-memory copy of the direct bearer.
                direct_workers[node]={**entry,'origin':origin,'workerPublicKey':key}
        source_spool=None
        source_attestation_keys=None
        if value["schemaVersion"] in (5,6):
            spool=value["sourceSpool"]
            p._closed(spool,("root","mountPath","mountId","markerName","safetyFloorBytes","maxObjectBytes","chunkBytes"),"source spool")
            from .source_spool import SourceSpool
            source_spool=SourceSpool(spool["root"],mount_path=spool["mountPath"],mount_id=spool["mountId"],
                marker_name=spool["markerName"],safety_floor_bytes=spool["safetyFloorBytes"],
                max_object_bytes=spool["maxObjectBytes"],chunk_bytes=spool["chunkBytes"])
        if value["schemaVersion"]==6:
            from .viewer_source_attestation import validate_trusted_keys
            source_attestation_keys=validate_trusted_keys(value["sourceAttestationKeys"])
        source_principals = {}
        if 'sourceBearerPrincipals' in value:
            configured_principals = value['sourceBearerPrincipals']
            if type(configured_principals) is not dict or not set(configured_principals) <= set(viewers):
                raise RuntimeConfigurationError('source principal environments must match Viewer credentials')
            for environment, principal in configured_principals.items():
                p._slug(principal, 'source bearer principal')
                if not any(key[0] == environment and record[0] == principal
                           for key, record in source_attestation_keys.records.items()):
                    raise RuntimeConfigurationError('source principal requires trusted signing key')
                source_principals[viewers[environment]] = principal
        release_settings=None
        if 'sourceRelease' in value:
            from .source_release import ReleaseSettings
            release_settings=ReleaseSettings(value['sourceRelease'],
                viewer_bearers={token:env for env,token in viewers.items()},spool=source_spool)
        return ReferenceSettings(dict(listener), {token: env for env, token in viewers.items()},
                                 {node: dict(worker) for node, worker in workers.items()}, dict(budget), recovery,
                                 viewer_keys, viewer_targets or None, source_spool, source_attestation_keys,release_settings,
                                 source_principals,DisabledDirectWorkerPreparation(direct_workers)
                                 if direct_workers is not None else None)
    except Exception:
        raise RuntimeConfigurationError("invalid private reference runtime configuration") from None


def _available_discovery_snapshot(snapshot):
    """Build the admission view from the healthy subset of the configured fleet.

    A single offline engine must not stop new work from entering the durable
    queue when another configured node can accept it. The worker selector and
    atomic claim still receive every node and mark unavailable ones ineligible.
    """
    if type(snapshot) is not dict or type(snapshot.get("nodes")) is not list:
        raise RuntimeConfigurationError("configured engine catalog unavailable")
    available = [node for node in snapshot["nodes"] if type(node) is dict
                 and node.get("available") is True
                 and node.get("configured", True) is True]
    if not available:
        raise RuntimeConfigurationError("no configured engine is available")
    if snapshot.get("ready") is True:
        definitions = snapshot.get("commonOptions")
    else:
        definitions = snapshot.get("availableOptions")
    if type(definitions) is not list:
        raise RuntimeConfigurationError("available engine definitions unavailable")
    return {**snapshot, "ready": True, "nodes": available, "commonOptions": definitions}


def discovered_policy(snapshot, config):
    """Build opaque identities from actual fleet definitions and routing policy."""
    snapshot = _available_discovery_snapshot(snapshot)
    options = sorted(advertised_option_definitions(snapshot['commonOptions']).values(), key=lambda option: option['name'])
    options_fingerprint = p.digest(dict(protocol="reference-options-v1", definitions=options))
    capability = p.digest(dict(protocol="reference-capabilities-v1", options=options_fingerprint,
        nodes=[{key: node.get(key) for key in ("role", "available", "capabilitiesSha256", "engine", "engineVersion")}
               for node in sorted(snapshot["nodes"], key=lambda node: node["role"])],
        localMaxPhotos=config.local_max_photos, localMaxInputBytes=config.local_max_input_bytes,
        treedar=False, gaussian_splat=False))
    return capability, options_fingerprint, frozenset(option["name"] for option in options)


def validate_discovered_option_values(payload, snapshot, allowed):
    """Enforce live option types and enum choices at the submission boundary."""
    try:
        values = payload["pipeline"]["odm"]["options"]
        if type(values) is not dict:
            raise ValueError
        definitions = safe_option_definitions(snapshot['commonOptions'])
        validate_option_values(values, {name: definition for name, definition in definitions.items() if name in allowed})
    except (KeyError, TypeError, ValueError, OverflowError):
        raise p.ProtocolError("ODM option value does not match the current engine definition") from None


class _DynamicAPI:
    def __init__(self, control, config, settings, discover, discover_capacities, discover_fleet=None):
        self.control, self.config, self.settings, self.discover = control, config, settings, discover
        self.coordinator_queue = control.coordinator_queue
        self.discover_capacities = discover_capacities
        self.discover_fleet = discover_fleet
        self._worker_refresh_lock = threading.RLock()

    def handle(self, method, path, headers, body=b"", *, protected_transport=False):
        if path == '/ltds/worker/v1/direct-source-policy':
            # Expose the coordinator's live fleet-derived policy to an already
            # enrolled worker. This is read-only and does not enable direct
            # admission, authorize a node, or accept source custody.
            if protected_transport is not True:
                return Response(503, {'error':'protected_transport_required'})
            identity=self.control.worker._authenticate(headers)
            if identity is None:
                return Response(401, {'error':'unauthorized'})
            if method!='GET' or body not in (b'', ''):
                return Response(400, {'error':'invalid_request'})
            with self._worker_refresh_lock:
                try:
                    snapshot=_available_discovery_snapshot(self.discover())
                    capability,options,allowed=discovered_policy(snapshot,self.config)
                    node=next(n for n in snapshot['nodes'] if n.get('role')==identity['node_id'])
                    if node.get('available') is not True:
                        return Response(503, {'error':'worker_catalog_unavailable'})
                    return Response(200,dict(schemaVersion=1,authorityId=self.coordinator_queue.authority_id,
                        nodeId=identity['node_id'],workerId=identity['worker_id'],
                        workerInstanceId=identity['worker_instance_id'],capabilityFingerprint=capability,
                        optionsFingerprint=options,allowedOdmOptions=sorted(allowed),
                        directPreparationEnabled=False,sourceCustodyAccepted=False))
                except Exception:
                    return Response(503, {'error':'worker_catalog_unavailable'})
        worker_admission = ((method == 'GET' and path == '/ltds/worker/v1/next')
                            or (method == 'POST' and path == '/ltds/worker/v1/claim'))
        if worker_admission:
            if protected_transport is not True:
                return self.control.handle(method, path, headers, body,
                                           protected_transport=protected_transport)
            identity = self.control.worker._authenticate(headers)
            if identity is not None:
                # Keep discovery and the resulting atomic claim decision in one
                # serialized admission turn. Partial node outages are retained
                # per node so an eligible later node can still take work.
                with self._worker_refresh_lock:
                    try:
                        if self.discover_fleet is None:
                            options = self.discover()
                            capacities = self.discover_capacities()
                        else:
                            options, capacities = self.discover_fleet()
                        self.control.worker.store.update_fleet_snapshot(capacities, options['nodes'])
                    except Exception:
                        return Response(503, {"error": "engine_capacity_unavailable"})
                    return self.control.handle(method, path, headers, body,
                                               protected_transport=protected_transport)
            return self.control.handle(method, path, headers, body,
                                       protected_transport=protected_transport)
        recovery=re.fullmatch(r'/ltds/worker/v1/jobs/([A-Za-z0-9][A-Za-z0-9_-]{0,127})/(recovery-state|recover)',path) if type(path) is str else None
        if (recovery and (method,recovery.group(2)) in (('GET','recovery-state'),('POST','recover'))
                and protected_transport is True):
            worker=self.control.worker
            identity=worker._authenticate(headers)
            token=worker._header(headers,'X-Claim-Token')
            if identity is not None and token is not None:
                with self._worker_refresh_lock:
                    try:
                        # Authenticate the exact claim BEFORE any network probe.
                        # A monotonic later phase must keep its existing offline
                        # continuation/delivery path, without catalog dependency.
                        claim=worker.store.claim_snapshot(recovery.group(1),**identity,claim_token=token)
                        unreserved=False
                        if (worker.recovery is not None and claim.get('sourceIntakeVersion')==2
                                and claim['executionPhase'] in ('queued','waiting_capacity')
                                and (claim['status'],claim['heldReason'])==('held','coordinator_restart')):
                            current=worker.recovery.state(recovery.group(1),identity=identity,claim_token=token)
                            unreserved=current['schemaVersion']==4
                        if method=='POST':
                            from .reference_recovery import ClaimRecoveryService
                            payload=ClaimRecoveryService._request(p.decode_submission(body))
                            unreserved=unreserved and payload['schemaVersion']==4
                        elif body:
                            unreserved=False
                    except Conflict:
                        return Response(409,{'error':'recovery_state_unavailable'})
                    except (ValueError,TypeError,KeyError):
                        return Response(400,{'error':'invalid_request'})
                    except Exception:
                        # Never fall through optimistic Store defaults when a
                        # pre-discovery read or its outcome is unknown.
                        return Response(503,{'error':'engine_capacity_unavailable'})
                    if unreserved:
                        try:
                            if self.discover_fleet is None:
                                options=self.discover()
                                capacities=self.discover_capacities()
                            else:
                                options,capacities=self.discover_fleet()
                            worker.store.update_fleet_snapshot(capacities,options['nodes'])
                        except Exception:
                            return Response(503,{'error':'engine_capacity_unavailable'})
                    return self.control.handle(method,path,headers,body,protected_transport=protected_transport)
        # Authenticate before engine discovery; ordinary status and worker actions
        # never depend on live catalog reads. Each admission sees a fresh snapshot.
        if path in (p.PREFIX + '/options', p.PREFIX + "/capabilities", p.PREFIX + "/jobs",
                    SOURCE_PREFIX + "/capabilities", SOURCE_PREFIX + "/jobs") and method in ("GET", "POST"):
            if protected_transport is not True:
                return Response(503, {"error": "protected_transport_required"})
            environment = self.control.viewer._authenticate(headers)
            if environment is None:
                return Response(401, {"error": "unauthorized"})
            if method == 'POST' and path == SOURCE_PREFIX + '/jobs':
                principal = self.control.viewer._source_principal(headers)
                if principal is None:
                    return Response(403, {'error': 'source_principal_required'})
                replay = self.control.viewer._source_replay_response(body, environment, principal)
                if replay is not None:
                    return replay
            try:
                snapshot = _available_discovery_snapshot(self.discover())
                capability, options, allowed = discovered_policy(snapshot, self.config)
                if path == p.PREFIX + '/options' and method == 'GET':
                    definitions = sorted((option for name, option in advertised_option_definitions(snapshot['commonOptions']).items()
                                          if name in allowed), key=lambda option: option['name'])
                    return Response(200, p.decode_submission(p.canonical_json(dict(schemaVersion=1,
                        environment=environment, capabilityFingerprint=capability,
                        optionsFingerprint=options, options=definitions))))
                api = ReferenceAPI(self.control.viewer.store, bearer_tokens=self.settings.viewer_bearers,
                    capability_fingerprint=capability, options_fingerprint=options,
                    allowed_odm_options=allowed, treedar_available=False,
                    result_proxy=self.control.viewer.result_proxy,
                    viewer_receipt_keys=self.settings.viewer_receipt_keys,
                    source_spool=self.settings.source_spool,
                    source_attestation_keys=self.settings.source_attestation_keys,
                    source_bearer_principals=self.settings.source_bearer_principals,
                    authority_id=self.control.viewer.store.queue.authority_id,
                    source_release_service=self.control.viewer.source_release_service)
            except Exception:
                return Response(503, {"error": "engine_catalog_unavailable"})
            if path in (p.PREFIX + "/jobs",SOURCE_PREFIX + "/jobs") and method == "POST":
                try:
                    # V2 source intake carries both source and engine manifests.
                    # Keep the legacy V1 decoder budget unchanged and grant the
                    # larger envelope only to this exact authenticated route.
                    max_bytes = (V2_SOURCE_INTAKE_BYTES
                        if path == SOURCE_PREFIX + "/jobs" else p.MAX_BODY_BYTES)
                    payload = p.decode_submission(body, max_bytes=max_bytes)
                    validate_discovered_option_values(payload, snapshot, allowed)
                except (p.ProtocolError, TypeError, ValueError):
                    return Response(400, {"error": "invalid_request"})
            return api.handle(method, path, headers, body, protected_transport=protected_transport)
        return self.control.handle(method, path, headers, body, protected_transport=protected_transport)

    @property
    def source_spool(self):return self.settings.source_spool

    def stream_source_object(self,path,headers,reader,content_length,*,protected_transport=False):
        return self.control.viewer.stream_source_object(path,headers,reader,content_length,
            protected_transport=protected_transport)


from .coordinator_freeze import runtime_constructor


class ReferenceRuntime:
    @runtime_constructor
    def __init__(self, queue, config, settings, *, discover=None, discover_capacities=None,
                 server_factory=None):
        """Construct services around the existing authority; never create a Queue."""
        self.queue, self.config, self.settings = queue, config, settings
        self._server_factory = server_factory or create_reference_http_server
        self._server, self._thread = None, None
        self._source_readers = None
        self._viewer_transfer_worker, self._viewer_transfer_thread = None, None
        self._source_release = None
        self._source_release_thread = None
        self._source_timer_thread = None
        self._source_release_stop = threading.Event()
        self._viewer_transfer_stop = threading.Event()
        try:
            recovery=settings.recovery_bindings
            node_ids = _validate_node_configuration(config, queue.node_ids)
            if type(settings.workers) is not dict or set(settings.workers) != set(node_ids):
                raise RuntimeConfigurationError("worker bindings must exactly cover configured nodes")
            if len(node_ids) > 1 and recovery is None:
                raise RuntimeConfigurationError(
                    "multi-node dispatch requires daemon identity bindings; these do not prove physical-host identity")
            if recovery is not None:
                if (type(recovery) is not dict or set(recovery)!=set(node_ids)):
                    raise RuntimeConfigurationError("recovery binding coverage changed")
                engine_ids = []
                for node,physical in recovery.items():
                    if (type(physical) is not RecoveryNodeBinding or physical.node_id!=node
                            or physical.authority_id!=queue.authority_id):
                        raise RuntimeConfigurationError("recovery queue authority mismatch")
                    # Recheck even direct settings callers before any listener
                    # or feature startup can expose the configured authority.
                    RecoveryNodeBinding(**vars(physical))
                    engine_ids.append(physical.engine_id)
                if len(engine_ids) != len(set(engine_ids)):
                    raise RuntimeConfigurationError("each configured node must bind a unique Docker Engine daemon")
                recovery=dict(recovery)
            server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            server_context.minimum_version = ssl.TLSVersion.TLSv1_2
            server_context.load_cert_chain(settings.listener["certificate"], settings.listener["privateKey"])
            worker_context = verified_client_context(settings.listener["clientCA"])
            self._server_context = server_context
            endpoints = config.node_endpoints
            node_limits = {node.id: {"maxImages": node_image_limit(node, config.local_max_photos),
                                    **({"maxInputBytes": node.max_input_bytes}
                                       if node.max_input_bytes is not None else {})}
                           for node in endpoints}
            self.store = ReferenceStore(queue, local_max_photos=config.local_max_photos,
                local_max_input_bytes=config.local_max_input_bytes,
                main_node_id=next((node.id for node in endpoints if node.id == "main"), None),
                node_limits=node_limits,
                source_routing_keys=settings.source_attestation_keys,
                node_option_validator=self._node_options_valid)
            from .node_inventory import pin_reference_inventory
            pin_reference_inventory(self.store, config, settings)
            self.source_reconciliation = (self.store.reconcile_source_spool(settings.source_spool)
                if queue._coordinator_freeze.status()['phase'] == 'open' else {'state': 'maintenance_frozen'})
            if settings.source_spool is not None:
                self._source_readers = self.store.source_readers()
            if settings.source_release is not None:
                from .source_release import SourceReleaseService
                self._source_release=SourceReleaseService(self.store,settings.source_spool,settings.source_release,
                    stop_requested=self._source_release_stop.is_set)
            self._streams = threading.BoundedSemaphore(settings.stream_budget["maxConcurrent"])
            proxy = ResultProxy({node: worker["resultOrigin"] for node, worker in settings.workers.items()},
                worker_bearers={node: worker["resultBearer"] for node, worker in settings.workers.items()},
                admit_read=self._admit_read, ssl_context=worker_context)
            workers = {worker["bearer"]: dict(node_id=node, worker_id=worker["workerId"],
                worker_instance_id=worker["instanceId"]) for node, worker in settings.workers.items()}
            placeholder = p.digest({"runtime": "discovery_required"})
            self.control = ReferenceControlAPI(self.store, viewer_bearers=settings.viewer_bearers,
                worker_bearers=workers, capability_fingerprint=placeholder,
                options_fingerprint=placeholder, result_proxy=proxy, treedar_available=False,
                recovery_bindings=recovery, viewer_receipt_keys=settings.viewer_receipt_keys,
                source_spool=settings.source_spool,
                source_attestation_keys=settings.source_attestation_keys,
                source_bearer_principals=settings.source_bearer_principals,
                authority_id=queue.authority_id,source_release_service=self._source_release)
            if settings.viewer_acceptance_targets:
                from .reference_viewer_transfer_worker import ReferenceViewerTransferWorker
                from .viewer_acceptance_transfer import ViewerAcceptanceTransfer, ViewerTransferUnavailable

                class EnvironmentSender:
                    def __init__(self, targets): self.targets = targets
                    def transfer(self, job, acceptance, *, viewer_result_manifest=None):
                        sender = self.targets.get(job.get("environment"))
                        if sender is None:
                            raise ViewerTransferUnavailable("viewer_environment_not_configured")
                        return sender.transfer(job, acceptance, viewer_result_manifest=viewer_result_manifest)

                senders = {environment: ViewerAcceptanceTransfer(target["origin"],
                    key_id=target["keyId"], secret=target["secret"],
                    trusted_receipt_keys=settings.viewer_receipt_keys, result_proxy=proxy)
                    for environment, target in settings.viewer_acceptance_targets.items()}
                self._viewer_transfer_worker = ReferenceViewerTransferWorker(self.store,
                    EnvironmentSender(senders), trusted_receipt_keys=settings.viewer_receipt_keys)
            from .reference_worker_catalog import WorkerCatalogClient
            self._worker_catalog = WorkerCatalogClient(settings.workers,node_ids=node_ids,
                authority_id=queue.authority_id,ssl_context=worker_context,recovery_bindings=recovery)
            discover_fleet = self._discover_fleet if discover is None and discover_capacities is None else None
            discover = discover or (lambda: self._discover_fleet()[0])
            discover_capacities = discover_capacities or self._discover_capacities
            self.api = _DynamicAPI(self.control, config, settings, discover, discover_capacities,discover_fleet)
        except Exception:
            raise RuntimeConfigurationError("reference TLS runtime initialization failed") from None

    def _discover_fleet(self):
        options, probes = self._worker_catalog.discover()
        capacities = {}
        for node in self.config.node_endpoints:
            probe = probes.get(node.id, {})
            maximum, queued = probe.get('maxImages'), probe.get('taskQueueCount')
            valid_maximum = maximum is None or type(maximum) is int and maximum >= 1
            valid_queue = type(queued) is int and queued >= 0
            capacities[node.id] = dict(ready=probe.get('ready') is True and valid_maximum and valid_queue,
                maxImages=maximum if valid_maximum else None, maxInputBytes=node.max_input_bytes,
                taskQueueCount=queued if valid_queue else 1)
        return options, capacities

    def _discover_capacities(self):
        return self._discover_fleet()[1]

    def _node_options_valid(self, submission, node_snapshot):
        options = node_snapshot.get("options")
        if node_snapshot.get("available") is not True or type(options) is not list:
            return False
        selected = {"ready": True, "commonOptions": options, "nodes": [node_snapshot]}
        try:
            _, _, allowed = discovered_policy(selected, self.config)
            validate_discovered_option_values(submission,
                selected, allowed)
            return True
        except (p.ProtocolError, TypeError, ValueError):
            return False

    @contextmanager
    def _admit_read(self, binding):
        if binding["byteSize"] > self.settings.stream_budget["maxObjectBytes"] or not self._streams.acquire(blocking=False):
            raise RuntimeConfigurationError("result stream capacity unavailable")
        try:
            yield
        finally:
            self._streams.release()

    def start(self):
        if self._server is not None:
            if self.queue._coordinator_freeze.status()['phase']=='open':
                self._start_source_release()
            return
        if self.settings.source_spool is not None:
            with self.queue._coordinator_freeze.runtime_startup(self):
                self._source_readers = self.store.source_readers()
        self._viewer_transfer_stop.clear()
        listener = self.settings.listener
        server = self._server_factory(self.api, listener["host"], listener["port"], ssl_context=self._server_context)
        # Join active requests before releasing the shared queue's authority lock.
        server.daemon_threads, server.block_on_close = False, True
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.1},
                                  name="processing-reference-tls", daemon=True)
        self._server, self._thread = server, thread
        try:
            thread.start()
            if self._viewer_transfer_worker is not None:
                self._viewer_transfer_thread = threading.Thread(target=self._viewer_transfer_loop,
                    name="processing-viewer-acceptance", daemon=True)
                self._viewer_transfer_thread.start()
            self._start_source_release()
        except Exception:
            self._source_release_stop.set()
            if self._source_timer_thread is not None:
                self._source_timer_thread.join(timeout=1)
                if self._source_timer_thread.is_alive():
                    raise RuntimeConfigurationError('source release timer still draining; authority retained') from None
            if self._source_release_thread is not None:
                self._source_release_thread.join(timeout=1)
                if self._source_release_thread.is_alive():
                    raise RuntimeConfigurationError('source release I/O still draining; authority retained') from None
            self._viewer_transfer_stop.set()
            if self._viewer_transfer_thread is not None and self._viewer_transfer_thread.is_alive():
                self._viewer_transfer_thread.join()
            if thread.is_alive():
                server.shutdown()
                server.server_close()
                thread.join()
            else:
                server.server_close()
            self._server, self._thread = None, None
            raise RuntimeConfigurationError("reference listener start failed") from None

    def _viewer_transfer_loop(self):
        while not self._viewer_transfer_stop.is_set():
            from .coordinator_freeze import CoordinatorFrozen
            try:
                with self.store.queue.coordinator_operation('viewer-transfer'):
                    result = self._viewer_transfer_worker.run_once() if self._viewer_transfer_worker is not None else {'state':'idle'}
            except CoordinatorFrozen:
                result={'state':'idle'}
            wait = 0 if result.get("state") == "accepted" else (2 if result.get("state") == "idle" else 1)
            if self._viewer_transfer_stop.wait(wait):
                return

    def _start_source_release(self):
        if self._source_release is None or not self.settings.source_release.enabled:
            return
        self._source_release_stop.clear()
        if self._source_timer_thread is None or not self._source_timer_thread.is_alive():
            self._source_timer_thread=threading.Thread(target=self._source_timer_loop,
                name='processing-source-retention-clock',daemon=False)
            self._source_timer_thread.start()
        if self._source_release_thread is not None and self._source_release_thread.is_alive():
            return
        self._source_release_thread=threading.Thread(target=self._source_release_loop,
            name='processing-source-release',daemon=False)
        self._source_release_thread.start()

    def _source_release_loop(self):
        while not self._source_release_stop.is_set():
            progress=self._source_release.run_io_batch()
            if self._source_release_stop.wait(0.1 if progress else 2):
                return

    def _source_timer_loop(self):
        while not self._source_release_stop.is_set():
            self._source_release.checkpoint_timers()
            if self._source_release_stop.wait(30):
                return

    def drain_source_release(self):
        """Actual service shutdown cannot abandon its source I/O authority.

        The ordinary close remains bounded/refusable for an operator who can
        cancel maintenance. Once the owning service exits, it must instead stay
        alive until these same threads join, keeping Queue and source leases.
        """
        self._source_release_stop.set()
        for thread in (self._source_timer_thread,self._source_release_thread):
            if thread is not None:thread.join()

    def close(self):
        # Do not stop HTTP/delivery first: an unresponsive NAS operation still
        # owns its exact source pin and must remain attached to this runtime.
        self._source_release_stop.set()
        if self._source_timer_thread is not None:
            self._source_timer_thread.join(timeout=1)
            if self._source_timer_thread.is_alive():
                raise RuntimeConfigurationError('source release timer still draining; authority retained')
            self._source_timer_thread=None
        if self._source_release_thread is not None:
            self._source_release_thread.join(timeout=1)
            if self._source_release_thread.is_alive():
                raise RuntimeConfigurationError('source release I/O still draining; authority retained')
            self._source_release_thread=None
        if self._source_readers is not None and self._source_readers._handles:
            raise RuntimeConfigurationError('source I/O still draining; authority retained')
        self._viewer_transfer_stop.set()
        if self._viewer_transfer_thread is not None:
            self._viewer_transfer_thread.join()
            self._viewer_transfer_thread = None
        if self._server is not None:
            server, thread = self._server, self._thread
            self._server, self._thread = None, None
            server.shutdown()
            server.server_close()
            thread.join()
        if self._source_readers is not None:
            self._source_readers.close()
