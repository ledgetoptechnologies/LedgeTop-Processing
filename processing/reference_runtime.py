"""Explicit optional TLS reference listener sharing the native queue authority.

Configuration is a private JSON file, never environment-serialized credentials.
No certificates, credentials, worker daemons, or cleanup are created here. The
discovered allowlist admits typed booleans, numbers, bounded enum choices and
bounded free text. Path/file options remain closed until a registered file
workflow exists.
"""
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import json
import math
import os
from pathlib import Path
import re
import ssl
import stat
import threading

from . import reference_protocol as p
from .options import catalog
from .provider import probe_node
from .reference_api import ReferenceAPI, Response
from .reference_control import ReferenceControlAPI
from .reference_http import create_reference_http_server
from .reference_result_proxy import ResultProxy
from .reference_store import ReferenceStore
from .reference_tls import verified_client_context
from .reference_recovery import RecoveryNodeBinding
from .config import normalized_node_origin

MAX_CONFIG_BYTES = 64 * 1024
_UNSAFE = re.compile(r"\b(file|filename|path|directory|folder|url|uri|endpoint|host|hostname|"
                     r"token|credential|secret|password|key|script|command|upload|download)\b", re.I)
_OPTION_BOUND = re.compile(
    r"^(?:int|integer|float|number|x)\s*(>=|>|<=|<)\s*"
    r"(-?(?:\d+(?:\.\d*)?|\.\d+)(?:e[+-]?\d+)?)$", re.I)
_OPTION_RANGE = re.compile(
    r"^(-?(?:\d+(?:\.\d*)?|\.\d+))\s*(<=|<)\s*"
    r"(?:x|int|integer|float|number)\s*(<=|<)\s*"
    r"(-?(?:\d+(?:\.\d*)?|\.\d+))$", re.I)
_FILE_OPTIONS = frozenset(("align", "cameras", "boundary", "geo", "gcp", "image-groups",
                           "input", "output", "project-path"))


def _option_domain(option):
    """Return a normalized Viewer-compatible domain, or None when malformed."""
    domain = option.get("domain")
    if domain is None or (type(domain) is str and not domain.strip()):
        return {}
    if type(domain) is str:
        try:
            domain = json.loads(domain)
        except (ValueError, TypeError):
            text = domain.strip().lower()
            if re.fullmatch(r"positive (?:integer|int|float|number)", text):
                return {"min": 0, "minExclusive": True}
            if re.fullmatch(r"non[- ]?negative (?:integer|int|float|number)", text):
                return {"min": 0}
            bound = _OPTION_BOUND.fullmatch(text)
            if bound:
                value = float(bound[2])
                if not math.isfinite(value):
                    return None
                return ({"min": value, "minExclusive": bound[1] == ">"}
                        if bound[1].startswith(">") else
                        {"max": value, "maxExclusive": bound[1] == "<"})
            interval = _OPTION_RANGE.fullmatch(text)
            if interval:
                low, high = float(interval[1]), float(interval[4])
                if math.isfinite(low) and math.isfinite(high) and low <= high:
                    return {"min": low, "max": high,
                            "minExclusive": interval[2] == "<",
                            "maxExclusive": interval[3] == "<"}
            # Unrecognized prose is display metadata, never an expression to
            # evaluate. As in the Viewer, it adds no constraints; path-like
            # definitions are rejected separately by the option restriction.
            return {}
    if type(domain) is list:
        if len(domain) > 1000 or any(type(value) not in (str, bool, int, float)
                or (type(value) is float and not math.isfinite(value)) for value in domain):
            return None
        if option.get("type") in ("string", "enum") and any(type(value) is not str for value in domain):
            return None
        return {"values": domain}
    if type(domain) is dict and set(domain) <= {"min", "max"}:
        result = {}
        for key in ("min", "max"):
            if key in domain:
                value = domain[key]
                if type(value) not in (int, float) or not math.isfinite(value):
                    return None
                result[key] = value
        if "min" in result and "max" in result and result["min"] > result["max"]:
            return None
        return result
    return None


def _value_matches_domain(value, kind, domain):
    if "values" in domain:
        def same_choice(choice):
            if type(value) in (int, float) and type(choice) in (int, float):
                return value == choice
            return type(value) is type(choice) and value == choice
        if not any(same_choice(choice) for choice in domain["values"]):
            return False
    if not ("min" in domain or "max" in domain):
        return True
    numeric = value
    if kind == "string":
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return False
    if type(numeric) not in (int, float) or not math.isfinite(numeric):
        return False
    if "min" in domain and (numeric <= domain["min"] if domain.get("minExclusive") else numeric < domain["min"]):
        return False
    if "max" in domain and (numeric >= domain["max"] if domain.get("maxExclusive") else numeric > domain["max"]):
        return False
    return True


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
class ReferenceSettings:
    listener: dict
    viewer_bearers: dict
    workers: dict
    stream_budget: dict
    recovery_bindings: dict | None = None


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
        if type(value.get("schemaVersion")) is not int or value["schemaVersion"] not in (1, 2):
            raise RuntimeConfigurationError("unsupported runtime configuration version")
        node_ids = _validate_node_configuration(config)
        if len(node_ids) > 1 and value["schemaVersion"] != 2:
            raise RuntimeConfigurationError(
                "multi-node dispatch requires daemon identity bindings; these do not prove physical-host identity")
        fields=("schemaVersion", "listener", "viewerBearers", "workers", "streamBudget")
        p._closed(value, fields + (("recoveryBindings",) if value["schemaVersion"]==2 else ()), "runtime configuration")
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
        if len(set(credentials)) != len(credentials) or set(credentials) & legacy:
            raise RuntimeConfigurationError("reference credentials must be disjoint from existing credentials")
        budget = value["streamBudget"]
        p._closed(budget, ("maxConcurrent", "maxObjectBytes"), "stream budget")
        if type(budget["maxConcurrent"]) is not int or not 1 <= budget["maxConcurrent"] <= 16:
            raise RuntimeConfigurationError("invalid stream concurrency budget")
        p._integer(budget["maxObjectBytes"], "stream object budget")
        recovery = None
        if value["schemaVersion"] == 2:
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
        return ReferenceSettings(dict(listener), {token: env for env, token in viewers.items()},
                                 {node: dict(worker) for node, worker in workers.items()}, dict(budget), recovery)
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
    options = []
    for option in snapshot["commonOptions"]:
        name = option.get("name")
        option_type = option.get("type")
        domain = _option_domain(option)
        supported = option_type in ("bool", "int", "float", "enum", "string") and domain is not None
        if option_type in ("enum", "string") and domain is not None and "values" in domain:
            choices = domain["values"]
            supported = supported and bool(choices) and len(choices) <= 1000 and all(
                type(value) is str and len(value) <= 4000
                and not re.search(
                    r"[A-Za-z][A-Za-z0-9+.-]*://|(?:^|[\\s=])[A-Za-z]:[\\\\/]"
                    r"|(?:^|[\\s=])[\\\\/]|(?:^|[\\\\/])\\.\\.(?:[\\\\/]|$)", value)
                and "\\\\" not in value for value in choices)
        if option_type in ("string", "enum"):
            description = json.dumps(option.get("domain", ""), ensure_ascii=False) + " " + str(option.get("help", ""))
            supported = supported and not (name in _FILE_OPTIONS or _UNSAFE.search(description))
        if (type(name) is not str or not re.fullmatch(r"[a-z][a-z0-9-]{0,79}", name)
                or not supported
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


def validate_discovered_option_values(payload, snapshot, allowed):
    """Enforce live option types and enum choices at the submission boundary."""
    try:
        values = payload["pipeline"]["odm"]["options"]
        if type(values) is not dict:
            raise ValueError
        definitions = {item["name"]: item for item in snapshot["commonOptions"]
                       if item.get("name") in allowed}
        for name, value in values.items():
            definition = definitions.get(name)
            if definition is None:
                raise ValueError
            kind = definition["type"]
            domain = _option_domain(definition)
            if domain is None:
                raise ValueError
            if kind == "bool":
                valid = type(value) is bool
            elif kind == "int":
                valid = type(value) is int
            elif kind == "float":
                valid = (type(value) in (int, float) and math.isfinite(value)
                         and abs(value) <= p.MAX_SAFE_INTEGER)
            elif kind == "enum":
                valid = type(value) is str and value in domain.get("values", [])
            elif kind == "string":
                valid = type(value) is str and len(value) <= 4000
            else:
                valid = False
            if valid:
                valid = _value_matches_domain(value, kind, domain)
            if not valid:
                raise ValueError
    except (KeyError, TypeError, ValueError, OverflowError):
        raise p.ProtocolError("ODM option value does not match the current engine definition") from None


class _DynamicAPI:
    def __init__(self, control, config, settings, discover, discover_capacities):
        self.control, self.config, self.settings, self.discover = control, config, settings, discover
        self.discover_capacities = discover_capacities
        self._worker_refresh_lock = threading.RLock()

    def handle(self, method, path, headers, body=b"", *, protected_transport=False):
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
                        options = self.discover()
                        capacities = self.discover_capacities()
                        self.control.worker.store.update_fleet_snapshot(capacities, options['nodes'])
                    except Exception:
                        return Response(503, {"error": "engine_capacity_unavailable"})
                    return self.control.handle(method, path, headers, body,
                                               protected_transport=protected_transport)
            return self.control.handle(method, path, headers, body,
                                       protected_transport=protected_transport)
        # Authenticate before engine discovery; ordinary status and worker actions
        # never depend on live catalog reads. Each admission sees a fresh snapshot.
        if path in (p.PREFIX + '/options', p.PREFIX + "/capabilities", p.PREFIX + "/jobs") and method in ("GET", "POST"):
            if protected_transport is not True:
                return Response(503, {"error": "protected_transport_required"})
            environment = self.control.viewer._authenticate(headers)
            if environment is None:
                return Response(401, {"error": "unauthorized"})
            try:
                snapshot = _available_discovery_snapshot(self.discover())
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
            if path == p.PREFIX + "/jobs" and method == "POST":
                try:
                    payload = p.decode_submission(body)
                    validate_discovered_option_values(payload, snapshot, allowed)
                except (p.ProtocolError, TypeError, ValueError):
                    return Response(400, {"error": "invalid_request"})
            return api.handle(method, path, headers, body, protected_transport=protected_transport)
        return self.control.handle(method, path, headers, body, protected_transport=protected_transport)


class ReferenceRuntime:
    def __init__(self, queue, config, settings, *, discover=None, discover_capacities=None,
                 server_factory=None):
        """Construct services around the existing authority; never create a Queue."""
        self.queue, self.config, self.settings = queue, config, settings
        self._server_factory = server_factory or create_reference_http_server
        self._server, self._thread = None, None
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
            node_limits = {node.id: {"maxInputBytes": node.max_input_bytes}
                           for node in endpoints if node.max_input_bytes is not None}
            self.store = ReferenceStore(queue, local_max_photos=config.local_max_photos,
                local_max_input_bytes=config.local_max_input_bytes,
                main_node_id=next((node.id for node in endpoints if node.id == "main"),
                                  next((node.id for node in endpoints if node.id != "cluster"), None)),
                node_limits=node_limits,
                node_option_validator=self._node_options_valid)
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
                recovery_bindings=recovery)
            discover = discover or (lambda: catalog([
                (node.id, node.url, node.token) for node in config.node_endpoints]))
            discover_capacities = discover_capacities or self._discover_capacities
            self.api = _DynamicAPI(self.control, config, settings, discover, discover_capacities)
        except Exception:
            raise RuntimeConfigurationError("reference TLS runtime initialization failed") from None

    def _discover_capacities(self):
        endpoints = self.config.node_endpoints
        with ThreadPoolExecutor(max_workers=len(endpoints)) as executor:
            probes = list(executor.map(lambda node: probe_node(node.id, node.url, node.token), endpoints))
        capacities = {}
        for node, probe in zip(endpoints, probes):
            queue_count = probe.get("taskQueueCount")
            max_images = probe.get("maxImages")
            valid_max_images = (max_images is None
                or (type(max_images) is int and max_images >= 1))
            valid_queue_count = type(queue_count) is int and queue_count >= 0
            capacities[node.id] = {
                # A malformed or partial probe disables only this node. Do
                # not let it invalidate healthy nodes' complete fleet view,
                # and never coerce an unknown task count to idle.
                "ready": (probe.get("ready") is True and valid_max_images
                          and valid_queue_count),
                "maxImages": max_images if valid_max_images else None,
                "maxInputBytes": node.max_input_bytes,
                "taskQueueCount": queue_count if valid_queue_count else 1,
            }
        return capacities

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
            return
        listener = self.settings.listener
        server = self._server_factory(self.api, listener["host"], listener["port"], ssl_context=self._server_context)
        # Join active requests before releasing the shared queue's authority lock.
        server.daemon_threads, server.block_on_close = False, True
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.1},
                                  name="processing-reference-tls", daemon=True)
        self._server, self._thread = server, thread
        try:
            thread.start()
        except Exception:
            self._server, self._thread = None, None
            server.server_close()
            raise RuntimeConfigurationError("reference listener start failed") from None

    def close(self):
        if self._server is None:
            return
        server, thread = self._server, self._thread
        self._server, self._thread = None, None
        server.shutdown()
        server.server_close()
        thread.join()
