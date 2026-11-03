"""Private routing probe with an opt-in synthetic worker queue."""

import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import signal
import sys
import threading

from .config import Config, from_environment
from .policy import plan
from .provider import probe_node
from .prototype import Queue, Conflict
from .options import catalog

MAX_BODY = 8192


def make_handler(config: Config):
    if config.per_node_slots and not config.node_inventory:
        from .config import require_distinct_node_hosts
        require_distinct_node_hosts(config.local_node_url, config.main_node_url)
    reference_settings = None
    if config.reference_config_path:
        from .reference_runtime import load_reference_settings
        reference_settings = load_reference_settings(config.reference_config_path, config)
    if config.viewer_production_held:
        if not config.prototype_state_dir:
            raise RuntimeError('production_hold_idle_state_unavailable')
        from .prototype import _require_safe_idle_database_file
        _require_safe_idle_database_file(Path(config.prototype_state_dir) / 'jobs.sqlite3')
    lease = None
    if config.prototype_state_dir:
        from . import authority_lease
        if config.authority_lease_dir:
            # Before any state write: a second holder of this authority refuses here.
            lease = authority_lease.acquire(config.authority_lease_dir, config.prototype_state_dir)
        elif authority_lease.required(config.prototype_state_dir):
            raise RuntimeError('authority_lease_configuration_required')
    try:
        queue = Queue(config.prototype_state_dir, config.photo_smoke_enabled,
                      node_ids=config.prototype_node_ids,
                      treedar_node_ids=config.treedar_node_ids,
                      viewer_enabled=config.viewer_enabled,
                      reference_enabled=reference_settings is not None,
                      require_idle_existing=config.viewer_production_held,
                      per_node_slots=config.per_node_slots,
                      node_capabilities={n['id']: n['capabilities'] for n in config.node_inventory} if config.node_inventory else None) if config.prototype_state_dir else None
    except BaseException:
        if lease is not None:
            lease.close()
        raise
    if config.viewer_production_held and queue is None:
        raise RuntimeError('production_hold_idle_state_unavailable')
    if lease is not None:
        try:
            authority_lease.record_required(queue, lease)
        except Exception:
            queue.close()
            raise
        queue.authority_lease = lease  # held, by this process, until it exits
    if queue is not None and config.admission_drain_enabled:
        from .admission_drain import AdmissionDrain
        try:
            queue.admission_drain = AdmissionDrain(queue)
        except Exception:
            queue.close()
            raise
    elif queue is not None:
        # Never silently bypass a previously enabled persistent barrier.
        with queue._db() as db:
            exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='coordinator_admission'").fetchone()
        if exists:
            queue.close()
            raise RuntimeError('admission_drain_configuration_required')
    service = facade = None
    if config.viewer_enabled:
        from .viewer_service import ViewerService
        from .viewer_http import ViewerFacade
        service = ViewerService(queue, config)
        spool = Path(config.prototype_state_dir) / 'viewer-spool'
        spool.mkdir(mode=0o700, exist_ok=True)
        facade = ViewerFacade(service, config.viewer_token, config.viewer_staging_token, temp_dir=spool,
                              production_held=config.viewer_production_held)
    reference_runtime = None
    if reference_settings is not None:
        from .reference_runtime import ReferenceRuntime
        try:
            reference_runtime = ReferenceRuntime(queue, config, reference_settings, viewer_service=service)
        except Exception:
            if service:
                service.close()
            if queue:
                queue.close()
            raise

    class Handler(BaseHTTPRequestHandler):
        server_version = "LedgeTopProcessing/0.1"
        prototype_queue = queue
        viewer_service = service
        reference_listener = reference_runtime
        lifecycle_service = None

        def setup(self):
            super().setup()
            self.connection.settimeout(10)

        def log_message(self, *_):
            # Request paths/headers and opaque claim credentials stay out of logs.
            pass

        def _reply(self, status: int, payload: dict):
            body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _authorized(self, operator=False, observer=False) -> bool:
            value = self.headers.get("Authorization", "")
            token = config.prototype_operator_token if operator else config.api_token
            if not token:
                self._reply(403, {"error": "operator_actions_disabled"})
                return False
            expected = f"Bearer {token}"
            if observer and config.prototype_observer_token and hmac.compare_digest(
                    value.encode(), f'Bearer {config.prototype_observer_token}'.encode()):
                return True
            if not hmac.compare_digest(value.encode(), expected.encode()):
                self._reply(401, {"error": "unauthorized"})
                return False
            return True

        def _nodes(self) -> tuple[dict, dict]:
            return (probe_node("local", config.local_node_url, config.local_node_token),
                    probe_node("main", config.main_node_url, config.main_node_token))

        def _inventory_nodes(self):
            if queue is None:
                raise Conflict('inventory queue unavailable')
            jobs = queue.list()
            occupied = {j['nodeId'] for j in jobs if j['status'] in ('running', 'held')}
            if any(node not in queue.node_ids for node in occupied):
                raise Conflict('inventory occupancy unavailable')
            result = []
            for spec in config.node_inventory:
                state = queue.get_node(spec['id'])
                if state is None:
                    raise Conflict('inventory node state unavailable')
                report = (probe_node(spec['id'], spec['origin'], spec['token']) if 'odm' in spec['capabilities']
                          else {'role': spec['id'], 'configured': True, 'ready': False,
                                'reason': 'health_probe_unsupported', 'maxImages': None, 'taskQueueCount': None})
                result.append({**report, 'id': spec['id'], 'physicalHostId': spec['physicalHostId'],
                    'maxPhotos': spec['maxPhotos'], 'capabilities': list(spec['capabilities']),
                    'state': state['state'], 'occupied': spec['id'] in occupied})
            return result

        def do_GET(self):
            if facade and facade.handle(self):
                return
            if self.path == '/v1/prototype/admission-drain':
                if not self._authorized(operator=True):return
                gate = getattr(queue, 'admission_drain', None)
                if gate is None:self._reply(404, {'error': 'not_found'});return
                self._reply(200, gate.status());return
            if self.path.startswith("/v1/prototype/"):
                prefix='/v1/prototype/nodes/'
                if self.path.startswith(prefix) and '/schedule-owned' in self.path[len(prefix):]:
                    if not self._authorized(operator=True):return
                    if queue is None:
                        self._reply(404,{'error':'prototype_disabled'});return
                    parts=self.path[len(prefix):].split('/')
                    if len(parts)!=3 or parts[1]!='schedule-owned':
                        self._reply(404,{'error':'not_found'});return
                    try:
                        receipt=queue.owned_schedule_receipt(parts[0],parts[2])
                        self._reply(200 if receipt is not None else 404,
                            {'receipt':receipt} if receipt is not None else {'error':'owned_schedule_not_found'})
                    except Conflict:
                        self._reply(409,{'error':'conflict'})
                    except ValueError:
                        self._reply(400,{'error':'invalid_request'})
                    return
                if not self._authorized(observer=True):
                    return
                if queue is None:
                    self._reply(404, {"error": "prototype_disabled"})
                elif self.path == "/v1/prototype/authority":
                    self._reply(200, {"authorityId": queue.authority_id})
                elif self.path == "/v1/prototype/jobs":
                    self._reply(200, {"jobs": queue.list(), "mode": "bounded_photo_test" if config.photo_smoke_enabled else "synthetic_prototype"})
                elif self.path == "/v1/prototype/nodes":
                    self._reply(200, {"nodes": queue.list_nodes()})
                elif self.path.startswith("/v1/prototype/nodes/"):
                    try:
                        node = queue.get_node(self.path.removeprefix("/v1/prototype/nodes/"))
                        self._reply(200 if node else 404, {"node": node})
                    except ValueError:
                        self._reply(404, {"error": "unknown_node"})
                elif self.path.startswith("/v1/prototype/jobs/"):
                    job = queue.get(self.path.removeprefix("/v1/prototype/jobs/"))
                    self._reply(200 if job else 404, {"job": job})
                else:
                    self._reply(404, {"error": "not_found"})
                return
            if self.path == "/healthz":
                ready = self.lifecycle_service is None or self.lifecycle_service.healthy()
                self._reply(200 if ready else 503, {"status": "ok" if ready else "unavailable", "mode": "routing_probe",
                                  "syntheticPrototypeEnabled": queue is not None,
                                  "photoSmokeEnabled": config.photo_smoke_enabled,
                                  "viewerEnabled": config.viewer_enabled})
            elif self.path in ('/v1/options', '/v2/options'):
                if self._authorized(observer=True):
                    if self.path == '/v2/options' and not config.node_inventory:
                        self._reply(404, {'error': 'not_found'}); return
                    if self.path == '/v1/options' and config.node_inventory:
                        self._reply(503, {'error': 'use_inventory_v2'}); return
                    if config.node_inventory:
                        nodes = [(n['id'], n['origin'], n['token']) for n in config.node_inventory if 'odm' in n['capabilities']]
                        if not nodes:
                            self._reply(503, {'error': 'inventory_diagnostic_unavailable'}); return
                        snapshot = catalog(nodes, inventory=True)
                        self._reply(200, {'mode': 'engine_option_discovery', 'executesWork': False,
                            'viewerNativeApiEnabled': config.viewer_enabled,
                            'viewerProviderReady': config.viewer_enabled and snapshot['ready'],
                            'snapshotConsistency': 'best_effort_read_only', **snapshot}); return
                    snapshot = catalog([
                        ('local', config.local_node_url, config.local_node_token),
                        ('main', config.main_node_url, config.main_node_token)])
                    self._reply(200, {'mode': 'engine_option_discovery', 'executesWork': False,
                        'viewerNativeApiEnabled': config.viewer_enabled,
                        'viewerProviderReady': config.viewer_enabled and snapshot['ready'],
                        'snapshotConsistency': 'best_effort_read_only', **snapshot})
            elif self.path in ("/v1/nodes", "/v2/nodes"):
                if self._authorized():
                    if self.path == '/v2/nodes' and not config.node_inventory:
                        self._reply(404, {'error': 'not_found'}); return
                    if self.path == '/v1/nodes' and config.node_inventory:
                        self._reply(503, {'error': 'use_inventory_v2'}); return
                    if config.node_inventory:
                        try:
                            self._reply(200, {'nodes': self._inventory_nodes()})
                        except Conflict:
                            self._reply(503, {'error': 'inventory_diagnostic_unavailable'})
                        return
                    self._reply(200, {"nodes": self._nodes()})
            else:
                self._reply(404, {"error": "not_found"})

        def do_POST(self):
            if facade and facade.handle(self):
                return
            prototype = self.path.startswith("/v1/prototype/")
            if self.path not in ("/v1/plan", "/v2/plan") and not prototype:
                self._reply(404, {"error": "not_found"})
                return
            operator = prototype and (self.path == '/v1/prototype/admission-drain' or self.path.endswith(("/hold", "/resolve"))
                                      or self.path.startswith("/v1/prototype/nodes/"))
            if not self._authorized(operator=operator):
                return
            if self.path == '/v1/plan' and config.node_inventory:
                self._reply(503, {'error': 'use_inventory_v2'}); return
            if self.path == '/v2/plan' and not config.node_inventory:
                self._reply(404, {'error': 'not_found'}); return
            if prototype and queue is None:
                self._reply(404, {"error": "prototype_disabled"})
                return
            if self.headers.get_content_type() != "application/json":
                self._reply(415, {"error": "json_required"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                length = -1
            if length < 1 or length > MAX_BODY or self.headers.get("Transfer-Encoding"):
                self._reply(413, {"error": "invalid_body_size"})
                return
            try:
                raw=self.rfile.read(length)
                if (self.path == '/v1/prototype/admission-drain' or
                        (self.path.startswith('/v1/prototype/nodes/') and self.path.endswith('/schedule-owned'))):
                    def unique(pairs):
                        value={}
                        for key,item in pairs:
                            if key in value:raise ValueError('duplicate JSON field')
                            value[key]=item
                        return value
                    request=json.loads(raw,object_pairs_hook=unique)
                else:
                    request=json.loads(raw)
                if prototype:
                    if self.path == '/v1/prototype/admission-drain':
                        if not hasattr(queue, 'admission_drain'):
                            self._reply(404, {'error': 'not_found'}); return
                        if not isinstance(request, dict) or set(request) != {'closed','requestId','expectedGeneration','expectedClosed','expectedAuthorityId'}:
                            raise ValueError('exact admission request required')
                        self._reply(200, queue.admission_drain.set_closed(request['closed'],request_id=request['requestId'],expected_generation=request['expectedGeneration'],expected_closed=request['expectedClosed'],expected_authority=request['expectedAuthorityId']))
                    elif self.path == "/v1/prototype/jobs":
                        from .admission_drain import AdmissionClosed
                        try:
                            from contextlib import nullcontext
                            gate = getattr(queue, 'admission_drain', None)
                            with gate.admit() if gate is not None else nullcontext():
                                job, created = queue.submit(request)
                        except AdmissionClosed:
                            self._reply(503, {'error': 'admission_draining'}); return
                        self._reply(201 if created else 200, {"job": job})
                    elif self.path == "/v1/prototype/claim":
                        self._reply(200, {"job": queue.claim(request)})
                    elif self.path.startswith("/v1/prototype/nodes/"):
                        identifier, _, action = self.path[len("/v1/prototype/nodes/"):].rpartition("/")
                        methods = {"schedule": queue.schedule_node, "schedule-owned":queue.schedule_node_owned,"cancel": queue.cancel_node,
                                   "begin": queue.begin_node_update, "finish": queue.finish_node_update,
                                   "hold": queue.hold_node, "resume": queue.resume_node}
                        if action not in methods:
                            self._reply(404, {"error": "not_found"})
                        else:
                            result = methods[action](identifier, request)
                            self._reply(200,result if action=='begin' else {'receipt':result} if action=='schedule-owned' else {'node':result})
                    elif operator and self.path.startswith("/v1/prototype/jobs/"):
                        identifier, _, action = self.path[len("/v1/prototype/jobs/"):].rpartition("/")
                        transition = service.operator if service else queue.operator
                        self._reply(200, {"job": transition(identifier, action, request)})
                    elif self.path.startswith("/v1/prototype/jobs/") and self.path.endswith("/complete"):
                        identifier = self.path[len("/v1/prototype/jobs/"):-len("/complete")]
                        self._reply(200, {"job": queue.complete(identifier, request)})
                    else:
                        self._reply(404, {"error": "not_found"})
                    return
                if not isinstance(request, dict) or request.get("engine") != "odm":
                    raise ValueError("only the ODM planning contract is supported")
                photos, size = request.get("photoCount"), request.get("inputBytes")
                if (type(photos) is not int or not 1 <= photos <= 1_000_000
                        or type(size) is not int or not 1 <= size <= 10**15):
                    raise ValueError("photoCount and inputBytes must be positive integers")
            except Conflict as exc:
                self._reply(409, {"error": "conflict", "detail": str(exc)})
                return
            except (ValueError, TypeError, RecursionError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                self._reply(400, {"error": "invalid_request", "detail": str(exc)[:160]})
                return
            if config.node_inventory:
                try:
                    nodes = self._inventory_nodes()
                except Conflict:
                    self._reply(503, {'error': 'inventory_diagnostic_unavailable'}); return
                selected = None
                for node in nodes:
                    maximum, actual = node['maxPhotos'], node.get('maxImages')
                    if ('odm' not in node['capabilities'] or node['occupied'] or node['state'] != 'active'
                            or node.get('ready') is not True or type(node.get('taskQueueCount')) is not int
                            or node['taskQueueCount'] != 0 or 'maxImages' not in node
                            or (actual is not None and (type(actual) is not int or actual < 1))):
                        continue
                    if any(value is not None and photos > value for value in (maximum, actual)):
                        continue
                    selected = node['id']; break
                self._reply(200, {'mode': 'routing_probe', 'executesWork': False,
                    'decision': 'node' if selected else 'wait', 'nodeId': selected,
                    'reason': 'first_eligible' if selected else 'no_eligible_node', 'nodes': nodes}); return
            local, main = self._nodes()
            decision = plan(photos, size, local, main,
                            config.local_max_photos, config.local_max_input_bytes)
            self._reply(200, {"mode": "routing_probe", "executesWork": False,
                              **decision, "nodes": [local, main]})

    return Handler


class CoordinatorService:
    """Supervise listeners and drain every queue consumer before unlocking state.

    Signals only set an Event. Shutdown runs outside serve_forever's thread;
    in-flight streams/mutations finish naturally, without timeout abandonment.
    """
    def __init__(self, server, handler):
        self.server, self.handler = server, handler
        self.server.daemon_threads, self.server.block_on_close = False, True
        self._lock = threading.Lock()
        self._state, self._reason = 'starting', None
        self._native_thread = None
        self._reference_server = self._reference_thread = None
        handler.lifecycle_service = self

    def _failed(self, reason, stop):
        with self._lock:
            self._state, self._reason = 'failed', reason
        stop.set()

    def health(self):
        reference = self.handler.reference_listener
        with self._lock:
            return dict(state=self._state, reason=self._reason,
                nativeAlive=self._native_thread is not None and self._native_thread.is_alive(),
                referenceAlive=reference is not None and self._reference_thread is not None and self._reference_thread.is_alive())

    def healthy(self):
        health = self.health()
        return (health['state'] == 'ready' and health['nativeAlive']
                and (self.handler.reference_listener is None or health['referenceAlive']))

    def _start_reference(self, stop):
        runtime = self.handler.reference_listener
        if runtime is None:
            return
        # The runtime owns start/close; wrap only this instance's server factory
        # so an unexpected serving exception is sanitized and fails supervision.
        factory = runtime._server_factory

        def supervised_factory(*args, **kwargs):
            server = factory(*args, **kwargs)
            serve = server.serve_forever

            def supervised(*serve_args, **serve_kwargs):
                try:
                    serve(*serve_args, **serve_kwargs)
                    if not stop.is_set():
                        self._failed('reference_listener_exited', stop)
                except BaseException:
                    self._failed('reference_listener_failed', stop)

            server.serve_forever = supervised
            self._reference_server = server
            return server

        runtime._server_factory = supervised_factory
        try:
            runtime.start()
            self._reference_thread = runtime._thread
        finally:
            runtime._server_factory = factory

    def _close_server(self, server, thread, stop):
        if server is None:
            return
        if thread is not None:
            if thread.is_alive():
                try:
                    server.shutdown()
                except BaseException:
                    self._failed('listener_drain_failed', stop)
            thread.join()
        try:
            server.server_close()  # ThreadingMixIn joins active non-daemon handlers.
        except BaseException:
            self._failed('listener_drain_failed', stop)
            handlers = getattr(server, '_threads', None)
            if handlers is not None:
                handlers.join()

    def run(self, stop):
        if self._native_thread is not None:
            raise ValueError('coordinator service already started')

        def serve_native():
            try:
                self.server.serve_forever(poll_interval=0.1)
                if not stop.is_set():
                    self._failed('native_listener_exited', stop)
            except BaseException:
                self._failed('native_listener_failed', stop)

        self._native_thread = threading.Thread(target=serve_native, name='processing-native-http', daemon=False)
        native_started = False
        try:
            if self.handler.viewer_service:
                self.handler.viewer_service.start()
            self._start_reference(stop)
            self._native_thread.start()
            native_started = True
            with self._lock:
                if self._state != 'failed':
                    self._state = 'ready'
            while not stop.wait(0.1):
                if not self._native_thread.is_alive():
                    self._failed('native_listener_exited', stop)
                if self.handler.reference_listener is not None and (
                        self._reference_thread is None or not self._reference_thread.is_alive()):
                    self._failed('reference_listener_exited', stop)
                if (self.handler.reference_listener is not None and
                        not self.handler.reference_listener.readiness_healthy()):
                    self._failed('reference_readiness_exited', stop)
        except BaseException:
            self._failed('coordinator_start_failed', stop)
        finally:
            stop.set()
            with self._lock:
                if self._state != 'failed':
                    self._state = 'draining'
            service = self.handler.viewer_service
            if service is not None and hasattr(service, '_stop'):
                service._stop.set()  # Prevent another dispatcher tick during HTTP drain.
            # Stop admission on BOTH listeners before waiting for either set of
            # handlers. A slow native upload must not leave reference admission
            # open throughout the drain (or vice versa).
            for listener, thread in ((self.server, self._native_thread if native_started else None),
                                     (self._reference_server, self._reference_thread)):
                if listener is not None and thread is not None and thread.is_alive():
                    try:
                        listener.shutdown()
                    except BaseException:
                        self._failed('listener_drain_failed', stop)
            self._close_server(self.server, self._native_thread if native_started else None, stop)
            runtime = self.handler.reference_listener
            if runtime is not None:
                try:
                    runtime.close()
                except BaseException:
                    self._failed('reference_drain_failed', stop)
                self._close_server(self._reference_server, self._reference_thread, stop)
            if service is not None:
                try:
                    service.close()
                except BaseException:
                    self._failed('dispatcher_drain_failed', stop)
                thread = getattr(service, '_thread', None)
                if thread is not None:
                    thread.join()  # close()'s legacy ten-second join is insufficient.
                service.wait_for_transfers()  # Keep queue authority until senders actually exit.
            if self.handler.prototype_queue:
                self.handler.prototype_queue.close()
            with self._lock:
                failed = self._state == 'failed'
                if not failed:
                    self._state = 'stopped'
        return 2 if failed else 0


def main():
    config = from_environment()
    handler = make_handler(config)
    server = ThreadingHTTPServer((config.bind_host, config.port), handler)
    stop = threading.Event()
    for name in ('SIGINT','SIGTERM'):
        if hasattr(signal, name):
            signal.signal(getattr(signal, name), lambda *_args: stop.set())
    print(f"LedgeTop Processing listening on {config.bind_host}:{config.port}", flush=True)
    service = CoordinatorService(server, handler)
    result = service.run(stop)
    if result:
        print('LedgeTop Processing unavailable: ' + service.health()['reason'], file=sys.stderr, flush=True)
    return result


if __name__ == "__main__":
    raise SystemExit(main())
