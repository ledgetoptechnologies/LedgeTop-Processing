"""Private routing probe with an opt-in synthetic worker queue."""

import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path

from .config import Config, from_environment
from .policy import plan
from .provider import probe_node
from .prototype import Queue, Conflict
from .options import catalog

MAX_BODY = 8192


def make_handler(config: Config):
    reference_settings = None
    if config.reference_config_path:
        from .reference_runtime import load_reference_settings
        reference_settings = load_reference_settings(config.reference_config_path, config)
    queue = Queue(config.prototype_state_dir, config.photo_smoke_enabled,
                  node_ids=config.prototype_node_ids,
                  treedar_node_ids=config.treedar_node_ids,
                  viewer_enabled=config.viewer_enabled,
                  reference_enabled=reference_settings is not None) if config.prototype_state_dir else None
    service = facade = None
    if config.viewer_enabled:
        from .viewer_service import ViewerService
        from .viewer_http import ViewerFacade
        service = ViewerService(queue, config)
        spool = Path(config.prototype_state_dir) / 'viewer-spool'
        spool.mkdir(mode=0o700, exist_ok=True)
        facade = ViewerFacade(service, config.viewer_token, config.viewer_staging_token, temp_dir=spool)
    reference_runtime = None
    if reference_settings is not None:
        from .reference_runtime import ReferenceRuntime
        try:
            reference_runtime = ReferenceRuntime(queue, config, reference_settings)
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

        def do_GET(self):
            if facade and facade.handle(self):
                return
            if self.path.startswith("/v1/prototype/"):
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
                self._reply(200, {"status": "ok", "mode": "routing_probe",
                                  "syntheticPrototypeEnabled": queue is not None,
                                  "photoSmokeEnabled": config.photo_smoke_enabled,
                                  "viewerEnabled": config.viewer_enabled})
            elif self.path == '/v1/options':
                if self._authorized(observer=True):
                    snapshot = catalog([
                        ('local', config.local_node_url, config.local_node_token),
                        ('main', config.main_node_url, config.main_node_token)])
                    self._reply(200, {'mode': 'engine_option_discovery', 'executesWork': False,
                        'viewerNativeApiEnabled': config.viewer_enabled,
                        'viewerProviderReady': config.viewer_enabled and snapshot['ready'],
                        'snapshotConsistency': 'best_effort_read_only', **snapshot})
            elif self.path == "/v1/nodes":
                if self._authorized():
                    self._reply(200, {"nodes": self._nodes()})
            else:
                self._reply(404, {"error": "not_found"})

        def do_POST(self):
            if facade and facade.handle(self):
                return
            prototype = self.path.startswith("/v1/prototype/")
            if self.path != "/v1/plan" and not prototype:
                self._reply(404, {"error": "not_found"})
                return
            operator = prototype and (self.path.endswith(("/hold", "/resolve"))
                                      or self.path.startswith("/v1/prototype/nodes/"))
            if not self._authorized(operator=operator):
                return
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
                request = json.loads(self.rfile.read(length))
                if prototype:
                    if self.path == "/v1/prototype/jobs":
                        job, created = queue.submit(request)
                        self._reply(201 if created else 200, {"job": job})
                    elif self.path == "/v1/prototype/claim":
                        self._reply(200, {"job": queue.claim(request)})
                    elif self.path.startswith("/v1/prototype/nodes/"):
                        identifier, _, action = self.path[len("/v1/prototype/nodes/"):].rpartition("/")
                        methods = {"schedule": queue.schedule_node, "cancel": queue.cancel_node,
                                   "begin": queue.begin_node_update, "finish": queue.finish_node_update,
                                   "hold": queue.hold_node, "resume": queue.resume_node}
                        if action not in methods:
                            self._reply(404, {"error": "not_found"})
                        else:
                            result = methods[action](identifier, request)
                            self._reply(200, result if action == "begin" else {"node": result})
                    elif operator and self.path.startswith("/v1/prototype/jobs/"):
                        identifier, _, action = self.path[len("/v1/prototype/jobs/"):].rpartition("/")
                        self._reply(200, {"job": queue.operator(identifier, action, request)})
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
            local, main = self._nodes()
            decision = plan(photos, size, local, main,
                            config.local_max_photos, config.local_max_input_bytes)
            self._reply(200, {"mode": "routing_probe", "executesWork": False,
                              **decision, "nodes": [local, main]})

    return Handler


def main():
    config = from_environment()
    handler = make_handler(config)
    server = ThreadingHTTPServer((config.bind_host, config.port), handler)
    if handler.reference_listener:
        server.daemon_threads, server.block_on_close = False, True
    try:
        if handler.viewer_service:
            handler.viewer_service.start()
        if handler.reference_listener:
            handler.reference_listener.start()
        print(f"LedgeTop Processing listening on {config.bind_host}:{config.port}", flush=True)
        server.serve_forever()
    finally:
        if handler.reference_listener:
            handler.reference_listener.close()
        server.server_close()
        if handler.viewer_service:
            handler.viewer_service.close()
        if handler.prototype_queue:
            handler.prototype_queue.close()


if __name__ == "__main__":
    main()
