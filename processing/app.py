"""Private, read-only HTTP API for routing and compatibility trials."""

import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json

from .config import Config, from_environment
from .policy import plan
from .provider import probe_node

MAX_BODY = 8192


def make_handler(config: Config):
    class Handler(BaseHTTPRequestHandler):
        server_version = "LedgeTopProcessing/0.1"

        def _reply(self, status: int, payload: dict):
            body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _authorized(self) -> bool:
            value = self.headers.get("Authorization", "")
            expected = f"Bearer {config.api_token}"
            if not hmac.compare_digest(value, expected):
                self._reply(401, {"error": "unauthorized"})
                return False
            return True

        def _nodes(self) -> tuple[dict, dict]:
            return (probe_node("local", config.local_node_url),
                    probe_node("main", config.main_node_url))

        def do_GET(self):
            if self.path == "/healthz":
                self._reply(200, {"status": "ok", "mode": "routing_probe"})
            elif self.path == "/v1/nodes":
                if self._authorized():
                    self._reply(200, {"nodes": self._nodes()})
            else:
                self._reply(404, {"error": "not_found"})

        def do_POST(self):
            if self.path != "/v1/plan":
                self._reply(404, {"error": "not_found"})
                return
            if not self._authorized():
                return
            if self.headers.get_content_type() != "application/json":
                self._reply(415, {"error": "json_required"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                length = -1
            if length < 1 or length > MAX_BODY:
                self._reply(413, {"error": "invalid_body_size"})
                return
            try:
                request = json.loads(self.rfile.read(length))
                if not isinstance(request, dict) or request.get("engine") != "odm":
                    raise ValueError("only the ODM planning contract is supported")
                photos, size = request.get("photoCount"), request.get("inputBytes")
                if (type(photos) is not int or not 1 <= photos <= 1_000_000
                        or type(size) is not int or not 1 <= size <= 10**15):
                    raise ValueError("photoCount and inputBytes must be positive integers")
            except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
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
    server = ThreadingHTTPServer((config.bind_host, config.port), make_handler(config))
    print(f"LedgeTop Processing routing probe listening on {config.bind_host}:{config.port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
