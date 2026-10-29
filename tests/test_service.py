import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from processing.app import make_handler
from processing.config import Config
from processing.policy import plan
from processing.provider import probe_node

TOKEN = "test-token-long-enough-for-private-api-123"
OPTIONS = [{"name": name} for name in ("pc-ept", "gltf", "3d-tiles")]
INFO = {
    "version": "2.2.3", "engine": "ODM", "engineVersion": "3.1",
    "taskQueueCount": 0, "maxImages": 1000,
    "totalMemory": 1000000000, "availableMemory": 500000000, "cpuCores": 8,
}


class FakeNode(BaseHTTPRequestHandler):
    def do_GET(self):
        value = INFO if self.path == "/info" else OPTIONS if self.path == "/options" else None
        if value is None:
            self.send_error(404)
            return
        body = json.dumps(value).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


def start_server(handler):
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


class ServiceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.node = start_server(FakeNode)
        origin = f"http://127.0.0.1:{cls.node.server_port}"
        cls.api = start_server(make_handler(Config(
            api_token=TOKEN, local_node_url=origin, main_node_url=origin,
            local_max_photos=800, local_max_input_bytes=20_000,
        )))
        cls.origin = f"http://127.0.0.1:{cls.api.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.api.shutdown()
        cls.api.server_close()
        cls.node.shutdown()
        cls.node.server_close()

    def request(self, path, payload=None, token=TOKEN):
        body = json.dumps(payload).encode() if payload is not None else None
        headers = {"Authorization": f"Bearer {token}"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        request = Request(self.origin + path, data=body, headers=headers)
        with urlopen(request, timeout=5) as response:
            return response.status, json.load(response)

    def test_local_and_large_job_route_without_dispatching(self):
        _, small = self.request("/v1/plan", {"engine": "odm", "photoCount": 200, "inputBytes": 4000})
        _, large = self.request("/v1/plan", {"engine": "odm", "photoCount": 801, "inputBytes": 4000})
        self.assertEqual(small["decision"], "local")
        self.assertEqual(large["decision"], "main")
        self.assertFalse(small["executesWork"])

    def test_large_job_waits_when_main_unavailable(self):
        local = {"ready": True, "maxImages": 1000}
        main = {"ready": False, "maxImages": None}
        self.assertEqual(plan(801, 4000, local, main, 800, 20000),
                         {"decision": "wait", "reason": "main_unavailable"})

    def test_small_job_waits_for_local_when_local_unavailable(self):
        self.assertEqual(plan(300, 4000, {"ready": False}, {"ready": True, "maxImages": 1000}, 800, 20000)["decision"], "wait")

    def test_byte_size_does_not_route_small_photo_job_away_from_local(self):
        self.assertEqual(plan(10, 20001, {"ready": True}, {"ready": False}, 800, 20000)["decision"], "local")

    def test_requires_authentication(self):
        with self.assertRaises(HTTPError) as exc:
            self.request("/v1/nodes", token="wrong")
        self.assertEqual(exc.exception.code, 401)

    def test_probe_reports_viewer_compatibility(self):
        result = probe_node("local", f"http://127.0.0.1:{self.node.server_port}")
        self.assertTrue(result["ready"])
        self.assertEqual(result["viewerProviderType"], "nodeodm")

    def test_rejects_invalid_job_body(self):
        with self.assertRaises(HTTPError) as exc:
            self.request("/v1/plan", {"engine": "odm", "photoCount": True, "inputBytes": 1})
        self.assertEqual(exc.exception.code, 400)


if __name__ == "__main__":
    unittest.main()
