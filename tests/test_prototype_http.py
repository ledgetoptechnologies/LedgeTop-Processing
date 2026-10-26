import json
import tempfile
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from processing.app import make_handler
from processing.config import Config
from tests.test_service import TOKEN, start_server
from tests.test_prototype import submission

OPERATOR_TOKEN = "separate-operator-token-for-prototype-test"


class PrototypeHTTPTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.handler = make_handler(Config(TOKEN, "", "", 800, 20000,
                                           prototype_state_dir=self.directory.name,
                                           prototype_operator_token=OPERATOR_TOKEN))
        self.server = start_server(self.handler)
        self.origin = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.handler.prototype_queue.close()
        self.directory.cleanup()

    def request(self, path, body=None, token=TOKEN):
        data = json.dumps(body).encode() if body is not None else None
        request = Request(self.origin + path, data=data, headers={
            "Authorization": f"Bearer {token}", "Content-Type": "application/json"})
        with urlopen(request, timeout=5) as response:
            return response.status, json.load(response)

    def test_real_http_submit_claim_complete_and_readback(self):
        status, response = self.request("/v1/prototype/jobs", submission("demo"))
        self.assertEqual(status, 201)
        identifier = response["job"]["id"]
        _, claimed = self.request("/v1/prototype/claim", {"workerId": "tiny-worker"})
        _, finished = self.request(f"/v1/prototype/jobs/{identifier}/complete", {
            "claimToken": claimed["job"]["claimToken"], "status": "succeeded", "result": {"trees": 3}})
        self.assertEqual(finished["job"]["status"], "succeeded")
        self.assertNotIn("claimToken", finished["job"])
        _, listed = self.request("/v1/prototype/jobs")
        self.assertEqual(listed["jobs"][0]["id"], identifier)

    def test_authentication_and_payload_errors(self):
        for path, payload, token, expected in (
                ("/v1/prototype/jobs", submission("demo"), "wrong", 401),
                ("/v1/prototype/claim", {"workerId": "a", "command": "exec"}, TOKEN, 400),
                ("/v1/prototype/jobs", submission("demo", "unknown"), TOKEN, 400)):
            with self.assertRaises(HTTPError) as caught:
                self.request(path, payload, token)
            self.assertEqual(caught.exception.code, expected)

    def test_default_probe_has_no_job_queue(self):
        disabled = start_server(make_handler(Config(TOKEN, "", "", 800, 20000)))
        try:
            request = Request(f"http://127.0.0.1:{disabled.server_port}/v1/prototype/jobs", headers={"Authorization": f"Bearer {TOKEN}"})
            with self.assertRaises(HTTPError) as caught:
                urlopen(request, timeout=5)
            self.assertEqual(caught.exception.code, 404)
        finally:
            disabled.shutdown()
            disabled.server_close()

    def test_observer_can_read_authority_but_cannot_submit_or_claim(self):
        from dataclasses import replace
        self.server.shutdown()
        self.server.server_close()
        self.handler.prototype_queue.close()
        observer = 'observer-only-test-token-1234567890123456'
        self.handler = make_handler(replace(Config(TOKEN, '', '', 800, 20000,
            prototype_state_dir=self.directory.name), prototype_observer_token=observer))
        self.server = start_server(self.handler)
        self.origin = f'http://127.0.0.1:{self.server.server_port}'
        _, authority = self.request('/v1/prototype/authority', token=observer)
        self.assertEqual(authority['authorityId'], self.handler.prototype_queue.authority_id)
        self.request('/v1/prototype/jobs', token=observer)
        self.request('/v1/prototype/nodes', token=observer)
        for route, payload in [('/v1/prototype/jobs', submission('observer-submit')),
                               ('/v1/prototype/claim', {'workerId': 'observer'})]:
            with self.assertRaises(HTTPError) as caught:
                self.request(route, payload, token=observer)
            self.assertEqual(caught.exception.code, 401)

    def test_worker_credential_cannot_resolve_held_work(self):
        _, submitted = self.request("/v1/prototype/jobs", submission("held"))
        identifier = submitted["job"]["id"]
        self.request("/v1/prototype/claim", {"workerId": "worker"})
        with self.assertRaises(HTTPError) as caught:
            self.request(f"/v1/prototype/jobs/{identifier}/hold", {"note": "worker crashed"})
        self.assertEqual(caught.exception.code, 401)
        self.request(f"/v1/prototype/jobs/{identifier}/hold", {"note": "worker crashed"}, OPERATOR_TOKEN)
        _, resolved = self.request(f"/v1/prototype/jobs/{identifier}/resolve",
                                   {"note": "confirmed worker stopped", "workerStopped": True}, OPERATOR_TOKEN)
        self.assertEqual(resolved["job"]["status"], "failed")
