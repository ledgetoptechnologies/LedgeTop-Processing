import json
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from processing.app import make_handler
from processing.config import Config
from processing.prototype import PHOTO_PROFILE, PROFILE
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

    def test_node_role_reads_and_claim_enforce_main_only_treedar(self):
        self.server.shutdown()
        self.server.server_close()
        self.handler.prototype_queue.close()
        self.handler = make_handler(Config(TOKEN, '', '', 1000, 20000,
            prototype_state_dir=self.directory.name, photo_smoke_enabled=True,
            prototype_node_ids=('cluster', 'main'), treedar_node_ids=('main',)))
        self.server = start_server(self.handler)
        self.origin = f'http://127.0.0.1:{self.server.server_port}'
        _, listed = self.request('/v1/prototype/nodes')
        nodes = {node['id']: node for node in listed['nodes']}
        self.assertEqual(nodes['cluster']['allowedProfiles'], [PHOTO_PROFILE])
        _, main = self.request('/v1/prototype/nodes/main')
        self.assertEqual(main['node']['allowedProfiles'], sorted([PHOTO_PROFILE, PROFILE]))
        self.request('/v1/prototype/jobs', submission('main-treedar', 'production'))
        _, denied = self.request('/v1/prototype/claim', {'workerId': 'cluster-worker',
                                                       'nodeId': 'cluster', 'profiles': [PROFILE]})
        self.assertIsNone(denied['job'])
        _, allowed = self.request('/v1/prototype/claim', {'workerId': 'main-worker',
                                                        'nodeId': 'main', 'profiles': [PROFILE]})
        self.assertEqual(allowed['job']['nodeId'], 'main')

    def test_inventory_claim_rejects_odm_on_treedar_only_node(self):
        self.server.shutdown(); self.server.server_close(); self.handler.prototype_queue.close()
        inventory = (
            {'id':'tree', 'physicalHostId':'host-tree', 'origin':'http://192.0.2.1:3000', 'token':'', 'maxPhotos':None, 'capabilities':('treedar',)},
            {'id':'photo', 'physicalHostId':'host-photo', 'origin':'http://192.0.2.2:3000', 'token':'', 'maxPhotos':1000, 'capabilities':('odm',)},
            {'id':'third', 'physicalHostId':'host-third', 'origin':'http://192.0.2.3:3000', 'token':'', 'maxPhotos':None, 'capabilities':('odm',)},
        )
        self.handler = make_handler(Config(TOKEN, '', '', 1000, 20000,
            prototype_state_dir=self.directory.name, photo_smoke_enabled=True,
            prototype_node_ids=('tree','photo','third'), treedar_node_ids=('tree',),
            per_node_slots=True, node_inventory=inventory))
        self.server = start_server(self.handler)
        self.origin = f'http://127.0.0.1:{self.server.server_port}'
        self.request('/v1/prototype/jobs', {'requestId':'odm', 'environment':'staging', 'profile':PHOTO_PROFILE})
        _, denied = self.request('/v1/prototype/claim', {'workerId':'tree-worker', 'nodeId':'tree', 'profiles':[PHOTO_PROFILE]})
        self.assertIsNone(denied['job'])
        _, allowed = self.request('/v1/prototype/claim', {'workerId':'photo-worker', 'nodeId':'photo', 'profiles':[PHOTO_PROFILE]})
        self.assertEqual(allowed['job']['nodeId'], 'photo')
        with patch('processing.app.probe_node', side_effect=lambda role, origin, token: {
                'role':role, 'ready':True, 'maxImages':None, 'taskQueueCount':0}), \
             patch('processing.app.catalog', return_value={'ready':True, 'nodes':[], 'commonOptions':[]}):
            _, nodes = self.request('/v1/nodes')
            self.assertEqual([n['id'] for n in nodes['nodes']], ['tree','photo','third'])
            _, decision = self.request('/v1/plan', {'engine':'odm','photoCount':1001,'inputBytes':1})
            self.assertEqual((decision['decision'], decision['nodeId']), ('node','third'))
            _, options = self.request('/v1/options')
            self.assertTrue(options['ready'])
            with tempfile.TemporaryDirectory() as removed_dir:
                removed_handler = make_handler(Config(TOKEN, '', '', 1000, 20000,
                    prototype_state_dir=removed_dir, photo_smoke_enabled=True,
                    prototype_node_ids=('tree','photo'), treedar_node_ids=('tree',),
                    per_node_slots=True, node_inventory=inventory[:2]))
                removed_server = start_server(removed_handler)
                try:
                    req = Request(f'http://127.0.0.1:{removed_server.server_port}/v1/nodes', headers={'Authorization':f'Bearer {TOKEN}'})
                    with urlopen(req, timeout=5) as response:
                        self.assertEqual([n['id'] for n in json.load(response)['nodes']], ['tree','photo'])
                finally:
                    removed_server.shutdown(); removed_server.server_close(); removed_handler.prototype_queue.close()

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
