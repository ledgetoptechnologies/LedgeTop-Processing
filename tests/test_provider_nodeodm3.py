"""Redacted installed ODM3 signature over real HTTP; no engine mutations."""
import copy
import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from processing.provider import _viewer_type, probe_node
from processing.policy import plan

# Relevant fields observed from the installed cluster GET /info on 2026-10-28.
# Only the required output names are retained from its dynamic option catalog.
INFO = {'version': '3.6.2', 'engine': 'odm', 'engineVersion': '3.6.2',
        'taskQueueCount': 0, 'maxImages': 1000, 'maxParallelTasks': 1}
OPTIONS = [{'name': n} for n in ('pc-ept', 'gltf', '3d-tiles')]
TOKEN = 'synthetic-provider-key'


class NodeODM3ProviderTests(unittest.TestCase):
    def setUp(self):
        self.info = copy.deepcopy(INFO)
        self.options = copy.deepcopy(OPTIONS)
        self.paths = []
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                u = urlsplit(self.path)
                if parse_qs(u.query) != {'token': [TOKEN]}:
                    self.send_error(401)
                    return
                owner.paths.append(u.path)
                value = owner.info if u.path == '/info' else owner.options if u.path == '/options' else None
                if value is None:
                    self.send_error(404)
                    return
                body = json.dumps(value).encode()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_):
                pass

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={'poll_interval': .01})
        self.thread.start()
        self.addCleanup(self.close)
        self.origin = 'http://127.0.0.1:' + str(self.server.server_port)

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)
        self.assertFalse(self.thread.is_alive())

    def probe(self):
        return probe_node('local', self.origin, TOKEN)

    def route(self, local, photos=10):
        return plan(photos, 100, local, {'ready': True, 'maxImages': None, 'taskQueueCount': 0},
                    1000, 20 * 1024 ** 3)

    def test_installed_signature_is_ready_and_routes_small_first_and_large_main(self):
        local = self.probe()
        self.assertTrue(local['ready'])
        self.assertEqual(local['viewerProviderType'], 'nodeodm')
        for count in (10, 50, 1000):
            with self.subTest(count=count):
                self.assertEqual(self.route(local, count)['decision'], 'local')
        self.assertEqual(self.route(local, 1001)['decision'], 'main')
        self.assertEqual(self.paths, ['/info', '/options'])

    def test_busy_and_draining_small_node_wait_without_main_fallback(self):
        self.info['taskQueueCount'] = 1
        self.assertEqual(self.route(self.probe())['decision'], 'wait')
        self.info['taskQueueCount'] = 0
        for state in ('draining', 'updating', 'held'):
            with self.subTest(state=state):
                self.assertEqual(self.route({**self.probe(), 'state': state})['decision'], 'wait')

    def test_missing_required_dynamic_output_stays_unready(self):
        self.options.pop()
        self.assertEqual(self.probe()['reason'], 'viewer_outputs_missing')
        self.assertEqual(self.route(self.probe())['decision'], 'wait')

    def test_unknown_engine_or_inconsistent_version_stays_unready(self):
        for engine, version in [('unknown', '3.6.2'), ('odx', '3.6.2'), ('ODM', '3.6.2'),
                                ('odm', '3.8.5'), ('odm', 'malformed')]:
            with self.subTest(engine=engine, version=version):
                self.info.update(engine=engine, engineVersion=version)
                self.assertEqual(self.probe()['reason'], 'viewer_provider_signature_unsupported')

    def test_existing_2x_and_sentinel_cluster1_preserved(self):
        self.assertEqual(_viewer_type({**INFO, 'version': '2.4.0', 'engine': 'odx', 'engineVersion': '3.8.5'}), 'nodeodm')
        sentinel = dict(totalMemory=99999999999, availableMemory=99999999999, cpuCores=99999999999)
        self.assertEqual(_viewer_type({**INFO, **sentinel, 'version': '1.5.0'}), 'clusterodm')
        self.assertIsNone(_viewer_type({**INFO, **sentinel}))

    def test_unknown_and_malformed_shapes_rejected(self):
        for value in [None, {}, {**INFO, 'version': '4.0.0'}, {**INFO, 'version': '3x'},
                      {**INFO, 'version': '3.6.2\n'}, {**INFO, 'taskQueueCount': True},
                      {**INFO, 'taskQueueCount': -1}, {**INFO, 'maxImages': True},
                      {**INFO, 'maxImages': 0}, {**INFO, 'maxImages': '1000'},
                      {**INFO, 'engine': ''}, {**INFO, 'engineVersion': None}]:
            with self.subTest(value=value):
                self.assertIsNone(_viewer_type(value))


if __name__ == '__main__':
    unittest.main()
