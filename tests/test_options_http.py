"""Actual coordinator HTTP discovery changes without process restart."""
import json
import os
import tempfile
import unittest
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlsplit
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from unittest.mock import patch

from processing.app import make_handler
from processing.config import Config, from_environment
from tests.test_service import TOKEN, start_server


class DynamicNode(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_GET(self):
        parsed = urlsplit(self.path)
        self.server.calls += 1
        if parse_qs(parsed.query).get('token') != ['upstream-private-token']:
            code, payload = 401, {'error': 'unauthorized'}
        elif self.server.unavailable:
            code, payload = 503, {'error': 'unavailable'}
        elif parsed.path == '/info':
            code, payload = 200, {'version': '2.3.1', 'engine': 'odx', 'engineVersion': self.server.engine_version}
        else:
            code, payload = 200, self.server.options
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class OptionsHTTPTests(unittest.TestCase):
    def setUp(self):
        self.node = start_server(DynamicNode)
        self.node.calls = 0
        self.node.unavailable = False
        self.node.engine_version = '3.8.2'
        self.node.options = [{'name': 'quality', 'type': 'enum', 'value': 'medium', 'domain': ['low', 'medium']}]
        origin = f'http://127.0.0.1:{self.node.server_port}'
        self.api = start_server(make_handler(Config(TOKEN, origin, '', 800, 20000,
            local_node_token='upstream-private-token',
            prototype_observer_token='observer-token-distinct-and-at-least-32-characters')))
        self.base = f'http://127.0.0.1:{self.api.server_port}'

    def tearDown(self):
        self.api.shutdown(); self.api.server_close()
        self.node.shutdown(); self.node.server_close()

    def get(self, path='/v1/options', token=TOKEN):
        request = Request(self.base + path, headers={'Authorization': 'Bearer ' + token})
        with urlopen(request, timeout=10) as response:
            self.assertEqual(response.headers['Cache-Control'], 'no-store')
            return json.load(response)

    def test_new_removed_and_changed_flags_refresh_without_restart(self):
        first = self.get()
        self.assertEqual(first['commonOptions'], self.node.options)
        self.node.options = [{'name': 'new-engine-flag', 'type': 'int', 'value': '7', 'domain': 'positive integer'}]
        self.node.engine_version = '3.9.0'
        second = self.get()
        self.assertEqual(second['commonOptions'], self.node.options)
        self.assertNotEqual(first['commonOptionsSha256'], second['commonOptionsSha256'])
        self.assertNotIn('upstream-private-token', json.dumps(second))
        self.assertFalse(second['viewerProviderReady'])
        self.assertFalse(second['executesWork'])

    def test_failure_does_not_return_last_good_catalog(self):
        self.get()
        self.node.unavailable = True
        failed = self.get()
        self.assertFalse(failed['ready'])
        self.assertEqual(failed['commonOptions'], [])

    def test_authentication_before_upstream_and_observer_read_only(self):
        with self.assertRaises(HTTPError) as caught:
            self.get(token='incorrect')
        self.assertEqual(caught.exception.code, 401)
        self.assertEqual(self.node.calls, 0)
        self.get(token='observer-token-distinct-and-at-least-32-characters')
        for route in ('/info', '/options'):
            with self.assertRaises(HTTPError) as caught:
                self.get(path=route)
            self.assertEqual(caught.exception.code, 404)

    def test_node_token_startup_rules_match_discovery(self):
        with patch.dict(os.environ, {'PROCESSING_API_TOKEN': TOKEN, 'LOCAL_NODE_TOKEN': 'contains space'}, clear=True):
            with self.assertRaises(ValueError):
                from_environment()
        with patch.dict(os.environ, {'PROCESSING_API_TOKEN': TOKEN, 'LOCAL_NODE_TOKEN': 'safe-private-token'}, clear=True):
            configured = from_environment()
            self.assertEqual(configured.local_node_token, 'safe-private-token')
            self.assertNotIn('safe-private-token', repr(configured))
