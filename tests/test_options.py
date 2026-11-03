"""Actual HTTP catalog discovery without processing, proxies or static flags."""
import copy
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from processing.options import catalog, discover_node, MAX_OPTIONS


INFO = {'version': '2.3.1', 'engine': 'ODX', 'engineVersion': '3.8.3'}


class Engine:
    def __init__(self):
        self.options = [{'name': 'engine-new-flag', 'type': 'bool', 'value': False,
                         'help': 'Engine-provided description', 'futureMetadata': {'choices': [True, False]}}]
        self.info = dict(INFO)
        self.token = 'private-token-keep-out-of-results'
        self.mode = 'normal'
        self.requests = []
        engine = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                parsed = urlsplit(self.path)
                engine.requests.append((parsed.path, parse_qs(parsed.query)))
                if parse_qs(parsed.query).get('token') != [engine.token]:
                    self.send_response(401)
                    self.end_headers()
                    return
                if engine.mode == 'redirect':
                    self.send_response(302)
                    self.send_header('Location', engine.origin + '/redirect-target')
                    self.end_headers()
                    return
                if engine.mode == 'unavailable':
                    self.send_response(503)
                    self.end_headers()
                    self.wfile.write(engine.token.encode())
                    return
                value = engine.info if parsed.path == '/info' else engine.options
                raw = json.dumps(value).encode()
                if engine.mode == 'oversize' and parsed.path == '/options':
                    raw = b' ' * (MAX_OPTIONS + 1)
                if engine.mode == 'malformed' and parsed.path == '/options':
                    raw = b'{' + engine.token.encode()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json' if engine.mode != 'html' else 'text/html')
                self.send_header('Content-Length', str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def log_message(self, *_):
                pass

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.origin = f'http://127.0.0.1:{self.server.server_port}'
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()


class OptionsTests(unittest.TestCase):
    def setUp(self):
        self.engine = Engine()
        self.addCleanup(self.engine.close)

    def discover(self):
        return discover_node('local', self.engine.origin, self.engine.token)

    def test_inventory_catalog_discovers_three_arbitrary_ids(self):
        result = catalog([(name, self.engine.origin, self.engine.token)
                          for name in ('field-small', 'workstation', 'gpu-3')], inventory=True)
        self.assertTrue(result['ready'])
        self.assertEqual([n['role'] for n in result['nodes']], ['field-small', 'workstation', 'gpu-3'])
        self.assertNotIn(self.engine.token, json.dumps(result))
        self.assertEqual(result['commonOptions'], self.engine.options)

    def test_fresh_calls_reflect_added_removed_and_changed_engine_metadata(self):
        first = self.discover()
        original = copy.deepcopy(self.engine.options)
        self.engine.options.append({'name': 'future-unrecognized-flag', 'value': 3, 'type': 'int'})
        second = self.discover()
        self.assertEqual(second['options'], self.engine.options)
        self.assertNotEqual(first['optionsSha256'], second['optionsSha256'])
        self.engine.options = [{**original[0], 'value': True}]
        third = self.discover()
        self.assertEqual(third['options'], self.engine.options)
        self.assertNotEqual(first['optionsSha256'], third['optionsSha256'])
        self.assertEqual([path for path, _ in self.engine.requests], ['/info', '/options'] * 3)

    def test_auth_is_forwarded_but_never_returned_and_environment_proxy_ignored(self):
        with patch.dict(os.environ, {'http_proxy': 'http://127.0.0.1:1', 'HTTP_PROXY': 'http://127.0.0.1:1', 'no_proxy': ''}):
            result = self.discover()
        self.assertTrue(result['available'])
        self.assertNotIn(self.engine.token, json.dumps(result))
        self.assertNotIn(self.engine.origin, json.dumps(result))
        self.assertTrue(all(query == {'token': [self.engine.token]} for _, query in self.engine.requests))
        rejected = discover_node('local', self.engine.origin, 'wrong-token')
        self.assertFalse(rejected['available'])
        self.assertNotIn('wrong-token', json.dumps(rejected))

    def test_info_version_changes_capability_hash_even_when_flags_are_unchanged(self):
        before = self.discover()
        self.engine.info['engineVersion'] = '3.9.0'
        after = self.discover()
        self.assertEqual(before['optionsSha256'], after['optionsSha256'])
        self.assertNotEqual(before['capabilitiesSha256'], after['capabilitiesSha256'])

    def test_reflected_credentials_in_successful_metadata_are_rejected(self):
        self.engine.options[0]['futureMetadata'] = {'help': 'private ' + self.engine.token}
        rejected = self.discover()
        self.assertFalse(rejected['available'])
        self.assertNotIn(self.engine.token, json.dumps(rejected))
        self.engine.options[0].pop('futureMetadata')
        self.engine.info['engineVersion'] = self.engine.token
        rejected = self.discover()
        self.assertFalse(rejected['available'])
        self.assertNotIn(self.engine.token, json.dumps(rejected))

    def test_heterogeneous_fleet_returns_only_exact_common_definitions(self):
        other = Engine()
        self.addCleanup(other.close)
        other.options = [dict(self.engine.options[0]), {'name': 'main-only', 'value': 2}]
        self.engine.options.append({'name': 'different-default', 'value': 1})
        other.options.append({'name': 'different-default', 'value': 2})
        result = catalog([('local', self.engine.origin, self.engine.token), ('main', other.origin, other.token)])
        self.assertTrue(result['ready'])
        self.assertEqual([item['name'] for item in result['commonOptions']], ['engine-new-flag'])
        self.assertEqual(result['conflictingNames'], ['different-default'])
        self.assertEqual(result['unsupportedByNode'], {'local': ['main-only'], 'main': []})
        self.assertEqual(result['nodes'][1]['options'], other.options)

    def test_configured_outage_closes_fleet_and_never_serves_stale_catalog(self):
        nodes = [('local', self.engine.origin, self.engine.token), ('main', self.engine.origin, self.engine.token)]
        self.assertTrue(catalog(nodes)['ready'])
        self.engine.mode = 'unavailable'
        failed = catalog(nodes)
        self.assertFalse(failed['ready'])
        self.assertEqual(failed['commonOptions'], [])
        self.assertIsNone(failed['commonOptionsSha256'])
        self.assertTrue(all(node['options'] == [] for node in failed['nodes']))
        self.assertNotIn(self.engine.token, json.dumps(failed))

    def test_missing_configuration_is_reported_and_does_not_hide_valid_single_node(self):
        result = catalog([('local', self.engine.origin, self.engine.token), ('main', '', '')])
        self.assertTrue(result['ready'])
        self.assertEqual(result['commonOptions'], self.engine.options)
        self.assertFalse(result['nodes'][1]['configured'])
        self.assertFalse(catalog([('local', '', '')])['ready'])

    def test_redirect_is_not_followed(self):
        self.engine.mode = 'redirect'
        self.assertFalse(self.discover()['available'])
        self.assertEqual([path for path, _ in self.engine.requests], ['/info'])

    def test_invalid_or_oversize_response_is_closed_without_content_disclosure(self):
        for mode in ('malformed', 'oversize', 'html'):
            with self.subTest(mode=mode):
                self.engine.mode = mode
                result = self.discover()
                self.assertFalse(result['available'])
                self.assertEqual(result['options'], [])
                self.assertNotIn(self.engine.token, json.dumps(result))

    def test_invalid_catalog_shapes_duplicates_and_nonfinite_data_fail_closed(self):
        for value in ([], {}, [{'name': 'duplicate'}, {'name': 'duplicate'}],
                      [{'name': '../escape'}], [{'name': 'safe', 'value': float('nan')}],
                      [{'name': 'n' * 129}], [{'name': 'safe'}] * 2049):
            with self.subTest(value_type=type(value).__name__):
                self.engine.options = value
                self.assertFalse(self.discover()['available'])

    def test_invalid_info_is_not_replaced_with_assumed_engine_version(self):
        for value in ({}, {'version': '2.3.1', 'engine': 'ODX', 'engineVersion': ''},
                      {**INFO, 'engineVersion': 'x' * 121}, {**INFO, 'engine': '\ninvalid'}):
            self.engine.info = value
            self.assertFalse(self.discover()['available'])

    def test_configured_origins_cannot_smuggle_credentials_paths_or_queries(self):
        for origin in ('http://user:pass@127.0.0.1', self.engine.origin + '/path',
                       self.engine.origin + '?token=embedded', self.engine.origin + '#fragment',
                       'file:///tmp/options.json', 'http://localhost:0', 'http://localhost:bad'):
            with self.subTest(origin=origin):
                self.assertFalse(discover_node('local', origin, self.engine.token)['available'])
        self.assertEqual(self.engine.requests, [])
        with self.assertRaises(ValueError):
            catalog([('local', '', ''), ('local', '', '')])
        with self.assertRaises(ValueError):
            catalog([('user-chosen-origin', '', '')])


if __name__ == '__main__':
    unittest.main()
