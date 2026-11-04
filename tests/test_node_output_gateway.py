import http.client
import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

from processing.node_output_gateway import OutputGateway, Unavailable, docker_incarnation

TASK = '11111111-1111-4111-8111-111111111111'
TOKEN = 'test-only-' + 'x' * 32


class Backend(BaseHTTPRequestHandler):
    def log_message(self, *args): pass
    def do_GET(self):
        payload = json.dumps(['line one', '', 'line three']).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


class GatewayTests(unittest.TestCase):
    def setUp(self):
        self.backend = ThreadingHTTPServer(('127.0.0.1', 0), Backend)
        self.backend_thread = threading.Thread(target=self.backend.serve_forever)
        self.backend_thread.start()
        self.epoch = ('a' * 64, 12, '2026-11-04T00:00:00Z', self.backend.server_port)
        self.inspections = []
        self.gateway = OutputGateway('nodeodm', self.backend.server_port, TOKEN, inspect=self.inspect)
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), self.gateway.handler())
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()

    def tearDown(self):
        for server, thread in ((self.server, self.thread), (self.backend, self.backend_thread)):
            server.shutdown(); thread.join(timeout=5); server.server_close()
            self.assertFalse(thread.is_alive())

    def inspect(self, *_):
        return self.inspections.pop(0) if self.inspections else self.epoch

    def request(self, route, token=TOKEN):
        connection = http.client.HTTPConnection('127.0.0.1', self.server.server_port, timeout=3)
        try:
            connection.request('GET', route, headers={'Authorization': 'Bearer ' + token})
            response = connection.getresponse()
            return response.status, json.loads(response.read())
        finally: connection.close()

    def test_authenticated_output_and_restart_during_exchange(self):
        route = '/task/' + TASK + '/output?line=0'
        status, output = self.request(route)
        self.assertEqual(status, 200)
        self.assertEqual(output['lines'], ['line one', '', 'line three'])
        self.assertEqual(len(output['containerEpoch']), 64)
        self.inspections = [self.epoch, ('b' * 64, 13, '2026-11-04T00:01:00Z', self.backend.server_port)]
        self.assertEqual(self.request(route), (503, {'error': 'output_unavailable'}))
        self.assertEqual(self.request(route, 'wrong'), (401, {'error': 'unauthorized'}))
        self.assertEqual(self.request('/task/not-a-uuid/output?line=0')[0], 400)
        self.assertEqual(self.request(route + '&line=1')[0], 400)

    def test_inspect_rejects_non_loopback_mapping(self):
        obj = {'Id': 'a' * 64, 'State': {'Running': True, 'Pid': 12, 'StartedAt': '2026-11-04T00:00:00Z'},
               'NetworkSettings': {'Ports': {'3000/tcp': [{'HostIp': '0.0.0.0', 'HostPort': '3001'}]}}}
        class Result: stdout = json.dumps([obj]).encode()
        with patch('processing.node_output_gateway.subprocess.run', return_value=Result()):
            with self.assertRaises(Unavailable): docker_incarnation('nodeodm', 3001)
            obj['NetworkSettings']['Ports']['3000/tcp'][0]['HostIp'] = '127.0.0.1'
            Result.stdout = json.dumps([obj]).encode()
            self.assertEqual(docker_incarnation('nodeodm', 3001)[0], 'a' * 64)


if __name__ == '__main__': unittest.main()
