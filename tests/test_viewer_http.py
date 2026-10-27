"""Real HTTP and byte-boundary tests for the native Viewer transport."""

import io
import json
from pathlib import Path
from email.message import Message
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import tempfile
import threading
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from processing.maintenance import Conflict
from processing.viewer_http import ViewerFacade, CHUNK, MAX_FORM_BYTES


TOKEN = 'native-viewer-test-token-1234567890123456'
STAGING = 'native-staging-test-token-12345678901234'
IDENTIFIER = '11111111-2222-4333-8444-555555555555'
BOUNDARY = 'viewer-test-boundary'


def multipart(parts, boundary=BOUNDARY):
    body = bytearray()
    for name, filename, data in parts:
        body.extend(('--' + boundary + '\r\nContent-Disposition: form-data; name="' + name + '"').encode())
        if filename is not None:
            body.extend(('; filename="' + filename + '"').encode())
        body.extend(b'\r\n\r\n' + data + b'\r\n')
    body.extend(('--' + boundary + '--\r\n').encode())
    return bytes(body)


class BoundedStream(io.BytesIO):
    def read(self, size=-1):
        if size < 0 or size > CHUNK:
            raise AssertionError('unbounded transport read')
        return super().read(size)


class Service:
    def __init__(self):
        self.tasks, self.uploads, self.calls = {}, {}, []
        self.result = bytes(range(256)) * 600
        self.download_stream = None

    def capabilities(self):
        return {'version': '2.2.3', 'engine': 'ledgetop-processing', 'engineVersion': '0.1.0',
                'taskQueueCount': 0, 'maxImages': None}

    def options(self):
        return [{'name': 'pc-ept', 'type': 'bool', 'value': True}]

    def initialize(self, identifier, name, options, outputs, environment):
        self.calls.append(('initialize', identifier, name, options, outputs, environment))
        if identifier in self.tasks:
            raise Conflict('private-token-must-never-be-returned')
        self.tasks[identifier] = {'uuid': identifier, 'status': {'code': 10}, 'progress': 0, 'imagesCount': 0}
        return {'uuid': identifier}

    def upload(self, identifier, filename, stream, length):
        self.info(identifier)
        data = bytearray()
        while len(data) < length:
            part = stream.read(min(CHUNK, length - len(data)))
            if not part:
                raise ValueError('truncated upload')
            data.extend(part)
        self.uploads.setdefault(identifier, {})[filename] = bytes(data)
        self.tasks[identifier]['imagesCount'] += 1
        return {'success': True}

    def commit(self, identifier):
        self.info(identifier)
        self.calls.append(('commit', identifier))
        return {'uuid': identifier}

    def info(self, identifier):
        return self.tasks[identifier]

    def output(self, identifier, line):
        self.info(identifier)
        return ['first', '', 'third'][line:]

    def download(self, identifier):
        self.info(identifier)
        self.download_stream = BoundedStream(self.result)
        return self.download_stream, len(self.result)

    def cancel(self, identifier):
        self.info(identifier)['status'] = {'code': 50}
        return {'success': True}

    def remove(self, identifier):
        self.tasks.pop(identifier)
        return {'success': True}


class MemoryHandler:
    def __init__(self, path, body=b'', method='POST', **headers):
        self.path, self.command = path, method
        self.headers = Message()
        for name, value in headers.items():
            self.headers[name.replace('_', '-')] = value
        self.rfile, self.wfile = BoundedStream(body), io.BytesIO()
        self.close_connection, self.response_headers = False, {}

    def send_response(self, code):
        self.status = code

    def send_header(self, name, value):
        self.response_headers[name] = value

    def end_headers(self):
        pass


class ViewerHTTPTests(unittest.TestCase):
    def setUp(self):
        self.spool = tempfile.TemporaryDirectory()
        self.service = Service()
        self.facade = ViewerFacade(self.service, TOKEN, STAGING, temp_dir=self.spool.name)
        facade = self.facade

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def setup(self):
                super().setup()
                self.connection.settimeout(2)

            def dispatch(self):
                if not facade.handle(self):
                    self.send_error(404)

            do_GET = do_POST = do_DELETE = dispatch

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.origin = 'http://127.0.0.1:' + str(self.server.server_port)

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.spool.cleanup()

    def request(self, path, data=None, *, method=None, token=TOKEN, headers=None, query=None):
        params = {'token': token, **(query or {})}
        request = Request(self.origin + path + '?' + urlencode(params), data=data,
                          method=method, headers=headers or {})
        with urlopen(request, timeout=4) as response:
            raw = response.read()
            return response.status, json.loads(raw) if 'json' in response.headers.get('Content-Type', '') else raw

    def initialize(self, token=TOKEN):
        return self.request('/task/new/init', multipart([
            ('name', None, b'demo'), ('options', None, b'[{"name":"pc-ept","value":true}]'),
            ('outputs', None, b'["all.zip"]')]), token=token,
            headers={'Content-Type': 'multipart/form-data; boundary=' + BOUNDARY, 'set-uuid': IDENTIFIER})

    def test_native_lifecycle_and_streamed_download(self):
        self.assertEqual(self.request('/info')[1]['engine'], 'ledgetop-processing')
        self.assertEqual(self.request('/options')[1][0]['name'], 'pc-ept')
        with self.assertRaises(HTTPError) as caught:
            self.request('/task/' + IDENTIFIER + '/info')
        self.assertEqual(caught.exception.code, 404)
        self.assertEqual(self.initialize()[1], {'uuid': IDENTIFIER})
        self.assertEqual(self.service.calls[0][-1], 'production')
        self.assertEqual(self.service.calls[0][3], [{'name': 'pc-ept', 'value': True}])
        self.assertEqual(self.request('/task/new/commit/' + IDENTIFIER, b'')[1], {'uuid': IDENTIFIER})
        self.assertEqual(self.request('/task/' + IDENTIFIER + '/info')[1]['status']['code'], 10)
        self.assertEqual(self.request('/task/' + IDENTIFIER + '/output', query={'line': 1})[1], ['', 'third'])
        self.assertEqual(self.request('/task/' + IDENTIFIER + '/download/all.zip')[1], self.service.result)
        self.assertTrue(self.service.download_stream.closed)
        for action in ('cancel', 'remove'):
            self.assertEqual(self.request('/task/' + action, urlencode({'uuid': IDENTIFIER}).encode(),
                headers={'Content-Type': 'application/x-www-form-urlencoded'})[1], {'success': True})

    def test_twenty_binary_files_preserve_false_and_cross_chunk_boundaries(self):
        self.initialize()
        parts = []
        expected = {}
        for index in range(20):
            name = 'photo-%02d.jpg' % index
            payload = (b'\x00\xff\r\n' * (CHUNK // 4 + index)
                       + b'\r\n--' + BOUNDARY.encode() + b'XY-not-a-delimiter\x00'
                       + bytes(range(256)))
            parts.append(('images', name, payload))
            expected[name] = payload
        status, result = self.request('/task/new/upload/' + IDENTIFIER, multipart(parts),
            headers={'Content-Type': 'multipart/form-data; boundary=' + BOUNDARY})
        self.assertEqual((status, result), (200, {'success': True}))
        self.assertEqual(self.service.uploads[IDENTIFIER], expected)
        self.assertEqual(self.service.tasks[IDENTIFIER]['imagesCount'], 20)
        self.assertFalse(any(call[0] == 'commit' for call in self.service.calls))
        self.assertEqual(list(Path(self.spool.name).iterdir()), [])

    def test_distinct_staging_token_and_query_auth_required(self):
        self.initialize(STAGING)
        self.assertEqual(self.service.calls[0][-1], 'staging')
        for query in ('', '?token=wrong', '?token=' + TOKEN + '&token=' + TOKEN):
            request = Request(self.origin + '/info' + query, headers={'Authorization': 'Bearer ' + TOKEN})
            with self.assertRaises(HTTPError) as caught:
                urlopen(request, timeout=4)
            self.assertEqual(caught.exception.code, 401)
            self.assertNotIn(TOKEN, caught.exception.read().decode())

    def test_safe_conflict_errors_and_invalid_queries(self):
        self.initialize()
        with self.assertRaises(HTTPError) as caught:
            self.initialize()
        self.assertEqual(caught.exception.code, 409)
        self.assertEqual(json.load(caught.exception), {'error': 'conflict'})
        for path, query in (('/info', {'line': 1}), ('/task/' + IDENTIFIER + '/output', {'line': -1}),
                            ('/task/' + IDENTIFIER + '/output', {'line': '1.0'})):
            with self.subTest(path=path, query=query), self.assertRaises(HTTPError) as caught:
                self.request(path, query=query)
            self.assertEqual(caught.exception.code, 400)

    def test_upload_rejects_unsafe_and_duplicate_names(self):
        self.initialize()
        for name in ('../bad.jpg', '..\\bad.jpg', 'bad:stream.jpg', '.', 'bad.jpg '):
            with self.subTest(name=name):
                with self.assertRaises(HTTPError) as caught:
                    self.request('/task/new/upload/' + IDENTIFIER, multipart([('images', name, b'x')]),
                        headers={'Content-Type': 'multipart/form-data; boundary=' + BOUNDARY})
                self.assertEqual(caught.exception.code, 400)
        with self.assertRaises(HTTPError) as caught:
            self.request('/task/new/upload/' + IDENTIFIER, multipart([
                ('images', 'same.jpg', b'x'), ('images', 'SAME.JPG', b'x')]),
                headers={'Content-Type': 'multipart/form-data; boundary=' + BOUNDARY})
        self.assertEqual(caught.exception.code, 400)

    def memory(self, path, body=b'', **headers):
        handler = MemoryHandler(path + '?token=' + TOKEN, body,
            Content_Length=str(len(body)), **headers)
        self.assertTrue(self.facade.handle(handler))
        return handler

    def test_body_limits_chunked_and_truncation_fail_before_execution(self):
        handler = MemoryHandler('/task/new/init?token=' + TOKEN,
            Content_Length=str(MAX_FORM_BYTES + 1), set_uuid=IDENTIFIER)
        self.facade.handle(handler)
        self.assertEqual(handler.status, 413)
        handler = MemoryHandler('/task/new/upload/' + IDENTIFIER + '?token=' + TOKEN,
            Transfer_Encoding='chunked')
        self.facade.handle(handler)
        self.assertEqual(handler.status, 400)
        body = multipart([('options', None, b'[]')])
        handler = MemoryHandler('/task/new/init?token=' + TOKEN, body[:-10],
            Content_Length=str(len(body)), set_uuid=IDENTIFIER,
            Content_Type='multipart/form-data; boundary=' + BOUNDARY)
        self.facade.handle(handler)
        self.assertEqual(handler.status, 400)
        self.assertFalse(self.service.tasks)

    def test_headers_part_count_and_ending_are_bounded(self):
        for body in (
            b'--' + BOUNDARY.encode() + b'\r\nContent-Disposition: ' + b'x' * 17000,
            multipart([('options', None, b'[]')]) + b'epilogue',
            multipart([('options', None, b'[]')])[:-4],
        ):
            with self.subTest(size=len(body)):
                handler = self.memory('/task/new/init', body, set_uuid=IDENTIFIER,
                    Content_Type='multipart/form-data; boundary=' + BOUNDARY)
                self.assertEqual(handler.status, 400)
        self.initialize()
        with patch('processing.viewer_http.MAX_PARTS', 1):
            handler = self.memory('/task/new/upload/' + IDENTIFIER, multipart([
                ('images', 'one.jpg', b'1'), ('images', 'two.jpg', b'2')]),
                Content_Type='multipart/form-data; boundary=' + BOUNDARY)
            self.assertEqual(handler.status, 400)

    def test_upload_limit_and_no_native_route_fallthrough(self):
        with patch('processing.viewer_http.MAX_UPLOAD_BYTES', 4):
            handler = self.memory('/task/new/upload/' + IDENTIFIER, b'12345')
            self.assertEqual(handler.status, 413)
        handler = MemoryHandler('/v1/prototype/jobs', method='GET')
        self.assertFalse(self.facade.handle(handler))
        handler = MemoryHandler('/info?token=' + TOKEN, method='DELETE')
        self.facade.handle(handler)
        self.assertEqual(handler.status, 405)

    def test_duplicate_lengths_and_invalid_action_forms_are_rejected(self):
        handler = MemoryHandler('/task/new/init?token=' + TOKEN, Content_Length='1')
        handler.headers['Content-Length'] = '1'
        self.facade.handle(handler)
        self.assertEqual(handler.status, 411)
        handler = MemoryHandler('/info?token=' + TOKEN, method='GET', Content_Length='0')
        handler.headers['Content-Length'] = '1'
        self.facade.handle(handler)
        self.assertEqual(handler.status, 400)
        for body in (b'uuid=' + IDENTIFIER.encode() + b'&uuid=' + IDENTIFIER.encode(),
                     b'uuid=not-a-uuid', b'uuid=' + IDENTIFIER.encode() + b'&other=value'):
            with self.subTest(body=body):
                handler = self.memory('/task/cancel', body,
                    Content_Type='application/x-www-form-urlencoded')
                self.assertEqual(handler.status, 400)

    def test_task_authorization_hides_other_environment_on_every_uuid_route(self):
        self.initialize()
        checked = []

        def authorize(identifier, environment):
            checked.append((identifier, environment))
            if environment != 'production':
                raise KeyError('unknown task')

        self.service.authorize = authorize
        paths = [('/task/' + IDENTIFIER + '/' + suffix, None, {})
                 for suffix in ('info', 'output', 'download/all.zip')]
        paths.extend([
            ('/task/new/upload/' + IDENTIFIER, multipart([('images', 'one.jpg', b'1')]),
             {'Content-Type': 'multipart/form-data; boundary=' + BOUNDARY}),
            ('/task/new/commit/' + IDENTIFIER, b'', {}),
            ('/task/cancel', urlencode({'uuid': IDENTIFIER}).encode(),
             {'Content-Type': 'application/x-www-form-urlencoded'}),
            ('/task/remove', urlencode({'uuid': IDENTIFIER}).encode(),
             {'Content-Type': 'application/x-www-form-urlencoded'}),
        ])
        for path, body, headers in paths:
            with self.subTest(path=path):
                with self.assertRaises(HTTPError) as caught:
                    self.request(path, body, token=STAGING, headers=headers)
                self.assertEqual(caught.exception.code, 404)
                self.assertEqual(json.load(caught.exception), {'error': 'task_not_found'})
        self.assertEqual(checked, [(IDENTIFIER, 'staging')] * 7)
        self.assertFalse(self.service.uploads)
        self.assertEqual(len(self.service.calls), 1)
        self.assertEqual(self.request('/task/' + IDENTIFIER + '/info')[1]['uuid'], IDENTIFIER)


if __name__ == '__main__':
    unittest.main()
