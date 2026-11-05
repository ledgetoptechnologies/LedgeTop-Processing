import copy
import io
import json
import ssl
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from processing import reference_logs as logs
from processing.reference_engine_worker import WorkerHeld
from processing.reference_http import create_reference_http_server
from processing.reference_result_api import ReferenceResultAPI
from processing.reference_result_proxy import ResultProxy, ResultProxyError
from tests.test_reference_results import ResultsFixture
from tests import test_reference_api as api_tests

TOKEN = 'synthetic-result-log-reader-token'


class ExecutionLogTests(ResultsFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.expected = self.worker.log_metadata(self.job['jobId'])
        self.calls = []
        self.output = ['Starting opensfm', 'second', 'third']
        self.engine.read = self.read_engine
        self.engine.output_page = self.read_page
        self.api = ReferenceResultAPI(self.results, bearer_readers={TOKEN: dict(node_id='cluster', worker_id='worker',
            environments=('staging',), share_policy=lambda metadata: metadata['environment'] == 'staging')})

    def read_engine(self, route, **query):
        self.calls.append((route, query))
        if route.endswith('/info'):
            return {'uuid': self.job['jobId']}
        return self.output[query['line']:]

    def read_page(self, identifier, *, line, limit):
        self.calls.append(('output_page', dict(identifier=identifier, line=line, limit=limit)))
        tail=self.output[line:]
        return tail[:limit], len(tail)>limit

    def read_logs(self, **changes):
        binding = dict(generation=1, environment='staging', attempt_id=self.expected['attemptId'], after=0, limit=2)
        binding.update(changes)
        return self.worker.read_logs(self.job['jobId'], **binding)

    def request(self, *, token=TOKEN, generation=1, after=0, limit=2, environment='staging', attempt=None, protected=True, tail=''):
        return self.api.handle('GET', '/ltds/worker/v1/jobs/' + self.job['jobId'] +
            f'/logs?generation={generation}&after={after}&limit={limit}' + tail,
            {'Authorization': 'Bearer ' + token, 'X-Log-Environment': environment,
             'X-Log-Attempt': attempt or self.expected['attemptId']}, protected_transport=protected)

    def test_exact_bound_page_and_cursor_without_mutations_or_file_pins(self):
        before = self.worker._row(self.job['jobId'])
        first = self.read_logs()
        self.assertEqual(first['lines'], [{'level': 'info', 'message': text} for text in self.output[:2]])
        self.assertEqual((first['nextCursor'], first['hasMore']), (2, True))
        last = self.read_logs(after=2)
        self.assertEqual((last['nextCursor'], last['hasMore']), (3, False))
        self.assertEqual(self.worker._row(self.job['jobId']), before)
        self.assertEqual(self.active_pins(), 0)
        self.assertEqual(self.calls[1], ('output_page', {'identifier': self.job['jobId'], 'limit': 2, 'line': 0}))

    def test_worker_api_auth_environment_generation_attempt_and_strict_queries(self):
        self.assertEqual(self.request().status, 200)
        for changes, status in (({'token': 'wrong-token'}, 401), ({'protected': False}, 503),
                ({'environment': 'production'}, 409), ({'generation': 2}, 409), ({'attempt': 'other'}, 409),
                ({'after': -1}, 400), ({'limit': 201}, 400), ({'tail': '&after=0'}, 400), ({'tail': '&path=secret'}, 400)):
            with self.subTest(changes=changes):
                self.assertEqual(self.request(**changes).status, status)

    def test_missing_execution_physical_engine_uuid_and_oversized_output_fail_closed(self):
        for changes in ({'generation': 2}, {'environment': 'production'}, {'attempt_id': 'other'}, {'after': True}, {'limit': 201}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.read_logs(**changes)
        with patch.object(self.worker, 'engine_probe', return_value='different-engine'), self.assertRaises(WorkerHeld):
            self.read_logs()
        with patch.object(self.engine, 'read', return_value={'uuid': 'different-task'}), self.assertRaises(WorkerHeld):
            self.read_logs()
        self.output = ['x' * (logs.MAX_LOG_BYTES + 1)]
        with self.assertRaises(WorkerHeld):
            self.read_logs()
        with patch.object(self.worker, '_row', side_effect=WorkerHeld('unknown')):
            self.assertEqual(self.request().status, 409)
        self.assertEqual(self.active_pins(), 0)
        # Gate and journal remain usable after all failures.
        self.output = ['recovered']
        self.assertEqual(self.request().status, 200)

    def test_unicode_http_page_is_bounded_and_cursor_counts_returned_lines(self):
        self.output = ['😀' * 4000] * 15
        page = self.read_logs(limit=200)
        self.assertLessEqual(logs.encoded_size(page), logs.MAX_LOG_BYTES)
        self.assertEqual(page['nextCursor'], len(page['lines']))
        self.assertTrue(page['hasMore'])

    def test_real_https_proxy_page_and_strict_http_query_boundary(self):
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'localhost')])
        now = datetime.now(timezone.utc)
        cert = (x509.CertificateBuilder().subject_name(subject).issuer_name(subject).public_key(key.public_key())
            .serial_number(x509.random_serial_number()).not_valid_before(now-timedelta(minutes=1)).not_valid_after(now+timedelta(hours=1))
            .add_extension(x509.SubjectAlternativeName([x509.DNSName('localhost')]), critical=False).sign(key, hashes.SHA256()))
        certfile, keyfile = self.root/'logs-cert.pem', self.root/'logs-key.pem'
        certfile.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
        keyfile.write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(certfile, keyfile)
        server = create_reference_http_server(self.api, ssl_context=context)
        thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': 0.02})
        thread.start()
        try:
            client = ssl.create_default_context(cafile=str(certfile))
            budgets = []
            @contextmanager
            def admit(binding):
                budgets.append(binding)
                try:
                    yield
                finally:
                    budgets.append('closed')
            proxy = ResultProxy({'cluster': 'https://localhost:' + str(server.server_address[1])},
                worker_bearers={'cluster': TOKEN}, admit_read=admit, ssl_context=client)
            page = proxy.logs(self.expected, after=0, limit=2)
            self.assertEqual(page, self.read_logs())
            self.assertEqual(budgets[-1], 'closed')
            self.assertEqual(budgets[0]['byteSize'], logs.MAX_LOG_BYTES)
            bad = dict(self.expected, environment='production')
            with self.assertRaises(ResultProxyError):
                proxy.logs(bad, after=0, limit=2)
            import http.client
            connection = http.client.HTTPSConnection('localhost', server.server_address[1], context=client, timeout=3)
            connection.request('GET', '/ltds/worker/v1/jobs/' + self.job['jobId'] + '/logs?generation=1&after=0&limit=2&path=secret')
            self.assertEqual(connection.getresponse().status, 400)
            connection.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=3)
            self.assertFalse(thread.is_alive())


class PublicLogTests(unittest.TestCase):
    setUp = api_tests.ReferenceAPITests.setUp
    tearDown = api_tests.ReferenceAPITests.tearDown
    make_api = api_tests.ReferenceAPITests.make_api
    request = api_tests.ReferenceAPITests.request
    submit = api_tests.ReferenceAPITests.submit
    ready = api_tests.ReferenceAPITests.ready
    def test_public_queued_bound_empty_and_wrong_scope(self):
        job = self.submit()
        path = '/jobs/' + job['jobId'] + '/logs?generation=1&after=3&limit=2'
        response = self.request('GET', path)
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body, logs.page(job, [], 3))
        self.assertEqual(self.request('GET', path, token=api_tests.PRODUCTION).status, 404)
        self.assertEqual(self.request('GET', path.replace('generation=1','generation=2')).status, 409)
        self.assertEqual(self.request('GET', path+'&after=0').status, 400)

    def test_assigned_job_uses_fixed_proxy_and_reports_unavailable(self):
        job, _result, _claim = self.ready()
        path = '/jobs/' + job['jobId'] + '/logs?generation=1&after=0&limit=2'
        self.assertEqual(self.request('GET', path).status, 503)
        from unittest.mock import Mock
        proxy = Mock()
        proxy.logs.return_value = logs.page(job, [{'level':'info','message':'engine line'}], 0)
        api = self.make_api(result_proxy=proxy)
        self.assertEqual(self.request('GET', path, api=api).status, 200)
        self.assertEqual(proxy.logs.call_args.args[0]['nodeId'], 'cluster')
        proxy.logs.side_effect = OSError('private bearer and engine URL')
        response = self.request('GET', path, api=api)
        self.assertEqual(response.status, 503)
        self.assertEqual(response.body, {'error':'execution_logs_unavailable'})


class LogProxyValidationTests(unittest.TestCase):
    def test_proxy_rejects_bad_binding_redirect_big_page_cursor_and_closes_budget(self):
        from tests.test_reference_result_proxy import Response, Lease
        job = dict(jobId='job', attemptId='00000000-0000-4000-8000-000000000001', generation=1, environment='staging', nodeId='cluster')
        expected = logs.page(job, [{'level':'info','message':'line'}], 0)
        cases = [dict(expected, generation=2), dict(expected, nextCursor=99), dict(expected, nodeId='other'),
            dict(expected, attemptId='other'), dict(expected, hasMore='true'), dict(expected, lines=[])]
        for value in cases + [b'x'*(logs.MAX_LOG_BYTES+1), 'redirect']:
            lease = Lease()
            responses=[]
            def opener(request, **kwargs):
                raw = value if type(value) is bytes else json.dumps(expected if value=='redirect' else value).encode()
                response = Response(raw, url='https://other.example' if value=='redirect' else request.full_url,
                    headers={'Content-Type':'application/json'})
                responses.append(response)
                return response
            proxy = ResultProxy({'cluster':'https://worker.example'}, worker_bearers={'cluster':TOKEN}, admit_read=lambda _:lease, opener=opener)
            with self.subTest(value=str(value)[:50]), self.assertRaises(ResultProxyError):
                proxy.logs(job, after=0, limit=2)
            self.assertTrue(responses[0].closed)
            self.assertTrue(lease.exited)


class NativeOutputPrefixTests(unittest.TestCase):
    def test_prefix_decoder_chunks_escaping_multibyte_eof_and_invalid_pages(self):
        class Chunked(io.BytesIO):
            def read(self,size):
                return super().read(min(size,1))
        values=['first "quoted" \\ line\n', 'é😀', 'third']
        raw=json.dumps(values,ensure_ascii=False).encode()
        with Chunked(raw) as response:
            self.assertEqual(logs.read_output_prefix(response,2),(values[:2],True))
        with Chunked(raw) as response:
            self.assertEqual(logs.read_output_prefix(response,3),(values,False))
        moderate=['x'*4000]*300
        with io.BytesIO(json.dumps(moderate).encode()) as response:
            page,more=logs.read_output_prefix(response,200)
            self.assertTrue(more)
            self.assertTrue(0<len(page)<200)
            self.assertEqual(page,moderate[:len(page)])
        for raw in (b'["first",]',b'["first"]garbage',b'["first",3]',b'["first", "broken\\q"]',
                    b'["first"',b'{"error":"secret"}',b'["\\ud800"]',b'["'+b'x'*(logs.MAX_LOG_BYTES+1)+b'"]',
                    b'["first","second","third"malformed'):
            with self.subTest(raw=raw[:30]), io.BytesIO(raw) as response, self.assertRaises((ValueError,UnicodeError)):
                logs.read_output_prefix(response,2)

    def test_native_actual_http_huge_remaining_tail_advances_without_buffering_tail(self):
        from processing.viewer_service import NativeEngine
        from urllib.parse import urlsplit,parse_qs
        values=['prefix 😀 "'+str(index)+'" '+('x'*80) for index in range(60000)]
        self.assertGreater(len(json.dumps(values,ensure_ascii=False).encode()),4*1024*1024)
        requested=[]
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args):
                pass
            def do_GET(self):
                parsed=urlsplit(self.path)
                if parsed.path!='/task/persisted-job/output':
                    self.send_error(404)
                    return
                after=int(parse_qs(parsed.query)['line'][0])
                requested.append(after)
                body=json.dumps(values[after:],ensure_ascii=False).encode()
                self.send_response(200)
                self.send_header('Content-Type','application/json')
                self.send_header('Content-Length',str(len(body)))
                self.end_headers()
                try:
                    self.wfile.write(body)
                except (BrokenPipeError,ConnectionResetError,ConnectionAbortedError):
                    pass
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        server.daemon_threads=False
        thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':0.02})
        thread.start()
        try:
            engine=NativeEngine('http://127.0.0.1:'+str(server.server_address[1]))
            cursor=0
            for _ in range(3):
                lines,more=engine.output_page('persisted-job',line=cursor,limit=200)
                self.assertEqual(lines,values[cursor:cursor+200])
                self.assertTrue(more)
                cursor+=len(lines)
            self.assertEqual(requested,[0,200,400])
            self.assertEqual(engine.output_page('persisted-job',line=len(values)-1,limit=200),([values[-1]],False))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=3)
            self.assertFalse(thread.is_alive())

    def test_native_prefix_closes_response_early_and_on_malformed_or_large_first_line(self):
        from processing.viewer_service import NativeEngine,EngineUnavailable
        from tests.test_reference_result_proxy import Response
        engine=NativeEngine('http://127.0.0.1:3001')
        target=engine.origin+'/task/persisted-job/output?line=0'
        for raw,success in ((b'["a","b","c",'+b'"long",'*100000+b'"end"]',True),
                (b'["a", false]',False),(b'["'+b'x'*(logs.MAX_LOG_BYTES+1)+b'"]',False)):
            response=Response(raw,url=target,headers={'Content-Type':'application/json'})
            from unittest.mock import Mock
            opener=Mock()
            opener.open.return_value=response
            with patch('processing.viewer_service.build_opener',return_value=opener):
                if success:
                    self.assertEqual(engine.output_page('persisted-job',line=0,limit=2),(['a','b'],True))
                    self.assertLess(sum(response.read_sizes),logs.MAX_LOG_BYTES)
                else:
                    with self.assertRaises(EngineUnavailable):
                        engine.output_page('persisted-job',line=0,limit=2)
            self.assertTrue(response.closed)
            self.assertTrue(all(size<=4096 for size in response.read_sizes))
