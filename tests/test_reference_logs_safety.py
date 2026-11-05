"""Log safety over the real chain: NodeODM HTTP, worker TLS API, proxy, public TLS API."""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import http.client
import json
import ssl
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from processing import reference_logs as logs
from processing.prototype import Conflict, Queue
from processing.reference_api import ReferenceAPI
from processing.reference_http import create_reference_http_server
from processing.reference_result_api import ReferenceResultAPI
from processing.reference_result_proxy import ResultProxy
from processing.reference_store import PROFILE, ReferenceStore
from processing.viewer_service import NativeEngine
from tests.test_reference_results import ResultsFixture

READER = 'synthetic-result-log-reader-token'
PUBLIC = 'synthetic-public-log-reader-token'
OTHER = 'synthetic-other-env-reader-token'
SECRET = 'SYNTHSECRET7'
SHAPES = (
    'Authorization: Bearer ' + SECRET, 'authorization: basic ' + SECRET, 'Proxy-Authorization: Bearer x' + SECRET,
    'curl -H "Authorization: Bearer ' + SECRET + '" https://h/x', 'GET /tiles?token=' + SECRET + '&z=1',
    'https://h/a?access_token=' + SECRET, '{"token":"' + SECRET + '"}', '{\\"token\\":\\"' + SECRET + '\\"}',
    "{'password': '" + SECRET + "'}", 'odm --token ' + SECRET + ' --fast', 'odm --password "a ' + SECRET + '"',
    'odm --token=' + SECRET, 'curl -u user:' + SECRET + ' h', 'https://user:' + SECRET + '@host/x',
    's3://key:' + SECRET + '@bucket/x', 'postgres://u:' + SECRET + '@db:5432/app', 'aws_secret_access_key=' + SECRET,
    'AWS_SESSION_TOKEN=' + SECRET, 'access_token=' + SECRET, 'client_secret=' + SECRET, 'api_key: ' + SECRET,
    'X-Api-Key: ' + SECRET, 'Cookie: session=' + SECRET + '; other=1', 'Set-Cookie: id=' + SECRET,
    'pass\x00word=' + SECRET, 'tok\x1b[0men=' + SECRET, 'pass\u200bword=' + SECRET, 'password="a\\"b' + SECRET + '"',
    '{"tokens":["a","' + SECRET + '"]}', 'X-Amz-Signature=' + SECRET + '&X-Amz-Date=1',
    '-----BEGIN PRIVATE KEY----- ' + SECRET, 'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.' + SECRET)
HOST_DETAIL = ('/var/www/data/job-1/' + SECRET + '.tif', 'C:\\Users\\' + SECRET + '\\x.tif',
               'file:///srv/' + SECRET + '/x', '\\\\nas\\' + SECRET + '\\share')


class EngineHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        node = self.server.node
        parsed = urlsplit(self.path)
        node.requests.append(self.path)
        status, body = 200, None
        if parsed.path == '/task/list':
            body = json.dumps([{'uuid': 'other'}] + ([] if node.missing else [{'uuid': node.uuid}])).encode()
        elif parsed.path.endswith('/info'):
            uuid = parsed.path.split('/')[2]
            if node.missing == '404':
                status, body = 404, b'{"error":"gone"}'
            elif node.missing == '200':
                body = json.dumps({'error': uuid + ' not found'}).encode()
            elif node.failing:
                status, body = 500, json.dumps({'error': node.failing}).encode()
            else:
                body = json.dumps({'uuid': uuid, 'status': {'code': node.code}}).encode()
        elif parsed.path.endswith('/output'):
            line = int(parse_qs(parsed.query)['line'][0])
            if node.raw is not None:
                body = node.raw
            else:
                body = b'[' + b','.join(item if type(item) is bytes else json.dumps(item).encode()
                                        for item in node.output[line:]) + b']'
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except OSError:
            pass


class Node:
    def __init__(self):
        self.output, self.raw, self.code, self.missing, self.failing, self.requests = [], None, 20, None, None, []


class LogChain(ResultsFixture, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'localhost')])
        now = datetime.now(timezone.utc)
        cert = (x509.CertificateBuilder().subject_name(subject).issuer_name(subject).public_key(key.public_key())
            .serial_number(x509.random_serial_number()).not_valid_before(now-timedelta(minutes=1)).not_valid_after(now+timedelta(hours=1))
            .add_extension(x509.SubjectAlternativeName([x509.DNSName('localhost')]), critical=False).sign(key, hashes.SHA256()))
        cls.pem = (cert.public_bytes(serialization.Encoding.PEM), key.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))

    def serve(self, server):
        thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': 0.02})
        thread.start()
        def stop():
            server.shutdown()
            server.server_close()
            thread.join(timeout=3)
        self.addCleanup(stop)
        return server.server_address[1]

    def setUp(self):
        super().setUp()
        certfile, keyfile = self.root/'logs-cert.pem', self.root/'logs-key.pem'
        certfile.write_bytes(self.pem[0])
        keyfile.write_bytes(self.pem[1])
        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls.load_cert_chain(certfile, keyfile)
        self.client = ssl.create_default_context(cafile=str(certfile))
        self.node = Node()
        self.node.uuid = self.job['jobId']
        engine_server = ThreadingHTTPServer(('127.0.0.1', 0), EngineHandler)
        engine_server.daemon_threads, engine_server.node = False, self.node
        self.worker.engine = NativeEngine('http://127.0.0.1:' + str(self.serve(engine_server)))
        self.shared = True
        worker_api = ReferenceResultAPI(self.results, bearer_readers={READER: dict(node_id='cluster', worker_id='worker',
            environments=('staging',), share_policy=lambda metadata: self.shared)})
        worker_port = self.serve(create_reference_http_server(worker_api, ssl_context=tls))
        @contextmanager
        def admit(binding):
            yield
        proxy = ResultProxy({'cluster': 'https://localhost:' + str(worker_port)}, worker_bearers={'cluster': READER},
            admit_read=admit, ssl_context=self.client)
        self.public = dict(self.worker.log_metadata(self.job['jobId']), status='running', phase='processing')
        self.queue = queue = Queue(str(self.root / 'queue'))
        queue.allowed_profiles.add(PROFILE)
        store = ReferenceStore(queue)
        def job(job_id, private=False):
            if job_id != self.public['jobId']:
                raise Conflict('missing')
            return self.public
        store.get = job
        self.api = ReferenceAPI(store, bearer_tokens={PUBLIC: 'staging', OTHER: 'production'},
            capability_fingerprint='0'*64, options_fingerprint='0'*64, allowed_odm_options=set(), result_proxy=proxy)
        self.port = self.serve(create_reference_http_server(self.api, ssl_context=tls))
        self.worker._phase(self.job['jobId'], 'processing')

    def tearDown(self):
        self.queue.close()
        super().tearDown()

    def raw(self, after=0, limit=200, *, token=PUBLIC, generation=1, tail=''):
        connection = http.client.HTTPSConnection('localhost', self.port, context=self.client, timeout=10)
        try:
            connection.request('GET', '/ltds/v1/jobs/' + self.job['jobId'] + '/logs?generation=%d&after=%d&limit=%d%s' % (generation, after, limit, tail),
                headers={'Authorization': 'Bearer ' + token})
            response = connection.getresponse()
            return response.status, response.read()
        finally:
            connection.close()

    def get(self, after=0, limit=200):
        status, body = self.raw(after, limit)
        return status, json.loads(body)

    def page(self, after=0, limit=200):
        status, body = self.get(after, limit)
        self.assertEqual(status, 200, body)
        return body

    def texts(self, page):
        return [line['message'] for line in page['lines']]

    def assertUnavailable(self, reason):
        status, body = self.get()
        self.assertEqual((status, body), (503, {'error': 'execution_logs_unavailable', 'state': 'unavailable', 'reason': reason}))


class RedactionTests(LogChain):
    def test_every_secret_shape_leaves_as_one_redacted_line(self):
        self.node.output = list(SHAPES)
        status, body = self.raw()
        self.assertEqual(status, 200)
        self.assertNotIn(SECRET.encode(), body)
        page = json.loads(body)
        self.assertEqual((len(page['lines']), page['nextCursor']), (len(SHAPES), len(SHAPES)))
        self.assertTrue(all('[redacted]' in text for text in self.texts(page)), self.texts(page))

    def test_header_keeps_no_scheme_tail(self):
        self.node.output = ['Authorization: Bearer abc']
        self.assertEqual(self.texts(self.page()), ['Authorization: [redacted]'])

    def test_host_paths_escapes_and_markup_are_removed_and_plain_lines_stay(self):
        self.node.output = list(HOST_DETAIL) + ['\x1b[31mred\x1b[0m\x00 \x1b]0;title\x07done\r\nnext', '<script>x</script>',
            'Running opensfm stage 2/5', 'sfm-algorithm: incremental', 'https://example.test/docs/page']
        status, body = self.raw()
        self.assertNotIn(SECRET.encode(), body)
        page = json.loads(body)
        self.assertEqual(self.texts(page)[:4], ['[path]'] * 4)
        self.assertEqual(self.texts(page)[4:], ['red done  next', '\u2039script>x\u2039/script>',
            'Running opensfm stage 2/5', 'sfm-algorithm: incremental', 'https://example.test/docs/page'])

    def test_expansion_is_sanitized_before_truncation_and_page_stays_bound(self):
        expanding = 'a=' + 'token=x ' * 3000
        boundary = 'y' * 3996 + ' token=' + SECRET
        self.node.output = [expanding, boundary] + [expanding] * 198
        status, body = self.raw()
        self.assertEqual(status, 200)
        self.assertLessEqual(len(body), logs.MAX_LOG_BYTES)
        self.assertNotIn(SECRET.encode(), body)
        page = json.loads(body)
        self.assertTrue(page['hasMore'])
        self.assertEqual(page['nextCursor'], len(page['lines']))
        self.assertNotIn('token=x', page['lines'][0]['message'])
        self.assertTrue(all(len(line['message']) <= 4000 for line in page['lines']))

    def test_sanitizer_failure_omits_the_line_and_never_blocks_the_page(self):
        self.node.output = ['token=' + SECRET, 'second']
        with patch.object(logs, 'sanitize', side_effect=RuntimeError(SECRET)):
            status, body = self.raw()
        self.assertEqual(status, 200)
        self.assertNotIn(SECRET.encode(), body)
        self.assertEqual(self.texts(json.loads(body)), ['[line omitted: unreadable]'] * 2)

    def test_adversarial_lines_stay_near_linear(self):
        import time
        size = logs.MAX_LINE_BYTES - 16
        for text in ('token="' * (size // 7), 'a://' * (size // 4), '--' * (size // 2), '/a' * (size // 2), '\\' * size):
            started = time.monotonic()
            logs.message(text)
            self.assertLess(time.monotonic() - started, 1)


class PoisonLineTests(LogChain):
    def test_poison_line_keeps_good_lines_and_cursor_advances_at_every_limit(self):
        self.node.output = ['zero', 'one', 'two', 'P' * 300 * 1024, 'four', 'five']
        before = self.worker._row(self.job['jobId'])
        first = self.page(0, 200)
        self.assertEqual(self.texts(first)[:3], ['zero', 'one', 'two'])
        self.assertRegex(first['lines'][3]['message'], r'^\[line omitted: over \d+ bytes\]$')
        self.assertEqual((first['nextCursor'], first['hasMore']), (4, True))
        rest = self.page(4, 200)
        self.assertEqual((self.texts(rest), rest['nextCursor'], rest['hasMore']), (['four', 'five'], 6, False))
        cursor, seen = 0, []
        for _ in range(6):
            page = self.page(cursor, 1)
            self.assertEqual(page['nextCursor'], cursor + 1)
            seen += self.texts(page)
            cursor = page['nextCursor']
        self.assertEqual(len(seen), 6)
        self.assertEqual(self.page(6, 1)['nextCursor'], 6)
        self.assertEqual(self.worker._row(self.job['jobId']), before)

    def test_consecutive_and_final_poison_lines_end(self):
        self.node.output = ['P' * 300 * 1024, 'Q' * 40 * 1024]
        cursor = 0
        for expected in (1, 2):
            page = self.page(cursor, 200)
            self.assertEqual(page['nextCursor'], expected)
            cursor = expected
        last = self.page(cursor, 200)
        self.assertEqual((last['lines'], last['nextCursor'], last['hasMore']), ([], 2, False))

    def test_invalid_utf8_and_lone_surrogates_stay_one_replaced_line(self):
        self.node.output = ['good', b'"bad \xff\xfe line"', '\ud800 lone', 'after']
        page = self.page()
        self.assertEqual(self.texts(page), ['good', 'bad \ufffd\ufffd line', '\ufffd lone', 'after'])
        self.assertEqual(page['nextCursor'], 4)

    def test_broken_array_structure_stays_unavailable(self):
        self.node.raw = b'{"error":"' + SECRET.encode() + b'"}'
        status, body = self.raw()
        self.assertEqual(status, 503)
        self.assertNotIn(SECRET.encode(), body)
        self.assertEqual(json.loads(body)['reason'], 'engine_unavailable')


class ContinuityTests(LogChain):
    def test_normal_pages_are_only_unverified(self):
        self.node.output = ['a', 'b', 'c', 'd', 'e']
        for after in (0, 3, 5):
            self.assertEqual(self.page(after)['continuity'], 'unverified')

    def test_shrunken_output_is_reset_and_unlike_idle(self):
        self.node.output = ['a', 'b', 'c', 'd', 'e']
        idle = self.page(5)
        self.assertEqual((idle['lines'], idle['nextCursor'], idle['hasMore'], idle['continuity']), ([], 5, False, 'unverified'))
        self.node.output = []
        reset = self.page(5)
        self.assertEqual((reset['lines'], reset['nextCursor'], reset['hasMore'], reset['continuity']), ([], 0, False, 'reset'))
        self.node.output = ['rerun']
        self.assertEqual(self.texts(self.page(reset['nextCursor'])), ['rerun'])

    def test_cursor_beyond_high_water_is_not_accepted(self):
        self.node.output = ['a', 'b']
        page = self.page(1000)
        self.assertEqual((page['nextCursor'], page['continuity']), (0, 'reset'))
        self.assertEqual(self.page(3)['continuity'], 'reset')
        self.assertEqual(self.page(2)['continuity'], 'unverified')

    def test_regrown_rerun_is_undetectable_and_is_not_called_continuous(self):
        self.node.output = ['rerun-%d' % index for index in range(7)]
        page = self.page(5)
        self.assertEqual(self.texts(page), ['rerun-5', 'rerun-6'])
        self.assertEqual(page['continuity'], 'unverified')

    def test_full_limit_positive_cursor_still_advances_within_engine_bound(self):
        self.node.output = [str(index) for index in range(500)]
        page = self.page(100, 200)
        self.assertEqual((page['nextCursor'], page['hasMore']), (299, True))
        self.assertEqual(self.texts(page)[0], '100')


class StateTests(LogChain):
    def test_pending_before_engine_makes_no_worker_or_engine_read(self):
        self.public.update(phase='fetching')
        page = self.page()
        self.assertEqual((page['state'], page['continuity'], page['lines']), ('pending', 'unverified', []))
        self.assertEqual(self.node.requests, [])
        stale = self.page(3)
        self.assertEqual((stale['state'], stale['continuity'], stale['nextCursor']), ('pending', 'reset', 0))

    def test_pending_on_worker_before_engine_task_exists(self):
        self.worker._phase(self.job['jobId'], 'upload_pending')
        self.assertEqual(self.page()['state'], 'pending')
        self.assertEqual(self.node.requests, [])

    def test_pending_while_queued_in_engine_then_open_then_complete(self):
        self.node.output = ['line']
        for code, expected in ((10, 'pending'), (20, 'open'), (30, 'complete'), (40, 'complete'), (50, 'complete')):
            self.node.code = code
            self.assertEqual(self.page()['state'], expected)
        self.node.code = 20
        self.worker._phase(self.job['jobId'], 'awaiting_delivery')
        self.assertEqual(self.page()['state'], 'complete')

    def test_expired_only_after_finish_for_both_engine_absence_answers(self):
        self.worker._phase(self.job['jobId'], 'delivered')
        for missing in ('404', '200'):
            self.node.missing = missing
            page = self.page(5)
            self.assertEqual((page['state'], page['continuity'], page['lines'], page['nextCursor'], page['hasMore']),
                             ('expired', 'unknown', [], 5, False))
        self.worker._phase(self.job['jobId'], 'processing')
        self.assertUnavailable('engine_task_missing')
        self.public.update(status='failed')
        self.assertEqual(self.page()['state'], 'expired')

    def test_never_started_finished_job_is_expired(self):
        self.public.update(status='cancelled', phase='cancelled', nodeId=None)
        self.assertEqual(self.page()['state'], 'expired')

    def test_unavailable_stays_distinct_and_reasoned_without_leaks(self):
        self.node.failing = 'token=' + SECRET
        status, body = self.raw()
        self.assertEqual(status, 503)
        self.assertNotIn(SECRET.encode(), body)
        self.assertUnavailable('engine_unavailable')
        self.node.failing = None
        with patch.object(self.worker, 'engine_probe', return_value='different-engine'):
            self.assertUnavailable('engine_identity')
        self.shared = False
        self.assertUnavailable('not_shared')
        self.shared = True
        self.assertEqual(self.get()[0], 200)
        with patch.object(self.worker, '_row', side_effect=RuntimeError(SECRET)):
            status, body = self.raw()
        self.assertEqual(status, 503)
        self.assertNotIn(SECRET.encode(), body)
        self.assertEqual(json.loads(body)['reason'], 'execution_unverified')
        self.api.result_proxy = None
        self.assertUnavailable('not_configured')

    def test_unsupported_and_unknown_worker_failures_are_closed_reasons(self):
        self.assertEqual(logs.reason(ValueError('execution_logs_unsupported')), 'unsupported')
        self.assertEqual(logs.reason(ValueError(SECRET)), 'worker_unavailable')
        self.assertEqual(logs.unavailable(SECRET)['reason'], 'worker_unavailable')
        self.assertEqual(logs.worker_reason(500, json.dumps({'error': SECRET})), 'worker_unavailable')

    def test_existing_refusals_are_unchanged(self):
        self.node.output = ['line']
        self.assertEqual(self.raw(token='synthetic-wrong-log-reader-token')[0], 401)
        self.assertEqual(self.raw(token=OTHER)[0], 404)
        self.assertEqual(self.raw(generation=2)[0], 409)
        self.assertEqual(self.raw(tail='&path=secret')[0], 400)
        self.assertEqual(self.raw(limit=201)[0], 400)

    def test_page_bound_and_message_truncation_hold(self):
        self.node.output = ['\U0001F600' * 4000] * 200 + ['x' * 5000]
        status, body = self.raw()
        self.assertEqual(status, 200)
        self.assertLessEqual(len(body), logs.MAX_LOG_BYTES)
        self.node.output = ['x' * 5000]
        self.assertEqual(len(self.texts(self.page())[0]), 4000)


class PageValidationTests(unittest.TestCase):
    job = dict(jobId='job', attemptId='a', generation=1, environment='staging', nodeId='cluster')

    def test_page_shape_is_closed_and_unavailable_is_never_a_page(self):
        good = logs.page(self.job, [], 5, state='open', continuity='reset')
        self.assertEqual(logs.validate(good, self.job, 5, 2)['nextCursor'], 0)
        line = [{'level': 'info', 'message': 'x'}]
        for bad in (dict(good, state='unavailable'), dict(good, continuity='verified'), dict(good, nextCursor=5),
                    dict(good, lines=line), dict(good, hasMore=True), {k: v for k, v in good.items() if k != 'state'},
                    dict(logs.idle(self.job, 5, 'expired'), lines=line, nextCursor=6)):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                logs.validate(bad, self.job, 5, 2)
        with self.assertRaises(ValueError):
            logs.validate(logs.page(self.job, [], 0, continuity='reset'), self.job, 0, 2)
