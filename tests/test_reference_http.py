import http.client
import copy
import hashlib
import json
from pathlib import Path
import socket
import tempfile
import threading
import unittest

from processing.prototype import Queue
from processing.reference_api import ReferenceAPI
from processing.reference_control import ReferenceControlAPI
from processing.reference_http import create_reference_http_server
from processing.reference_protocol import MAX_BODY_BYTES
from processing.reference_protocol import digest
from processing.reference_store import ReferenceStore, PROFILE
from processing.reference_failure import FailureOutbox, make_failure
from processing.reference_binary import BinaryResponse


STAGING = "synthetic-http-staging-token"
PRODUCTION = "synthetic-http-production-token"
WORKER = 'synthetic-http-worker-token'


class BinaryHeadOrderingTests(unittest.TestCase):
    def serve(self, stream, length='22'):
        class API:
            def handle(self, *args, **kwargs):
                return BinaryResponse(stream, 200, {'Content-Type':'application/zip','Content-Length':length})
        server=create_reference_http_server(API())
        thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':0.02})
        thread.start()
        self.addCleanup(lambda:(server.shutdown(),server.server_close(),thread.join(2)))
        return server

    def test_head_success_waits_for_reader_close(self):
        entered,release=threading.Event(),threading.Event();calls=[]
        class Stream:
            def close(self):
                calls.append('close');entered.set()
                if not release.wait(3):raise RuntimeError('test close timed out')
        server=self.serve(Stream())
        with socket.create_connection(server.server_address,timeout=2) as client:
            try:
                client.sendall(b'HEAD / HTTP/1.1\r\nHost: localhost\r\n\r\n')
                self.assertTrue(entered.wait(2));client.settimeout(0.05)
                with self.assertRaises(socket.timeout):client.recv(4096)
                release.set();client.settimeout(2)
                self.assertIn(b'200 OK',client.recv(4096))
                self.assertEqual(calls,['close'])
            finally:release.set()

    def test_uncertain_head_close_never_sends_success_or_retries_close(self):
        calls=[]
        class Stream:
            def close(self):calls.append('close');raise OSError('synthetic close uncertainty')
        server=self.serve(Stream())
        with socket.create_connection(server.server_address,timeout=2) as client:
            client.sendall(b'HEAD / HTTP/1.1\r\nHost: localhost\r\n\r\n')
            self.assertEqual(client.recv(4096),b'')
        self.assertEqual(calls,['close'])


class BinaryBodyOrderingTests(unittest.TestCase):
    serve = BinaryHeadOrderingTests.serve

    def test_get_withholds_final_byte_until_reader_close_finishes(self):
        entered, release = threading.Event(), threading.Event()
        calls = []
        class Stream:
            def chunks(self):
                yield b'a'
                yield b''
                yield b'bc'
            def close(self):
                calls.append('close'); entered.set()
                if not release.wait(3): raise OSError('synthetic close timeout')
        server = self.serve(Stream(), '3')
        with socket.create_connection(server.server_address, timeout=2) as client:
            try:
                client.sendall(b'GET / HTTP/1.1\r\nHost: localhost\r\n\r\n')
                self.assertTrue(entered.wait(2))
                received = b''
                while b'\r\n\r\n' not in received or len(received.split(b'\r\n\r\n', 1)[1]) < 2:
                    part = client.recv(4096); self.assertTrue(part); received += part
                self.assertEqual(received.split(b'\r\n\r\n', 1)[1], b'ab')
                client.settimeout(0.05)
                with self.assertRaises(socket.timeout): client.recv(4096)
                release.set(); client.settimeout(2)
                remainder = b''
                while part := client.recv(4096): remainder += part
                self.assertEqual(remainder, b'c')
                self.assertEqual(calls, ['close'])
            finally:
                release.set()

    def test_failed_get_close_leaves_incomplete_body_and_is_not_retried(self):
        for body in (b'a', b'abc'):
            with self.subTest(bytes=len(body)):
                calls = []
                class Stream:
                    def chunks(self): yield body
                    def close(self):
                        calls.append('close'); raise OSError('synthetic close uncertainty')
                server = self.serve(Stream(), str(len(body)))
                connection = http.client.HTTPConnection(*server.server_address, timeout=2)
                try:
                    connection.request('GET', '/')
                    response = connection.getresponse()
                    with self.assertRaises(http.client.IncompleteRead) as failure: response.read()
                    self.assertEqual(failure.exception.partial, body[:-1])
                    self.assertEqual(calls, ['close'])
                finally:
                    connection.close()

    def test_late_iterator_failure_extra_bytes_and_truncation_cannot_complete(self):
        for mode in ('late_failure', 'extra', 'truncated'):
            with self.subTest(mode=mode):
                calls = []
                class Stream:
                    def chunks(self):
                        yield b'ab' if mode == 'truncated' else b'abc'
                        if mode == 'late_failure': raise OSError('synthetic late validation failure')
                        if mode == 'extra': yield b'd'
                    def close(self): calls.append('close')
                server = self.serve(Stream(), '3')
                connection = http.client.HTTPConnection(*server.server_address, timeout=2)
                try:
                    connection.request('GET', '/')
                    response = connection.getresponse()
                    with self.assertRaises(http.client.IncompleteRead) as failure: response.read()
                    self.assertEqual(failure.exception.partial, b'a' if mode == 'truncated' else b'ab')
                    self.assertEqual(calls, ['close'])
                finally:
                    connection.close()

    def test_empty_get_waits_for_empty_eof_and_close_before_headers(self):
        entered, release = threading.Event(), threading.Event()
        calls = []
        class Stream:
            def chunks(self):
                yield b''
            def close(self):
                calls.append('close'); entered.set()
                if not release.wait(3): raise OSError('synthetic close timeout')
        server = self.serve(Stream(), '0')
        with socket.create_connection(server.server_address, timeout=2) as client:
            try:
                client.sendall(b'GET / HTTP/1.1\r\nHost: localhost\r\n\r\n')
                self.assertTrue(entered.wait(2)); client.settimeout(0.05)
                with self.assertRaises(socket.timeout): client.recv(4096)
                release.set(); client.settimeout(2)
                self.assertIn(b'200 OK', client.recv(4096))
                self.assertEqual(calls, ['close'])
            finally:
                release.set()

    def test_empty_get_rejects_extra_bytes_or_late_failure_before_success(self):
        for mode in ('extra', 'late_failure'):
            with self.subTest(mode=mode):
                calls = []
                class Stream:
                    def chunks(self):
                        if mode == 'extra': yield b'x'
                        else: raise OSError('synthetic late empty validation failure')
                    def close(self): calls.append('close')
                server = self.serve(Stream(), '0')
                with socket.create_connection(server.server_address, timeout=2) as client:
                    client.sendall(b'GET / HTTP/1.1\r\nHost: localhost\r\n\r\n')
                    self.assertEqual(client.recv(4096), b'')
                self.assertEqual(calls, ['close'])


class ReferenceHTTPTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.queue = Queue(self.directory.name)
        self.queue.allowed_profiles.add(PROFILE)
        self.store = ReferenceStore(self.queue)
        self.fixture = json.loads((Path(__file__).resolve().parents[1] /
                                  "contracts/reference-v1.contract.json").read_text())
        self.api = ReferenceControlAPI(self.store, viewer_bearers={STAGING: "staging", PRODUCTION: "production"},
            worker_bearers={WORKER: dict(node_id='cluster', worker_id='worker', worker_instance_id='instance')},
            capability_fingerprint=self.fixture["capabilities"]["capabilityFingerprint"],
            options_fingerprint=self.fixture["capabilities"]["engines"]["odm"]["optionsFingerprint"])
        self.server = create_reference_http_server(self.api, request_timeout=0.2)
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       kwargs={"poll_interval": 0.02}, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.queue.close()
        self.directory.cleanup()

    def request(self, method, path, payload=None, *, token=STAGING):
        connection = http.client.HTTPConnection(*self.server.server_address, timeout=2)
        headers = {} if token is None else {"Authorization": "Bearer " + token}
        body = None if payload is None else json.dumps(payload).encode()
        connection.request(method, path, body=body, headers=headers)
        response = connection.getresponse()
        result = response.status, json.loads(response.read()), dict(response.getheaders())
        connection.close()
        return result

    def raw(self, request, *, eof=False):
        connection = socket.create_connection(self.server.server_address, timeout=2)
        connection.sendall(request)
        if eof:
            connection.shutdown(socket.SHUT_WR)
        result = b""
        while True:
            chunk = connection.recv(4096)
            if not chunk:
                break
            result += chunk
        connection.close()
        headers, body = result.split(b"\r\n\r\n", 1)
        status = int(headers.split(b" ", 2)[1])
        return status, json.loads(body), headers

    def test_real_http_auth_capability_submit_status_replay_and_cross_environment(self):
        self.assertEqual(self.request("GET", "/ltds/v1/capabilities", token=None)[0], 401)
        status, capabilities, headers = self.request("GET", "/ltds/v1/capabilities")
        self.assertEqual((status, capabilities), (200, self.fixture["capabilities"]))
        self.assertEqual(headers["Cache-Control"], "no-store")
        self.assertEqual(headers["Connection"], "close")
        status, job, _ = self.request("POST", "/ltds/v1/jobs", self.fixture["submission"])
        self.assertEqual(status, 201)
        status, replay, _ = self.request("POST", "/ltds/v1/jobs", self.fixture["submission"])
        self.assertEqual((status, replay), (200, job))
        recovery = '/ltds/v1/jobs/by-submission/' + self.fixture['submission']['submissionId']
        self.assertEqual(self.request('GET', recovery)[:2], (200, job))
        self.assertEqual(self.request('GET', recovery, token=PRODUCTION)[0], 404)
        path = "/ltds/v1/jobs/" + job["jobId"]
        self.assertEqual(self.request("GET", path)[:2], (200, job))
        self.assertEqual(self.request("GET", path, token=PRODUCTION)[:2], (404, {"error": "not_found"}))
        self.assertEqual(self.request("GET", path + "/results")[0], 409)
        for forbidden in (STAGING, PRODUCTION, "claimToken", "submission", "claim_hash"):
            self.assertNotIn(forbidden, json.dumps(job))

    def test_scoped_grant_replay_and_status_recovery_never_expose_secret(self):
        _, job, _ = self.request('POST', '/ltds/v1/jobs', self.fixture['submission'])
        path = '/ltds/v1/jobs/' + job['jobId']
        grant = copy.deepcopy(self.fixture['transferGrant'])
        grant['jobId'] = job['jobId']
        self.assertEqual(self.request('POST', path + '/transfer-grants', grant)[0], 409)
        self.store.claim(job['jobId'], node_id='cluster', worker_id='worker',
            worker_instance_id='instance', claim_request_id='claim-1')
        self.store.reserve(job['jobId'], self.fixture['transferRequest'], node_id='cluster')
        self.assertEqual(self.request('POST', path + '/transfer-grants', grant, token=PRODUCTION)[0], 404)
        status, ack, _ = self.request('POST', path + '/transfer-grants', grant)
        self.assertEqual(status, 200)
        self.assertTrue(ack['accepted'])
        self.assertEqual(self.request('POST', path + '/transfer-grants', grant)[:2], (200, ack))
        status, current, _ = self.request('GET', path)
        self.assertEqual(current['transferGrantAcknowledgement'], ack)
        self.assertNotIn(grant['token'], json.dumps(current))
        changed = {**grant, 'token': 'different-synthetic-grant-token'}
        self.assertEqual(self.request('POST', path + '/transfer-grants', changed)[0], 409)
        self.assertEqual(self.store.private_transfer_grant(job['jobId']), grant)

    def test_private_claim_is_accessible_only_through_separate_worker_identity(self):
        _, job, _ = self.request('POST', '/ltds/v1/jobs', self.fixture['submission'])
        route = '/ltds/worker/v1/claim'
        payload = dict(jobId=job['jobId'], claimRequestId='private-claim-1')
        self.assertEqual(self.request('POST', route, payload)[0], 401)
        status, claim, _ = self.request('POST', route, payload, token=WORKER)
        self.assertEqual(status, 200)
        self.assertEqual(claim['jobId'], job['jobId'])
        self.assertIsNotNone(claim['claimToken'])
        self.assertEqual(self.request('POST', route, payload, token=WORKER)[:2], (200, claim))
        public = self.request('GET', '/ltds/v1/jobs/' + job['jobId'])[1]
        self.assertNotIn(claim['claimToken'], json.dumps(public))
        self.assertEqual(self.request('GET', '/ltds/v1/capabilities', token=WORKER)[0], 401)

    def test_duplicate_private_claim_headers_fail_before_adapter(self):
        status, body, _ = self.raw(b'GET /ltds/worker/v1/jobs/job-1/transfer-grant HTTP/1.1\r\n'
            b'Host: localhost\r\nX-Claim-Token: first\r\nx-claim-token: second\r\n\r\n')
        self.assertEqual((status, body), (400, {'error': 'duplicate_header'}))

    def test_binary_result_http_scopes_auth_and_closes_stream_lease(self):
        _, job, _ = self.request('POST', '/ltds/v1/jobs', self.fixture['submission'])
        self.store.claim(job['jobId'], node_id='cluster', worker_id='worker',
            worker_instance_id='instance', claim_request_id='binary-claim')
        self.store.reserve(job['jobId'], self.fixture['transferRequest'], node_id='cluster')
        self.store.update_phase(job['jobId'], 'fetching')
        self.store.update_phase(job['jobId'], 'processing')
        archive = b'PK\x05\x06' + bytes(18)  # Valid empty synthetic ZIP; transport only.
        manifest = copy.deepcopy(self.fixture['resultManifest'])
        manifest.update(jobId=job['jobId'], pipelineDigest=job['pipelineDigest'])
        manifest['objects'][0].update(role='odm_archive', byteSize=len(archive), sha256=hashlib.sha256(archive).hexdigest())
        manifest['resultManifestSha256'] = digest({k:v for k,v in manifest.items() if k != 'resultManifestSha256'})
        self.store.set_result_manifest(job['jobId'], manifest)
        closed, calls = [], []
        class Stream:
            def __init__(self, body, status, headers):
                self.body, self.status, self.headers = body, status, headers
            def chunks(self):
                yield self.body
            def close(self):
                closed.append(True)
        class Proxy:
            def open(self, public, result, object_id, *, range_header=None):
                calls.append((public, range_header))
                body, status = archive, 200
                headers = {'Content-Type': 'application/zip', 'Content-Length': str(len(body)),
                           'Set-Cookie': 'must-not-forward'}
                if range_header:
                    start, end = map(int, range_header.removeprefix('bytes=').split('-'))
                    body, status = archive[start:end + 1], 206
                    headers.update({'Content-Length': str(len(body)),
                                    'Content-Range': f'bytes {start}-{end}/{len(archive)}'})
                return Stream(body, status, headers)
        self.api.viewer.result_proxy = Proxy()
        route = '/ltds/v1/jobs/' + job['jobId'] + '/results/result-1'
        self.assertEqual(self.request('GET', route, token=PRODUCTION)[0], 404)
        connection = http.client.HTTPConnection(*self.server.server_address, timeout=2)
        connection.request('GET', route, headers={'Authorization': 'Bearer ' + STAGING})
        response = connection.getresponse()
        self.assertEqual(response.status, 200)
        self.assertEqual(response.read(), archive)
        self.assertEqual(response.getheader('Content-Type'), 'application/zip')
        self.assertIsNone(response.getheader('Set-Cookie'))
        connection.close()
        for name in ('Range', 'range', 'rAnGe'):
            connection = http.client.HTTPConnection(*self.server.server_address, timeout=2)
            connection.request('GET', route, headers={'Authorization': 'Bearer ' + STAGING,
                                                       name: 'bytes=0-3'})
            response = connection.getresponse()
            self.assertEqual(response.status, 206)
            self.assertEqual(response.read(), archive[:4])
            self.assertEqual(response.getheader('Content-Range'), f'bytes 0-3/{len(archive)}')
            connection.close()
        connection = http.client.HTTPConnection(*self.server.server_address, timeout=2)
        connection.putrequest('GET', route)
        connection.putheader('Authorization', 'Bearer ' + STAGING)
        connection.putheader('Range', 'bytes=0-3')
        connection.putheader('range', 'bytes=4-7')
        connection.endheaders()
        response = connection.getresponse()
        self.assertEqual(response.status, 400)
        self.assertEqual(json.loads(response.read()), {'error': 'duplicate_header'})
        connection.close()
        self.assertTrue(closed)
        self.assertNotIn('claimToken', calls[0][0])
        self.assertEqual([call[1] for call in calls[1:]], ['bytes=0-3'] * 3)

    def test_confirmed_cancelled_outcome_and_metadata_acceptance_over_http(self):
        _, job, _ = self.request('POST', '/ltds/v1/jobs', self.fixture['submission'])
        path = '/ltds/v1/jobs/' + job['jobId']
        self.assertEqual(self.request('GET', path + '/terminal-outcome')[0], 409)
        _, claim, _ = self.request('POST', '/ltds/worker/v1/claim',
            {'jobId':job['jobId'],'claimRequestId':'cancel-claim'}, token=WORKER)
        failure = make_failure(job_id=job['jobId'], generation=1, environment='staging',
            input_manifest_sha256=job['manifestSha256'], pipeline_digest=job['pipelineDigest'],
            terminal_state='cancelled', error_code='confirmed_engine_cancelled')
        outbox = FailureOutbox(Path(self.directory.name) / 'failure-outbox.sqlite')
        outbox.persist(failure, engine_terminal=True)
        delivery = outbox.claim_next()
        envelope = {k:delivery[k] for k in ('outboxId','payloadSha256','payload')}
        status, acknowledgement, _ = self.request('POST', '/ltds/worker/v1/terminal-outcomes',
            dict(claimRequestId='cancel-claim',claimToken=claim['claimToken'],delivery=envelope),token=WORKER)
        self.assertEqual(status,200)
        self.assertEqual(acknowledgement['type'],'durable-failure-receipt')
        self.assertEqual(self.request('GET',path + '/terminal-outcome')[:2],(200,failure))
        self.assertEqual(self.request('GET',path + '/terminal-outcome',token=PRODUCTION)[0],404)
        public = self.request('GET',path)[1]
        self.assertEqual((public['status'],public['phase'],public['resultManifestSha256']),('cancelled','awaiting_acceptance',None))
        receipt = dict(schemaVersion=1,receiptId='terminal-receipt',storageReceiptId='terminal-storage',
            jobId=job['jobId'],generation=1,environment='staging',inputManifestSha256=job['manifestSha256'],
            pipelineDigest=job['pipelineDigest'],failureSha256=failure['failureSha256'],
            acceptedAt='2026-10-27T00:00:00.000Z',validationVersion='ltds-reference-v1')
        status, accepted, _ = self.request('POST',path + '/terminal-acceptance',receipt)
        self.assertEqual(status,200)
        self.assertTrue(accepted['terminalAccepted'])
        self.assertEqual((accepted['job']['status'],accepted['job']['phase']),('cancelled','completed'))
        self.assertFalse(self.request('POST',path + '/terminal-acceptance',receipt)[1]['created'])

    def test_plaintext_wildcard_and_external_bind_fail_before_listening(self):
        for host in ("0.0.0.0", "192.0.2.1", "::"):
            with self.subTest(host=host), self.assertRaises(ValueError):
                create_reference_http_server(self.api, host=host)

    def test_duplicate_headers_chunking_query_and_oversize_rejected(self):
        for request, expected in (
            (b"GET /ltds/v1/capabilities?token=secret HTTP/1.1\r\nHost: localhost\r\n\r\n", 400),
            (b"POST /ltds/v1/jobs HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n", 400),
            (b"POST /ltds/v1/jobs HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1\r\ncontent-length: 1\r\n\r\nx", 400),
            (b"GET /ltds/v1/capabilities HTTP/1.1\r\nHost: localhost\r\nAuthorization: one\r\nauthorization: two\r\n\r\n", 400),
            ((f"POST /ltds/v1/jobs HTTP/1.1\r\nHost: localhost\r\nContent-Length: {MAX_BODY_BYTES + 1}\r\n\r\n").encode(), 413),
            (b"POST /ltds/v1/jobs HTTP/1.1\r\nHost: localhost\r\n\r\n", 411),
            (b"POST /ltds/v1/jobs HTTP/1.1\r\nHost: localhost\r\nContent-Length: -1\r\n\r\n", 400)):
            with self.subTest(request=request):
                status, body, headers = self.raw(request)
                self.assertEqual(status, expected)
                self.assertIn("error", body)
                self.assertIn(b"Cache-Control: no-store", headers)
                self.assertNotIn(b"secret", json.dumps(body).encode())

    def test_truncated_body_and_read_timeout_return_safe_json(self):
        request = b"POST /ltds/v1/jobs HTTP/1.1\r\nHost: localhost\r\nContent-Length: 10\r\n\r\n{}"
        self.assertEqual(self.raw(request, eof=True)[:2], (400, {"error": "truncated_request_body"}))
        self.assertEqual(self.raw(request)[:2], (408, {"error": "request_timeout"}))

    def test_forwarding_headers_cannot_establish_connection_trust(self):
        seen = []

        class Adapter:
            def handle(self, *args, **kwargs):
                from processing.reference_api import Response
                seen.append(kwargs["protected_transport"])
                return Response(200, {"ok": True})

        server = create_reference_http_server(Adapter())
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True)
        thread.start()
        try:
            connection = http.client.HTTPConnection(*server.server_address, timeout=2)
            connection.request("GET", "/ltds/v1/capabilities", headers={"X-Forwarded-Proto": "http", "X-Forwarded-For": "192.0.2.1"})
            response = connection.getresponse()
            self.assertEqual(response.status, 200)
            response.read()
            connection.close()
            self.assertEqual(seen, [True])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
