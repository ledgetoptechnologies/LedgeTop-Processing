import copy
import io
import json
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
from urllib.request import ProxyHandler, build_opener
from pathlib import Path
import ssl
import tempfile
import unittest
from unittest.mock import patch

from processing import reference_protocol as p
from processing.reference_sender import (
    MAX_RECEIPT_BYTES, RESULTS_PATH, ReferenceSender, SenderError, decode_receipt,
)
from processing.worker_outbox import BINDING, WorkerOutbox
from processing.prototype import Queue
from processing.reference_store import ReferenceStore
from processing.reference_delivery import DeliveryReceiver
from processing.reference_worker_api import ReferenceWorkerAPI
from processing.reference_http import create_reference_http_server


def spawn_fixture_sender(code, data):
    process=subprocess.Popen([sys.executable,'-c',code],stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
    try:
        process.stdin.write(json.dumps(data).encode())
        process.stdin.close();process.stdin=None
        return process
    except BaseException:
        try:
            if process.poll() is None:process.kill()
            process.wait(timeout=3)
        finally:
            for stream in (process.stdin,process.stdout,process.stderr):
                if stream is not None:stream.close()
        raise


class Response(io.BytesIO):
    def __init__(self, body, *, url, status=200, headers=None):
        super().__init__(body)
        self.url, self.status = url, status
        self.headers = {"Content-Type": "application/json"} if headers is None else headers

    def geturl(self):
        return self.url


class ReferenceSenderTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.now = 100
        self.outbox = self.open_outbox()
        fixture = json.loads((Path(__file__).resolve().parents[1] /
            "contracts/reference-v1.contract.json").read_text())
        self.manifest = fixture["resultManifest"]
        self.outbox_id = self.outbox.persist_terminal(job_id=self.manifest["jobId"], generation=1,
            environment="staging", input_manifest_sha256="a"*64,
            pipeline_digest=self.manifest["pipelineDigest"],
            result_manifest_sha256=self.manifest["resultManifestSha256"], result_manifest=self.manifest,
            owned_prefix="jobs/reservation-1", local_objects={obj["id"]:
                "jobs/reservation-1/" + obj["id"] + ".bin" for obj in self.manifest["objects"]})
        self.requests, self.bindings = [], []
        self.bearer, self.claim_token = "private-configured-bearer", "private-original-queue-claim"
        self.url = "https://coordinator.example" + RESULTS_PATH

    def open_outbox(self):
        return WorkerOutbox(self.root / "worker.sqlite", clock=lambda: self.now,
                            random_source=lambda: 0, retry_base_seconds=5)

    def provider(self, binding):
        self.bindings.append(binding)
        return {"claimRequestId": "original-claim-1", "claimToken": self.claim_token}

    def receipt(self, envelope):
        payload = envelope["payload"]
        return dict(type="durable-result-receipt", durable=True, receiptId="receipt-1",
            storageReceiptId="storage-1", outboxId=envelope["outboxId"],
            payloadSha256=envelope["payloadSha256"],
            acceptedObjectIds=[obj["id"] for obj in payload["resultManifest"]["objects"]],
            **{key: payload[key] for key in BINDING})

    def sender(self, *, mutate_receipt=None, raw_body=None, **response_options):
        def opener(request, *, timeout):
            self.requests.append(request)
            envelope = json.loads(request.data)["delivery"]
            receipt = self.receipt(envelope)
            if mutate_receipt:
                mutate_receipt(receipt)
            body = p.canonical_json(receipt) if raw_body is None else raw_body
            return Response(body, url=request.full_url, **response_options)
        return ReferenceSender(self.outbox, "https://coordinator.example", bearer_token=self.bearer,
                               claim_binding_provider=self.provider, opener=opener)

    def assert_retained(self):
        row = self.outbox.get(self.outbox_id)
        self.assertIsNotNone(row["payload"])
        self.assertEqual(row["local_metadata"]["ownedPrefix"], "jobs/reservation-1")
        return row

    @unittest.skipUnless(shutil.which('openssl'), 'openssl required for isolated TLS test')
    def test_real_tls_deadline_and_ambiguous_post_replay(self):
        cert, key = self.root / 'cert.pem', self.root / 'key.pem'
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
            '-subj', '/CN=synthetic-loopback', '-addext', 'subjectAltName=IP:127.0.0.1',
            '-addext', 'basicConstraints=critical,CA:TRUE', '-keyout', str(key), '-out', str(cert)],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=15)
        server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_context.load_cert_chain(cert, key)
        client_context = ssl.create_default_context(cafile=str(cert))
        delivered = []
        for phase in ('handshake', 'header', 'body', 'normal'):
            with self.subTest(phase=phase):
                listener = socket.socket(); listener.bind(('127.0.0.1', 0)); listener.listen(1)
                listener.settimeout(3); port = listener.getsockname()[1]; errors = []
                def serve():
                    try:
                        raw, _ = listener.accept()
                        if phase == 'handshake':
                            time.sleep(.7); raw.close(); return
                        with server_context.wrap_socket(raw, server_side=True) as conn:
                            conn.settimeout(3); request = b''
                            while b'\r\n\r\n' not in request:
                                request += conn.recv(4096)
                            headers, body = request.split(b'\r\n\r\n', 1)
                            length = next(int(line.split(b':', 1)[1]) for line in headers.split(b'\r\n')
                                          if line.lower().startswith(b'content-length:'))
                            while len(body) < length: body += conn.recv(4096)
                            delivery = json.loads(body)['delivery']; delivered.append(delivery)
                            result = p.canonical_json(self.receipt(delivery))
                            if phase == 'header':
                                conn.sendall(b'HTTP/1.1 200 OK\r\nX-Slow: ')
                                for _ in range(30): conn.sendall(b'x'); time.sleep(.04)
                            else:
                                conn.sendall(b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: '
                                             + str(len(result)).encode() + b'\r\n\r\n' + result[:1])
                                if phase == 'body':
                                    for byte in result[1:]:
                                        time.sleep(.04); conn.sendall(bytes([byte]))
                                else:
                                    conn.sendall(result[1:])
                    except (OSError, ssl.SSLError): pass
                    except Exception as exc: errors.append(type(exc).__name__)
                    finally: listener.close()
                thread = threading.Thread(target=serve); thread.start()
                sender = ReferenceSender(self.outbox, 'https://127.0.0.1:' + str(port),
                    bearer_token=self.bearer, claim_binding_provider=self.provider,
                    ssl_context=client_context, timeout=1, total_timeout=.25)
                start = time.monotonic()
                try:
                    result = sender.send_next()
                    self.assertLess(time.monotonic() - start, 1.5)
                    self.assertEqual(result['state'], 'acknowledged' if phase == 'normal' else 'retry_scheduled')
                    self.assert_retained()
                    if phase != 'normal': self.now = self.outbox.get(self.outbox_id)['next_at']
                finally:
                    listener.close(); thread.join(3)
                self.assertFalse(thread.is_alive()); self.assertEqual(errors, [])
        self.assertEqual(len(delivered), 3)
        self.assertTrue(all(item == delivered[0] for item in delivered))
        self.assertEqual(self.outbox.get(self.outbox_id)['state'], 'acknowledged')

    @unittest.skipUnless(os.name=='posix' and shutil.which('openssl'), 'POSIX SIGKILL and openssl required')
    def test_actual_coordinator_commit_lost_response_restart_replays_receipt(self):
        cert, key = self.root / 'coordinator-cert.pem', self.root / 'coordinator-key.pem'
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
            '-subj', '/CN=synthetic-loopback', '-addext', 'subjectAltName=IP:127.0.0.1',
            '-addext', 'basicConstraints=critical,CA:TRUE', '-keyout', str(key), '-out', str(cert)],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=15)
        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER); tls.load_cert_chain(cert, key)
        client_tls = ssl.create_default_context(cafile=str(cert))
        fixture = json.loads((Path(__file__).resolve().parents[1] /
                              'contracts/reference-v1.contract.json').read_text())
        queue = Queue(self.root / 'coordinator', reference_enabled=True)
        store = ReferenceStore(queue)
        raw = fixture['submission']
        job = store.submit(p.validate_submission(raw, environment='staging',
                           capability_fingerprint=raw['capabilityFingerprint']))[0]
        identity = dict(node_id='cluster', worker_id='worker', worker_instance_id='instance')
        claim = store.claim(job['jobId'], **identity, claim_request_id='claim-1')
        store.reserve(job['jobId'], fixture['transferRequest'], node_id='cluster')
        store.update_phase(job['jobId'], 'fetching'); store.update_phase(job['jobId'], 'processing')
        manifest = copy.deepcopy(fixture['resultManifest'])
        manifest.update(jobId=job['jobId'], pipelineDigest=job['pipelineDigest'])
        manifest['objects'][0]['role'] = 'odm_archive'
        manifest['resultManifestSha256'] = p.digest({k: v for k, v in manifest.items()
                                                     if k != 'resultManifestSha256'})
        worker_root = self.root / 'worker-integration'; worker_root.mkdir()
        original_input = worker_root / 'input.bin'; original_input.write_bytes(b'synthetic-input')
        output_dir = worker_root / 'jobs' / 'reservation-1'; output_dir.mkdir(parents=True)
        results = {obj['id']: output_dir / (obj['id'] + '.bin') for obj in manifest['objects']}
        for output in results.values(): output.write_bytes(b'synthetic-result')
        path = worker_root / 'outbox.sqlite'
        outbox = WorkerOutbox(path, clock=lambda: self.now, random_source=lambda: 0, retry_base_seconds=5)
        outbox_id = outbox.persist_terminal(job_id=job['jobId'], generation=1, environment='staging',
            input_manifest_sha256=job['manifestSha256'], pipeline_digest=job['pipelineDigest'],
            result_manifest_sha256=manifest['resultManifestSha256'], result_manifest=manifest,
            owned_prefix='jobs/reservation-1', local_objects={obj['id']:
                'jobs/reservation-1/' + obj['id'] + '.bin' for obj in manifest['objects']})
        api = ReferenceWorkerAPI(store, DeliveryReceiver(store), bearer_workers={self.bearer: identity})
        committed = threading.Event(); release = threading.Event(); calls = []
        class StallFirstCommittedResponse:
            def handle(_, method, route, headers, body, *, protected_transport=False):
                response = api.handle(method, route, headers, body, protected_transport=protected_transport)
                if route == RESULTS_PATH:
                    calls.append((json.loads(body)['delivery'], copy.deepcopy(response.body), response.status))
                    if len(calls) == 1:
                        committed.set(); release.wait(4)
                return response
        server = create_reference_http_server(StallFirstCommittedResponse(), ssl_context=tls)
        thread = threading.Thread(target=server.serve_forever); thread.start()
        try:
            origin = 'https://127.0.0.1:' + str(server.server_port)
            child_code = r"""
import json, ssl, sys
from processing.worker_outbox import WorkerOutbox
from processing.reference_sender import ReferenceSender
c=json.load(sys.stdin)
outbox=WorkerOutbox(c['path'], clock=lambda:c['now'], random_source=lambda:0, retry_base_seconds=5)
sender=ReferenceSender(outbox,c['origin'],bearer_token=c['bearer'],
    claim_binding_provider=lambda _: {'claimRequestId':'claim-1','claimToken':c['claim']},
    ssl_context=ssl.create_default_context(cafile=c['cert']),timeout=3,total_timeout=3,lease_seconds=1)
print(json.dumps(sender.send_next()),flush=True)
"""
            def spawn_sender(now):
                data=dict(path=str(path),now=now,origin=origin,bearer=self.bearer,
                          claim=claim['claimToken'],cert=str(cert))
                return spawn_fixture_sender(child_code,data)
            worker=spawn_sender(self.now)
            try:
                self.assertTrue(committed.wait(4),'coordinator did not commit in time')
                self.assertIsNone(worker.poll(),'sender exited before forced loss')
                worker.kill();worker.wait(timeout=3)
                self.assertNotEqual(worker.returncode,0)
                self.assertEqual(queue.get(job['jobId'])['status'], 'succeeded')
                self.assertEqual(store.get(job['jobId'])['phase'], 'awaiting_acceptance')
                self.assertEqual(calls[0][2], 200)
                self.assertEqual(outbox.get(outbox_id)['state'], 'sending')
                self.now+=2;release.set()
                restarted_worker=spawn_sender(self.now)
                try:
                    stdout,stderr=restarted_worker.communicate(timeout=6)
                    self.assertEqual(restarted_worker.returncode,0,stderr.decode())
                    self.assertEqual(json.loads(stdout)['state'],'acknowledged')
                finally:
                    if restarted_worker.poll() is None:
                        restarted_worker.kill();restarted_worker.wait(timeout=3)
            finally:
                if worker.poll() is None:worker.kill();worker.wait(timeout=3)
                worker.stdout.close();worker.stderr.close()
            restarted = WorkerOutbox(path, clock=lambda: self.now, random_source=lambda: 0, retry_base_seconds=5)
            self.assertEqual(len(calls), 2)
            self.assertEqual(calls[0], calls[1])
            self.assertEqual(restarted.get(outbox_id)['receipt'], calls[0][1])
            self.assertEqual(store.get(job['jobId'])['phase'], 'awaiting_acceptance')
            self.assertEqual(original_input.read_bytes(), b'synthetic-input')
            for output in results.values(): self.assertEqual(output.read_bytes(), b'synthetic-result')
            self.assertIsNotNone(restarted.get(outbox_id)['payload'])
        finally:
            release.set(); server.shutdown(); server.server_close(); thread.join(5); queue.close()
        self.assertFalse(thread.is_alive())

    @unittest.skipUnless(os.name=='posix' and shutil.which('openssl'), 'POSIX SIGKILL and openssl required')
    def test_coordinator_sigkill_after_receipt_commit_replays_once(self):
        cert, key = self.root / 'coordinator-cert.pem', self.root / 'coordinator-key.pem'
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
            '-subj', '/CN=synthetic-loopback', '-addext', 'subjectAltName=IP:127.0.0.1',
            '-addext', 'basicConstraints=critical,CA:TRUE', '-keyout', str(key), '-out', str(cert)],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=15)
        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER); tls.load_cert_chain(cert, key)
        client_tls = ssl.create_default_context(cafile=str(cert))
        fixture = json.loads((Path(__file__).resolve().parents[1] /
                              'contracts/reference-v1.contract.json').read_text())
        queue = Queue(self.root / 'coordinator', reference_enabled=True)
        store = ReferenceStore(queue)
        raw = fixture['submission']
        job = store.submit(p.validate_submission(raw, environment='staging',
                           capability_fingerprint=raw['capabilityFingerprint']))[0]
        identity = dict(node_id='cluster', worker_id='worker', worker_instance_id='instance')
        claim = store.claim(job['jobId'], **identity, claim_request_id='claim-1')
        store.reserve(job['jobId'], fixture['transferRequest'], node_id='cluster')
        store.update_phase(job['jobId'], 'fetching'); store.update_phase(job['jobId'], 'processing')
        manifest = copy.deepcopy(fixture['resultManifest'])
        manifest.update(jobId=job['jobId'], pipelineDigest=job['pipelineDigest'])
        manifest['objects'][0]['role'] = 'odm_archive'
        manifest['resultManifestSha256'] = p.digest({k: v for k, v in manifest.items()
                                                     if k != 'resultManifestSha256'})
        worker_root = self.root / 'worker-integration'; worker_root.mkdir()
        original_input = worker_root / 'input.bin'; original_input.write_bytes(b'synthetic-input')
        output_dir = worker_root / 'jobs' / 'reservation-1'; output_dir.mkdir(parents=True)
        results = {obj['id']: output_dir / (obj['id'] + '.bin') for obj in manifest['objects']}
        for output in results.values(): output.write_bytes(b'synthetic-result')
        path = worker_root / 'outbox.sqlite'
        outbox = WorkerOutbox(path, clock=lambda: self.now, random_source=lambda: 0, retry_base_seconds=5)
        outbox_id = outbox.persist_terminal(job_id=job['jobId'], generation=1, environment='staging',
            input_manifest_sha256=job['manifestSha256'], pipeline_digest=job['pipelineDigest'],
            result_manifest_sha256=manifest['resultManifestSha256'], result_manifest=manifest,
            owned_prefix='jobs/reservation-1', local_objects={obj['id']:
                'jobs/reservation-1/' + obj['id'] + '.bin' for obj in manifest['objects']})
        coordinator_root=self.root/'coordinator'
        queue.close()
        marker=self.root/'committed.json'
        code=r"""
import json,os,ssl,sys,time
from processing.prototype import Queue
from processing.reference_store import ReferenceStore
from processing.reference_delivery import DeliveryReceiver
from processing.reference_worker_api import ReferenceWorkerAPI
from processing.reference_http import create_reference_http_server
from processing.reference_sender import RESULTS_PATH
c=json.load(sys.stdin)
queue=Queue(c['root'],reference_enabled=True);store=ReferenceStore(queue)
api=ReferenceWorkerAPI(store,DeliveryReceiver(store),bearer_workers={c['bearer']:{'node_id':'cluster','worker_id':'worker','worker_instance_id':'instance'}})
class API:
 def handle(self,method,route,headers,body,*,protected_transport=False):
  result=api.handle(method,route,headers,body,protected_transport=protected_transport)
  if route==RESULTS_PATH and c['stall']:
   with open(c['marker'],'x') as f:
    json.dump({'receipt':result.body,'delivery':json.loads(body)['delivery'],'status':result.status},f)
    f.flush();os.fsync(f.fileno())
   time.sleep(20)
  return result
tls=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);tls.load_cert_chain(c['cert'],c['key'])
server=create_reference_http_server(API(),ssl_context=tls)
print(server.server_port,flush=True);server.serve_forever()
"""
        def start_server(stall):
            proc=spawn_fixture_sender(code,dict(root=str(coordinator_root),bearer=self.bearer,
                cert=str(cert),key=str(key),marker=str(marker),stall=stall))
            try:
                import select
                if not select.select([proc.stdout],[],[],5)[0]:raise RuntimeError('server start timeout')
                port=int(proc.stdout.readline())
                return proc,'https://127.0.0.1:'+str(port)
            except BaseException:
                if proc.poll() is None:proc.kill()
                proc.wait(timeout=3);proc.stdout.close();proc.stderr.close();raise
        def stop_server(proc):
            if proc.poll() is None:proc.kill()
            proc.wait(timeout=3);proc.stdout.close();proc.stderr.close()
        server,origin=start_server(True)
        outcome=[]
        provider=lambda _: {'claimRequestId':'claim-1','claimToken':claim['claimToken']}
        sender=ReferenceSender(outbox,origin,bearer_token=self.bearer,claim_binding_provider=provider,
                               ssl_context=client_tls,timeout=5,total_timeout=5,lease_seconds=10)
        thread=threading.Thread(target=lambda:outcome.append(sender.send_next()))
        try:
            thread.start()
            deadline=time.monotonic()+5
            while not marker.exists() and time.monotonic()<deadline:time.sleep(.01)
            self.assertTrue(marker.exists(),'coordinator did not commit in time')
            evidence=json.loads(marker.read_text())
            self.assertEqual(evidence['status'],200)
            stop_server(server);server=None
            thread.join(5);self.assertFalse(thread.is_alive())
            self.assertEqual(outcome[0]['state'],'retry_scheduled')
            self.now=outbox.get(outbox_id)['next_at']
            server,origin=start_server(False)
            restarted=WorkerOutbox(path,clock=lambda:self.now,random_source=lambda:0,retry_base_seconds=5)
            sender=ReferenceSender(restarted,origin,bearer_token=self.bearer,claim_binding_provider=provider,
                                   ssl_context=client_tls,timeout=2,total_timeout=2)
            self.assertEqual(sender.send_next()['state'],'acknowledged')
            self.assertEqual(restarted.get(outbox_id)['receipt'],evidence['receipt'])
            self.assertEqual(evidence['delivery']['outboxId'],outbox_id)
            self.assertEqual(evidence['delivery']['payload'],restarted.get(outbox_id)['payload'])
            self.assertEqual(original_input.read_bytes(),b'synthetic-input')
            for output in results.values():self.assertEqual(output.read_bytes(),b'synthetic-result')
        finally:
            if server is not None:stop_server(server)
            thread.join(5)
        # Inspect coordinator state only after the restarted process has stopped.
        import sqlite3
        db=sqlite3.connect(str(coordinator_root/'jobs.sqlite3'))
        try:self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_worker_receipts').fetchone()[0],1)
        finally:db.close()

    def test_sender_spawn_write_failure_reaps_owned_child(self):
        class BadInput:
            closed=False
            def write(self,_):raise BrokenPipeError()
            def close(self):self.closed=True
        class FakeChild:
            def __init__(self):
                self.stdin=BadInput();self.stdout=io.BytesIO();self.stderr=io.BytesIO()
                self.killed=False;self.waited=False
            def poll(self):return None
            def kill(self):self.killed=True
            def wait(self,timeout):self.waited=(timeout==3)
        child=FakeChild()
        with patch('tests.test_reference_sender.subprocess.Popen',return_value=child):
            with self.assertRaises(BrokenPipeError):spawn_fixture_sender('',{})
        self.assertTrue(child.killed);self.assertTrue(child.waited)
        self.assertTrue(child.stdin.closed and child.stdout.closed and child.stderr.closed)

    def test_default_internal_sender_ignores_proxy_environment(self):
        with patch.dict(os.environ, {"HTTPS_PROXY": "http://127.0.0.1:9",
                                     "https_proxy": "http://127.0.0.1:9",
                                     "NO_PROXY": "", "no_proxy": ""}):
            inherited = build_opener()
            sender = ReferenceSender(self.outbox, "https://coordinator.example",
                bearer_token=self.bearer, claim_binding_provider=self.provider)
        inherited_handlers = [handler for handler in inherited.handlers if isinstance(handler, ProxyHandler)]
        self.assertTrue(inherited_handlers)
        self.assertEqual(inherited_handlers[0].proxies.get("https"), "http://127.0.0.1:9")
        self.assertFalse(any(isinstance(handler, ProxyHandler) for handler in sender._open.__self__.handlers))

    def test_exact_contract_and_verified_receipt_acknowledge_without_deleting_metadata(self):
        sender = self.sender()
        result = sender.send_next()
        self.assertEqual(result, {"state": "acknowledged", "outboxId": self.outbox_id})
        row = self.assert_retained()
        self.assertEqual(row["state"], "acknowledged")
        request = self.requests[0]
        self.assertEqual(request.full_url, self.url)
        self.assertEqual(request.method, "POST")
        self.assertEqual(request.get_header("Authorization"), "Bearer " + self.bearer)
        body = json.loads(request.data)
        self.assertEqual(set(body), {"claimRequestId", "claimToken", "delivery"})
        self.assertEqual(body["claimToken"], self.claim_token)
        self.assertEqual(set(body["delivery"]), {"outboxId", "payloadSha256", "payload"})
        self.assertNotIn("jobs/reservation", request.data.decode())
        self.assertEqual(set(self.bindings[0]), set(BINDING))
        self.assertNotIn(self.bearer, repr(sender))
        self.assertNotIn(self.claim_token, repr(sender))
        self.assertEqual(sender.send_next(), {"state": "idle"})
        self.assertEqual(len(self.requests), 1)

    def test_bad_receipts_back_off_and_remain_replayable(self):
        mutations = (lambda r: r.update(durable=False), lambda r: r.pop("receiptId"),
                     lambda r: r.update(environment="production"),
                     lambda r: r.update(acceptedObjectIds=["result-1"]),
                     lambda r: r.update(extra="unknown"), lambda r: r.update(payloadSha256="0"*64))
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                result = self.sender(mutate_receipt=mutation).send_next()
                self.assertEqual(result["state"], "retry_scheduled")
                row = self.assert_retained()
                self.assertEqual(row["state"], "pending")
                self.assertIsNone(row["receipt"])
                self.assertGreater(row["next_at"], self.now)
                self.now = row["next_at"]
        self.assertEqual(self.sender().send_next()["state"], "acknowledged")

    def test_duplicate_nonfinite_malformed_and_wrapped_json_never_acknowledge(self):
        for raw in (b'{"durable":true,"durable":false}', b'{"x":NaN}',
                    b'{"x":Infinity}', b'{"x":1e999}', b'{"x":{"a":1,"a":2}}',
                    b'[]', b'{} trailing', b'\xff', b'{"receipt":{}}'):
            with self.subTest(raw=raw):
                result = self.sender(raw_body=raw).send_next()
                self.assertEqual(result["state"], "retry_scheduled")
                self.now = self.assert_retained()["next_at"]
        self.assertIsNone(self.outbox.get(self.outbox_id)["receipt"])

    def test_non200_encoded_or_untyped_receipt_does_not_acknowledge(self):
        options = ({"status": 202}, {"status": 302}, {"status": 500}, {"headers": {}},
                   {"headers": {"Content-Type": "text/plain"}},
                   {"headers": {"Content-Type": "application/json", "Content-Encoding": "gzip"}},
                   {"headers": {"Content-Type": "application/json", "Content-Length": "9999"}})
        for option in options:
            with self.subTest(option=option):
                result = self.sender(**option).send_next()
                self.assertEqual(result["state"], "retry_scheduled")
                self.now = self.assert_retained()["next_at"]

    def test_response_byte_limit_is_enforced_even_without_content_length(self):
        result = self.sender(raw_body=b" " * (MAX_RECEIPT_BYTES + 1)).send_next()
        self.assertEqual(result["state"], "retry_scheduled")
        self.assertIsNone(self.assert_retained()["receipt"])
        with self.assertRaises(SenderError):
            decode_receipt(b" " * (MAX_RECEIPT_BYTES + 1))

    def test_transport_errors_cannot_expose_credentials_in_return_value(self):
        def broken(request, **kwargs):
            raise RuntimeError(self.bearer + self.claim_token)
        sender = ReferenceSender(self.outbox, "https://coordinator.example", bearer_token=self.bearer,
            claim_binding_provider=self.provider, opener=broken)
        result = sender.send_next()
        self.assertEqual(result["state"], "retry_scheduled")
        for secret in (self.bearer, self.claim_token):
            self.assertNotIn(secret, repr(result))
            self.assertNotIn(secret, repr(sender))
        self.assert_retained()

    def test_lost_response_and_worker_restart_resend_exact_delivery(self):
        deliveries = []
        def lost(request, **kwargs):
            deliveries.append(json.loads(request.data)["delivery"])
            raise OSError("response lost")
        sender = ReferenceSender(self.outbox, "https://coordinator.example", bearer_token=self.bearer,
            claim_binding_provider=self.provider, opener=lost)
        sender.send_next()
        self.now = self.outbox.get(self.outbox_id)["next_at"]
        self.outbox = self.open_outbox()
        self.assertEqual(self.sender().send_next()["state"], "acknowledged")
        self.assertEqual(deliveries[0], json.loads(self.requests[0].data)["delivery"])
        self.assertEqual(self.outbox.get(self.outbox_id)["attempts"], 2)

    def test_verified_receipt_is_not_lost_if_acknowledgement_outlives_deadline(self):
        acknowledge = self.outbox.acknowledge
        def delayed_ack(*args):
            time.sleep(.08)
            return acknowledge(*args)
        sender = self.sender()
        sender.total_timeout = .04
        with patch.object(self.outbox, 'acknowledge', side_effect=delayed_ack):
            self.assertEqual(sender.send_next()['state'], 'acknowledged')
        self.assertEqual(self.assert_retained()['state'], 'acknowledged')

    def test_invalid_total_deadline_is_rejected(self):
        for value in (0, -1, 301, float('nan'), True):
            with self.subTest(value=value), self.assertRaises(ValueError):
                ReferenceSender(self.outbox, 'https://coordinator.example', bearer_token=self.bearer,
                    claim_binding_provider=self.provider, total_timeout=value)

    def test_expired_attempt_is_retained_until_reclaim_and_late_receipt_can_ack(self):
        def slow_failure(request, **kwargs):
            self.now += 61
            raise OSError("timeout")
        sender = ReferenceSender(self.outbox, "https://coordinator.example", bearer_token=self.bearer,
            claim_binding_provider=self.provider, opener=slow_failure)
        self.assertEqual(sender.send_next()["state"], "retry_wait")
        self.assertEqual(self.assert_retained()["state"], "sending")
        self.assertEqual(self.sender().send_next()["state"], "acknowledged")

    def test_claim_provider_cannot_add_urls_or_invalid_credentials(self):
        for claim in ({"claimRequestId": "original", "claimToken": "secret", "url": "https://other.example"},
                      {"claimRequestId": "original", "claimToken": "bad\nsecret"}):
            def provider(binding):
                return claim
            sender = ReferenceSender(self.outbox, "https://coordinator.example", bearer_token=self.bearer,
                claim_binding_provider=provider, opener=lambda *a, **k: self.fail("must not open"))
            self.assertEqual(sender.send_next()["state"], "retry_scheduled")
            self.now = self.assert_retained()["next_at"]

    def test_default_opener_refuses_redirect_without_forwarding_authorization(self):
        sender = ReferenceSender(self.outbox, "https://coordinator.example", bearer_token=self.bearer,
            claim_binding_provider=self.provider)
        handler = next(h for h in sender._open.__self__.handlers if hasattr(h, "redirect_request"))
        from urllib.request import Request
        for code in (301, 302, 303, 307, 308):
            with self.subTest(code=code), self.assertRaises(SenderError):
                handler.redirect_request(Request(self.url), None, code, "redirect", {}, "https://other.example")

    def test_changed_final_url_is_not_trusted(self):
        def opener(request, **kwargs):
            receipt = self.receipt(json.loads(request.data)["delivery"])
            return Response(p.canonical_json(receipt), url="https://other.example" + RESULTS_PATH)
        sender = ReferenceSender(self.outbox, "https://coordinator.example", bearer_token=self.bearer,
            claim_binding_provider=self.provider, opener=opener)
        self.assertEqual(sender.send_next()["state"], "retry_scheduled")
        self.assertIsNone(self.assert_retained()["receipt"])

    def test_configuration_rejects_arbitrary_paths_credentials_and_insecure_tls(self):
        for origin in ("http://coordinator.example", "https://user:pass@coordinator.example",
                       "https://coordinator.example/custom", "https://coordinator.example?url=other",
                       "https://coordinator.example#fragment", "https://coordinator.example:invalid"):
            with self.subTest(origin=origin), self.assertRaises(ValueError):
                ReferenceSender(self.outbox, origin, bearer_token=self.bearer,
                                claim_binding_provider=self.provider)
        for token in ("", "x"*4097, "secret\nheader", "non ASCII β"):
            with self.subTest(token_length=len(token)), self.assertRaises(ValueError):
                ReferenceSender(self.outbox, "https://coordinator.example", bearer_token=token,
                                claim_binding_provider=self.provider)
        with self.assertRaises(ValueError):
            ReferenceSender(self.outbox, "https://coordinator.example", bearer_token=self.bearer,
                claim_binding_provider=self.provider, ssl_context=ssl._create_unverified_context())


if __name__ == "__main__":
    unittest.main()
