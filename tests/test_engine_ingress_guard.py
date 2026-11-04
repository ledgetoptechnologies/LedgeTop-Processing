import io
import json
from pathlib import Path
import tempfile
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import unittest
from unittest.mock import patch
import uuid

from processing.engine_ingress_guard import AdmissionError, Binding, EngineIngressGuard, OwnerPin
from processing.node_gate import NodeGate
from processing.prototype import Queue, VIEWER_PROFILE
from processing.viewer_tasks import TaskStore


class GuardTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.paths = patch('processing.node_gate.canonical_gate_path',
            side_effect=lambda engine_id: self.root / 'gates' / engine_id)
        self.paths.start()
        self.queue = Queue(self.root / 'queue', viewer_enabled=True, node_ids=('cluster', 'main'))
        self.store = TaskStore(self.queue, self.root / 'inputs')
        self.task = str(uuid.uuid4()); self.engine = str(uuid.uuid4())
        self.store.initialize(self.task, 'synthetic guard fixture', [], None, 'production')
        self.store.upload(self.task, 'fixture.jpg', io.BytesIO(b'fixture'), 7)
        self.store.commit(self.task)
        self.task_job_id = self.store.task(self.task)['job_id']
        self.claim = self.queue.claim({'workerId': 'viewer-dispatch', 'nodeId': 'cluster',
            'profiles': [VIEWER_PROFILE], 'expectedJobId': self.task_job_id})
        self.store.update(self.task, stage='running', phase='init_pending', node_id='cluster',
                          engine_uuid=self.engine, claim_token=self.claim['claimToken'])
        self.calls = []
        self.slow = False
        self.observer = None
        fixture = self
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers.get('Content-Length', '0')))
                fixture.calls.append(self.path.split('?')[0])
                if fixture.observer: fixture.observer()
                if fixture.slow:
                    # A peer that drips bytes cannot reset the client's absolute
                    # deadline. A bounded handler eventually exits as well.
                    for byte in b'HTTP/1.1 200 OK\r\n':
                        try: self.wfile.write(bytes([byte])); self.wfile.flush()
                        except OSError: break
                        time.sleep(.025)
                    return
                body = b'{"success":true}'
                self.send_response(200); self.send_header('Content-Length', str(len(body))); self.end_headers()
                self.wfile.write(body)
            def do_GET(self):
                fixture.calls.append(self.path.split('?')[0])
                body = b'{"engine":"synthetic"}'
                self.send_response(200); self.send_header('Content-Length', str(len(body))); self.end_headers()
                self.wfile.write(body)
            def log_message(self, *_): pass
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.server.daemon_threads = False
        self.thread = threading.Thread(target=lambda: self.server.serve_forever(poll_interval=.01))
        self.thread.start()
        self.binding = Binding('synthetic-engine', 'cluster', self.queue.authority_id,
            'http://127.0.0.1:18080', 1, self.server.server_port,
            'coordinator-' + 'a' * 40, 'management-' + 'b' * 40, 'backend-' + 'c' * 40)
        with NodeGate(self.binding.engine_id) as gate:
            gate.provision('cluster', self.binding.coordinator_origin, self.queue.authority_id)
        self.owner_pin = OwnerPin.capture(self.queue)
        self.guard = EngineIngressGuard(self.queue, self.binding, owner_pin=self.owner_pin,
                                       engine_probe=lambda _: self.binding.engine_id)

    def tearDown(self):
        self.server.shutdown(); self.server.server_close(); self.thread.join(timeout=2)
        self.assertFalse(self.thread.is_alive())
        self.queue.close(); self.paths.stop(); self.temp.cleanup()

    def headers(self, **replace):
        values = {'Authorization': 'Bearer ' + self.binding.coordinator_secret,
            'X-LedgeTop-Job': self.claim['id'], 'X-LedgeTop-Claim': self.claim['claimToken'], 'Set-UUID': self.engine}
        values.update(replace)
        return list(values.items())

    def init(self):
        return self.guard.mutate('POST', '/task/new/init', self.headers(), b'{}')

    def reservation(self):
        with NodeGate(self.binding.engine_id) as gate: return gate.snapshot()['reservation']

    def test_direct_post_and_management_credential_cannot_mutate(self):
        for headers in ([], self.headers(Authorization='Bearer ' + self.binding.management_secret),
                        self.headers(Authorization='Bearer ' + self.binding.backend_secret)):
            with self.assertRaises(AdmissionError): self.guard.mutate('POST', '/task/new/init', headers, b'{}')
        self.assertFalse(self.calls); self.assertIsNone(self.reservation())

    def test_actual_running_claim_reserves_and_forwards(self):
        self.assertEqual(self.init(), (200, b'{"success":true}'))
        self.assertEqual(self.calls, ['/task/new/init'])
        self.assertEqual(self.reservation()['job']['id'], self.claim['id'])
        self.store.update(self.task, phase='upload_pending')
        self.guard.mutate('POST', '/task/new/upload/' + self.engine, self.headers(), b'small synthetic body')
        self.store.update(self.task, phase='commit_pending')
        self.guard.mutate('POST', '/task/new/commit/' + self.engine, self.headers())
        self.assertEqual(len(self.calls), 3)

    def test_distinct_node_engine_can_mutate_while_cluster_engine_is_running(self):
        identifier = str(uuid.uuid4())
        self.store.initialize(identifier, 'second synthetic node job', [], None, 'production')
        self.store.upload(identifier, 'second.jpg', io.BytesIO(b'second'), 6)
        self.store.commit(identifier)
        task_job_id = self.store.task(identifier)['job_id']
        claim = self.queue.claim({'workerId': 'viewer-dispatch', 'nodeId': 'main',
                                  'profiles': [VIEWER_PROFILE], 'expectedJobId': task_job_id})
        self.assertIsNotNone(claim)
        engine_uuid = str(uuid.uuid4())
        self.store.update(identifier, stage='running', phase='init_pending', node_id='main',
                          engine_uuid=engine_uuid, claim_token=claim['claimToken'])
        binding = Binding('synthetic-engine-main', 'main', self.queue.authority_id,
            'http://127.0.0.1:18081', 1, self.server.server_port,
            'coordinator-' + 'd' * 40, 'management-' + 'e' * 40, 'backend-' + 'f' * 40)
        with NodeGate(binding.engine_id) as gate:
            gate.provision('main', binding.coordinator_origin, self.queue.authority_id)
        guard = EngineIngressGuard(self.queue, binding, owner_pin=self.owner_pin,
            engine_probe=lambda _: binding.engine_id)
        headers = {'Authorization': 'Bearer ' + binding.coordinator_secret,
            'X-LedgeTop-Job': claim['id'], 'X-LedgeTop-Claim': claim['claimToken'],
            'Set-UUID': engine_uuid}
        self.assertEqual(guard.mutate('POST', '/task/new/init', list(headers.items()), b'{}'),
                         (200, b'{"success":true}'))
        self.assertEqual(self.queue.get(self.claim['id'])['status'], 'running')
        with NodeGate(binding.engine_id) as gate:
            self.assertEqual(gate.snapshot()['reservation']['job']['nodeId'], 'main')

    def test_database_and_gate_stay_owned_during_forward(self):
        observed = []
        def observe():
            import sqlite3
            with self.assertRaises(Exception):
                with NodeGate(self.binding.engine_id): pass
            db = sqlite3.connect(self.queue.path, timeout=.01)
            try:
                with self.assertRaises(sqlite3.OperationalError): db.execute('BEGIN IMMEDIATE')
            finally: db.close()
            observed.append(True)
        self.observer = observe
        self.init(); self.assertEqual(observed, [True])

    def test_operator_held_claim_and_forged_claim_fail_closed(self):
        with self.assertRaises(AdmissionError):
            self.guard.mutate('POST', '/task/new/init', self.headers(**{'X-LedgeTop-Claim': 'z' * 40}), b'{}')
        self.queue.operator(self.claim['id'], 'hold', {'note': 'synthetic inspection'})
        with self.assertRaises(AdmissionError): self.init()
        self.assertFalse(self.calls)

    def test_existing_running_job_can_finish_while_draining(self):
        self.init()
        node = self.queue.get_node('cluster'); now = datetime.now(timezone.utc).isoformat()
        self.queue.schedule_node('cluster', {'expectedRevision': node['revision'], 'drainAt': now,
            'notBefore': now, 'afterJobId': self.claim['id'], 'targetImage': 'sha256:' + 'd' * 64})
        self.assertEqual(self.queue.get_node('cluster')['state'], 'draining')
        self.store.update(self.task, phase='commit_pending')
        self.guard.mutate('POST', '/task/new/commit/' + self.engine, self.headers())
        self.assertIsNone(self.queue.claim({'workerId': 'another'}))

    def test_unknown_methods_paths_aliases_duplicates_and_wrong_engine_denied(self):
        cases = [('GET','/task/new/init'),('POST','/task/remove'),('POST','/task/restart'),
                 ('POST','//task/new/init'),('POST','/task/new/%69nit'),
                 ('POST','/task/new/init?token=anything'),('POST','http://elsewhere/task/new/init'),
                 ('DELETE','/task/new/init'),('POST','/task/new/commit/'+str(uuid.uuid4()))]
        for method,path in cases:
            with self.subTest(method=method,path=path), self.assertRaises(AdmissionError):
                self.guard.mutate(method,path,self.headers(),b'{}')
        with self.assertRaises(AdmissionError):
            self.guard.mutate('POST','/task/new/init', self.headers()+[('authorization','Bearer anything')], b'{}')
        self.assertFalse(self.calls)

    def test_management_reads_work_while_held_and_do_not_reserve(self):
        self.queue.operator(self.claim['id'], 'hold', {'note': 'synthetic inspection'})
        with NodeGate(self.binding.engine_id):
            self.assertEqual(self.guard.read('GET','/info',[('Authorization','Bearer '+self.binding.management_secret)])[0],200)
        self.assertIsNone(self.reservation())
        with self.assertRaises(AdmissionError): self.guard.read('POST','/task/new/init',self.headers())

    def test_physical_engine_change_and_stale_binding_denied(self):
        self.guard.engine_probe = lambda _: 'different-engine'
        with self.assertRaises(AdmissionError): self.init()
        self.assertFalse(self.calls)

    def test_large_upload_refused_and_failed_cancelled_result_keeps_guard(self):
        self.init(); self.store.update(self.task, phase='upload_pending')
        with self.assertRaises(AdmissionError):
            self.guard.mutate('POST','/task/new/upload/'+self.engine,self.headers(),b'x'*65537)
        with self.assertRaises(AdmissionError): self.guard.mutate('POST','/task/cancel',self.headers(),b'uuid='+self.engine.encode())
        self.queue.complete(self.claim['id'], {'claimToken': self.claim['claimToken'], 'status': 'failed',
            'result': {'engineTaskId':self.engine,'cancelled':True}})
        self.store.update(self.task, phase='complete')
        with self.assertRaises(AdmissionError): self.guard.completed(self.headers())
        self.assertIsNotNone(self.reservation())

    def test_successful_database_result_does_not_assert_engine_quiescence(self):
        self.init()
        self.queue.complete(self.claim['id'], {'claimToken': self.claim['claimToken'], 'status': 'succeeded',
            'result': {'engineTaskId':self.engine,'nodeId':'cluster'}})
        self.store.update(self.task, phase='complete')
        with self.assertRaises(AdmissionError): self.guard.completed(self.headers())
        self.assertIsNotNone(self.reservation())
        with self.assertRaises(AdmissionError): self.init()

    def test_absolute_network_deadline_retains_uncertain_reservation(self):
        from dataclasses import replace
        self.slow = True
        self.guard = EngineIngressGuard(self.queue, replace(self.binding,deadline_seconds=.12),
                                       owner_pin=self.owner_pin,
                                       engine_probe=lambda _:self.binding.engine_id)
        start=time.monotonic()
        with self.assertRaises(AdmissionError): self.init()
        self.assertLess(time.monotonic()-start,.8)
        self.assertIsNotNone(self.reservation())

    def test_same_authority_clone_is_not_canonical_owner(self):
        clone = Queue(self.root / 'cloned-queue', viewer_enabled=True)
        try:
            with clone._db() as db:
                db.execute('UPDATE coordinator_identity SET authority_id=?', (self.queue.authority_id,))
            clone.authority_id = self.queue.authority_id
            with self.assertRaises(AdmissionError):
                EngineIngressGuard(clone, self.binding, owner_pin=self.owner_pin,
                                   engine_probe=lambda _:self.binding.engine_id)
        finally: clone.close()
        self.assertFalse(self.calls)

    def test_changed_startup_epoch_and_closed_lease_fail_closed(self):
        self.queue.startup_id = str(uuid.uuid4())
        with self.assertRaises(AdmissionError): self.init()
        self.assertFalse(self.calls)
        self.queue.close()
        with self.assertRaises((AdmissionError, RuntimeError)): self.init()

    def test_other_durable_host_reservation_cannot_be_replaced(self):
        with NodeGate(self.binding.engine_id) as gate:
            gate.reserve('cluster', self.binding.coordinator_origin, self.queue.authority_id,
                         1, uuid.uuid4().hex)
        before=self.reservation()
        with self.assertRaises(AdmissionError):self.init()
        self.assertEqual(before,self.reservation());self.assertFalse(self.calls)

    def test_contended_sqlite_writer_obeys_absolute_budget(self):
        import sqlite3
        from dataclasses import replace
        self.guard=EngineIngressGuard(self.queue,replace(self.binding,deadline_seconds=.12),
            owner_pin=self.owner_pin,engine_probe=lambda _:self.binding.engine_id)
        db=sqlite3.connect(self.queue.path);db.execute('BEGIN IMMEDIATE')
        start=time.monotonic()
        try:
            with self.assertRaises(AdmissionError):self.init()
        finally:db.rollback();db.close()
        self.assertLess(time.monotonic()-start,.8);self.assertFalse(self.calls)

    def test_competing_lifecycle_mutex_obeys_deadline_before_host_gate(self):
        from dataclasses import replace
        self.guard=EngineIngressGuard(self.queue,replace(self.binding,deadline_seconds=.12),
            owner_pin=self.owner_pin,engine_probe=lambda _:self.binding.engine_id)
        ready=threading.Event();release=threading.Event()
        def hold():
            with self.queue._lifecycle_lock:
                ready.set();release.wait(2)
        holder=threading.Thread(target=hold);holder.start()
        try:
            self.assertTrue(ready.wait(1));start=time.monotonic()
            with self.assertRaises(AdmissionError):self.init()
            self.assertLess(time.monotonic()-start,.8)
            with NodeGate(self.binding.engine_id) as gate:
                self.assertIsNone(gate.snapshot()['reservation'])
            self.assertFalse(self.calls)
        finally:
            release.set();holder.join(2)
            self.assertFalse(holder.is_alive())


if __name__ == '__main__': unittest.main()
