import io
import hashlib
import json
import socket
import subprocess
import sys
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
        self.paths = patch('processing.node_gate.canonical_gate_path', return_value=self.root / 'gate')
        self.paths.start()
        self.queue = Queue(self.root / 'queue', viewer_enabled=True)
        self.store = TaskStore(self.queue, self.root / 'inputs')
        self.task = str(uuid.uuid4()); self.engine = str(uuid.uuid4())
        self.store.initialize(self.task, 'synthetic guard fixture', [], None, 'production')
        self.store.upload(self.task, 'fixture.jpg', io.BytesIO(b'fixture'), 7)
        self.store.commit(self.task)
        self.claim = self.queue.claim({'workerId': 'viewer-dispatch', 'nodeId': 'cluster', 'profiles': [VIEWER_PROFILE]})
        self.store.update(self.task, stage='running', phase='init_pending', node_id='cluster',
                          engine_uuid=self.engine, claim_token=self.claim['claimToken'])
        self.calls = []
        self.slow = False
        self.response_body = b'{"success":true}'
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
                body = fixture.response_body
                try:
                    self.send_response(200); self.send_header('Content-Length', str(len(body))); self.end_headers()
                    self.wfile.write(body)
                except (BrokenPipeError,ConnectionResetError): pass  # Expected on synthetic disconnects.
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

    def test_stream_holds_gate_and_claim_through_bounded_forward(self):
        import sqlite3
        self.init();self.store.update(self.task, phase='upload_pending')
        source,writer=socket.socketpair();payload=b'x'*131072;writer.settimeout(2)
        first=threading.Event();release=threading.Event();errors=[];result=[]
        def feed():
            try:
                writer.sendall(payload[:1024]);first.set()
                if release.wait(2):writer.sendall(payload[1024:]);writer.shutdown(socket.SHUT_WR)
            except OSError as error:errors.append(error)
        headers=self.headers(**{'X-LedgeTop-Upload-ID':str(uuid.uuid4())})
        def transfer():
            try:result.append(self.guard.stream_upload('/task/new/upload/'+self.engine,headers,source,len(payload),hashlib.sha256(payload).hexdigest(),'multipart/form-data; boundary=synthetic'))
            except Exception as error:errors.append(error)
        feeding=threading.Thread(target=feed);streaming=threading.Thread(target=transfer)
        feeding.start();streaming.start()
        try:
            self.assertTrue(first.wait(1));deadline=time.monotonic()+2;started=False
            while time.monotonic()<deadline:
                db=sqlite3.connect(self.queue.path,timeout=.1)
                try:
                    exists=db.execute("SELECT 1 FROM sqlite_master WHERE name='engine_ingress_uploads'").fetchone()
                    started=bool(exists and db.execute("SELECT 1 FROM engine_ingress_uploads WHERE state='started'").fetchone())
                finally:db.close()
                if started:break
                time.sleep(.01)
            self.assertTrue(started)
            with self.assertRaises(Exception):
                with NodeGate(self.binding.engine_id):pass
            self.assertFalse(self.queue._lifecycle_lock.acquire(blocking=False))
            script=('import sqlite3,sys\n'
                    'db=sqlite3.connect(sys.argv[1],timeout=.1)\n'
                    'try:\n db.execute("BEGIN IMMEDIATE");print("unexpected-write")\n'
                    'except sqlite3.OperationalError:\n print("blocked")\n'
                    'finally:\n db.close()\n')
            other=subprocess.run([sys.executable,'-c',script,str(self.queue.path)],capture_output=True,text=True,timeout=2)
            self.assertEqual((other.returncode,other.stdout.strip()),(0,'blocked'),other.stderr)
            release.set();feeding.join(3);streaming.join(3)
            self.assertFalse(feeding.is_alive());self.assertFalse(streaming.is_alive())
            self.assertEqual(errors,[]);self.assertEqual(result,[(200,b'{"success":true}')])
            self.assertEqual(source.gettimeout(),None)
            with self.queue._db() as db:self.assertEqual(db.execute('SELECT state FROM engine_ingress_uploads').fetchone()[0],'complete')
            with self.assertRaises(AdmissionError):self.guard.stream_upload('/task/new/upload/'+self.engine,headers,source,len(payload),hashlib.sha256(payload).hexdigest(),'multipart/form-data; boundary=synthetic')
        finally:
            release.set();source.close();writer.close();feeding.join(3);streaming.join(3)
            self.assertFalse(feeding.is_alive());self.assertFalse(streaming.is_alive())

    def test_partial_stream_and_bad_ack_hold_all_subsequent_mutations(self):
        self.init();self.store.update(self.task,phase='upload_pending')
        source,writer=socket.socketpair();headers=self.headers(**{'X-LedgeTop-Upload-ID':str(uuid.uuid4())})
        try:
            writer.sendall(b'ab');writer.shutdown(socket.SHUT_WR)
            with self.assertRaises(AdmissionError):self.guard.stream_upload('/task/new/upload/'+self.engine,headers,source,3,hashlib.sha256(b'abc').hexdigest(),'multipart/form-data; boundary=synthetic')
            with self.queue._db() as db:self.assertEqual(db.execute('SELECT state FROM engine_ingress_uploads').fetchone()[0],'started')
            self.assertIsNotNone(self.reservation())
            with self.assertRaises(AdmissionError):self.guard.mutate('POST','/task/new/upload/'+self.engine,self.headers(),b'x')
            with self.assertRaises(AdmissionError):self.guard.stream_upload('/task/new/upload/'+self.engine,self.headers(**{'X-LedgeTop-Upload-ID':str(uuid.uuid4())}),source,1,hashlib.sha256(b'x').hexdigest(),'multipart/form-data; boundary=synthetic')
        finally:source.close();writer.close()

    def test_ambiguous_json_ack_is_held(self):
        self.init();self.store.update(self.task,phase='upload_pending');self.response_body=b'{"success":false,"success":true}'
        source,writer=socket.socketpair();headers=self.headers(**{'X-LedgeTop-Upload-ID':str(uuid.uuid4())})
        try:
            writer.sendall(b'abc');writer.shutdown(socket.SHUT_WR)
            with self.assertRaises(AdmissionError):self.guard.stream_upload('/task/new/upload/'+self.engine,headers,source,3,hashlib.sha256(b'abc').hexdigest(),'multipart/form-data; boundary=synthetic')
            with self.queue._db() as db:self.assertEqual(db.execute('SELECT state FROM engine_ingress_uploads').fetchone()[0],'started')
        finally:source.close();writer.close()

    def test_stream_overlimit_refuses_before_source_and_journal(self):
        from processing.engine_ingress_guard import MAX_STREAM_BYTES
        self.init();self.store.update(self.task,phase='upload_pending');source,writer=socket.socketpair()
        try:
            with self.assertRaises(AdmissionError):self.guard.stream_upload('/task/new/upload/'+self.engine,self.headers(**{'X-LedgeTop-Upload-ID':str(uuid.uuid4())}),source,MAX_STREAM_BYTES+1,'a'*64,'multipart/form-data; boundary=synthetic')
            with self.queue._db() as db:self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='engine_ingress_uploads'").fetchone())
        finally:source.close();writer.close()

    def test_claim_change_between_intent_and_reacquire_holds_without_reading_body(self):
        import sqlite3
        from processing.engine_ingress_guard import _resume_upload_transaction
        self.init();self.store.update(self.task,phase='upload_pending');source,writer=socket.socketpair()
        headers=self.headers(**{'X-LedgeTop-Upload-ID':str(uuid.uuid4())})
        def interposed(db,deadline):
            external=sqlite3.connect(self.queue.path,timeout=1)
            try:
                external.execute('BEGIN IMMEDIATE')
                external.execute("UPDATE viewer_tasks SET phase='cancelled' WHERE uuid=?",(self.task,))
                external.commit()
            finally:external.close()
            return _resume_upload_transaction(db,deadline)
        try:
            writer.sendall(b'abc')
            with patch('processing.engine_ingress_guard._resume_upload_transaction',side_effect=interposed):
                with self.assertRaises(AdmissionError):
                    self.guard.stream_upload('/task/new/upload/'+self.engine,headers,source,3,hashlib.sha256(b'abc').hexdigest(),'multipart/form-data; boundary=synthetic')
            self.assertEqual(source.recv(3,socket.MSG_PEEK),b'abc')
            self.assertEqual(self.calls,['/task/new/init'])
            with self.queue._db() as db:
                self.assertEqual(db.execute('SELECT state FROM engine_ingress_uploads').fetchone()[0],'started')
                self.assertEqual(db.execute('SELECT phase FROM viewer_tasks WHERE uuid=?',(self.task,)).fetchone()[0],'cancelled')
        finally:source.close();writer.close()

    def test_journal_change_before_reacquire_holds_without_forwarding(self):
        import sqlite3
        from processing.engine_ingress_guard import _resume_upload_transaction
        self.init();self.store.update(self.task,phase='upload_pending');source,writer=socket.socketpair()
        headers=self.headers(**{'X-LedgeTop-Upload-ID':str(uuid.uuid4())})
        def interposed(db,deadline):
            external=sqlite3.connect(self.queue.path,timeout=1)
            try:
                external.execute('BEGIN IMMEDIATE')
                external.execute("UPDATE engine_ingress_uploads SET sha256=?",('b'*64,))
                external.commit()
            finally:external.close()
            return _resume_upload_transaction(db,deadline)
        try:
            writer.sendall(b'abc')
            with patch('processing.engine_ingress_guard._resume_upload_transaction',side_effect=interposed):
                with self.assertRaises(AdmissionError):
                    self.guard.stream_upload('/task/new/upload/'+self.engine,headers,source,3,hashlib.sha256(b'abc').hexdigest(),'multipart/form-data; boundary=synthetic')
            self.assertEqual(source.recv(3,socket.MSG_PEEK),b'abc');self.assertEqual(self.calls,['/task/new/init'])
            with self.queue._db() as db:self.assertEqual(db.execute('SELECT state FROM engine_ingress_uploads').fetchone()[0],'started')
        finally:source.close();writer.close()

    def test_slow_trickle_hits_absolute_deadline_and_holds(self):
        from dataclasses import replace
        self.init();self.store.update(self.task,phase='upload_pending')
        self.guard=EngineIngressGuard(self.queue,replace(self.binding,deadline_seconds=.15),owner_pin=self.owner_pin,engine_probe=lambda _:self.binding.engine_id)
        source,writer=socket.socketpair();stop=threading.Event()
        def feed():
            while not stop.wait(.035):
                try:writer.sendall(b'x')
                except OSError:return
        feeding=threading.Thread(target=feed);feeding.start();start=time.monotonic()
        try:
            with self.assertRaises(AdmissionError):self.guard.stream_upload('/task/new/upload/'+self.engine,self.headers(**{'X-LedgeTop-Upload-ID':str(uuid.uuid4())}),source,100,hashlib.sha256(b'x'*100).hexdigest(),'multipart/form-data; boundary=synthetic')
            self.assertLess(time.monotonic()-start,1)
            with self.queue._db() as db:self.assertEqual(db.execute('SELECT state FROM engine_ingress_uploads').fetchone()[0],'started')
        finally:stop.set();source.close();writer.close();feeding.join(2);self.assertFalse(feeding.is_alive())


if __name__ == '__main__': unittest.main()
