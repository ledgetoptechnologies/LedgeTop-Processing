"""Owned loopback NodeODM peer and persistent node gate; retained test roots."""
import hashlib
import io
import json
import socket
from pathlib import Path
import tempfile
import threading
import time
import unittest
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

from processing import node_mutation_broker as b
from processing.engine_ingress_guard import AdmissionError, BackendPin
from processing.node_gate import NodeGate
from processing.prototype import Queue, VIEWER_PROFILE
from processing.viewer_tasks import TaskStore


class NodeMutationBrokerTests(unittest.TestCase):
    def setUp(self):
        self.root=Path(tempfile.mkdtemp(prefix='lt-node-broker-test-')).resolve()
        self.addCleanup(lambda:print('retained synthetic node broker root:',self.root,flush=True))
        self.engine='engine-synthetic';self.node='main';self.authority=str(uuid.uuid4());self.epoch=str(uuid.uuid4())
        self.origin='http://127.0.0.1:18080';self.secret=b'synthetic-key-'*4;self.calls=[]
        self.drip=False;self.drip_done=threading.Event()
        path=self.root/'gate';self.patches=[patch('processing.node_gate.canonical_gate_path',return_value=path),
            patch('processing.node_mutation_broker.canonical_gate_path',return_value=path),patch.object(b,'QUALIFIED',True),
            patch.object(b,'local_engine_identity',return_value=self.engine),patch.object(b,'local_backend_identity',return_value=None)]
        for item in self.patches:
            item.start();self.addCleanup(item.stop)
        with NodeGate(self.engine) as gate:gate.provision(self.node,self.origin,self.authority)
        state=path/'mutation-broker';state.mkdir(mode=0o700)
        owner=self
        class OwnedHTTPServer(ThreadingHTTPServer):
            def __init__(self,*args):
                self.handler_threads=[]
                super().__init__(*args)
            def process_request(self,request,address):
                worker=threading.Thread(target=self.process_request_thread,args=(request,address))
                self.handler_threads.append(worker)
                try:worker.start()
                except BaseException:
                    self.handler_threads.remove(worker);self.shutdown_request(request);raise
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*_):pass
            def setup(self):
                super().setup();self.connection.settimeout(2)
            def do_POST(self):
                owner.calls.append((self.path,self.rfile.read(int(self.headers['Content-Length']))))
                value=json.dumps({'uuid':self.headers['set-uuid'],'success':True}).encode()
                self.send_response(200);self.send_header('Content-Length',str(len(value)));self.end_headers()
                if owner.drip:
                    try:
                        for byte in value:self.wfile.write(bytes([byte]));self.wfile.flush();time.sleep(.04)
                    except (BrokenPipeError,ConnectionResetError):pass
                    finally:owner.drip_done.set()
                else:
                    try:self.wfile.write(value)
                    except (BrokenPipeError,ConnectionResetError):pass
        self.server=OwnedHTTPServer(('127.0.0.1',0),Handler)
        self.addCleanup(self.server.server_close)
        self.thread=threading.Thread(target=self.server.serve_forever)
        def stop_server():
            if self.thread.ident is None:return
            self.server.shutdown();self.thread.join(5)
            self.assertFalse(self.thread.is_alive())
            for handler in self.server.handler_threads:
                handler.join(5)
                self.assertFalse(handler.is_alive())
        self.addCleanup(stop_server)
        self.thread.start()
        self.adapter=b.LoopbackNodeODM(self.engine,BackendPin('a'*64,'sha256:'+'b'*64,self.server.server_port),'')
        self.config=dict(node_id=self.node,engine_id=self.engine,authority_id=self.authority,coordinator_epoch=self.epoch,
                         coordinator_origin=self.origin,gate_generation=1,secret=self.secret,adapter=self.adapter)
        self.broker=b.NodeMutationBroker(state,**self.config)
        self.addCleanup(self.broker.close)
        self.task=str(uuid.uuid4());self.job=str(uuid.uuid4());self.body=b'synthetic-init'

    def grant(self,operation='init',body=None):
        body=self.body if body is None else body
        return dict(authorityId=self.authority,nodeId=self.node,engineId=self.engine,coordinatorEpoch=self.epoch,
                    jobId=self.job,engineTaskId=self.task,environment='production',gateGeneration=1,taskGeneration=1,
                    operationId=str(uuid.uuid4()),operation=operation,bodySha256=hashlib.sha256(body).hexdigest(),
                    contentType='application/octet-stream')

    def test_signed_local_init_replays_and_binds_job_task_epoch(self):
        grant=self.grant();signature=b.sign_grant(grant,self.secret)
        with self.assertRaises(b.BrokerHeld):self.broker.mutate(grant,'0'*64,self.body)
        reply=self.broker.mutate(grant,signature,self.body)
        self.assertEqual(reply['uuid'],self.task);self.assertEqual(len(self.calls),1)
        self.assertEqual(self.calls[0][0],'/task/new/init')
        self.assertEqual(self.broker.mutate(grant,signature,self.body),reply);self.assertEqual(len(self.calls),1)
        changed=dict(grant,operationId=str(uuid.uuid4()),engineTaskId=str(uuid.uuid4()))
        with self.assertRaises(b.BrokerHeld):self.broker.mutate(changed,b.sign_grant(changed,self.secret),self.body)
        changed=dict(grant,operationId=str(uuid.uuid4()),coordinatorEpoch=str(uuid.uuid4()))
        with self.assertRaises(b.BrokerHeld):self.broker.mutate(changed,b.sign_grant(changed,self.secret),self.body)
        duplicate=self.grant()
        with self.assertRaises(b.BrokerHeld):self.broker.mutate(duplicate,b.sign_grant(duplicate,self.secret),self.body)
        commit=self.grant(operation='commit')
        with self.assertRaises(b.BrokerHeld):self.broker.mutate(commit,b.sign_grant(commit,self.secret),self.body)
        self.assertEqual(len(self.calls),1)

    def test_nonempty_node_token_is_query_encoded(self):
        self.adapter.token='a+b'
        grant=self.grant()
        self.broker.mutate(grant,b.sign_grant(grant,self.secret),self.body)
        self.assertEqual(self.calls[0][0],'/task/new/init?token=a%2Bb')

    def test_lost_response_survives_broker_restart_without_replay(self):
        grant=self.grant();signature=b.sign_grant(grant,self.secret);original=self.adapter.exchange
        def lost(*args):original(*args);raise OSError('synthetic response loss')
        with patch.object(self.adapter,'exchange',side_effect=lost):
            with self.assertRaises(OSError):self.broker.mutate(grant,signature,self.body)
        self.broker.close();self.broker=b.NodeMutationBroker(self.root/'gate'/'mutation-broker',**self.config)
        self.addCleanup(self.broker.close)
        with self.assertRaises(b.BrokerHeld):self.broker.mutate(grant,signature,self.body)
        self.assertEqual(len(self.calls),1)
        with self.broker.db:
            self.assertEqual(self.broker.db.execute('SELECT state FROM operations').fetchone()[0],'started')

    def test_replaced_database_path_fails_before_engine_mutation(self):
        original=self.broker.path.with_name('retained-original.sqlite3')
        self.broker.path.rename(original)
        self.broker.path.touch(mode=0o600)
        grant=self.grant()
        with self.assertRaises(b.BrokerHeld):self.broker.mutate(grant,b.sign_grant(grant,self.secret),self.body)
        self.assertEqual(self.calls,[])
        self.assertTrue(original.is_file())

    def test_upload_response_loss_retains_started_and_refuses_replay_after_reopen(self):
        init=self.grant();self.broker.mutate(init,b.sign_grant(init,self.secret),self.body)
        payload=b'synthetic-photo'
        upload=dict(self.grant(),operation='upload',fileName='photo.jpg',fileBytes=len(payload),
                    bodySha256=hashlib.sha256(payload).hexdigest(),contentType='application/octet-stream')
        signature=b.sign_grant(upload,self.secret);source,writer=socket.socketpair()
        original=self.adapter.exchange_upload
        def lost(*args):original(*args);raise OSError('synthetic upload response loss')
        try:
            writer.sendall(payload);writer.shutdown(socket.SHUT_WR)
            with patch.object(self.adapter,'exchange_upload',side_effect=lost):
                with self.assertRaises(OSError):self.broker.upload(upload,signature,source)
        finally:source.close();writer.close()
        self.assertEqual(self.broker.db.execute("SELECT state FROM operations WHERE operation='upload'").fetchone()[0],'started')
        self.broker.close();self.broker=b.NodeMutationBroker(self.root/'gate'/'mutation-broker',**self.config)
        self.addCleanup(self.broker.close)
        source,writer=socket.socketpair()
        try:
            with self.assertRaises(b.BrokerHeld):self.broker.upload(upload,signature,source)
        finally:source.close();writer.close()
        self.assertEqual(len(self.calls),2)

    def test_accepted_upload_waits_for_authenticated_sender_confirmation(self):
        init=self.grant();self.broker.mutate(init,b.sign_grant(init,self.secret),self.body)
        payload=b'synthetic-photo'
        upload=dict(self.grant(),operation='upload',fileName='photo.jpg',fileBytes=len(payload),
                    bodySha256=hashlib.sha256(payload).hexdigest(),contentType='application/octet-stream')
        signature=b.sign_grant(upload,self.secret);source,writer=socket.socketpair()
        try:
            writer.sendall(payload);writer.shutdown(socket.SHUT_WR)
            self.assertTrue(self.broker.upload(upload,signature,source)['success'])
        finally:source.close();writer.close()
        self.assertEqual(self.broker.db.execute("SELECT state FROM operations WHERE operation='upload'").fetchone()[0],'awaiting_sender')
        with self.assertRaises(b.BrokerHeld):self.broker.confirm_upload(upload,signature,'0'*64)
        self.assertEqual(self.broker.db.execute("SELECT state FROM operations WHERE operation='upload'").fetchone()[0],'awaiting_sender')
        self.broker.confirm_upload(upload,signature,b._sender_confirmation(upload,self.secret))
        self.assertEqual(self.broker.db.execute("SELECT state FROM operations WHERE operation='upload'").fetchone()[0],'complete')
        self.broker.db.execute("UPDATE operations SET file_key='changed.jpg' WHERE operation='upload'")
        source,writer=socket.socketpair()
        try:
            with self.assertRaises(b.BrokerHeld):self.broker.upload(upload,signature,source)
        finally:source.close();writer.close()
        self.assertEqual(len(self.calls),2)

    def test_sender_verification_failure_never_confirms_accepted_upload(self):
        from processing.guarded_native_engine import SenderCompletion
        from processing.odx_smoke_worker import SmokeError
        init=self.grant();self.broker.mutate(init,b.sign_grant(init,self.secret),self.body)
        payload=b'synthetic-photo';path=self.root/'photo.jpg';path.write_bytes(payload);path.chmod(0o600)
        upload=dict(self.grant(),operation='upload',fileName='photo.jpg',fileBytes=len(payload),
                    bodySha256=hashlib.sha256(payload).hexdigest(),contentType='application/octet-stream')
        item=dict(name='photo.jpg',bytes=len(payload),sha256=upload['bodySha256'],path=str(path))
        sender=b.LocalUploadSender()
        with patch.object(SenderCompletion,'verify',side_effect=SmokeError('synthetic sender verification failure')), \
             patch.object(b.LocalUploadSender,'_issued_item',return_value=item):
            with self.assertRaises(SmokeError):sender.send(self.broker,upload,b.sign_grant(upload,self.secret),store=object(),claim_token='synthetic',secret=self.secret)
        self.assertEqual(sender._unreaped,[])
        self.assertEqual(self.broker.db.execute("SELECT state FROM operations WHERE operation='upload'").fetchone()[0],'awaiting_sender')
        self.assertEqual(path.read_bytes(),payload)
        with patch.object(b.LocalUploadSender,'_issued_item',return_value=item), \
             patch('processing.guarded_native_engine.start_guarded_sender',side_effect=AssertionError('uncertain replay started producer')):
            with self.assertRaises(b.BrokerHeld):sender.send(self.broker,upload,b.sign_grant(upload,self.secret),store=object(),claim_token='synthetic',secret=self.secret)

    def test_mismatched_stream_stays_started_without_retry(self):
        init=self.grant();self.broker.mutate(init,b.sign_grant(init,self.secret),self.body)
        expected=b'synthetic-photo';altered=b'wrong-photo----'
        self.assertEqual(len(expected),len(altered))
        upload=dict(self.grant(),operation='upload',fileName='photo.jpg',fileBytes=len(expected),
                    bodySha256=hashlib.sha256(expected).hexdigest(),contentType='application/octet-stream')
        signature=b.sign_grant(upload,self.secret);source,writer=socket.socketpair()
        try:
            writer.sendall(altered);writer.shutdown(socket.SHUT_WR)
            with self.assertRaises(AdmissionError):self.broker.upload(upload,signature,source)
        finally:source.close();writer.close()
        self.assertEqual(self.broker.db.execute("SELECT state FROM operations WHERE operation='upload'").fetchone()[0],'started')
        source,writer=socket.socketpair()
        try:
            with self.assertRaises(b.BrokerHeld):self.broker.upload(upload,signature,source)
        finally:source.close();writer.close()

    def test_tampered_upload_grant_is_rejected_before_node_intent(self):
        init=self.grant();self.broker.mutate(init,b.sign_grant(init,self.secret),self.body)
        payload=b'synthetic-photo'
        upload=dict(self.grant(),operation='upload',fileName='photo.jpg',fileBytes=len(payload),
                    bodySha256=hashlib.sha256(payload).hexdigest(),contentType='application/octet-stream')
        signature=b.sign_grant(upload,self.secret)
        tampered=dict(upload,fileName='other.jpg');source,writer=socket.socketpair()
        try:
            with self.assertRaises(b.BrokerHeld):self.broker.upload(tampered,signature,source)
        finally:source.close();writer.close()
        self.assertEqual(self.broker.db.execute("SELECT COUNT(*) FROM operations WHERE operation='upload'").fetchone()[0],0)
        self.assertEqual(len(self.calls),1)

    def test_slow_drip_hits_absolute_deadline_and_retains_started_intent(self):
        self.drip=True;self.adapter.deadline_seconds=.2
        grant=self.grant();started=time.monotonic()
        with self.assertRaises((b.BrokerHeld,AdmissionError)):
            self.broker.mutate(grant,b.sign_grant(grant,self.secret),self.body)
        self.assertLess(time.monotonic()-started,1.5)
        self.assertTrue(self.drip_done.wait(3))
        self.assertEqual(self.broker.db.execute('SELECT state FROM operations').fetchone()[0],'started')
        with self.assertRaises(b.BrokerHeld):self.broker.mutate(grant,b.sign_grant(grant,self.secret),self.body)
        self.assertEqual(len(self.calls),1)

    def test_close_waits_for_inflight_mutation(self):
        grant=self.grant();entered=threading.Event();release=threading.Event();closed=threading.Event();out=[]
        original=self.adapter.exchange
        def slow(*args):
            entered.set()
            if not release.wait(3):raise RuntimeError('synthetic exchange timed out')
            return original(*args)
        def run():
            try:out.append(self.broker.mutate(grant,b.sign_grant(grant,self.secret),self.body))
            except Exception as exc:out.append(exc)
        worker=threading.Thread(target=run)
        closer=threading.Thread(target=lambda:(self.broker.close(),closed.set()))
        with patch.object(self.adapter,'exchange',side_effect=slow):
            worker.start()
            try:
                self.assertTrue(entered.wait(3));closer.start()
                self.assertFalse(closed.wait(.2))
            finally:
                release.set();worker.join(5)
                if closer.ident is not None:closer.join(5)
        self.assertFalse(worker.is_alive());self.assertFalse(closer.is_alive());self.assertTrue(closed.is_set())
        self.assertEqual(out[0]['uuid'],self.task)
        with self.assertRaises(b.BrokerHeld):self.broker.mutate(grant,b.sign_grant(grant,self.secret),self.body)

    def test_issuer_binds_real_queue_claim_and_exact_viewer_phase(self):
        queue=Queue(self.root/'coordinator',node_ids=('cluster','main'),viewer_enabled=True)
        try:
            store=TaskStore(queue,self.root/'coordinator'/'viewer-tasks')
            store.initialize(self.task,'Synthetic',[],None,'production')
            store.upload(self.task,'synthetic.jpg',io.BytesIO(b'x'),1);store.commit(self.task)
            claim=queue.claim({'workerId':'viewer-dispatch','nodeId':'main','profiles':[VIEWER_PROFILE]})
            store.update(self.task,stage='running',phase='init_pending',node_id='main',
                         engine_uuid=str(uuid.uuid4()),claim_token=claim['claimToken'])
            kwargs=dict(job_id=claim['id'],claim_token=claim['claimToken'],engine_id=self.engine,
                        gate_generation=1,operation_id=str(uuid.uuid4()),body=self.body,
                        content_type='application/octet-stream',secret=self.secret)
            grant,signature=b.issue_init_grant(queue,**kwargs)
            self.assertEqual(grant['jobId'],claim['id']);self.assertEqual(grant['coordinatorEpoch'],queue.startup_id)
            self.assertEqual(grant['engineTaskId'],store.task(self.task)['engine_uuid'])
            self.assertEqual(signature,b.sign_grant(grant,self.secret))
            with self.assertRaises(b.BrokerHeld):b.issue_init_grant(queue,**dict(kwargs,claim_token='wrong'))
            store.update(self.task,phase='initialized')
            with self.assertRaises(b.BrokerHeld):b.issue_init_grant(queue,**kwargs)
        finally:queue.close()

    def test_real_queue_grant_reaches_separate_node_gate_and_loopback_peer(self):
        queue=Queue(self.root/'coordinator',node_ids=('cluster','main'),viewer_enabled=True)
        node_path=self.root/'remote-node-gate'
        try:
            store=TaskStore(queue,self.root/'coordinator'/'viewer-tasks')
            b.install_upload_issuer_schema(queue)
            store.initialize(self.task,'Synthetic',[],None,'production')
            store.upload(self.task,'synthetic.jpg',io.BytesIO(b'x'),1);store.commit(self.task)
            claim=queue.claim({'workerId':'viewer-dispatch','nodeId':'main','profiles':[VIEWER_PROFILE]})
            store.update(self.task,stage='running',phase='init_pending',node_id='main',
                         engine_uuid=self.task,claim_token=claim['claimToken'])
            with patch('processing.node_gate.canonical_gate_path',return_value=node_path), \
                 patch('processing.node_mutation_broker.canonical_gate_path',return_value=node_path):
                with NodeGate(self.engine) as gate:gate.provision('main',self.origin,queue.authority_id)
                state=node_path/'mutation-broker';state.mkdir(mode=0o700)
                remote=b.NodeMutationBroker(state,**dict(self.config,authority_id=queue.authority_id,coordinator_epoch=queue.startup_id))
                try:
                    grant,signature=b.issue_init_grant(queue,job_id=claim['id'],claim_token=claim['claimToken'],
                        engine_id=self.engine,gate_generation=1,operation_id=str(uuid.uuid4()),body=self.body,
                        content_type='application/octet-stream',secret=self.secret)
                    self.assertEqual(remote.mutate(grant,signature,self.body)['uuid'],self.task)
                    self.assertEqual(len(self.calls),1)
                    store.update(self.task,phase='upload_pending')
                    upload_id=str(uuid.uuid4())
                    upload,upload_signature=b.issue_upload_grant(queue,job_id=claim['id'],claim_token=claim['claimToken'],
                        engine_id=self.engine,gate_generation=1,operation_id=upload_id,file_key='synthetic.jpg',secret=self.secret)
                    sender=b.LocalUploadSender()
                    stored=store.task(self.task)['files'][0]
                    original=Path(stored['path']);alternate=original.parent/('input-'+uuid.uuid4().hex)
                    alternate.write_bytes(original.read_bytes());alternate.chmod(0o600)
                    with queue._db() as db:db.execute('UPDATE viewer_files SET path=? WHERE uuid=?',(alternate.name,self.task))
                    with self.assertRaises(b.BrokerHeld):
                        sender.send(remote,upload,upload_signature,store=store,claim_token=claim['claimToken'],secret=self.secret)
                    self.assertEqual(len(self.calls),1)
                    with queue._db() as db:db.execute('UPDATE viewer_files SET path=? WHERE uuid=?',(original.name,self.task))
                    self.assertTrue(sender.send(remote,upload,upload_signature,store=store,claim_token=claim['claimToken'],secret=self.secret)['success'])
                    self.assertEqual(sender._unreaped,[])
                    self.assertEqual(len(self.calls),2)
                    self.assertIn(b'filename="synthetic.jpg"',self.calls[1][1])
                    self.assertIn(b'\r\n\r\nx\r\n',self.calls[1][1])
                    with patch('processing.guarded_native_engine.start_guarded_sender',side_effect=AssertionError('replay started producer')):
                        self.assertTrue(sender.send(remote,upload,upload_signature,store=store,claim_token=claim['claimToken'],secret=self.secret)['success'])
                    self.assertEqual(len(self.calls),2)
                    replay,replay_signature=b.issue_upload_grant(queue,job_id=claim['id'],claim_token=claim['claimToken'],
                        engine_id=self.engine,gate_generation=1,operation_id=upload_id,file_key='synthetic.jpg',secret=self.secret)
                    self.assertEqual((replay,replay_signature),(upload,upload_signature))
                    with self.assertRaises(b.BrokerHeld):
                        b.issue_upload_grant(queue,job_id=claim['id'],claim_token=claim['claimToken'],
                            engine_id=self.engine,gate_generation=1,operation_id=str(uuid.uuid4()),file_key='synthetic.jpg',secret=self.secret)
                finally:remote.close()
        finally:queue.close()


if __name__=='__main__':unittest.main()
