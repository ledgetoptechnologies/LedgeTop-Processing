"""Owned loopback NodeODM peer and persistent node gate; retained test roots."""
import hashlib
import io
import json
import os
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
from processing.authority_lease import AuthorityLease, record_required


def attach_lease(queue, directory):
    directory.mkdir(mode=0o700)
    lease=AuthorityLease(directory, queue.authority_id)
    queue.authority_lease=lease
    record_required(queue,lease)
    return lease


class NodeMutationBrokerTests(unittest.TestCase):
    def setUp(self):
        self.root=Path(tempfile.mkdtemp(prefix='lt-node-broker-test-')).resolve()
        self.addCleanup(lambda:print('retained synthetic node broker root:',self.root,flush=True))
        self.engine='engine-synthetic';self.node='main'
        self.owner_queue=Queue(self.root/'owner-queue',node_ids=('cluster','main'),viewer_enabled=True)
        self.addCleanup(self.owner_queue.close)
        self.owner_lease=attach_lease(self.owner_queue,self.root/'owner-lease')
        self.addCleanup(self.owner_lease.close)
        self.authority=self.owner_queue.authority_id;self.epoch=self.owner_queue.startup_id
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
                else:self.wfile.write(value)
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
        self.task=str(uuid.uuid4());self.body=b'synthetic-init'
        store=TaskStore(self.owner_queue,self.root/'owner-queue'/'viewer-tasks')
        store.initialize(self.task,'Synthetic',[],None,'production')
        store.upload(self.task,'synthetic.jpg',io.BytesIO(b'x'),1);store.commit(self.task)
        claim=self.owner_queue.claim({'workerId':'viewer-dispatch','nodeId':'main','profiles':[VIEWER_PROFILE]})
        self.job=claim['id']
        store.update(self.task,stage='running',phase='init_pending',node_id='main',
                     engine_uuid=self.task,claim_token=claim['claimToken'])
        self.config=dict(node_id=self.node,engine_id=self.engine,authority_id=self.authority,coordinator_epoch=self.epoch,
                         coordinator_origin=self.origin,gate_generation=1,secret=self.secret,adapter=self.adapter,
                         authority_witness=b.LocalCoordinatorAuthority(self.owner_queue))
        self.broker=b.NodeMutationBroker(state,**self.config)
        self.addCleanup(self.broker.close)

    def grant(self,operation='init',body=None):
        body=self.body if body is None else body
        return dict(authorityId=self.authority,nodeId=self.node,engineId=self.engine,coordinatorEpoch=self.epoch,
                    jobId=self.job,engineTaskId=self.task,environment='production',gateGeneration=1,
                    operationId=str(uuid.uuid4()),operation=operation,bodySha256=hashlib.sha256(body).hexdigest(),
                    contentType='application/octet-stream')

    def test_prior_inspection_refuses_sidecar_without_mutation(self):
        self.broker.close()
        state=self.root/'gate'/'mutation-broker'
        sidecar=state/'node-broker.sqlite3-wal'
        sidecar.write_bytes(b'synthetic unresolved WAL')
        before={p.name:(p.stat().st_size,p.stat().st_mtime_ns) for p in state.iterdir()}
        with self.assertRaises(b.BrokerHeld):b.inspect_prior_node_state(self.engine)
        after={p.name:(p.stat().st_size,p.stat().st_mtime_ns) for p in state.iterdir()}
        self.assertEqual(before,after)

    @unittest.skipUnless(hasattr(os, 'mkfifo'), 'POSIX FIFO required')
    def test_prior_inspection_refuses_fifo_without_blocking(self):
        self.broker.close()
        state=self.root/'gate'/'mutation-broker'
        journal=state/'node-broker.sqlite3'
        journal.rename(state/'retained-original.sqlite3')
        os.mkfifo(journal,0o600)
        started=time.monotonic()
        with self.assertRaises(b.BrokerHeld):b.inspect_prior_node_state(self.engine)
        self.assertLess(time.monotonic()-started,2)

    def test_prior_inspection_refuses_replaced_journal_symlink(self):
        self.broker.close()
        state=self.root/'gate'/'mutation-broker'
        journal=state/'node-broker.sqlite3'
        original=state/'retained-original.sqlite3'
        journal.rename(original)
        journal.symlink_to(original)
        with self.assertRaises(b.BrokerHeld):b.inspect_prior_node_state(self.engine)
        self.assertTrue(original.exists())

    def test_reopen_missing_binding_preserves_existing_journal(self):
        self.broker.db.execute('DELETE FROM binding')
        self.broker.close()
        with self.assertRaises(b.BrokerHeld):
            b.NodeMutationBroker(self.root/'gate'/'mutation-broker',**self.config)
        import sqlite3
        db=sqlite3.connect((self.root/'gate'/'mutation-broker'/'node-broker.sqlite3').as_uri()+'?mode=ro',uri=True)
        try:self.assertEqual(db.execute('SELECT COUNT(*) FROM binding').fetchone()[0],0)
        finally:db.close()

    def test_prior_epoch_inspection_preserves_uncertain_state_and_refuses_identity_change(self):
        self.broker.close()
        report=b.inspect_prior_node_state(self.engine)
        self.assertEqual(report['identity']['coordinatorEpoch'],self.epoch)
        self.assertFalse(report['authorizesAdmission'])
        self.assertEqual(report['operations'],[])
        self.broker=b.NodeMutationBroker(self.root/'gate'/'mutation-broker',**self.config)
        grant=self.grant()
        with patch.object(self.adapter,'exchange',side_effect=RuntimeError('synthetic disconnect')):
            with self.assertRaises(RuntimeError):self.broker.mutate(grant,b.sign_grant(grant,self.secret),self.body)
        self.broker.close()
        report=b.inspect_prior_node_state(self.engine)
        self.assertEqual(report['operations'][0]['state'],'started')
        self.assertEqual(report['gateReservation']['job']['id'],self.job)
        self.assertIsNotNone(report['journalReservation'])
        manifest=self.root/'gate'/'mutation-broker'/'identity.json'
        value=json.loads(manifest.read_text());value['coordinatorEpoch']=str(uuid.uuid4())
        manifest.write_text(json.dumps(value))
        with self.assertRaises(b.BrokerHeld):b.inspect_prior_node_state(self.engine)
        with self.assertRaises(b.BrokerHeld):b.NodeMutationBroker(self.root/'gate'/'mutation-broker',**self.config)

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

    def test_revoked_local_queue_refuses_previously_signed_grant(self):
        grant=self.grant();signature=b.sign_grant(grant,self.secret)
        self.owner_queue.close()
        with self.assertRaises(b.BrokerHeld):self.broker.mutate(grant,signature,self.body)
        self.assertEqual(self.calls,[])

    def test_lost_separate_authority_lease_refuses_grant_while_queue_stays_open(self):
        grant=self.grant();signature=b.sign_grant(grant,self.secret)
        self.owner_lease.close()
        self.assertFalse(self.owner_queue._lock.closed)
        with self.assertRaises(b.BrokerHeld):self.broker.mutate(grant,signature,self.body)
        self.assertEqual(self.calls,[])

    def test_changed_authority_lease_path_after_forward_retains_started_intent(self):
        grant=self.grant();signature=b.sign_grant(grant,self.secret);original=self.adapter.exchange
        def replace_after_response(*args):
            result=original(*args)
            self.owner_lease.path.rename(self.owner_lease.path.with_name('retained-original.lease'))
            self.owner_lease.path.touch(mode=0o600)
            return result
        with patch.object(self.adapter,'exchange',side_effect=replace_after_response):
            with self.assertRaises(b.BrokerHeld):self.broker.mutate(grant,signature,self.body)
        self.assertEqual(len(self.calls),1)
        self.assertEqual(self.broker.db.execute('SELECT state FROM operations').fetchone()[0],'started')
        with self.assertRaises(b.BrokerHeld):self.broker.mutate(grant,signature,self.body)

    def test_live_queue_rejects_valid_grant_after_task_phase_changes(self):
        grant=self.grant();signature=b.sign_grant(grant,self.secret)
        store=TaskStore(self.owner_queue,self.root/'owner-queue'/'viewer-tasks')
        store.update(self.task,phase='initialized')
        with self.assertRaises(b.BrokerHeld):self.broker.mutate(grant,signature,self.body)
        self.assertEqual(self.calls,[])

    def test_queue_close_waits_for_authorized_mutation_then_revokes(self):
        grant=self.grant();signature=b.sign_grant(grant,self.secret)
        entered=threading.Event();release=threading.Event();closed=threading.Event();out=[]
        original=self.adapter.exchange
        def slow(*args):
            entered.set()
            if not release.wait(3):raise RuntimeError('synthetic exchange timed out')
            return original(*args)
        def send():
            try:out.append(self.broker.mutate(grant,signature,self.body))
            except Exception as error:out.append(error)
        worker=threading.Thread(target=send)
        closer=threading.Thread(target=lambda:(self.owner_queue.close(),closed.set()))
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
        with self.assertRaises(b.BrokerHeld):self.broker.mutate(grant,signature,self.body)
        self.assertEqual(len(self.calls),1)

    def test_authority_lease_close_waits_for_inflight_mutation(self):
        grant=self.grant();signature=b.sign_grant(grant,self.secret)
        entered=threading.Event();release=threading.Event();closed=threading.Event();out=[]
        original=self.adapter.exchange
        def slow(*args):
            entered.set()
            if not release.wait(3):raise RuntimeError('synthetic exchange timed out')
            return original(*args)
        def send():
            try:out.append(self.broker.mutate(grant,signature,self.body))
            except Exception as error:out.append(error)
        worker=threading.Thread(target=send)
        closer=threading.Thread(target=lambda:(self.owner_lease.close(),closed.set()))
        with patch.object(self.adapter,'exchange',side_effect=slow):
            worker.start()
            try:
                self.assertTrue(entered.wait(3));closer.start();self.assertFalse(closed.wait(.2))
            finally:
                release.set();worker.join(5)
                if closer.ident is not None:closer.join(5)
        self.assertFalse(worker.is_alive());self.assertFalse(closer.is_alive());self.assertTrue(closed.is_set())
        self.assertEqual(out[0]['uuid'],self.task)
        with self.assertRaises(b.BrokerHeld):self.broker.mutate(grant,signature,self.body)
        self.assertEqual(len(self.calls),1)

    def test_reentrant_close_cannot_drop_authority_during_mutation(self):
        grant=self.grant();original=self.adapter.exchange;checked=[]
        def attempt_close(*args):
            with self.assertRaises(RuntimeError):self.owner_queue.close()
            with self.assertRaises(RuntimeError):self.owner_lease.close()
            checked.append(True)
            return original(*args)
        with patch.object(self.adapter,'exchange',side_effect=attempt_close):
            self.assertEqual(self.broker.mutate(grant,b.sign_grant(grant,self.secret),self.body)['uuid'],self.task)
        self.assertEqual(checked,[True]);self.assertFalse(self.owner_queue._lock.closed)
        self.assertIsNotNone(self.owner_lease._descriptor)

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

    def test_other_local_queue_grant_reaches_its_node_gate_and_loopback_peer(self):
        queue=Queue(self.root/'coordinator',node_ids=('cluster','main'),viewer_enabled=True)
        node_path=self.root/'remote-node-gate'
        lease=attach_lease(queue,self.root/'other-lease')
        try:
            store=TaskStore(queue,self.root/'coordinator'/'viewer-tasks')
            store.initialize(self.task,'Synthetic',[],None,'production')
            store.upload(self.task,'synthetic.jpg',io.BytesIO(b'x'),1);store.commit(self.task)
            claim=queue.claim({'workerId':'viewer-dispatch','nodeId':'main','profiles':[VIEWER_PROFILE]})
            store.update(self.task,stage='running',phase='init_pending',node_id='main',
                         engine_uuid=self.task,claim_token=claim['claimToken'])
            with patch('processing.node_gate.canonical_gate_path',return_value=node_path), \
                 patch('processing.node_mutation_broker.canonical_gate_path',return_value=node_path):
                with NodeGate(self.engine) as gate:gate.provision('main',self.origin,queue.authority_id)
                state=node_path/'mutation-broker';state.mkdir(mode=0o700)
                remote=b.NodeMutationBroker(state,**dict(self.config,authority_id=queue.authority_id,coordinator_epoch=queue.startup_id,
                    authority_witness=b.LocalCoordinatorAuthority(queue)))
                try:
                    grant,signature=b.issue_init_grant(queue,job_id=claim['id'],claim_token=claim['claimToken'],
                        engine_id=self.engine,gate_generation=1,operation_id=str(uuid.uuid4()),body=self.body,
                        content_type='application/octet-stream',secret=self.secret)
                    self.assertEqual(remote.mutate(grant,signature,self.body)['uuid'],self.task)
                    self.assertEqual(len(self.calls),1)
                finally:remote.close()
        finally:queue.close();lease.close()


if __name__=='__main__':unittest.main()
