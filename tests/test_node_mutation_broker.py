"""Owned loopback NodeODM peer and persistent node gate; retained test roots."""
import hashlib
import io
import json
import socket
import sqlite3
import subprocess
import sys
from pathlib import Path
import tempfile
import threading
import time
import unittest
import uuid
from types import SimpleNamespace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

from processing import node_mutation_broker as b
from processing.engine_ingress_guard import AdmissionError, BackendPin
from processing.node_gate import NodeGate
from processing.prototype import Queue, VIEWER_PROFILE
from processing.maintenance import Conflict
from processing.viewer_tasks import TaskStore
from processing.authority_lease import AuthorityLease, record_required
from processing.broker_intent_fence import install_offline


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
        self.task=str(uuid.uuid4());self.body=b'synthetic-init'
        store=TaskStore(self.owner_queue,self.root/'owner-queue'/'viewer-tasks')
        self.owner_store=store
        with self.owner_queue._db() as db: install_offline(db,enabled=True)
        store.initialize(self.task,'Synthetic',[],None,'production')
        store.upload(self.task,'synthetic.jpg',io.BytesIO(b'x'),1);store.commit(self.task)
        claim=self.owner_queue.claim({'workerId':'viewer-dispatch','nodeId':'main','profiles':[VIEWER_PROFILE]})
        self.owner_claim=claim
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
                    jobId=self.job,engineTaskId=self.task,environment='production',gateGeneration=1,taskGeneration=1,
                    operationId=str(uuid.uuid4()),operation=operation,bodySha256=hashlib.sha256(body).hexdigest(),
                    contentType='application/octet-stream')

    def begin_upload(self):
        init=self.grant();self.broker.mutate(init,b.sign_grant(init,self.secret),self.body)
        self.owner_store.update(self.task,phase='upload_pending')

    def test_signed_grant_evidence_verifier_requires_exact_mac_and_canonical_bytes(self):
        grant=self.grant();raw=b._json(grant).decode('ascii');signature=b.sign_grant(grant,self.secret)
        self.assertEqual(b.verify_grant_evidence(raw,signature,self.secret),grant)
        for altered,mac,key in ((raw,'0'*64,self.secret),(' '+raw,signature,self.secret),
                                (raw,signature,b'wrong-test-key-'*3),
                                (raw[:-1]+',"operationId":"'+grant['operationId']+'"}',signature,self.secret)):
            with self.subTest(altered=altered[:20]):
                with self.assertRaises(b.BrokerHeld):b.verify_grant_evidence(altered,mac,key)

    def test_evidence_rejects_insert_or_replace_with_recursive_triggers_off_and_on(self):
        grant=self.grant();signature=b.sign_grant(grant,self.secret)
        self.broker.mutate(grant,signature,self.body)
        op=grant['operationId']
        for flag in ('OFF','ON'):
            with self.subTest(recursive_triggers=flag):
                self.broker.db.execute('PRAGMA recursive_triggers='+flag)
                node=self.broker.db.execute('SELECT grant_json,signature FROM node_grants WHERE operation_id=?',(op,)).fetchone()
                observation=self.broker.db.execute('SELECT grant_hash,reply_json FROM engine_observations WHERE operation_id=?',(op,)).fetchone()
                with self.assertRaises(sqlite3.IntegrityError):
                    self.broker.db.execute('INSERT INTO node_grants VALUES(?,?,?)',(None,'{}','0'*64))
                with self.assertRaises(sqlite3.IntegrityError):
                    self.broker.db.execute('INSERT INTO engine_observations VALUES(?,?,?)',(None,'0'*64,'{}'))
                with self.assertRaises(sqlite3.IntegrityError):
                    self.broker.db.execute('INSERT OR REPLACE INTO node_grants VALUES(?,?,?)',(op,'{}','0'*64))
                with self.assertRaises(sqlite3.IntegrityError):
                    self.broker.db.execute('INSERT OR REPLACE INTO engine_observations VALUES(?,?,?)',(op,'0'*64,'{}'))
                self.assertEqual(tuple(self.broker.db.execute('SELECT grant_json,signature FROM node_grants WHERE operation_id=?',(op,)).fetchone()),tuple(node))
                self.assertEqual(tuple(self.broker.db.execute('SELECT grant_hash,reply_json FROM engine_observations WHERE operation_id=?',(op,)).fetchone()),tuple(observation))
                with self.owner_queue._db() as db:
                    db.execute('PRAGMA recursive_triggers='+flag)
                    coordinator=db.execute('SELECT grant_json,signature FROM broker_grant_evidence WHERE operation_id=?',(op,)).fetchone()
                    with self.assertRaises(sqlite3.IntegrityError):
                        db.execute('INSERT INTO broker_grant_evidence VALUES(?,?,?)',(None,'{}','0'*64))
                    with self.assertRaises(sqlite3.IntegrityError):
                        db.execute('INSERT OR REPLACE INTO broker_grant_evidence VALUES(?,?,?)',(op,'{}','0'*64))
                    self.assertEqual(tuple(db.execute('SELECT grant_json,signature FROM broker_grant_evidence WHERE operation_id=?',(op,)).fetchone()),tuple(coordinator))

    def test_unsigned_coordinator_prepare_cannot_create_intent(self):
        grant=self.grant()
        with self.assertRaises(b.BrokerHeld):
            with b.LocalCoordinatorAuthority(self.owner_queue).hold(grant,3,create=True):
                self.fail('unsigned grant was accepted')
        with self.owner_queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM broker_intents').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM broker_grant_evidence').fetchone()[0],0)
        self.assertEqual(self.calls,[])

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

    def test_queue_close_refuses_during_mutation_then_revokes(self):
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
        def attempt_close():
            try:self.owner_queue.close()
            except RuntimeError:closed.set()
        closer=threading.Thread(target=attempt_close)
        with patch.object(self.adapter,'exchange',side_effect=slow):
            worker.start()
            try:
                self.assertTrue(entered.wait(3));closer.start()
                self.assertTrue(closed.wait(2))
            finally:
                release.set();worker.join(5)
                if closer.ident is not None:closer.join(5)
        self.assertFalse(worker.is_alive());self.assertFalse(closer.is_alive());self.assertTrue(closed.is_set())
        self.assertEqual(out[0]['uuid'],self.task)
        self.owner_queue.close()
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

    def test_reentrant_and_external_transitions_are_fenced_without_blocking_reads(self):
        pending=str(uuid.uuid4())
        self.owner_store.initialize(pending,'Next',[],None,'production')
        self.owner_store.upload(pending,'next.jpg',io.BytesIO(b'x'),1);self.owner_store.commit(pending)
        grant=self.grant();original=self.adapter.exchange
        def callback(*args):
            self.assertEqual(self.owner_queue.get(self.job)['status'],'running')
            self.assertIsNone(self.owner_queue.claim({'workerId':'viewer-dispatch','nodeId':'main','profiles':[VIEWER_PROFILE]}))
            with self.assertRaises(sqlite3.IntegrityError):self.owner_queue.operator(self.job,'hold',{'note':'synthetic'})
            with self.assertRaises(Conflict):self.owner_queue.complete(self.job,{'claimToken':self.owner_claim['claimToken'],'status':'succeeded','result':{}})
            raw=sqlite3.connect(self.owner_queue.path,timeout=0)
            try:
                with self.assertRaises(sqlite3.IntegrityError):raw.execute("UPDATE jobs SET status='failed' WHERE id=?",(self.job,))
            finally:raw.close()
            return original(*args)
        with patch.object(self.adapter,'exchange',side_effect=callback):
            self.broker.mutate(grant,b.sign_grant(grant,self.secret),self.body)
        self.assertEqual(self.owner_queue.get(self.job)['status'],'running')

    def test_upload_keeps_barrier_until_confirmation_and_queue_remains_readable(self):
        self.begin_upload();payload=b'synthetic-photo'
        grant=dict(self.grant(),operation='upload',fileName='photo.jpg',fileBytes=len(payload),
                   bodySha256=hashlib.sha256(payload).hexdigest(),contentType='application/octet-stream')
        signature=b.sign_grant(grant,self.secret);source,writer=socket.socketpair()
        original=self.adapter.exchange_upload
        def callback(*args):
            self.assertEqual(self.owner_queue.get(self.job)['status'],'running')
            with self.assertRaises(sqlite3.IntegrityError):self.owner_queue.operator(self.job,'hold',{'note':'synthetic'})
            return original(*args)
        try:
            writer.sendall(payload);writer.shutdown(socket.SHUT_WR)
            with patch.object(self.adapter,'exchange_upload',side_effect=callback):
                self.broker.upload(grant,signature,source)
        finally:source.close();writer.close()
        with self.owner_queue._db() as db:
            self.assertEqual(db.execute("SELECT state FROM broker_intents WHERE operation_id=?",(grant['operationId'],)).fetchone()[0],'started')
        with self.assertRaises(Conflict):self.owner_queue.complete(self.job,{'claimToken':self.owner_claim['claimToken'],'status':'succeeded','result':{}})
        self.broker.confirm_upload(grant,signature,b._sender_confirmation(grant,self.secret))
        with self.owner_queue._db() as db:
            self.assertEqual(db.execute("SELECT state FROM broker_intents WHERE operation_id=?",(grant['operationId'],)).fetchone()[0],'complete')

    def test_orphan_started_intent_blocks_next_claim_even_without_active_job(self):
        next_id=str(uuid.uuid4())
        self.owner_store.initialize(next_id,'Next',[],None,'production')
        self.owner_store.upload(next_id,'next.jpg',io.BytesIO(b'x'),1);self.owner_store.commit(next_id)
        with self.owner_queue._db() as db:
            db.execute("INSERT INTO broker_intents VALUES(?,?,?,'started')",(str(uuid.uuid4()),str(uuid.uuid4()),'a'*64))
            db.execute("UPDATE jobs SET status='failed' WHERE id=?",(self.job,))
        self.assertIsNone(self.owner_queue.claim({'workerId':'viewer-dispatch','nodeId':'main','profiles':[VIEWER_PROFILE]}))
        raw=sqlite3.connect(self.owner_queue.path,timeout=0)
        try:
            with self.assertRaises(sqlite3.IntegrityError):raw.execute("UPDATE jobs SET status='running' WHERE status='queued'")
        finally:raw.close()

    def test_changed_jobs_schema_rejects_restart_exception(self):
        with self.owner_queue._db() as db:
            db.execute("INSERT INTO broker_intents VALUES(?,?,?,'started')",(str(uuid.uuid4()),self.job,'a'*64))
            db.execute('ALTER TABLE jobs ADD COLUMN new_unqualified_column TEXT')
        raw=sqlite3.connect(self.owner_queue.path,timeout=0)
        try:
            with self.assertRaises(sqlite3.IntegrityError):
                raw.execute("UPDATE jobs SET status='held', held_reason='coordinator_restart' WHERE id=?",(self.job,))
        finally:raw.close()

    def test_missing_fence_trigger_fails_closed(self):
        grant=self.grant()
        with self.owner_queue._db() as db:db.execute('DROP TRIGGER broker_job_update')
        with self.assertRaises(b.BrokerHeld):self.broker.mutate(grant,b.sign_grant(grant,self.secret),self.body)
        self.assertEqual(self.calls,[])
        with self.owner_queue._db() as db:self.assertFalse(self.owner_queue._transfer_admission_clear(db))

    def test_abrupt_owned_process_exit_preserves_intent_and_holds_restart(self):
        directory=self.root/'owned-crash-queue'
        child="""
import os, sys, uuid
from pathlib import Path
from processing.prototype import Queue
from processing.viewer_tasks import TaskStore
from processing.broker_intent_fence import install_offline
root=Path(sys.argv[1]);q=Queue(root);TaskStore(q,root/'viewer-tasks')
with q._db() as db:install_offline(db,enabled=True)
job,_=q.submit({'requestId':'crash-fixture','environment':'production','profile':'treedar-synthetic-v1'})
q.claim({'workerId':'crash-fixture'})
with q._db() as db:db.execute("INSERT INTO broker_intents VALUES(?,?,?,'started')",(str(uuid.uuid4()),job['id'],'a'*64))
os._exit(27)
"""
        result=subprocess.run([sys.executable,'-c',child,str(directory)],timeout=10,capture_output=True,text=True)
        self.assertEqual(result.returncode,27,result.stderr)
        restarted=Queue(directory)
        try:
            jobs=restarted.list();self.assertEqual(len(jobs),1);self.assertEqual(jobs[0]['status'],'held')
            with restarted._db() as db:self.assertEqual(db.execute("SELECT state FROM broker_intents").fetchone()[0],'started')
            self.assertIsNone(restarted.claim({'workerId':'crash-fixture'}))
        finally:restarted.close()

    def test_lost_forward_persists_barrier_across_coordinator_restart(self):
        pending=str(uuid.uuid4())
        self.owner_store.initialize(pending,'Next',[],None,'production')
        self.owner_store.upload(pending,'next.jpg',io.BytesIO(b'x'),1);self.owner_store.commit(pending)
        grant=self.grant();original=self.adapter.exchange
        def lost(*args):original(*args);raise OSError('synthetic lost response')
        with patch.object(self.adapter,'exchange',side_effect=lost):
            with self.assertRaises(OSError):self.broker.mutate(grant,b.sign_grant(grant,self.secret),self.body)
        self.broker.close();self.owner_lease.close();self.owner_queue.close()
        restarted=Queue(self.root/'owner-queue',node_ids=('cluster','main'),viewer_enabled=True)
        try:
            self.assertEqual(restarted.get(self.job)['status'],'held')
            self.assertIsNone(restarted.claim({'workerId':'viewer-dispatch','nodeId':'main','profiles':[VIEWER_PROFILE]}))
            with self.assertRaises(Conflict):restarted.operator(self.job,'resolve',{'note':'synthetic','workerStopped':True})
            with restarted._db() as db:
                self.assertEqual(db.execute("SELECT state FROM broker_intents WHERE operation_id=?",(grant['operationId'],)).fetchone()[0],'started')
        finally:restarted.close()

    def test_match_node_and_new_coordinator_evidence_without_release(self):
        from processing.authority_lease import acquire
        grant=self.grant();signature=b.sign_grant(grant,self.secret);checks=[]
        original=self.broker.witness._check
        def lose_authority_after_forward():
            checks.append(True)
            if len(checks)==2:raise b.BrokerHeld('synthetic lost authority')
            return original()
        with patch.object(self.broker.witness,'_check',side_effect=lose_authority_after_forward):
            with self.assertRaises(b.BrokerHeld):self.broker.mutate(grant,signature,self.body)
        self.assertEqual(len(self.calls),1)
        nonce='a'*64
        node=self.broker.inspect_held_evidence(grant,signature,nonce)
        self.assertEqual(node['report']['operationState'],'started')
        self.assertEqual(node['report']['observation']['uuid'],grant['engineTaskId'])
        self.owner_lease.close();self.owner_queue.close()
        lease=acquire(self.root/'owner-lease',self.root/'owner-queue')
        restarted=Queue(self.root/'owner-queue',node_ids=('cluster','main'),viewer_enabled=True)
        try:
            restarted.authority_lease=lease;witness=b.LocalCoordinatorAuthority(restarted)
            result=b.match_coordinator_node_evidence(restarted,witness,grant['operationId'],node,nonce,self.secret)
            self.assertTrue(result['observationPresent']);self.assertFalse(result['authorizesRelease'])
            self.assertEqual(result['nodeIdentity'],'not-independently-authenticated')
            self.assertEqual(result['engineTaskState'],'unverified')
            self.assertEqual(restarted.get(self.job)['status'],'held')
            tampered=json.loads(json.dumps(node));tampered['report']['reservationId']='0'*32
            with self.assertRaises(b.BrokerHeld):
                b.match_coordinator_node_evidence(restarted,witness,grant['operationId'],tampered,nonce,self.secret)
            with self.assertRaises(b.BrokerHeld):
                b.match_coordinator_node_evidence(restarted,witness,grant['operationId'],node,'b'*64,self.secret)
            with self.assertRaises(b.BrokerHeld):
                b.match_coordinator_node_evidence(restarted,witness,grant['operationId'],node,nonce,b'wrong-synthetic-key-'*3)
            self.assertIsNone(restarted.claim({'workerId':'viewer-dispatch','nodeId':'main','profiles':[VIEWER_PROFILE]}))
        finally:restarted.close();lease.close()

    def test_match_missing_engine_observation_remains_unverified(self):
        from processing.authority_lease import acquire
        grant=self.grant();signature=b.sign_grant(grant,self.secret);original=self.adapter.exchange
        def lost(*args):original(*args);raise OSError('synthetic lost response')
        with patch.object(self.adapter,'exchange',side_effect=lost):
            with self.assertRaises(OSError):self.broker.mutate(grant,signature,self.body)
        nonce='c'*64;node=self.broker.inspect_held_evidence(grant,signature,nonce)
        self.assertIsNone(node['report']['observation'])
        self.owner_lease.close();self.owner_queue.close()
        lease=acquire(self.root/'owner-lease',self.root/'owner-queue')
        restarted=Queue(self.root/'owner-queue',node_ids=('cluster','main'),viewer_enabled=True)
        try:
            restarted.authority_lease=lease;witness=b.LocalCoordinatorAuthority(restarted)
            result=b.match_coordinator_node_evidence(restarted,witness,grant['operationId'],node,nonce,self.secret)
            self.assertFalse(result['observationPresent']);self.assertFalse(result['authorizesRelease'])
            self.assertEqual(result['engineTaskState'],'unverified');self.assertEqual(len(self.calls),1)
        finally:restarted.close();lease.close()

    def test_coordinator_restart_inspects_authenticated_grant_without_release(self):
        from processing.authority_lease import acquire
        grant=self.grant();signature=b.sign_grant(grant,self.secret);original=self.adapter.exchange
        def lost(*args):original(*args);raise OSError('synthetic response loss')
        with patch.object(self.adapter,'exchange',side_effect=lost):
            with self.assertRaises(OSError):self.broker.mutate(grant,signature,self.body)
        with self.assertRaises(b.BrokerHeld):
            b.inspect_coordinator_held_grant(self.owner_queue,b.LocalCoordinatorAuthority(self.owner_queue),grant['operationId'],self.secret)
        self.broker.close();self.owner_lease.close();self.owner_queue.close()
        lease=acquire(self.root/'owner-lease',self.root/'owner-queue')
        restarted=Queue(self.root/'owner-queue',node_ids=('cluster','main'),viewer_enabled=True)
        try:
            restarted.authority_lease=lease;witness=b.LocalCoordinatorAuthority(restarted)
            evidence=b.inspect_coordinator_held_grant(restarted,witness,grant['operationId'],self.secret)
            self.assertEqual(evidence['grant'],grant)
            self.assertEqual((evidence['nodeState'],evidence['engineState']),('unverified','unverified'))
            self.assertFalse(evidence['authorizesRelease'])
            self.assertEqual(restarted.get(self.job)['status'],'held')
            with self.assertRaises(b.BrokerHeld):
                b.inspect_coordinator_held_grant(restarted,witness,grant['operationId'],b'wrong-synthetic-key-'*3)
            self.assertIsNone(restarted.claim({'workerId':'viewer-dispatch','nodeId':'main','profiles':[VIEWER_PROFILE]}))
            lease.close()
            with self.assertRaises(b.BrokerHeld):
                b.inspect_coordinator_held_grant(restarted,witness,grant['operationId'],self.secret)
        finally:restarted.close();lease.close()

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
            self.assertIsNone(self.broker.db.execute('SELECT 1 FROM engine_observations').fetchone())
            stored=self.broker.db.execute('SELECT grant_json,signature FROM node_grants').fetchone()
            self.assertEqual(json.loads(stored['grant_json']),grant);self.assertEqual(stored['signature'],signature)

    def test_replaced_database_path_fails_before_engine_mutation(self):
        original=self.broker.path.with_name('retained-original.sqlite3')
        self.broker.path.rename(original)
        self.broker.path.touch(mode=0o600)
        grant=self.grant()
        with self.assertRaises(b.BrokerHeld):self.broker.mutate(grant,b.sign_grant(grant,self.secret),self.body)
        self.assertEqual(self.calls,[])
        self.assertTrue(original.is_file())

    def test_upload_response_loss_retains_started_and_refuses_replay_after_reopen(self):
        self.begin_upload()
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

    def test_closed_authority_lease_refuses_upload_before_intent(self):
        self.begin_upload();self.owner_lease.close()
        payload=b'synthetic-photo'
        upload=dict(self.grant(),operation='upload',fileName='photo.jpg',fileBytes=len(payload),
                    bodySha256=hashlib.sha256(payload).hexdigest(),contentType='application/octet-stream')
        signature=b.sign_grant(upload,self.secret);source,writer=socket.socketpair()
        try:
            with self.assertRaises(b.BrokerHeld):self.broker.upload(upload,signature,source)
            with self.assertRaises(b.BrokerHeld):self.broker.lookup_upload(upload,signature)
        finally:source.close();writer.close()
        self.assertEqual(self.broker.db.execute("SELECT COUNT(*) FROM operations WHERE operation='upload'").fetchone()[0],0)
        self.assertEqual(len(self.calls),1)

    def test_sender_rejects_store_outside_broker_witness_queue(self):
        self.begin_upload();payload=b'synthetic-photo'
        upload=dict(self.grant(),operation='upload',fileName='photo.jpg',fileBytes=len(payload),
                    bodySha256=hashlib.sha256(payload).hexdigest(),contentType='application/octet-stream')
        sender=b.LocalUploadSender()
        with self.assertRaises(b.BrokerHeld):
            sender.send(self.broker,upload,b.sign_grant(upload,self.secret),
                        store=SimpleNamespace(queue=object()),claim_token=self.owner_claim['claimToken'],secret=self.secret)
        self.assertEqual(sender._unreaped,[]);self.assertEqual(len(self.calls),1)

    def test_authority_loss_after_upload_prevents_confirmation(self):
        self.begin_upload();payload=b'synthetic-photo'
        upload=dict(self.grant(),operation='upload',fileName='photo.jpg',fileBytes=len(payload),
                    bodySha256=hashlib.sha256(payload).hexdigest(),contentType='application/octet-stream')
        signature=b.sign_grant(upload,self.secret);source,writer=socket.socketpair()
        try:
            writer.sendall(payload);writer.shutdown(socket.SHUT_WR)
            self.assertTrue(self.broker.upload(upload,signature,source)['success'])
        finally:source.close();writer.close()
        self.owner_lease.close()
        with self.assertRaises(b.BrokerHeld):
            self.broker.confirm_upload(upload,signature,b._sender_confirmation(upload,self.secret))
        self.assertEqual(self.broker.db.execute("SELECT state FROM operations WHERE operation='upload'").fetchone()[0],'awaiting_sender')

    def test_authority_lease_remains_held_through_upload_exchange(self):
        self.begin_upload();payload=b'synthetic-photo'
        upload=dict(self.grant(),operation='upload',fileName='photo.jpg',fileBytes=len(payload),
                    bodySha256=hashlib.sha256(payload).hexdigest(),contentType='application/octet-stream')
        signature=b.sign_grant(upload,self.secret);source,writer=socket.socketpair()
        writer.sendall(payload);writer.shutdown(socket.SHUT_WR)
        entered=threading.Event();release=threading.Event();closed=threading.Event();out=[]
        original=self.adapter.exchange_upload
        def pause(*args):
            entered.set()
            if not release.wait(3):raise RuntimeError('synthetic upload timed out')
            return original(*args)
        def send():
            try:out.append(self.broker.upload(upload,signature,source))
            except Exception as exc:out.append(exc)
        worker=threading.Thread(target=send)
        closer=threading.Thread(target=lambda:(self.owner_lease.close(),closed.set()))
        with patch.object(self.adapter,'exchange_upload',side_effect=pause):
            worker.start()
            try:
                self.assertTrue(entered.wait(3));closer.start();self.assertFalse(closed.wait(.2))
            finally:
                release.set();worker.join(5)
                if closer.ident is not None:closer.join(5)
                source.close();writer.close()
        self.assertFalse(worker.is_alive());self.assertFalse(closer.is_alive());self.assertTrue(closed.is_set())
        self.assertTrue(out[0]['success'])
        self.assertEqual(self.broker.db.execute("SELECT state FROM operations WHERE operation='upload'").fetchone()[0],'awaiting_sender')

    def test_accepted_upload_waits_for_authenticated_sender_confirmation(self):
        self.begin_upload()
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
        self.broker.confirm_upload(upload,signature,b._sender_confirmation(upload,self.secret))
        with self.owner_queue._db() as db:
            db.execute("UPDATE broker_intents SET grant_hash=? WHERE operation_id=?",('0'*64,upload['operationId']))
        with self.assertRaises(b.BrokerHeld):
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
        self.begin_upload()
        payload=b'synthetic-photo';path=self.root/'photo.jpg';path.write_bytes(payload);path.chmod(0o600)
        upload=dict(self.grant(),operation='upload',fileName='photo.jpg',fileBytes=len(payload),
                    bodySha256=hashlib.sha256(payload).hexdigest(),contentType='application/octet-stream')
        item=dict(name='photo.jpg',bytes=len(payload),sha256=upload['bodySha256'],path=str(path))
        sender=b.LocalUploadSender()
        with patch.object(SenderCompletion,'verify',side_effect=SmokeError('synthetic sender verification failure')), \
             patch.object(b.LocalUploadSender,'_issued_item',return_value=item):
            with self.assertRaises(SmokeError):sender.send(self.broker,upload,b.sign_grant(upload,self.secret),store=self.owner_store,claim_token=self.owner_claim['claimToken'],secret=self.secret)
        self.assertEqual(sender._unreaped,[])
        self.assertEqual(self.broker.db.execute("SELECT state FROM operations WHERE operation='upload'").fetchone()[0],'awaiting_sender')
        self.assertEqual(path.read_bytes(),payload)
        with patch.object(b.LocalUploadSender,'_issued_item',return_value=item), \
             patch('processing.guarded_native_engine.start_guarded_sender',side_effect=AssertionError('uncertain replay started producer')):
            with self.assertRaises(b.BrokerHeld):sender.send(self.broker,upload,b.sign_grant(upload,self.secret),store=self.owner_store,claim_token=self.owner_claim['claimToken'],secret=self.secret)

    def test_mismatched_stream_stays_started_without_retry(self):
        self.begin_upload()
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
        self.begin_upload()
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

    def test_other_local_queue_grant_reaches_its_node_gate_and_loopback_peer(self):
        queue=Queue(self.root/'coordinator',node_ids=('cluster','main'),viewer_enabled=True)
        node_path=self.root/'remote-node-gate'
        lease=attach_lease(queue,self.root/'other-lease')
        try:
            store=TaskStore(queue,self.root/'coordinator'/'viewer-tasks')
            with queue._db() as db: install_offline(db,enabled=True)
            b.install_upload_issuer_schema(queue)
            store.initialize(self.task,'Synthetic',[],None,'production')
            store.upload(self.task,'synthetic.jpg',io.BytesIO(b'x'),1)
            store.upload(self.task,'other.jpg',io.BytesIO(b'y'*65536),65536);store.commit(self.task)
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
                    store.update(self.task,phase='upload_pending')
                    upload_id=str(uuid.uuid4())
                    upload,upload_signature=b.issue_upload_grant(queue,store=store,job_id=claim['id'],claim_token=claim['claimToken'],
                        engine_id=self.engine,gate_generation=1,operation_id=upload_id,file_key='synthetic.jpg',secret=self.secret)
                    sender=b.LocalUploadSender()
                    stored=store.task(self.task)['files'][0]
                    original=Path(stored['path']);alternate=original.parent/('input-'+uuid.uuid4().hex)
                    alternate.write_bytes(original.read_bytes());alternate.chmod(0o600)
                    with queue._db() as db:db.execute("UPDATE viewer_files SET path=? WHERE uuid=? AND name_key='synthetic.jpg'",(alternate.name,self.task))
                    with self.assertRaises(b.BrokerHeld):
                        sender.send(remote,upload,upload_signature,store=store,claim_token=claim['claimToken'],secret=self.secret)
                    self.assertEqual(len(self.calls),1)
                    with queue._db() as db:db.execute("UPDATE viewer_files SET path=? WHERE uuid=? AND name_key='synthetic.jpg'",(original.name,self.task))
                    task_dir=original.parent;different_dir=store.root/('other-'+uuid.uuid4().hex)
                    different_dir.mkdir(mode=0o700);(different_dir/original.name).write_bytes(original.read_bytes())
                    with queue._db() as db:db.execute('UPDATE viewer_tasks SET directory=? WHERE uuid=?',(different_dir.name,self.task))
                    with self.assertRaises(b.BrokerHeld):
                        sender.send(remote,upload,upload_signature,store=store,claim_token=claim['claimToken'],secret=self.secret)
                    with queue._db() as db:db.execute('UPDATE viewer_tasks SET directory=? WHERE uuid=?',(task_dir.name,self.task))
                    old_root=store.root;other_root=self.root/'alternate-root';other_root.mkdir(mode=0o700)
                    other_dir=other_root/task_dir.name;other_dir.mkdir(mode=0o700)
                    (other_dir/original.name).write_bytes(original.read_bytes())
                    try:
                        store.root=other_root
                        with self.assertRaises(b.BrokerHeld):
                            sender.send(remote,upload,upload_signature,store=store,claim_token=claim['claimToken'],secret=self.secret)
                    finally:store.root=old_root
                    self.assertTrue(sender.send(remote,upload,upload_signature,store=store,claim_token=claim['claimToken'],secret=self.secret)['success'])
                    self.assertEqual(sender._unreaped,[])
                    self.assertEqual(len(self.calls),2)
                    self.assertIn(b'filename="synthetic.jpg"',self.calls[1][1])
                    self.assertIn(b'\r\n\r\nx\r\n',self.calls[1][1])
                    with patch('processing.guarded_native_engine.start_guarded_sender',side_effect=AssertionError('replay started producer')):
                        self.assertTrue(sender.send(remote,upload,upload_signature,store=store,claim_token=claim['claimToken'],secret=self.secret)['success'])
                    self.assertEqual(len(self.calls),2)
                    replay,replay_signature=b.issue_upload_grant(queue,store=store,job_id=claim['id'],claim_token=claim['claimToken'],
                        engine_id=self.engine,gate_generation=1,operation_id=upload_id,file_key='synthetic.jpg',secret=self.secret)
                    self.assertEqual((replay,replay_signature),(upload,upload_signature))
                    with self.assertRaises(b.BrokerHeld):
                        b.issue_upload_grant(queue,store=store,job_id=claim['id'],claim_token=claim['claimToken'],
                            engine_id=self.engine,gate_generation=1,operation_id=str(uuid.uuid4()),file_key='synthetic.jpg',secret=self.secret)
                    racing,racing_signature=b.issue_upload_grant(queue,store=store,job_id=claim['id'],claim_token=claim['claimToken'],
                        engine_id=self.engine,gate_generation=1,operation_id=str(uuid.uuid4()),file_key='other.jpg',secret=self.secret)
                    racing_item=next(item for item in store.task(self.task)['files'] if item['name_key']=='other.jpg')
                    from processing.guarded_native_engine import start_guarded_sender
                    other_owner=SimpleNamespace(_unreaped=[]);original_lookup=remote.lookup_upload
                    def complete_between_lookup_and_send(g,s):
                        self.assertIsNone(original_lookup(g,s))
                        deadline=time.monotonic()+5
                        other=start_guarded_sender(racing_item,g['operationId'],deadline,other_owner)
                        try:
                            remote.upload(g,s,other.source)
                            other.verify(deadline)
                            remote.confirm_upload(g,s,b._sender_confirmation(g,self.secret))
                        finally:other.stop()
                        return None
                    def replay_sender(*args,**kwargs):
                        started=start_guarded_sender(*args,**kwargs)
                        started.verify=lambda _deadline:(_ for _ in ()).throw(AssertionError('replay verified an aborted producer'))
                        return started
                    with patch.object(remote,'lookup_upload',side_effect=complete_between_lookup_and_send), \
                         patch('processing.guarded_native_engine.start_guarded_sender',side_effect=replay_sender):
                        self.assertTrue(sender.send(remote,racing,racing_signature,store=store,claim_token=claim['claimToken'],secret=self.secret)['success'])
                    self.assertEqual(other_owner._unreaped,[]);self.assertEqual(sender._unreaped,[])
                    self.assertEqual(len(self.calls),3)
                    lease.close()
                    with self.assertRaises(b.BrokerHeld):
                        sender.send(remote,upload,upload_signature,store=store,claim_token=claim['claimToken'],secret=self.secret)
                    self.assertEqual(len(self.calls),3)
                finally:remote.close()
        finally:queue.close();lease.close()


if __name__=='__main__':unittest.main()
