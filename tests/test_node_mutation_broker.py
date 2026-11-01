"""Owned loopback NodeODM peer and persistent node gate; retained test roots."""
import hashlib
import io
import json
from pathlib import Path
import tempfile
import threading
import unittest
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

from processing import node_mutation_broker as b
from processing.engine_ingress_guard import BackendPin
from processing.node_gate import NodeGate
from processing.prototype import Queue, VIEWER_PROFILE
from processing.viewer_tasks import TaskStore


class NodeMutationBrokerTests(unittest.TestCase):
    def setUp(self):
        self.root=Path(tempfile.mkdtemp(prefix='lt-node-broker-test-')).resolve()
        self.engine='engine-synthetic';self.node='main';self.authority=str(uuid.uuid4());self.epoch=str(uuid.uuid4())
        self.origin='http://127.0.0.1:18080';self.secret=b'synthetic-key-'*4;self.calls=[]
        path=self.root/'gate';self.patches=[patch('processing.node_gate.canonical_gate_path',return_value=path),
            patch('processing.node_mutation_broker.canonical_gate_path',return_value=path),patch.object(b,'QUALIFIED',True),
            patch.object(b,'local_engine_identity',return_value=self.engine),patch.object(b,'local_backend_identity',return_value=None)]
        for item in self.patches:item.start()
        with NodeGate(self.engine) as gate:gate.provision(self.node,self.origin,self.authority)
        state=path/'mutation-broker';state.mkdir(mode=0o700)
        owner=self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*_):pass
            def do_POST(self):
                owner.calls.append((self.path,self.rfile.read(int(self.headers['Content-Length']))))
                value=json.dumps({'uuid':self.headers['set-uuid'],'success':True}).encode()
                self.send_response(200);self.send_header('Content-Length',str(len(value)));self.end_headers();self.wfile.write(value)
        self.server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        self.thread=threading.Thread(target=self.server.serve_forever);self.thread.start()
        self.adapter=b.LoopbackNodeODM(self.engine,BackendPin('a'*64,'sha256:'+'b'*64,self.server.server_port),'')
        self.config=dict(node_id=self.node,engine_id=self.engine,authority_id=self.authority,coordinator_epoch=self.epoch,
                         coordinator_origin=self.origin,gate_generation=1,secret=self.secret,adapter=self.adapter)
        self.broker=b.NodeMutationBroker(state,**self.config)
        self.task=str(uuid.uuid4());self.job=str(uuid.uuid4());self.body=b'synthetic-init'

    def tearDown(self):
        self.broker.close();self.server.shutdown();self.server.server_close();self.thread.join(5)
        self.assertFalse(self.thread.is_alive())
        for item in reversed(self.patches):item.stop()
        print('retained synthetic node broker root:',self.root,flush=True)

    def grant(self,operation='init',body=None):
        body=self.body if body is None else body
        return dict(authorityId=self.authority,nodeId=self.node,engineId=self.engine,coordinatorEpoch=self.epoch,
                    jobId=self.job,engineTaskId=self.task,environment='production',gateGeneration=1,
                    operationId=str(uuid.uuid4()),operation=operation,bodySha256=hashlib.sha256(body).hexdigest(),
                    contentType='application/octet-stream')

    def test_signed_local_init_replays_and_binds_job_task_epoch(self):
        grant=self.grant();signature=b.sign_grant(grant,self.secret)
        with self.assertRaises(b.BrokerHeld):self.broker.mutate(grant,'0'*64,self.body)
        reply=self.broker.mutate(grant,signature,self.body)
        self.assertEqual(reply['uuid'],self.task);self.assertEqual(len(self.calls),1)
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

    def test_lost_response_survives_broker_restart_without_replay(self):
        grant=self.grant();signature=b.sign_grant(grant,self.secret);original=self.adapter.exchange
        def lost(*args):original(*args);raise OSError('synthetic response loss')
        with patch.object(self.adapter,'exchange',side_effect=lost):
            with self.assertRaises(OSError):self.broker.mutate(grant,signature,self.body)
        self.broker.close();self.broker=b.NodeMutationBroker(self.root/'gate'/'mutation-broker',**self.config)
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
                finally:remote.close()
        finally:queue.close()


if __name__=='__main__':unittest.main()
