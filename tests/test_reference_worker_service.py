"""Canonical factory/service proof; synthetic TLS/native fixture, no installed workers."""
from contextlib import closing, contextmanager, redirect_stdout
from datetime import datetime
import io
import json
import os
from pathlib import Path
import socket
import sqlite3
import ssl
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import HTTPSHandler, ProxyHandler, Request, build_opener
import uuid

from ops import reference_worker_daemon as daemon
from ops.native_node_fixture import NativeNodeFixture
from processing.node_gate import NodeGate, GateBusy
from processing.prototype import Queue
from processing.reference_control import ReferenceControlAPI
from processing.reference_result_proxy import ResultProxy
from processing.reference_store import ReferenceStore
from tests import test_reference_parent_guard as tls
from tests import test_reference_joint_flow as joint


def free_port():
    with socket.socket() as connection:
        connection.bind(('127.0.0.1', 0))
        return connection.getsockname()[1]


def private_config(root, cert, key, native, authority, *, coordinator='https://127.0.0.1:19998',
        broker='https://127.0.0.1:19997', node='cluster', worker='service-worker', instance='service-instance',
        bearer='synthetic-service-worker-'+'a'*32, reader='synthetic-service-reader-'+'b'*32, gate='service-engine',
        gate_origin='http://127.0.0.1:18080'):
    return dict(schemaVersion=1, stateRoot=str(root/'service-state'), scratchRoot=str(root/'service-scratch'),
        nodeId=node, workerId=worker, workerInstanceId=instance, authorityId=authority,
        engineId=gate, gateGeneration=1, gateCoordinatorOrigin=gate_origin, coordinatorOrigin=coordinator,
        brokerOrigin=broker, engineOrigin=native.origin, engineToken='', engineVersion=native.read('/info')['engineVersion'],
        workerBearer=bearer, resultReadBearer=reader, caFile=str(cert), serverCertFile=str(cert), serverKeyFile=str(key),
        listenHost='127.0.0.1', listenPort=free_port(), outputBudget=4194304, safetyFloor=0,
        transferTtl=240, cleanupGrace=0, pollSeconds=0.1, shareResults=True,
        cleanupEnabled=True, engineCleanupEnabled=True, allowedEnvironments=['staging'],
        serviceProfile=dict(schemaVersion=1, mode='odm_only'))


@unittest.skipUnless(os.name == 'posix', 'actual Linux ownership and private TLS service')
class WorkerServiceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls): tls.ParentGuardTests.setUpClass.__func__(cls)

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(); self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name); self.root.chmod(0o700)
        scoped = patch('processing.node_gate.canonical_gate_path', return_value=self.root/'gate')
        scoped.start(); self.addCleanup(scoped.stop)
        self.native = NativeNodeFixture(self.root/'native.sqlite', synthetic_enabled=True).start()
        self.addCleanup(self.native.close)
        self.authority = str(uuid.uuid4())
        with NodeGate('service-engine') as gate: gate.provision('cluster', 'http://127.0.0.1:18080', self.authority)
        from processing.viewer_service import NativeEngine
        self.config = private_config(self.root, self.cert, self.key, NativeEngine(self.native.origin), self.authority)
        self.path = self.root/'private.json'; self.write_config()

    def write_config(self):
        self.path.write_text(json.dumps(self.config)); self.path.chmod(0o600)

    def build(self):
        self.loaded_config = daemon.load_config(str(self.path))
        self.driver, self.server = daemon.build_worker(self.loaded_config)
        self.addCleanup(self.server.server_close)
        return self.driver, self.server

    def start(self):
        self.build(); self.service = daemon.WorkerService(self.driver, self.server, poll_seconds=0.1)
        self.stop = threading.Event(); self.outcomes = []
        self.thread = threading.Thread(target=lambda:self.outcomes.append(self.service.run(self.stop)))
        self.thread.start()
        def close():
            self.stop.set(); self.thread.join(10); self.assertFalse(self.thread.is_alive())
        self.addCleanup(close)
        until = time.monotonic()+3
        while self.service.health()['state'] != 'ready' and time.monotonic() < until: self.stop.wait(0.01)
        self.assertEqual(self.service.health()['state'], 'ready')

    def health(self, token=None):
        context = ssl.create_default_context(cafile=str(self.cert))
        headers = {} if token is None else {'Authorization':'Bearer '+token}
        url = 'https://127.0.0.1:'+str(self.server.server_port)+daemon.WorkerHealthAPI.PATH
        try:
            with build_opener(ProxyHandler({}), HTTPSHandler(context=context)).open(Request(url, headers=headers), timeout=3) as response:
                return response.status, json.loads(response.read(8192))
        except HTTPError as error:
            try: return error.code, json.loads(error.read(8192))
            finally: error.close()

    def test_profile_is_closed_odm_only_and_check_config_opens_no_state(self):
        original = json.loads(json.dumps(self.config))
        for change in ('version','mode','routing','unknown','path','reflection'):
            self.config = json.loads(json.dumps(original))
            if change == 'version': self.config['serviceProfile']['schemaVersion'] = True
            elif change == 'mode': self.config['serviceProfile']['mode'] = 'treedar'
            elif change == 'routing': self.config['executionRouting'] = dict(schemaVersion=1, mode='per-job-v1', treedar=None)
            elif change == 'path': self.config['engineVersion'] = '/private/not-a-version'
            elif change == 'reflection': self.config['workerId'] = self.config['workerBearer']
            else: self.config['serviceProfile']['unknown'] = True
            self.write_config()
            with self.subTest(change=change), self.assertRaisesRegex(ValueError, 'invalid private worker configuration'):
                daemon.load_config(str(self.path))
        self.config = original; self.write_config()
        output = io.StringIO()
        with patch.object(daemon, 'build_worker', side_effect=AssertionError('must not open worker')), redirect_stdout(output):
            self.assertEqual(daemon.main(['--config', str(self.path), '--check-config']), 0)
        self.assertEqual(output.getvalue(), 'reference worker configuration valid\n')
        self.assertFalse(Path(self.config['stateRoot']).exists())

    def test_actual_factory_private_health_has_real_empty_state_and_role_boundaries(self):
        self.start()
        self.assertEqual(self.health()[0], 401)
        self.assertEqual(self.health(self.config['workerBearer'])[0], 401)
        status, body = self.health(self.config['resultReadBearer'])
        deadline = time.monotonic()+3
        while body.get('workState') == 'busy' and time.monotonic() < deadline:
            self.stop.wait(0.01); status, body = self.health(self.config['resultReadBearer'])
        self.assertEqual(status, 200); self.assertEqual(body['scope'], 'service_liveness')
        self.assertEqual(body['identityScope'], 'configured_service')
        for key in ('nodeId','authorityId','workerId','workerInstanceId','engineId','gateGeneration','engineVersion'):
            self.assertEqual(body[key], self.config[key])
        self.assertEqual(body['workState'], 'empty'); self.assertEqual(body['retainedJobs'], 0)
        self.assertEqual(body['pendingJobs'], 0); self.assertEqual(body['awaitingAcceptance'], 0)
        wire = json.dumps(body)
        self.assertNotIn(self.config['workerBearer'], wire); self.assertNotIn(self.config['resultReadBearer'], wire)
        self.assertNotIn(str(self.root), wire)
        self.loaded_config['workerId'] = 'changed-but-not-applied'
        self.assertEqual(self.health(self.config['resultReadBearer'])[1]['workerId'], self.config['workerId'])
        with self.driver._lock:
            _, busy = self.health(self.config['resultReadBearer'])
            self.assertEqual(busy['workState'], 'busy'); self.assertIsNone(busy['retainedJobs'])

    def test_health_sql_snapshot_is_readonly_and_does_not_replay_driver(self):
        self.build(); before = self.driver.path.read_bytes(); inventory = set(self.driver.path.parent.iterdir())
        with patch.object(self.driver, '_db', side_effect=AssertionError('no writable driver transaction')):
            self.assertEqual(daemon._work_health(self.driver), dict(workState='empty', retainedJobs=0, pendingJobs=0, awaitingAcceptance=0))
        self.assertEqual(before, self.driver.path.read_bytes()); self.assertEqual(inventory, set(self.driver.path.parent.iterdir()))

    def test_health_before_service_binding_grants_no_ready_state(self):
        self.build()
        api = self.server.worker_health_api
        response = api.handle('GET', api.PATH, {'Authorization':'Bearer '+self.config['resultReadBearer']}, protected_transport=True)
        self.assertEqual(response.status, 503)
        self.assertEqual(response.body, {'error':'worker_service_not_started'})

    def test_health_is_absent_without_opt_in_and_factory_owner_releases_after_join(self):
        self.config.pop('serviceProfile'); self.write_config(); self.start()
        self.assertEqual(self.health(self.config['resultReadBearer'])[0], 404)
        with self.assertRaises(Exception): daemon.build_worker(daemon.load_config(str(self.path)))
        self.stop.set(); self.thread.join(10)
        self.assertEqual(self.outcomes, [0]); self.assertFalse(self.thread.is_alive())
        driver, server = daemon.build_worker(daemon.load_config(str(self.path)))
        server.server_close()
        self.assertEqual(driver.worker.node_id, 'cluster')


@unittest.skipUnless(os.name == 'posix' and os.environ.get('VIEWER_REFERENCE_ROOT'), 'opt-in pinned Viewer867/Node24 service assembly oracle')
class CanonicalWorkerJointTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls): joint.ReferenceJointFlowTests.setUpClass.__func__(cls)
    serve = joint.ReferenceJointFlowTests.serve
    start_node = joint.ReferenceJointFlowTests.start_node
    stop_node = joint.ReferenceJointFlowTests.stop_node
    call = joint.ReferenceJointFlowTests.call
    assert_retained_before_acceptance = joint.ReferenceJointFlowTests.assert_retained_before_acceptance

    def setUp(self):
        # Reuse only pinned transport scaffolding; its unused manual driver never steps.
        joint.ReferenceJointFlowTests.setUp(self)
        self.unused_driver = self.driver
        self.config = private_config(self.root, self.cert, self.key, self.native, self.queue.authority_id,
            coordinator=self.control_origin, broker=self.node_origin, worker='joint-worker', instance='joint-instance',
            bearer=joint.WORKER, reader=joint.READER, gate='joint-engine', gate_origin=self.gate_origin)
        path = self.root/'service-private.json'; path.write_text(json.dumps(self.config)); path.chmod(0o600)
        self.driver, self.service_server = daemon.build_worker(daemon.load_config(str(path)))
        self.stack.callback(self.service_server.server_close)
        self.worker, self.ledger, self.outbox = self.driver.worker, self.driver.ledger, self.driver.outbox
        self.outbox_now = time.time(); self.outbox.clock = lambda:self.outbox_now
        @contextmanager
        def admit(binding):
            if self.streams or binding['byteSize'] > 4194304: raise ValueError('synthetic_capacity')
            self.streams += 1
            try: yield
            finally: self.streams -= 1
        result_origin = 'https://127.0.0.1:'+str(self.service_server.server_port)
        self.control_options['result_proxy'] = ResultProxy({'cluster':result_origin}, worker_bearers={'cluster':joint.READER},
            admit_read=admit, ssl_context=self.tls)
        self.control = ReferenceControlAPI(self.store, **self.control_options)
        dispatch = self.native_fixture._dispatch
        def controlled_fixture(method, route, headers, body):
            result = dispatch(method, route, headers, body)
            if method == 'POST' and route.startswith('/task/new/commit/') and result[0] == 200:
                with self.native_fixture._db() as db: db.execute('UPDATE tasks SET status=20 WHERE uuid=?', (route.rsplit('/',1)[1],))
            return result
        self.native_fixture._dispatch = controlled_fixture
        self.service = daemon.WorkerService(self.driver, self.service_server, poll_seconds=0.1)
        self.stop = threading.Event(); self.outcomes = []
        self.thread = threading.Thread(target=lambda:self.outcomes.append(self.service.run(self.stop)))
        self.thread.start()
        def close_service():
            self.stop.set(); self.thread.join(10); self.assertFalse(self.thread.is_alive())
        self.stack.callback(close_service)

    def wait_stage(self, target, *, pump=True):
        deadline = time.monotonic()+12
        while time.monotonic() < deadline:
            if pump: self.call('pump')
            with self.driver._db() as db:
                row = db.execute('SELECT stage,data,claim_request FROM driver_jobs WHERE job=?', (self.job,)).fetchone()
            if row:
                data = json.loads(row['data']); claim = data.get('claim')
                if claim:
                    identity = {key:claim[key] for key in ('jobId','generation','claimToken','manifestSha256','pipelineDigest')}
                    identity['claimRequestId'] = row['claim_request']
                    if self.original_claim is None: self.original_claim = identity
                    else: self.assertEqual(self.original_claim, identity)
                if row['stage'] in ('executing','delivering','awaiting_acceptance','completed'):
                    receipt = self.worker._row(self.job)['receipt']
                    if self.original_receipt is None: self.original_receipt = receipt
                    else: self.assertEqual(self.original_receipt, receipt)
                try:
                    with NodeGate('joint-engine') as gate:
                        held = gate.snapshot()['reservation']
                        if held:
                            if self.original_receipt is None: self.original_receipt = held['receiptId']
                            else: self.assertEqual(self.original_receipt, held['receiptId'])
                except GateBusy:
                    pass  # The actual service may hold its gate during this observational poll.
                if row['stage'] == target: return data
            self.outbox_now += 10
            self.stop.wait(0.02)
        self.fail('canonical service did not reach '+target+'; '+repr(self.service.health()))

    def finish(self):
        result = self.call('ingest'); self.assertTrue(result['acknowledged']); self.assertFalse(result['published'])
        self.wait_stage('completed')
        deadline = time.monotonic()+5
        while (self.native_fixture.counts().get('remove',0) != 1 or list(self.ledger.root.glob('jobs/*/all.zip'))) and time.monotonic() < deadline:
            self.stop.wait(0.02)
        self.assertEqual(self.native_fixture.counts()['remove'], 1)
        self.assertEqual(list(self.ledger.root.glob('jobs/*/all.zip')), [])
        inputs = list(self.ledger.root.glob('jobs/*/*.input')); self.assertEqual(len(inputs), 1); self.assertEqual(inputs[0].read_bytes(), b'abc')
        self.assertEqual((self.viewer_state/'input/Survey_001.JPG').read_bytes(), b'abc')
        self.assertEqual(list((self.viewer_state/'results').glob('*.zip'))[0].read_bytes(), joint.ARCHIVE)
        with self.unused_driver._db() as db: self.assertEqual(db.execute('SELECT COUNT(*) FROM driver_jobs').fetchone()[0], 0)
        self.assertEqual({key:self.native_fixture.counts()[key] for key in ('initialize','upload','commit','download','remove')},
            {'initialize':1,'upload':1,'commit':1,'download':1,'remove':1})
        with closing(sqlite3.connect(self.viewer_state/'acceptance.sqlite')) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM acceptances').fetchone()[0], 1)
            self.assertEqual(db.execute('SELECT acked FROM ack_outbox').fetchone()[0], 1)
        with closing(sqlite3.connect(self.ledger.path)) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reservations').fetchone()[0], 1)
        url = 'https://127.0.0.1:'+str(self.service_server.server_port)+daemon.WorkerHealthAPI.PATH
        deadline = time.monotonic()+3
        while True:
            with build_opener(ProxyHandler({}), HTTPSHandler(context=self.tls)).open(Request(url,
                    headers={'Authorization':'Bearer '+joint.READER}), timeout=3) as response:
                health = json.loads(response.read(8192))
            if health.get('workState') != 'busy' or time.monotonic() >= deadline: break
            self.stop.wait(0.01)
        self.assertEqual(health['workState'], 'idle'); self.assertEqual(health['retainedJobs'], 1)
        self.assertEqual(health['pendingJobs'], 0); self.assertEqual(health['awaitingAcceptance'], 0)

    def test_actual_canonical_factory_service_runs_and_acceptance_precedes_cleanup(self):
        self.wait_stage('executing')
        with self.native_fixture._db() as db: db.execute('UPDATE tasks SET status=40 WHERE uuid=?', (self.job,))
        self.wait_stage('awaiting_acceptance'); self.assert_retained_before_acceptance()
        self.finish()

    def test_expired_original_grant_after_commit_and_coordinator_outage_keeps_original_work(self):
        started = self.wait_stage('executing'); original = dict(self.original_claim)
        expires = datetime.fromisoformat(started['reservation']['expiresAt'].replace('Z','+00:00')).timestamp()
        self.driver.clock = lambda:expires+3600
        self.driver.broker.clock = lambda:expires+3600
        _, coordinator_thread, stop_coordinator = self.servers[self.control_origin]
        server = self.servers[self.control_origin][0]; port = server.server_port
        authority = self.queue.authority_id
        stop_coordinator(); self.assertFalse(coordinator_thread.is_alive()); self.queue.close()
        with self.assertRaises(HTTPError) as unavailable: self.call('pump')
        try:
            self.assertEqual(unavailable.exception.code, 409)
            self.assertEqual(json.loads(unavailable.exception.read()), {'error':'transport_error'})
        finally: unavailable.exception.close()
        with self.native_fixture._db() as db: db.execute('UPDATE tasks SET status=40 WHERE uuid=?', (self.job,))
        sealed = self.wait_stage('delivering', pump=False); outbox_id = sealed['outboxId']; payload = self.outbox.get(outbox_id)['payload']
        self.assertEqual(sealed['grant'], started['grant'])
        deadline = time.monotonic()+4
        while self.outbox.get(outbox_id)['attempts'] < 2 and time.monotonic() < deadline:
            self.outbox_now += 10; self.stop.wait(0.04)
        self.assertGreaterEqual(self.outbox.get(outbox_id)['attempts'], 2)
        self.assertEqual(self.outbox.get(outbox_id)['payload'], payload)
        self.assert_retained_before_acceptance()
        self.queue = Queue(self.root/'coordinator', reference_enabled=True); self.stack.callback(self.queue.close)
        self.store = ReferenceStore(self.queue); self.assertEqual(self.queue.authority_id, authority)
        self.control = ReferenceControlAPI(self.store, **self.control_options)
        self.assertEqual(self.serve(self.control_endpoint, port=port), self.control_origin)
        delivered = self.wait_stage('awaiting_acceptance'); self.assertEqual(self.original_claim, original)
        self.assertEqual(delivered['grant'], started['grant'])
        self.assertEqual(self.outbox.get(outbox_id)['payload'], payload)
        self.finish()
        with closing(sqlite3.connect(self.viewer_state/'broker.sqlite')) as db:
            grant = db.execute('SELECT metadata,used_bytes,reserved_bytes,in_flight FROM reference_grants').fetchone()
        self.assertEqual(json.loads(grant[0])['reservationId'], started['reservation']['reservationId'])
        self.assertEqual(json.loads(grant[0])['expiresAt'], started['reservation']['expiresAt'])
        self.assertEqual(json.loads(grant[0])['byteBudget'], 9); self.assertEqual(grant[1:], (3,0,0))


if __name__ == '__main__': unittest.main()
