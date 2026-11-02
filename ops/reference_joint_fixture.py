"""Synthetic transport fixture. No real engine, production token or project data.

Run explicitly in disposable containers with an internal-only network and
generated test TLS certificates. This is not an operational deployment driver.
"""
import argparse
from contextlib import contextmanager
import hashlib
import hmac
import io
import json
import os
from pathlib import Path
import sqlite3
import ssl
import threading

from processing.node_gate import NodeGate
from processing.prototype import Queue
from processing.reference_api import Response
from processing.reference_control import ReferenceControlAPI
from processing.reference_engine_worker import ReferenceEngineWorker
from processing.reference_http import create_reference_http_server
from processing.reference_result_api import ReferenceResultAPI
from processing.reference_result_proxy import ResultProxy
from processing.reference_results import ReferenceResults
from processing.reference_store import ReferenceStore
from processing.reference_transfer import BrokerClient
from processing.scratch_cleanup import ScratchCleanupExecutor
from processing.scratch_policy import ScratchLedger
from processing.worker_outbox import WorkerOutbox

VIEWER_TOKEN = 'synthetic-reference-staging-token'
WORKER_TOKEN = 'synthetic-reference-worker-token'
READ_TOKEN = 'synthetic-reference-result-read-token'
FIXTURE_TOKEN = 'synthetic-reference-fixture-token'


class SyntheticEngine:
    synthetic_fixture = True
    origin = 'http://127.0.0.1:3001'

    def __init__(self, path):
        self.path = path
        with self._db() as db:
            db.execute('CREATE TABLE IF NOT EXISTS calls (action TEXT,job TEXT)')

    @contextmanager
    def _db(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.execute('PRAGMA synchronous=FULL')
        try:
            with db:
                yield db
        finally:
            db.close()

    def _record(self, action, job):
        with self._db() as db:
            if action in ('initialize', 'commit') and db.execute(
                    'SELECT 1 FROM calls WHERE action=? AND job=?', (action, job)).fetchone():
                raise ValueError('synthetic engine mutation repeated')
            db.execute('INSERT INTO calls VALUES(?,?)', (action, job))

    def initialize(self, task):
        self._record('initialize', task['engine_uuid'])

    def upload(self, job, item):
        if Path(item['path']).read_bytes() != b'abc':
            raise ValueError('synthetic input mismatch')
        self._record('upload', job)

    def action(self, action, job):
        if action != 'commit':
            raise ValueError('unsupported synthetic action')
        self._record(action, job)

    def read(self, route):
        job = route.split('/')[2]
        with self._db() as db:
            completed = db.execute("SELECT 1 FROM calls WHERE action='commit' AND job=?", (job,)).fetchone()
        return {'uuid': job, 'status': {'code': 40 if completed else 10}}

    def download(self, job):
        self._record('download', job)
        result = io.BytesIO(b'PK\x05\x06' + bytes(18))
        result.headers = {'Content-Length': '22'}
        return result

    def counts(self):
        with self._db() as db:
            return dict(db.execute('SELECT action,count(*) FROM calls GROUP BY action'))


class FixtureAPI:
    def __init__(self, control, store, engine, ledger):
        self.control, self.store, self.engine, self.ledger = control, store, engine, ledger
        self.driver = None
        self.lock = threading.Lock()

    def handle(self, method, path, headers, body=b'', *, protected_transport=False):
        if not path.startswith('/fixture/'):
            return self.control.handle(method, path, headers, body, protected_transport=protected_transport)
        supplied = headers.get('Authorization', '')
        if not protected_transport or not hmac.compare_digest(supplied, 'Bearer ' + FIXTURE_TOKEN):
            return Response(401, {'error': 'unauthorized'})
        if self.driver is None:
            return Response(503, {'error': 'fixture_initializing'})
        if not self.lock.acquire(blocking=False):
            return Response(409, {'error': 'fixture_busy'})
        try:
            if method == 'POST' and path == '/fixture/step' and body == b'{}':
                return Response(200, self.driver.step())
            if method == 'GET' and path == '/fixture/state':
                return Response(200, {'syntheticFixture': True, 'jobs': self.store.list(),
                    'engineCalls': self.engine.counts(), 'cleanupCandidates': len(self.ledger.cleanup_candidates()),
                    'retainedArchiveCount': len(list((self.ledger.root / 'jobs').glob('*/all.zip')))})
            if method == 'POST' and path == '/fixture/cleanup' and body == b'{}':
                count = 0
                executor = ScratchCleanupExecutor(self.ledger)
                for item in self.ledger.cleanup_candidates():
                    executor.execute(item['reservation'], item['id'], generation=item['generation'],
                        sha256=item['sha'], byte_size=item['bytes'], relative_path=item['path'])
                    count += 1
                return Response(200, {'cleanedObjects': count, 'published': False})
            return Response(404, {'error': 'not_found'})
        finally:
            self.lock.release()


def build_operational_fixture(root, tls, queue, store, engine_id, origin):
    """Actual runtime/daemon builders against a synthetic local HTTP engine."""
    from ops.native_node_fixture import NativeNodeFixture
    from ops.reference_worker_daemon import build_worker, load_config
    from processing.config import Config
    from processing.reference_runtime import ReferenceRuntime, load_reference_settings
    backend = NativeNodeFixture(root/'native-node.sqlite',port=3001,synthetic_enabled=True)
    backend.start()
    worker_config = dict(schemaVersion=1,stateRoot=str(root/'operational-worker'),scratchRoot=str(root/'scratch'),
        nodeId='cluster',workerId='worker',workerInstanceId='operational-joint-instance',authorityId=queue.authority_id,
        engineId=engine_id,gateGeneration=1,gateCoordinatorOrigin=origin,coordinatorOrigin=origin,
        brokerOrigin='https://viewer:9444',engineOrigin=backend.origin,engineToken='',workerBearer=WORKER_TOKEN,
        resultReadBearer=READ_TOKEN,caFile=str(tls/'cert.pem'),serverCertFile=str(tls/'cert.pem'),
        serverKeyFile=str(tls/'key.pem'),listenHost='0.0.0.0',listenPort=9445,outputBudget=1024*1024,
        safetyFloor=1024*1024,transferTtl=240,cleanupGrace=0,pollSeconds=0.1,
        engineVersion='synthetic-transport-only',shareResults=True,allowedEnvironments=['staging'],cleanupEnabled=False)
    def private_json(path, value):
        descriptor = os.open(path,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
        with os.fdopen(descriptor,'w') as target:
            json.dump(value,target)
    worker_path = root/'operational-worker.json'
    if not worker_path.exists():
        private_json(worker_path,worker_config)
    driver, result_server = build_worker(load_config(str(worker_path)))
    config = Config(api_token='synthetic-legacy-api-token-00000000',local_node_url=backend.origin,
        main_node_url=backend.origin,local_max_photos=1000,local_max_input_bytes=20*1024**3,
        prototype_state_dir=str(root/'coordinator'),prototype_node_ids=('cluster','main'),treedar_node_ids=('main',))
    settings = dict(schemaVersion=1,listener=dict(host='0.0.0.0',port=9443,certificate=str(tls/'cert.pem'),
        privateKey=str(tls/'key.pem'),clientCA=str(tls/'cert.pem')),viewerBearers={'staging':VIEWER_TOKEN},
        workers={'cluster':dict(bearer=WORKER_TOKEN,workerId='worker',instanceId='operational-joint-instance',
            resultOrigin='https://processing:9445',resultBearer=READ_TOKEN),
            'main':dict(bearer='synthetic-reference-main-worker-token',workerId='main-worker',instanceId='main-joint-instance',
            resultOrigin='https://processing:9445',resultBearer='synthetic-reference-main-result-read-token')},
        streamBudget=dict(maxConcurrent=2,maxObjectBytes=1024*1024))
    runtime_path = root/'operational-runtime.json'
    if not runtime_path.exists():
        private_json(runtime_path,settings)
    runtime = ReferenceRuntime(queue,config,load_reference_settings(str(runtime_path),config))
    api = FixtureAPI(runtime.api,store,backend,driver.ledger)
    api.driver = driver
    runtime.api = api
    runtime.start()
    threading.Thread(target=result_server.serve_forever,daemon=True).start()
    return backend,runtime,result_server


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--synthetic', action='store_true', required=True)
    parser.add_argument('--state-root', required=True)
    parser.add_argument('--cert-dir', required=True)
    parser.add_argument('--operational-driver',action='store_true')
    args = parser.parse_args()
    root, tls = Path(args.state_root), Path(args.cert_dir)
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    scratch = root / 'scratch'
    scratch.mkdir(mode=0o700, exist_ok=True)
    Path(os.environ['XDG_STATE_HOME']).mkdir(parents=True, mode=0o700, exist_ok=True)
    server_tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_tls.load_cert_chain(tls / 'cert.pem', tls / 'key.pem')
    client_tls = ssl.create_default_context(cafile=str(tls / 'cert.pem'))
    queue = Queue(root / 'coordinator', reference_enabled=True, node_ids=('cluster', 'main'), treedar_node_ids=('main',))
    store = ReferenceStore(queue)
    engine_id, origin = 'synthetic-reference-joint-engine', 'https://processing:9443'
    with NodeGate(engine_id) as gate:
        state = gate._load()
        if state is None:
            gate.provision('cluster', origin, queue.authority_id)
        elif any(state[k] != v for k,v in dict(nodeId='cluster', coordinatorOrigin=origin,
                                               authorityId=queue.authority_id).items()):
            raise ValueError('synthetic gate binding changed')
    if args.operational_driver:
        backend,runtime,result_server = build_operational_fixture(root,tls,queue,store,engine_id,origin)
        print('synthetic operational runtime fixture ready',flush=True)
        try:
            threading.Event().wait()
        finally:
            result_server.shutdown()
            result_server.server_close()
            runtime.close()
            backend.close()
            queue.close()
        return
    ledger = ScratchLedger(root / 'scratch.sqlite', scratch, safety_floor_bytes=1024*1024, cleanup_grace_seconds=0)
    outbox = WorkerOutbox(root / 'outbox.sqlite')
    engine = SyntheticEngine(root / 'engine.sqlite')
    worker = ReferenceEngineWorker(root / 'worker.sqlite', ledger=ledger, outbox=outbox, engine=engine,
        engine_id=engine_id, node_id='cluster', authority_id=queue.authority_id, coordinator_origin=origin,
        gate_generation=1, worker_id='worker', engine_version='synthetic-transport-only')
    read_slots = threading.BoundedSemaphore(2)
    @contextmanager
    def admit_read(binding):
        if binding['byteSize'] > 1024*1024 or not read_slots.acquire(blocking=False):
            raise ValueError('synthetic read budget unavailable')
        try:
            yield
        finally:
            read_slots.release()
    proxy = ResultProxy(worker_origins={'cluster':'https://processing:9445'},
        worker_bearers={'cluster':READ_TOKEN}, admit_read=admit_read, ssl_context=client_tls)
    fixture = json.loads((Path(__file__).resolve().parents[1] / 'contracts/reference-v1.contract.json').read_text())
    control = ReferenceControlAPI(store, viewer_bearers={VIEWER_TOKEN:'staging'},
        worker_bearers={WORKER_TOKEN:dict(node_id='cluster',worker_id='worker',worker_instance_id='joint-instance')},
        capability_fingerprint=fixture['capabilities']['capabilityFingerprint'],
        options_fingerprint=fixture['capabilities']['engines']['odm']['optionsFingerprint'], result_proxy=proxy)
    api = FixtureAPI(control, store, engine, ledger)
    result_api = ReferenceResultAPI(ReferenceResults(worker), bearer_readers={READ_TOKEN:dict(
        node_id='cluster', worker_id='worker', environments=('staging',), share_policy=lambda metadata: True)})
    servers = [create_reference_http_server(api,'0.0.0.0',9443,ssl_context=server_tls),
               create_reference_http_server(result_api,'0.0.0.0',9445,ssl_context=server_tls)]
    for server in servers:
        threading.Thread(target=server.serve_forever, daemon=True).start()
    from ops.reference_joint_driver import JointDriver
    api.driver = JointDriver(store, control.worker, worker, ledger, outbox,
        BrokerClient('https://viewer:9444', ssl_context=client_tls), origin,
        WORKER_TOKEN, 'worker', 'cluster', client_tls, state_path=root / 'driver.sqlite', synthetic_enabled=True)
    print('synthetic joint fixture ready', flush=True)
    try:
        threading.Event().wait()
    finally:
        for server in servers:
            server.shutdown()
            server.server_close()
        queue.close()


if __name__ == '__main__':
    main()
