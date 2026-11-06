"""The two child processes behind test_reference_source_fetch_process_restart.

``coordinator`` serves the real Queue, store and control API over verified TLS.
``worker`` runs real ReferenceDriver.step() against it, one step per stdin line,
with the real journal, fenced ledger, range client, HTTP request path, node gate
and ReferenceEngineWorker. Each is killed with SIGKILL by the test, so only what
reached disk survives. The one stand-in is the engine: FileEngine answers from a
log that outlives both processes, so starts are counted across deaths. Its status
stays 20, so the run ends at executing with no archive. The driver is built
without __init__, which would demand a BrokerClient that a v2 fetch never uses.
"""
import json
import os
from pathlib import Path
import signal
import ssl
import sys
import threading
import time
from unittest.mock import patch
from urllib.request import HTTPSHandler, ProxyHandler, build_opener

from processing.prototype import Queue
from processing.reference_control import ReferenceControlAPI
from processing.reference_driver import ReferenceDriver, _NoRedirects
from processing.reference_engine_worker import ReferenceEngineWorker
from processing.reference_failure import FailureOutbox
from processing.reference_http import create_reference_http_server
from processing.reference_recovery import RecoveryNodeBinding
from processing.reference_store import PROFILE, ReferenceStore
from processing.reference_tls import verified_client_context
from processing.scratch_policy import ScratchLedger
from processing.source_range_transfer import SourceRangeClient
from processing.source_spool import SourceSpool
from processing.viewer_service import NativeEngine
from processing.worker_outbox import WorkerOutbox

WORKER_TOKEN = 'synthetic-worker-token-012345'
CLAIM_REQUEST = 'source-claim-process'
GATE_ORIGIN = 'http://127.0.0.1:18080'
ENGINE_LOG = 'engine-calls.log'
ENGINE_SCRIPT = 'engine-script.json'


class FileEngine(NativeEngine):
    """Records each call before it can die, so a killed process cannot lose a start."""

    def __init__(self, root, die_during):
        super().__init__('http://127.0.0.1:3001')
        self.log, self.die_during = Path(root) / ENGINE_LOG, die_during

    def call(self, name):
        with self.log.open('a') as stream:
            stream.write(name + '\n')
            stream.flush()
            os.fsync(stream.fileno())
        if name == self.die_during:
            os.kill(os.getpid(), signal.SIGKILL)

    def script(self):
        """A test's status and output, which outlive either process; empty means 20 and the usual output."""
        path = self.log.parent / ENGINE_SCRIPT
        return json.loads(path.read_text()) if path.exists() else {}

    def started(self):
        return self.log.exists() and 'initialize' in self.log.read_text().split()

    def initialize(self, task):
        self.task = task['engine_uuid']
        self.call('initialize')

    def upload(self, job, item):
        self.call('upload')

    def action(self, action, job):
        self.call(action)

    def output_page(self, identifier, *, line, limit):
        """Output exists only once a commit was sent, like a task that really began."""
        default = ['synthetic output'] if 'commit' in self.log.read_text().split() else []
        output = self.script().get('output', default)
        return output[line:line + limit], line + limit < len(output)

    def read(self, route, **_kwargs):
        if route == '/info':
            return {'engine': 'odx', 'engineVersion': 'pinned-engine-1', 'version': 'wrapper', 'taskQueueCount': 0, 'maxImages': None}
        if route == '/options':
            return [dict(name='numeric-probe', type='float', value=1, help='Numeric setting.')]
        job = json.loads(Path(self.log.parent / 'process-config.json').read_text())['jobId']
        if route == '/task/list':
            return [{'uuid': job}] if self.started() else []
        if not self.started():
            raise OSError('synthetic missing task')
        return {'uuid': job, 'status': {'code': self.script().get('code', 20)}}


def coordinator(config):
    root = Path(config['root'])
    with patch('processing.source_spool.os.path.ismount', return_value=True):
        queue = Queue(str(root))
        queue.allowed_profiles.add(PROFILE)
        store = ReferenceStore(queue)
        spool = SourceSpool(root / 'spool', mount_id='a' * 64, safety_floor_bytes=0, max_object_bytes=1024, chunk_bytes=3)
        control = ReferenceControlAPI(store, viewer_bearers={'synthetic-staging-token': 'staging'},
            worker_bearers={WORKER_TOKEN: dict(node_id='cluster', worker_id='worker', worker_instance_id='instance')},
            capability_fingerprint='a' * 64, options_fingerprint=config['optionsFingerprint'],
            allowed_odm_options={'numeric-probe'}, source_spool=spool, source_attestation_keys=config['trustedKeys'],
            authority_id=queue.authority_id, recovery_bindings={'cluster': RecoveryNodeBinding('cluster',
                'physical-cluster', queue.authority_id, 'http://127.0.0.1:18080', 1, 1000)})
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(config['cert'], config['key'])
        server = create_reference_http_server(control, port=config['port'], ssl_context=context)
        print('ready', flush=True)
        server.serve_forever()


def build_worker(config):
    root = Path(config['root'])
    origin = 'https://127.0.0.1:' + str(config['port'])
    context = verified_client_context(config['cert'])
    driver = object.__new__(ReferenceDriver)
    driver.path, driver.origin, driver._bearer = root / 'driver-journal.sqlite', origin, WORKER_TOKEN
    driver._combined_policy = driver._combined_policy_initial = driver._combined_preparation_initial = None
    driver._state_owner = driver.execution_router = driver.combined_preparation = None
    driver._active_step = driver._active_job = None
    driver._lock, driver.clock = threading.Lock(), time.time
    driver.node_id, driver.worker_id, driver.worker_instance_id = 'cluster', 'worker', 'instance'
    driver.ledger = ScratchLedger(root / 'ledger.sqlite', root / 'scratch', capacity_probe=lambda _: 10**9, safety_floor_bytes=0)
    driver.worker = ReferenceEngineWorker(root / 'worker.sqlite', ledger=driver.ledger,
        outbox=WorkerOutbox(root / 'outbox.sqlite'), engine=FileEngine(root, os.environ.get('SOURCE_ENGINE_DIE')),
        engine_id='engine', node_id='cluster', authority_id=config['authorityId'], coordinator_origin=GATE_ORIGIN,
        gate_generation=1, worker_id='worker', engine_version='pinned-engine-1', allowed_odm_options={'numeric-probe'},
        engine_probe=lambda _deadline: 'engine')
    driver.outbox, driver.failure_outbox = driver.worker.outbox, FailureOutbox(root / 'failures.sqlite')
    driver.allowed_environments, driver._option_definitions = frozenset(('production', 'staging')), {}
    driver.source_range_v2_enabled, driver.output_budget, driver.transfer_ttl = True, 10000, 300
    driver._open = build_opener(ProxyHandler({}), _NoRedirects(), HTTPSHandler(context=context)).open
    driver.source_range_client = SourceRangeClient(origin, root / 'worker-ledger.sqlite', ssl_context=context,
        renewal_origin=origin, renewal_headers=driver._source_renewal_headers)
    with driver._db() as db:
        db.execute('''CREATE TABLE IF NOT EXISTS driver_jobs (
          created INTEGER PRIMARY KEY AUTOINCREMENT, job TEXT UNIQUE NOT NULL,
          claim_request TEXT UNIQUE NOT NULL, stage TEXT NOT NULL, data TEXT NOT NULL)''')
        db.execute('CREATE TABLE IF NOT EXISTS readiness_tasks(uuid TEXT PRIMARY KEY)')
        fresh = db.execute('SELECT 1 FROM driver_jobs').fetchone() is None
    driver._refresh_options()
    if fresh:
        normal_claim = os.environ.get('SOURCE_NORMAL_CLAIM') == '1'
        claim = None
        if not normal_claim:
            _, claim = driver._request('POST', '/claim', {'jobId': config['jobId'], 'claimRequestId': CLAIM_REQUEST})
        with driver._db() as db:
            db.execute('INSERT INTO driver_jobs(job,claim_request,stage,data) VALUES(?,?,?,?)',
                (config['jobId'], CLAIM_REQUEST, 'claim_pending' if normal_claim else 'claimed',
                 json.dumps({} if normal_claim else {'claim': claim})))
    return driver


def die_around_recovery(driver, moment):
    """SIGKILL this process around a recovery POST, or once /phase committed processing."""
    real = driver._request

    def request(method, suffix, payload=None, **kwargs):
        recovering = method == 'POST' and suffix.endswith('/recover')
        reserving = method == 'POST' and suffix.endswith('/reservation')
        witnessing = method == 'POST' and suffix.endswith('/start-witness')
        if reserving and moment == 'before_reservation':
            os.kill(os.getpid(), signal.SIGKILL)
        if (recovering and moment == 'before_post') or (witnessing and moment == 'before_witness_post'):
            os.kill(os.getpid(), signal.SIGKILL)
        reply = real(method, suffix, payload, **kwargs)
        if method == 'POST' and ((reserving and moment == 'after_reservation')
                or (suffix == '/claim' and moment == 'after_claim')):
            os.kill(os.getpid(), signal.SIGKILL)
        if (recovering and moment == 'after_post') or (witnessing and moment == 'after_witness_post'):
            os.kill(os.getpid(), signal.SIGKILL)
        if moment == 'after_processing_phase' and suffix.endswith('/phase') and payload['phase'] == 'processing':
            os.kill(os.getpid(), signal.SIGKILL)
        return reply

    driver._request = request


def worker(config):
    patch('processing.node_gate.canonical_gate_path', return_value=Path(config['root']) / 'gate').start()
    driver = build_worker(config)
    if os.environ.get('SOURCE_FETCH_DIE'):
        die_around_recovery(driver, os.environ['SOURCE_FETCH_DIE'])
    print('ready', flush=True)
    for _ in sys.stdin:
        print(json.dumps(driver.step()), flush=True)


if __name__ == '__main__':
    {'coordinator': coordinator, 'worker': worker}[sys.argv[1]](json.loads(Path(sys.argv[2]).read_text()))
