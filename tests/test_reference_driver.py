"""Real durable components and NativeEngine with transport mocked at its edge."""
import copy
import hashlib
import io
import http.client
import json
from pathlib import Path
import ssl
import os
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import uuid
import zipfile

from processing.node_gate import NodeGate
from processing.prototype import Queue
from processing.reference_control import ReferenceControlAPI
from processing.reference_driver import ReferenceDriver, DriverHeld
from processing.reference_engine_worker import ReferenceEngineWorker
from processing.reference_failure import FailureOutbox
from processing.reference_protocol import manifest_sha256, validate_submission
from processing.reference_store import ReferenceStore
from processing.reference_transfer import BrokerClient
from processing.scratch_policy import ScratchLedger
from processing.viewer_service import NativeEngine
from processing.worker_outbox import WorkerOutbox
from processing.scratch_cleanup import ScratchCleanupExecutor
from ops.reference_worker_daemon import load_config, DaemonStateLock, WorkerService
from processing.reference_http import create_reference_http_server
from processing.reference_binary import BinaryResponse
from processing.reference_api import Response
from tests.test_reference_results import ResultsFixture
from processing.reference_engine_cleanup import EngineCacheCleanup
from ops.native_node_fixture import NativeNodeFixture

TOKEN = 'synthetic-operational-worker-token'
VIEWER = 'synthetic-operational-viewer-token'


class EngineTransport:
    def __init__(self):
        self.calls, self.tasks = [], {}
        self.version, self.queue_count, self.code = 'pinned-engine-1', 0, 40
        self.fail_initialize = False
        self.options = [dict(name='numeric-limit', type='int', value=1, help='Numeric setting.'),
                        dict(name='fast-mode', type='bool', value=False, help='Boolean setting.')]
        stream = io.BytesIO()
        with zipfile.ZipFile(stream, 'w') as archive:
            archive.writestr('report.txt', b'bounded synthetic result')
        self.archive = stream.getvalue()

    def initialize(self, task):
        self.calls.append(('initialize', task['engine_uuid']))
        if self.fail_initialize:
            raise OSError('synthetic uncertain response')
        self.tasks[task['engine_uuid']] = self.code

    def upload(self, job, item):
        self.calls.append(('upload', job))

    def action(self, action, job):
        self.calls.append((action, job))

    def read(self, route, **kwargs):
        self.calls.append(('read', route))
        if route == '/info':
            return {'engine': 'odx', 'engineVersion': self.version, 'version': 'different-wrapper-2',
                    'taskQueueCount': self.queue_count, 'maxImages': None}
        if route == '/task/list':
            return [{'uuid': job} for job in self.tasks]
        if route == '/options':
            return copy.deepcopy(self.options)
        job = route.split('/')[2]
        return {'uuid': job, 'status': {'code': self.tasks[job]}}

    def download(self, job):
        self.calls.append(('download', job))
        response = io.BytesIO(self.archive)
        response.headers = {'Content-Length': str(len(self.archive))}
        return response


class ReferenceDriverTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.patches = []
        self.gate_patch = patch('processing.node_gate.canonical_gate_path', return_value=self.root / 'gate')
        self.gate_patch.start()
        self.queue = Queue(self.root / 'queue', reference_enabled=True)
        self.store = ReferenceStore(self.queue)
        self.fixture = json.loads((Path(__file__).resolve().parents[1] / 'contracts/reference-v1.contract.json').read_text())
        self.api = ReferenceControlAPI(self.store, viewer_bearers={VIEWER: 'staging'},
            worker_bearers={TOKEN: dict(node_id='cluster', worker_id='worker', worker_instance_id='instance')},
            capability_fingerprint=self.fixture['capabilities']['capabilityFingerprint'],
            options_fingerprint=self.fixture['capabilities']['engines']['odm']['optionsFingerprint'])
        self.scratch = self.root / 'scratch'
        self.scratch.mkdir(mode=0o700)
        self.ledger = ScratchLedger(self.root / 'ledger.sqlite', self.scratch, capacity_probe=lambda _: 10**9,
            safety_floor_bytes=1024, cleanup_grace_seconds=0)
        self.outbox = WorkerOutbox(self.root / 'outbox.sqlite')
        self.failures = FailureOutbox(self.root / 'failures.sqlite')
        self.native = NativeEngine('http://127.0.0.1:3001')
        self.transport = EngineTransport()
        for method in ('initialize','upload','action','read','download'):
            patched = patch.object(self.native, method, side_effect=getattr(self.transport, method))
            patched.start()
            self.patches.append(patched)
        self.gate_origin = 'http://127.0.0.1:18080'
        with NodeGate('engine') as gate:
            gate.provision('cluster', self.gate_origin, self.queue.authority_id)
        self.worker = ReferenceEngineWorker(self.root / 'worker.sqlite', ledger=self.ledger, outbox=self.outbox,
            engine=self.native, engine_id='engine', node_id='cluster', authority_id=self.queue.authority_id,
            coordinator_origin=self.gate_origin, gate_generation=1, worker_id='worker', engine_version='pinned-engine-1')
        self.context = ssl.create_default_context()
        self.broker = BrokerClient('https://broker.invalid:9444', ssl_context=self.context)
        self.contents = {}
        broker_patch = patch.object(self.broker, 'download_object', side_effect=self.download)
        broker_patch.start()
        self.patches.append(broker_patch)
        self.driver = self.make_driver()

    def tearDown(self):
        for item in reversed(self.patches):
            item.stop()
        self.queue.close()
        self.gate_patch.stop()
        self.temp.cleanup()

    def make_driver(self, **kwargs):
        driver = ReferenceDriver(self.worker, self.ledger, self.outbox, self.broker,
            'https://coordinator.invalid:9443', TOKEN, state_path=self.root / 'driver.sqlite', ssl_context=self.context,
            output_budget=1024 * 1024, transfer_ttl=240, safety_floor=1024, failure_outbox=self.failures,
            worker_instance_id='instance', **kwargs)
        driver._request = self.request
        driver.sender._open = self.sender_open
        return driver

    def request(self, method, suffix, payload=None, *, claim_token=None):
        headers = {'Authorization': 'Bearer ' + TOKEN}
        if claim_token is not None:
            headers['X-Claim-Token'] = claim_token
        response = self.api.handle(method, '/ltds/worker/v1' + suffix, headers,
            b'' if payload is None else json.dumps(payload).encode(), protected_transport=True)
        if response.status == 409:
            return 409, None
        if response.status != 200:
            raise DriverHeld('test_control_rejected')
        return 200, response.body

    def sender_open(self, request, timeout):
        response = self.api.handle('POST', '/ltds/worker/v1/results', {'Authorization': 'Bearer ' + TOKEN},
                                   request.data, protected_transport=True)
        if response.status != 200:
            raise ValueError('synthetic coordinator rejected result')
        raw = json.dumps(response.body).encode()

        class Reply(io.BytesIO):
            status = 200
            headers = {'Content-Type': 'application/json', 'Content-Length': str(len(raw))}

            def geturl(self):
                return request.full_url

        return Reply(raw)

    def download(self, grant, object_id, destination):
        content = self.contents[object_id]
        Path(destination).write_bytes(content)
        return Path(destination)

    def submit(self, *, environment='staging', contents=(b'arbitrary-photo-data',)):
        raw = copy.deepcopy(self.fixture['submission'])
        raw['submissionId'] = str(uuid.uuid4())
        objects = []
        for index, content in enumerate(contents):
            object_id = 'image-' + raw['submissionId'] + '-' + str(index)
            self.contents[object_id] = content
            objects.append(dict(id=object_id, name='photo-' + str(index) + '.jpg', byteSize=len(content),
                sha256=hashlib.sha256(content).hexdigest(), kind='image', mediaType='image/jpeg'))
        raw['manifest'].update(objects=objects, objectCount=len(objects), totalBytes=sum(map(len, contents)),
            inputCounts=dict(images=len(objects), lidarObjects=0, points=0))
        raw['manifest']['sha256'] = manifest_sha256(raw['manifest'])
        normalized = validate_submission(raw, environment=environment, capability_fingerprint=raw['capabilityFingerprint'])
        return self.store.submit(normalized)[0]

    def supply_grants(self):
        for job in self.store.list(private=True):
            if job['phase'] == 'awaiting_transfer' and self.store.private_transfer_grant(job['jobId']) is None:
                request = job['transferRequest']
                self.store.receive_transfer_grant(job['jobId'], dict(request, jobId=job['jobId'], environment=job['environment'],
                    objectIds=[obj['id'] for obj in job['submission']['manifest']['objects']], token='synthetic-transfer-token'))

    def advance(self, job, target, maximum=80):
        for _ in range(maximum):
            self.supply_grants()
            result = self.driver.step()
            with self.driver._db() as db:
                row = db.execute('SELECT stage FROM driver_jobs WHERE job=?', (job['jobId'],)).fetchone()
            if row and row[0] == target:
                return result
            if result['state'] == 'held':
                self.fail('unexpected driver hold: ' + result['reason'])
        self.fail('driver target stage not reached: ' + target)

    def test_arbitrary_images_production_and_next_job_before_viewer_acceptance(self):
        first = self.submit(environment='production', contents=(b'not-abc-one', b'another-photo'))
        self.advance(first, 'awaiting_acceptance')
        self.assertEqual(self.queue.get(first['jobId'])['status'], 'succeeded')
        with NodeGate('engine') as gate:
            self.assertIsNone(gate.snapshot()['reservation'])
        second = self.submit()
        self.advance(second, 'awaiting_acceptance')
        self.assertEqual(sum(call[0] == 'initialize' for call in self.transport.calls), 2)
        with self.driver._db() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM driver_jobs WHERE stage='awaiting_acceptance'").fetchone()[0], 2)

    def test_authority_version_and_active_native_tasks_prevent_claim(self):
        job = self.submit()
        self.transport.version = 'unapproved-engine-version'
        self.assertEqual(self.driver.step()['state'], 'not_ready')
        self.transport.version = 'pinned-engine-1'
        self.transport.queue_count = 1
        self.assertEqual(self.driver.step()['state'], 'not_ready')
        self.transport.queue_count = 0
        foreign = str(uuid.uuid4())
        self.transport.tasks[foreign] = 20
        self.assertEqual(self.driver.step()['state'], 'not_ready')
        self.assertEqual(self.queue.get(job['jobId'])['status'], 'queued')
        self.assertFalse(any(call[0] == 'initialize' for call in self.transport.calls))

    def test_restart_after_claim_response_loss_reuses_exact_claim(self):
        job = self.submit()
        self.driver.step()
        original = self.driver._request

        def lost(method, suffix, payload=None, **kwargs):
            result = original(method, suffix, payload, **kwargs)
            if suffix == '/claim':
                raise OSError('synthetic lost claim response')
            return result

        self.driver._request = lost
        self.assertEqual(self.driver.step()['state'], 'retry_wait')
        token = self.store.get(job['jobId'], private=True)['claimToken']
        self.driver = self.make_driver()
        self.advance(job, 'awaiting_acceptance')
        self.assertEqual(self.store.get(job['jobId'], private=True)['claimToken'], token)
        self.assertEqual(sum(call[0] == 'initialize' for call in self.transport.calls), 1)

    def test_uncertain_initialize_is_held_and_never_replayed_after_restart(self):
        job = self.submit()
        self.transport.fail_initialize = True
        self.advance(job, 'execution_pending')
        self.assertEqual(self.driver.step()['state'], 'retry_wait')
        self.driver = self.make_driver()
        self.driver.step()
        self.assertEqual(self.driver.step()['state'], 'held')
        self.assertEqual(sum(call[0] == 'initialize' for call in self.transport.calls), 1)

    def test_unrelated_failure_ack_does_not_release_current_job_gate(self):
        from processing.reference_failure import make_failure, _receipt
        # An earlier retained record is due first in the actual shared outbox.
        other = make_failure(job_id=str(uuid.uuid4()), generation=1, environment='staging',
            input_manifest_sha256='a'*64, pipeline_digest='b'*64,
            terminal_state='failed', error_code='synthetic_failure')
        other_id = self.failures.persist(other, engine_terminal=True)
        self.transport.code = 30
        job = self.submit()
        self.advance(job, 'failure_delivering')
        with self.driver._db() as db:
            data = json.loads(db.execute('SELECT data FROM driver_jobs WHERE job=?', (job['jobId'],)).fetchone()[0])
        current_id = data['failureOutboxId']
        self.assertNotEqual(current_id, other_id)
        with NodeGate('engine') as gate:
            before = gate.snapshot()['reservation']
            self.assertEqual(before['job']['id'], job['jobId'])
        original_request = self.driver._request
        original_binding = self.driver._claim_binding
        def binding(payload):
            if payload['jobId'] == other['jobId']:
                return {'claimRequestId': 'synthetic-other', 'claimToken': 'synthetic-token'}
            return original_binding(payload)
        def request(method, suffix, payload=None, **kwargs):
            if suffix == '/terminal-outcomes' and payload['delivery']['outboxId'] == other_id:
                return 200, _receipt(payload['delivery'], other)
            return original_request(method, suffix, payload, **kwargs)
        with patch.object(self.driver, '_claim_binding', side_effect=binding), \
             patch.object(self.driver, '_request', side_effect=request):
            result = self.driver.step()
        self.assertEqual(result['state'], 'failure_delivering')
        self.assertEqual(self.failures.get(other_id)['state'], 'acknowledged')
        self.assertNotEqual(self.failures.get(current_id)['state'], 'acknowledged')
        with NodeGate('engine') as gate:
            self.assertEqual(gate.snapshot()['reservation'], before)
        self.advance(job, 'awaiting_failure_acceptance')
        self.assertEqual(self.failures.get(current_id)['state'], 'acknowledged')
        with NodeGate('engine') as gate:
            self.assertIsNone(gate.snapshot()['reservation'])

    def test_unrelated_success_ack_does_not_advance_current_job(self):
        job = self.submit()
        self.advance(job, 'delivering')
        with self.driver._db() as db:
            data = json.loads(db.execute('SELECT data FROM driver_jobs WHERE job=?', (job['jobId'],)).fetchone()[0])
        self.assertNotEqual(self.outbox.get(data['outboxId'])['state'], 'acknowledged')
        with NodeGate('engine') as gate:
            before = gate.snapshot()['reservation']
        with patch.object(self.driver.sender, 'send_next',
                          return_value={'state': 'acknowledged', 'outboxId': 'a' * 64}):
            result = self.driver.step()
        self.assertEqual(result['state'], 'delivering')
        self.assertEqual(result['deliveryState'], 'other_acknowledged')
        with NodeGate('engine') as gate:
            self.assertEqual(gate.snapshot()['reservation'], before)
        self.advance(job, 'awaiting_acceptance')

    def test_confirmed_failed_engine_delivers_metadata_without_fake_archive_and_releases_gate(self):
        self.transport.code = 30
        job = self.submit()
        self.advance(job, 'awaiting_failure_acceptance')
        self.assertEqual(self.queue.get(job['jobId'])['status'], 'failed')
        self.assertEqual(sum(call[0] == 'download' for call in self.transport.calls), 0)
        with NodeGate('engine') as gate:
            self.assertIsNone(gate.snapshot()['reservation'])
        with self.ledger._transaction() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM objects').fetchone()[0], 0)
        self.transport.code = 40
        second = self.submit()
        self.advance(second, 'awaiting_acceptance')

    def test_terminal_viewer_ack_is_validated_and_retained_without_artifact_deletion(self):
        self.transport.code = 50
        job = self.submit()
        self.advance(job, 'awaiting_failure_acceptance')
        failure = self.api.viewer.terminal.outcome(job['jobId'], 'staging')
        receipt = dict(schemaVersion=1, receiptId='terminal-viewer', storageReceiptId='terminal-storage',
            jobId=job['jobId'], generation=1, environment='staging', inputManifestSha256=job['manifestSha256'],
            pipelineDigest=job['pipelineDigest'], failureSha256=failure['failureSha256'],
            acceptedAt='2026-10-27T22:00:00Z', validationVersion='ltds-reference-v1')
        self.api.viewer.terminal.accept(job['jobId'], receipt, 'staging')
        self.driver.step()
        with self.driver._db() as db:
            row = db.execute('SELECT stage,data FROM driver_jobs WHERE job=?', (job['jobId'],)).fetchone()
        self.assertEqual(row['stage'], 'completed')
        self.assertEqual(json.loads(row['data'])['terminalAcceptance'], receipt)
        self.assertEqual(len(list(self.scratch.glob('jobs/*/*.input'))), 1)

    def test_background_success_acceptance_and_optional_cleanup_do_not_block_new_work(self):
        job = self.submit()
        self.advance(job, 'awaiting_acceptance')
        private = self.store.get(job['jobId'], private=True)
        result = private['resultManifest']
        receipt = dict(schemaVersion=1, receiptId='success-viewer', storageReceiptId='success-storage',
            jobId=job['jobId'], generation=1, environment='staging', inputManifestSha256=job['manifestSha256'],
            pipelineDigest=job['pipelineDigest'], resultManifestSha256=result['resultManifestSha256'],
            acceptedObjectIds=[obj['id'] for obj in result['objects']], acceptedAt='2026-10-27T22:00:00Z', validationVersion='ltds-reference-v1')
        self.store.accept(job['jobId'], receipt)
        self.driver.step()
        with self.driver._db() as db:
            self.assertEqual(db.execute('SELECT stage FROM driver_jobs WHERE job=?', (job['jobId'],)).fetchone()[0], 'completed')
        self.assertEqual(len(self.ledger.cleanup_candidates()), 1)
        self.assertEqual(len(list(self.scratch.glob('jobs/*/all.zip'))), 1)

    def test_dynamic_local_options_refresh_and_strict_scalar_types(self):
        self.assertTrue(self.driver._ready())
        self.assertEqual(self.worker.allowed_options, frozenset(('numeric-limit','fast-mode')))
        raw = copy.deepcopy(self.fixture['submission'])
        raw['pipeline']['odm']['options'] = {'numeric-limit': 5, 'fast-mode': True}
        normalized = validate_submission(raw, environment='staging', capability_fingerprint=raw['capabilityFingerprint'],
                                         allowed_odm_options=self.worker.allowed_options)
        job = dict(submission=normalized, environment='staging', nodeId='cluster',
                   manifestSha256=normalized['manifestSha256'], pipelineDigest=normalized['pipelineDigest'])
        self.driver._validate_job(job)
        for value in ('5', True, 2.5):
            raw['pipeline']['odm']['options']['numeric-limit'] = value
            normalized = validate_submission(raw, environment='staging', capability_fingerprint=raw['capabilityFingerprint'],
                                             allowed_odm_options=self.worker.allowed_options)
            job.update(submission=normalized, pipelineDigest=normalized['pipelineDigest'])
            with self.assertRaises(DriverHeld):
                self.driver._validate_job(job)
        self.transport.options = [dict(name='new-flag', type='bool', value=False, help='New boolean flag.'),
                                  dict(name='unsafe-path', type='string', value='', help='Path setting.')]
        self.driver._ready()
        self.assertEqual(self.worker.allowed_options, frozenset(('new-flag',)))

    def test_unknown_retained_task_readiness_scan_is_bounded_per_poll(self):
        self.submit()
        self.transport.tasks = {str(uuid.uuid4()): 40 for _ in range(5)}
        self.assertEqual(self.driver.step()['state'], 'not_ready')
        checks = [route for kind, route in self.transport.calls if kind == 'read' and route.startswith('/task/') and route.endswith('/info')]
        self.assertEqual(len(checks), 4)
        self.assertEqual(self.driver.step()['state'], 'claim_pending')

    def test_authority_mismatch_and_actual_native_engine_requirement(self):
        self.submit()
        original = self.driver._request
        self.driver._request = lambda method, suffix, *args, **kwargs: (200, {'authorityId': str(uuid.uuid4())}) if suffix == '/authority' else original(method, suffix, *args, **kwargs)
        self.assertEqual(self.driver.step(), {'state': 'held', 'reason': 'coordinator_authority_changed'})
        self.assertTrue(all(job['status'] == 'queued' for job in self.queue.list()))
        self.worker.engine = object()
        try:
            with self.assertRaises(ValueError):
                self.make_driver()
        finally:
            self.worker.engine = self.native

    def test_failure_gate_ack_lost_checkpoint_recovers_without_mutation_replay(self):
        self.transport.code = 30
        job = self.submit()
        self.advance(job, 'failure_delivering')
        original = self.driver._save

        def lost(job_id, stage, data):
            if stage == 'awaiting_failure_acceptance':
                raise OSError('synthetic lost local checkpoint')
            return original(job_id, stage, data)

        self.driver._save = lost
        self.assertEqual(self.driver.step()['state'], 'retry_wait')
        self.driver = self.make_driver()
        self.advance(job, 'awaiting_failure_acceptance')
        self.assertEqual(sum(call[0] == 'initialize' for call in self.transport.calls), 1)

    def test_maintenance_engine_pin_change_retains_old_receipts_and_requires_idle_execution(self):
        first = self.submit()
        self.advance(first, 'awaiting_acceptance')
        self.worker.version = 'updated-engine-2'
        self.transport.version = 'updated-engine-2'
        self.driver = self.make_driver()
        second = self.submit()
        self.advance(second, 'claimed')
        self.worker.version = 'unsafe-update-during-claim'
        with self.assertRaises(ValueError):
            self.make_driver()
        self.worker.version = 'updated-engine-2'
        self.advance(second, 'awaiting_acceptance')
        self.assertEqual(self.store.get(first['jobId'], private=True)['resultManifest']['engineVersions'], {'odm':'pinned-engine-1'})
        self.assertEqual(self.store.get(second['jobId'], private=True)['resultManifest']['engineVersions'], {'odm':'updated-engine-2'})

    def test_private_daemon_config_rejects_static_options_and_never_echoes_secret(self):
        cert, key = self.root / 'cert.pem', self.root / 'key.pem'
        cert.write_text('synthetic placeholder certificate')
        key.write_text('synthetic placeholder key')
        key.chmod(0o600)
        config = dict(stateRoot=str(self.root), scratchRoot=str(self.scratch), nodeId='cluster', workerId='worker',
            workerInstanceId='instance', authorityId=self.queue.authority_id, engineId='engine', gateGeneration=1,
            gateCoordinatorOrigin=self.gate_origin, coordinatorOrigin='https://coordinator.invalid:9443',
            brokerOrigin='https://broker.invalid:9444', engineOrigin=self.native.origin, engineToken='',
            workerBearer=TOKEN, resultReadBearer='separate-result-reader-token-00000001', caFile=str(cert),
            serverCertFile=str(cert), serverKeyFile=str(key), listenHost='127.0.0.1', listenPort=9445,
            outputBudget=1024*1024, safetyFloor=1024, transferTtl=240, cleanupGrace=3600,
            pollSeconds=1, engineVersion='pinned-engine-1', shareResults=True)
        path = self.root / 'private-config.json'
        path.write_text(json.dumps(config))
        path.chmod(0o600)
        settings = load_config(str(path))
        self.assertFalse(settings['cleanupEnabled'])
        self.assertEqual(settings['allowedEnvironments'], ['production','staging'])
        config['allowedOdmOptions'] = ['numeric-limit']
        path.write_text(json.dumps(config))
        with self.assertRaises(ValueError) as observed:
            load_config(str(path))
        self.assertNotIn(TOKEN, str(observed.exception))

    @unittest.skipUnless(sys.platform.startswith('linux'), 'exact-object cleanup requires Linux')
    def test_opted_in_background_cleanup_removes_only_accepted_result_after_receipt(self):
        job = self.submit()
        self.advance(job, 'awaiting_acceptance')
        private = self.store.get(job['jobId'], private=True)
        result = private['resultManifest']
        receipt = dict(schemaVersion=1, receiptId='cleanup-viewer', storageReceiptId='cleanup-storage',
            jobId=job['jobId'], generation=1, environment='staging', inputManifestSha256=job['manifestSha256'],
            pipelineDigest=job['pipelineDigest'], resultManifestSha256=result['resultManifestSha256'],
            acceptedObjectIds=[obj['id'] for obj in result['objects']], acceptedAt='2026-10-27T22:00:00Z', validationVersion='ltds-reference-v1')
        self.store.accept(job['jobId'], receipt)
        self.driver.cleanup_executor = ScratchCleanupExecutor(self.ledger)
        self.driver.step()
        self.assertEqual(len(list(self.scratch.glob('jobs/*/all.zip'))), 0)
        self.assertEqual(len(list(self.scratch.glob('jobs/*/*.input'))), 1)
        self.assertFalse(any(kind in ('remove','cancel') for kind, _ in self.transport.calls))


class NativeHTTPCacheCleanupDriverTests(unittest.TestCase):
    # Reuse durable coordinator/transport helpers, while replacing the patched
    # engine instance with a separate real NativeEngine and local HTTP backend.
    make_driver = ReferenceDriverTests.make_driver
    request = ReferenceDriverTests.request
    sender_open = ReferenceDriverTests.sender_open
    download = ReferenceDriverTests.download
    submit = ReferenceDriverTests.submit
    supply_grants = ReferenceDriverTests.supply_grants
    advance = ReferenceDriverTests.advance

    def setUp(self):
        ReferenceDriverTests.setUp(self)
        self.node_fixture = NativeNodeFixture(self.root / 'native-http.sqlite', engine_version='pinned-engine-1', synthetic_enabled=True).start()
        self.native = NativeEngine(self.node_fixture.origin)
        self.worker = ReferenceEngineWorker(self.root / 'actual-native-worker.sqlite', ledger=self.ledger,
            outbox=self.outbox, engine=self.native, engine_id='engine', node_id='cluster',
            authority_id=self.queue.authority_id, coordinator_origin=self.gate_origin,
            gate_generation=1, worker_id='worker', engine_version='pinned-engine-1')
        self.engine_cleanup = EngineCacheCleanup(self.worker, path=self.root / 'actual-engine-cleanup.sqlite')
        self.driver = ReferenceDriver(self.worker, self.ledger, self.outbox, self.broker,
            'https://coordinator.invalid:9443', TOKEN, state_path=self.root / 'actual-driver.sqlite', ssl_context=self.context,
            output_budget=1024 * 1024, transfer_ttl=240, safety_floor=1024, failure_outbox=self.failures,
            worker_instance_id='instance', engine_cleanup=self.engine_cleanup)
        self.driver._request = self.request
        self.driver.sender._open = self.sender_open

    def tearDown(self):
        self.node_fixture.close()
        ReferenceDriverTests.tearDown(self)

    def acceptance(self, job, *, full):
        private = self.store.get(job['jobId'], private=True)
        result = private['resultManifest']
        label = 'full' if full else 'partial'
        return dict(schemaVersion=1, receiptId='native-viewer-' + label, storageReceiptId='native-storage-' + label,
            jobId=job['jobId'], generation=1, environment='staging', inputManifestSha256=job['manifestSha256'],
            pipelineDigest=job['pipelineDigest'], resultManifestSha256=result['resultManifestSha256'],
            acceptedObjectIds=[obj['id'] for obj in result['objects']] if full else [],
            acceptedAt='2026-10-27T22:00:00Z', validationVersion='ltds-reference-v1')

    def test_actual_native_http_cache_removed_once_only_after_full_viewer_ack_with_inputs_retained(self):
        job = self.submit(contents=(b'abc',))
        self.advance(job, 'awaiting_acceptance')
        for _ in range(2):
            self.driver.step()
        self.assertEqual(self.node_fixture.counts().get('remove', 0), 0)
        self.assertEqual(self.native.read('/task/' + job['jobId'] + '/info')['status']['code'], 40)
        self.store.accept(job['jobId'], self.acceptance(job, full=False))
        self.driver.step()
        self.assertEqual(self.node_fixture.counts().get('remove', 0), 0)
        self.store.accept(job['jobId'], self.acceptance(job, full=True))
        self.driver.step()
        self.assertEqual(self.node_fixture.counts().get('remove', 0), 1)
        self.assertIsNone(self.native.read('/task/' + job['jobId'] + '/info', missing=True))
        for _ in range(2):
            self.driver.step()
        self.assertEqual(self.node_fixture.counts().get('remove', 0), 1)
        inputs = list(self.scratch.glob('jobs/*/*.input'))
        self.assertEqual(len(inputs), 1)
        self.assertEqual(inputs[0].read_bytes(), b'abc')
        self.assertEqual(len(list(self.scratch.glob('jobs/*/all.zip'))), 1)
        with self.driver._db() as db:
            self.assertEqual(db.execute('SELECT stage FROM driver_jobs WHERE job=?', (job['jobId'],)).fetchone()[0], 'completed')

    def test_actual_failed_http_task_terminal_ack_never_admits_cache_removal(self):
        job = self.submit(contents=(b'abc',))
        self.advance(job, 'executing')
        # Fixture-only terminal failure injection, subsequently observed over the
        # actual NativeEngine HTTP path; no synthetic success archive is fetched.
        with self.node_fixture._db() as db:
            db.execute('UPDATE tasks SET status=30 WHERE uuid=?', (job['jobId'],))
        self.advance(job, 'awaiting_failure_acceptance')
        failure = self.api.viewer.terminal.outcome(job['jobId'], 'staging')
        receipt = dict(schemaVersion=1, receiptId='native-terminal-viewer', storageReceiptId='native-terminal-storage',
            jobId=job['jobId'], generation=1, environment='staging', inputManifestSha256=job['manifestSha256'],
            pipelineDigest=job['pipelineDigest'], failureSha256=failure['failureSha256'],
            acceptedAt='2026-10-27T22:00:00Z', validationVersion='ltds-reference-v1')
        self.api.viewer.terminal.accept(job['jobId'], receipt, 'staging')
        with patch.object(self.engine_cleanup, 'remove', wraps=self.engine_cleanup.remove) as attempted:
            self.driver.step()
            self.assertEqual(attempted.call_count, 0)
        self.assertEqual(self.node_fixture.counts().get('remove', 0), 0)
        self.assertEqual(self.node_fixture.counts()['download'], 0)
        self.assertEqual(self.native.read('/task/' + job['jobId'] + '/info')['status']['code'], 30)
        self.assertEqual(len(list(self.scratch.glob('jobs/*/*.input'))), 1)
        self.assertEqual(self.engine_cleanup.pending(), [])
        with self.driver._db() as db:
            self.assertEqual(db.execute('SELECT stage FROM driver_jobs WHERE job=?', (job['jobId'],)).fetchone()[0], 'completed')


class DaemonLifecycleTests(ResultsFixture, unittest.TestCase):
    def await_state(self, service, wanted):
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            if service.health()['state'] == wanted:
                return
            threading.Event().wait(0.01)
        self.fail('daemon lifecycle state not reached')

    def test_shutdown_joins_actual_http_read_pin_and_inflight_driver_before_exit(self):
        stream_entered, stream_release, stream_closed = threading.Event(), threading.Event(), threading.Event()
        driver_entered, driver_release = threading.Event(), threading.Event()
        source = self.open_result()

        class BlockingStream:
            def chunks(self):
                stream_entered.set()
                stream_release.wait(timeout=5)
                yield from source.chunks()

            def close(self):
                source.close()
                stream_closed.set()

        class Adapter:
            def handle(self, *_args, **_kwargs):
                return BinaryResponse(BlockingStream(), status=source.status, headers=source.headers)

        class Driver:
            def step(self):
                driver_entered.set()
                driver_release.wait(timeout=5)
                return {'state': 'idle'}

        server = create_reference_http_server(Adapter())
        stop, exits, errors = threading.Event(), [], []
        service = WorkerService(Driver(), server, poll_seconds=0.1)
        ownership = DaemonStateLock(self.root)
        ownership.__enter__()
        running = threading.Thread(target=lambda: exits.append(service.run(stop)))
        running.start()

        def consume():
            connection = http.client.HTTPConnection(*server.server_address, timeout=5)
            try:
                connection.request('GET', '/result')
                response = connection.getresponse()
                response.read()
            except Exception as error:
                errors.append(type(error).__name__)
            finally:
                connection.close()

        client = threading.Thread(target=consume)
        client.start()
        try:
            self.assertTrue(driver_entered.wait(timeout=2))
            self.assertTrue(stream_entered.wait(timeout=2))
            stop.set()
            self.await_state(service, 'draining')
            self.assertFalse(server.daemon_threads)
            self.assertTrue(server.block_on_close)
            self.assertFalse(service._driver_thread.daemon)
            self.assertEqual(self.active_pins(), 1)
            self.assertTrue(running.is_alive())
            with self.assertRaises(ValueError):
                with DaemonStateLock(self.root):
                    self.fail('second daemon acquired state while draining')
            stream_release.set()
            self.assertTrue(stream_closed.wait(timeout=2))
            self.assertEqual(self.active_pins(), 0)
            self.assertTrue(running.is_alive())
            driver_release.set()
            running.join(timeout=2)
            client.join(timeout=2)
            self.assertFalse(running.is_alive())
            self.assertEqual(exits, [0])
            self.assertEqual(errors, [])
            self.assertEqual(service.health()['state'], 'stopped')
        finally:
            stop.set()
            stream_release.set()
            driver_release.set()
            running.join(timeout=6)
            client.join(timeout=6)
            source.close()
            ownership.__exit__(None, None, None)

    def test_unexpected_driver_failure_exits_unhealthy_without_secret_details(self):
        class Driver:
            def step(self):
                raise RuntimeError(TOKEN + ' C:/private/worker')

        class Adapter:
            def handle(self, *_args, **_kwargs):
                return Response(200, {})

        server = create_reference_http_server(Adapter())
        service = WorkerService(Driver(), server, poll_seconds=0.1)
        self.assertEqual(service.run(threading.Event()), 2)
        health = service.health()
        self.assertEqual((health['state'], health['reason']), ('failed','driver_thread_failed'))
        self.assertFalse(health['driverAlive'])
        self.assertFalse(health['listenerAlive'])
        self.assertNotIn(TOKEN, json.dumps(health))
        self.assertNotIn('C:/private', json.dumps(health))

    def test_state_root_os_lock_excludes_second_daemon_until_owner_finishes(self):
        with DaemonStateLock(self.root):
            with self.assertRaises(ValueError):
                with DaemonStateLock(self.root):
                    self.fail('second daemon acquired live state root')
        with DaemonStateLock(self.root):
            pass


if __name__ == '__main__':
    unittest.main()
