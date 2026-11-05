"""Real durable components and NativeEngine with transport mocked at its edge."""
import copy
from datetime import datetime, timezone
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
from processing.reference_driver import ReferenceDriver, DriverHeld, DriverWait
from processing.reference_engine_worker import ReferenceEngineWorker
from processing.reference_failure import FailureOutbox
from processing.reference_protocol import manifest_sha256, validate_submission
from processing.reference_store import ReferenceStore
from processing.reference_transfer import BrokerClient
from processing.source_range_transfer import source_version_for_manifest
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
from tests.viewer_receipt_signer import SyntheticViewerSigner

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
        state_path = kwargs.pop('state_path', self.root / 'driver.sqlite')
        driver = ReferenceDriver(self.worker, self.ledger, self.outbox, self.broker,
            'https://coordinator.invalid:9443', TOKEN, state_path=state_path, ssl_context=self.context,
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
        raw['manifest']['id'] = 'dataset-' + raw['submissionId']
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
                grant = dict(request, jobId=job['jobId'], environment=job['environment'],
                    objectIds=[obj['id'] for obj in job['submission']['manifest']['objects']], token='synthetic-transfer-token')
                if request.get('protocol') == 'source-range-v2':
                    grant['expiresAt'] = datetime.fromtimestamp(time.time() + 30, timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
                self.store.receive_transfer_grant(job['jobId'], grant)

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

    def test_worker_waits_for_exact_viewer_transfer_grant_before_fetching_inputs(self):
        job = self.submit(contents=(b'queued-photo',))
        original_download = self.broker.download_object
        with patch.object(self.broker, 'download_object', wraps=original_download) as download:
            self.assertEqual(self.driver.step()['state'], 'claim_pending')
            self.driver.step()  # Persist the claim.
            self.assertEqual(self.driver.step()['state'], 'awaiting_grant')
            self.assertEqual(self.driver.step()['state'], 'awaiting_grant')
            self.assertEqual(download.call_count, 0)
            self.assertEqual(list(self.scratch.glob('jobs/*/*.input')), [])
            self.assertFalse(any(call[0] == 'initialize' for call in self.transport.calls))

            private = self.store.get(job['jobId'], private=True)
            request = private['transferRequest']
            grant = dict(request, jobId=job['jobId'], environment=job['environment'],
                objectIds=[obj['id'] for obj in private['submission']['manifest']['objects']],
                token='synthetic-transfer-token')
            production_viewer = 'synthetic-production-viewer-token'
            production_api = ReferenceControlAPI(self.store,
                viewer_bearers={VIEWER: 'staging', production_viewer: 'production'},
                worker_bearers={TOKEN: dict(node_id='cluster', worker_id='worker', worker_instance_id='instance')},
                capability_fingerprint=self.fixture['capabilities']['capabilityFingerprint'],
                options_fingerprint=self.fixture['capabilities']['engines']['odm']['optionsFingerprint'])
            cross_environment = production_api.handle('POST', '/ltds/v1/jobs/' + job['jobId'] + '/transfer-grants',
                {'Authorization': 'Bearer ' + production_viewer}, json.dumps(grant).encode(), protected_transport=True)
            self.assertEqual(cross_environment.status, 404)
            self.assertIsNone(self.store.private_transfer_grant(job['jobId']))
            response = self.api.handle('POST', '/ltds/v1/jobs/' + job['jobId'] + '/transfer-grants',
                {'Authorization': 'Bearer ' + VIEWER}, json.dumps(grant).encode(), protected_transport=True)
            self.assertEqual(response.status, 200)
            self.assertTrue(response.body['accepted'])
            replay = self.api.handle('POST', '/ltds/v1/jobs/' + job['jobId'] + '/transfer-grants',
                {'Authorization': 'Bearer ' + VIEWER}, json.dumps(grant).encode(), protected_transport=True)
            self.assertEqual(replay.status, 200)
            self.assertEqual(replay.body, response.body)
            changed = dict(grant, token='different-synthetic-transfer-token')
            conflict = self.api.handle('POST', '/ltds/v1/jobs/' + job['jobId'] + '/transfer-grants',
                {'Authorization': 'Bearer ' + VIEWER}, json.dumps(changed).encode(), protected_transport=True)
            self.assertEqual(conflict.status, 409)
            self.advance(job, 'awaiting_acceptance')
            self.assertEqual(download.call_count, 1)

    def test_source_range_v2_reservation_persists_and_fetches_through_range_client(self):
        self.driver = self.make_driver(source_range_v2_enabled=True,
            state_path=self.root / 'driver-source-range-v2.sqlite')
        job = self.submit(contents=(b'v2-queued-photo',))
        downloads = []
        def synthetic_download(*, grant, object_id, destination, grant_provider=None):
            downloads.append((grant.identity.source_version, object_id))
            Path(destination).write_bytes(self.contents[object_id])
            return Path(destination)
        with patch.object(self.driver.source_range_client, 'download_object', side_effect=synthetic_download):
            self.advance(job, 'awaiting_acceptance')
        persisted = self.store.get(job['jobId'], private=True)
        transfer = persisted['transferRequest']
        self.assertEqual(transfer['protocol'], 'source-range-v2')
        self.assertEqual(transfer['sourceVersion'], source_version_for_manifest(persisted['submission']['manifest']))
        self.assertEqual(transfer['objectIds'], [obj['id'] for obj in persisted['submission']['manifest']['objects']])
        self.assertEqual(downloads, [(transfer['sourceVersion'], transfer['objectIds'][0])])

    def test_expired_source_grant_is_durably_renewed_and_ack_replays_without_new_grant(self):
        from processing.source_range_transfer import SourceRenewalUnavailable
        state_path = self.root / 'driver-source-range-renewal.sqlite'
        self.driver = self.make_driver(source_range_v2_enabled=True,
            state_path=state_path)
        job = self.submit(contents=(b'renewal-test-photo',))
        self.advance(job, 'fetching')
        with self.driver._db() as db:
            row = db.execute('SELECT data FROM driver_jobs WHERE job=?', (job['jobId'],)).fetchone()
        data = json.loads(row[0])
        claim = data['claim']
        previous = data['grant']
        previous['expiresAt'] = '1970-01-01T00:00:00.000Z'
        data['grant'] = previous
        self.driver._save(job['jobId'], 'fetching', data)
        successor = dict(previous, expiresAt=claim['transferRequest']['expiresAt'], token='renewed-synthetic-token')
        renewals, acknowledgements = [], []

        def renew_lost(request):
            renewals.append(request)
            raise SourceRenewalUnavailable('synthetic renewal response loss')

        def renew_success(request):
            renewals.append(request)
            return 200, {'protocol': 'source-range-v2-renewal', 'renewalId': request['renewalId'],
                'sequence': request['sequence'], 'previousGrantSha256': request['previousGrantSha256'],
                'grant': successor}

        def acknowledge_lost(request):
            acknowledgements.append(request)
            raise SourceRenewalUnavailable('synthetic ack response loss')

        def acknowledge_success(request):
            acknowledgements.append(request)
            return 200, {'protocol': 'source-range-v2-renewal-ack', 'renewalId': request['renewalId'],
                'sequence': request['sequence'], 'grantSha256': request['grantSha256']}

        def download(*, grant, object_id, destination, grant_provider=None):
            self.assertEqual(grant.token, 'renewed-synthetic-token')
            self.assertTrue(callable(grant_provider))
            Path(destination).write_bytes(self.contents[object_id])
            return Path(destination)

        with patch.object(self.driver.source_range_client, 'renew_grant', side_effect=renew_lost):
            first = self.driver.step()
            self.assertEqual(first['state'], 'retry_wait')
            with self.driver._db() as db:
                row = db.execute('SELECT data FROM driver_jobs WHERE job=?', (job['jobId'],)).fetchone()
            pending = json.loads(row[0])
            self.assertEqual(pending['sourceRenewal']['phase'], 'request_pending')
            request_intent = pending['sourceRenewal']['request']

        # Reconstruct the driver to model process loss after the renewal POST.
        self.driver = self.make_driver(source_range_v2_enabled=True, state_path=state_path)
        with patch.object(self.driver.source_range_client, 'renew_grant', side_effect=renew_success), \
             patch.object(self.driver.source_range_client, 'acknowledge_grant_renewal', side_effect=acknowledge_lost):
            second = self.driver.step()
            self.assertEqual(second['state'], 'retry_wait')
            with self.driver._db() as db:
                row = db.execute('SELECT data FROM driver_jobs WHERE job=?', (job['jobId'],)).fetchone()
            pending = json.loads(row[0])
            self.assertEqual(pending['sourceRenewal']['phase'], 'ack_pending')
            self.assertEqual(pending['sourceRenewal']['successorGrant'], successor)
            self.assertEqual(pending['grant'], previous)

        # Reconstruct again to verify the durable ack intent, not a new renewal, is replayed.
        self.driver = self.make_driver(source_range_v2_enabled=True, state_path=state_path)
        with patch.object(self.driver.source_range_client, 'acknowledge_grant_renewal', side_effect=acknowledge_success), \
             patch.object(self.driver.source_range_client, 'download_object', side_effect=download):
            third = self.driver.step()
            self.assertEqual(third['state'], 'fetching')
        self.assertEqual(len(renewals), 2)
        self.assertEqual(renewals[0], request_intent)
        self.assertEqual(renewals[1], request_intent)
        self.assertEqual(len(acknowledgements), 2)
        self.assertEqual(acknowledgements[0], acknowledgements[1])
        with self.driver._db() as db:
            data = json.loads(db.execute('SELECT data FROM driver_jobs WHERE job=?', (job['jobId'],)).fetchone()[0])
        self.assertEqual(data['grant'], successor)
        self.assertEqual(data['sourceRenewalSequence'], 1)
        self.assertNotIn('sourceRenewal', data)

    def test_mid_transfer_renewal_wait_and_rejection_keep_driver_state(self):
        from processing.source_range_transfer import validate_source_grant
        self.driver = self.make_driver(source_range_v2_enabled=True,
            state_path=self.root / 'driver-source-range-mid-transfer.sqlite')
        job = self.submit(contents=(b'mid-range-renewal-photo',))
        self.advance(job, 'fetching')
        with self.driver._db() as db:
            row = db.execute('SELECT data FROM driver_jobs WHERE job=?', (job['jobId'],)).fetchone()
        data = json.loads(row[0])
        grant = validate_source_grant(data['grant'], job=data['claim'])
        cases = ((DriverWait('synthetic renewal outage'), 'retry_wait', 'source_renewal_endpoint_unavailable'),
                 (DriverHeld('synthetic renewal conflict'), 'held', 'source_renewal_response_invalid'))
        for renewal_error, expected_state, expected_reason in cases:
            with patch.object(self.driver, '_source_range_grant', side_effect=[grant, renewal_error]), \
                 patch.object(self.driver.source_range_client, 'download_object',
                    side_effect=lambda **kwargs: kwargs['grant_provider']()):
                result = self.driver.step()
            self.assertEqual((result['state'], result.get('reason')), (expected_state, expected_reason))

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

    def test_restart_after_durable_completion_response_loss_replays_same_receipt_without_rerun(self):
        job = self.submit(contents=(b'synthetic-photo-for-completion-replay',))
        self.advance(job, 'delivering')
        delivery_time = 1_800_000_000
        self.outbox.clock = lambda: delivery_time
        self.outbox.random = lambda: 0
        attempted = []

        def coordinator_accepts_then_connection_drops(request, timeout):
            envelope = json.loads(request.data)['delivery']
            attempted.append(envelope)
            self.sender_open(request, timeout)
            raise OSError('synthetic lost durable-receipt response')

        self.driver.sender._open = coordinator_accepts_then_connection_drops
        self.assertEqual(self.driver.step()['state'], 'delivering')
        self.assertEqual(self.queue.get(job['jobId'])['status'], 'succeeded')
        self.assertEqual(len(attempted), 1)
        outbox_id = attempted[0]['outboxId']
        due = self.outbox.get(outbox_id)['next_at']
        self.assertGreater(due, delivery_time)

        # Reopen the durable outbox, engine journal, and driver journal as a
        # replacement process would; only the synthetic transport remains shared.
        self.outbox = WorkerOutbox(self.root / 'outbox.sqlite',
            clock=lambda: due + 1, random_source=lambda: 0)
        self.worker = ReferenceEngineWorker(self.root / 'worker.sqlite', ledger=self.ledger,
            outbox=self.outbox, engine=self.native, engine_id='engine', node_id='cluster',
            authority_id=self.queue.authority_id, coordinator_origin=self.gate_origin,
            gate_generation=1, worker_id='worker', engine_version='pinned-engine-1')
        self.driver = self.make_driver()
        replayed = []

        def record_replay(request, timeout):
            replayed.append(json.loads(request.data)['delivery'])
            return self.sender_open(request, timeout)

        self.driver.sender._open = record_replay
        self.advance(job, 'awaiting_acceptance')
        self.assertEqual(replayed, attempted)
        self.assertEqual(self.outbox.get(outbox_id)['state'], 'acknowledged')
        self.assertEqual(sum(call[0] == 'initialize' for call in self.transport.calls), 1)
        with self.outbox._transaction() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM durable_receipts').fetchone()[0], 1)

        private = self.store.get(job['jobId'], private=True)
        result = private['resultManifest']
        receipt = dict(schemaVersion=1, receiptId='restart-viewer-receipt',
            storageReceiptId='restart-viewer-storage', jobId=job['jobId'], generation=1,
            environment='staging', inputManifestSha256=job['manifestSha256'],
            pipelineDigest=job['pipelineDigest'], resultManifestSha256=result['resultManifestSha256'],
            acceptedObjectIds=[obj['id'] for obj in result['objects']],
            acceptedAt='2026-11-02T00:00:00Z', validationVersion='ltds-reference-v1')
        self.store.accept(job['jobId'], receipt)
        self.assertEqual(self.driver.step()['state'], 'completed')
        self.assertEqual(len(list(self.scratch.glob('jobs/*/all.zip'))), 1)
        self.assertEqual(len(list(self.scratch.glob('jobs/*/*.input'))), 1)

    def test_uncertain_initialize_is_held_and_never_replayed_after_restart(self):
        job = self.submit()
        self.transport.fail_initialize = True
        self.advance(job, 'execution_pending')
        self.assertEqual(self.driver.step()['state'], 'retry_wait')
        self.driver = self.make_driver()
        self.driver.step()
        self.assertEqual(self.driver.step()['state'], 'held')
        self.assertEqual(sum(call[0] == 'initialize' for call in self.transport.calls), 1)

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
        self.assertEqual(self.ledger.cleanup_candidates(), [])  # Legacy acceptance has no signed copy proof.
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
        config['cleanupEnabled'] = True
        path.write_text(json.dumps(config))
        with self.assertRaises(ValueError):
            load_config(str(path))
        signer = SyntheticViewerSigner()
        config['viewerReceiptKeys'] = signer.trusted_keys
        path.write_text(json.dumps(config))
        self.assertEqual(load_config(str(path))['viewerReceiptKeys'], signer.trusted_keys)
        config.pop('viewerReceiptKeys')
        config['cleanupEnabled'] = False
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
        signer = SyntheticViewerSigner()
        self.api.viewer.viewer_receipt_keys = signer.trusted_keys
        self.worker.viewer_receipt_keys = signer.trusted_keys
        receipt = dict(schemaVersion=1, receiptId='cleanup-viewer', storageReceiptId='cleanup-storage',
            jobId=job['jobId'], generation=1, environment='staging', inputManifestSha256=job['manifestSha256'],
            pipelineDigest=job['pipelineDigest'], resultManifestSha256=result['resultManifestSha256'],
            acceptedObjectIds=[obj['id'] for obj in result['objects']], acceptedAt='2026-10-27T22:00:00Z', validationVersion='ltds-reference-v1')
        self.store.accept(job['jobId'], receipt, viewer_receipt=signer.receipt(receipt, result),
            trusted_viewer_receipt_keys=signer.trusted_keys)
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
        self.viewer_signer = SyntheticViewerSigner()
        self.api.viewer.viewer_receipt_keys = self.viewer_signer.trusted_keys
        self.worker = ReferenceEngineWorker(self.root / 'actual-native-worker.sqlite', ledger=self.ledger,
            outbox=self.outbox, engine=self.native, engine_id='engine', node_id='cluster',
            authority_id=self.queue.authority_id, coordinator_origin=self.gate_origin,
            gate_generation=1, worker_id='worker', engine_version='pinned-engine-1',
            viewer_receipt_keys=self.viewer_signer.trusted_keys)
        self.engine_cleanup = EngineCacheCleanup(self.worker, path=self.root / 'actual-engine-cleanup.sqlite',
            viewer_receipt_keys=self.viewer_signer.trusted_keys)
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

    def accept_viewer_copy(self, job, *, full):
        acceptance = self.acceptance(job, full=full)
        result = self.store.get(job['jobId'], private=True)['resultManifest']
        payload = dict(acceptance=acceptance, viewerReceipt=self.viewer_signer.receipt(acceptance, result))
        response = self.api.handle('POST', '/ltds/v1/jobs/' + job['jobId'] + '/acceptance',
            {'Authorization': 'Bearer ' + VIEWER}, json.dumps(payload).encode(), protected_transport=True)
        if response.status != 200:
            raise AssertionError('synthetic Viewer acceptance was rejected')
        return response.body

    def accept_legacy_receipt(self, job, *, full):
        response = self.api.handle('POST', '/ltds/v1/jobs/' + job['jobId'] + '/acceptance',
            {'Authorization': 'Bearer ' + VIEWER}, json.dumps(self.acceptance(job, full=full)).encode(),
            protected_transport=True)
        if response.status != 200:
            raise AssertionError('synthetic legacy Viewer acceptance was rejected')
        return response.body

    def test_actual_native_http_cache_removed_once_only_after_full_viewer_ack_with_inputs_retained(self):
        job = self.submit(contents=(b'abc',))
        self.advance(job, 'awaiting_acceptance')
        for _ in range(2):
            self.driver.step()
        self.assertEqual(self.node_fixture.counts().get('remove', 0), 0)
        self.assertEqual(self.native.read('/task/' + job['jobId'] + '/info')['status']['code'], 40)
        self.accept_legacy_receipt(job, full=False)
        self.driver.step()
        self.assertEqual(self.node_fixture.counts().get('remove', 0), 0)
        self.accept_viewer_copy(job, full=True)
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
