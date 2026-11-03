"""ViewerService reads against a task-bound guarded mTLS node, synthetically."""
import copy
import hashlib
import json
import shutil
import ssl
import unittest
import uuid
from unittest.mock import patch

from ops.native_node_fixture import ARCHIVE, task_archive
from processing import reference_protocol as protocol
from processing.guarded_native_client import GuardedNativeClient
from processing.viewer_service import EngineUnavailable, ViewerService
from processing.prototype import Queue
from processing.config import parse_node_inventory
from processing.node_gate import NodeGate
from processing.host_mutation_journal import HostMutationJournal
from tests.guarded_test_signer import operation_signer
from tests.test_bound_node_job_candidate import BoundJobTests, KEY
from tests.test_guarded_native_https import TOKEN
from tests.test_viewer_service import ViewerServiceTests, FakeEngine, OPTIONS


class GuardedViewerAdapterTests(BoundJobTests):
    def _malformed_mutation_ack_holds_checkpoint(self, operation, response_body):
        coordinator = ViewerServiceTests(methodName='runTest')
        coordinator.setUp()
        self.addCleanup(coordinator.tearDown)
        coordinator.service.close(); coordinator.queue.close()
        coordinator.config.prototype_state_dir = str(coordinator.root / 'malformed-ack-state')
        coordinator.config.viewer_require_guard = True
        coordinator.config.node_inventory = parse_node_inventory(json.dumps([
            dict(id='main', physicalHostId='main', origin='http://192.0.2.1:3000',
                 tokenEnv='NODE_TEST_TOKEN', maxPhotos=None, capabilities=['odm'])]), {})
        coordinator.queue = Queue(coordinator.config.prototype_state_dir, node_ids=('main',),
            treedar_node_ids=(), viewer_enabled=True, per_node_slots=True)
        coordinator.engines = {'main': FakeEngine('main', coordinator.fleet)}
        self.proxy._require_job_claim = True
        self.commit_reply(True)
        server = self.start_listener()
        context = ssl.create_default_context(cafile=str(self.root / 'server.pem'))
        context.load_cert_chain(self.root / 'client.pem', self.root / 'client.key')
        claims = {}
        def factory(identity):
            job = identity['viewerJobId']
            self.job = identity['engineTaskId']
            if job not in claims:
                claims[job] = self.envelope(viewerJobId=job, viewerGeneration=identity['viewerGeneration'],
                    engineTaskId=identity['engineTaskId'], nodeId=identity['nodeId'],
                    environment=identity['environment'], requestSha256=identity['requestSha256'],
                    nonce=hashlib.sha256(job.encode()).hexdigest())
            claim = claims[job]
            return GuardedNativeClient('127.0.0.1', server.server_port, context, TOKEN, timeout=10,
                job_claim=claim, operation_signer=operation_signer(self.proxy, KEY, claim))
        coordinator.service = ViewerService(coordinator.queue, coordinator.config,
            engines=coordinator.engines, probe=lambda *_: {'ready': True, 'maxImages': None, 'taskQueueCount': 0},
            guard_probe=lambda _: True, guarded_engine_factory=factory)
        coordinator.service._catalog = lambda: {'ready': True, 'commonOptions': [dict(x) for x in OPTIONS]}
        coordinator.service._selected_options = lambda _: [dict(x) for x in OPTIONS]
        identifier = coordinator.initialize(options=[])
        coordinator.upload(identifier, name='photo.jpg', content=b'abc')
        coordinator.service.commit(identifier)
        handler = server.RequestHandlerClass
        original = handler._reply
        target = '/v1/initialize-bound' if operation == 'init' else '/v1/commit-bound'
        def corrupt_response(instance, status, body):
            if status == 200 and instance.path == target:
                body = response_body(body)
            return original(instance, status, body)
        with patch.object(handler, '_reply', corrupt_response):
            coordinator.service.tick()
        task = coordinator.task(identifier)
        checkpoint = coordinator.service.store.guarded_dispatch_checkpoint(identifier)
        self.assertEqual(task['stage'], 'held')
        self.assertEqual((checkpoint['operation'], checkpoint['state']), (operation, 'pending'))
        self.assertEqual(coordinator.service.info(identifier)['status']['code'], 10)
        self.assertEqual(self.backend.counts()['initialize'], 1)
        self.assertEqual(self.backend.counts()['commit'], int(operation == 'commit'))
        coordinator.service.tick()
        self.assertEqual(coordinator.service.store.guarded_dispatch_checkpoint(identifier), checkpoint)
        self.assertEqual(self.backend.counts()['initialize'], 1)
        self.assertEqual(self.backend.counts()['commit'], int(operation == 'commit'))

    def test_wrong_initialization_uuid_keeps_checkpoint_pending(self):
        self._malformed_mutation_ack_holds_checkpoint('init', lambda body: {**body, 'uuid': str(uuid.uuid4())})

    def test_rejected_initialization_ack_keeps_checkpoint_pending(self):
        self._malformed_mutation_ack_holds_checkpoint('init', lambda body: {**body, 'accepted': False})

    def test_wrong_commit_uuid_keeps_checkpoint_pending(self):
        self._malformed_mutation_ack_holds_checkpoint('commit', lambda body: {**body, 'uuid': str(uuid.uuid4())})

    def test_rejected_commit_ack_keeps_checkpoint_pending(self):
        self._malformed_mutation_ack_holds_checkpoint('commit', lambda body: {**body, 'success': False})

    def test_factory_dispatches_two_distinct_assignments_with_per_job_claims(self):
        self.backend.task_specific_output = True
        coordinator = ViewerServiceTests(methodName='runTest')
        coordinator.setUp()
        self.addCleanup(coordinator.tearDown)
        coordinator.service.close(); coordinator.queue.close()
        coordinator.config.prototype_state_dir = str(coordinator.root / 'guarded-factory-state')
        coordinator.config.viewer_require_guard = True
        coordinator.config.node_inventory = parse_node_inventory(json.dumps([
            dict(id='main', physicalHostId='main', origin='http://192.0.2.1:3000',
                 tokenEnv='NODE_TEST_TOKEN', maxPhotos=None, capabilities=['odm'])]), {})
        coordinator.queue = Queue(coordinator.config.prototype_state_dir, node_ids=('main',),
            treedar_node_ids=(), viewer_enabled=True, per_node_slots=True)
        coordinator.engines = {'main': FakeEngine('main', coordinator.fleet)}
        self.proxy._require_job_claim = True
        self.commit_reply(True)
        server = self.start_listener()
        context = ssl.create_default_context(cafile=str(self.root / 'server.pem'))
        context.load_cert_chain(self.root / 'client.pem', self.root / 'client.key')
        claims = {}
        def factory(identity):
            job = identity['viewerJobId']
            self.job = identity['engineTaskId']
            if job not in claims:
                claims[job] = self.envelope(viewerJobId=job, viewerGeneration=identity['viewerGeneration'],
                    engineTaskId=identity['engineTaskId'], nodeId=identity['nodeId'],
                    environment=identity['environment'], requestSha256=identity['requestSha256'],
                    nonce=hashlib.sha256(job.encode()).hexdigest())
            claim = claims[job]
            return GuardedNativeClient('127.0.0.1', server.server_port, context, TOKEN, timeout=10,
                job_claim=claim, operation_signer=operation_signer(self.proxy, KEY, claim))
        coordinator.service = ViewerService(coordinator.queue, coordinator.config,
            engines=coordinator.engines, probe=lambda *_: {'ready': True, 'maxImages': None, 'taskQueueCount': 0},
            guard_probe=lambda _: True, guarded_engine_factory=factory)
        coordinator.service._catalog = lambda: {'ready': True, 'commonOptions': [dict(x) for x in OPTIONS]}
        coordinator.service._selected_options = lambda _: [dict(x) for x in OPTIONS]
        def submit(*, lose_factory=False):
            identifier = coordinator.initialize(options=[])
            coordinator.upload(identifier, name='photo.jpg', content=b'abc')
            coordinator.service.commit(identifier)
            if lose_factory:
                coordinator.service._guarded_engine_factory = None
                coordinator.service.tick()
                self.assertEqual(coordinator.service.info(identifier)['status']['code'], 10)
                self.assertIsNone(coordinator.task(identifier)['node_id'])
                self.assertEqual(self.backend.counts()['initialize'], 0)
                coordinator.service._guarded_engine_factory = factory
            coordinator.service.tick()  # actual Viewer scheduler claims and dispatches
            self.assertEqual(coordinator.task(identifier)['phase'], 'processing')
            history = coordinator.service.store.guarded_dispatch_history(identifier)
            self.assertEqual([row['operation'] for row in history], ['init', 'upload', 'commit'])
            self.assertEqual([row['state'] for row in history], ['acknowledged'] * 3)
            self.assertEqual([row['sequence'] for row in history], [1, 2, 3])
            coordinator.service.tick()  # observe synthetic node completion
            self.assertEqual(coordinator.service.info(identifier)['status']['code'], 40)
            # Both assignments must use their own bound node for progress logs
            # and archive retrieval, not just dispatch successfully.
            engine_id = coordinator.task(identifier)['engine_uuid']
            self.assertEqual(coordinator.service.output(identifier, 1), ['', f'finished {engine_id}'])
            with coordinator.service.download(identifier)[0] as stream:
                self.assertEqual(stream.read(), task_archive(engine_id))
            return identifier
        first = submit(lose_factory=True)
        first_task = coordinator.task(first)
        old_client = coordinator.service._task_engine(first_task)
        detached_claim = old_client.job_claim
        detached_claim['claim']['engineTaskId'] = str(uuid.uuid4())
        self.assertEqual(old_client.job_claim['claim']['engineTaskId'], first_task['engine_uuid'])
        guarded_factory = coordinator.service._guarded_engine_factory
        coordinator.service._guarded_engine_factory = None
        with self.assertRaises(EngineUnavailable):
            coordinator.service._task_engine(first_task)
        coordinator.service._guarded_engine_factory = guarded_factory
        original_claim = copy.deepcopy(claims[first_task['job_id']])
        claims[first_task['job_id']]['claim']['viewerGeneration'] += 1
        with self.assertRaises(EngineUnavailable):
            coordinator.service._task_engine(first_task)
        claims[first_task['job_id']] = original_claim
        with coordinator.service.download(first)[0] as stream:
            self.assertEqual(stream.read(), task_archive(first_task['engine_uuid']))
        # Explicitly simulate a matching *trusted* completion receipt. Real
        # end-to-end authenticated outbox delivery is not wired by this test.
        with NodeGate('physical-engine') as gate:
            receipt = gate.snapshot()['reservation']['receiptId']
            journal = HostMutationJournal(gate, receipt).open()
            result = {**journal.job, 'status': 'succeeded', 'result': {'synthetic': True}}
            journal.checkpoint_completion(engine_task=first_task['engine_uuid'], outbox_id='d'*64, payload=result)
            journal.acknowledge_completion(outbox_id='d'*64, job_ack=result)
            journal.close()
        second = submit()
        second_task = coordinator.task(second)
        self.assertNotEqual(first_task['engine_uuid'], second_task['engine_uuid'])
        self.assertNotEqual(first_task['job_id'], second_task['job_id'])
        self.assertNotEqual(task_archive(first_task['engine_uuid']), task_archive(second_task['engine_uuid']))
        self.assertEqual(self.backend.counts()['initialize'], 2)
        self.assertEqual(self.backend.counts()['upload'], 2)
        self.assertEqual(self.backend.counts()['commit'], 2)
        with self.assertRaises(EngineUnavailable):
            old_client.read('/task/' + second_task['engine_uuid'] + '/info')
        # Even the correct former task ID cannot read the new node reservation.
        with self.assertRaises(EngineUnavailable):
            old_client.read('/task/' + first_task['engine_uuid'] + '/info')

    def test_service_status_output_archive_use_bound_guard_and_hold_pending(self):
        coordinator = ViewerServiceTests(methodName='runTest')
        coordinator.setUp()
        self.addCleanup(coordinator.tearDown)
        identifier = coordinator.initialize(options=[])
        coordinator.upload(identifier, name='photo.jpg', content=b'abc')
        coordinator.service.commit(identifier)
        coordinator.service.tick()
        task = coordinator.task(identifier)
        # The synthetic guard fixture is provisioned as main. Align this
        # isolated coordinator record before creating its signed claim.
        with coordinator.queue._db() as db:
            db.execute("UPDATE viewer_tasks SET node_id='main' WHERE uuid=?", (identifier,))
            db.execute("UPDATE jobs SET node_id='main' WHERE id=?", (task['job_id'],))
        task = coordinator.task(identifier)
        self.job = task['engine_uuid']
        self.task = {k: task[k] for k in ('name', 'engine_uuid', 'outputs', 'options')}
        claim = self.envelope(viewerJobId=task['job_id'], viewerGeneration=task['generation'],
                              requestSha256=protocol.digest(self.task))
        self.commit_reply(True)
        server = self.start_listener()
        context = ssl.create_default_context(cafile=str(self.root / 'server.pem'))
        context.load_cert_chain(self.root / 'client.pem', self.root / 'client.key')
        client = GuardedNativeClient('127.0.0.1', server.server_port, context, TOKEN, timeout=10,
            job_claim=claim, operation_signer=operation_signer(self.proxy, KEY, claim))
        client.initialize(self.task)
        coordinator.service.engines['main'] = client
        # Until the node has a durable commit, a poll must not report it as
        # running or complete, nor fetch its archive.
        coordinator.service._poll(task)
        self.assertEqual(coordinator.service.info(identifier)['status']['code'], 10)
        self.assertEqual(self.backend.counts()['download'], 0)
        with self.assertRaises(EngineUnavailable):
            client.read('/task/' + str(uuid.uuid4()) + '/info')
        for route, line in (('/info', 0), ('/task/' + self.job + '/remove', 0),
                            ('/task/' + self.job + '/output', -1),
                            ('/task/' + self.job + '/info', 1)):
            with self.subTest(route=route, line=line), self.assertRaises(EngineUnavailable):
                client.read(route, line=line)
        with self.assertRaises(EngineUnavailable):
            with client.download(str(uuid.uuid4())):
                self.fail('other task archive')
        source = self.spool / 'viewer-source.jpg'
        shutil.copyfile(task['files'][0]['path'], source)
        source.chmod(0o600)
        item = task['files'][0]
        client.upload(self.job, {'path': str(source), **{k: item[k] for k in ('name', 'bytes', 'sha256')}})
        client.action('commit', self.job)
        coordinator.service._backoff.clear()
        coordinator.service._poll(coordinator.task(identifier))
        self.assertEqual(coordinator.service.info(identifier)['status']['code'], 40)
        self.assertEqual(coordinator.service.output(identifier, 1), ['', 'synthetic phase two'])
        stream, size = coordinator.service.download(identifier)
        with stream:
            self.assertEqual(stream.read(), ARCHIVE)
        self.assertEqual(size, len(ARCHIVE))
        self.assertEqual(self.backend.counts()['download'], 1)


if __name__ == '__main__':
    unittest.main()
