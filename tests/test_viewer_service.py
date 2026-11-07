"""Synthetic coordinator integration: real queue/store, exact fake engine bytes.

These tests prove dispatch/transport contracts, not genuine photogrammetry or
Viewer output ingestion. No network, Docker, real datasets or host writes.
"""

import hashlib
import io
import json
from pathlib import Path
import tempfile
from datetime import datetime, timedelta, timezone
from dataclasses import replace
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid
import zipfile

from processing.maintenance import Conflict
from processing.odx_smoke_worker import SmokeError
from processing.prototype import PROFILE, VIEWER_PROFILE, Queue
from processing.viewer_service import EngineUnavailable, ViewerService


OPTIONS = [{'name': name, 'type': 'bool', 'value': False}
           for name in ('pc-ept', 'gltf', '3d-tiles')]
REQUEST_OPTIONS = [{'name': 'pc-ept', 'value': True}]


def synthetic_zip():
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, 'w') as archive:
        archive.writestr('synthetic-result.txt', b'owned synthetic transfer fixture\n')
    return stream.getvalue()


class DownloadResponse(io.BytesIO):
    status = 200

    def __init__(self, body, interrupt=False):
        super().__init__(body)
        self.headers = {'Content-Length': str(len(body))}
        self.interrupt = interrupt
        self.calls = 0

    def read(self, length=-1):
        self.calls += 1
        if self.interrupt:
            if self.calls > 1:
                raise OSError('synthetic connection lost')
            return super().read(7)
        return super().read(length)


class FakeFleet:
    def __init__(self):
        self.active = set()
        self.max_active = 0
        self.commits = []


class FakeEngine:
    def __init__(self, name, fleet):
        self.name, self.fleet = name, fleet
        self.tasks = {}
        self.calls = []
        self.fail = None
        self.commit_status = 20
        self.read_unavailable = False
        self.output_unavailable = False
        self.interrupt_download_once = False
        self.archive = synthetic_zip()
        self.download_calls = 0

    def initialize(self, task):
        identifier = task['engine_uuid']
        self.calls.append(('initialize', identifier))
        if identifier in self.tasks:
            raise AssertionError('duplicate engine initialization')
        self.tasks[identifier] = {'uuid': identifier, 'status': {'code': 10},
                                  'progress': 0, 'files': {}, 'cancel_requested': False}
        if self.fail == 'initialize':
            raise SmokeError('synthetic init response lost')

    def upload(self, identifier, item):
        self.calls.append(('upload', identifier, item['name']))
        content = Path(item['path']).read_bytes()
        if len(content) != item['bytes'] or hashlib.sha256(content).hexdigest() != item['sha256']:
            raise AssertionError('upload metadata does not match exact file bytes')
        self.tasks[identifier]['files'][item['name']] = content
        if self.fail == 'upload':
            raise SmokeError('synthetic upload response lost')

    def action(self, action, identifier):
        self.calls.append((action, identifier))
        task = self.tasks[identifier]
        if action == 'commit':
            if self.name + ':' + identifier in self.fleet.active:
                raise AssertionError('duplicate engine commit')
            task['status']['code'] = self.commit_status
            if self.commit_status == 20:
                self.fleet.active.add(self.name + ':' + identifier)
                self.fleet.max_active = max(self.fleet.max_active, len(self.fleet.active))
            self.fleet.commits.append((self.name, identifier))
            if self.fail == 'commit':
                raise SmokeError('synthetic commit response lost after start')
        elif action == 'cancel':
            task['cancel_requested'] = True
            # Deliberately remains running until a later explicit finish(50).
        else:
            raise AssertionError('unexpected engine mutation')
        return {'success': True, 'uuid': identifier}

    def read(self, route, *, missing=False, **query):
        _, _, identifier, kind = route.split('/')
        self.calls.append(('read', identifier, kind))
        if self.read_unavailable or (kind == 'output' and self.output_unavailable):
            raise EngineUnavailable('synthetic_read_unavailable')
        task = self.tasks.get(identifier)
        if task is None:
            if missing:
                return None
            raise EngineUnavailable('synthetic_task_not_found')
        if kind == 'info':
            return {'uuid': identifier, 'status': dict(task['status']), 'progress': task['progress']}
        if kind == 'output':
            return ['synthetic engine output', '', 'comma,record'][query.get('line', 0):]
        raise AssertionError('unexpected engine read')

    def finish(self, identifier, code=40):
        self.tasks[identifier]['status']['code'] = code
        self.tasks[identifier]['progress'] = 100 if code == 40 else 50
        self.fleet.active.discard(self.name + ':' + identifier)

    def download(self, identifier):
        self.calls.append(('download', identifier))
        self.download_calls += 1
        interrupt = self.interrupt_download_once
        self.interrupt_download_once = False
        return DownloadResponse(self.archive, interrupt=interrupt)


class ViewerServiceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.config = SimpleNamespace(prototype_state_dir=str(self.root / 'state'),
            viewer_max_input_bytes=20 * 1024 ** 3, viewer_max_archive_bytes=4 * 1024 ** 2,
            local_node_url='http://synthetic-cluster.invalid:3000', local_node_token='private-cluster',
            main_node_url='http://synthetic-main.invalid:3000', main_node_token='private-main',
            local_max_photos=1000, local_max_input_bytes=20 * 1024 ** 3)
        self.fleet = FakeFleet()
        self.engines = {name: FakeEngine(name, self.fleet) for name in ('cluster', 'main')}
        self.busy = {}
        self.unavailable = set()
        self.option_catalog = [dict(item) for item in OPTIONS]
        self.catalog_ready = True
        self.make_queue_service()

    def make_queue_service(self):
        self.queue = Queue(self.root / 'state', node_ids=('cluster', 'main'),
                           treedar_node_ids=('main',), viewer_enabled=True)
        self.service = ViewerService(self.queue, self.config, engines=self.engines, probe=self.probe)
        self.service._catalog = lambda: {'ready': self.catalog_ready,
                                         'commonOptions': [dict(item) for item in self.option_catalog]}

    def set_cluster_max_photos(self, value):
        self.config.local_max_photos = value
        self.service.nodes = tuple(replace(node, max_photos=value) if node.node_id == 'cluster' else node
                                   for node in self.service.nodes)

    def tearDown(self):
        self.service.close()
        self.queue.close()
        self.temporary.cleanup()

    def probe(self, role, origin, token):
        node = 'cluster' if role == 'local' else 'main'
        return {'ready': node not in self.unavailable,
                'maxImages': 1000 if node == 'cluster' else None,
                'taskQueueCount': self.busy.get(node, sum(x.startswith(node + ':') for x in self.fleet.active))}

    def initialize(self, *, environment='production', options=None):
        identifier = str(uuid.uuid4())
        self.service.initialize(identifier, 'Synthetic survey',
                                REQUEST_OPTIONS if options is None else options, None, environment)
        return identifier

    def upload(self, identifier, name='photo.jpg', content=b'owned synthetic photo bytes'):
        self.service.upload(identifier, name, io.BytesIO(content), len(content))

    def submit(self, *, environment='production', count=1, options=None):
        identifier = self.initialize(environment=environment, options=options)
        for index in range(count):
            self.upload(identifier, name=f'photo-{index:05}.jpg')
        self.service.commit(identifier)
        return identifier

    def task(self, identifier):
        return self.service.store.task(identifier)

    def engine_for(self, identifier):
        return self.engines[self.task(identifier)['node_id']]

    def finish(self, identifier, code=40):
        task = self.task(identifier)
        self.engines[task['node_id']].finish(task['engine_uuid'], code)

    def test_exact_upload_bytes_use_small_node_and_main_is_untouched(self):
        identifier = self.initialize()
        content = b'\xff\xd8\0\r\n--binary' * 8192
        self.upload(identifier, 'owned.jpg', content)
        self.service.commit(identifier)
        self.service.tick()
        task = self.task(identifier)
        self.assertEqual(task['node_id'], 'cluster')
        self.assertEqual(self.engines['cluster'].tasks[task['engine_uuid']]['files']['owned.jpg'], content)
        self.assertEqual(self.engines['main'].calls, [])
        self.assertEqual(self.service.info(identifier)['status']['code'], 20)
        self.assertEqual(self.fleet.max_active, 1)

    def test_commit_ack_keeps_viewer_queued_until_engine_confirms_start(self):
        identifier = self.submit()
        engine = self.engines['cluster']
        engine.commit_status = 10
        self.service.tick()
        task = self.task(identifier)
        self.assertEqual(task['phase'], 'awaiting_engine_start')
        self.assertEqual(self.service.info(identifier)['status']['code'], 10)
        self.service.tick()
        self.assertEqual(self.task(identifier)['phase'], 'awaiting_engine_start')
        self.assertEqual(self.service.info(identifier)['status']['code'], 10)
        engine.tasks[task['engine_uuid']]['status']['code'] = 20
        self.fleet.active.add('cluster:' + task['engine_uuid'])
        self.service.tick()
        self.assertEqual(self.task(identifier)['phase'], 'processing')
        self.assertEqual(self.service.info(identifier)['status']['code'], 20)

    def test_over_cluster_photo_ceiling_uses_main_and_never_cluster(self):
        # A lowered synthetic threshold exercises the same boundary without
        # fabricating 1,001 files on disk; the policy's 1,000 boundary is separate.
        self.set_cluster_max_photos(2)
        identifier = self.submit(count=3)
        self.service.tick()
        self.assertEqual(self.task(identifier)['node_id'], 'main')
        self.assertEqual(self.engines['cluster'].calls, [])
        self.assertEqual(self.fleet.max_active, 1)

    def test_priority_fifo_and_one_heavy_job_per_physical_node(self):
        staging = self.submit(environment='staging')
        production_first = self.submit(count=2)
        production_second = self.submit()
        self.set_cluster_max_photos(1)
        self.service.tick()
        self.assertEqual(self.task(production_first)['node_id'], 'main')
        self.assertEqual(self.task(production_second)['node_id'], 'cluster')
        self.assertIsNone(self.task(staging)['node_id'])
        self.assertEqual(self.fleet.max_active, 2)
        self.finish(production_first)
        self.finish(production_second)
        self.service.tick()
        self.assertEqual(self.task(staging)['node_id'], 'cluster')
        self.finish(staging)
        self.service.tick()
        self.assertTrue(all(self.service.info(i)['status']['code'] == 40
                            for i in (production_first, production_second, staging)))
        self.assertEqual(self.fleet.max_active, 2)
        self.assertEqual(len(self.fleet.commits), 3)

    def test_tree_running_on_main_does_not_block_cluster_photo_slot(self):
        tree, _ = self.queue.submit({'requestId': 'tree', 'environment': 'production', 'profile': PROFILE})
        claim = self.queue.claim({'workerId': 'tree-worker', 'nodeId': 'main'})
        identifier = self.submit(environment='staging')
        self.service.tick()
        self.assertEqual(self.task(identifier)['node_id'], 'cluster')
        self.assertEqual(self.queue.get(tree['id'])['nodeId'], 'main')
        self.assertIsNotNone(claim)

    def test_waiting_production_tree_keeps_staging_photo_queued(self):
        self.queue.submit({'requestId': 'production-tree', 'environment': 'production',
                           'profile': PROFILE})
        identifier = self.submit(environment='staging')
        self.service.tick()
        self.assertEqual(self.task(identifier)['job']['status'], 'queued')
        self.assertIsNone(self.task(identifier)['node_id'])
        self.assertFalse(self.fleet.commits)

    def test_busy_small_node_falls_back_to_available_main_for_small_job(self):
        identifier = self.submit()
        self.busy['cluster'] = 1
        self.service.tick()
        self.assertEqual(self.task(identifier)['node_id'], 'main')
        self.assertTrue(self.engines['main'].calls)
        self.assertFalse(self.engines['cluster'].calls)

    def test_large_job_waits_for_main_without_overflow_to_small_node(self):
        self.set_cluster_max_photos(1)
        identifier = self.submit(count=2)
        self.unavailable.add('main')
        self.service.tick()
        self.assertIsNone(self.task(identifier)['node_id'])
        self.assertFalse(self.fleet.commits)

    def test_maintenance_drain_keeps_priority_head_queued(self):
        self.set_cluster_max_photos(1)
        production = self.submit(count=2)
        staging = self.submit(environment='staging')
        now = datetime.now(timezone.utc)
        drain = (now - timedelta(seconds=2)).isoformat()
        not_before = (now + timedelta(hours=1)).isoformat()
        self.queue.schedule_node('main', {'expectedRevision': 0, 'drainAt': drain,
            'notBefore': not_before, 'afterJobId': None, 'targetImage': 'sha256:' + 'a' * 64})
        self.service.tick()
        self.assertEqual(self.task(production)['job']['status'], 'queued')
        self.assertEqual(self.task(staging)['job']['status'], 'queued')
        self.assertFalse(self.fleet.commits)

    def test_new_production_during_selection_is_not_claimed_using_staging_routing(self):
        staging = self.submit(environment='staging')
        production = self.initialize()
        self.upload(production, 'a.jpg')
        self.upload(production, 'b.jpg')
        self.set_cluster_max_photos(1)
        original = self.queue.claim
        inserted = []

        def insert_then_claim(payload, **kwargs):
            if not inserted:
                inserted.append(True)
                self.service.commit(production)
            return original(payload, **kwargs)

        with patch.object(self.queue, 'claim', side_effect=insert_then_claim):
            self.service.tick()
        self.assertFalse(self.fleet.commits)
        self.assertEqual(self.task(staging)['job']['status'], 'queued')
        self.service.tick()
        self.assertEqual(self.task(production)['node_id'], 'main')

    def test_completed_engine_is_reconciled_after_coordinator_restart_without_replay(self):
        identifier = self.submit()
        self.service.tick()
        engine_uuid = self.task(identifier)['engine_uuid']
        self.service.close()
        self.queue.close()
        self.engines['cluster'].finish(engine_uuid)
        self.make_queue_service()
        self.assertEqual(self.task(identifier)['job']['status'], 'held')
        self.assertEqual(self.service.info(identifier)['status']['code'], 20)
        self.service.tick()
        self.assertEqual(self.service.info(identifier)['status']['code'], 40)
        self.assertEqual(sum(c[0] == 'initialize' for c in self.engines['cluster'].calls), 1)
        self.assertEqual(len(self.fleet.commits), 1)

    def test_restart_before_engine_start_keeps_viewer_queued_until_code20(self):
        identifier = self.submit()
        engine = self.engines['cluster']
        engine.commit_status = 10
        self.service.tick()
        task = self.task(identifier)
        self.assertEqual(task['phase'], 'awaiting_engine_start')
        self.service.close()
        self.queue.close()
        self.make_queue_service()
        self.assertEqual(self.task(identifier)['job']['status'], 'held')
        self.assertEqual(self.service.info(identifier)['status']['code'], 10)
        self.service.tick()
        self.assertEqual(self.service.info(identifier)['status']['code'], 10)
        engine.tasks[task['engine_uuid']]['status']['code'] = 20
        self.fleet.active.add('cluster:' + task['engine_uuid'])
        self.service.tick()
        self.assertEqual(self.task(identifier)['phase'], 'processing')
        self.assertEqual(self.service.info(identifier)['status']['code'], 20)
        self.assertEqual(sum(call[0] == 'initialize' for call in engine.calls), 1)
        self.assertEqual(len(self.fleet.commits), 1)

    def test_engine_read_outage_backs_off_and_retains_slot_then_recovers(self):
        first = self.submit()
        second = self.submit()
        self.service.tick()
        self.assertEqual(self.task(first)['node_id'], 'cluster')
        self.assertEqual(self.task(second)['node_id'], 'main')
        self.engines['cluster'].read_unavailable = True
        self.service.tick()
        initial = self.service._backoff[first]
        self.service.tick()
        self.assertEqual(self.service._backoff[first], initial)
        self.assertEqual(self.task(first)['job']['status'], 'running')
        self.assertEqual(self.task(second)['job']['status'], 'running')
        self.finish(first)
        self.engines['cluster'].read_unavailable = False
        self.service._backoff[first] = (0, initial[1])
        self.service.tick()
        self.assertEqual(self.service.info(first)['status']['code'], 40)
        self.assertEqual(self.task(second)['node_id'], 'main')
        self.assertEqual(self.fleet.max_active, 2)

    def test_repeated_public_commit_never_initializes_or_executes_twice(self):
        identifier = self.submit()
        self.service.commit(identifier)
        self.service.tick()
        self.service.commit(identifier)
        self.service.tick()
        self.assertEqual(len(self.fleet.commits), 1)
        self.assertEqual(sum(c[0] == 'initialize' for c in self.engines['cluster'].calls), 1)

    def test_lost_init_or_upload_ack_holds_slot_and_never_replays(self):
        for failure in ('initialize', 'upload'):
            with self.subTest(failure=failure):
                identifier = self.submit()
                self.engines['cluster'].fail = failure
                self.service.tick()
                self.assertEqual(self.task(identifier)['job']['status'], 'held')
                before = list(self.engines['cluster'].calls)
                self.service.tick()
                self.assertEqual(self.engines['cluster'].calls, before)
                self.assertEqual(self.service.info(identifier)['status']['code'], 30)
                # Resolve only the fake's known-not-started engine so the next
                # independent subcase can use this shared test fixture.
                job = self.task(identifier)['job']
                self.queue.operator(job['id'], 'resolve', {'note': 'synthetic engine never committed',
                                                         'workerStopped': True})
                self.engines['cluster'].fail = None

    def test_lost_commit_ack_polls_known_uuid_without_replay_and_completes(self):
        identifier = self.submit()
        self.engines['cluster'].fail = 'commit'
        self.service.tick()
        self.assertIn(self.task(identifier)['phase'], ('commit_pending', 'processing'))
        self.assertEqual(self.task(identifier)['job']['status'], 'running')
        self.finish(identifier)
        self.service.tick()
        self.assertEqual(self.service.info(identifier)['status']['code'], 40)
        self.assertEqual(len(self.fleet.commits), 1)
        self.assertEqual(sum(c[0] == 'commit' for c in self.engines['cluster'].calls), 1)

    def test_running_cancel_does_not_release_slot_until_engine_reports_50(self):
        identifier = self.submit()
        next_id = self.submit()
        third_id = self.submit()
        self.service.tick()
        self.service.cancel(identifier)
        self.service.cancel(identifier)
        self.assertEqual(self.task(identifier)['job']['status'], 'running')
        self.assertEqual(self.task(next_id)['node_id'], 'main')
        self.assertIsNone(self.task(third_id)['node_id'])
        self.service.tick()
        self.assertIsNone(self.task(third_id)['node_id'])
        self.assertEqual(sum(c[0] == 'cancel' for c in self.engines['cluster'].calls), 1)
        self.finish(identifier, 50)
        self.service.tick()
        self.assertEqual(self.service.info(identifier)['status']['code'], 50)
        self.assertEqual(self.task(third_id)['node_id'], 'cluster')
        self.assertEqual(self.fleet.max_active, 2)

    def test_lost_commit_ack_queued_upstream_retains_slot_until_later_completion(self):
        identifier = self.submit()
        self.engines['cluster'].fail = 'commit'
        self.service.tick()
        task = self.task(identifier)
        self.engines['cluster'].tasks[task['engine_uuid']]['status']['code'] = 10
        self.service.tick()
        self.assertEqual(self.task(identifier)['job']['status'], 'running')
        self.assertIn(self.task(identifier)['phase'], ('commit_pending', 'processing'))
        self.engines['cluster'].tasks[task['engine_uuid']]['status']['code'] = 20
        self.service.tick()
        self.finish(identifier)
        self.service.tick()
        self.assertEqual(self.service.info(identifier)['status']['code'], 40)
        self.assertEqual(sum(c[0] == 'commit' for c in self.engines['cluster'].calls), 1)

    def test_download_twice_matches_hash_and_retains_archive_and_inputs(self):
        identifier = self.submit()
        self.service.tick()
        self.finish(identifier)
        self.service.tick()
        streams = []
        for _ in range(2):
            stream, length = self.service.download(identifier)
            with stream:
                body = stream.read()
            self.assertEqual(length, len(body))
            streams.append(hashlib.sha256(body).hexdigest())
        self.assertEqual(streams, [hashlib.sha256(self.engines['cluster'].archive).hexdigest()] * 2)
        self.assertEqual(self.engines['cluster'].download_calls, 1)
        task = self.task(identifier)
        self.service.remove(identifier)
        self.assertTrue(Path(task['archive_path']).is_file())
        self.assertTrue(Path(task['files'][0]['path']).is_file())

    def test_interrupted_archive_get_can_retry_without_new_execution(self):
        identifier = self.submit()
        self.service.tick()
        self.finish(identifier)
        self.service.tick()
        self.engines['cluster'].interrupt_download_once = True
        with self.assertRaises((EngineUnavailable, OSError, Conflict)):
            self.service.download(identifier)
        stream, _ = self.service.download(identifier)
        with stream:
            body = stream.read()
        self.assertEqual(hashlib.sha256(body).hexdigest(), hashlib.sha256(self.engines['cluster'].archive).hexdigest())
        self.assertEqual(self.engines['cluster'].download_calls, 2)
        self.assertEqual(len(self.fleet.commits), 1)

    def test_reset_public_uuid_assigns_new_private_engine_uuid(self):
        identifier = self.submit()
        self.service.tick()
        first_engine_uuid = self.task(identifier)['engine_uuid']
        self.finish(identifier)
        self.service.tick()
        self.service.remove(identifier)
        self.service.initialize(identifier, 'Retry survey', REQUEST_OPTIONS, None, 'production')
        self.upload(identifier)
        self.service.commit(identifier)
        self.service.tick()
        self.assertNotEqual(self.task(identifier)['engine_uuid'], first_engine_uuid)
        self.assertEqual(len(self.fleet.commits), 2)
        self.assertEqual(len(self.engines['cluster'].tasks), 2)

    def test_http200_asset_not_ready_json_is_not_cached_as_zip(self):
        identifier = self.submit()
        self.service.tick()
        self.finish(identifier)
        self.service.tick()
        engine = self.engines['cluster']
        expected = engine.archive
        engine.archive = b'{"error":"Asset not ready"}'
        with self.assertRaises(EngineUnavailable):
            self.service.download(identifier)
        self.assertIsNone(self.task(identifier)['archive_path'])
        engine.archive = expected
        stream, length = self.service.download(identifier)
        with stream:
            self.assertEqual(stream.read(), expected)
        self.assertEqual(length, len(expected))
        self.assertEqual(len(self.fleet.commits), 1)

    def test_dynamic_options_removed_after_enqueue_prevent_dispatch(self):
        identifier = self.submit()
        self.option_catalog = [item for item in self.option_catalog if item['name'] != 'pc-ept']
        with self.assertRaises((ValueError, Conflict)):
            self.service.tick()
        self.assertEqual(self.task(identifier)['job']['status'], 'queued')
        self.assertFalse(self.fleet.commits)
        with self.assertRaises(ValueError):
            self.service.initialize(str(uuid.uuid4()), options=[{'name': 'unsupported', 'value': True}])

    def test_native_output_cursor_preserves_blank_and_comma_records(self):
        identifier = self.submit()
        self.service.tick()
        self.assertEqual(self.service.output(identifier, 1), ['', 'comma,record'])
        self.engines['cluster'].output_unavailable = True
        self.service.store.update(identifier, output_lines=['retained output'])
        self.assertEqual(self.service.output(identifier, 0), ['retained output'])


if __name__ == '__main__':
    unittest.main()
