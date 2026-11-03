"""Synthetic coordinator integration: real queue/store, exact fake engine bytes.

These tests prove dispatch/transport contracts, not genuine photogrammetry or
Viewer output ingestion. No network, Docker, real datasets or host writes.
"""

import hashlib
from concurrent.futures import ThreadPoolExecutor
import subprocess
import sys
import io
import json
import os
import subprocess
import sys
from pathlib import Path
import tempfile
import threading
from datetime import datetime, timezone
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
        # Fake engine instances in separate processes represent the same
        # retained result; ZIP's default current timestamp would change bytes.
        entry = zipfile.ZipInfo('synthetic-result.txt', date_time=(2020, 1, 1, 0, 0, 0))
        archive.writestr(entry, b'owned synthetic transfer fixture\n')
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
            task['status']['code'] = 20
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
        self.service._selected_options = lambda _node: [dict(item) for item in self.option_catalog]

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
        self.assertEqual(self.service.info(identifier)['status']['code'], 10)
        self.service.tick()
        self.assertEqual(self.service.info(identifier)['status']['code'], 20)
        self.assertEqual(self.fleet.max_active, 1)

    def test_capabilities_report_the_enforced_cumulative_input_limit(self):
        from processing.viewer_http import MAX_UPLOAD_BYTES
        self.assertEqual(self.service.capabilities()['ledgetopMaxInputBytes'], self.service.store.max_input_bytes)
        self.assertEqual(self.service.capabilities()['ledgetopMaxUploadRequestBytes'], MAX_UPLOAD_BYTES)

    def test_real_http_batch_over_limit_retains_prior_file_and_retries_safely(self):
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        from urllib.error import HTTPError
        from urllib.request import Request, urlopen
        from processing.viewer_http import ViewerFacade
        from tests.test_viewer_http import multipart, BOUNDARY
        identifier = self.initialize()
        self.service.store.max_input_bytes = 8
        spool = self.root / 'http-spool'; spool.mkdir(mode=0o700)
        facade = ViewerFacade(self.service, 'synthetic-production-token', temp_dir=spool)
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_): pass
            def do_POST(self): facade.handle(self)
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
        try:
            url = f'http://127.0.0.1:{server.server_port}/task/new/upload/{identifier}?token=synthetic-production-token'
            def upload(parts):
                request = Request(url, data=multipart(parts), headers={'Content-Type': 'multipart/form-data; boundary=' + BOUNDARY})
                return urlopen(request, timeout=4)
            with self.assertRaises(HTTPError) as caught:
                upload([('images', 'one.jpg', b'123456'), ('images', 'two.jpg', b'789')])
            self.assertEqual(caught.exception.code, 413)
            self.assertEqual(json.load(caught.exception), {'error': 'task_input_limit_exceeded'})
            task = self.task(identifier)
            self.assertEqual(task['input_bytes'], 6)
            self.assertEqual([f['name'] for f in task['files']], ['one.jpg'])
            self.assertEqual(Path(task['files'][0]['path']).read_bytes(), b'123456')
            self.assertFalse(list(Path(task['directory']).glob('.incoming-*')))
            self.assertEqual(list(spool.iterdir()), [])
            with upload([('images', 'one.jpg', b'123456')]) as response:
                self.assertEqual(json.load(response), {'success': True})
            self.assertEqual(self.task(identifier)['input_bytes'], 6)
        finally:
            server.shutdown(); server.server_close(); thread.join(timeout=2)
            self.assertFalse(thread.is_alive())

    def test_over_cluster_photo_ceiling_uses_main_and_never_cluster(self):
        # A lowered synthetic threshold exercises the same boundary without
        # fabricating 1,001 files on disk; the policy's 1,000 boundary is separate.
        self.config.local_max_photos = 2
        identifier = self.submit(count=3)
        self.service.tick()
        self.assertEqual(self.task(identifier)['node_id'], 'main')
        self.assertEqual(self.engines['cluster'].calls, [])
        self.assertEqual(self.fleet.max_active, 1)

    def test_priority_fifo_and_global_execution_slot_across_nodes(self):
        staging = self.submit(environment='staging')
        production_first = self.submit(count=2)
        production_second = self.submit()
        self.config.local_max_photos = 1
        self.service.tick()
        self.assertEqual(self.task(production_first)['node_id'], 'main')
        self.assertIsNone(self.task(production_second)['node_id'])
        self.assertIsNone(self.task(staging)['node_id'])
        self.finish(production_first)
        self.service.tick()
        self.assertEqual(self.task(production_second)['node_id'], 'cluster')
        self.finish(production_second)
        self.service.tick()
        self.assertEqual(self.task(staging)['node_id'], 'cluster')
        self.finish(staging)
        self.service.tick()
        self.assertTrue(all(self.service.info(i)['status']['code'] == 40
                            for i in (production_first, production_second, staging)))
        self.assertEqual(self.fleet.max_active, 1)
        self.assertEqual(len(self.fleet.commits), 3)

    def test_running_staging_finishes_before_later_production_then_priority_applies(self):
        # Use a low synthetic boundary to exercise the real dispatcher without
        # creating thousands of files. The 1000/1001 boundary is tested by policy.
        self.config.local_max_photos=1
        running_staging=self.submit(environment='staging')
        self.service.tick()
        self.assertEqual(self.task(running_staging)['node_id'],'cluster')
        self.assertEqual(self.task(running_staging)['job']['status'],'running')
        waiting_staging=self.submit(environment='staging')
        production_large=self.submit(environment='production',count=2)
        production_small=self.submit(environment='production')
        self.service.tick()
        self.assertEqual(self.task(running_staging)['job']['status'],'running')
        for identifier in (waiting_staging,production_large,production_small):
            self.assertEqual(self.task(identifier)['job']['status'],'queued')
            self.assertIsNone(self.task(identifier)['node_id'])
        self.assertEqual(len(self.fleet.commits),1)
        self.assertFalse(any(call[0]=='cancel' for engine in self.engines.values() for call in engine.calls))
        self.finish(running_staging);self.service.tick()
        self.assertEqual(self.task(production_large)['node_id'],'main')
        self.assertIsNone(self.task(production_small)['node_id'])
        self.assertIsNone(self.task(waiting_staging)['node_id'])
        self.finish(production_large);self.service.tick()
        self.assertEqual(self.task(production_small)['node_id'],'cluster')
        self.assertIsNone(self.task(waiting_staging)['node_id'])
        self.finish(production_small);self.service.tick()
        self.assertEqual(self.task(waiting_staging)['node_id'],'cluster')
        self.finish(waiting_staging);self.service.tick()
        self.assertEqual(self.fleet.max_active,1)
        self.assertEqual([node for node,_ in self.fleet.commits],['cluster','main','cluster','cluster'])
        self.assertTrue(all(self.task(identifier)['job']['status']=='succeeded' for identifier in
            (running_staging,production_large,production_small,waiting_staging)))

    def test_small_production_precedes_large_staging_while_main_busy(self):
        self.config.local_max_photos=1
        staging=self.submit(environment='staging',count=2)
        production=self.submit(environment='production')
        self.busy['main']=1
        self.service.tick()
        self.assertEqual(self.task(production)['node_id'],'cluster')
        self.assertEqual(self.task(staging)['job']['status'],'queued')
        self.assertFalse(self.engines['main'].calls)
        self.finish(production);self.service.tick()
        self.assertEqual(self.task(staging)['job']['status'],'queued')
        self.assertIsNone(self.task(staging)['node_id'])
        self.assertFalse(self.engines['main'].calls)
        self.busy['main']=0;self.service.tick()
        self.assertEqual(self.task(staging)['node_id'],'main')
        self.finish(staging);self.service.tick()
        self.assertEqual([node for node,_ in self.fleet.commits],['cluster','main'])
        self.assertEqual(self.fleet.max_active,1)

    def test_unavailable_large_production_head_keeps_small_and_staging_queued(self):
        self.config.local_max_photos=1
        production_large=self.submit(environment='production',count=2)
        production_small=self.submit(environment='production')
        staging=self.submit(environment='staging')
        self.busy['main']=1
        self.service.tick()
        for identifier in (production_large,production_small,staging):
            self.assertEqual(self.task(identifier)['job']['status'],'queued')
            self.assertIsNone(self.task(identifier)['node_id'])
        self.assertEqual(self.fleet.commits,[])
        self.busy['main']=0;self.service.tick()
        self.assertEqual(self.task(production_large)['node_id'],'main')
        self.finish(production_large);self.service.tick()
        self.assertEqual(self.task(production_small)['node_id'],'cluster')
        self.finish(production_small);self.service.tick()
        self.assertEqual(self.task(staging)['node_id'],'cluster')
        self.finish(staging);self.service.tick()
        self.assertEqual([node for node,_ in self.fleet.commits],['main','cluster','cluster'])
        self.assertEqual(self.fleet.max_active,1)

    def test_opt_in_small_overflows_to_main_while_cluster_occupied(self):
        self.queue.per_node_slots = True
        first = self.submit()
        second = self.submit()
        third = self.submit(environment='staging')
        self.service.tick()
        self.service.tick()
        self.service.tick()
        self.assertEqual(self.task(first)['node_id'], 'cluster')
        self.assertEqual(self.task(second)['node_id'], 'main')
        self.assertIsNone(self.task(third)['node_id'])
        self.assertEqual(self.fleet.max_active, 2)
        self.finish(first)
        self.service.tick()
        self.assertIsNone(self.task(third)['node_id'])
        self.finish(second)
        self.service.tick()
        self.assertEqual(self.task(third)['node_id'], 'cluster')

    def test_parallel_transfer_does_not_block_other_node_or_release_its_slot(self):
        self.queue.per_node_slots = True
        self.service._parallel_dispatch = True
        entered, release = threading.Event(), threading.Event()
        original_upload = self.engines['cluster'].upload
        def slow_upload(identifier, item):
            entered.set()
            if not release.wait(10):
                raise RuntimeError('synthetic transfer timeout')
            return original_upload(identifier, item)
        self.engines['cluster'].upload = slow_upload
        first, second = self.submit(), self.submit()
        try:
            self.service.tick()
            self.assertTrue(entered.wait(5))
            first_job = self.task(first)['job_id']
            self.assertEqual(self.task(first)['node_id'], 'cluster')
            with self.assertRaisesRegex(Conflict, 'submission in progress'):
                self.service.cancel(first)
            with self.assertRaisesRegex(Conflict, 'submission in progress'):
                self.service.operator(first_job, 'hold', {'note': 'test'})
            self.service.tick()
            deadline = threading.Event()
            for _ in range(100):
                if self.task(second)['phase'] == 'processing':
                    break
                deadline.wait(0.02)
            self.assertEqual(self.task(second)['node_id'], 'main')
            self.assertEqual(self.task(second)['phase'], 'processing')
            self.assertEqual(self.task(first)['job']['status'], 'running')
            self.finish(second)
            self.service.tick()
            self.assertEqual(self.task(second)['job']['status'], 'succeeded')
            self.assertEqual(self.task(first)['job']['status'], 'running')
        finally:
            release.set()
            self.service.close()

    def test_parallel_dispatch_requires_explicit_opt_in_and_per_node_slots(self):
        self.assertFalse(self.service._parallel_dispatch)
        self.config.viewer_parallel_dispatch = True
        with self.assertRaisesRegex(ValueError, 'requires per-node slots'):
            ViewerService(self.queue, self.config, engines=self.engines, probe=self.probe)
        self.queue.per_node_slots = True
        parallel = ViewerService(self.queue, self.config, engines=self.engines, probe=self.probe)
        try:
            self.assertTrue(parallel._parallel_dispatch)
        finally:
            parallel.close()

    def test_parallel_uncertain_upload_holds_its_node_and_other_can_run(self):
        self.queue.per_node_slots = True
        self.service._parallel_dispatch = True
        self.engines['cluster'].fail = 'upload'
        first, second = self.submit(), self.submit()
        self.service.tick()
        wait = threading.Event()
        for _ in range(100):
            if self.task(first)['job']['status'] == 'held':
                break
            wait.wait(0.02)
        self.assertEqual(self.task(first)['job']['status'], 'held')
        self.assertEqual(self.task(first)['phase'], 'held')
        self.service.tick()
        for _ in range(100):
            if self.task(second)['phase'] == 'processing':
                break
            wait.wait(0.02)
        self.assertEqual(self.task(second)['node_id'], 'main')
        self.assertEqual(self.task(second)['phase'], 'processing')
        self.assertEqual(len([x for x in self.engines['cluster'].calls if x[0] == 'upload']), 1)

    def test_shutdown_drain_keeps_sender_owned_until_it_exits(self):
        self.queue.per_node_slots = True
        self.service._parallel_dispatch = True
        entered, release, drained = threading.Event(), threading.Event(), threading.Event()
        original_upload = self.engines['cluster'].upload
        def slow_upload(identifier, item):
            entered.set()
            if not release.wait(10):
                raise RuntimeError('synthetic transfer timeout')
            return original_upload(identifier, item)
        self.engines['cluster'].upload = slow_upload
        first, second = self.submit(), self.submit()
        self.service.tick()
        self.assertTrue(entered.wait(5))
        self.service._stop.set()
        waiter = threading.Thread(target=lambda: (self.service.wait_for_transfers(), drained.set()))
        waiter.start()
        try:
            self.assertFalse(drained.wait(0.1))
            self.assertEqual(self.task(first)['job']['status'], 'running')
            self.service.tick()
            self.assertIsNone(self.task(second)['node_id'])
        finally:
            release.set()
            waiter.join(5)
        self.assertFalse(waiter.is_alive())
        self.assertTrue(drained.is_set())
        self.assertEqual(self.task(first)['job']['status'], 'held')
        self.assertEqual(self.task(first)['error'], 'dispatch_interrupted_for_shutdown')
        self.assertFalse(any(call[0] == 'commit' for call in self.engines['cluster'].calls))

    def test_slow_inbound_upload_still_blocks_scheduler_store_access(self):
        """Characterizes a remaining deployment blocker, not desired behavior."""
        queued = self.submit()
        incoming = self.initialize()
        entered, release, tick_done = threading.Event(), threading.Event(), threading.Event()
        errors = []
        class SlowStream(io.BytesIO):
            def read(self, size=-1):
                if not entered.is_set():
                    entered.set()
                    if not release.wait(5):
                        raise RuntimeError('synthetic inbound timeout')
                return super().read(size)
        def upload():
            try:
                self.service.upload(incoming, 'slow.jpg', SlowStream(b'abc'), 3)
            except BaseException as exc:
                errors.append(exc)
        def tick():
            try:
                self.service.tick()
            except BaseException as exc:
                errors.append(exc)
            finally:
                tick_done.set()
        uploader = threading.Thread(target=upload)
        scheduler = threading.Thread(target=tick)
        uploader.start()
        try:
            self.assertTrue(entered.wait(5))
            scheduler.start()
            self.assertFalse(tick_done.wait(0.1), 'scheduler unexpectedly passed the store-wide lock')
        finally:
            release.set()
            uploader.join(5)
            if scheduler.ident is not None:
                scheduler.join(5)
        self.assertFalse(uploader.is_alive())
        self.assertFalse(scheduler.is_alive())
        self.assertEqual(errors, [])
        self.assertTrue(tick_done.is_set())
        self.assertEqual(self.task(queued)['job']['status'], 'running')

    def test_maintenance_drain_holds_inflight_transfer_before_next_mutation(self):
        self.queue.per_node_slots = True
        self.service._parallel_dispatch = True
        entered, release = threading.Event(), threading.Event()
        original_upload = self.engines['cluster'].upload
        def slow_upload(identifier, item):
            entered.set()
            if not release.wait(10):
                raise RuntimeError('synthetic transfer timeout')
            return original_upload(identifier, item)
        self.engines['cluster'].upload = slow_upload
        identifier = self.submit(count=2)
        self.service.tick()
        self.assertTrue(entered.wait(5))
        now = datetime.now(timezone.utc).isoformat()
        node = self.queue.schedule_node('cluster', {'expectedRevision': 0, 'drainAt': now,
            'notBefore': now, 'afterJobId': None, 'targetImage': 'sha256:' + 'a' * 64})
        try:
            self.assertEqual(node['state'], 'draining')
        finally:
            release.set()
            self.service.wait_for_transfers()
        task = self.task(identifier)
        self.assertEqual(task['job']['status'], 'held')
        self.assertEqual(task['error'], 'dispatch_interrupted_for_maintenance')
        self.assertEqual(len([c for c in self.engines['cluster'].calls if c[0] == 'upload']), 1)
        self.assertFalse(any(c[0] == 'commit' for c in self.engines['cluster'].calls))
        with self.assertRaisesRegex(Conflict, 'owns running or held work'):
            self.queue.begin_node_update('cluster', {'expectedRevision': node['revision'],
                                                      'planId': node['planId']})

    def test_maintenance_explicitly_after_claimed_job_allows_that_job_to_finish_submission(self):
        self.queue.per_node_slots = True
        self.service._parallel_dispatch = True
        entered, release = threading.Event(), threading.Event()
        original_upload = self.engines['cluster'].upload
        def slow_upload(identifier, item):
            entered.set()
            if not release.wait(10):
                raise RuntimeError('synthetic transfer timeout')
            return original_upload(identifier, item)
        self.engines['cluster'].upload = slow_upload
        identifier = self.submit(count=2)
        self.service.tick()
        self.assertTrue(entered.wait(5))
        now = datetime.now(timezone.utc).isoformat()
        job_id = self.task(identifier)['job_id']
        node = self.queue.schedule_node('cluster', {'expectedRevision': 0, 'drainAt': now,
            'notBefore': now, 'afterJobId': job_id, 'targetImage': 'sha256:' + 'a' * 64})
        try:
            self.assertEqual(node['state'], 'draining')
        finally:
            release.set()
            self.service.wait_for_transfers()
        task = self.task(identifier)
        self.assertEqual(task['job']['status'], 'running')
        self.assertEqual(task['phase'], 'processing')
        self.assertEqual(len([c for c in self.engines['cluster'].calls if c[0] == 'upload']), 2)
        self.assertEqual(len([c for c in self.engines['cluster'].calls if c[0] == 'commit']), 1)
        with self.assertRaisesRegex(Conflict, 'owns running or held work'):
            self.queue.begin_node_update('cluster', {'expectedRevision': node['revision'],
                                                      'planId': node['planId']})

    def test_opt_in_held_treedar_blocks_its_host_only(self):
        self.queue.per_node_slots = True
        tree, _ = self.queue.submit({'requestId': 'tree', 'environment': 'production', 'profile': PROFILE})
        self.queue.claim({'workerId': 'tree-worker', 'nodeId': 'main'})
        self.queue.operator(tree['id'], 'hold', {'note': 'synthetic'})
        first = self.submit()
        second = self.submit()
        self.service.tick()
        self.service.tick()
        self.assertEqual(self.task(first)['node_id'], 'cluster')
        self.assertIsNone(self.task(second)['node_id'])

    def test_opt_in_production_takes_free_node_before_waiting_staging(self):
        self.queue.per_node_slots = True
        running = self.submit(environment='staging')
        self.service.tick()
        waiting = self.submit(environment='staging')
        production = self.submit(environment='production')
        self.service.tick()
        self.assertEqual(self.task(running)['node_id'], 'cluster')
        self.assertEqual(self.task(production)['node_id'], 'main')
        self.assertIsNone(self.task(waiting)['node_id'])

    def test_opt_in_staging_waits_while_production_is_running_or_held(self):
        self.queue.per_node_slots = True
        production = self.submit(environment='production')
        self.service.tick()
        staging = self.submit(environment='staging')
        self.service.tick()
        self.assertIsNone(self.task(staging)['node_id'])
        self.queue.operator(self.task(production)['job_id'], 'hold', {'note':'inspect'})
        self.service.tick()
        self.assertIsNone(self.task(staging)['node_id'])

    def test_opt_in_tree_cannot_claim_photo_busy_main(self):
        self.queue.per_node_slots = True
        self.config.local_max_photos = 1
        photo = self.submit(count=2)
        self.service.tick()
        self.assertEqual(self.task(photo)['node_id'], 'main')
        tree, _ = self.queue.submit({'requestId': 'tree-after-photo', 'environment': 'production', 'profile': PROFILE})
        self.assertIsNone(self.queue.claim({'workerId': 'tree-worker', 'nodeId': 'main'}))
        self.assertEqual(self.queue.get(tree['id'])['status'], 'queued')

    def test_opt_in_main_busy_does_not_route_large_job_to_cluster(self):
        self.queue.per_node_slots = True
        self.config.local_max_photos = 1
        first = self.submit(count=2)
        self.service.tick()
        second = self.submit(count=2)
        self.service.tick()
        self.assertEqual(self.task(first)['node_id'], 'main')
        self.assertIsNone(self.task(second)['node_id'])
        self.assertEqual(self.engines['cluster'].calls, [])

    def test_opt_in_reopen_preserves_both_running_engine_identities(self):
        self.queue.per_node_slots = True
        first = self.submit()
        second = self.submit()
        self.service.tick()
        self.service.tick()
        original = [(self.task(i)['node_id'], self.task(i)['engine_uuid']) for i in (first, second)]
        self.assertEqual({node for node, _ in original}, {'cluster', 'main'})
        self.service.close()
        self.queue.close()
        self.make_queue_service()
        self.queue.per_node_slots = True
        self.service.tick()
        self.assertEqual([(self.task(i)['node_id'], self.task(i)['engine_uuid']) for i in (first, second)], original)
        self.assertEqual(len(self.fleet.commits), 2)
        self.finish(first)
        self.service.tick()
        self.assertEqual(self.task(first)['job']['status'], 'succeeded')
        self.assertIn(self.task(second)['job']['status'], ('running', 'held'))

    def test_reconciliation_failure_holds_one_job_but_polls_other(self):
        self.queue.per_node_slots = True
        first = self.submit()
        second = self.submit()
        self.service.tick()
        self.service.tick()
        self.finish(second)
        original_poll = self.service._poll
        def sometimes_broken(task):
            if task['uuid'] == first:
                raise ValueError('synthetic corrupt task')
            return original_poll(task)
        with patch.object(self.service, '_poll', side_effect=sometimes_broken):
            with self.assertRaisesRegex(Conflict, 'reconciliation requires inspection'):
                self.service.tick()
        self.assertEqual(self.task(first)['job']['status'], 'held')
        self.assertEqual((self.task(first)['stage'], self.task(first)['phase'], self.task(first)['error']),
                         ('held', 'held', 'reconciliation_error'))
        self.assertEqual(self.task(second)['job']['status'], 'succeeded')

    def test_opt_in_does_not_fallback_for_unavailable_cluster(self):
        self.queue.per_node_slots = True
        self.unavailable.add('cluster')
        first = self.submit()
        self.service.tick()
        self.assertIsNone(self.task(first)['node_id'])

    def test_per_node_configuration_rejects_obvious_same_host_aliases(self):
        from processing.config import require_distinct_node_hosts
        for local, main in (('http://localhost:3001', 'http://127.0.0.1:3004'),
                            ('http://[::1]:3001', 'http://127.1.2.3:3004'),
                            ('http://127.0.0.2:3001', 'http://127.0.0.3:3004'),
                            ('http://[::ffff:192.0.2.1]:3001', 'http://192.0.2.1:3004'),
                            ('http://NODE.example.:3001', 'http://node.example:3004'),
                            ('', 'http://192.0.2.2:3000')):
            with self.subTest(local=local, main=main):
                with self.assertRaises(ValueError):
                    require_distinct_node_hosts(local, main)
        require_distinct_node_hosts('http://192.0.2.1:3001', 'http://192.0.2.2:3004')

    def test_ordered_three_node_inventory_prefers_first_eligible_and_falls_through(self):
        from processing.config import parse_node_inventory
        self.service.close(); self.queue.close()
        entries = [dict(id=name, physicalHostId=name, origin=f'http://192.0.2.{i}:3000',
                        tokenEnv=f'NODE_{i}_TOKEN', maxPhotos=limit, capabilities=['odm'])
                   for i, (name, limit) in enumerate((('tiny', 1), ('medium', 2), ('large', None)), 1)]
        inventory = parse_node_inventory(json.dumps(entries), {})
        self.config.node_inventory = inventory
        self.config.prototype_state_dir = str(self.root / 'inventory-state')
        self.engines = {n['id']: FakeEngine(n['id'], self.fleet) for n in inventory}
        self.queue = Queue(self.config.prototype_state_dir, node_ids=tuple(n['id'] for n in inventory),
                           treedar_node_ids=(), viewer_enabled=True, per_node_slots=True)
        self.service = ViewerService(self.queue, self.config, engines=self.engines,
            probe=lambda role, origin, token: {'ready': role not in self.unavailable, 'maxImages': None,
                'taskQueueCount': sum(x.startswith(role + ':') for x in self.fleet.active)})
        self.service._catalog = lambda: {'ready': True, 'commonOptions': [dict(item) for item in OPTIONS]}
        self.service._selected_options = lambda _node: [dict(item) for item in OPTIONS]
        first = self.submit(); second = self.submit(); third = self.submit(count=3)
        self.service.tick(); self.service.tick(); self.service.tick()
        self.assertEqual([self.task(i)['node_id'] for i in (first, second, third)], ['tiny', 'medium', 'large'])
        self.assertEqual(self.fleet.max_active, 3)
        self.assertEqual(self.service.info(third)['status']['code'], 10)
        self.engines['tiny'].tasks[self.task(first)['engine_uuid']]['progress'] = 67
        self.service.tick()
        self.assertEqual(self.service.info(first)['progress'], 67)
        self.assertEqual(self.service.info(third)['status']['code'], 20)
        self.assertEqual(self.service.output(first), ['synthetic engine output', '', 'comma,record'])
        self.assertIn(('read', self.task(first)['engine_uuid'], 'output'), self.engines['tiny'].calls)
        self.finish(first)
        blocked = self.submit(count=3)
        eligible = self.submit()
        self.service.tick()
        self.assertIsNone(self.task(blocked)['node_id'])
        self.assertEqual(self.task(eligible)['node_id'], 'tiny')
        self.finish(eligible); self.finish(third)
        self.service.tick()
        self.assertEqual(self.task(blocked)['node_id'], 'large')
        staging = self.submit(environment='staging')
        production = self.submit(environment='production')
        self.finish(second); self.service.tick()
        self.assertEqual(self.task(production)['node_id'], 'tiny')
        self.assertIsNone(self.task(staging)['node_id'])
        self.finish(production); self.finish(blocked)
        self.service.tick()
        self.assertEqual(self.task(staging)['node_id'], 'tiny')
        self.finish(staging)
        self.service.tick()
        self.unavailable.add('tiny')
        now = datetime.now(timezone.utc).isoformat()
        self.queue.schedule_node('medium', {'expectedRevision': 0, 'drainAt': now,
            'notBefore': now, 'afterJobId': None, 'targetImage': 'sha256:' + 'a' * 64})
        fallback = self.submit()
        self.service.tick()
        self.assertEqual(self.task(fallback)['node_id'], 'large')

    def test_inventory_rejects_invalid_caps_and_duplicate_hosts(self):
        from processing.config import parse_node_inventory
        valid = dict(id='n1', physicalHostId='host1', origin='http://192.0.2.1:3000',
                     tokenEnv='NODE_TOKEN', maxPhotos=1, capabilities=['odm'])
        for invalid in (0, -1, True, 1.5, '1000'):
            with self.subTest(limit=invalid):
                with self.assertRaises(ValueError):
                    parse_node_inventory(json.dumps([{**valid, 'maxPhotos': invalid}]), {})
        with self.assertRaises(ValueError):
            parse_node_inventory(json.dumps([valid, {**valid, 'id':'n2', 'origin':'http://192.0.2.2:3000'}]), {})

    def test_opt_in_atomic_node_slot_blocks_simultaneous_claims(self):
        self.queue.per_node_slots = True
        first = self.submit()
        second = self.submit()
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(self.queue.claim, {'workerId': 'worker-' + str(i),
                'nodeId': 'cluster', 'profiles': [VIEWER_PROFILE]}) for i in range(2)]
            claims = [f.result() for f in futures]
        self.assertEqual(sum(claim is not None for claim in claims), 1)
        self.assertEqual(sum(self.task(i)['job']['status'] == 'running' for i in (first, second)), 1)

    def test_priority_skip_rechecks_production_under_queue_transaction(self):
        self.queue.per_node_slots = True
        staging = self.submit(environment='staging')
        self.submit(environment='production')
        claim = self.queue.claim({'workerId':'scheduler', 'nodeId':'main',
            'profiles':[VIEWER_PROFILE], 'expectedJobId':self.task(staging)['job_id']},
            allow_viewer_same_priority_skip=True)
        self.assertIsNone(claim)
        self.assertEqual(self.task(staging)['job']['status'], 'queued')

    def test_opt_in_other_coordinator_process_cannot_claim_same_database(self):
        self.queue.per_node_slots = True
        task = self.submit()
        script = '''
import sys
from processing.prototype import Queue
try:
    q = Queue(sys.argv[1], node_ids=('cluster','main'), viewer_enabled=True, per_node_slots=True)
except RuntimeError as e:
    sys.exit(0 if str(e) == 'prototype state already owned' else 2)
else:
    try:
        q.claim({'workerId':'other', 'nodeId':'cluster', 'profiles':['odm-viewer-v1']})
    finally:
        q.close()
    sys.exit(3)
'''
        result = subprocess.run([sys.executable, '-c', script, str(self.root / 'state')],
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.task(task)['job']['status'], 'queued')

    def test_tree_running_and_held_block_all_photogrammetry(self):
        tree, _ = self.queue.submit({'requestId': 'tree', 'environment': 'production', 'profile': PROFILE})
        claim = self.queue.claim({'workerId': 'tree-worker', 'nodeId': 'main'})
        identifier = self.submit(environment='staging')
        self.service.tick()
        self.assertIsNone(self.task(identifier)['node_id'])
        self.queue.operator(tree['id'], 'hold', {'note': 'synthetic active TreeDar'})
        self.service.tick()
        self.assertIsNone(self.task(identifier)['node_id'])
        self.assertFalse(self.fleet.commits)
        self.queue.operator(tree['id'], 'resolve', {'note': 'confirmed synthetic stopped', 'workerStopped': True})
        self.service.tick()
        self.assertEqual(self.task(identifier)['node_id'], 'cluster')
        self.assertIsNotNone(claim)

    def test_small_photo_job_uses_cluster_above_old_byte_routing_ceiling(self):
        # Admission remains separately governed by viewer_max_input_bytes.
        # A deliberately low former routing ceiling lets this exercise the
        # real dispatch with only a tiny synthetic upload.
        self.config.local_max_input_bytes = 1
        identifier = self.submit()
        self.assertGreater(self.task(identifier)['input_bytes'], self.config.local_max_input_bytes)
        self.service.tick()
        self.assertEqual(self.task(identifier)['node_id'], 'cluster')
        self.assertFalse(self.engines['main'].calls)

    def test_busy_small_node_waits_without_consuming_main(self):
        identifier = self.submit()
        self.busy['cluster'] = 1
        self.service.tick()
        self.assertIsNone(self.task(identifier)['node_id'])
        self.assertFalse(self.engines['main'].calls)
        self.busy['cluster'] = 0
        self.service.tick()
        self.assertEqual(self.task(identifier)['node_id'], 'cluster')

    def test_large_job_waits_for_main_without_overflow_to_small_node(self):
        self.config.local_max_photos = 1
        identifier = self.submit(count=2)
        self.unavailable.add('main')
        self.service.tick()
        self.assertIsNone(self.task(identifier)['node_id'])
        self.assertFalse(self.fleet.commits)

    def test_maintenance_drain_keeps_priority_head_queued(self):
        self.config.local_max_photos = 1
        production = self.submit(count=2)
        staging = self.submit(environment='staging')
        now = datetime.now(timezone.utc).isoformat()
        self.queue.schedule_node('main', {'expectedRevision': 0, 'drainAt': now,
            'notBefore': now, 'afterJobId': None, 'targetImage': 'sha256:' + 'a' * 64})
        self.service.tick()
        self.assertEqual(self.task(production)['job']['status'], 'queued')
        self.assertEqual(self.task(staging)['job']['status'], 'queued')
        self.assertFalse(self.fleet.commits)

    def test_new_production_during_selection_is_not_claimed_using_staging_routing(self):
        staging = self.submit(environment='staging')
        production = self.initialize()
        self.upload(production, 'a.jpg')
        self.upload(production, 'b.jpg')
        self.config.local_max_photos = 1
        original = self.queue.claim
        inserted = []

        def insert_then_claim(payload):
            if not inserted:
                inserted.append(True)
                self.service.commit(production)
            return original(payload)

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
        before = list(self.engines['cluster'].calls)
        with self.assertRaises(Conflict):
            self.service.cancel(identifier)
        self.service.store.update(identifier, phase='cancel_pending')
        with self.assertRaises(Conflict):
            self.service.cancel(identifier)
        self.assertEqual(self.engines['cluster'].calls, before)
        self.service.store.update(identifier, phase='processing')
        self.service.tick()
        self.assertEqual(self.service.info(identifier)['status']['code'], 40)
        self.assertEqual(sum(c[0] == 'initialize' for c in self.engines['cluster'].calls), 1)
        self.assertEqual(len(self.fleet.commits), 1)

    def test_terminal_job_and_viewer_update_roll_back_together(self):
        identifier = self.submit()
        self.service.tick()
        engine_uuid = self.task(identifier)['engine_uuid']
        self.engines['cluster'].finish(engine_uuid)
        with self.queue._db() as db:
            db.execute("CREATE TRIGGER fail_terminal BEFORE UPDATE OF stage ON viewer_tasks WHEN NEW.stage='succeeded' BEGIN SELECT RAISE(ABORT,'synthetic failure'); END")
        with self.assertRaises(Exception):
            self.service.tick()
        task = self.task(identifier)
        self.assertEqual(task['job']['status'], 'running')
        self.assertNotEqual(task['stage'], 'succeeded')
        with self.queue._db() as db:
            db.execute('DROP TRIGGER fail_terminal')
        self.service.tick()
        self.assertEqual(self.task(identifier)['job']['status'], 'succeeded')
        self.assertEqual(self.service.info(identifier)['status']['code'], 40)

    def test_existing_terminal_split_recovers_after_restart(self):
        identifier=self.submit();self.service.tick();task=self.task(identifier)
        result={'engineTaskId':task['engine_uuid'],'nodeId':task['node_id'],
                'environment':task['environment'],'cancelled':False}
        self.queue.complete(task['job_id'],{'claimToken':task['claim_token'],'status':'succeeded','result':result})
        self.service.close();self.queue.close();self.make_queue_service()
        self.service.tick()
        self.assertEqual(self.service.info(identifier)['status']['code'],40)
        self.assertEqual(len(self.fleet.commits),1)

    def test_stopped_copy_repairs_split_before_held_startup(self):
        import shutil,sqlite3
        identifier=self.submit();self.service.tick();task=self.task(identifier)
        result={'engineTaskId':task['engine_uuid'],'nodeId':task['node_id'],
                'environment':task['environment'],'cancelled':False}
        self.queue.complete(task['job_id'],{'claimToken':task['claim_token'],'status':'succeeded','result':result})
        self.service.close();self.queue.close()
        original=self.root/'state'; copied=self.root/'copy'
        shutil.copytree(original,copied)
        original_bytes=(original/'jobs.sqlite3').read_bytes()
        with self.assertRaises(RuntimeError):
            Queue(copied,node_ids=('cluster','main'),viewer_enabled=True,require_idle_existing=True)
        before=sqlite3.connect(copied/'jobs.sqlite3')
        jobs_before=before.execute('SELECT * FROM jobs ORDER BY id').fetchall()
        before.close()
        config=SimpleNamespace(**vars(self.config));config.prototype_state_dir=str(copied)
        queue=Queue(copied,node_ids=('cluster','main'),viewer_enabled=True)
        service=ViewerService(queue,config,engines=self.engines,probe=self.probe)
        calls={node:list(engine.calls) for node,engine in self.engines.items()}
        try:
            with queue._db() as db:
                db.execute("UPDATE viewer_tasks SET claim_token='wrong' WHERE uuid=?",(identifier,))
            with self.assertRaises(Conflict):service.tick()
            self.assertNotEqual(service.store.task(identifier)['phase'],'complete')
            with queue._db() as db:
                db.execute('UPDATE viewer_tasks SET claim_token=? WHERE uuid=?',(task['claim_token'],identifier))
            service.tick()
            self.assertEqual(service.info(identifier)['status']['code'],40)
            self.assertEqual({node:list(engine.calls) for node,engine in self.engines.items()},calls)
        finally:service.close();queue.close()
        after=sqlite3.connect(copied/'jobs.sqlite3')
        self.assertEqual(after.execute('SELECT * FROM jobs ORDER BY id').fetchall(),jobs_before)
        after.close()
        self.assertEqual((original/'jobs.sqlite3').read_bytes(),original_bytes)
        held=Queue(copied,node_ids=('cluster','main'),viewer_enabled=True,require_idle_existing=True)
        held.close()
        self.make_queue_service()

    def test_existing_terminal_split_with_wrong_claim_holds_dispatch(self):
        identifier=self.submit();self.service.tick();task=self.task(identifier)
        result={'engineTaskId':task['engine_uuid'],'nodeId':task['node_id'],
                'environment':task['environment'],'cancelled':False}
        self.queue.complete(task['job_id'],{'claimToken':task['claim_token'],'status':'succeeded','result':result})
        with self.queue._db() as db:
            db.execute("UPDATE viewer_tasks SET claim_token='wrong' WHERE uuid=?",(identifier,))
        with self.assertRaises(Conflict):self.service.tick()
        self.assertNotEqual(self.task(identifier)['phase'],'complete')

    def test_terminal_split_rejects_mismatched_job_binding_before_dispatch(self):
        identifier=self.submit();self.service.tick();task=self.task(identifier)
        result={'engineTaskId':task['engine_uuid'],'nodeId':task['node_id'],
                'environment':task['environment'],'cancelled':False}
        self.queue.complete(task['job_id'],{'claimToken':task['claim_token'],'status':'succeeded','result':result})
        second=self.submit()
        for field,bad,original in (('node_id','main',task['node_id']),
                                   ('environment','staging',task['environment']),
                                   ('profile','photo-smoke',task['job']['profile'])):
            with self.subTest(field=field):
                with self.queue._db() as db:
                    db.execute('UPDATE jobs SET '+field+'=? WHERE id=?',(bad,task['job_id']))
                with self.assertRaises(Conflict):self.service.tick()
                self.assertEqual(self.task(second)['job']['status'],'queued')
                with self.queue._db() as db:
                    db.execute('UPDATE jobs SET '+field+'=? WHERE id=?',(original,task['job_id']))

    def assert_operator_resolution_settled(self, first, second):
        task = self.task(first)
        self.assertEqual((task['stage'], task['phase'], task['error']), ('held', 'held', 'operator_resolved'))
        self.assertEqual(task['job']['status'], 'failed')
        self.assertEqual(self.service.info(first)['status'], {'code': 30, 'errorMessage': 'operator_resolved'})
        self.assertTrue(Path(task['directory']).is_dir())
        self.assertEqual(len(task['files']), 1)
        self.assertEqual(self.task(second)['job']['status'], 'running')
        self.assertEqual([identifier for _, identifier in self.fleet.commits],
                         [task['engine_uuid'], self.task(second)['engine_uuid']])

    def test_operator_hold_and_resolve_of_running_job_releases_dispatch(self):
        first = self.submit(); self.service.tick()
        job_id = self.task(first)['job_id']
        self.service.operator(job_id, 'hold', {'note': 'synthetic operator inspection'})
        self.service.tick()
        self.assertEqual(self.task(first)['phase'], 'processing')
        self.finish(first, 30)  # the operator stopped the engine task
        self.service.operator(job_id, 'resolve', {'note': 'synthetic stopped', 'workerStopped': True})
        second = self.submit(environment='staging')
        self.service.tick()
        self.assert_operator_resolution_settled(first, second)
        self.service.tick()
        self.assertEqual(len(self.fleet.commits), 2)

    def test_interrupted_hold_resolved_by_operator_recovers_after_restart(self):
        first = self.submit(); self.service.tick()
        self.finish(first, 50)
        update = self.service.store.update
        def crash(identifier, **fields):
            if fields.get('phase') == 'held':
                raise OSError('synthetic crash after queue hold')
            return update(identifier, **fields)
        self.service.store.update = crash
        with self.assertRaises(OSError):
            self.service.tick()
        task = self.task(first)
        self.assertEqual((task['phase'], task['job']['status']), ('processing', 'held'))
        self.service.close(); self.queue.close(); self.make_queue_service()
        self.service.operator(task['job_id'], 'resolve', {'note': 'synthetic stopped', 'workerStopped': True})
        second = self.submit()
        self.service.tick()
        self.assert_operator_resolution_settled(first, second)

    def test_operator_resolution_split_with_wrong_binding_still_blocks(self):
        first = self.submit(); self.service.tick()
        job_id = self.task(first)['job_id']
        self.service.operator(job_id, 'hold', {'note': 'synthetic operator inspection'})
        self.service.operator(job_id, 'resolve', {'note': 'synthetic stopped', 'workerStopped': True})
        second = self.submit()
        with self.queue._db() as db:
            original = db.execute('SELECT result FROM jobs WHERE id=?', (job_id,)).fetchone()[0]
        for change in ({'workerStopped': False}, {'environment': 'staging'}, {'extra': 1}):
            with self.subTest(change=change):
                with self.queue._db() as db:
                    db.execute('UPDATE jobs SET result=? WHERE id=?', (json.dumps({**json.loads(original), **change}), job_id))
                with self.assertRaises(Conflict):
                    self.service.tick()
                self.assertEqual(self.task(first)['phase'], 'processing')
                self.assertEqual(self.task(second)['job']['status'], 'queued')
        with self.queue._db() as db:
            db.execute('UPDATE jobs SET result=?,node_id=? WHERE id=?', (original, 'main', job_id))
        with self.assertRaises(Conflict):
            self.service.tick()
        self.assertEqual(self.task(second)['job']['status'], 'queued')

    def test_operator_resolution_requires_complete_unchanged_identity(self):
        first = self.submit(); self.service.tick()
        task = self.task(first); job_id = task['job_id']
        self.service.operator(job_id, 'hold', {'note': 'synthetic operator inspection'})
        self.service.operator(job_id, 'resolve', {'note': 'synthetic stopped', 'workerStopped': True})
        second = self.submit()
        def blocked():
            with self.assertRaises(Conflict):
                self.service.tick()
            current = self.task(first)
            self.assertEqual((current['stage'], current['phase']), (task['stage'], 'processing'))
            self.assertEqual(self.task(second)['job']['status'], 'queued')
        for field in ('engine_uuid', 'node_id', 'claim_token'):
            for missing in (None, ''):
                with self.subTest(field=field, missing=missing):
                    with self.queue._db() as db:
                        db.execute('UPDATE viewer_tasks SET ' + field + '=? WHERE uuid=?', (missing, first))
                        if field == 'node_id':  # an equal missing binding on both sides
                            db.execute('UPDATE jobs SET node_id=? WHERE id=?', (missing, job_id))
                    blocked()
                    with self.queue._db() as db:
                        db.execute('UPDATE viewer_tasks SET ' + field + '=? WHERE uuid=?', (task[field], first))
                        db.execute('UPDATE jobs SET node_id=? WHERE id=?', (task['node_id'], job_id))
        with self.subTest('claim not bound to the job'):
            with self.queue._db() as db:
                db.execute("UPDATE viewer_tasks SET claim_token='other-claim' WHERE uuid=?", (first,))
            blocked()
            with self.queue._db() as db:
                db.execute('UPDATE viewer_tasks SET claim_token=? WHERE uuid=?', (task['claim_token'], first))
        for field, changed in (('engine_uuid', str(uuid.uuid4())), ('node_id', 'main'),
                               ('claim_token', 'stale-claim'), ('job_id', 'stale-job')):
            with self.subTest(stale=field):
                with self.assertRaises(Conflict):
                    self.service.store.settle_operator_resolution({**self.task(first), field: changed})
                self.assertEqual(self.task(first)['phase'], 'processing')
        self.finish(first, 30)  # the operator stopped the engine task
        self.service.tick()
        self.assert_operator_resolution_settled(first, second)

    def test_operator_resolution_requires_one_live_mapping_in_transaction(self):
        first = self.submit(); self.service.tick()
        job_id = self.task(first)['job_id']
        self.service.operator(job_id, 'hold', {'note': 'synthetic operator inspection'})
        self.service.operator(job_id, 'resolve', {'note': 'synthetic stopped', 'workerStopped': True})
        second = self.submit()
        snapshot = self.task(first)  # read before the mapping changes
        duplicate = str(uuid.uuid4())
        with self.queue._db() as db:
            columns = [row[1] for row in db.execute('PRAGMA table_info(viewer_tasks)')]
            copied = ','.join('?' if name in ('uuid', 'engine_uuid') else name for name in columns)
            db.execute('INSERT INTO viewer_tasks (' + ','.join(columns) + ') SELECT ' + copied +
                       ' FROM viewer_tasks WHERE uuid=?', (duplicate, str(uuid.uuid4()), first))
        with self.assertRaises(Conflict):
            self.service.store.settle_operator_resolution(snapshot)
        with self.assertRaises(Conflict):
            self.service.tick()
        with self.queue._db() as db:
            db.execute('UPDATE viewer_tasks SET removed=1 WHERE uuid=?', (first,))
        with self.assertRaises((Conflict, KeyError)):  # a removed task is unknown
            self.service.store.settle_operator_resolution(snapshot)
        with self.queue._db() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM viewer_tasks WHERE phase='processing' AND job_id=?",
                                        (job_id,)).fetchone()[0], 2)
            db.execute('DELETE FROM viewer_tasks WHERE uuid=?', (duplicate,))
            db.execute('UPDATE viewer_tasks SET removed=0 WHERE uuid=?', (first,))
        self.assertEqual(self.task(second)['job']['status'], 'queued')
        self.finish(first, 30)
        self.service.tick()
        self.assert_operator_resolution_settled(first, second)

    def test_restart_while_node_offline_retains_slot_then_returns_completed_result(self):
        first=self.submit();second=self.submit()
        self.service.tick();task=self.task(first);engine_uuid=task['engine_uuid']
        self.assertEqual(task['node_id'],'cluster');self.assertEqual(task['job']['status'],'running')
        self.service.close();self.queue.close()
        # The engine completes while its coordinator is stopped, then becomes
        # temporarily unavailable to the restarted coordinator.
        self.engines['cluster'].finish(engine_uuid);self.engines['cluster'].read_unavailable=True
        self.make_queue_service()
        self.assertEqual(self.task(first)['job']['status'],'held')
        self.service.tick()
        self.assertEqual(self.task(first)['job']['status'],'held')
        self.assertEqual(self.service.info(first)['status']['code'],20)
        self.assertEqual(self.task(second)['job']['status'],'queued')
        self.assertIsNone(self.task(second)['node_id'])
        self.assertEqual(len(self.fleet.commits),1)
        self.engines['cluster'].read_unavailable=False
        self.service._backoff[first]=(0,self.service._backoff[first][1])
        self.service.tick()
        self.assertEqual(self.service.info(first)['status']['code'],40)
        self.assertEqual(self.task(second)['node_id'],'cluster')
        self.assertEqual(len(self.fleet.commits),2)
        stream,length=self.service.download(first)
        with stream:result=stream.read()
        self.assertEqual(result,self.engines['cluster'].archive);self.assertEqual(length,len(result))
        self.assertEqual(hashlib.sha256(result).hexdigest(),hashlib.sha256(self.engines['cluster'].archive).hexdigest())
        self.assertEqual(sum(c[0]=='initialize' for c in self.engines['cluster'].calls),2)
        self.assertEqual(self.fleet.max_active,1)
        self.finish(second);self.service.tick()
        self.service.close();self.queue.close();self.make_queue_service()
        with patch.object(self.engines['cluster'],'download',side_effect=EngineUnavailable('offline')):
            stream,_=self.service.download(first)
            with stream:self.assertEqual(stream.read(),result)

    def test_operator_hold_serializes_with_viewer_cancel(self):
        identifier = self.submit()
        self.service.tick()
        job_id = self.task(identifier)['job_id']
        before = list(self.engines['cluster'].calls)
        held, release, cancel_started, cancel_done = (threading.Event() for _ in range(4))
        outcomes = []
        original = self.queue.operator

        def blocked_operator(*args):
            held.set()
            if not release.wait(5):
                raise AssertionError('test did not release operator')
            return original(*args)

        def do_cancel():
            cancel_started.set()
            try:
                self.service.cancel(identifier)
                outcomes.append('cancelled')
            except Conflict:
                outcomes.append('held')
            finally:
                cancel_done.set()

        with patch.object(self.queue, 'operator', side_effect=blocked_operator):
            holder = threading.Thread(target=self.service.operator,
                args=(job_id, 'hold', {'note': 'synthetic hold'}))
            holder.start()
            self.assertTrue(held.wait(5))
            canceller = threading.Thread(target=do_cancel)
            canceller.start()
            self.assertTrue(cancel_started.wait(5))
            self.assertFalse(cancel_done.wait(0.2))
            release.set()
            holder.join(5); canceller.join(5)
        self.assertFalse(holder.is_alive())
        self.assertFalse(canceller.is_alive())
        self.assertEqual(outcomes, ['held'])
        self.assertEqual(self.engines['cluster'].calls, before)
        self.assertEqual(self.queue.get(job_id)['status'], 'held')

    def test_engine_read_outage_backs_off_and_retains_slot_then_recovers(self):
        first = self.submit()
        second = self.submit()
        self.service.tick()
        self.engines['cluster'].read_unavailable = True
        self.service.tick()
        initial = self.service._backoff[first]
        self.service.tick()
        self.assertEqual(self.service._backoff[first], initial)
        self.assertEqual(self.task(first)['job']['status'], 'running')
        self.assertIsNone(self.task(second)['node_id'])
        self.finish(first)
        self.engines['cluster'].read_unavailable = False
        self.service._backoff[first] = (0, initial[1])
        self.service.tick()
        self.assertEqual(self.service.info(first)['status']['code'], 40)
        self.assertEqual(self.task(second)['node_id'], 'cluster')
        self.assertEqual(self.fleet.max_active, 1)

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
                info = self.service.info(identifier)
                self.assertEqual(info['status']['code'], 20)
                self.assertIs(info['ledgetopHeld'], True)
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

    def test_running_cancel_status_50_holds_slot_even_after_restart(self):
        identifier = self.submit()
        next_id = self.submit()
        self.service.tick()
        self.service.cancel(identifier)
        self.service.cancel(identifier)
        self.assertEqual(self.task(identifier)['job']['status'], 'running')
        self.assertIsNone(self.task(next_id)['node_id'])
        self.service.tick()
        self.assertIsNone(self.task(next_id)['node_id'])
        self.assertEqual(sum(c[0] == 'cancel' for c in self.engines['cluster'].calls), 1)
        self.finish(identifier, 50)
        self.service.tick()
        self.assertEqual(self.service.info(identifier)['status']['code'], 20)
        self.assertTrue(self.service.info(identifier)['ledgetopHeld'])
        self.assertEqual(self.task(identifier)['job']['status'], 'held')
        self.assertEqual(self.task(identifier)['error'], 'cancel_termination_unverified')
        self.assertIsNone(self.task(next_id)['node_id'])
        self.service.close()
        self.queue.close()
        self.make_queue_service()
        self.service.tick()
        self.assertEqual(self.task(identifier)['job']['status'], 'held')
        self.assertEqual(self.task(identifier)['error'], 'cancel_termination_unverified')
        self.assertEqual(self.service.info(identifier)['status']['code'], 20)
        self.assertIsNone(self.task(next_id)['node_id'])
        self.assertEqual(self.fleet.max_active, 1)

    def test_cancel_hold_survives_crash_before_task_metadata_update(self):
        identifier = self.submit()
        next_id = self.submit()
        self.service.tick()
        self.service.cancel(identifier)
        self.finish(identifier, 50)
        with patch.object(self.service.store, 'update', side_effect=OSError('synthetic crash')):
            with self.assertRaises(OSError):
                self.service.tick()
        self.assertEqual(self.task(identifier)['job']['status'], 'held')
        self.assertIsNone(self.task(next_id)['node_id'])
        self.service.close()
        self.queue.close()
        self.make_queue_service()
        self.service.tick()
        self.assertEqual(self.task(identifier)['job']['status'], 'held')
        self.assertEqual(self.service.info(identifier)['status']['code'], 20)
        self.assertIsNone(self.task(next_id)['node_id'])

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

    def test_missing_recorded_archive_refetches_only_exact_receipt_bytes_after_restart(self):
        identifier=self.submit();self.service.tick();self.finish(identifier);self.service.tick()
        stream,_=self.service.download(identifier)
        with stream:expected=stream.read()
        path=Path(self.task(identifier)['archive_path']);path.unlink()
        self.service.close();self.queue.close();self.make_queue_service()
        stream,length=self.service.download(identifier)
        with stream:self.assertEqual(stream.read(),expected)
        self.assertEqual(length,len(expected));self.assertEqual(path.read_bytes(),expected)
        self.assertEqual(self.engines['cluster'].download_calls,2);self.assertEqual(len(self.fleet.commits),1)

    def test_missing_recorded_archive_rejects_mismatched_node_and_missing_receipt(self):
        identifier=self.submit();self.service.tick();self.finish(identifier);self.service.tick()
        stream,_=self.service.download(identifier);stream.close()
        path=Path(self.task(identifier)['archive_path']);path.unlink()
        changed=bytearray(self.engines['cluster'].archive);changed[10]^=1
        self.engines['cluster'].archive=bytes(changed)
        with self.assertRaises(Conflict):self.service.download(identifier)
        self.assertFalse(path.exists())
        path.with_suffix('.zip.receipt').unlink()
        before=self.engines['cluster'].download_calls
        with self.assertRaises(Conflict):self.service.download(identifier)
        self.assertEqual(self.engines['cluster'].download_calls,before)

    def test_missing_recorded_archive_node_unavailable_and_competing_destination_hold(self):
        identifier=self.submit();self.service.tick();self.finish(identifier);self.service.tick()
        stream,_=self.service.download(identifier);stream.close()
        path=Path(self.task(identifier)['archive_path']);path.unlink()
        with patch.object(self.engines['cluster'],'download',side_effect=EngineUnavailable('offline')):
            with self.assertRaises(EngineUnavailable):self.service.download(identifier)
        self.assertFalse(path.exists())
        original=os.link
        def compete(source,destination):
            Path(destination).write_bytes(b'foreign')
            return original(source,destination)
        with patch('processing.viewer_service.os.link',side_effect=compete):
            with self.assertRaises(Conflict):self.service.download(identifier)
        self.assertEqual(path.read_bytes(),b'foreign')
        with self.assertRaises(Conflict):self.service.download(identifier)
        self.assertEqual(path.read_bytes(),b'foreign')

    def test_missing_recorded_archive_interrupted_transfer_can_retry(self):
        identifier=self.submit();self.service.tick();self.finish(identifier);self.service.tick()
        stream,_=self.service.download(identifier);stream.close()
        path=Path(self.task(identifier)['archive_path']);path.unlink()
        orphan=path.parent/'.all.zip.restore-unrelated';orphan.write_bytes(b'unrelated')
        self.engines['cluster'].interrupt_download_once=True
        with self.assertRaises(OSError):self.service.download(identifier)
        self.assertFalse(path.exists());self.assertEqual(orphan.read_bytes(),b'unrelated')
        stream,_=self.service.download(identifier)
        with stream:self.assertEqual(stream.read(),self.engines['cluster'].archive)
        self.assertEqual(orphan.read_bytes(),b'unrelated')

    def test_real_service_over_http_returns_receipt_backed_digest(self):
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        from urllib.request import urlopen
        from processing.viewer_http import ViewerFacade
        identifier = self.submit(); self.service.tick(); self.finish(identifier); self.service.tick()
        facade = ViewerFacade(self.service, 'synthetic-production-token', 'synthetic-staging-token')
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_): pass
            def do_GET(self): facade.handle(self)
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            url = ('http://127.0.0.1:' + str(server.server_port) + '/task/' + identifier +
                   '/download/all.zip?token=synthetic-production-token')
            with urlopen(url, timeout=4) as response:
                body = response.read()
                self.assertEqual(response.headers['X-LedgeTop-Archive-SHA256'],
                                 hashlib.sha256(body).hexdigest())
            self.assertEqual(body, self.engines['cluster'].archive)
        finally:
            server.shutdown(); server.server_close(); thread.join(timeout=2)
            self.assertFalse(thread.is_alive())

    def test_real_service_and_facade_emit_receipt_backed_archive_digest(self):
        from processing.viewer_http import ViewerFacade
        from tests.test_viewer_http import MemoryHandler
        identifier = self.submit(); self.service.tick(); self.finish(identifier); self.service.tick()
        facade = ViewerFacade(self.service, 'synthetic-production-token', 'synthetic-staging-token')
        handler = MemoryHandler('/task/' + identifier + '/download/all.zip?token=synthetic-production-token',
                                method='GET')
        facade.handle(handler)
        self.assertEqual(handler.status, 200)
        self.assertEqual(handler.response_headers['X-LedgeTop-Archive-SHA256'],
                         hashlib.sha256(handler.wfile.getvalue()).hexdigest())
        self.assertEqual(handler.wfile.getvalue(), self.engines['cluster'].archive)

    def test_receipt_backed_stream_carries_exact_digest_but_legacy_does_not(self):
        identifier = self.submit(); self.service.tick(); self.finish(identifier); self.service.tick()
        stream, _ = self.service.download(identifier)
        with stream:
            self.assertEqual(stream.ledgetop_archive_sha256, hashlib.sha256(stream.read()).hexdigest())
        task = self.task(identifier)
        Path(task['archive_path']).with_suffix('.zip.receipt').unlink()
        stream, _ = self.service.download(identifier)
        with stream:
            self.assertFalse(hasattr(stream, 'ledgetop_archive_sha256'))

    def test_archive_receipt_recovers_after_state_write_failure_and_restart(self):
        identifier = self.submit(); self.service.tick(); self.finish(identifier); self.service.tick()
        original = self.service.store.update
        def fail_archive_update(task_id, **fields):
            if 'archive_path' in fields: raise OSError('synthetic lost state write')
            return original(task_id, **fields)
        with patch.object(self.service.store, 'update', side_effect=fail_archive_update):
            with self.assertRaises(OSError): self.service.download(identifier)
        task = self.task(identifier); final = Path(task['directory']) / 'all.zip'
        self.assertIsNone(task['archive_path']); self.assertTrue(final.is_file())
        self.service.close(); self.queue.close(); self.make_queue_service()
        with patch.object(self.engines['cluster'], 'download', side_effect=EngineUnavailable('offline')):
            stream, length = self.service.download(identifier)
            with stream: self.assertEqual(stream.read(), self.engines['cluster'].archive)
        self.assertEqual(length, len(self.engines['cluster'].archive))
        self.assertEqual(self.task(identifier)['archive_path'], str(final))

    def test_interrupted_metadata_marker_publication_recovers_without_overwriting(self):
        # Exit exactly before/after hard-link publication; no external service or data.
        script="""import json,os,sys
from pathlib import Path
from unittest.mock import patch
from processing.viewer_service import ViewerService
path=Path(sys.argv[1]);value=json.loads(sys.argv[2]);phase=sys.argv[3];original=os.link
def interrupted(src,dst):
 if phase=='after':original(src,dst)
 os._exit(73)
with patch('processing.viewer_service.os.link',side_effect=interrupted):
 ViewerService.__new__(ViewerService)._publish_archive_metadata(path,value)
"""
        for marker_kind,phase in (('intent','before'),('intent','after'),('receipt','after')):
            with self.subTest(marker=marker_kind,phase=phase):
                identifier=self.submit();self.service.tick();self.finish(identifier);self.service.tick()
                task=self.task(identifier);directory=Path(task['directory']);path=directory/('all.zip.'+marker_kind)
                value=self.service._archive_identity(task)
                if marker_kind=='receipt':
                    data=self.engines['cluster'].archive;(directory/'all.zip.partial').write_bytes(data)
                    value={**value,'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()}
                process=subprocess.run([sys.executable,'-c',script,str(path),json.dumps(value),phase],capture_output=True,timeout=15)
                self.assertEqual(process.returncode,73)
                names=list(directory.glob('.all.zip.'+marker_kind+'.tmp-*'));self.assertEqual(len(names),1)
                self.assertEqual(path.exists(),phase=='after')
                with self.service.download(identifier)[0] as stream:self.assertEqual(stream.read(),self.engines['cluster'].archive)
                self.assertTrue(path.is_file())
                if phase=='after':self.assertFalse(names[0].exists())
                else:self.assertTrue(names[0].exists()) # unrelated, uncommitted temp is retained

    def test_concurrent_archive_recovery_uses_one_validated_receipt(self):
        identifier=self.submit();self.service.tick();self.finish(identifier);self.service.tick()
        task=self.task(identifier);directory=Path(task['directory']);data=self.engines['cluster'].archive
        (directory/'all.zip.partial').write_bytes(data)
        receipt=directory/'all.zip.receipt';metadata={**self.service._archive_identity(task),'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()}
        receipt.write_text(json.dumps(metadata,sort_keys=True,separators=(',',':')))
        staged=directory/('.all.zip.receipt.tmp-'+'a'*32);os.link(receipt,staged)
        barrier=threading.Barrier(2);results=[];errors=[]
        def download():
            try:
                barrier.wait(3)
                stream,_=self.service.download(identifier)
                with stream:results.append(stream.read())
            except Exception as error:errors.append(error)
        workers=[threading.Thread(target=download) for _ in range(2)]
        with patch.object(self.engines['cluster'],'download',side_effect=EngineUnavailable('offline')):
            try:
                for worker in workers:worker.start()
                for worker in workers:worker.join(5)
            finally:
                barrier.abort()
                for worker in workers:worker.join(3)
        self.assertFalse(any(worker.is_alive() for worker in workers));self.assertEqual(errors,[])
        self.assertEqual(results,[data,data]);self.assertFalse(staged.exists());self.assertEqual(receipt.stat().st_nlink,1)
        self.assertEqual(self.engines['cluster'].download_calls,0)

    def test_unrelated_metadata_hardlink_is_preserved_and_held(self):
        identifier=self.submit();self.service.tick();self.finish(identifier);self.service.tick()
        task=self.task(identifier);directory=Path(task['directory']);path=directory/'all.zip.intent'
        path.write_text(json.dumps(self.service._archive_identity(task),sort_keys=True,separators=(',',':')))
        foreign=directory/'unrelated-link';os.link(path,foreign)
        with self.assertRaises(Conflict):self.service.download(identifier)
        self.assertTrue(path.exists());self.assertTrue(foreign.exists());self.assertEqual(path.stat().st_nlink,2)

    def test_foreign_or_corrupt_metadata_is_never_overwritten(self):
        identifier=self.submit();self.service.tick();self.finish(identifier);self.service.tick()
        task=self.task(identifier);directory=Path(task['directory']);path=directory/'all.zip.intent';path.write_bytes(b'foreign')
        temporary=directory/('.all.zip.intent.tmp-'+'a'*32);os.link(path,temporary)
        with self.assertRaises(Conflict):self.service.download(identifier)
        self.assertEqual(path.read_bytes(),b'foreign');self.assertEqual(temporary.read_bytes(),b'foreign')

    def test_receipt_recovers_partial_after_interrupted_publication(self):
        identifier = self.submit(); self.service.tick(); self.finish(identifier); self.service.tick()
        original_link=os.link
        def interrupt_archive_link(source,destination):
            if str(destination).endswith('all.zip'): raise OSError('synthetic interrupted link')
            return original_link(source,destination)
        with patch('processing.viewer_service.os.link', side_effect=interrupt_archive_link):
            with self.assertRaises(OSError): self.service.download(identifier)
        with patch.object(self.engines['cluster'], 'download', side_effect=EngineUnavailable('offline')):
            stream, _ = self.service.download(identifier)
            with stream: self.assertEqual(stream.read(), self.engines['cluster'].archive)

    def test_orphan_without_receipt_or_modified_archive_is_never_adopted(self):
        identifier = self.submit(); self.service.tick(); self.finish(identifier); self.service.tick()
        task = self.task(identifier); final = Path(task['directory']) / 'all.zip'
        final.write_bytes(b'foreign data')
        with self.assertRaises(Conflict): self.service.download(identifier)
        self.assertEqual(final.read_bytes(), b'foreign data')
        final.unlink()
        with patch.object(self.service.store, 'update', side_effect=OSError('synthetic lost state write')):
            with self.assertRaises(OSError): self.service.download(identifier)
        final.write_bytes(b'tampered data')
        with self.assertRaises(Conflict): self.service.download(identifier)
        self.assertEqual(final.read_bytes(), b'tampered data')

    def test_receipt_for_other_generation_is_never_adopted(self):
        identifier = self.submit(); self.service.tick(); self.finish(identifier); self.service.tick()
        with patch.object(self.service.store, 'update', side_effect=OSError('synthetic lost state write')):
            with self.assertRaises(OSError): self.service.download(identifier)
        task = self.task(identifier); receipt = Path(task['directory']) / 'all.zip.receipt'
        value = json.loads(receipt.read_text()); value['generation'] += 1
        receipt.write_text(json.dumps(value))
        with self.assertRaises(Conflict): self.service.download(identifier)

    def test_unknown_partial_and_malformed_intent_are_not_removed(self):
        identifier = self.submit(); self.service.tick(); self.finish(identifier); self.service.tick()
        directory = Path(self.task(identifier)['directory']); partial = directory / 'all.zip.partial'
        partial.write_bytes(b'foreign partial')
        with self.assertRaises(Conflict): self.service.download(identifier)
        self.assertEqual(partial.read_bytes(), b'foreign partial')
        (directory / 'all.zip.intent').write_bytes(b'{incomplete')
        with self.assertRaises(Conflict): self.service.download(identifier)
        self.assertEqual(partial.read_bytes(), b'foreign partial')

    def test_competing_archive_is_not_overwritten(self):
        identifier = self.submit(); self.service.tick(); self.finish(identifier); self.service.tick()
        directory = Path(self.task(identifier)['directory']); final = directory / 'all.zip'
        original_link = os.link
        def competing_link(source, destination):
            final.write_bytes(b'foreign result')
            return original_link(source, destination)
        with patch('processing.viewer_service.os.link', side_effect=competing_link):
            with self.assertRaises(FileExistsError): self.service.download(identifier)
        self.assertEqual(final.read_bytes(), b'foreign result')
        with self.assertRaises(Conflict): self.service.download(identifier)
        self.assertEqual(final.read_bytes(), b'foreign result')

    @unittest.skipUnless(os.name == 'posix', 'POSIX symlink fixture required')
    def test_archive_symlinks_do_not_modify_or_read_foreign_target(self):
        identifier = self.submit(); self.service.tick(); self.finish(identifier); self.service.tick()
        directory = Path(self.task(identifier)['directory']); victim = self.root / 'foreign.bin'
        victim.write_bytes(b'foreign data')
        for name in ('all.zip', 'all.zip.partial', 'all.zip.intent', 'all.zip.receipt'):
            link = directory / name; link.symlink_to(victim)
            try:
                with self.subTest(name=name), self.assertRaises(Conflict): self.service.download(identifier)
                self.assertEqual(victim.read_bytes(), b'foreign data')
            finally: link.unlink()

    @unittest.skipUnless(os.name == 'posix', 'POSIX rename with open descriptor required')
    def test_recovery_serves_validated_descriptor_and_rechecks_later_reads(self):
        identifier = self.submit(); self.service.tick(); self.finish(identifier); self.service.tick()
        with patch.object(self.service.store, 'update', side_effect=OSError('synthetic lost state write')):
            with self.assertRaises(OSError): self.service.download(identifier)
        task = self.task(identifier); final = Path(task['directory']) / 'all.zip'
        original = self.service.store.update
        def replace_after_update(task_id, **fields):
            result=original(task_id, **fields)
            if 'archive_path' in fields:
                replacement=final.with_name('competing.zip');replacement.write_bytes(b'foreign data')
                os.replace(replacement, final)
            return result
        with patch.object(self.service.store, 'update', side_effect=replace_after_update):
            stream, _ = self.service.download(identifier)
            with stream:self.assertEqual(stream.read(), self.engines['cluster'].archive)
        with self.assertRaises(Conflict):self.service.download(identifier)

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

    def test_queued_small_job_runs_when_main_catalog_is_unavailable(self):
        identifier = self.submit()
        self.unavailable.add('main'); self.catalog_ready = False
        self.service._selected_options = ViewerService._selected_options.__get__(self.service)
        with patch('processing.viewer_service.discover_node', return_value={'available': True, 'options': OPTIONS}) as discover:
            self.service.tick()
        discover.assert_called_once_with('local', self.config.local_node_url, self.config.local_node_token)
        self.assertEqual(self.task(identifier)['node_id'], 'cluster')
        self.assertEqual(self.engines['main'].calls, [])
        self.finish(identifier); self.service.tick()
        stream, _ = self.service.download(identifier)
        with stream: self.assertEqual(stream.read(), self.engines['cluster'].archive)

    def test_new_submission_still_requires_fleet_catalog(self):
        self.catalog_ready = False
        with self.assertRaises(Conflict): self.initialize()

    def test_selected_engine_option_refresh_refuses_missing_or_unavailable(self):
        identifier = self.submit()
        self.service._selected_options = ViewerService._selected_options.__get__(self.service)
        for snapshot, error in (({'available':False,'options':[]}, Conflict),
                                ({'available':True,'options':[item for item in OPTIONS if item['name']!='pc-ept']}, ValueError)):
            with self.subTest(snapshot=snapshot), patch('processing.viewer_service.discover_node', return_value=snapshot):
                with self.assertRaises(error): self.service.tick()
            self.assertEqual(self.task(identifier)['job']['status'], 'queued')
            self.assertEqual(self.engines['cluster'].calls, [])

    def test_selected_engine_changed_definition_is_refused(self):
        identifier = self.submit()
        self.service._selected_options = ViewerService._selected_options.__get__(self.service)
        changed = [dict(item, type='string') if item['name']=='pc-ept' else dict(item) for item in OPTIONS]
        with patch('processing.viewer_service.discover_node',return_value={'available':True,'options':changed}):
            with self.assertRaises(Conflict):self.service.tick()
        self.assertEqual(self.task(identifier)['job']['status'],'queued')
        self.assertEqual(self.engines['cluster'].calls,[])

    def test_new_task_missing_evidence_is_held_even_if_fleet_is_healthy(self):
        identifier=self.submit()
        with self.queue._db() as db:db.execute('DELETE FROM viewer_option_evidence WHERE uuid=?',(identifier,))
        with self.assertRaises(Conflict):self.task(identifier)
        with self.assertRaises(Conflict):self.service.tick()
        self.assertEqual(self.engines['cluster'].calls,[])

    def test_corrupt_completed_task_does_not_block_new_job(self):
        finished=self.submit();self.service.tick();self.finish(finished);self.service.tick()
        with self.queue._db() as db:db.execute('DELETE FROM viewer_option_evidence WHERE uuid=?',(finished,))
        next_job=self.submit();self.service.tick()
        self.assertEqual(self.task(next_job)['job']['status'],'running')
        with self.assertRaises(Conflict):self.task(finished)

    def test_corrupt_queued_staging_does_not_block_production(self):
        staging=self.submit(environment='staging');production=self.submit(environment='production')
        with self.queue._db() as db:db.execute('DELETE FROM viewer_option_evidence WHERE uuid=?',(staging,))
        self.service.tick()
        self.assertEqual(self.task(production)['job']['status'],'running')
        with self.assertRaises(Conflict):self.task(staging)

    def test_legacy_job_without_evidence_still_requires_full_fleet(self):
        identifier=str(uuid.uuid4())
        self.service.store.initialize(identifier,'Legacy task',REQUEST_OPTIONS,None,'production')
        self.upload(identifier);self.service.commit(identifier)
        self.assertIsNone(self.task(identifier)['option_evidence'])
        self.catalog_ready=False;self.unavailable.add('main')
        with self.assertRaises(Conflict):self.service.tick()
        self.assertEqual(self.task(identifier)['job']['status'],'queued')
        self.assertEqual(self.engines['cluster'].calls,[])

    def test_evidence_survives_restart_and_missing_table_migrates_conservatively(self):
        identifier=self.submit();self.assertIsNotNone(self.task(identifier)['option_evidence'])
        self.service.close();self.queue.close();self.make_queue_service()
        self.assertIsNotNone(self.task(identifier)['option_evidence'])
        self.catalog_ready=False;self.unavailable.add('main')
        with patch('processing.viewer_service.discover_node',return_value={'available':True,'options':OPTIONS}):
            self.service._selected_options=ViewerService._selected_options.__get__(self.service)
            self.service.tick()
        self.assertEqual(self.task(identifier)['node_id'],'cluster')

    def test_missing_evidence_table_is_created_without_backfilling_legacy_job(self):
        identifier=str(uuid.uuid4())
        self.service.store.initialize(identifier,'Legacy task',REQUEST_OPTIONS,None,'production')
        self.upload(identifier);self.service.commit(identifier)
        with self.queue._db() as db:
            db.execute('DROP TABLE viewer_option_evidence')
            db.execute('ALTER TABLE viewer_tasks DROP COLUMN option_evidence_required')
        self.service.close();self.queue.close();self.make_queue_service()
        self.assertIsNone(self.task(identifier)['option_evidence'])
        self.catalog_ready=False;self.unavailable.add('main')
        with self.assertRaises(Conflict):self.service.tick()
        self.assertEqual(self.task(identifier)['job']['status'],'queued')

    def test_large_queued_job_uses_main_when_cluster_catalog_is_unavailable(self):
        self.config.local_max_photos = 1
        identifier = self.submit(count=2)
        self.unavailable.add('cluster'); self.catalog_ready = False
        self.service._selected_options = ViewerService._selected_options.__get__(self.service)
        with patch('processing.viewer_service.discover_node', return_value={'available': True, 'options': OPTIONS}) as discover:
            self.service.tick()
        discover.assert_called_once_with('main', self.config.main_node_url, self.config.main_node_token)
        self.assertEqual(self.task(identifier)['node_id'], 'main')
        self.assertEqual(self.engines['cluster'].calls, [])

    def test_native_output_cursor_preserves_blank_and_comma_records(self):
        identifier = self.submit()
        self.service.tick()
        self.assertEqual(self.service.output(identifier, 1), ['', 'comma,record'])
        self.engines['cluster'].output_unavailable = True
        self.service.store.update(identifier, output_lines=['retained output'])
        self.assertEqual(self.service.output(identifier, 0), ['retained output'])


if __name__ == '__main__':
    unittest.main()
