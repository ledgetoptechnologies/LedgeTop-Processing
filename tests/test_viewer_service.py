"""Synthetic coordinator integration: real queue/store, exact fake engine bytes.

These tests prove dispatch/transport contracts, not genuine photogrammetry or
Viewer output ingestion. No network, Docker, real datasets or host writes.
"""

import hashlib
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import subprocess
import sys
import io
import json
import os
import subprocess
import sys
from pathlib import Path
import sqlite3
import tempfile
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid
import zipfile

from processing.maintenance import Conflict
from processing.odx_smoke_worker import SmokeError
from processing.prototype import PROFILE, VIEWER_PROFILE, Queue
from processing.admission_drain import AdmissionClosed
from processing.viewer_result_adoption import ADOPTION_NAME
from processing.viewer_tasks import TaskStore
from processing.viewer_output_log import (ARCHIVED_OUTPUT_MAX_BYTES, CLIPPED, MARKER, OUTPUT_ANCHOR_BYTES,
                                          OUTPUT_ANCHOR_LINES, OUTPUT_LINE_CHARS, OUTPUT_LOG_NAME,
                                          OUTPUT_TOTAL_BYTES, archived_output_lines)
from processing.viewer_service import (ENGINE_LOST_SECONDS, ENGINE_MISSING_POLLS, EngineTaskMissing,
                                       EngineUnavailable, NODE_LOST_POLLS, NativeEngine,
                                       OUTPUT_SNAPSHOT_ATTEMPTS, PartialLines, ViewerService)
from tests.reference_activation_fixture import activated_prepared_v3_job


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
            raise EngineTaskMissing('synthetic_task_not_found')
        if kind == 'info':
            return {'uuid': identifier, 'status': dict(task['status']), 'progress': task['progress'],
                    **{key: task[key] for key in ('processingTime',) if key in task}, **task.get('identity', {})}
        if kind == 'output':
            return task.get('console', ['synthetic engine output', '', 'comma,record'])[query.get('line', 0):]
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

    def test_queue_indicator_covers_all_waiting_jobs_only_until_dispatch(self):
        uploading = self.initialize()
        self.assertNotIn('ledgetopQueueState', self.service.info(uploading))
        first = self.submit()
        second = self.submit()
        for identifier in (first, second):
            info = self.service.info(identifier)
            self.assertEqual(info['status']['code'], 10)
            self.assertEqual(info['ledgetopQueueState'], 'waiting_for_dispatch')
        self.service.tick()
        claimed = self.service.info(first)
        self.assertEqual(claimed['status']['code'], 10)
        self.assertNotIn('ledgetopQueueState', claimed)
        self.assertEqual(self.service.info(second)['ledgetopQueueState'], 'waiting_for_dispatch')
        self.service._hold(self.task(first), 'synthetic_hold')
        held = self.service.info(first)
        self.assertTrue(held['ledgetopHeld'])
        self.assertNotIn('ledgetopQueueState', held)
        self.service.cancel(second)
        self.assertNotIn('ledgetopQueueState', self.service.info(second))

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

    def test_real_http_commit_conflicts_between_multipart_files(self):
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        from urllib.error import HTTPError
        from urllib.request import Request, urlopen
        from processing.viewer_http import ViewerFacade
        from tests.test_viewer_http import multipart, BOUNDARY

        self.queue.parallel_viewer_uploads = True
        identifier = self.initialize()
        spool = self.root / 'http-interleave-spool'; spool.mkdir(mode=0o700)
        facade = ViewerFacade(self.service, 'synthetic-production-token', temp_dir=spool)
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_): pass
            def do_POST(self): facade.handle(self)
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()
        base = f'http://127.0.0.1:{server.server_port}/task/new'
        suffix = f'/{identifier}?token=synthetic-production-token'
        paused, release = threading.Event(), threading.Event()
        original = self.service.upload
        results = []
        def upload_and_pause(*args, **kwargs):
            result = original(*args, **kwargs)
            if not paused.is_set():
                paused.set()
                if not release.wait(10):
                    raise RuntimeError('test upload release timed out')
            return result
        def upload_request():
            try:
                request = Request(base + '/upload' + suffix,
                    data=multipart([('images', 'one.jpg', b'one'), ('images', 'two.jpg', b'two')]),
                    headers={'Content-Type': 'multipart/form-data; boundary=' + BOUNDARY})
                with urlopen(request, timeout=10) as response:
                    results.append((response.status, json.load(response)))
            except Exception as exc:
                results.append(exc)
        worker = threading.Thread(target=upload_request)
        try:
            with patch.object(self.service, 'upload', side_effect=upload_and_pause):
                worker.start()
                self.assertTrue(paused.wait(5))
                with self.assertRaises(HTTPError) as caught:
                    urlopen(Request(base + '/commit' + suffix, data=b''), timeout=5)
                self.assertEqual(caught.exception.code, 409)
                self.assertEqual([f['name'] for f in self.task(identifier)['files']], ['one.jpg'])
                release.set(); worker.join(10)
                self.assertFalse(worker.is_alive())
                self.assertEqual(results, [(200, {'success': True})])
            with urlopen(Request(base + '/commit' + suffix, data=b''), timeout=5) as response:
                self.assertEqual(response.status, 200)
            self.assertEqual([f['name'] for f in self.task(identifier)['files']], ['one.jpg', 'two.jpg'])
        finally:
            release.set()
            if worker.ident is not None: worker.join(10)
            server.shutdown(); server.server_close(); server_thread.join(timeout=2)
            self.assertFalse(worker.is_alive())
            self.assertFalse(server_thread.is_alive())

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
        self.assertEqual(self.task(third)['node_id'], 'cluster')  # Running production keeps only main.
        self.assertEqual(self.task(second)['job']['status'], 'running')

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

    def hold_state(self, identifier):
        """Read queue and task halves as a separate connection, in one snapshot."""
        reader = sqlite3.connect(self.queue.path)
        try:
            return reader.execute("""SELECT j.status,t.phase FROM viewer_tasks t JOIN jobs j ON j.id=t.job_id
                                     WHERE t.uuid=?""", (identifier,)).fetchone()
        finally:
            reader.close()

    def test_every_committed_hold_state_is_coherent(self):
        """SQLite exposes only commits, so sampling each one covers every interleaving."""
        self.queue.per_node_slots = True
        self.service._parallel_dispatch = True
        self.engines['cluster'].fail = 'upload'
        identifier = self.submit()
        states, original = [], self.queue._db
        @contextmanager
        def sampled():
            with original() as db:
                yield db
            if threading.current_thread().name == 'viewer-node-transfer':
                states.append(self.hold_state(identifier))
        with patch.object(self.queue, '_db', sampled):
            self.service.tick()
            self.service.wait_for_transfers()
        self.assertEqual(states[-1], ('held', 'held'))
        self.assertEqual([state for state in states if (state[0] == 'held') != (state[1] == 'held')], [])

    def test_observer_cannot_read_a_hold_until_both_halves_commit(self):
        self.queue.per_node_slots = True
        self.service._parallel_dispatch = True
        self.engines['cluster'].fail = 'upload'
        identifier = self.submit()
        parked, resume, seen = threading.Event(), threading.Event(), []
        store = self.service.store
        original = store._hold_in_transaction
        def park(*args):
            original(*args)  # Both rows written, not committed.
            parked.set()
            self.assertTrue(resume.wait(10))
        with patch.object(store, '_hold_in_transaction', side_effect=park):
            self.service.tick()
            self.assertTrue(parked.wait(10))
            self.assertEqual(self.hold_state(identifier), ('running', 'upload_pending'))
            observer = threading.Thread(target=lambda: seen.append(self.task(identifier)), daemon=True)
            observer.start()
            observer.join(0.1)
            self.assertEqual(seen, [])  # In-process readers wait for the commit.
            resume.set()
            observer.join(10)
            self.service.wait_for_transfers()
        self.assertEqual((seen[0]['job']['status'], seen[0]['phase'], seen[0]['error']),
                         ('held', 'held', 'engine_submission_uncertain'))

    def test_hold_keeps_a_settled_job_and_rejects_unsafe_reasons_before_writing(self):
        identifier = self.submit()
        self.service.tick()
        task = self.task(identifier)
        with self.assertRaises(ValueError):
            self.service.store.hold(identifier, 'not a reason code')
        self.assertEqual(self.hold_state(identifier), ('running', 'processing'))
        self.queue.operator(task['job_id'], 'hold', {'note': 'inspect'})
        self.queue.operator(task['job_id'], 'resolve', {'note': 'synthetic', 'workerStopped': True})
        self.service.store.hold(identifier, 'late_observation')
        self.assertEqual(self.hold_state(identifier), ('failed', 'held'))
        self.assertEqual(self.task(identifier)['job']['result']['operatorAction'], 'resolve')

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

    def test_opt_in_slow_inbound_upload_rejects_cancel_without_blocking_other_dispatch(self):
        self.queue.parallel_viewer_uploads = True
        queued = self.submit()
        incoming = self.initialize()
        entered, release, done = threading.Event(), threading.Event(), threading.Event()
        errors = []
        class SlowStream(io.BytesIO):
            def read(self, size=-1):
                if not entered.is_set():
                    entered.set()
                    if not release.wait(5): raise TimeoutError('test upload not released')
                return super().read(size)
        def upload():
            try: self.service.upload(incoming, 'slow.jpg', SlowStream(b'abc'), 3)
            except BaseException as exc: errors.append(exc)
        def tick():
            try: self.service.tick()
            except BaseException as exc: errors.append(exc)
            finally: done.set()
        uploader, scheduler = threading.Thread(target=upload), threading.Thread(target=tick)
        uploader.start()
        try:
            self.assertTrue(entered.wait(5))
            with self.assertRaisesRegex(Conflict, 'upload is in progress'):
                self.service.cancel(incoming)
            scheduler.start()
            self.assertTrue(done.wait(3), 'unrelated dispatch stalled behind inbound upload')
            self.assertEqual(self.task(queued)['job']['status'], 'running')
        finally:
            release.set()
            uploader.join(5)
            if scheduler.ident is not None: scheduler.join(5)
        self.assertFalse(uploader.is_alive())
        self.assertFalse(scheduler.is_alive())
        self.assertEqual(errors, [])

    def test_multipart_batch_fence_covers_gap_between_files(self):
        self.queue.parallel_viewer_uploads = True
        incoming = self.initialize()
        first_done, resume = threading.Event(), threading.Event()
        errors = []
        def batch():
            try:
                with self.service.upload_batch(incoming):
                    self.service.upload(incoming, 'first.jpg', io.BytesIO(b'first'), 5)
                    first_done.set()
                    if not resume.wait(5): raise TimeoutError('test batch not released')
                    self.service.upload(incoming, 'second.jpg', io.BytesIO(b'second'), 6)
            except BaseException as exc: errors.append(exc)
        worker = threading.Thread(target=batch)
        worker.start()
        try:
            self.assertTrue(first_done.wait(5))
            with self.assertRaisesRegex(Conflict, 'upload is in progress'):
                self.service.commit(incoming)
        finally:
            resume.set()
            worker.join(5)
        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.assertTrue(self.service.commit(incoming)['success'])
        self.assertEqual(len(self.task(incoming)['files']), 2)

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

    def test_opt_in_staging_uses_idle_node_beside_running_production(self):
        self.queue.per_node_slots = True
        production = self.submit(environment='production')
        self.service.tick()
        staging = self.submit(environment='staging')
        self.service.tick()
        self.assertEqual((self.task(production)['node_id'], self.task(staging)['node_id']), ('cluster', 'main'))

    def test_opt_in_held_production_blocks_staging_on_every_node(self):
        self.queue.per_node_slots = True
        production = self.submit(environment='production')
        self.service.tick()
        self.queue.operator(self.task(production)['job_id'], 'hold', {'note':'inspect'})
        staging = self.submit(environment='staging')
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

    def test_engine_returns_to_queued_without_losing_claim_or_replaying(self):
        identifier = self.submit()
        self.service.tick()
        self.service.tick()
        task = self.task(identifier)
        self.assertEqual(self.service.info(identifier)['status']['code'], 20)
        node, engine_uuid = task['node_id'], task['engine_uuid']
        before = list(self.engines[node].calls)
        self.engines[node].tasks[engine_uuid]['status']['code'] = 10
        self.service.tick()
        observed = self.task(identifier)
        self.assertEqual(self.service.info(identifier)['status']['code'], 10)
        self.assertEqual(observed['job']['status'], 'running')
        self.assertEqual((observed['node_id'], observed['engine_uuid']), (node, engine_uuid))
        later_calls = self.engines[node].calls[len(before):]
        self.assertTrue(later_calls)
        self.assertTrue(all(call[0] == 'read' for call in later_calls))

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

    def test_restart_offline_node_retains_result_and_other_node_can_advance_queue(self):
        self.queue.per_node_slots = True
        first, second, waiting = self.submit(), self.submit(), self.submit()
        self.service.tick(); self.service.tick()
        original = {i: (self.task(i)['node_id'], self.task(i)['engine_uuid'])
                    for i in (first, second)}
        self.assertEqual([original[i][0] for i in (first, second)], ['cluster', 'main'])
        self.service.close(); self.queue.close()
        self.engines['cluster'].finish(original[first][1])
        self.engines['cluster'].read_unavailable = True
        self.engines['main'].finish(original[second][1])
        self.make_queue_service(); self.queue.per_node_slots = True
        self.service.tick()
        self.assertEqual(self.task(first)['job']['status'], 'held')
        self.assertEqual(self.service.info(first)['status']['code'], 10)
        self.assertEqual(self.task(second)['job']['status'], 'succeeded')
        self.assertEqual(self.task(waiting)['node_id'], 'main')
        self.assertEqual(self.task(first)['engine_uuid'], original[first][1])
        self.assertEqual(len(self.fleet.commits), 3)
        self.engines['cluster'].read_unavailable = False
        self.service._backoff[first] = (0, self.service._backoff[first][1])
        self.service.tick()
        self.assertEqual(self.service.info(first)['status']['code'], 40)
        with self.service.download(first)[0] as stream:
            self.assertEqual(stream.read(), self.engines['cluster'].archive)
        self.assertEqual(sum(call[0] == 'initialize' for call in self.engines['cluster'].calls), 1)
        self.assertEqual(self.task(waiting)['node_id'], 'main')

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

    def test_guard_readiness_alone_cannot_start_or_send_native_mutations(self):
        # A readiness snapshot is not a guarded mutation transport.
        self.config.viewer_require_guard = True
        with self.assertRaisesRegex(ValueError, 'factory required'):
            ViewerService(self.queue, self.config, engines=self.engines,
                probe=self.probe, guard_probe=lambda _: True)
        self.assertEqual(self.engines['cluster'].calls, [])
        self.assertEqual(self.engines['main'].calls, [])

    def test_guarded_inventory_skips_guard_unavailable_then_recovers(self):
        from processing.config import parse_node_inventory
        self.service.close(); self.queue.close()
        entries = [dict(id=name, physicalHostId=name, origin=f'http://192.0.2.{i}:3000',
                        tokenEnv=f'NODE_{i}_TOKEN', maxPhotos=None, capabilities=['odm'])
                   for i, name in enumerate(('tiny', 'large'), 1)]
        self.config.node_inventory = parse_node_inventory(json.dumps(entries), {})
        self.config.prototype_state_dir = str(self.root / 'guard-inventory-state')
        self.config.viewer_require_guard = True
        self.engines = {n['id']: FakeEngine(n['id'], self.fleet) for n in self.config.node_inventory}
        self.queue = Queue(self.config.prototype_state_dir, node_ids=('tiny', 'large'),
                           treedar_node_ids=(), viewer_enabled=True, per_node_slots=True)
        guard = {'tiny': False, 'large': True}
        self.service = ViewerService(self.queue, self.config, engines=self.engines,
            probe=lambda role, origin, token: {'ready': True, 'maxImages': None, 'taskQueueCount': 0},
            guard_probe=lambda node: guard[node], guarded_engine_factory=lambda _: None)
        # Scheduling-only fixture; real mutation boundary is exercised by
        # the separate mTLS guarded factory integration test.
        self.service._task_engine = lambda task: self.engines[task['node_id']]
        self.service._catalog = lambda: {'ready': True, 'commonOptions': [dict(item) for item in OPTIONS]}
        self.service._selected_options = lambda _: [dict(item) for item in OPTIONS]
        first = self.submit(); self.service.tick()
        self.assertEqual(self.task(first)['node_id'], 'large')
        guard['tiny'] = True
        second = self.submit(); self.service.tick()
        self.assertEqual(self.task(second)['node_id'], 'tiny')

    def test_ordered_three_node_inventory_prefers_first_eligible_and_falls_through(self):
        from processing.config import parse_node_inventory
        class DistinctEngine(FakeEngine):
            def read(self, route, *, missing=False, **query):
                result = super().read(route, missing=missing, **query)
                if route.endswith('/output'):
                    identifier = route.split('/')[2]
                    return [self.name + ':' + identifier][query.get('line', 0):]
                return result

            def download(self, identifier):
                self.calls.append(('download', identifier))
                body = io.BytesIO()
                with zipfile.ZipFile(body, 'w') as archive:
                    archive.writestr('synthetic-result.txt', self.name + ':' + identifier)
                return DownloadResponse(body.getvalue())

        self.service.close(); self.queue.close()
        entries = [dict(id=name, physicalHostId=name, origin=f'http://192.0.2.{i}:3000',
                        tokenEnv=f'NODE_{i}_TOKEN', maxPhotos=limit, capabilities=['odm'])
                   for i, (name, limit) in enumerate((('tiny', 1), ('medium', 2), ('large', None)), 1)]
        inventory = parse_node_inventory(json.dumps(entries), {})
        self.config.node_inventory = inventory
        self.config.prototype_state_dir = str(self.root / 'inventory-state')
        self.engines = {n['id']: DistinctEngine(n['id'], self.fleet) for n in inventory}
        self.queue = Queue(self.config.prototype_state_dir, node_ids=tuple(n['id'] for n in inventory),
                           treedar_node_ids=(), viewer_enabled=True, per_node_slots=True)
        self.service = ViewerService(self.queue, self.config, engines=self.engines,
            probe=lambda role, origin, token: {'ready': role not in self.unavailable, 'maxImages': None,
                'taskQueueCount': sum(x.startswith(role + ':') for x in self.fleet.active)})
        self.service._catalog = lambda: {'ready': True, 'commonOptions': [dict(item) for item in OPTIONS],
                                        'nodes': [{'available':True, 'options':[dict(item) for item in OPTIONS]}]}
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
        self.assertEqual(self.service.output(first), ['tiny:' + self.task(first)['engine_uuid']])
        self.assertIn(('read', self.task(first)['engine_uuid'], 'output'), self.engines['tiny'].calls)
        self.finish(first)
        blocked = self.submit(count=3)
        eligible = self.submit()
        self.service.tick()
        self.assertIsNone(self.task(blocked)['node_id'])
        self.assertEqual(self.service.info(blocked)['status']['code'], 10)
        self.assertEqual(self.service.info(blocked)['ledgetopQueueState'], 'waiting_for_dispatch')
        self.assertEqual(self.task(eligible)['node_id'], 'tiny')
        self.assertNotIn('ledgetopQueueState', self.service.info(eligible))
        self.finish(eligible); self.finish(third)
        self.service.tick()
        archives = []
        for completed in (first, third):
            with self.service.download(completed)[0] as stream:
                with zipfile.ZipFile(io.BytesIO(stream.read())) as archive:
                    archives.append(archive.read('synthetic-result.txt').decode())
        self.assertEqual(archives, ['tiny:' + self.task(first)['engine_uuid'],
                                    'large:' + self.task(third)['engine_uuid']])
        self.assertNotEqual(*archives)
        self.assertEqual(self.task(blocked)['node_id'], 'large')
        self.assertEqual(self.service.info(blocked)['status']['code'], 10)
        self.assertNotIn('ledgetopQueueState', self.service.info(blocked))
        self.service.tick()
        self.assertEqual(self.service.info(blocked)['status']['code'], 20)
        self.assertEqual(self.service.info(blocked)['ledgetopNodeId'], 'large')
        self.assertEqual(self.service.output(blocked), ['large:' + self.task(blocked)['engine_uuid']])
        self.assertIn(('read', self.task(blocked)['engine_uuid'], 'output'), self.engines['large'].calls)
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

    def test_viewer_uses_small_node_behind_oversized_reference_job(self):
        from processing.reference_store import ReferenceStore
        from processing import reference_protocol as protocol
        self.service.close(); self.queue.close()
        inventory = (dict(id='cluster', physicalHostId='cluster', origin='http://192.0.2.1:3000',
                          token='', maxPhotos=1, capabilities=('odm',)),
                     dict(id='main', physicalHostId='main', origin='http://192.0.2.2:3000',
                          token='', maxPhotos=None, capabilities=('odm',)))
        self.config.node_inventory = inventory
        self.config.prototype_state_dir = str(self.root / 'mixed-profile-state')
        self.queue = Queue(self.config.prototype_state_dir, node_ids=('cluster', 'main'),
                           treedar_node_ids=(), viewer_enabled=True, reference_enabled=True,
                           per_node_slots=True, node_photo_limits={'cluster': 1, 'main': None})
        self.service = ViewerService(self.queue, self.config, engines=self.engines,
            probe=lambda role, origin, token: {'ready': True, 'maxImages': None, 'taskQueueCount': 0})
        self.service._catalog = lambda: {'ready': True, 'commonOptions': [dict(item) for item in OPTIONS]}
        self.service._selected_options = lambda _: [dict(item) for item in OPTIONS]
        fixture = json.loads((Path(__file__).resolve().parents[1] / 'contracts/reference-v1.contract.json').read_text())
        raw = fixture['submission']; manifest = raw['manifest']
        manifest['objects'] = [{**manifest['objects'][0], 'id': f'image-{i}',
                                'name': f'image-{i}.jpg'} for i in range(2)]
        manifest.update(objectCount=2, totalBytes=6,
                        inputCounts={'images': 2, 'lidarObjects': 0, 'points': 0})
        manifest['sha256'] = protocol.manifest_sha256(manifest)
        reference = ReferenceStore(self.queue)
        first, _ = reference.submit(protocol.validate_submission(raw, environment='production',
            capability_fingerprint=raw['capabilityFingerprint']))
        viewer = self.submit()
        self.service.tick()
        self.assertEqual(self.task(viewer)['node_id'], 'cluster')
        self.assertEqual(self.queue.get(first['jobId'])['status'], 'queued')
        self.assertEqual(self.service.info(viewer)['status']['code'], 10)
        self.service.tick()
        self.assertEqual(self.service.info(viewer)['status']['code'], 20)

    def mixed_inventory_service(self, name, probe, *, readiness=None):
        from processing.reference_store import ReferenceStore
        self.service.close(); self.queue.close()
        inventory = (dict(id='cluster', physicalHostId='cluster', origin='http://192.0.2.1:3000',
                          token='', maxPhotos=1, capabilities=('odm',)),
                     dict(id='main', physicalHostId='main', origin='http://192.0.2.2:3000',
                          token='', maxPhotos=None, capabilities=('odm',)))
        self.config.node_inventory = inventory
        self.config.prototype_state_dir = str(self.root / name)
        self.queue = Queue(self.config.prototype_state_dir, node_ids=('cluster', 'main'),
                           treedar_node_ids=(), viewer_enabled=True, reference_enabled=True, per_node_slots=True,
                           node_capabilities={'cluster': ('odm',), 'main': ('odm',)},
                           node_photo_limits={'cluster': 1, 'main': None})
        self.service = ViewerService(self.queue, self.config, engines=self.engines, probe=probe)
        self.service._catalog = lambda: {'ready': True, 'commonOptions': [dict(item) for item in OPTIONS]}
        self.service._selected_options = lambda _: [dict(item) for item in OPTIONS]
        if not readiness:
            return None
        # Only exercises post-activation mechanics with a synthetic future grant.
        ready = patch.object(ReferenceStore, '_activation_bound_grant_ready', return_value=True)
        ready.start()
        self.addCleanup(ready.stop)
        return ReferenceStore(self.queue, jcs_runtime_enabled=True, node_inventory=inventory, node_readiness=readiness)

    def test_viewer_uses_idle_node_behind_reference_exceeding_its_scratch(self):
        from processing import reference_protocol as protocol
        reports = {node: {'ready': True, 'taskQueueCount': 0, 'maxImages': None, 'outputBudgetBytes': 1024,
                          'availableScratchBytes': 10 ** 9} for node in ('cluster', 'main')}
        reports['cluster']['availableScratchBytes'] = 5000
        reference = self.mixed_inventory_service('scratch-proof-state',
            lambda role, origin, token: {'ready': True, 'maxImages': None, 'taskQueueCount': 0},
            readiness=lambda node: reports[node])
        fixture = json.loads((Path(__file__).resolve().parents[1] / 'contracts/reference-v1.contract.json').read_text())
        def submit(images, size):
            raw = json.loads(json.dumps(fixture['submission']))
            raw['submissionId'] = str(uuid.uuid4())
            manifest = raw['manifest']
            manifest['objects'] = [{**manifest['objects'][0], 'id': f'image-{i}', 'name': f'image-{i}.jpg',
                                    'byteSize': size} for i in range(images)]
            manifest.update(objectCount=images, totalBytes=images * size)
            manifest['inputCounts']['images'] = images
            manifest['sha256'] = protocol.manifest_sha256(manifest)
            return activated_prepared_v3_job(reference, fixture, protocol.validate_submission(
                raw, environment='production', capability_fingerprint=raw['capabilityFingerprint']))
        running = submit(2, 3)
        self.assertIsNotNone(reference.claim(running['jobId'], node_id='main', worker_id='main',
            worker_instance_id='instance', claim_request_id='main-claim'))
        waiting = submit(1, 100000)
        viewer = self.submit()
        known, reports['cluster'] = reports['cluster'], None
        self.service.tick()
        self.assertIsNone(self.task(viewer)['node_id'])  # Scratch unknown: the barrier holds.
        reports['cluster'] = known
        self.service.tick()
        self.assertEqual(self.task(viewer)['node_id'], 'cluster')
        self.assertEqual(self.queue.get(waiting['jobId'])['status'], 'queued')

    def staging_past_production_service(self, name):
        reports = {node: {'ready': True, 'taskQueueCount': 0, 'maxImages': None, 'outputBudgetBytes': 1024,
                          'availableScratchBytes': 10 ** 9} for node in ('cluster', 'main')}
        reference = self.mixed_inventory_service(name, lambda role, origin, token: reports[role],
                                                 readiness=lambda node: reports[node])
        return reference, reports

    def production_reference(self, reference, images):
        from processing import reference_protocol as protocol
        fixture = json.loads((Path(__file__).resolve().parents[1] / 'contracts/reference-v1.contract.json').read_text())
        raw = json.loads(json.dumps(fixture['submission']))
        raw['submissionId'] = str(uuid.uuid4())
        manifest = raw['manifest']
        manifest['objects'] = [{**manifest['objects'][0], 'id': f'image-{i}', 'name': f'image-{i}.jpg',
                                'byteSize': 3} for i in range(images)]
        manifest.update(objectCount=images, totalBytes=images * 3)
        manifest['inputCounts']['images'] = images
        manifest['sha256'] = protocol.manifest_sha256(manifest)
        return activated_prepared_v3_job(reference, fixture, protocol.validate_submission(
            raw, environment='production', capability_fingerprint=raw['capabilityFingerprint']))

    def test_staging_viewer_takes_idle_cluster_past_oversized_production_viewer(self):
        for name, production_first in (('production-first', True), ('staging-first', False)):
            with self.subTest(order=name):
                _, reports = self.staging_past_production_service(name)
                reports['main']['taskQueueCount'] = 1
                if production_first:
                    production = self.submit(environment='production', count=2)
                    staging = self.submit(environment='staging')
                else:
                    staging = self.submit(environment='staging')
                    production = self.submit(environment='production', count=2)
                self.service.tick()
                self.assertEqual(self.task(staging)['node_id'], 'cluster')
                self.assertIsNone(self.task(production)['node_id'])
                reports['main']['taskQueueCount'] = 0
                self.service.tick()
                self.assertEqual(self.task(production)['node_id'], 'main')

    def test_staging_viewer_takes_idle_cluster_past_oversized_production_reference(self):
        for name, production_first in (('reference-first', True), ('viewer-first', False)):
            with self.subTest(order=name):
                reference, reports = self.staging_past_production_service(name)
                reports['main']['taskQueueCount'] = 1
                if production_first:
                    production = self.production_reference(reference, 2)
                    staging = self.submit(environment='staging')
                else:
                    staging = self.submit(environment='staging')
                    production = self.production_reference(reference, 2)
                self.service.tick()
                self.assertEqual(self.task(staging)['node_id'], 'cluster')
                self.assertEqual(self.queue.get(production['jobId'])['status'], 'queued')
                reports['main']['taskQueueCount'] = 0
                self.assertEqual(reference.next_for_node('main')['jobId'], production['jobId'])
                self.assertIsNotNone(reference.claim(production['jobId'], node_id='main', worker_id='main',
                    worker_instance_id='instance', claim_request_id='main-claim'))

    def test_staging_viewer_waits_while_production_reference_lands_on_idle_main(self):
        reference, _ = self.staging_past_production_service('landing-state')
        production = self.production_reference(reference, 2)
        staging = self.submit(environment='staging')
        self.service.tick()
        self.assertIsNone(self.task(staging)['node_id'])
        self.assertEqual(reference.next_for_node('main')['jobId'], production['jobId'])

    def test_production_viewer_that_fits_takes_cluster_over_staging(self):
        for name, production_first in (('fits-production-first', True), ('fits-staging-first', False)):
            with self.subTest(order=name):
                _, reports = self.staging_past_production_service(name)
                reports['main']['taskQueueCount'] = 1
                if production_first:
                    production = self.submit(environment='production')
                    staging = self.submit(environment='staging')
                else:
                    staging = self.submit(environment='staging')
                    production = self.submit(environment='production')
                self.service.tick()
                self.assertEqual(self.task(production)['node_id'], 'cluster')
                self.service.tick()
                self.assertIsNone(self.task(staging)['node_id'])

    def test_unproven_production_keeps_staging_viewer_off_the_idle_node(self):
        reference, reports = self.staging_past_production_service('unproven-state')
        reports['main']['taskQueueCount'] = 1
        production = self.production_reference(reference, 2)
        staging = self.submit(environment='staging')
        with self.queue._db() as db:
            row = db.execute('SELECT input_json FROM reference_jobs WHERE job_id=?', (production['jobId'],)).fetchone()
            submission = json.loads(row['input_json'])
            submission['manifest']['totalBytes'] += 1
            db.execute('UPDATE reference_jobs SET input_json=? WHERE job_id=?',
                       (json.dumps(submission), production['jobId']))
        self.service.tick()
        self.assertIsNone(self.task(staging)['node_id'])
        self.assertEqual(self.fleet.commits, [])

    def test_production_added_during_selection_fences_staging_at_claim(self):
        _, reports = self.staging_past_production_service('late-production-state')
        reports['main']['taskQueueCount'] = 1
        self.submit(environment='production', count=2)
        staging = self.submit(environment='staging')
        late = self.initialize()
        self.upload(late, 'a.jpg')
        original = self.queue.staging_may_claim
        committed = []
        def commit_then_answer(node):
            answer = original(node)
            if not committed:
                committed.append(True)
                self.service.commit(late)
            return answer
        with patch.object(self.queue, 'staging_may_claim', side_effect=commit_then_answer):
            self.service.tick()
        self.assertIsNone(self.task(staging)['node_id'])
        self.assertEqual(self.task(staging)['job']['status'], 'queued')

    def test_corrupt_staging_behind_production_stops_only_staging(self):
        _, reports = self.staging_past_production_service('corrupt-staging-state')
        reports['main']['taskQueueCount'] = 1
        production = self.submit(environment='production', count=2)
        staging = self.submit(environment='staging')
        later = self.submit(environment='staging')
        with self.queue._db() as db:
            db.execute('DELETE FROM viewer_option_evidence WHERE uuid=?', (staging,))
        self.service.tick()
        self.assertIsNone(self.task(later)['node_id'])
        reports['main']['taskQueueCount'] = 0
        self.service.tick()
        self.assertEqual(self.task(production)['node_id'], 'main')

    def test_unanswered_node_holds_viewer_overflow_for_a_bounded_time(self):
        from processing import viewer_service
        cluster = [{'role': 'cluster', 'configured': True, 'ready': False, 'reason': 'probe_failed'}]
        self.mixed_inventory_service('unanswered-state', lambda role, origin, token: cluster[0] if role == 'cluster'
            else {'ready': True, 'maxImages': None, 'taskQueueCount': 0})
        small = self.submit()
        self.service.tick()
        self.assertIsNone(self.task(small)['node_id'])
        self.assertEqual(self.task(small)['error'], 'no_eligible_node')
        self.service._unanswered['cluster'] -= viewer_service.UNANSWERED_NODE_SECONDS
        self.service.tick()
        self.assertEqual(self.task(small)['node_id'], 'main')

    def test_silent_node_does_not_hold_a_viewer_job_it_cannot_run(self):
        self.mixed_inventory_service('oversized-state', lambda role, origin, token: {'ready': False}
            if role == 'cluster' else {'ready': True, 'maxImages': None, 'taskQueueCount': 0})
        large = self.submit(count=2)
        self.service.tick()
        self.assertEqual(self.task(large)['node_id'], 'main')

    def test_answered_not_ready_node_may_overflow_immediately(self):
        self.mixed_inventory_service('answered-state', lambda role, origin, token: {'ready': role == 'main',
            'maxImages': None, 'taskQueueCount': 0})
        small = self.submit()
        self.service.tick()
        self.assertEqual(self.task(small)['node_id'], 'main')

    def test_partial_catalog_saved_evidence_routes_only_to_compatible_returned_node(self):
        from tests.test_options import Engine
        self.service.close(); self.queue.close()
        first, second = Engine(), Engine()
        self.addCleanup(first.close); self.addCleanup(second.close)
        first.options = [dict(item) for item in OPTIONS]
        second.options = [dict(item) for item in OPTIONS]
        second.mode = 'unavailable'
        inventory = tuple(dict(id=name, physicalHostId=name, origin=engine.origin,
                               token=engine.token, maxPhotos=None, capabilities=('odm',))
                          for name, engine in [('first', first), ('second', second)])
        self.config.node_inventory = inventory
        self.config.prototype_state_dir = str(self.root / 'partial-inventory-state')
        self.engines = {n['id']: FakeEngine(n['id'], self.fleet) for n in inventory}
        self.queue = Queue(self.config.prototype_state_dir, node_ids=('first', 'second'),
                           treedar_node_ids=(), viewer_enabled=True, per_node_slots=True)
        self.service = ViewerService(self.queue, self.config, engines=self.engines,
            probe=lambda role, origin, token: {'ready': (first if role == 'first' else second).mode == 'normal',
                'maxImages': None, 'taskQueueCount': sum(x.startswith(role + ':') for x in self.fleet.active)})
        self.assertEqual(self.service.options(), sorted(OPTIONS, key=lambda option: option['name']))
        self.assertTrue(self.service.capabilities()['ledgetopCatalogDegraded'])
        identifier = self.submit()
        self.service.close(); self.queue.close()
        self.queue = Queue(self.config.prototype_state_dir, node_ids=('first', 'second'),
                           treedar_node_ids=(), viewer_enabled=True, per_node_slots=True)
        self.service = ViewerService(self.queue, self.config, engines=self.engines,
            probe=lambda role, origin, token: {'ready': (first if role == 'first' else second).mode == 'normal',
                'maxImages': None, 'taskQueueCount': sum(x.startswith(role + ':') for x in self.fleet.active)})
        # An omitted setting's default is local to each node and may change.
        first.options[1]['value'] = True
        self.service.tick()
        self.assertEqual(self.task(identifier)['node_id'], 'first')
        self.assertEqual(self.service.info(identifier)['ledgetopNodeId'], 'first')
        self.finish(identifier)
        self.service.tick()
        # The selected option is incompatible on the first node; a returning
        # second node supporting it must take the queued task instead.
        other = self.submit()
        first.options = [dict(item, type='string') if item['name'] == 'pc-ept' else dict(item) for item in first.options]
        self.service.tick()
        self.assertIsNone(self.task(other)['node_id'])
        second.mode = 'normal'
        self.service.tick()
        self.assertEqual(self.task(other)['node_id'], 'second')
        self.assertEqual(self.service.info(other)['ledgetopNodeId'], 'second')

    def test_direct_option_values_are_validated_before_admission(self):
        self.option_catalog = [
            {'name':'switch','type':'bool','value':False},
            {'name':'quality','type':'enum','value':'low','domain':['low','high']},
            {'name':'count','type':'int','value':1,'domain':{'min':1,'max':5}},
        ]
        for options in ([{'name':'switch','value':'true'}],
                        [{'name':'quality','value':'unknown'}],
                        [{'name':'count','value':True}],
                        [{'name':'count','value':6}],
                        [{'name':'count','value':1},{'name':'count','value':2}]):
            with self.subTest(options=options), self.assertRaises(ValueError):
                self.service.initialize(str(uuid.uuid4()), 'bad', options)
        self.service.initialize(str(uuid.uuid4()), 'valid', [{'name':'quality','value':'low'}])
        self.option_catalog[1]['domain'] = []
        with self.assertRaises(ValueError):
            self.service.initialize(str(uuid.uuid4()), 'invalid enum schema', [{'name':'quality','value':'low'}])

    def test_real_nodeodm_numeric_and_enum_domains(self):
        validate = ViewerService._option_values
        for kind, domain, valid, invalid in (
            ('enum', ['ultra','high','medium','low','lowest'], 'low', 'bad'),
            ('int', 'integer', -1, True),
            ('int', 'positive integer', 1, 0),
            ('float', 'float', -0.5, '0.5'),
            ('float', 'positive float', 0.5, -0.5),
            ('float', 'float > 0.0', 0.5, 0),
            ('int', 'integer: 1 <= x <= 14', 14, 15),
        ):
            with self.subTest(domain=domain):
                definition = [{'name':'x', 'type':kind, 'domain':domain}]
                validate([{'name':'x', 'value':valid}], definition)
                with self.assertRaises(ValueError):
                    validate([{'name':'x', 'value':invalid}], definition)
        with self.assertRaises(ValueError):
            validate([{'name':'x','value':1}], [{'name':'x','type':'int','domain':'unknown domain'}])

    def test_previous_full_catalog_policy_requires_reconciliation(self):
        with self.assertRaises(Conflict):
            self.service._check_options([], node_id='cluster', evidence=[], full_evidence=[])

    def test_unrecognized_object_domains_and_invalid_json_fail_closed(self):
        validate = ViewerService._option_values
        option = [{'name':'x', 'value':2}]
        for domain in ({}, {'step':2}, {'min':1, 'exclusiveMax':3},
                       {'min':None}, {'min':4, 'max':3}, {'min':True}):
            with self.subTest(domain=domain), self.assertRaises(ValueError):
                validate(option, [{'name':'x','type':'int','domain':domain}])
        validate(option, [{'name':'x','type':'int','domain':{'min':1,'max':3}}])
        json_spec = [{'name':'x','type':'string','domain':'json'}]
        for value in ('', ' ', 'NaN', '{'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate([{'name':'x','value':value}], json_spec)
        validate([{'name':'x','value':'{"key":1}'}], json_spec)

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
        self.assertEqual(self.service.info(identifier)['status']['code'], 10)
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
        # The split an earlier, two-transaction hold could leave on disk.
        self.queue.operator(self.task(first)['job_id'], 'hold', {'note': 'synthetic split hold'})
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
        self.assertEqual(self.service.info(first)['status']['code'],10)
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
                self.assertEqual(info['status']['code'], 10)
                self.assertIs(info['ledgetopHeld'], True)
                # Resolve only the fake's known-not-started engine so the next
                # independent subcase can use this shared test fixture.
                job = self.task(identifier)['job']
                self.queue.operator(job['id'], 'resolve', {'note': 'synthetic engine never committed',
                                                         'workerStopped': True})
                self.engines['cluster'].fail = None

    def test_guarded_lost_upload_retains_exact_checkpoint_across_restart(self):
        identifier = self.submit()
        self.engines['cluster'].node_guarded = True
        self.engines['cluster'].fail = 'upload'
        self.service.tick()
        task = self.task(identifier)
        self.assertEqual(task['job']['status'], 'held')
        checkpoint = self.service.store.guarded_dispatch_checkpoint(identifier)
        self.assertEqual((checkpoint['operation'], checkpoint['state']), ('upload', 'pending'))
        history = self.service.store.guarded_dispatch_history(identifier)
        self.assertEqual([(entry['operation'], entry['state']) for entry in history],
                         [('init', 'acknowledged'), ('upload', 'pending')])
        self.assertEqual(checkpoint['job_id'], task['job_id'])
        self.assertEqual(checkpoint['engine_uuid'], task['engine_uuid'])
        self.assertEqual(json.loads(checkpoint['evidence']), {
            key: task['files'][0][key] for key in ('name', 'bytes', 'sha256')})
        calls = list(self.engines['cluster'].calls)
        self.service.close()
        self.queue.close()
        self.make_queue_service()
        self.assertEqual(self.service.store.guarded_dispatch_checkpoint(identifier), checkpoint)
        self.assertEqual(self.service.store.guarded_dispatch_history(identifier), history)
        self.service.tick()
        self.assertEqual(self.engines['cluster'].calls, calls)
        self.assertEqual(self.task(identifier)['job']['status'], 'held')

    def test_guarded_success_checkpoints_commit_response(self):
        identifier = self.submit()
        self.engines['cluster'].node_guarded = True
        self.service.tick()
        task = self.task(identifier)
        checkpoint = self.service.store.guarded_dispatch_checkpoint(identifier)
        self.assertEqual((checkpoint['operation'], checkpoint['state']), ('commit', 'acknowledged'))
        self.assertEqual(json.loads(checkpoint['evidence']), {'engine_uuid': task['engine_uuid']})
        self.assertEqual(checkpoint['sequence'], len(task['files']) + 2)
        history = self.service.store.guarded_dispatch_history(identifier)
        self.assertEqual([entry['sequence'] for entry in history], list(range(1, len(task['files']) + 3)))
        self.assertEqual([entry['operation'] for entry in history],
                         ['init'] + ['upload'] * len(task['files']) + ['commit'])
        self.assertTrue(all(entry['state'] == 'acknowledged' for entry in history))

    def test_guarded_history_mismatch_rolls_back_acknowledgement(self):
        identifier = self.submit()
        engine = self.engines['cluster']
        engine.node_guarded = True
        original = engine.upload

        def upload(task_uuid, item):
            # Corrupt only the history row after intent is committed. An ACK
            # must not leave the current checkpoint acknowledged on its own.
            with self.queue._db() as db:
                db.execute("DELETE FROM viewer_guarded_dispatch_history WHERE uuid=? AND operation='upload'", (identifier,))
            return original(task_uuid, item)

        engine.upload = upload
        self.service.tick()
        checkpoint = self.service.store.guarded_dispatch_checkpoint(identifier)
        self.assertEqual((checkpoint['operation'], checkpoint['state']), ('upload', 'pending'))
        self.assertEqual(self.task(identifier)['job']['status'], 'held')

    def test_legacy_last_checkpoint_does_not_create_a_fabricated_history(self):
        identifier = self.submit()
        self.engines['cluster'].node_guarded = True
        self.service.tick()
        checkpoint = self.service.store.guarded_dispatch_checkpoint(identifier)
        with self.queue._db() as db:
            db.execute('DELETE FROM viewer_guarded_dispatch_history WHERE uuid=?', (identifier,))
        with self.assertRaises(Conflict):
            self.service.store.checkpoint_guarded_dispatch(identifier, 'commit',
                                                           {'engine_uuid': self.task(identifier)['engine_uuid']})
        self.assertEqual(self.service.store.guarded_dispatch_checkpoint(identifier), checkpoint)
        self.assertEqual(self.service.store.guarded_dispatch_history(identifier), [])

    def test_guarded_final_commit_ack_rejects_mismatched_history(self):
        identifier = self.submit()
        engine = self.engines['cluster']
        engine.node_guarded = True
        original = engine.action

        def action(kind, task_uuid):
            response = original(kind, task_uuid)
            if kind == 'commit':
                with self.queue._db() as db:
                    db.execute("UPDATE viewer_guarded_dispatch_history SET evidence='{}' WHERE uuid=? AND operation='commit'", (identifier,))
            return response

        engine.action = action
        self.service.tick()
        checkpoint = self.service.store.guarded_dispatch_checkpoint(identifier)
        self.assertEqual((checkpoint['operation'], checkpoint['state']), ('commit', 'pending'))
        self.assertEqual(self.task(identifier)['job']['status'], 'held')
        # A separately mismatched operation must also be rejected even when
        # the evidence is identical. Neither acknowledgement can proceed.
        with self.queue._db() as db:
            db.execute("UPDATE viewer_guarded_dispatch_history SET operation='upload', evidence=? WHERE uuid=? AND sequence=?",
                       (checkpoint['evidence'], identifier, checkpoint['sequence']))
        with self.assertRaises(Conflict):
            self.service.store.acknowledge_guarded_dispatch(identifier, checkpoint['sequence'])
        self.assertEqual(self.service.store.guarded_dispatch_checkpoint(identifier), checkpoint)

    def test_guarded_checkpoint_is_durable_before_engine_upload(self):
        identifier = self.submit()
        engine = self.engines['cluster']
        engine.node_guarded = True
        original = engine.upload

        def upload(task_uuid, item):
            # A separate connection sees the checkpoint before the engine can
            # mutate its input; a lost reply must not authorize a replay.
            checkpoint = self.service.store.guarded_dispatch_checkpoint(identifier)
            self.assertEqual((checkpoint['operation'], checkpoint['state']), ('upload', 'pending'))
            self.assertEqual(checkpoint['engine_uuid'], task_uuid)
            self.assertEqual(json.loads(checkpoint['evidence'])['sha256'], item['sha256'])
            return original(task_uuid, item)

        engine.upload = upload
        self.service.tick()
        self.assertEqual(self.service.store.guarded_dispatch_checkpoint(identifier)['operation'], 'commit')

    def test_guarded_decision_is_captured_once_for_checkpoint_and_persistence(self):
        class ChangingGuardEngine(FakeEngine):
            @property
            def node_guarded(self):
                self.guard_reads += 1
                return self.guard_reads == 1

        engine = ChangingGuardEngine('cluster', self.fleet)
        engine.guard_reads = 0
        self.service.engines['cluster'] = engine
        identifier = self.submit()
        self.service.tick()
        self.assertEqual(engine.guard_reads, 1)
        self.assertEqual(self.task(identifier)['node_guarded'], 1)
        checkpoint = self.service.store.guarded_dispatch_checkpoint(identifier)
        self.assertEqual((checkpoint['operation'], checkpoint['state']), ('commit', 'acknowledged'))

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
        self.assertEqual(self.service.info(identifier)['status']['code'], 10)
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
        self.assertEqual(self.service.info(identifier)['status']['code'], 10)
        self.assertIsNone(self.task(next_id)['node_id'])
        self.assertEqual(self.fleet.max_active, 1)

    def test_cancel_hold_crash_keeps_slot_and_restart_holds(self):
        identifier = self.submit()
        next_id = self.submit()
        self.service.tick()
        self.service.cancel(identifier)
        self.finish(identifier, 50)
        with patch.object(self.service.store, '_hold_in_transaction', side_effect=OSError('synthetic crash')):
            with self.assertRaises(OSError):
                self.service.tick()
        self.assertEqual(self.hold_state(identifier), ('running', 'cancel_pending'))  # Neither half landed.
        self.assertIsNone(self.task(next_id)['node_id'])
        self.service.close()
        self.queue.close()
        self.make_queue_service()
        self.service.tick()
        self.assertEqual(self.task(identifier)['job']['status'], 'held')
        self.assertEqual(self.service.info(identifier)['status']['code'], 10)
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

    def test_held_output_reads_only_bound_engine_without_resuming(self):
        identifier = self.submit()
        self.service.tick()
        task = self.task(identifier)
        self.service._hold(task, 'synthetic_uncertainty')
        before = {node: list(engine.calls) for node, engine in self.engines.items()}
        self.assertEqual(self.service.output(identifier, 1), ['', 'comma,record'])
        held = self.task(identifier)
        self.assertEqual(held['job']['status'], 'held')
        self.assertEqual((held['node_id'], held['engine_uuid']), (task['node_id'], task['engine_uuid']))
        for node, engine in self.engines.items():
            calls = engine.calls[len(before[node]):]
            if node == task['node_id']:
                self.assertEqual(len(calls), 1)
                self.assertEqual(calls[0][0], 'read')
                self.assertIn(task['engine_uuid'], calls[0][1])
            else:
                self.assertEqual(calls, [])
        self.engines[task['node_id']].output_unavailable = True
        self.service.store.update(identifier, output_lines=['retained output'])
        self.assertEqual(self.service.output(identifier, 0), ['retained output'])

    def running(self):
        self.service._node_lost_hold = True
        identifier = self.submit()
        self.service.tick()
        self.polls(1)
        self.assertEqual(self.service.info(identifier)['status']['code'], 20)
        return identifier

    def polls(self, count):
        for _ in range(count):
            self.service._backoff.clear()
            self.service._output_due.clear()
            self.service.tick()

    def assert_held_intact(self, identifier, before, reason):
        held = self.task(identifier)
        self.assertEqual((held['job']['status'], held['job']['heldReason'], held['stage'], held['phase'], held['error']),
                         ('held', 'operator', 'held', 'held', reason))
        self.assertEqual(held['job']['result']['note'], reason)
        self.assertEqual(held['job']['nodeId'], before['node_id'])
        for field in ('node_id', 'engine_uuid', 'claim_token', 'job_id', 'files', 'progress'):
            self.assertEqual(held[field], before[field])
        info = self.service.info(identifier)
        self.assertEqual((info['status'], info['ledgetopHeld'], info['ledgetopHoldReason'], info['ledgetopNodeId']),
                         ({'code': 10}, True, reason, before['node_id']))
        self.assertNotIn('ledgetopEngineStatus', info)
        # No cancel, delete, restart or second submission reached either node.
        mutations = [call for engine in self.engines.values() for call in engine.calls if call[0] != 'read']
        self.assertEqual(mutations.count(('commit', before['engine_uuid'])), 1)
        self.assertEqual(mutations.count(('initialize', before['engine_uuid'])), 1)
        self.assertFalse([call for call in mutations if call[0] not in ('initialize', 'upload', 'commit')])
        self.assertEqual(len(self.fleet.commits), 1)

    def test_network_failures_hold_node_lost_only_at_the_threshold(self):
        identifier = self.running()
        before = self.task(identifier)
        engine = self.engine_for(identifier)
        engine.read_unavailable = True
        self.polls(NODE_LOST_POLLS - 1)
        self.assertEqual(self.task(identifier)['job']['status'], 'running')
        info = self.service.info(identifier)
        self.assertEqual((info['status'], info['ledgetopEngineStatus']), ({'code': 20}, 'unknown'))
        self.polls(1)
        self.assert_held_intact(identifier, before, 'node_lost')
        self.assertIn(before['engine_uuid'], engine.tasks)

    def test_not_found_holds_engine_task_missing_and_keeps_the_slot(self):
        identifier = self.running()
        before = self.task(identifier)
        del self.engine_for(identifier).tasks[before['engine_uuid']]
        self.polls(ENGINE_MISSING_POLLS - 1)
        self.assertEqual(self.task(identifier)['job']['status'], 'running')
        self.polls(1)
        self.assert_held_intact(identifier, before, 'engine_task_missing')
        waiting = self.submit()
        self.polls(3)
        self.assertEqual(self.task(waiting)['job']['status'], 'queued')
        self.assertEqual(len(self.fleet.commits), 1)

    def test_native_read_separates_not_found_from_other_failures(self):
        replies = {'/task/ok/info': (200, {'uuid': 'ok'}), '/task/gone200/info': (200, {'error': 'abc not found'}),
                   '/task/gone404/info': (404, {'error': 'abc not found'}), '/task/route/info': (404, {'detail': 'no route'}),
                   '/task/denied/info': (200, {'error': 'Invalid authentication token'}),
                   '/task/broken/info': (500, {'error': 'abc not found'})}

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(self):
                code, value = replies[self.path]
                body = json.dumps(value).encode()
                self.send_response(code)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        client = NativeEngine('http://127.0.0.1:' + str(server.server_port))
        try:
            self.assertEqual(client.read('/task/ok/info'), {'uuid': 'ok'})
            for route in ('gone200', 'gone404'):
                with self.subTest(route=route), self.assertRaises(EngineTaskMissing):
                    client.read('/task/' + route + '/info')
            for route in ('route', 'denied', 'broken'):
                with self.subTest(route=route), self.assertRaises(EngineUnavailable) as raised:
                    client.read('/task/' + route + '/info')
                self.assertNotIsInstance(raised.exception, EngineTaskMissing)
            self.assertIsNone(client.read('/task/gone404/info', missing=True))
        finally:
            server.shutdown()
            server.server_close()

    def test_a_good_poll_resets_the_failure_count(self):
        identifier = self.running()
        engine = self.engine_for(identifier)
        for _ in range(3):
            engine.read_unavailable = True
            self.polls(NODE_LOST_POLLS - 1)
            engine.read_unavailable = False
            self.polls(1)
            self.assertNotIn('ledgetopEngineStatus', self.service.info(identifier))
        self.assertEqual(self.task(identifier)['job']['status'], 'running')

    def test_alternating_failures_hold_after_the_elapsed_bound(self):
        identifier = self.running()
        engine = self.engine_for(identifier)
        engine.read_unavailable = True
        self.polls(1)
        self.service._lost[identifier] = ('engine_task_missing', 1, time.monotonic() - ENGINE_LOST_SECONDS)
        self.polls(1)
        self.assertEqual(self.task(identifier)['error'], 'node_lost')

    def test_recovered_task_is_reported_to_the_operator_and_stays_held(self):
        identifier = self.running()
        before = self.task(identifier)
        engine = self.engine_for(identifier)
        engine.read_unavailable = True
        self.polls(NODE_LOST_POLLS)
        self.polls(1)
        self.assertEqual(self.service.info(identifier)['ledgetopEngineObservation'], {'state': 'unreachable'})
        engine.read_unavailable = False
        self.finish(identifier)
        self.service._held_observations.clear()
        self.polls(1)
        info = self.service.info(identifier)
        self.assertEqual(info['ledgetopEngineObservation'], {'state': 'task_present', 'code': 40})
        self.assert_held_intact(identifier, before, 'node_lost')
        reads = len(engine.calls)
        self.polls(3)
        self.assertEqual(len(engine.calls), reads)  # Observation is rate limited.
        del engine.tasks[before['engine_uuid']]
        self.service._held_observations.clear()
        self.polls(1)
        self.assertEqual(self.service.info(identifier)['ledgetopEngineObservation'], {'state': 'task_missing'})
        resolved = self.queue.operator(before['job_id'], 'resolve', {'note': 'synthetic', 'workerStopped': True})
        self.assertEqual(resolved['status'], 'failed')
        self.polls(1)
        self.assertEqual(self.service.info(identifier)['status'], {'code': 30, 'errorMessage': 'node_lost'})

    def test_coordinator_restart_keeps_a_lost_hold(self):
        identifier = self.running()
        before = self.task(identifier)
        engine = self.engine_for(identifier)
        engine.read_unavailable = True
        self.polls(NODE_LOST_POLLS)
        self.service.close()
        self.queue.close()
        engine.read_unavailable = False
        self.config.viewer_node_lost_hold = True
        self.make_queue_service()
        self.polls(2)
        self.assert_held_intact(identifier, before, 'node_lost')
        self.assertEqual(self.service.info(identifier)['ledgetopEngineObservation'], {'state': 'task_present', 'code': 20})

    def test_node_lost_hold_off_keeps_polling_a_silent_node(self):
        identifier = self.submit()
        self.service.tick()
        self.polls(1)
        engine = self.engine_for(identifier)
        engine.read_unavailable = True
        self.polls(NODE_LOST_POLLS * 3)
        self.assertEqual(self.task(identifier)['job']['status'], 'running')
        self.assertEqual(self.task(identifier)['stage'], 'running')
        info = self.service.info(identifier)
        self.assertEqual(info['status'], {'code': 20})
        self.assertFalse({'ledgetopHeld', 'ledgetopEngineStatus', 'ledgetopHoldReason'} & set(info))
        engine.read_unavailable = False
        self.finish(identifier)
        self.polls(1)
        self.assertEqual(self.task(identifier)['job']['status'], 'succeeded')

    def retaining(self, console=None):
        self.service._log_retention = True
        identifier = self.submit()
        self.service.tick()
        task = self.task(identifier)
        if console is not None:
            self.engines[task['node_id']].tasks[task['engine_uuid']]['console'] = console
        self.polls(1)
        return identifier, self.engines[task['node_id']].tasks[task['engine_uuid']]

    def retained_file(self, identifier):
        return Path(self.task(identifier)['directory']) / OUTPUT_LOG_NAME

    def test_output_failure_is_explicit_and_keeps_retained_lines(self):
        identifier, _ = self.retaining(['one', 'two'])
        self.assertEqual(self.service.output(identifier, 0), ['one', 'two'])
        self.engine_for(identifier).output_unavailable = True
        self.polls(1)
        self.assertEqual(self.service.output(identifier, 1), {
            'availability': 'unavailable', 'reason': 'engine_output_unavailable',
            'error': 'engine_output_unavailable', 'lines': ['two']})
        self.assertEqual(self.task(identifier)['job']['status'], 'running')
        self.engine_for(identifier).output_unavailable = False
        self.polls(1)
        self.assertEqual(self.service.output(identifier, 0), ['one', 'two'])

    def test_output_failure_before_any_capture_is_not_an_empty_log(self):
        self.service._log_retention = True
        identifier = self.submit()
        self.service.tick()
        self.engine_for(identifier).output_unavailable = True
        self.assertEqual(self.service.output(identifier, 0)['availability'], 'unavailable')
        self.assertEqual(self.service.output(identifier, 0)['lines'], [])

    def test_lost_node_output_names_the_hold(self):
        self.service._node_lost_hold = True
        identifier, _ = self.retaining(['one'])
        self.engine_for(identifier).read_unavailable = True
        self.polls(NODE_LOST_POLLS)
        result = self.service.output(identifier, 0)
        self.assertEqual((result['reason'], result['lines']), ('node_lost', ['one']))
        self.engine_for(identifier).read_unavailable = False
        self.polls(1)
        self.assertEqual(self.service.output(identifier, 0), ['one'])
        self.assertEqual(self.task(identifier)['job']['status'], 'held')

    def test_node_restart_marks_the_gap_and_keeps_cursors_valid(self):
        for changed in (['new'], ['new', 'lines', 'are', 'longer']):
            with self.subTest(changed=changed):
                identifier, engine_task = self.retaining(['one', 'two', 'three'])
                engine_task['console'] = list(changed)
                self.polls(2)
                lines = self.service.output(identifier, 0)
                self.assertEqual(lines[:3], ['one', 'two', 'three'])
                self.assertTrue(lines[3].startswith(MARKER + 'engine output restarted'))
                self.assertEqual(lines[4:], changed)
                engine_task['console'].append('next')
                self.polls(1)
                self.assertEqual(self.service.output(identifier, len(lines)), ['next'])
                self.finish(identifier)
                self.polls(1)

    def test_retained_lines_survive_coordinator_restart_without_duplicates(self):
        identifier, engine_task = self.retaining(['one', 'two'])
        self.service.close()
        self.queue.close()
        self.make_queue_service()
        self.service._log_retention = True
        engine_task['console'].append('three')
        self.polls(2)
        self.assertEqual(self.service.output(identifier, 0), ['one', 'two', 'three'])

    def test_terminal_snapshot_outlives_engine_cleanup_and_restart(self):
        identifier, engine_task = self.retaining(['one'])
        engine_task['console'] = ['one', 'two', 'last words']
        self.finish(identifier)
        self.polls(1)
        self.assertEqual(self.task(identifier)['job']['status'], 'succeeded')
        engine = self.engine_for(identifier)
        del engine.tasks[self.task(identifier)['engine_uuid']]
        engine.read_unavailable = True
        reads = len(engine.calls)
        self.assertEqual(self.service.output(identifier, 1), ['two', 'last words'])
        self.assertEqual(len(engine.calls), reads)
        self.service.close()
        self.queue.close()
        self.make_queue_service()
        self.service._log_retention = True
        self.assertEqual(self.service.output(identifier, 0), ['one', 'two', 'last words'])

    def test_failed_terminal_snapshot_is_explicit_and_never_blocks_completion(self):
        identifier, _ = self.retaining(['one'])
        self.engine_for(identifier).output_unavailable = True
        self.finish(identifier)
        self.polls(1)
        self.assertEqual(self.task(identifier)['job']['status'], 'succeeded')
        result = self.service.output(identifier, 0)
        self.assertEqual(result['reason'], 'output_snapshot_incomplete')
        self.assertEqual(result['lines'][0], 'one')
        self.assertTrue(result['lines'][1].startswith(MARKER))

    def test_retained_log_is_bounded_and_truncation_is_marked(self):
        with patch('processing.viewer_output_log.OUTPUT_HEAD_LINES', 3), \
                patch('processing.viewer_service.OUTPUT_TAIL_LINES', 2):
            identifier, engine_task = self.retaining(['x' * (OUTPUT_LINE_CHARS + 50)] + ['line %d' % i for i in range(20)])
            lines = self.service.output(identifier, 0)
            self.assertEqual(len(lines[0]), OUTPUT_LINE_CHARS)
            self.assertTrue(lines[0].endswith(CLIPPED))
            self.assertEqual(lines[1:3], ['line 0', 'line 1'])
            self.assertEqual(lines[3], MARKER + 'output limit reached; later lines are omitted')
            self.assertEqual(len(lines), 4)
            engine_task['console'] += ['failure detail', 'final line']
            self.finish(identifier)
            self.polls(1)
            final = self.service.output(identifier, 0)
            self.assertEqual(final[:4], lines)  # Retained indices never move.
            self.assertEqual(final[4], MARKER + '18 engine lines omitted; final lines follow')
            self.assertEqual(final[5:], ['failure detail', 'final line'])

    def test_retained_log_stays_inside_its_total_size(self):
        identifier, engine_task = self.retaining(['\u00e9' * OUTPUT_LINE_CHARS] * 1000)
        self.finish(identifier)
        self.polls(1)
        self.assertLessEqual(len(json.dumps(self.service.output(identifier, 0))), OUTPUT_TOTAL_BYTES)
        self.assertLess(self.retained_file(identifier).stat().st_size, OUTPUT_TOTAL_BYTES + 4096)

    def test_backlog_over_the_read_limit_advances_and_never_stalls(self):
        identifier, engine_task = self.retaining(['one'])
        engine = self.engine_for(identifier)
        console = engine_task['console'] = ['one', 'two', 'three', 'four', 'huge', 'five']

        def read_output(route, line):
            # Two lines fit the synthetic limit and 'huge' never does.
            lines = console[line:]
            kept = lines[:lines.index('huge')] if 'huge' in lines else lines
            return PartialLines(kept[:2]) if len(kept) > 2 or 'huge' in lines else kept

        engine.read_output = read_output
        try:
            self.polls(6)
        finally:
            del engine.read_output
        lines = self.service.output(identifier, 0)
        self.assertEqual(lines, ['one', 'two', 'three', 'four', MARKER + 'engine line over the read limit omitted', 'five'])

    def test_native_output_prefix_keeps_only_complete_lines(self):
        lines = ['one', 'caf\u00e9', 'a,"b"', 'tail ' * 50]
        raw = json.dumps(lines, ensure_ascii=False).encode()

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(self):
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        client = NativeEngine('http://127.0.0.1:' + str(server.server_port))
        try:
            partial = client.read('/task/x/output', limit=40, prefix=True, line=0)
            self.assertIsInstance(partial, PartialLines)
            self.assertEqual(partial, lines[:3])
            self.assertEqual(client.read_output_prefix('/task/x/output', 40), lines[:3])
            self.assertNotIsInstance(client.read('/task/x/output', prefix=True, line=0), PartialLines)
            with self.assertRaises(EngineUnavailable):
                client.read('/task/x/output', limit=40, line=0)
        finally:
            server.shutdown()
            server.server_close()

    def test_invalid_retained_file_is_reported_and_not_overwritten(self):
        identifier, _ = self.retaining(['one'])
        self.retained_file(identifier).write_text('{}')
        self.polls(1)
        self.assertEqual(self.retained_file(identifier).read_text(), '{}')
        self.assertEqual(self.task(identifier)['job']['status'], 'running')
        self.assertEqual(self.service.output(identifier, 0)['reason'], 'retained_output_invalid')

    def test_retained_writes_never_follow_a_preplaced_link(self):
        identifier, engine_task = self.retaining(['one'])
        path = self.retained_file(identifier)
        victim = self.root / 'unrelated-file'
        victim.write_text('unrelated bytes')
        old_temporary = path.with_name(OUTPUT_LOG_NAME + '.tmp')
        for place in (lambda: os.link(victim, old_temporary), lambda: old_temporary.symlink_to(victim)):
            place()
            engine_task['console'].append('next')
            self.polls(1)
            self.assertEqual(victim.read_text(), 'unrelated bytes')
            self.assertEqual(self.service.output(identifier, 0), engine_task['console'])
            self.assertEqual(path.stat().st_nlink, 1)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            old_temporary.unlink()
        self.assertEqual(sorted(item.name for item in path.parent.iterdir() if item.name.startswith(OUTPUT_LOG_NAME)),
                         [OUTPUT_LOG_NAME])

    def test_retained_file_identity_is_checked_before_read_and_replace(self):
        for replace in (lambda path, other: os.link(path, other),
                        lambda path, other: (other.write_bytes(path.read_bytes()), path.unlink(), path.symlink_to(other))):
            identifier, engine_task = self.retaining(['one'])
            path = self.retained_file(identifier)
            other = self.root / ('other-' + identifier)
            replace(path, other)
            before = other.read_bytes()
            engine_task['console'].append('next')
            self.polls(1)
            self.assertEqual(other.read_bytes(), before)
            self.assertEqual(self.service.output(identifier, 0)['reason'], 'retained_output_invalid')
            with self.assertRaises(Conflict):
                self.service.store.save_output_log(self.task(identifier), {})
            self.assertEqual(self.task(identifier)['job']['status'], 'running')
            self.finish(identifier)
            self.polls(1)

    def test_failed_retained_write_leaves_no_temporary_and_keeps_the_log(self):
        identifier, engine_task = self.retaining(['one'])
        path = self.retained_file(identifier)
        engine_task['console'].append('next')
        with patch('processing.viewer_tasks.os.replace', side_effect=OSError('synthetic')):
            self.polls(1)
        self.assertEqual(self.service.output(identifier, 0)['reason'], 'output_retention_failed')
        self.assertEqual(self.service.output(identifier, 0)['lines'], ['one'])
        self.assertEqual([item.name for item in path.parent.iterdir() if '.tmp-' in item.name], [])
        self.polls(1)
        self.assertEqual(self.service.output(identifier, 0), ['one', 'next'])

    def anchor_due(self):
        self.service._output_anchor_due.clear()

    def restarted_at(self, lines):
        return [index for index, line in enumerate(lines) if line.startswith(MARKER + 'engine output restarted')]

    def test_repeated_line_at_the_same_index_does_not_hide_a_restart(self):
        # Codex's reproduction: the reread line matches, 'boot' was never read.
        identifier, engine_task = self.retaining(['old run', 'same progress'])
        engine_task['console'] = ['boot', 'same progress', 'new run']
        self.anchor_due()
        self.polls(1)
        lines = self.service.output(identifier, 0)
        self.assertEqual(lines[:2], ['old run', 'same progress'])
        self.assertIn('lines may be missing', lines[2])
        self.assertEqual(lines[3:], ['boot', 'same progress', 'new run'])
        self.assertEqual(self.service.info(identifier)['ledgetopOutputStream']['generation'], 2)
        engine_task['console'].append('later')
        self.polls(1)
        self.assertEqual(self.service.output(identifier, len(lines)), ['later'])

    def test_restart_between_anchor_reads_is_marked_when_found(self):
        identifier, engine_task = self.retaining(['old run', 'same progress'])
        self.anchor_due()
        self.polls(1)
        engine_task['console'] = ['boot', 'same progress', 'new run']
        self.polls(1)  # The anchor is not due: the line digest alone is fooled.
        self.assertEqual(self.service.output(identifier, 0), ['old run', 'same progress', 'new run'])
        self.anchor_due()
        self.polls(1)
        lines = self.service.output(identifier, 0)
        self.assertIn('lines from index 2 to here are unproven', lines[3])
        self.assertEqual(lines[4:], ['boot', 'same progress', 'new run'])

    def test_terminal_snapshot_always_proves_the_anchor(self):
        identifier, engine_task = self.retaining(['old run', 'same progress'])
        self.anchor_due()
        self.polls(1)
        engine_task['console'] = ['boot', 'same progress', 'new run']
        self.finish(identifier)
        self.polls(1)
        lines = self.service.output(identifier, 0)
        self.assertEqual(lines[3:], ['boot', 'same progress', 'new run'])
        self.assertEqual(len(self.restarted_at(lines)), 1)

    def test_status_regression_resets_without_waiting_for_the_anchor(self):
        for name, first, second in (('time', {'processingTime': 9000}, {'processingTime': 40}),
                                    ('progress', {'progress': 60}, {'progress': 12}),
                                    ('queued', {}, {'status': {'code': 10}})):
            with self.subTest(name=name):
                identifier, engine_task = self.retaining(['same run'])
                engine_task.update(first)
                self.anchor_due()
                self.polls(1)
                self.polls(1)
                self.assertEqual(self.service.output(identifier, 0), ['same run'])
                engine_task.update(second)
                self.polls(1)
                lines = self.service.output(identifier, 0)
                self.assertEqual(len(self.restarted_at(lines)), 1)
                self.assertEqual(lines[-1], 'same run')  # A false marker, by choice.
                self.finish(identifier)
                self.polls(1)

    def test_good_anchor_adds_nothing_and_keeps_the_generation(self):
        identifier, engine_task = self.retaining(['one', 'two'])
        stream = self.service.info(identifier)['ledgetopOutputStream']
        for line in ('three', 'four'):
            engine_task['console'].append(line)
            self.anchor_due()
            self.polls(1)
        self.assertEqual(self.service.output(identifier, 0), ['one', 'two', 'three', 'four'])
        self.assertEqual(self.service.info(identifier)['ledgetopOutputStream'], stream)
        self.assertEqual(stream['continuity'], 'anchored')

    def test_anchor_read_is_bounded_and_periodic(self):
        identifier, engine_task = self.retaining(['one'])
        engine = self.engine_for(identifier)
        limits = []
        engine.read_output_prefix = lambda route, limit: limits.append(limit) or engine_task['console']
        self.polls(5)
        self.assertEqual(limits, [OUTPUT_ANCHOR_BYTES])
        self.anchor_due()
        self.polls(1)
        self.assertEqual(limits, [OUTPUT_ANCHOR_BYTES] * 2)

    def test_anchor_covers_only_the_leading_lines(self):
        console = ['line %d' % index for index in range(OUTPUT_ANCHOR_LINES + 3)]
        identifier, engine_task = self.retaining(list(console))
        engine_task['console'][OUTPUT_ANCHOR_LINES] = 'changed past the anchor'
        self.anchor_due()
        self.polls(1)
        self.assertEqual(self.service.output(identifier, 0), console)
        engine_task['console'][OUTPUT_ANCHOR_LINES - 1] = 'changed inside the anchor'
        self.anchor_due()
        self.polls(1)
        self.assertEqual(self.restarted_at(self.service.output(identifier, 0)), [len(console)])

    def test_failed_anchor_read_consumes_nothing_and_reports(self):
        identifier, engine_task = self.retaining(['one'])
        engine = self.engine_for(identifier)
        engine_task['console'] = ['one', 'unproven']

        def fail(route, limit):
            raise EngineUnavailable('synthetic')

        engine.read_output_prefix = fail
        self.polls(1)
        result = self.service.output(identifier, 0)
        self.assertEqual((result['reason'], result['lines']), ('engine_output_unavailable', ['one']))
        del engine.read_output_prefix
        self.polls(1)
        self.assertEqual(self.service.output(identifier, 0), ['one', 'unproven'])

    def test_output_without_a_provable_anchor_says_so_once(self):
        identifier, engine_task = self.retaining(['one'])
        engine = self.engine_for(identifier)
        engine.node_guarded = True
        engine_task['console'].append('two')
        self.polls(3)
        engine_task['console'].append('three')
        self.polls(1)
        lines = self.service.output(identifier, 0)
        self.assertEqual(lines[0], 'one')
        self.assertEqual(lines[1], MARKER + 'output continuity cannot be verified; lines may be missing')
        self.assertEqual(lines[2:], ['two', 'three'])
        self.assertEqual(self.service.info(identifier)['ledgetopOutputStream']['continuity'], 'unverified')
        engine.node_guarded = False
        self.finish(identifier)
        self.polls(1)

    def test_first_line_too_long_to_anchor_is_marked_not_reset_forever(self):
        identifier, engine_task = self.retaining(['x' * OUTPUT_ANCHOR_BYTES, 'two'])
        self.anchor_due()
        self.polls(3)
        lines = self.service.output(identifier, 0)
        self.assertEqual(lines[0], MARKER + 'output continuity cannot be verified; lines may be missing')
        self.assertEqual(len(lines), 3)
        self.assertEqual(self.service.info(identifier)['ledgetopOutputStream']['generation'], 1)

    def test_version_one_log_resets_its_cursor_at_the_cutover(self):
        identifier, engine_task = self.retaining(['one', 'two'])
        path = self.retained_file(identifier)
        log = json.loads(path.read_text())
        path.write_text(json.dumps({key: log[key] for key in ('engine', 'cursor', 'last', 'omitted', 'omittedFrom',
                                                               'snapshot', 'lines')} | {'version': 1}))
        self.service._output_streams.clear()
        first = self.service.info(identifier)['ledgetopOutputStream']
        self.assertEqual(self.service.info(identifier)['ledgetopOutputStream'], first)
        self.assertEqual(first['continuity'], 'unverified')
        self.assertEqual(self.service.output(identifier, 0), ['one', 'two'])
        self.polls(1)
        lines = self.service.output(identifier, 0)
        self.assertEqual(lines[:2], ['one', 'two'])
        self.assertEqual(self.restarted_at(lines), [2])
        self.assertEqual(lines[3:], ['one', 'two'])
        stream = self.service.info(identifier)['ledgetopOutputStream']
        self.assertEqual((stream['id'], stream['generation'], stream['continuity']), (first['id'], 2, 'anchored'))

    def test_stream_survives_restart_and_is_absent_until_retained(self):
        self.service._log_retention = True
        identifier = self.submit()
        self.assertNotIn('ledgetopOutputStream', self.service.info(identifier))
        self.service.tick()
        self.engine_for(identifier).tasks[self.task(identifier)['engine_uuid']]['console'] = ['one']
        self.polls(1)
        stream = self.service.info(identifier)['ledgetopOutputStream']
        self.assertRegex(stream['id'], r'\A[0-9a-f]{32}\Z')
        self.assertEqual(set(stream), {'id', 'generation', 'continuity'})
        self.service.close()
        self.queue.close()
        self.make_queue_service()
        self.service._log_retention = True
        self.assertEqual(self.service.info(identifier)['ledgetopOutputStream'], stream)

    def expire_snapshot_retry(self, identifier):
        self.service._output_retries[identifier] = 0

    def test_terminal_snapshot_retries_and_completes(self):
        identifier, engine_task = self.retaining(['one'])
        engine = self.engine_for(identifier)
        engine.output_unavailable = True
        engine_task['console'] = ['one', 'last words']
        self.finish(identifier)
        self.polls(1)
        self.assertEqual(self.service.output(identifier, 0)['reason'], 'output_snapshot_incomplete')
        reads = len(engine.calls)
        self.polls(3)
        self.assertEqual(len(engine.calls), reads)  # The retry waits its turn.
        engine.output_unavailable = False
        self.expire_snapshot_retry(identifier)
        self.polls(1)
        lines = self.service.output(identifier, 0)
        self.assertEqual((lines[0], lines[-1]), ('one', 'last words'))
        self.assertEqual(self.service._output_retries, {})

    def test_terminal_snapshot_retries_are_bounded_then_marked(self):
        identifier, _ = self.retaining(['one'])
        engine = self.engine_for(identifier)
        engine.output_unavailable = True
        self.finish(identifier)
        self.polls(1)
        for _ in range(OUTPUT_SNAPSHOT_ATTEMPTS - 1):
            self.expire_snapshot_retry(identifier)
            self.polls(1)
        result = self.service.output(identifier, 0)
        self.assertEqual(result['reason'], 'output_snapshot_incomplete')
        self.assertIn('lines may be missing', result['lines'][-1])
        self.assertEqual(self.service._output_retries, {})
        reads = len(engine.calls)
        self.polls(3)
        self.assertEqual(len(engine.calls), reads)

    def test_terminal_snapshot_retry_survives_a_coordinator_restart(self):
        identifier, engine_task = self.retaining(['one'])
        engine = self.engine_for(identifier)
        engine.output_unavailable = True
        engine_task['console'] = ['one', 'last words']
        self.finish(identifier)
        self.polls(1)
        self.service.close()
        self.queue.close()
        engine.output_unavailable = False
        self.make_queue_service()
        self.service._log_retention = True
        self.polls(1)
        self.assertEqual(self.service.output(identifier, 0)[-1], 'last words')

    def console_archive(self, text):
        stream = io.BytesIO()
        with zipfile.ZipFile(stream, 'w') as archive:
            archive.writestr('synthetic-result.txt', b'owned synthetic transfer fixture')
            archive.writestr('task_output.txt', text)
        return stream.getvalue()

    def test_verified_archive_completes_an_incomplete_snapshot(self):
        for console in (['one', 'two', 'three'], ['boot', 'two', 'three']):
            with self.subTest(console=console):
                identifier, engine_task = self.retaining(['one', 'two'])
                engine = self.engine_for(identifier)
                engine.archive = self.console_archive('\n'.join(console))
                engine.output_unavailable = True
                self.finish(identifier)
                self.polls(1)
                self.assertEqual(self.service.output(identifier, 0)['reason'], 'output_snapshot_incomplete')
                stream, length = self.service.download(identifier)
                with stream:
                    self.assertEqual(stream.read(), engine.archive)
                lines = self.service.output(identifier, 0)
                self.assertIsInstance(lines, list)
                self.assertEqual(lines[-1], 'three')
                self.assertEqual(lines[0:2], ['one', 'two'])
                self.assertEqual(len(self.restarted_at(lines)), 1 if console[0] == 'boot' else 0)
                self.assertEqual(self.service._output_retries, {})
                engine.output_unavailable = False

    def test_archive_without_a_safe_console_leaves_the_snapshot_incomplete(self):
        identifier, _ = self.retaining(['one'])
        engine = self.engine_for(identifier)
        engine.output_unavailable = True
        self.finish(identifier)
        self.polls(1)
        stream, _ = self.service.download(identifier)
        with stream:
            self.assertEqual(stream.read(), engine.archive)
        self.assertEqual(self.service.output(identifier, 0)['reason'], 'output_snapshot_incomplete')

    def test_archived_console_read_is_bounded(self):
        with patch('processing.viewer_output_log.ARCHIVED_OUTPUT_MAX_BYTES', 8):
            self.assertIsNone(archived_output_lines(io.BytesIO(self.console_archive('x' * 64))))
            self.assertEqual(archived_output_lines(io.BytesIO(self.console_archive('a\nb\n'))), ['a', 'b'])
        forged = bytearray(self.console_archive('y' * 2048))
        size = (2048).to_bytes(4, 'little')
        for index in range(len(forged) - 3):
            if forged[index:index + 4] == size:
                forged[index:index + 4] = (3).to_bytes(4, 'little')
        self.assertIsNone(archived_output_lines(io.BytesIO(bytes(forged))))
        self.assertIsNone(archived_output_lines(io.BytesIO(b'not a zip')))
        self.assertGreater(ARCHIVED_OUTPUT_MAX_BYTES, OUTPUT_TOTAL_BYTES)

    OPERATOR = {'note': 'synthetic inspection', 'workerStopped': True}

    def adoptable(self, *, bind=True):
        """A running task, optionally bound while running, then lost."""
        self.service._node_lost_hold = True
        self.service._adopt_lost_result = True
        identifier = self.submit()
        self.service.tick()
        task = self.task(identifier)
        engine_task = self.engines[task['node_id']].tasks[task['engine_uuid']]
        engine_task['identity'] = {'dateCreated': 1700000000000, 'imagesCount': 1, 'processingTime': -1,
                                   'options': [{'name': 'pc-ept', 'value': True}]}
        if not bind:
            self.service._adopt_lost_result = False
        self.polls(1)
        self.service._adopt_lost_result = True
        engine = self.engine_for(identifier)
        engine.read_unavailable = True
        self.polls(NODE_LOST_POLLS)
        engine.read_unavailable = False
        self.assertEqual(self.task(identifier)['job']['status'], 'held')
        return identifier, engine, engine_task

    def report_finished(self, identifier, engine_task):
        self.finish(identifier)
        engine_task['identity']['processingTime'] = 5000

    def observe(self):
        """One held observation, and any fetch it started, to the end."""
        self.service._held_observations.clear()
        self.service._adopt_due.clear()
        self.polls(1)
        for thread in list(self.service._adopt_threads.values()):
            thread.join(10)

    def custody(self, identifier):
        return self.service.info(identifier).get('ledgetopResultCustody')

    def adoption_file(self, identifier):
        return Path(self.task(identifier)['directory']) / ADOPTION_NAME

    def archive_files(self, identifier):
        directory = Path(self.task(identifier)['directory'])
        return {name: (directory / name).exists() for name in ('all.zip', 'all.zip.receipt', 'all.zip.partial')}

    def assert_no_custody_files(self, identifier):
        files = self.archive_files(identifier)
        self.assertFalse(files['all.zip'] or files['all.zip.receipt'], files)

    def assert_still_held(self, identifier):
        task = self.task(identifier)
        self.assertEqual((task['job']['status'], task['phase'], task['stage']), ('held', 'held', 'held'))
        self.assertEqual(self.service.info(identifier)['status'], {'code': 10})

    def test_held_result_is_fetched_verified_adopted_and_finishes_normally(self):
        identifier, engine, engine_task = self.adoptable()
        before = self.task(identifier)
        self.observe()
        self.assertEqual(engine.download_calls, 0)  # Still running: nothing to fetch.
        self.report_finished(identifier, engine_task)
        self.observe()
        self.assertEqual(self.custody(identifier), {'state': 'verified'})
        self.assert_still_held(identifier)
        self.assertEqual(self.service.info(identifier)['ledgetopHoldReason'], 'node_lost')
        self.assertEqual(Path(before['directory'], 'all.zip').read_bytes(), engine.archive)
        with self.assertRaises(Conflict):
            self.service.download(identifier)  # Custody is not yet a result.
        job = self.service.operator(before['job_id'], 'adopt', self.OPERATOR)
        self.assertEqual((job['status'], job['heldReason']), ('succeeded', None))
        self.assertEqual(job['result'], {'engineTaskId': before['engine_uuid'], 'nodeId': before['node_id'],
                                         'environment': 'production', 'cancelled': False})
        task = self.task(identifier)
        self.assertEqual((task['stage'], task['phase'], task['error'], task['progress']),
                         ('succeeded', 'complete', None, 100))
        info = self.service.info(identifier)
        self.assertEqual((info['status'], info['progress']), ({'code': 40}, 100))
        self.assertFalse({'ledgetopHeld', 'ledgetopHoldReason', 'ledgetopResultCustody'} & set(info))
        stream, length = self.service.download(identifier)
        with stream:
            self.assertEqual(stream.read(), engine.archive)
            self.assertEqual(stream.ledgetop_archive_sha256, hashlib.sha256(engine.archive).hexdigest())
        self.assertEqual(length, len(engine.archive))
        self.assertEqual(engine.download_calls, 1)  # The Viewer got the custody copy.
        record = json.loads(self.adoption_file(identifier).read_text())
        self.assertEqual(record['adoption'], {'note': 'synthetic inspection'})
        self.assertEqual(self.service.remove(identifier)['success'], True)
        self.assertIn(before['engine_uuid'], engine.tasks)  # Removal never reaches the engine.
        self.assertEqual({call[0] for call in engine.calls} - {'read', 'download'}, {'initialize', 'upload', 'commit'})
        waiting = self.submit()
        self.polls(1)
        self.assertEqual(self.task(waiting)['job']['status'], 'running')  # The slot is released.

    def test_engine_task_missing_hold_adopts_the_same_way(self):
        self.service._node_lost_hold = True
        self.service._adopt_lost_result = True
        identifier = self.submit()
        self.service.tick()
        task = self.task(identifier)
        engine = self.engines[task['node_id']]
        engine_task = engine.tasks[task['engine_uuid']]
        engine_task['identity'] = {'dateCreated': 1700000000000, 'imagesCount': 1, 'options': REQUEST_OPTIONS}
        self.polls(1)
        del engine.tasks[task['engine_uuid']]
        self.polls(ENGINE_MISSING_POLLS)
        self.assertEqual(self.task(identifier)['error'], 'engine_task_missing')
        engine.tasks[task['engine_uuid']] = engine_task
        self.finish(identifier)
        self.observe()
        self.assertEqual(self.custody(identifier), {'state': 'verified'})
        self.assertEqual(self.service.operator(task['job_id'], 'adopt', self.OPERATOR)['status'], 'succeeded')

    def test_identity_mismatch_is_refused_latched_and_never_fetched(self):
        for name, change in (('date', {'dateCreated': 1700000000001}), ('images', {'imagesCount': 2}),
                             ('options', {'options': [{'name': 'pc-ept', 'value': False}]})):
            with self.subTest(name=name):
                identifier, engine, engine_task = self.adoptable()
                self.report_finished(identifier, engine_task)
                engine_task['identity'].update(change)
                self.observe()
                self.assertEqual(self.custody(identifier), {'state': 'refused', 'reason': 'engine_identity_mismatch'})
                self.assertEqual(engine.download_calls, 0)
                self.assert_no_custody_files(identifier)
                engine_task['identity'] = {'dateCreated': 1700000000000, 'imagesCount': 1, 'processingTime': 5000,
                                           'options': [{'name': 'pc-ept', 'value': True}]}
                self.observe()
                self.assertEqual(engine.download_calls, 0)  # A refusal does not clear itself.
                with self.assertRaisesRegex(Conflict, 'custody'):
                    self.service.operator(self.task(identifier)['job_id'], 'adopt', self.OPERATOR)
                self.assert_still_held(identifier)
                self.queue.operator(self.task(identifier)['job_id'], 'resolve', self.OPERATOR)

    def test_without_a_recorded_identity_the_task_files_bind_it(self):
        identifier, engine, engine_task = self.adoptable(bind=False)
        self.report_finished(identifier, engine_task)
        engine_task['identity']['imagesCount'] = 7
        self.observe()
        self.assertEqual(self.custody(identifier)['state'], 'refused')
        self.queue.operator(self.task(identifier)['job_id'], 'resolve', self.OPERATOR)
        identifier, engine, engine_task = self.adoptable(bind=False)
        self.report_finished(identifier, engine_task)
        self.observe()
        self.assertEqual(self.custody(identifier), {'state': 'verified'})

    def test_unreported_identity_cannot_be_shown_and_is_refused(self):
        identifier, engine, engine_task = self.adoptable(bind=False)
        self.report_finished(identifier, engine_task)
        del engine_task['identity']['imagesCount']
        self.observe()
        self.assertEqual(self.custody(identifier)['reason'], 'engine_identity_mismatch')
        self.assertEqual(engine.download_calls, 0)

    def test_requested_options_are_compared_when_nothing_was_recorded(self):
        identifier = self.submit()
        self.service._node_lost_hold = True
        self.service._adopt_lost_result = True
        self.service.tick()
        task = self.task(identifier)
        engine = self.engines[task['node_id']]
        engine.tasks[task['engine_uuid']]['identity'] = {'imagesCount': 1, 'options': [{'name': 'pc-ept', 'value': 'false'}]}
        self.service._hold(task, 'node_lost')
        self.finish(identifier)
        self.observe()
        self.assertEqual(self.custody(identifier)['reason'], 'engine_identity_mismatch')

    def test_corrupt_archive_is_refused_and_retried(self):
        identifier, engine, engine_task = self.adoptable()
        self.report_finished(identifier, engine_task)
        good = engine.archive
        flipped = bytearray(good)
        flipped[good.index(b'owned synthetic')] ^= 1  # Still a ZIP; its CRC now fails.
        for corrupt in (b'not a zip archive', bytes(flipped)):
            engine.archive = corrupt
            self.observe()
            self.assertEqual(self.custody(identifier), {'state': 'fetching', 'reason': 'archive_fetch_failed'})
            self.assert_no_custody_files(identifier)
            self.assert_still_held(identifier)
        engine.archive = good
        self.observe()
        self.assertEqual(self.custody(identifier), {'state': 'verified'})
        self.assertEqual(Path(self.task(identifier)['directory'], 'all.zip').read_bytes(), good)
        self.assertFalse(self.archive_files(identifier)['all.zip.partial'])

    def test_failed_fetch_waits_before_retrying(self):
        identifier, engine, engine_task = self.adoptable()
        self.report_finished(identifier, engine_task)
        engine.interrupt_download_once = True
        self.observe()
        self.assertEqual(engine.download_calls, 1)
        self.service._held_observations.clear()
        self.polls(1)
        self.assertEqual(engine.download_calls, 1)
        self.assertGreater(self.service._adopt_due[identifier], time.monotonic())

    def test_interrupted_fetch_retries_without_a_receipt(self):
        identifier, engine, engine_task = self.adoptable()
        self.report_finished(identifier, engine_task)
        engine.interrupt_download_once = True
        self.observe()
        files = self.archive_files(identifier)
        self.assertEqual(files, {'all.zip': False, 'all.zip.receipt': False, 'all.zip.partial': True})
        self.observe()
        self.assertEqual(self.custody(identifier), {'state': 'verified'})
        self.assertEqual(engine.download_calls, 2)

    def test_restart_during_fetch_resumes_from_the_durable_state(self):
        identifier, engine, engine_task = self.adoptable()
        self.report_finished(identifier, engine_task)
        engine.interrupt_download_once = True
        self.observe()
        self.service.close()
        self.queue.close()
        self.make_queue_service()
        self.service._node_lost_hold = True
        self.service._adopt_lost_result = True
        self.assertEqual(self.custody(identifier), {'state': 'fetching'})
        self.observe()
        self.assertEqual(self.custody(identifier), {'state': 'verified'})

    def test_restart_after_the_receipt_keeps_the_verified_bytes(self):
        identifier, engine, engine_task = self.adoptable()
        self.report_finished(identifier, engine_task)
        original = TaskStore.save_adoption
        calls = []

        def crash_on_verified(task, record):
            if record['state'] == 'verified' and not calls:
                calls.append(record)
                raise OSError('synthetic crash before the record')
            original(task, record)

        with patch.object(TaskStore, 'save_adoption', staticmethod(crash_on_verified)):
            self.observe()
        self.assertEqual(self.custody(identifier)['state'], 'fetching')
        self.assertTrue(self.archive_files(identifier)['all.zip.receipt'])
        replaced = engine.archive
        engine.archive = self.console_archive('a different archive')
        self.observe()
        self.assertEqual(self.custody(identifier), {'state': 'verified'})
        self.assertEqual(engine.download_calls, 1)  # A verified archive is never replaced.
        self.assertEqual(Path(self.task(identifier)['directory'], 'all.zip').read_bytes(), replaced)

    def test_task_vanishing_mid_fetch_stays_held_without_a_receipt(self):
        identifier, engine, engine_task = self.adoptable()
        self.report_finished(identifier, engine_task)
        original = engine.download

        def download(task_id):
            response = original(task_id)
            del engine.tasks[task_id]
            return response

        engine.download = download
        self.observe()
        self.assert_no_custody_files(identifier)
        self.assertEqual(self.custody(identifier)['state'], 'fetching')
        self.assert_still_held(identifier)
        self.service._held_observations.clear()
        self.polls(1)
        self.assertEqual(self.service.info(identifier)['ledgetopEngineObservation'], {'state': 'task_missing'})
        with self.assertRaisesRegex(Conflict, 'custody'):
            self.service.operator(self.task(identifier)['job_id'], 'adopt', self.OPERATOR)

    def test_result_changing_during_the_fetch_is_not_published(self):
        identifier, engine, engine_task = self.adoptable()
        self.report_finished(identifier, engine_task)
        original = engine.download

        def download(task_id):
            response = original(task_id)
            engine_task['identity']['processingTime'] = 9000
            return response

        engine.download = download
        self.observe()
        self.assert_no_custody_files(identifier)
        self.assertEqual(self.custody(identifier)['state'], 'fetching')

    def test_stale_rerun_after_40_holds_and_refuses_adoption(self):
        for verified in (False, True):
            with self.subTest(verified=verified):
                identifier, engine, engine_task = self.adoptable()
                self.report_finished(identifier, engine_task)
                engine.interrupt_download_once = not verified
                self.observe()
                engine_task['status']['code'] = 20
                self.observe()
                self.assertEqual(self.custody(identifier), {'state': 'refused', 'reason': 'engine_result_changed'})
                fetched = engine.download_calls
                self.finish(identifier)  # Its rerun finishes: still not the result in custody.
                self.observe()
                self.assertEqual(engine.download_calls, fetched)
                with self.assertRaisesRegex(Conflict, 'custody'):
                    self.service.operator(self.task(identifier)['job_id'], 'adopt', self.OPERATOR)
                self.assert_still_held(identifier)
                self.queue.operator(self.task(identifier)['job_id'], 'resolve', self.OPERATOR)

    def test_rerun_seen_only_at_adoption_refuses(self):
        for change in ({'status': {'code': 10}}, {'status': {'code': 30}}):
            with self.subTest(change=change):
                identifier, engine, engine_task = self.adoptable()
                self.report_finished(identifier, engine_task)
                self.observe()
                engine_task.update(change)
                with self.assertRaisesRegex(Conflict, 'adoption refused'):
                    self.service.operator(self.task(identifier)['job_id'], 'adopt', self.OPERATOR)
                self.assertEqual(self.custody(identifier)['reason'], 'engine_result_changed')
                self.assert_still_held(identifier)
                self.queue.operator(self.task(identifier)['job_id'], 'resolve', self.OPERATOR)

    def test_silently_rerun_result_is_caught_by_its_processing_time(self):
        identifier, engine, engine_task = self.adoptable()
        self.report_finished(identifier, engine_task)
        self.observe()
        engine_task['identity']['processingTime'] = 7000
        self.observe()
        self.assertEqual(self.custody(identifier)['reason'], 'engine_result_changed')

    def test_failed_and_cancelled_codes_stay_held_and_fetch_nothing(self):
        for code in (30, 50):
            with self.subTest(code=code):
                identifier, engine, engine_task = self.adoptable()
                self.finish(identifier, code)
                self.observe()
                self.assertIsNone(self.custody(identifier))
                self.assertEqual(engine.download_calls, 0)
                self.assert_still_held(identifier)
                self.queue.operator(self.task(identifier)['job_id'], 'resolve', self.OPERATOR)

    def test_adoption_after_engine_cleanup_uses_the_custody_copy(self):
        identifier, engine, engine_task = self.adoptable()
        before = self.task(identifier)
        self.report_finished(identifier, engine_task)
        self.observe()
        del engine.tasks[before['engine_uuid']]
        self.assertEqual(self.service.operator(before['job_id'], 'adopt', self.OPERATOR)['status'], 'succeeded')
        stream, _ = self.service.download(identifier)
        with stream:
            self.assertEqual(stream.read(), engine.archive)

    def test_adoption_needs_a_current_read_and_intact_bytes(self):
        identifier, engine, engine_task = self.adoptable()
        before = self.task(identifier)
        self.report_finished(identifier, engine_task)
        self.observe()
        engine.read_unavailable = True
        with self.assertRaisesRegex(Conflict, 'current engine read'):
            self.service.operator(before['job_id'], 'adopt', self.OPERATOR)
        engine.read_unavailable = False
        path = Path(before['directory'], 'all.zip')
        good = path.read_bytes()
        path.chmod(0o600)
        path.write_bytes(good[:-1] + bytes((good[-1] ^ 1,)))
        with self.assertRaises(Conflict):
            self.service.operator(before['job_id'], 'adopt', self.OPERATOR)
        self.assert_still_held(identifier)
        path.write_bytes(good)
        self.assertEqual(self.service.operator(before['job_id'], 'adopt', self.OPERATOR)['status'], 'succeeded')

    def test_adoption_requires_the_operator_attestation(self):
        identifier, engine, engine_task = self.adoptable()
        job = self.task(identifier)['job_id']
        self.report_finished(identifier, engine_task)
        self.observe()
        for request in ({'note': 'synthetic'}, {'note': '', 'workerStopped': True},
                        {'note': 'synthetic', 'workerStopped': False}):
            with self.assertRaises(ValueError):
                self.service.operator(job, 'adopt', request)
        self.assert_still_held(identifier)

    def test_adoption_of_any_other_hold_is_refused(self):
        self.service._node_lost_hold = True
        self.service._adopt_lost_result = True
        identifier = self.submit()
        self.service.tick()
        self.polls(1)
        task = self.task(identifier)
        self.service._hold(task, 'cancel_termination_unverified')
        self.finish(identifier)
        self.observe()
        self.assertEqual(self.engine_for(identifier).download_calls, 0)
        with self.assertRaisesRegex(Conflict, 'operator transition'):
            self.service.operator(task['job_id'], 'adopt', self.OPERATOR)

    def test_resolving_as_failed_still_works_and_keeps_the_copy(self):
        identifier, engine, engine_task = self.adoptable()
        before = self.task(identifier)
        self.report_finished(identifier, engine_task)
        self.observe()
        job = self.service.operator(before['job_id'], 'resolve', self.OPERATOR)
        self.assertEqual(job['status'], 'failed')
        self.polls(1)
        self.assertEqual(self.service.info(identifier)['status'], {'code': 30, 'errorMessage': 'node_lost'})
        self.assertEqual(Path(before['directory'], 'all.zip').read_bytes(), engine.archive)
        self.assertTrue(Path(before['directory'], 'all.zip.receipt').exists())
        with self.assertRaises(Conflict):
            self.service.download(identifier)
        with self.assertRaises(Conflict):
            self.service.operator(before['job_id'], 'adopt', self.OPERATOR)
        self.assertIn(before['engine_uuid'], engine.tasks)

    def test_fetch_waits_for_guard_admission_and_production_hold(self):
        identifier, engine, engine_task = self.adoptable()
        self.report_finished(identifier, engine_task)

        class Closed:
            def admit(self):
                raise AdmissionClosed()

        self.service.queue.admission_drain = Closed()
        self.observe()
        self.assertEqual(engine.download_calls, 0)
        del self.service.queue.admission_drain
        self.config.viewer_production_held = True
        self.observe()
        self.assertEqual(engine.download_calls, 0)
        self.config.viewer_production_held = False
        with patch.object(self.service, '_guard_ready', return_value=False):
            self.observe()
        self.assertEqual(engine.download_calls, 0)
        self.observe()
        self.assertEqual(self.custody(identifier), {'state': 'verified'})

    def test_fetch_stops_at_shutdown_and_keeps_the_hold(self):
        identifier, engine, engine_task = self.adoptable()
        self.report_finished(identifier, engine_task)
        self.service._stop.set()
        self.service._review_held_result(self.task(identifier), self.service._read_status(self.task(identifier)))
        for thread in list(self.service._adopt_threads.values()):
            thread.join(10)
        self.assert_no_custody_files(identifier)
        self.assertEqual(self.custody(identifier)['state'], 'fetching')

    def test_app_shutdown_keeps_queue_authority_until_result_custody_writer_exits(self):
        from processing.app import CoordinatorService

        identifier, engine, engine_task = self.adoptable()
        self.report_finished(identifier, engine_task)
        fetch_entered, release_fetch = threading.Event(), threading.Event()
        original_download = engine.download

        def blocked_download(engine_task_id):
            fetch_entered.set()
            if not release_fetch.wait(5):
                raise AssertionError('test did not release blocked result fetch')
            return original_download(engine_task_id)

        engine.download = blocked_download
        task = self.task(identifier)
        self.service._review_held_result(task, self.service._read_status(task))
        self.assertTrue(fetch_entered.wait(2), 'result fetch did not reach the blocked engine response')
        fetch = self.service._adopt_threads[identifier]

        class Server:
            def __init__(self):
                self.serving = threading.Event()
                self.stopped = threading.Event()
                self.daemon_threads = True
                self.block_on_close = False

            def serve_forever(self, **_kwargs):
                self.serving.set()
                self.stopped.wait(5)

            def shutdown(self):
                self.stopped.set()

            def server_close(self):
                pass

        server = Server()
        handler = SimpleNamespace(viewer_service=self.service, reference_listener=None,
                                  prototype_queue=self.queue, lifecycle_service=None)
        coordinator = CoordinatorService(server, handler)
        stop = threading.Event()
        result = []
        coordinator_thread = threading.Thread(target=lambda: result.append(coordinator.run(stop)),
                                              name='synthetic-coordinator-shutdown')
        queue_closed_while_fetching = threading.Event()
        original_queue_close = self.queue.close

        def record_queue_close():
            if fetch.is_alive():
                queue_closed_while_fetching.set()
            original_queue_close()

        self.queue.close = record_queue_close
        original_join = threading.Thread.join

        def bounded_fetch_join(thread, timeout=None):
            if thread is fetch and timeout is not None:
                return original_join(thread, min(timeout, 0.05))
            return original_join(thread, timeout)

        coordinator_thread.start()
        try:
            deadline = time.monotonic() + 2
            while coordinator.health()['state'] != 'ready' and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertEqual(coordinator.health()['state'], 'ready')
            with patch.object(threading.Thread, 'join', new=bounded_fetch_join):
                stop.set()
                coordinator_thread.join(3)
            self.assertFalse(coordinator_thread.is_alive(), 'coordinator shutdown did not finish')
            self.assertEqual(result, [2])
            self.assertFalse(queue_closed_while_fetching.is_set(),
                             'coordinator released queue authority under a live custody writer')
            self.assertTrue(fetch.is_alive(), 'blocked result fetch unexpectedly exited before queue close')
        finally:
            release_fetch.set()
            original_join(fetch, 3)
            stop.set()
            coordinator_thread.join(3)
            self.queue.close = original_queue_close
        self.assertFalse(fetch.is_alive(), 'result fetch leaked after the test released its engine response')

    def test_custody_completes_an_incomplete_snapshot(self):
        self.service._log_retention = True
        identifier, engine, engine_task = self.adoptable()
        engine_task['console'] = ['one']
        engine.output_unavailable = True
        engine.archive = self.console_archive('one\ntwo\nlast words')
        self.report_finished(identifier, engine_task)
        self.observe()
        self.assertEqual(self.custody(identifier), {'state': 'verified'})
        self.assertEqual(self.service.output(identifier, 0)[-1], 'last words')
        self.assertEqual(self.service.operator(self.task(identifier)['job_id'], 'adopt', self.OPERATOR)['status'], 'succeeded')
        self.assertEqual(self.service.output(identifier, 0)[-1], 'last words')

    def test_invalid_adoption_file_stops_at_inspection(self):
        identifier, engine, engine_task = self.adoptable()
        self.report_finished(identifier, engine_task)
        self.adoption_file(identifier).write_text('{}')
        self.observe()
        self.assertEqual(engine.download_calls, 0)
        self.assertEqual(self.custody(identifier), {'state': 'unavailable'})
        self.assert_still_held(identifier)
        self.assertEqual(self.adoption_file(identifier).read_text(), '{}')

    def test_adoption_off_changes_nothing(self):
        self.service._node_lost_hold = True
        identifier = self.submit()
        self.service.tick()
        self.polls(1)
        task = self.task(identifier)
        engine = self.engines[task['node_id']]
        engine.read_unavailable = True
        self.polls(NODE_LOST_POLLS)
        engine.read_unavailable = False
        self.finish(identifier)
        self.observe()
        self.assertEqual(engine.download_calls, 0)
        self.assertFalse(self.adoption_file(identifier).exists())
        self.assertNotIn('ledgetopResultCustody', self.service.info(identifier))
        with self.assertRaises(ValueError):
            self.service.operator(task['job_id'], 'adopt', self.OPERATOR)
        with self.assertRaises(Conflict):
            self.service.operator(task['job_id'], 'adopt', {'note': 'synthetic'})
        self.assert_still_held(identifier)

    def test_retention_off_keeps_the_live_list_and_writes_nothing(self):
        identifier = self.submit()
        self.service.tick()
        self.polls(3)
        engine = self.engine_for(identifier)
        self.assertFalse([call for call in engine.calls if call[0] == 'read' and call[2] == 'output'])
        self.assertEqual(self.service.output(identifier, 1), ['', 'comma,record'])
        self.assertFalse(self.retained_file(identifier).exists())
        self.assertNotIn('ledgetopOutputStream', self.service.info(identifier))
        engine.output_unavailable = True
        self.assertEqual(self.service.output(identifier, 0), [])


if __name__ == '__main__':
    unittest.main()
