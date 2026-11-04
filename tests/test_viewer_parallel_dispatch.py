"""PROCESSING_VIEWER_PARALLEL_DISPATCH with fake engines and exact fixture bytes.

Real queue, store and service, wired as startup wires them. Blocking engines
stand in for slow transfers; no network, real engine or host writes.
"""

from http.server import ThreadingHTTPServer
import io
import logging
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid

from processing.app import CoordinatorService, make_handler
from processing.config import Config, from_environment
from processing.maintenance import Conflict
from processing.odx_smoke_worker import SmokeError
from processing.prototype import Queue
from processing.viewer_service import ViewerService
from tests.test_viewer_service import FakeEngine, FakeFleet, OPTIONS, REQUEST_OPTIONS

TOKEN = 'p' * 32
VIEWER = 'v' * 32
WAIT = 10


def wait_until(predicate, timeout=WAIT):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.005)
    return predicate()


def returns(function, timeout=WAIT):
    """Call on a side thread; (finished, outcome) never hangs the test."""
    outcome = []
    def run():
        try:
            outcome.append(function())
        except BaseException as exc:
            outcome.append(exc)
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    thread.join(timeout)
    return not thread.is_alive(), outcome[0] if outcome else None


class Harness(unittest.TestCase):
    parallel = True

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.config = SimpleNamespace(prototype_state_dir=str(self.root / 'state'),
            viewer_max_input_bytes=20 * 1024 ** 3, viewer_max_archive_bytes=4 * 1024 ** 2,
            local_node_url='http://synthetic-cluster.invalid:3000', local_node_token='private-cluster',
            main_node_url='http://synthetic-main.invalid:3000', main_node_token='private-main',
            local_max_photos=2, local_max_input_bytes=20 * 1024 ** 3)
        if self.parallel:
            self.config.viewer_parallel_dispatch = True
        self.fleet = FakeFleet()
        self.engines = {name: FakeEngine(name, self.fleet) for name in ('cluster', 'main')}
        self.release = threading.Event()
        self.entered = threading.Event()
        self.open_queue()

    def open_queue(self):
        self.queue = Queue(self.root / 'state', node_ids=('cluster', 'main'), treedar_node_ids=('main',),
                           viewer_enabled=True, per_node_slots=self.parallel)
        self.service = ViewerService(self.queue, self.config, engines=self.engines, probe=self.probe)
        self.service._catalog = lambda: {'ready': True, 'commonOptions': [dict(item) for item in OPTIONS]}
        self.service._selected_options = lambda _node: [dict(item) for item in OPTIONS]

    def tearDown(self):
        self.release.set()
        self.service.close()
        self.service.wait_for_transfers()
        if not self.queue._lock.closed:
            self.queue.close()

    def probe(self, role, origin, token):
        node = 'cluster' if role == 'local' else 'main'
        return {'ready': True, 'maxImages': None,
                'taskQueueCount': sum(x.startswith(node + ':') for x in self.fleet.active)}

    def submit(self, count=1):
        identifier = str(uuid.uuid4())
        self.service.initialize(identifier, 'Synthetic survey', REQUEST_OPTIONS, None, 'production')
        for index in range(count):
            content = b'owned synthetic photo bytes'
            self.service.upload(identifier, f'photo-{index:05}.jpg', io.BytesIO(content), len(content))
        self.service.commit(identifier)
        return identifier

    def task(self, identifier):
        return self.service.store.task(identifier)

    def block_uploads(self, node='cluster'):
        original = self.engines[node].upload
        def blocked(identifier, item):
            self.entered.set()
            if not self.release.wait(30):
                raise RuntimeError('synthetic transfer timeout')
            return original(identifier, item)
        self.engines[node].upload = blocked

    def start_blocked(self):
        self.block_uploads()
        identifier = self.submit()
        self.service.tick()
        self.assertTrue(self.entered.wait(WAIT))
        return identifier

    def phase(self, identifier, phase):
        self.assertTrue(wait_until(lambda: self.task(identifier)['phase'] == phase),
                        (phase, self.task(identifier)['phase'], self.task(identifier)['error']))

    def drained(self):
        self.assertTrue(returns(self.service.wait_for_transfers)[0])


class FlagOffTests(Harness):
    parallel = False

    def test_direct_queue_close_cannot_release_authority_during_synchronous_sender(self):
        self.block_uploads()
        self.submit()
        sender = threading.Thread(target=self.service.tick, daemon=True)
        sender.start()
        self.assertTrue(self.entered.wait(WAIT))
        with self.assertRaisesRegex(RuntimeError, 'worker still active'):
            self.queue.close()
        with self.assertRaisesRegex(RuntimeError, 'already owned'):
            Queue(self.root / 'state', node_ids=('cluster', 'main'))
        self.release.set()
        sender.join(WAIT)
        self.assertFalse(sender.is_alive())
        self.service.close()
        self.queue.close()

    def test_absent_configuration_keeps_synchronous_transfer_and_lock(self):
        self.assertFalse(self.service._parallel_dispatch)
        self.assertFalse(self.service.store.parallel_dispatch)
        senders = []
        original = self.engines['cluster'].upload
        self.engines['cluster'].upload = lambda *args: (senders.append(threading.current_thread()), original(*args))
        identifier = self.submit()
        self.service.tick()
        self.assertEqual(senders, [threading.current_thread()])
        self.assertEqual(self.task(identifier)['phase'], 'processing')
        self.assertEqual(self.service._dispatch_threads, {})

    def test_absent_configuration_still_blocks_tick_and_cancel_during_upload(self):
        self.block_uploads()
        identifier = self.submit()
        first = threading.Thread(target=self.service.tick, daemon=True)
        first.start()
        self.assertTrue(self.entered.wait(WAIT))
        results = []
        second = threading.Thread(target=lambda: results.append(self.service.tick()), daemon=True)
        cancel = threading.Thread(target=lambda: results.append(self.service.cancel(identifier)), daemon=True)
        second.start(); cancel.start()
        second.join(0.2); cancel.join(0.2)
        self.assertEqual(results, [])
        self.assertTrue(second.is_alive() and cancel.is_alive())
        self.release.set()
        for thread in (first, second, cancel):
            thread.join(WAIT)
            self.assertFalse(thread.is_alive())
        self.assertEqual(self.task(identifier)['phase'], 'cancel_pending')

    def test_absent_configuration_keeps_fenced_update_blocking(self):
        identifier = self.submit()
        release, entered = threading.Event(), threading.Event()
        def fence():
            with self.service.upload_batch(identifier):
                entered.set()
                release.wait(WAIT)
        holder = threading.Thread(target=fence, daemon=True)
        holder.start()
        self.assertTrue(entered.wait(WAIT))
        update = threading.Thread(target=lambda: self.service.store.update(identifier, error=None), daemon=True)
        update.start()
        update.join(0.2)
        self.assertTrue(update.is_alive())
        release.set()
        for thread in (holder, update):
            thread.join(WAIT)
            self.assertFalse(thread.is_alive())


class ParallelDispatchTests(Harness):
    def fenced(self, identifier):
        """Keep a late multipart request's fence open on another thread."""
        release, entered = threading.Event(), threading.Event()
        def fence():
            with self.service.upload_batch(identifier):
                entered.set()
                release.wait(30)
        holder = threading.Thread(target=fence, daemon=True)
        holder.start()
        self.assertTrue(entered.wait(WAIT))
        self.addCleanup(holder.join, WAIT)
        self.addCleanup(release.set)
        return release

    def test_flag_keeps_inbound_uploads_on_the_store_lock(self):
        """Inbound parallelism stays off until shared disk is reserved."""
        self.assertFalse(self.queue.parallel_viewer_uploads)
        entered, release = threading.Event(), threading.Event()
        class SlowStream(io.BytesIO):
            def read(self, size=-1):
                entered.set()
                release.wait(WAIT)
                return super().read(size)
        identifier = str(uuid.uuid4())
        self.service.initialize(identifier, 'Synthetic survey', REQUEST_OPTIONS, None, 'production')
        upload = threading.Thread(target=self.service.upload,
                                  args=(identifier, 'photo.jpg', SlowStream(b'owned'), 5), daemon=True)
        upload.start()
        self.addCleanup(upload.join, WAIT)
        self.addCleanup(release.set)
        self.assertTrue(entered.wait(WAIT))
        reader = threading.Thread(target=self.service.store.task, args=(identifier,), daemon=True)
        reader.start()
        reader.join(0.1)
        self.assertTrue(reader.is_alive())
        release.set()
        reader.join(WAIT)
        upload.join(WAIT)
        self.assertFalse(reader.is_alive() or upload.is_alive())
        self.assertEqual([item['name'] for item in self.task(identifier)['files']], ['photo.jpg'])

    def test_second_tick_does_not_block_during_upload(self):
        identifier = self.start_blocked()
        self.assertEqual(returns(self.service.tick), (True, None))
        self.assertEqual(self.task(identifier)['job']['status'], 'running')
        self.assertEqual(self.task(identifier)['phase'], 'upload_pending')

    def test_large_job_starts_on_main_during_cluster_upload(self):
        small = self.start_blocked()
        large = self.submit(count=3)
        self.assertEqual(returns(self.service.tick), (True, None))
        self.phase(large, 'processing')
        self.assertEqual(self.task(large)['node_id'], 'main')
        self.assertEqual(self.task(small)['node_id'], 'cluster')
        self.assertEqual(self.task(small)['phase'], 'upload_pending')
        self.assertEqual(len(self.engines['main'].tasks), 1)

    def test_unrelated_cancels_return_during_upload(self):
        self.start_blocked()
        running, queued = self.submit(count=3), self.submit()
        self.service.tick()
        self.phase(running, 'processing')
        finished, result = returns(lambda: self.service.cancel(queued))
        self.assertTrue(finished)
        self.assertEqual(result['success'], True)
        self.assertEqual(self.task(queued)['stage'], 'cancelled')
        finished, result = returns(lambda: self.service.cancel(running))
        self.assertTrue(finished)
        self.assertEqual(result, {'success': True})
        self.assertEqual(self.task(running)['phase'], 'cancel_pending')

    def test_cancel_of_uploading_job_refuses_then_succeeds_after_commit(self):
        identifier = self.start_blocked()
        engine = self.engines['cluster']
        finished, result = returns(lambda: self.service.cancel(identifier))
        self.assertTrue(finished)
        self.assertIsInstance(result, Conflict)
        self.assertFalse(any(call[0] == 'cancel' for call in engine.calls))
        self.assertEqual(self.task(identifier)['job']['status'], 'running')
        self.release.set()
        self.phase(identifier, 'processing')
        self.drained()
        self.assertEqual(self.service.cancel(identifier), {'success': True})
        self.assertEqual(len([call for call in engine.calls if call[0] == 'cancel']), 1)
        self.assertEqual(len([call for call in engine.calls if call[0] == 'commit']), 1)

    def test_polling_completes_other_node_during_upload(self):
        small = self.start_blocked()
        large = self.submit(count=3)
        self.service.tick()
        self.phase(large, 'processing')
        task = self.task(large)
        self.engines['main'].finish(task['engine_uuid'])
        self.assertEqual(returns(self.service.tick), (True, None))
        self.assertEqual(self.task(large)['job']['status'], 'succeeded')
        self.assertEqual(self.task(small)['phase'], 'upload_pending')

    def test_same_task_request_during_upload_does_not_hold_the_job(self):
        identifier = self.start_blocked()
        fence = self.fenced(identifier)
        self.release.set()
        self.phase(identifier, 'processing')
        fence.set()
        self.drained()
        self.assertEqual(self.task(identifier)['job']['status'], 'running')
        self.assertIsNone(self.task(identifier)['error'])

    def test_same_task_request_does_not_change_the_reason_for_a_real_hold(self):
        self.engines['cluster'].fail = 'upload'
        identifier = self.start_blocked()
        fence = self.fenced(identifier)
        self.release.set()
        self.phase(identifier, 'held')
        fence.set()
        self.drained()
        task = self.task(identifier)
        self.assertEqual((task['job']['status'], task['error']), ('held', 'engine_submission_uncertain'))

    def test_late_multipart_fence_does_not_hold_job_at_claim(self):
        identifier = self.submit()
        fence = self.fenced(identifier)
        self.assertEqual(returns(self.service.tick), (True, None))
        self.phase(identifier, 'processing')
        fence.set()
        self.drained()
        self.assertEqual(self.task(identifier)['job']['status'], 'running')

    def test_remove_and_commit_replay_race_transfer_without_false_hold(self):
        identifier = self.submit(count=2)
        stop = threading.Event()
        def noise():
            while not stop.is_set():
                if not self.task(identifier)['node_id']:
                    continue  # Removing a queued job is a legitimate cancellation.
                for call in (lambda: self.service.remove(identifier), lambda: self.service.commit(identifier),
                             lambda: self.service.info(identifier)):
                    try:
                        call()
                    except Conflict:
                        pass
        worker = threading.Thread(target=noise, daemon=True)
        worker.start()
        try:
            self.service.tick()
            self.drained()
        finally:
            stop.set()
            worker.join(WAIT)
        self.assertFalse(worker.is_alive())
        self.assertEqual(self.task(identifier)['phase'], 'processing')
        self.assertEqual(self.task(identifier)['job']['status'], 'running')

    def test_one_transfer_per_node_under_concurrent_ticks(self):
        guard = threading.Lock()
        current, peak, violations = {}, {}, []
        for name, engine in self.engines.items():
            def wrap(original, name=name):
                def upload(identifier, item):
                    with guard:
                        current.setdefault(name, set()).add(identifier)
                        peak[name] = max(peak.get(name, 0), len(current[name]))
                    time.sleep(0.002)
                    return original(identifier, item)
                return upload
            engine.upload = wrap(engine.upload)
        tasks = [self.submit(count=1 + index % 4) for index in range(10)]
        stop = threading.Event()
        def ticks():
            while not stop.is_set():
                try:
                    self.service.tick()
                except Exception as exc:
                    violations.append(exc)
        workers = [threading.Thread(target=ticks, daemon=True) for _ in range(4)]
        for worker in workers:
            worker.start()
        def finished():
            for identifier in tasks:
                task = self.task(identifier)
                if task['phase'] == 'processing' and task['engine_uuid']:
                    engine = self.engines[task['node_id']]
                    with guard:
                        current.get(task['node_id'], set()).discard(task['engine_uuid'])
                    if engine.tasks[task['engine_uuid']]['status']['code'] != 40:
                        engine.finish(task['engine_uuid'])
            return all(self.task(identifier)['job']['status'] == 'succeeded' for identifier in tasks)
        try:
            self.assertTrue(wait_until(finished, 60), [self.task(i)['phase'] for i in tasks])
        finally:
            stop.set()
            for worker in workers:
                worker.join(WAIT)
                self.assertFalse(worker.is_alive())
        self.assertEqual(violations, [])
        self.assertEqual(max(peak.values()), 1)
        for engine in self.engines.values():
            self.assertEqual(len([c for c in engine.calls if c[0] == 'commit']), len(engine.tasks))
        self.assertEqual(sum(len(e.tasks) for e in self.engines.values()), len(tasks))

    def test_engine_exception_holds_job_and_resolve_frees_the_slot(self):
        self.engines['cluster'].fail = 'upload'
        identifier = self.submit()
        self.service.tick()
        self.phase(identifier, 'held')
        self.drained()
        task = self.task(identifier)
        self.assertEqual((task['job']['status'], task['error']), ('held', 'engine_submission_uncertain'))
        self.assertEqual(self.service._dispatch_threads, {})
        self.assertEqual(len([c for c in self.engines['cluster'].calls if c[0] == 'upload']), 1)
        self.engines['cluster'].fail = None
        self.engines['main'].fail = 'initialize'  # Main stays occupied too.
        blocker = self.submit()
        self.service.tick()
        self.phase(blocker, 'held')
        self.drained()
        waiting = self.submit()
        self.service.tick()
        self.assertIsNone(self.task(waiting)['node_id'])  # The held slot stays closed.
        self.service.operator(task['job_id'], 'resolve', {'note': 'synthetic', 'workerStopped': True})
        self.service.tick()
        self.phase(waiting, 'processing')
        self.assertEqual(self.task(waiting)['node_id'], 'cluster')

    def test_failure_escaping_sender_holds_atomically_without_traceback(self):
        identifier = self.submit()
        hooks = []
        def broken(_identifier):
            raise SmokeError('private-route?token=private-cluster')
        with patch.object(self.service, '_send_claimed_task', broken), \
                patch.object(threading, 'excepthook', hooks.append), \
                self.assertLogs('processing.viewer_service', logging.ERROR) as logs:
            self.service.tick()
            self.phase(identifier, 'held')
            self.drained()
        self.assertEqual(hooks, [])
        self.assertNotIn('private', ' '.join(logs.output))
        task = self.task(identifier)
        self.assertEqual((task['job']['status'], task['error']), ('held', 'reconciliation_error'))

    def test_sender_and_its_hold_failing_leaves_next_tick_to_hold(self):
        identifier = self.submit()
        def broken(_identifier):
            raise OSError('synthetic database failure')
        with patch.object(self.service, '_send_claimed_task', broken), \
                patch.object(self.service.store, 'hold_reconciliation_error',
                             side_effect=OSError('synthetic')) as failed, \
                self.assertLogs('processing.viewer_service', logging.ERROR):
            self.service.tick()
            self.drained()
        self.assertTrue(failed.called)
        self.assertEqual(self.task(identifier)['job']['status'], 'running')
        self.service.tick()
        task = self.task(identifier)
        self.assertEqual((task['job']['status'], task['phase'], task['error']),
                         ('held', 'held', 'submission_interrupted'))

    def test_thread_start_failure_holds_and_clears_registry(self):
        identifier = self.submit()
        with patch('processing.viewer_service.threading.Thread.start', side_effect=RuntimeError('synthetic')):
            self.service.tick()
        task = self.task(identifier)
        self.assertEqual((task['job']['status'], task['error']), ('held', 'engine_submission_uncertain'))
        self.assertEqual(self.service._dispatch_threads, {})

    def test_shutdown_joins_sender_before_queue_authority_releases(self):
        self.block_uploads()
        identifier = self.submit(count=2)
        job_id = self.task(identifier)['job_id']
        server = ThreadingHTTPServer(('127.0.0.1', 0), None)
        handler = SimpleNamespace(viewer_service=self.service, reference_listener=None, prototype_queue=self.queue)
        coordinator = CoordinatorService(server, handler)
        stop, result = threading.Event(), []
        runner = threading.Thread(target=lambda: result.append(coordinator.run(stop)), daemon=True)
        runner.start()
        self.assertTrue(self.entered.wait(WAIT))
        with self.service._lock:
            sender = next(thread for _, thread in self.service._dispatch_threads.values())
        stop.set()
        runner.join(0.3)
        self.assertTrue(runner.is_alive())
        self.assertFalse(self.queue._lock.closed)
        self.release.set()
        runner.join(WAIT + 5)
        self.assertFalse(runner.is_alive())
        self.assertEqual(result, [0])
        self.assertFalse(sender.is_alive())
        self.assertTrue(self.queue._lock.closed)
        self.open_queue()
        task = self.service.store.task_for_job(job_id)
        self.assertEqual((task['job']['status'], task['error']), ('held', 'dispatch_interrupted_for_shutdown'))
        self.assertFalse(any(call[0] == 'commit' for call in self.engines['cluster'].calls))
        self.assertEqual(len([c for c in self.engines['cluster'].calls if c[0] == 'upload']), 1)

    def test_direct_queue_close_cannot_release_authority_during_sender(self):
        self.start_blocked()
        with self.assertRaisesRegex(RuntimeError, 'worker still active'):
            self.queue.close()
        self.assertFalse(self.queue._lock.closed)
        with self.assertRaisesRegex(RuntimeError, 'already owned'):
            Queue(self.root / 'state', node_ids=('cluster', 'main'))
        self.release.set()
        self.service.wait_for_transfers()
        self.service.close()
        self.queue.close()
        self.assertTrue(self.queue._lock.closed)
        with self.assertRaisesRegex(RuntimeError, 'authority unavailable'):
            self.queue.register_external_worker()


class InventoryTests(Harness):
    def open_queue(self):
        self.config.node_inventory = tuple(
            {'id': node, 'physicalHostId': node + '-host', 'origin': f'http://{node}.invalid:3000',
             'token': 'private', 'maxPhotos': limit, 'capabilities': ('odm',)}
            for node, limit in (('cluster', 2), ('main', None)))
        super().open_queue()

    def probe(self, node, origin, token):
        return {'ready': True, 'maxImages': None,
                'taskQueueCount': sum(x.startswith(node + ':') for x in self.fleet.active)}

    def test_large_job_starts_on_main_during_cluster_upload(self):
        small = self.start_blocked()
        large = self.submit(count=3)
        self.assertEqual(returns(self.service.tick), (True, None))
        self.phase(large, 'processing')
        self.assertEqual(self.task(large)['node_id'], 'main')
        self.assertEqual(self.task(small)['phase'], 'upload_pending')

    def test_occupied_nodes_are_not_offered_during_upload(self):
        small = self.start_blocked()
        large = self.submit(count=3)
        self.service.tick()
        self.phase(large, 'processing')
        third = self.submit()
        self.assertEqual(returns(self.service.tick), (True, None))
        self.assertIsNone(self.task(third)['node_id'])
        self.assertEqual(self.task(third)['phase'], 'waiting_node')
        self.assertEqual(len(self.engines['cluster'].tasks), 1)
        self.assertEqual(self.task(small)['phase'], 'upload_pending')


DYING_SENDER = '''import io,os,sys
from pathlib import Path
from types import SimpleNamespace
from processing.prototype import Queue
from processing.viewer_service import ViewerService
from tests.test_viewer_service import FakeEngine,FakeFleet,OPTIONS,REQUEST_OPTIONS
root,identifier,point=Path(sys.argv[1]),sys.argv[2],sys.argv[3]
config=SimpleNamespace(prototype_state_dir=str(root/'state'),viewer_max_input_bytes=10**9,viewer_max_archive_bytes=10**6,
 local_node_url='http://synthetic-cluster.invalid:3000',local_node_token='a',main_node_url='http://synthetic-main.invalid:3000',
 main_node_token='b',local_max_photos=2,local_max_input_bytes=10**9,viewer_parallel_dispatch=True)
queue=Queue(root/'state',node_ids=('cluster','main'),treedar_node_ids=('main',),viewer_enabled=True,
 per_node_slots=True)
fleet=FakeFleet();engines={name:FakeEngine(name,fleet) for name in ('cluster','main')}
def die(*_):os._exit(73)
if point=='upload':engines['cluster'].upload=die
else:engines['cluster'].action=die
service=ViewerService(queue,config,engines=engines,probe=lambda *_:{'ready':True,'maxImages':None,'taskQueueCount':0})
service._catalog=lambda:{'ready':True,'commonOptions':[dict(o) for o in OPTIONS]}
service._selected_options=lambda _:[dict(o) for o in OPTIONS]
service.initialize(identifier,'Synthetic survey',REQUEST_OPTIONS,None,'production')
service.upload(identifier,'photo.jpg',io.BytesIO(b'owned'),5)
service.commit(identifier)
service.tick()
service.wait_for_transfers()
os._exit(0)
'''


class RestartTests(Harness):
    def die_at(self, point):
        self.service.close()
        self.queue.close()
        identifier = str(uuid.uuid4())
        process = subprocess.run([sys.executable, '-c', DYING_SENDER, str(self.root), identifier, point],
                                 capture_output=True, timeout=60)
        self.assertEqual(process.returncode, 73, process.stderr)
        self.open_queue()
        return identifier

    def test_process_death_mid_upload_holds_without_replay(self):
        identifier = self.die_at('upload')
        self.assertEqual(self.task(identifier)['job']['heldReason'], 'coordinator_restart')
        self.assertEqual(self.task(identifier)['phase'], 'upload_pending')
        self.service.tick()
        task = self.task(identifier)
        self.assertEqual((task['job']['status'], task['phase'], task['error']),
                         ('held', 'held', 'submission_interrupted'))
        self.assertEqual(self.engines['cluster'].calls, [])
        self.assertEqual(self.service._dispatch_threads, {})

    def test_process_death_at_commit_reconciles_by_reads_without_replay(self):
        identifier = self.die_at('commit')
        task = self.task(identifier)
        self.assertEqual(task['phase'], 'commit_pending')
        engine = self.engines['cluster']
        engine.tasks[task['engine_uuid']] = {'uuid': task['engine_uuid'], 'status': {'code': 20},
                                             'progress': 5, 'files': {}, 'cancel_requested': False}
        self.service.tick()
        self.assertEqual(self.task(identifier)['phase'], 'processing')
        self.assertEqual({call[0] for call in engine.calls}, {'read'})


class StartupTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.state = Path(self.temporary.name) / 'state'
        self.env = {'PROCESSING_API_TOKEN': TOKEN, 'PROCESSING_PER_NODE_SLOTS': '1',
                    'PROCESSING_VIEWER_ENABLED': '1', 'PROCESSING_VIEWER_TOKEN': VIEWER,
                    'PROCESSING_PROTOTYPE_STATE_DIR': str(self.state),
                    'PROCESSING_PROTOTYPE_NODE_IDS': 'cluster,main',
                    'LOCAL_NODE_URL': 'http://192.0.2.1:3000', 'MAIN_NODE_URL': 'http://192.0.2.2:3000'}

    def test_flag_is_absent_by_default(self):
        with patch.dict(os.environ, self.env, clear=True):
            self.assertIs(from_environment().viewer_parallel_dispatch, False)
        self.assertIs(Config(TOKEN, '', '', 800, 20000).viewer_parallel_dispatch, False)

    def test_invalid_values_refuse(self):
        for value in ('', 'true', 'TRUE', 'yes', '2', ' 1', '1 '):
            with self.subTest(value=value), patch.dict(
                    os.environ, {**self.env, 'PROCESSING_VIEWER_PARALLEL_DISPATCH': value}, clear=True):
                with self.assertRaisesRegex(ValueError, 'PROCESSING_VIEWER_PARALLEL_DISPATCH must be 0 or 1'):
                    from_environment()

    def test_flag_without_per_node_slots_refuses_before_state(self):
        for slots in ('0', None):
            env = {**self.env, 'PROCESSING_VIEWER_PARALLEL_DISPATCH': '1'}
            if slots is None:
                del env['PROCESSING_PER_NODE_SLOTS']
            else:
                env['PROCESSING_PER_NODE_SLOTS'] = slots
            with self.subTest(slots=slots), patch.dict(os.environ, env, clear=True):
                with self.assertRaisesRegex(ValueError, 'requires per-node slots'):
                    from_environment()
        self.assertFalse(self.state.exists())

    def test_direct_config_refuses_before_lease_or_state(self):
        for value, slots in ((True, False), (1, True), ('1', True)):
            config = Config(TOKEN, '', '', 800, 20000, prototype_state_dir=str(self.state),
                            per_node_slots=slots, viewer_parallel_dispatch=value)
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, 'requires per-node slots'):
                    make_handler(config)
        self.assertFalse(self.state.exists())

    def test_flag_with_reference_runtime_refuses_before_state(self):
        env = {**self.env, 'PROCESSING_VIEWER_PARALLEL_DISPATCH': '1',
               'PROCESSING_REFERENCE_CONFIG': str(Path(self.temporary.name) / 'absent.json')}
        with patch.dict(os.environ, env, clear=True):
            with self.assertRaisesRegex(ValueError, 'not qualified with the reference runtime'):
                from_environment()
        with patch.dict(os.environ, {**env, 'PROCESSING_VIEWER_PARALLEL_DISPATCH': '0'}, clear=True):
            self.assertIs(from_environment().viewer_parallel_dispatch, False)
        self.assertFalse(self.state.exists())

    def test_direct_config_with_reference_runtime_refuses_before_lease_or_state(self):
        lease = Path(self.temporary.name) / 'lease'
        config = Config(TOKEN, '', '', 800, 20000, prototype_state_dir=str(self.state), per_node_slots=True,
                        viewer_parallel_dispatch=True, authority_lease_dir=str(lease),
                        reference_config_path=str(Path(self.temporary.name) / 'absent.json'))
        with patch('processing.reference_runtime.load_reference_settings',
                   side_effect=AssertionError('no settings read')):
            with self.assertRaisesRegex(ValueError, 'not qualified with the reference runtime'):
                make_handler(config)
        self.assertFalse(self.state.exists() or lease.exists())

    def test_startup_wires_service_only_when_enabled_and_never_inbound(self):
        for value, expected in ((None, False), ('0', False), ('1', True)):
            env = dict(self.env)
            if value is not None:
                env['PROCESSING_VIEWER_PARALLEL_DISPATCH'] = value
            with self.subTest(value=value), patch.dict(os.environ, env, clear=True):
                handler = make_handler(from_environment())
                try:
                    self.assertIs(handler.prototype_queue.parallel_viewer_uploads, False)
                    self.assertIs(handler.viewer_service._parallel_dispatch, expected)
                    self.assertIs(handler.viewer_service.store.parallel_dispatch, expected)
                finally:
                    handler.viewer_service.close()
                    handler.prototype_queue.close()


if __name__ == '__main__':
    unittest.main()
