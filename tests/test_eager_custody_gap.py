"""Synthetic normal-completion custody regression; no real engine or data.

The fake engine is configured to reject a download after its task is removed,
as a NodeODM terminal cleanup would. These tests do not simulate a real timer.
With PROCESSING_VIEWER_EAGER_RESULT_CUSTODY off the gap stays as characterized;
with it on the same outage ends with the result retained.
"""
import hashlib
import io
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
from types import SimpleNamespace
import threading
import time
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import uuid
import zipfile

from processing.admission_drain import AdmissionClosed, AdmissionDrain
from processing.app import CoordinatorService, make_handler
from processing.config import Config, from_environment
from processing.maintenance import Conflict
from processing.prototype import Queue
from processing.viewer_result_adoption import (CUSTODY_INTENT, CUSTODY_NAME, IDENTITY_MISMATCH, IDENTITY_UNAVAILABLE,
                                               RESULT_CHANGED)
from processing import viewer_service as service_module
from processing.viewer_service import (CENSUS_RETAINED_FILE_BYTES, EAGER_DEFERRED_RETRY_SECONDS, EAGER_FETCH_CONCURRENCY,
                                       ENGINE_MISSING_POLLS, EngineUnavailable, NODE_LOST_POLLS, NativeEngine)
from processing.viewer_tasks import TaskStore
from tests import test_viewer_parallel_dispatch as startup_fixture, test_viewer_service as viewer_fixture
from tests.test_service import start_server

ROOT = Path(__file__).resolve().parents[1]


class NormalCompletionCustody(unittest.TestCase):
    def setUp(self):
        self.fixture = viewer_fixture.ViewerServiceTests('test_download_twice_matches_hash_and_retains_archive_and_inputs')
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)

    def complete_without_viewer(self):
        fixture = self.fixture
        identifier = fixture.submit()
        fixture.service.tick()
        fixture.finish(identifier)
        fixture.service.tick()
        self.assertEqual(fixture.service.info(identifier)['status']['code'], 40)
        return identifier

    def test_characterizes_lazy_fetch_then_engine_cleanup(self):
        fixture = self.fixture
        identifier = self.complete_without_viewer()
        task = fixture.task(identifier)
        engine = fixture.engines[task['node_id']]
        self.assertEqual(engine.download_calls, 0)
        self.assertIsNone(task['archive_path'])
        del engine.tasks[task['engine_uuid']]
        with patch.object(engine, 'download', side_effect=EngineUnavailable('synthetic_terminal_cleanup')):
            with self.assertRaises(EngineUnavailable):
                fixture.service.download(identifier)
        self.assertIsNone(fixture.task(identifier)['archive_path'])
        self.assertTrue(fixture.task(identifier)['files'])

    def test_verified_local_archive_survives_later_engine_cleanup(self):
        fixture = self.fixture
        identifier = self.complete_without_viewer()
        task = fixture.task(identifier)
        engine = fixture.engines[task['node_id']]
        # This manually triggers the existing custody path. It is not evidence
        # that eager/background acquisition has been implemented.
        with fixture.service.download(identifier)[0] as result:
            first = result.read()
        del engine.tasks[task['engine_uuid']]
        with patch.object(engine, 'download', side_effect=EngineUnavailable('synthetic_terminal_cleanup')):
            with fixture.service.download(identifier)[0] as result:
                retained = result.read()
        self.assertEqual(hashlib.sha256(retained).digest(), hashlib.sha256(first).digest())
        self.assertEqual(engine.download_calls, 1)

    def test_flag_off_leaves_no_record_status_or_thread(self):
        fixture = self.fixture
        identifier = self.complete_without_viewer()
        fixture.service.tick()
        task = fixture.task(identifier)
        self.assertFalse(fixture.service._eager_threads or fixture.service._eager_due)
        self.assertNotIn('ledgetopResultCustody', fixture.service.info(identifier))
        self.assertIsNone(task['error'])
        self.assertEqual(fixture.service.store.pending_custody(), [])
        self.assertTrue(all(path.name.startswith('input-') for path in Path(task['directory']).iterdir()))
        with fixture.service.download(identifier)[0]:
            pass
        self.assertFalse((Path(task['directory']) / CUSTODY_NAME).exists())
        self.assertEqual(fixture.engines[task['node_id']].download_calls, 1)

    def test_flag_is_opt_in_strict_and_bounded(self):
        startup = startup_fixture.StartupTests('test_invalid_values_refuse')
        startup.setUp()
        self.addCleanup(startup.doCleanups)
        environment = startup.env
        with patch.dict(os.environ, environment, clear=True):
            config = from_environment()
            self.assertIs(config.viewer_eager_result_custody, False)
            self.assertIsNone(config.viewer_eager_custody_reserve_bytes)
            self.assertIsNone(config.viewer_eager_custody_max_bytes)
        self.assertIs(Config('t' * 32, '', '', 800, 20000).viewer_eager_result_custody, False)
        with patch.dict(os.environ, {**environment, 'PROCESSING_VIEWER_EAGER_RESULT_CUSTODY': '1',
                                     'PROCESSING_VIEWER_EAGER_CUSTODY_RESERVE_BYTES': '7',
                                     'PROCESSING_VIEWER_EAGER_CUSTODY_MAX_BYTES': '9'}, clear=True):
            config = from_environment()
            self.assertIs(config.viewer_eager_result_custody, True)
            self.assertEqual((config.viewer_eager_custody_reserve_bytes, config.viewer_eager_custody_max_bytes), (7, 9))
        for key, value in (('PROCESSING_VIEWER_EAGER_RESULT_CUSTODY', 'yes'),
                           ('PROCESSING_VIEWER_EAGER_CUSTODY_RESERVE_BYTES', '0'),
                           ('PROCESSING_VIEWER_EAGER_CUSTODY_MAX_BYTES', 'x')):
            with patch.dict(os.environ, {**environment, key: value}, clear=True):
                with self.assertRaises(ValueError):
                    from_environment()

    def test_on_without_a_stated_capacity_refuses_before_any_state(self):
        startup = startup_fixture.StartupTests('test_invalid_values_refuse')
        startup.setUp()
        self.addCleanup(startup.doCleanups)
        reserve, maximum = 'PROCESSING_VIEWER_EAGER_CUSTODY_RESERVE_BYTES', 'PROCESSING_VIEWER_EAGER_CUSTODY_MAX_BYTES'
        on = {**startup.env, 'PROCESSING_VIEWER_EAGER_RESULT_CUSTODY': '1'}
        for sizes in ({}, {reserve: '7'}, {maximum: '9'}, {reserve: '', maximum: '9'}, {reserve: '7', maximum: '0'},
                      {reserve: '-1', maximum: '9'}, {reserve: '7', maximum: '9x'}):
            with self.subTest(sizes), patch.dict(os.environ, {**on, **sizes}, clear=True):
                with self.assertRaises(ValueError):
                    from_environment()
        finished = subprocess.run([sys.executable, '-m', 'processing.app'], cwd=ROOT, capture_output=True, timeout=60,
                                  env={**on, 'PATH': os.environ.get('PATH', '')})
        self.assertNotEqual(finished.returncode, 0)
        self.assertFalse(startup.state.exists())

    def test_a_service_built_without_stated_capacity_refuses(self):
        fixture = self.fixture
        fixture.config.viewer_eager_result_custody = True
        for values in ((None, None), (1, None), (None, 1), (0, 1), (1, True)):
            with self.subTest(values):
                fixture.config.viewer_eager_custody_reserve_bytes, fixture.config.viewer_eager_custody_max_bytes = values
                with self.assertRaisesRegex(ValueError, 'explicit reserve and maximum'):
                    service_module.ViewerService(fixture.queue, fixture.config, engines=fixture.engines)

    def test_census_limit_constant_matches_the_updater(self):
        source = (ROOT / 'ops' / 'update_reference_coordinator.py').read_text()
        self.assertIn('info.st_size <= (%d if retained' % CENSUS_RETAINED_FILE_BYTES, source)


class Blocked:
    """Hold a download at its first read until released."""
    def __init__(self, engine):
        self.engine, self.original = engine, engine.download
        self.reading, self.release = threading.Event(), threading.Event()
        engine.download = self.download

    def download(self, identifier):
        response = self.original(identifier)
        read = response.read

        def blocked(length=-1):
            self.reading.set()
            if not self.release.wait(10):
                raise OSError('synthetic release never came')
            return read(length)

        response.read = blocked
        return response

    def undo(self):
        self.release.set()
        self.engine.download = self.original


def reported_identity(task):
    """What NodeODM's info says of a task it ran from these inputs."""
    return {'dateCreated': 1700000000000, 'imagesCount': task['images_count'],
            'options': [{'name': item['name'], 'value': item['value']} for item in task['options']]}


class StalledOrigin:
    """A real loopback engine that answers a download, then stops sending.

    ``NativeEngine`` reads it through a real socket, so a fetch waits in recv and
    no flag or mock can reach it; only the socket or the process ending can.
    """
    def __init__(self):
        self.listener = socket.create_server(('127.0.0.1', 0))
        self.url = 'http://127.0.0.1:%d' % self.listener.getsockname()[1]
        self.sent, self.connections = threading.Event(), []
        threading.Thread(target=self.serve, daemon=True).start()

    def serve(self):
        try:
            while True:
                connection, _ = self.listener.accept()
                self.connections.append(connection)
                while b'\r\n\r\n' not in connection.recv(65536):
                    pass
                connection.sendall(b'HTTP/1.1 200 OK\r\nContent-Type: application/zip\r\n'
                                   b'Content-Length: 100000\r\n\r\nPK')
                self.sent.set()
        except OSError:
            pass

    def close(self):
        for item in (self.listener, *self.connections):
            item.close()


class StubServer:
    """The native listener's lifecycle with no socket of its own."""
    daemon_threads, block_on_close = True, False

    def __init__(self):
        self.stopped = threading.Event()

    def serve_forever(self, **_kwargs):
        self.stopped.wait(30)

    def shutdown(self):
        self.stopped.set()

    def server_close(self):
        pass


def coordinator_for(service, queue):
    return CoordinatorService(StubServer(), SimpleNamespace(viewer_service=service, reference_listener=None,
                                                            prototype_queue=queue, lifecycle_service=None))


# A real process with a real read that nothing can wake, as a wedged kernel socket
# would be: a response hiding its descriptor. Reports, then lives until killed.
WEDGED_WRITER = """
import json, sys, threading
from processing.viewer_service import NativeEngine
from tests.test_eager_custody_gap import EagerCustody, StalledOrigin, coordinator_for, reported_identity

class Opaque:
    def __init__(self, response):
        self.response = response
    def __getattr__(self, name):
        return getattr(self.response, name)
    def __enter__(self):
        return self
    def __exit__(self, *details):
        self.response.close()
    def fileno(self):
        raise OSError('synthetic wedge')

case = EagerCustody('test_outage_after_completion_keeps_the_result')
case.setUp()
identifier = case.finished(wait=False)
origin = StalledOrigin()
real = NativeEngine(origin.url).download
case.engine(identifier).download = lambda task: Opaque(real(task))
case.service.tick()
assert origin.sent.wait(10)
fetch = case.service._eager_threads[identifier]
join = threading.Thread.join
threading.Thread.join = lambda thread, timeout=None: join(
    thread, min(timeout, 0.2) if thread is fetch and timeout is not None else timeout)
stop = threading.Event()
stop.set()
service = coordinator_for(case.service, case.fixture.queue)
code = service.run(stop)
task = case.fixture.task(identifier)
print(json.dumps({'code': code, 'alive': fetch.is_alive(), 'state': service.health()['state'],
                  'reason': service.health()['reason'], 'root': str(case.fixture.root), 'identifier': identifier,
                  'engine': task['engine_uuid'], 'node': task['node_id'],
                  'identity': reported_identity(task)}), flush=True)
sys.stdin.read()
"""

# A real SIGKILL straight after the commit of a job whose sidecar could not be written.
KILLED_AFTER_COMMIT = """
import json, os, signal
from unittest.mock import patch
from processing.viewer_tasks import TaskStore
from tests.test_eager_custody_gap import EagerCustody, reported_identity

case = EagerCustody('test_outage_after_completion_keeps_the_result')
case.setUp()
identifier = case.fixture.submit()
case.service.tick()
case.fixture.finish(identifier)
task = case.fixture.task(identifier)
print(json.dumps({'root': str(case.fixture.root), 'identifier': identifier, 'engine': task['engine_uuid'],
                  'node': task['node_id'], 'identity': reported_identity(task)}), flush=True)
commit = TaskStore.complete_engine_task

def commit_then_die(*args, **kwargs):
    commit(*args, **kwargs)
    os.kill(os.getpid(), signal.SIGKILL)

with patch.object(TaskStore, 'save_custody', side_effect=OSError('synthetic full disk')), \\
        patch.object(TaskStore, 'complete_engine_task', commit_then_die):
    case.service.tick()
"""


class EagerCustody(unittest.TestCase):
    def setUp(self):
        self.fixture = viewer_fixture.ViewerServiceTests('test_download_twice_matches_hash_and_retains_archive_and_inputs')
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.fixture.config.viewer_eager_result_custody = True
        self.fixture.config.viewer_eager_custody_reserve_bytes = 1
        self.fixture.config.viewer_eager_custody_max_bytes = 1024 ** 3
        for engine in self.fixture.engines.values():
            self.reports_identity(engine)
        self.restart()

    @staticmethod
    def reports_identity(engine):
        initialize = engine.initialize

        def initialized(task):
            initialize(task)
            engine.tasks[task['engine_uuid']].update(processingTime=5000, identity=reported_identity(task))

        engine.initialize = initialized

    @property
    def service(self):
        return self.fixture.service

    def restart(self):
        self.service.close()
        self.fixture.queue.close()
        self.fixture.make_queue_service()

    def join(self):
        for thread in list(self.service._eager_threads.values()):
            thread.join(10)
            self.assertFalse(thread.is_alive())

    def settle(self):
        """One due attempt, and the fetch it started, to the end."""
        self.service._eager_due.update({key: 0 for key in self.service._eager_due})
        self.service.tick()
        self.join()

    def finished(self, *, wait=True):
        """A job that reached code 40 with Viewer absent."""
        identifier = self.fixture.submit()
        self.service.tick()
        self.fixture.finish(identifier)
        if wait:
            self.service.tick()
            self.join()
        return identifier

    def engine(self, identifier):
        return self.fixture.engine_for(identifier)

    def custody(self, identifier):
        return self.service.info(identifier).get('ledgetopResultCustody')

    def record(self, identifier):
        return json.loads((self.directory(identifier) / CUSTODY_NAME).read_text())

    def directory(self, identifier):
        return Path(self.fixture.task(identifier)['directory'])

    def clean_up_engine(self, identifier):
        task = self.fixture.task(identifier)
        engine = self.engine(identifier)
        del engine.tasks[task['engine_uuid']]
        return patch.object(engine, 'download', side_effect=EngineUnavailable('synthetic_terminal_cleanup'))

    def assert_job_succeeded(self, identifier):
        task = self.fixture.task(identifier)
        self.assertEqual((task['job']['status'], task['stage'], task['phase']), ('succeeded', 'succeeded', 'complete'))
        self.assertIn(task['error'], (None, CUSTODY_INTENT))  # The intent is no failure, and info never shows it.
        self.assertEqual(self.service.info(identifier)['status'], {'code': 40})

    def assert_no_half_archive(self, identifier):
        names = {path.name for path in self.directory(identifier).iterdir()}
        self.assertFalse(names & {'all.zip', 'all.zip.receipt', 'all.zip.partial'}, names)
        self.assertIsNone(self.fixture.task(identifier)['archive_path'])

    def test_outage_after_completion_keeps_the_result(self):
        identifier = self.finished()
        engine = self.engine(identifier)
        expected = hashlib.sha256(engine.archive).hexdigest()
        self.assertEqual(self.custody(identifier), {'state': 'verified'})
        self.assertEqual(engine.download_calls, 1)
        record = self.record(identifier)
        self.assertEqual(record['archive'], {'bytes': len(engine.archive), 'sha256': expected})
        self.assert_job_succeeded(identifier)
        with self.clean_up_engine(identifier):
            stream, length = self.service.download(identifier)
            with stream:
                self.assertEqual(hashlib.sha256(stream.read()).hexdigest(), expected)
                self.assertEqual(stream.ledgetop_archive_sha256, expected)
            self.assertEqual(length, len(engine.archive))
        self.assertEqual(engine.download_calls, 1)
        self.assertFalse({call[0] for call in engine.calls} - {'initialize', 'upload', 'commit', 'read', 'download'})

    def test_transfer_runs_outside_the_service_lock(self):
        identifier = self.finished(wait=False)
        engine = self.engine(identifier)
        blocked = Blocked(engine)
        self.addCleanup(blocked.undo)
        self.service.tick()
        self.assertTrue(blocked.reading.wait(10))
        self.assertEqual(self.custody(identifier), {'state': 'fetching'})
        acquired = self.service._lock.acquire(timeout=5)
        self.assertTrue(acquired)
        self.service._lock.release()
        self.service.tick()  # The scheduler keeps turning.
        blocked.undo()
        self.join()
        self.assertEqual(self.custody(identifier), {'state': 'verified'})

    def test_restart_during_a_fetch_resumes(self):
        identifier = self.finished(wait=False)
        engine = self.engine(identifier)
        blocked = Blocked(engine)
        self.addCleanup(blocked.undo)
        self.service.tick()
        self.assertTrue(blocked.reading.wait(10))
        stopping = threading.Thread(target=self.service.close)
        stopping.start()
        blocked.undo()
        stopping.join(10)
        self.fixture.queue.close()
        self.fixture.make_queue_service()
        self.assert_no_half_archive(identifier)
        self.assertEqual(self.record(identifier)['state'], 'pending')
        self.assertEqual(self.custody(identifier), {'state': 'pending'})
        self.service.tick()
        self.join()
        self.assertEqual(self.custody(identifier), {'state': 'verified'})
        self.assertEqual(engine.download_calls, 2)

    def test_kill_mid_transfer_leaves_nothing_verified_and_resumes(self):
        identifier = self.finished(wait=False)
        engine = self.engine(identifier)
        # What SIGKILL leaves: intent and a truncated partial, no receipt.
        engine.download, original = (lambda _: (_ for _ in ()).throw(EngineUnavailable('synthetic'))), engine.download
        self.service.tick()
        self.join()
        engine.download = original
        directory = self.directory(identifier)
        self.assertTrue((directory / 'all.zip.intent').exists())
        (directory / 'all.zip.partial').write_bytes(engine.archive[:9])
        self.restart()
        self.assertEqual(self.custody(identifier)['state'], 'pending')
        with self.assertRaises(FileNotFoundError):
            (directory / 'all.zip.receipt').lstat()
        self.service.tick()
        self.join()
        self.assertEqual(self.custody(identifier), {'state': 'verified'})
        self.assertEqual((directory / 'all.zip').read_bytes(), engine.archive)
        self.assertFalse((directory / 'all.zip.partial').exists())

    def test_kill_after_the_receipt_publishes_those_bytes_without_the_engine(self):
        identifier = self.finished(wait=False)
        engine = self.engine(identifier)
        original = TaskStore.save_custody
        calls = []

        def crash_on_verified(task, record):
            if record['state'] == 'verified' and not calls:
                calls.append(record)
                raise OSError('synthetic crash before the record')
            original(task, record)

        with patch.object(TaskStore, 'save_custody', staticmethod(crash_on_verified)):
            self.service.tick()
            self.join()
        self.assertEqual(self.record(identifier)['state'], 'pending')
        kept = (self.directory(identifier) / 'all.zip').read_bytes()
        engine.archive = b'PK\x05\x06' + bytes(18)  # A later, different result.
        self.restart()
        self.service.tick()
        self.join()
        self.assertEqual(self.custody(identifier), {'state': 'verified'})
        self.assertEqual(engine.download_calls, 1)  # A verified archive is never replaced.
        self.assertEqual((self.directory(identifier) / 'all.zip').read_bytes(), kept)

    def test_kill_between_receipt_and_link_publishes_those_bytes(self):
        identifier = self.finished(wait=False)
        engine = self.engine(identifier)
        original = os.link

        def link(source, target, **kwargs):
            if Path(target).name == 'all.zip':
                raise OSError('synthetic kill before the link')
            return original(source, target, **kwargs)

        with patch('processing.viewer_service.os.link', link):
            self.service.tick()
            self.join()
        directory = self.directory(identifier)
        self.assertEqual(sorted(path.name for path in directory.iterdir() if path.name.startswith('all.zip')),
                         ['all.zip.intent', 'all.zip.partial', 'all.zip.receipt'])
        self.assertEqual(self.record(identifier)['state'], 'pending')
        self.restart()
        self.service.tick()
        self.join()
        self.assertEqual(self.custody(identifier), {'state': 'verified'})
        self.assertEqual((directory / 'all.zip').read_bytes(), engine.archive)
        self.assertEqual((directory / 'all.zip').lstat().st_nlink, 1)
        self.assertFalse((directory / 'all.zip.partial').exists())
        self.assertEqual(engine.download_calls, 1)

    def test_partial_never_replaces_a_verified_archive(self):
        identifier = self.finished()
        engine = self.engine(identifier)
        directory = self.directory(identifier)
        kept = (directory / 'all.zip').read_bytes()
        (directory / 'all.zip.partial').write_bytes(b'a stale interrupted transfer')
        record = self.record(identifier)
        TaskStore.save_custody(self.fixture.task(identifier), {**record, 'state': 'pending', 'archive': None})
        with self.fixture.queue._db() as db:  # As a crash before the verified record leaves it.
            db.execute('UPDATE viewer_tasks SET error=? WHERE uuid=?', (CUSTODY_INTENT, identifier))
        self.restart()
        self.service.tick()
        self.join()
        self.assertEqual(self.custody(identifier), {'state': 'verified'})
        self.assertEqual((directory / 'all.zip').read_bytes(), kept)
        self.assertEqual(engine.download_calls, 1)

    def test_corrupt_zip_is_refused_and_retried(self):
        identifier = self.finished(wait=False)
        engine = self.engine(identifier)
        good = engine.archive
        bad = bytearray(good)
        bad[bad.index(b'owned synthetic')] ^= 0xff  # The directory stays readable.
        engine.archive = bytes(bad)
        self.assertTrue(zipfile.is_zipfile(io.BytesIO(engine.archive)))
        self.service.tick()
        self.join()
        self.assert_no_half_archive(identifier)
        self.assertEqual(self.custody(identifier), {'state': 'pending', 'reason': 'archive_fetch_failed'})
        self.assert_job_succeeded(identifier)
        self.assertGreater(self.service._eager_due[identifier], time.monotonic())
        self.service.tick()
        self.join()
        self.assertEqual(engine.download_calls, 1)  # Backing off.
        engine.archive = good
        self.settle()
        self.assertEqual(self.custody(identifier), {'state': 'verified'})
        self.assertEqual((self.directory(identifier) / 'all.zip').read_bytes(), good)
        self.assertEqual(engine.download_calls, 2)

    def test_result_changing_during_the_fetch_is_refused(self):
        identifier = self.finished(wait=False)
        engine = self.engine(identifier)
        task = self.fixture.task(identifier)
        engine_task = engine.tasks[task['engine_uuid']]
        engine_task['processingTime'] = 5000
        original = engine.download

        def download(task_id):
            response = original(task_id)
            engine_task['processingTime'] = 9000
            return response

        engine.download = download
        self.service.tick()
        self.join()
        self.assert_no_half_archive(identifier)
        self.assertEqual(self.custody(identifier), {'state': 'refused', 'reason': RESULT_CHANGED})
        self.assert_job_succeeded(identifier)
        self.settle()
        self.assertEqual(engine.download_calls, 1)  # Latched, and Viewer is unaffected.
        with self.service.download(identifier)[0] as stream:
            self.assertEqual(stream.read(), engine.archive)

    def test_result_changing_before_the_fetch_is_refused_without_a_transfer(self):
        identifier = self.finished(wait=False)
        task = self.fixture.task(identifier)
        engine = self.engine(identifier)
        with patch.object(self.service, '_advance_eager_custody', lambda: None):
            self.service.tick()
        engine.tasks[task['engine_uuid']]['status']['code'] = 20  # A stale tasks.json reran it.
        self.settle()
        self.assertEqual(self.custody(identifier), {'state': 'refused', 'reason': RESULT_CHANGED})
        self.assertEqual(engine.download_calls, 0)

    def test_insufficient_disk_defers_then_recovers(self):
        identifier = self.finished(wait=False)
        engine = self.engine(identifier)
        usage = lambda free: patch('processing.viewer_service.shutil.disk_usage', lambda path: SimpleNamespace(free=free))
        with usage(0):
            self.service.tick()
            self.join()
            self.assertEqual(self.custody(identifier), {'state': 'deferred', 'reason': 'insufficient_scratch'})
            self.assertEqual(engine.download_calls, 0)
            self.assert_no_half_archive(identifier)
            self.assert_job_succeeded(identifier)
            self.assertAlmostEqual(self.service._eager_due[identifier] - time.monotonic(), EAGER_DEFERRED_RETRY_SECONDS, delta=5)
            self.assertNotIn(identifier, self.service._eager_failures)
        self.restart()  # A deferred fetch survives a restart too.
        with usage(1 + len(engine.archive) - 1):
            self.service.tick()
            self.join()
            # Room for the reserve, not for the archive the engine declared.
            self.assertEqual(self.custody(identifier), {'state': 'deferred', 'reason': 'insufficient_scratch'})
            self.assert_no_half_archive(identifier)
        with usage(1 + len(engine.archive)):
            self.settle()
        self.assertEqual(self.custody(identifier), {'state': 'verified'})

    def test_free_space_is_read_where_the_archive_lives_and_held_to_the_reserve(self):
        identifier = self.finished(wait=False)
        size = len(self.engine(identifier).archive)
        seen = []

        def probe(free):
            return patch('processing.viewer_service.shutil.disk_usage',
                         lambda path: seen.append(str(path)) or SimpleNamespace(free=free))

        self.service._eager_reserve = 1000
        with probe(999):  # Below the reserve before a byte moves.
            self.service.tick()
            self.join()
        self.assertEqual(self.custody(identifier), {'state': 'deferred', 'reason': 'insufficient_scratch'})
        self.assertEqual(self.engine(identifier).download_calls, 0)
        self.assertEqual(set(seen), {str(self.directory(identifier))})
        self.assertEqual(self.service.eager_custody_status()['lastFree']['bytes'], 999)
        with probe(1000 + size):  # Exactly enough to keep the reserve afterwards.
            self.settle()
        self.assertEqual(self.custody(identifier), {'state': 'verified'})
        self.assertEqual(self.service.eager_custody_status()['lastFree']['bytes'], 1000 + size)

    def test_an_unreadable_filesystem_defers(self):
        identifier = self.finished(wait=False)
        with patch('processing.viewer_service.shutil.disk_usage', side_effect=OSError('synthetic statvfs failure')):
            self.service.tick()
            self.join()
        self.assertEqual(self.custody(identifier), {'state': 'deferred', 'reason': 'free_space_unavailable'})
        self.assertEqual(self.engine(identifier).download_calls, 0)
        self.settle()
        self.assertEqual(self.custody(identifier), {'state': 'verified'})

    def test_status_reports_the_policy_and_what_is_owed(self):
        with patch.object(self.service, '_advance_eager_custody', lambda: None):
            self.finished()
        status = self.service.eager_custody_status()
        self.assertEqual(set(status['lastFree']), {'bytes', 'observedAt'})
        self.assertEqual({key: status[key] for key in status if key != 'lastFree'},
                         {'reserveBytes': 1, 'maxBytes': 1024 ** 3, 'fileLimitBytes': CENSUS_RETAINED_FILE_BYTES,
                          'pendingTasks': 1, 'resumeScanned': False, 'next': None, 'tasks': status['tasks']})
        self.assertEqual(len(status['tasks']), 1)

    def test_status_route_exists_only_with_the_flag(self):
        startup = startup_fixture.StartupTests('test_invalid_values_refuse')
        startup.setUp()
        self.addCleanup(startup.doCleanups)
        startup.state.mkdir()

        def read(sizes):
            with patch.dict(os.environ, {**startup.env, **sizes}, clear=True):
                handler = make_handler(from_environment())
            server = start_server(handler)
            try:
                request = Request('http://127.0.0.1:%d/v1/prototype/result-custody' % server.server_port,
                                  headers={'Authorization': 'Bearer ' + startup.env['PROCESSING_API_TOKEN']})
                try:
                    with urlopen(request, timeout=5) as response:
                        return response.status, json.load(response)
                except HTTPError as error:
                    return error.code, None
            finally:
                server.shutdown()
                server.server_close()
                handler.viewer_service.close()
                handler.prototype_queue.close()

        self.assertEqual(read({})[0], 404)
        status, body = read({'PROCESSING_VIEWER_EAGER_RESULT_CUSTODY': '1',
                             'PROCESSING_VIEWER_EAGER_CUSTODY_RESERVE_BYTES': '7',
                             'PROCESSING_VIEWER_EAGER_CUSTODY_MAX_BYTES': '9'})
        self.assertEqual(status, 200)
        self.assertEqual((body['resultCustody']['reserveBytes'], body['resultCustody']['maxBytes']), (7, 9))
        self.assertGreater(body['resultCustody']['lastFree']['bytes'], 0)

    def test_disk_filling_during_the_transfer_defers_and_frees_the_partial(self):
        identifier = self.finished(wait=False)
        engine = self.engine(identifier)
        original = engine.download

        def undeclared(task_id):
            response = original(task_id)
            del response.headers['Content-Length']
            return response

        engine.download = undeclared
        free = iter([10 ** 12, 10 ** 12])  # Before the request, and at its headers.
        with patch('processing.viewer_service.shutil.disk_usage',
                   lambda path: SimpleNamespace(free=next(free, 0))):
            self.service.tick()
            self.join()
        self.assertEqual(self.custody(identifier), {'state': 'deferred', 'reason': 'insufficient_scratch'})
        self.assert_no_half_archive(identifier)
        self.settle()
        self.assertEqual(self.custody(identifier), {'state': 'verified'})

    def test_stale_partial_counts_as_room_it_will_free(self):
        identifier = self.finished(wait=False)
        engine = self.engine(identifier)
        with patch.object(self.service, '_advance_eager_custody', lambda: None):
            self.service.tick()
        self.service._eager_due.clear()
        directory = self.directory(identifier)
        task = self.fixture.task(identifier)
        self.service._publish_archive_metadata(directory / 'all.zip.intent', self.service._archive_identity(task))
        (directory / 'all.zip.partial').write_bytes(b'x' * 1000)
        partial = directory / 'all.zip.partial'
        with patch.object(self.service, '_eager_reserve', 1000), patch(
                'processing.viewer_service.shutil.disk_usage',
                lambda path: SimpleNamespace(free=500 if partial.exists() and partial.stat().st_size == 1000 else 10 ** 6)):
            self.service._eager_due[identifier] = 0
            self.service.tick()
            self.join()
        self.assertEqual(self.custody(identifier), {'state': 'verified'})
        self.assertEqual(engine.download_calls, 1)

    def test_total_custody_bytes_are_bounded_and_nothing_is_deleted(self):
        first = self.finished()
        size = (self.directory(first) / 'all.zip').stat().st_size
        self.service._eager_max_bytes = size + 1
        second = self.finished()
        engine = self.engine(second)
        self.assertEqual(self.custody(second), {'state': 'deferred', 'reason': 'custody_budget_exceeded'})
        self.assert_no_half_archive(second)
        self.assertTrue((self.directory(first) / 'all.zip').exists())
        self.assertEqual(self.custody(first), {'state': 'verified'})
        self.service._eager_max_bytes = 2 * size
        self.settle()
        self.assertEqual(self.custody(second), {'state': 'verified'})
        self.assertEqual(self.record(first)['state'], 'verified')
        self.assertLessEqual(self.service._eager_custody_bytes(), 2 * size)
        self.assertGreaterEqual(engine.download_calls, 1)

    def test_archive_over_the_census_file_limit_is_not_fetched(self):
        identifier = self.finished(wait=False)
        engine = self.engine(identifier)
        original = engine.download
        self.fixture.config.viewer_max_archive_bytes = 10 * CENSUS_RETAINED_FILE_BYTES

        def oversized(task_id):
            response = original(task_id)
            response.headers['Content-Length'] = str(CENSUS_RETAINED_FILE_BYTES + 1)
            return response

        engine.download = oversized
        self.service.tick()
        self.join()
        self.assertEqual(self.custody(identifier), {'state': 'refused', 'reason': 'archive_exceeds_census_limit'})
        self.assert_no_half_archive(identifier)
        self.assert_job_succeeded(identifier)

    def test_viewer_download_during_a_fetch_waits_for_those_bytes(self):
        identifier = self.finished(wait=False)
        engine = self.engine(identifier)
        blocked = Blocked(engine)
        self.addCleanup(blocked.undo)
        self.service.tick()
        self.assertTrue(blocked.reading.wait(10))
        results = []

        def viewer():
            stream, length = self.service.download(identifier)
            with stream:
                results.append((stream.read(), length))

        downloading = threading.Thread(target=viewer)
        downloading.start()
        downloading.join(0.2)
        self.assertTrue(downloading.is_alive())
        blocked.undo()
        downloading.join(10)
        self.join()
        self.assertEqual(results, [(engine.archive, len(engine.archive))])
        self.assertEqual(engine.download_calls, 1)
        self.assertEqual(self.custody(identifier), {'state': 'verified'})

    def test_viewer_download_before_the_fetch_is_recorded_without_a_second_transfer(self):
        identifier = self.finished(wait=False)
        engine = self.engine(identifier)
        with patch.object(self.service, '_advance_eager_custody', lambda: None):
            self.service.tick()
        with self.service.download(identifier)[0]:
            pass
        self.assertEqual(self.custody(identifier), {'state': 'pending', 'retained': True})
        self.service.tick()
        self.join()
        self.assertEqual(self.custody(identifier), {'state': 'verified'})
        self.assertEqual(engine.download_calls, 1)

    def test_one_fetch_at_a_time(self):
        self.assertEqual(EAGER_FETCH_CONCURRENCY, 1)
        first = self.finished(wait=False)
        engine = self.engine(first)
        blocked = Blocked(engine)
        self.addCleanup(blocked.undo)
        self.service.tick()
        self.assertTrue(blocked.reading.wait(10))
        fleet = [first]
        for _ in range(2):
            identifier = self.fixture.submit()
            self.service.tick()
            self.fixture.finish(identifier)
            self.service.tick()
            fleet.append(identifier)
            self.assertEqual(len(self.service._eager_threads), 1)
        states = [self.custody(identifier)['state'] for identifier in fleet]
        self.assertEqual(states, ['fetching', 'pending', 'pending'])
        blocked.undo()
        for _ in fleet:
            self.join()
            self.service.tick()
            self.assertLessEqual(len(self.service._eager_threads), 1)
        self.join()
        self.assertEqual([self.custody(identifier) for identifier in fleet], [{'state': 'verified'}] * 3)

    def test_fetch_failure_never_changes_the_job_and_viewer_still_downloads(self):
        identifier = self.finished(wait=False)
        engine = self.engine(identifier)
        engine.interrupt_download_once = True
        self.service.tick()
        self.join()
        self.assert_job_succeeded(identifier)
        self.assertEqual(self.custody(identifier), {'state': 'pending', 'reason': 'archive_fetch_failed'})
        self.assert_no_half_archive(identifier)
        with self.service.download(identifier)[0] as stream:
            self.assertEqual(stream.read(), engine.archive)
        self.settle()
        self.assertEqual(self.custody(identifier), {'state': 'verified'})
        self.assertEqual(engine.download_calls, 2)

    def test_recording_failure_at_completion_never_changes_the_job(self):
        identifier = self.fixture.submit()
        self.service.tick()
        self.fixture.finish(identifier)
        original = TaskStore.save_custody
        calls = []

        def fail_once(task, record):
            calls.append(record)
            if len(calls) == 1:
                raise OSError('synthetic full disk')
            original(task, record)

        with patch.object(TaskStore, 'save_custody', staticmethod(fail_once)):
            self.service.tick()
            self.join()
        self.assert_job_succeeded(identifier)
        self.assertEqual(self.custody(identifier), {'state': 'verified'})

    def test_engine_answering_missing_keeps_retrying_and_can_return(self):
        identifier = self.finished(wait=False)
        task = self.fixture.task(identifier)
        engine = self.engine(identifier)
        with patch.object(self.service, '_advance_eager_custody', lambda: None):
            self.service.tick()
        kept = engine.tasks.pop(task['engine_uuid'])
        for attempt in range(ENGINE_MISSING_POLLS + 3):
            self.settle()
            self.assertEqual(self.custody(identifier), {'state': 'pending', 'reason': 'engine_task_missing'})
        self.assertIn(identifier, self.service._eager_due)
        self.assert_job_succeeded(identifier)
        engine.tasks[task['engine_uuid']] = kept  # An engine back from an outage.
        self.settle()
        self.assertEqual(self.custody(identifier), {'state': 'verified'})

    def test_unreachable_node_retries_until_it_answers(self):
        identifier = self.finished(wait=False)
        engine = self.engine(identifier)
        with patch.object(self.service, '_advance_eager_custody', lambda: None):
            self.service.tick()
        engine.read_unavailable = True
        for _ in range(ENGINE_MISSING_POLLS + 1):
            self.settle()
        self.assertEqual(self.custody(identifier), {'state': 'pending', 'reason': 'archive_fetch_failed'})
        engine.read_unavailable = False
        self.settle()
        self.assertEqual(self.custody(identifier), {'state': 'verified'})

    def gate(self):
        gate = AdmissionDrain(self.fixture.queue)
        self.fixture.queue.admission_drain = gate
        return gate

    def close_gate(self, gate, value=True):
        status = gate.status()
        return gate.set_closed(value, request_id=str(uuid.uuid4()), expected_generation=status['generation'],
                               expected_closed=status['closed'], expected_authority=status['authorityId'])

    def test_fetch_does_not_block_admissions_and_a_drain_stops_it(self):
        gate = self.gate()
        identifier = self.finished(wait=False)
        engine = self.engine(identifier)
        blocked = Blocked(engine)
        self.addCleanup(blocked.undo)
        self.service.tick()
        self.assertTrue(blocked.reading.wait(10))
        admitted = []

        def admission():
            with gate.admit():
                admitted.append(True)

        other = threading.Thread(target=admission)
        other.start()
        other.join(5)
        self.assertEqual(admitted, [True])  # The transfer is still in progress.
        closed = []
        closing = threading.Thread(target=lambda: closed.append(self.close_gate(gate)))
        closing.start()
        closing.join(0.2)
        self.assertTrue(closing.is_alive())  # Nothing durable until the fetch left.
        self.assertFalse(gate.status()['closed'])
        with self.assertRaises(AdmissionClosed):
            with gate.admit():
                pass
        blocked.undo()
        closing.join(10)
        self.join()
        self.assertTrue(closed and gate.status()['closed'])
        self.assert_no_half_archive(identifier)
        before = sorted((path.name, path.stat().st_mtime_ns) for path in self.directory(identifier).iterdir())
        self.settle()  # A closed gate starts nothing, and this is not a failure.
        self.assertEqual(engine.download_calls, 1)
        self.assertEqual(self.custody(identifier), {'state': 'pending', 'reason': 'admission_closed'})
        self.assertEqual(self.service._eager_failures, {})
        self.assertEqual(sorted((path.name, path.stat().st_mtime_ns) for path in self.directory(identifier).iterdir()), before)
        self.close_gate(gate, False)
        self.settle()
        self.assertEqual(self.custody(identifier), {'state': 'verified'})

    def test_drain_stops_a_member_check_at_its_next_read(self):
        gate = self.gate()
        identifier = self.finished(wait=False)
        original = service_module._check_archive_members
        checking, release = threading.Event(), threading.Event()

        def paused(path, stop=None):
            checking.set()
            self.assertTrue(release.wait(10))
            original(path, stop)

        with patch.object(service_module, '_check_archive_members', paused):
            self.service.tick()
            self.assertTrue(checking.wait(10))
            closing = threading.Thread(target=self.close_gate, args=(gate,))
            closing.start()
            while not gate._closing.is_set():
                time.sleep(0.01)
            release.set()
            closing.join(10)
            self.join()
        self.assertTrue(gate.status()['closed'])
        self.assert_no_half_archive(identifier)

    def test_guarded_mode_and_production_hold_wait_without_failures(self):
        identifier = self.finished(wait=False)
        engine = self.engine(identifier)
        with patch.object(self.service, '_advance_eager_custody', lambda: None):
            self.service.tick()
        self.service._require_guard = True
        self.service._guarded_engine_factory = object
        self.service._guard_probe = lambda node: False
        self.settle()
        self.assertEqual(self.custody(identifier), {'state': 'pending', 'reason': 'guard_unavailable'})
        self.service._require_guard = False
        self.service._guarded_engine_factory = None
        self.fixture.config.viewer_production_held = True
        self.settle()
        self.assertEqual(self.custody(identifier), {'state': 'pending', 'reason': 'production_held'})
        self.fixture.config.viewer_production_held = False
        self.assertEqual((engine.download_calls, self.service._eager_failures), (0, {}))
        self.settle()
        self.assertEqual(self.custody(identifier), {'state': 'verified'})

    def test_guarded_reads_use_the_task_client_as_the_held_fetch_does(self):
        identifier = self.finished(wait=False)
        engine = self.engine(identifier)
        with patch.object(self.service, '_advance_eager_custody', lambda: None):
            self.service.tick()
        used = []
        original = self.service._task_engine
        self.service._task_engine = lambda task: (used.append(task['uuid']), original(task))[1]
        self.settle()
        self.assertEqual(self.custody(identifier), {'state': 'verified'})
        self.assertEqual(used, [identifier] * 3)  # Status, transfer, status.
        self.assertEqual(engine.download_calls, 1)

    def test_tasks_from_before_the_flag_are_not_backfilled(self):
        self.fixture.config.viewer_eager_result_custody = False
        self.restart()
        identifier = self.finished()
        engine = self.engine(identifier)
        self.fixture.config.viewer_eager_result_custody = True
        self.restart()
        self.service.tick()
        self.join()
        self.assertEqual(engine.download_calls, 0)
        self.assertNotIn('ledgetopResultCustody', self.service.info(identifier))
        self.assertFalse((self.directory(identifier) / CUSTODY_NAME).exists())

    def engine_task(self, identifier):
        task = self.fixture.task(identifier)
        return self.engine(identifier).tasks[task['engine_uuid']]

    def assert_refused_and_lazy(self, identifier, reason):
        self.assertEqual(self.custody(identifier), {'state': 'refused', 'reason': reason})
        self.assertEqual(self.record(identifier)['archive'], None)
        self.assertIn(identifier, self.service.store.pending_custody())
        self.assert_job_succeeded(identifier)
        with self.service.download(identifier)[0] as stream:
            self.assertEqual(stream.read(), self.engine(identifier).archive)
        self.assertEqual(self.custody(identifier), {'state': 'refused', 'reason': reason, 'retained': True})

    def test_a_code_40_missing_any_evidence_binds_nothing(self):
        for field in ('dateCreated', 'imagesCount', 'options', 'processingTime'):
            with self.subTest(field):
                identifier = self.finished(wait=False)
                self.engine_task(identifier).pop(field, None)
                self.engine_task(identifier)['identity'].pop(field, None)
                self.service.tick()
                self.join()
                self.assertEqual(self.engine(identifier).download_calls, 0)
                self.assert_no_half_archive(identifier)
                self.assert_refused_and_lazy(identifier, IDENTITY_UNAVAILABLE)
                self.engine(identifier).download_calls = 0

    def test_a_code_40_disagreeing_with_the_submission_binds_nothing(self):
        for change in ({'imagesCount': 2}, {'options': [{'name': 'pc-ept', 'value': False}]}):
            with self.subTest(change):
                identifier = self.finished(wait=False)
                self.engine_task(identifier)['identity'].update(change)
                self.service.tick()
                self.join()
                self.assertEqual(self.engine(identifier).download_calls, 0)
                self.assert_refused_and_lazy(identifier, IDENTITY_MISMATCH)
                self.engine(identifier).download_calls = 0

    def test_evidence_vanishing_during_the_transfer_publishes_nothing(self):
        identifier = self.finished(wait=False)
        engine = self.engine(identifier)
        original = engine.download

        def download(task_id):
            del engine.tasks[task_id]['identity']['dateCreated']
            return original(task_id)

        engine.download = download
        self.service.tick()
        self.join()
        self.assert_no_half_archive(identifier)
        self.assertEqual(self.custody(identifier), {'state': 'refused', 'reason': IDENTITY_UNAVAILABLE})
        self.assertFalse(self.record(identifier)['checked'])

    def test_a_verified_record_holds_a_complete_checked_binding(self):
        identifier = self.finished()
        record = self.record(identifier)
        self.assertEqual((record['state'], record['checked']), ('verified', True))
        self.assertTrue(all(value is not None for value in record['result'].values()))

    def test_viewer_bytes_are_verified_only_if_the_same_run_is_still_reported(self):
        identifier = self.finished(wait=False)
        with patch.object(self.service, '_advance_eager_custody', lambda: None):
            self.service.tick()
        with self.service.download(identifier)[0]:
            pass
        self.engine_task(identifier)['processingTime'] = 9000
        self.service.tick()
        self.join()
        self.assertEqual(self.custody(identifier), {'state': 'refused', 'reason': RESULT_CHANGED, 'retained': True})
        self.assertEqual((self.directory(identifier) / 'all.zip').read_bytes(), self.engine(identifier).archive)

    def test_viewer_bytes_with_unreadable_members_are_not_verified(self):
        identifier = self.finished(wait=False)
        engine = self.engine(identifier)
        with patch.object(self.service, '_advance_eager_custody', lambda: None):
            self.service.tick()
        with patch.object(service_module, '_check_archive_members', side_effect=EngineUnavailable('synthetic_bad_member')):
            with self.service.download(identifier)[0]:
                pass
            self.service.tick()
            self.join()
        self.assertEqual(self.custody(identifier), {'state': 'refused', 'reason': 'archive_members_unreadable', 'retained': True})
        self.assertEqual((self.directory(identifier) / 'all.zip').read_bytes(), engine.archive)

    def test_adoption_without_a_binding_needs_every_field_the_engine_reports(self):
        fixture = self.fixture
        for field in ('options', 'dateCreated'):
            with self.subTest(field):
                identifier, engine, engine_task = fixture.adoptable(bind=False)
                self.assertIsNone(self.service.store.adoption(fixture.task(identifier)))
                fixture.report_finished(identifier, engine_task)
                del engine_task['identity'][field]
                calls = engine.download_calls
                fixture.observe()
                self.assertEqual(fixture.custody(identifier), {'state': 'refused', 'reason': IDENTITY_UNAVAILABLE})
                self.assertEqual(engine.download_calls, calls)
                fixture.service.operator(fixture.task(identifier)['job_id'], 'resolve', fixture.OPERATOR)
                self.service.tick()

    def test_every_one_of_a_thousand_finished_tasks_resumes_after_a_restart(self):
        with patch.object(self.service, '_advance_eager_custody', lambda: None):
            finished = [self.finished() for _ in range(1000)]
        self.assertEqual(self.service.store.pending_custody(), finished)
        self.restart()
        self.service.tick()
        self.join()
        self.assertEqual(len(self.service._eager_due) + 1, 1000)  # All found at once; one has already ended.
        while self.service._eager_due:
            self.service.tick()
            self.join()
        self.assertEqual(self.service.store.pending_custody(), [])
        self.assertEqual({self.record(identifier)['state'] for identifier in finished}, {'verified'})
        self.assertEqual(sum(engine.download_calls for engine in self.fixture.engines.values()), 1000)
        self.restart()  # Settled work stays settled.
        self.service.tick()
        self.assertFalse(self.service._eager_due)

    def test_a_failed_resume_scan_repeats_rather_than_skipping_work(self):
        identifier = self.finished(wait=False)
        with patch.object(self.service, '_advance_eager_custody', lambda: None):
            self.service.tick()
        self.restart()
        with patch.object(TaskStore, 'pending_custody', side_effect=OSError('synthetic read failure')):
            self.service.tick()
        self.assertFalse(self.service._eager_due)
        self.service.tick()
        self.join()
        self.assertEqual(self.custody(identifier), {'state': 'verified'})

    def test_an_unwritten_record_stays_visible_across_a_restart_and_is_retried(self):
        identifier = self.fixture.submit()
        self.service.tick()
        self.fixture.finish(identifier)
        unwritable = patch.object(TaskStore, 'save_custody', side_effect=OSError('synthetic full disk'))
        unwritten = {'state': 'pending', 'reason': 'custody_record_unwritten'}
        with unwritable:
            self.service.tick()
            self.join()
            self.assert_job_succeeded(identifier)
            self.assertEqual(self.custody(identifier), unwritten)
            self.assertEqual(self.service.store.pending_custody(), [identifier])
            self.restart()
            self.assertEqual(self.custody(identifier), unwritten)
            self.settle()
            self.assertEqual(self.custody(identifier), unwritten)
        with self.service.download(identifier)[0]:  # Viewer's lazy path never waited for any of this.
            pass
        self.settle()
        self.assertEqual(self.engine(identifier).download_calls, 1)
        # Those bytes came before any record of the run, so nothing vouches for them.
        self.assertEqual(self.custody(identifier), {'state': 'refused', 'reason': 'result_bound_too_late', 'retained': True})
        self.assertEqual(self.service.store.pending_custody(), [identifier])

    @unittest.skipIf(os.name == 'nt', 'needs SIGKILL')
    def test_a_kill_right_after_the_commit_cannot_lose_the_intent(self):
        report = self.child(KILLED_AFTER_COMMIT)
        self.assertEqual(report['code'], -9)
        self.adopt_child_state(report)
        identifier = report['identifier']
        self.assertFalse((self.directory(identifier) / CUSTODY_NAME).exists())
        self.assertEqual(self.custody(identifier), {'state': 'pending', 'reason': 'custody_record_unwritten'})
        self.service.tick()
        self.join()
        self.assertEqual(self.custody(identifier), {'state': 'verified'})

    def test_invalid_record_is_reported_and_left_for_inspection(self):
        identifier = self.finished(wait=False)
        with patch.object(self.service, '_advance_eager_custody', lambda: None):
            self.service.tick()
        path = self.directory(identifier) / CUSTODY_NAME
        path.write_text('{"state":"verified"}')
        self.settle()
        self.assertEqual(self.custody(identifier), {'state': 'refused', 'reason': 'custody_record_invalid'})
        self.assertEqual(path.read_text(), '{"state":"verified"}')
        self.assertEqual(self.engine(identifier).download_calls, 0)

    def test_unsafe_partial_stops_at_inspection_and_is_kept(self):
        identifier = self.finished(wait=False)
        with patch.object(self.service, '_advance_eager_custody', lambda: None):
            self.service.tick()
        partial = self.directory(identifier) / 'all.zip.partial'
        partial.write_bytes(b'no intent was recorded for this')
        self.settle()
        self.assertEqual(self.custody(identifier), {'state': 'refused', 'reason': 'archive_requires_inspection'})
        self.assertTrue(partial.exists())

    def entry(self, identifier):
        return next((item for item in self.listing() if item['taskId'] == identifier), None)

    def listing(self, **page):
        status = self.service.eager_custody_status(**page)
        items = status['tasks']
        while status['next'] is not None:
            status = self.service.eager_custody_status(**{**page, 'after': status['next']})
            items = items + status['tasks']
        return items

    def act(self, identifier, action, note='synthetic note'):
        return self.service.custody_action(self.job(identifier), action, {'note': note})

    def job(self, identifier):
        return self.service.store.custody_task(identifier)['job_id']

    def attempt(self, identifier):
        """One attempt for this task alone, whatever else is due."""
        self.service._take_eager_custody(identifier, None)

    def owed(self, *, read_unavailable=False):
        """A finished job whose fetch has not started."""
        identifier = self.finished(wait=False)
        with patch.object(self.service, '_advance_eager_custody', lambda: None):
            self.service.tick()
        self.engine(identifier).read_unavailable = read_unavailable
        return identifier

    def test_remove_while_owed_keeps_custody_and_completes_it(self):
        identifier = self.owed()
        engine, directory = self.engine(identifier), self.directory(identifier)
        self.assertEqual(self.service.remove(identifier), {'success': True, 'uuid': identifier})
        with self.assertRaises(KeyError):
            self.service.info(identifier)  # Viewer's view of it is exactly as before.
        self.assertEqual({key: self.entry(identifier)[key] for key in ('state', 'removed')}, {'state': 'pending', 'removed': True})
        self.settle()
        self.assertEqual(json.loads((directory / CUSTODY_NAME).read_text())['state'], 'verified')
        self.assertEqual((directory / 'all.zip').read_bytes(), engine.archive)
        self.assertEqual(self.service.store.pending_custody(), [])

    def test_remove_during_a_fetch_lets_it_finish(self):
        identifier = self.finished(wait=False)
        directory = self.directory(identifier)
        blocked = Blocked(self.engine(identifier))
        self.addCleanup(blocked.undo)
        self.service.tick()
        self.assertTrue(blocked.reading.wait(10))
        self.service.remove(identifier)
        blocked.undo()
        self.join()
        self.assertEqual(json.loads((directory / CUSTODY_NAME).read_text())['state'], 'verified')
        self.assertEqual(self.service.store.custody_task(identifier)['archive_path'], str(directory / 'all.zip'))

    def test_removed_custody_survives_a_restart_and_an_outage(self):
        identifier = self.owed(read_unavailable=True)
        engine, directory = self.engine(identifier), self.directory(identifier)
        self.service.remove(identifier)
        self.settle()
        self.restart()
        self.assertEqual(self.service.store.pending_custody(), [identifier])
        self.assertEqual(self.entry(identifier)['reason'], 'archive_fetch_failed')
        engine.read_unavailable = False
        self.settle()
        self.service.tick()
        self.join()
        self.assertEqual((directory / 'all.zip').read_bytes(), engine.archive)

    def test_a_uuid_that_owes_custody_is_not_reset(self):
        identifier = self.owed()
        task = self.fixture.task(identifier)
        self.service.remove(identifier)
        with self.assertRaisesRegex(Conflict, 'still owes result custody'):
            self.service.store.initialize(identifier, task['name'], task['options'], task['outputs'], task['environment'])
        self.assertEqual(self.service.store.pending_custody(), [identifier])

    def test_abandon_records_who_when_why_and_repeats_harmlessly(self):
        identifier = self.owed(read_unavailable=True)
        engine = self.engine(identifier)
        self.settle()
        began = int(time.time())
        first = self.act(identifier, 'abandon', 'client confirmed a rerun')
        self.assertEqual((first['state'], first['reason']), ('abandoned', 'operator_abandoned'))
        recorded = self.record(identifier)['abandoned']
        self.assertEqual({key: recorded[key] for key in ('action', 'by', 'note')},
                         {'action': 'abandon', 'by': 'operator_credential', 'note': 'client confirmed a rerun'})
        self.assertEqual(recorded['from'], {'state': 'pending', 'reason': 'archive_fetch_failed'})
        self.assertGreaterEqual(recorded['at'], began)
        self.assertEqual(self.act(identifier, 'abandon', 'a different reason'), first)
        self.assertEqual(self.record(identifier)['abandoned'], recorded)
        self.assertEqual(len(self.record(identifier)['operator']), 1)
        self.assertNotIn(identifier, self.service._eager_due)
        engine.read_unavailable = False
        self.restart()
        self.service.tick()
        self.join()
        self.assertEqual(engine.download_calls, 0)
        self.assertEqual(self.custody(identifier), {'state': 'abandoned', 'reason': 'operator_abandoned'})
        self.assertEqual(self.entry(identifier)['abandoned'], recorded)  # Still listed, and nothing was deleted.
        self.assert_job_succeeded(identifier)
        with self.service.download(identifier)[0] as stream:  # Abandoning custody is not refusing Viewer.
            self.assertEqual(stream.read(), engine.archive)
        self.assertEqual(self.custody(identifier)['retained'], True)

    def test_a_mistaken_abandonment_can_be_retried(self):
        identifier = self.owed()
        self.act(identifier, 'abandon')
        self.act(identifier, 'retry', 'that was the wrong job')
        self.service.tick()
        self.join()
        self.assertEqual(self.custody(identifier), {'state': 'verified'})
        self.assertEqual([item['action'] for item in self.record(identifier)['operator']], ['abandon', 'retry'])
        self.assertIsNone(self.record(identifier)['abandoned'])

    def test_a_verified_result_has_nothing_to_abandon_or_retry(self):
        identifier = self.finished()
        for action in ('abandon', 'retry'):
            with self.assertRaisesRegex(Conflict, 'no result custody is owed'):
                self.service.custody_action(self.fixture.task(identifier)['job_id'], action, {'note': 'synthetic note'})
        self.assertEqual(self.custody(identifier), {'state': 'verified'})

    def test_operator_actions_need_a_bounded_note_only(self):
        identifier = self.owed()
        for request in ({}, {'note': ''}, {'note': 'x' * 257}, {'note': 7}, {'note': 'a\nb'}, {'note': 'caf\u00e9'},
                        {'note': 'x', 'by': 'someone'}, 'note'):
            with self.subTest(request), self.assertRaises(ValueError):
                self.service.custody_action(self.job(identifier), 'abandon', request)
        self.assertNotEqual(self.custody(identifier)['state'], 'abandoned')

    def test_an_operator_action_never_waits_on_a_transfer(self):
        identifier = self.finished(wait=False)
        blocked = Blocked(self.engine(identifier))
        self.addCleanup(blocked.undo)
        self.service.tick()
        self.assertTrue(blocked.reading.wait(10))
        with self.assertRaisesRegex(Conflict, 'transfer in progress'):
            self.act(identifier, 'abandon')
        blocked.undo()
        self.join()
        self.assertEqual(self.custody(identifier), {'state': 'verified'})

    def test_operator_actions_stop_at_a_closed_gate(self):
        gate = self.gate()
        identifier = self.owed()
        self.close_gate(gate)
        with self.assertRaises(AdmissionClosed):
            self.act(identifier, 'abandon')
        self.assertEqual(self.custody(identifier), {'state': 'pending'})

    def test_custody_routes_need_the_operator_credential_and_the_flag(self):
        startup = startup_fixture.StartupTests('test_invalid_values_refuse')
        startup.setUp()
        self.addCleanup(startup.doCleanups)
        startup.state.mkdir()
        startup.env['PROCESSING_PROTOTYPE_OPERATOR_TOKEN'] = 'o' * 32

        def post(sizes, token, path='/v1/prototype/jobs/%s/custody/abandon' % uuid.uuid4()):
            with patch.dict(os.environ, {**startup.env, **sizes}, clear=True):
                handler = make_handler(from_environment())
            server = start_server(handler)
            try:
                request = Request('http://127.0.0.1:%d%s' % (server.server_port, path), data=b'{"note":"synthetic"}',
                                  headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'})
                try:
                    with urlopen(request, timeout=5) as response:
                        return response.status
                except HTTPError as error:
                    return error.code
            finally:
                server.shutdown()
                server.server_close()
                handler.viewer_service.close()
                handler.prototype_queue.close()

        on = {'PROCESSING_VIEWER_EAGER_RESULT_CUSTODY': '1', 'PROCESSING_VIEWER_EAGER_CUSTODY_RESERVE_BYTES': '7',
              'PROCESSING_VIEWER_EAGER_CUSTODY_MAX_BYTES': '9'}
        self.assertEqual(post({}, startup.env['PROCESSING_API_TOKEN']), 404)
        self.assertEqual(post(on, startup.env['PROCESSING_API_TOKEN']), 401)
        self.assertEqual(post(on, 'o' * 32), 409)  # Authorized, and no such custody is owed.

    def test_no_failure_or_length_of_time_abandons_anything(self):
        identifier = self.owed(read_unavailable=True)
        for _ in range(30):
            self.settle()
        self.assertEqual(self.record(identifier)['abandoned'], None)
        self.assertEqual(self.custody(identifier)['state'], 'pending')

    def test_a_long_disconnection_keeps_retrying_at_a_capped_rate_then_succeeds(self):
        identifier = self.owed(read_unavailable=True)
        engine = self.engine(identifier)
        delays = []
        for attempt in range(40):
            self.settle()
            delays.append(self.service._eager_due[identifier] - time.monotonic())
            if attempt == 20:
                self.restart()  # An outage may outlive the coordinator too; the rate survives.
                self.assertEqual(self.custody(identifier), {'state': 'pending', 'reason': 'archive_fetch_failed'})
                self.service._eager_scanned = False
                self.service.tick()
                self.join()
                delays.append(self.service._eager_due[identifier] - time.monotonic())
        self.assertLess(delays[0], delays[3])
        self.assertLessEqual(max(delays), service_module.RESULT_FETCH_RETRY_MAX_SECONDS)
        self.assertGreater(min(delays[-10:]), service_module.RESULT_FETCH_RETRY_MAX_SECONDS - 60)
        self.assertGreaterEqual(self.record(identifier)['attempts'], 40)
        engine.read_unavailable = False
        self.settle()
        self.assertEqual(self.custody(identifier), {'state': 'verified'})
        self.assertEqual((self.directory(identifier) / 'all.zip').read_bytes(), engine.archive)

    def test_hard_refusal_waits_for_an_operator_then_retry_retains_the_result(self):
        identifier = self.owed()
        engine = self.engine(identifier)
        engine_task = self.engine_task(identifier)
        engine_task['processingTime'] = 9000
        self.settle()
        self.assertEqual(self.custody(identifier), {'state': 'refused', 'reason': RESULT_CHANGED})
        self.assertNotIn(identifier, self.service._eager_due)
        self.restart()
        self.service.tick()
        self.join()
        self.assertEqual(engine.download_calls, 0)  # Latched across a restart, and listed.
        self.assertEqual(self.entry(identifier)['reason'], RESULT_CHANGED)
        self.act(identifier, 'retry', 'checked the engine: same run')
        self.assertEqual(self.custody(identifier), {'state': 'pending'})
        self.service.tick()
        self.join()
        self.assertEqual(self.record(identifier)['result']['processingTime'], 9000)
        self.assertEqual((self.directory(identifier) / 'all.zip').read_bytes(), engine.archive)
        self.assertEqual(self.service.store.pending_custody(), [] if self.record(identifier)['state'] == 'verified' else [identifier])

    def test_retry_holds_bytes_that_remain_refused(self):
        identifier = self.finished(wait=False)
        self.engine_task(identifier)['identity']['imagesCount'] = 2
        self.service.tick()
        self.join()
        self.assert_refused_without_bytes(identifier, IDENTITY_MISMATCH)
        self.act(identifier, 'retry', 'keep them while we look')
        self.service.tick()
        self.join()
        self.assertEqual(self.custody(identifier), {'state': 'refused', 'reason': IDENTITY_MISMATCH, 'retained': True})
        record = self.record(identifier)
        self.assertEqual(record['archive']['bytes'], len(self.engine(identifier).archive))
        self.assertEqual(self.service.store.pending_custody(), [identifier])

    def assert_refused_without_bytes(self, identifier, reason):
        self.assertEqual(self.custody(identifier), {'state': 'refused', 'reason': reason})
        self.assert_no_half_archive(identifier)

    def test_retry_of_a_census_refusal_still_never_holds_the_file(self):
        identifier = self.finished(wait=False)
        engine = self.engine(identifier)
        original = engine.download
        self.fixture.config.viewer_max_archive_bytes = 10 * CENSUS_RETAINED_FILE_BYTES

        def oversized(task_id):
            response = original(task_id)
            response.headers['Content-Length'] = str(CENSUS_RETAINED_FILE_BYTES + 1)
            return response

        engine.download = oversized
        self.service.tick()
        self.join()
        self.act(identifier, 'retry')
        self.service.tick()
        self.join()
        self.assert_refused_without_bytes(identifier, 'archive_exceeds_census_limit')

    def test_retry_when_the_engine_is_gone_keeps_trying(self):
        identifier = self.finished(wait=False)
        self.engine_task(identifier)['processingTime'] = 9000
        original = self.engine(identifier).download

        def download(task_id):
            self.engine_task(identifier)['processingTime'] = 9001
            return original(task_id)

        self.engine(identifier).download = download
        self.service.tick()
        self.join()
        self.assertEqual(self.custody(identifier), {'state': 'refused', 'reason': RESULT_CHANGED})
        task = self.fixture.task(identifier)
        del self.engine(identifier).tasks[task['engine_uuid']]
        self.act(identifier, 'retry')
        self.service.tick()
        self.join()
        self.assertEqual(self.custody(identifier), {'state': 'pending', 'reason': 'engine_task_missing'})
        self.assertTrue(self.record(identifier)['rebind'])  # Still the operator's retry when it returns.

    def test_the_list_shows_everything_unverified_across_pages(self):
        done = self.finished()
        with patch.object(self.service, '_advance_eager_custody', lambda: None):
            fleet = [self.finished() for _ in range(7)]
        self.assertEqual([item['taskId'] for item in self.listing(limit=2)], fleet)
        self.assertNotIn(done, self.service.store.pending_custody())
        first = self.service.eager_custody_status(limit=3)
        self.assertEqual([item['taskId'] for item in first['tasks']], fleet[:3])
        second = self.service.eager_custody_status(after=first['next'], limit=3)
        self.assertEqual([item['taskId'] for item in second['tasks']], fleet[3:6])
        third = self.service.eager_custody_status(after=second['next'], limit=3)
        self.assertEqual(([item['taskId'] for item in third['tasks']], third['next']), (fleet[6:], None))
        self.assertEqual(first['pendingTasks'], 7)
        for page in ({'limit': 0}, {'limit': service_module.CUSTODY_PAGE_MAX + 1}, {'after': -1}, {'limit': True}):
            with self.subTest(page), self.assertRaises(ValueError):
                self.service.eager_custody_status(**page)

    def test_an_invalid_record_is_listed_not_hidden(self):
        bad, good = self.owed(), self.owed()
        (self.directory(bad) / CUSTODY_NAME).write_text('{"state":"verified"}')
        items = {item['taskId']: item for item in self.listing()}
        self.assertEqual((items[bad]['state'], items[bad]['reason']), ('refused', 'custody_record_invalid'))
        self.assertEqual(items[good]['state'], 'pending')

    def test_a_restart_preserves_every_state(self):
        with patch.object(self.service, '_advance_eager_custody', lambda: None):
            deferred, retrying, refused, abandoned, verified = (self.finished() for _ in range(5))
        with patch('processing.viewer_service.shutil.disk_usage', lambda path: SimpleNamespace(free=0)):
            self.attempt(deferred)
        self.engine(retrying).read_unavailable = True
        self.attempt(retrying)
        self.engine(retrying).read_unavailable = False
        self.engine_task(refused)['processingTime'] = 9000
        self.attempt(refused)
        self.act(abandoned, 'abandon')
        self.attempt(verified)
        states = {identifier: self.custody(identifier) for identifier in (deferred, retrying, refused, abandoned, verified)}
        self.assertEqual([item['state'] for item in states.values()], ['deferred', 'pending', 'refused', 'abandoned', 'verified'])
        listed = {item['taskId']: item for item in self.listing()}
        self.restart()
        self.assertEqual({identifier: self.custody(identifier) for identifier in states}, states)
        self.assertEqual({item['taskId']: item for item in self.listing()}, listed)

    def test_the_list_route_pages_and_refuses_a_bad_page(self):
        startup = startup_fixture.StartupTests('test_invalid_values_refuse')
        startup.setUp()
        self.addCleanup(startup.doCleanups)
        startup.state.mkdir()
        on = {'PROCESSING_VIEWER_EAGER_RESULT_CUSTODY': '1', 'PROCESSING_VIEWER_EAGER_CUSTODY_RESERVE_BYTES': '7',
              'PROCESSING_VIEWER_EAGER_CUSTODY_MAX_BYTES': '9'}
        with patch.dict(os.environ, {**startup.env, **on}, clear=True):
            handler = make_handler(from_environment())
        server = start_server(handler)

        def read(query):
            request = Request('http://127.0.0.1:%d/v1/prototype/result-custody%s' % (server.server_port, query),
                              headers={'Authorization': 'Bearer ' + startup.env['PROCESSING_API_TOKEN']})
            try:
                with urlopen(request, timeout=5) as response:
                    return response.status
            except HTTPError as error:
                return error.code

        try:
            self.assertEqual([read(item) for item in ('?limit=2', '?after=5&limit=500', '?limit=0', '?limit=x', '?other=1', '?limit=1&limit=2')],
                             [200, 200, 400, 400, 400, 400])
        finally:
            server.shutdown()
            server.server_close()
            handler.viewer_service.close()
            handler.prototype_queue.close()

    def test_kept_archive_survives_resolve(self):
        identifier = self.finished()
        task = self.fixture.task(identifier)
        engine = self.engine(identifier)
        request = {'note': 'synthetic inspection', 'workerStopped': True}
        with self.assertRaises(Conflict):
            self.service.operator(task['job_id'], 'resolve', request)
        self.assertEqual((self.directory(identifier) / 'all.zip').read_bytes(), engine.archive)
        self.assertEqual(self.custody(identifier), {'state': 'verified'})
        self.assert_job_succeeded(identifier)
        # A lost job's copy held in custody, resolved as failed, is kept the same.
        self.service._node_lost_hold = True
        self.service._adopt_lost_result = True
        held = self.fixture.submit()
        self.service.tick()
        task = self.fixture.task(held)
        engine = self.engine(held)
        engine_task = engine.tasks[task['engine_uuid']]
        engine_task['identity'] = {'dateCreated': 1700000000000, 'imagesCount': 1, 'processingTime': -1,
                                   'options': [{'name': 'pc-ept', 'value': True}]}
        self.service.tick()
        engine.read_unavailable = True
        for _ in range(NODE_LOST_POLLS):
            self.service._backoff.clear()
            self.service.tick()
        engine.read_unavailable = False
        self.assertEqual(self.fixture.task(held)['job']['status'], 'held')
        self.fixture.finish(held)
        engine_task['identity']['processingTime'] = 5000
        self.service._held_observations.clear()
        self.service.tick()
        for thread in list(self.service._adopt_threads.values()):
            thread.join(10)
        self.assertEqual(self.service.operator(task['job_id'], 'resolve', request)['status'], 'failed')
        self.service.tick()
        self.join()
        self.assertEqual((self.directory(held) / 'all.zip').read_bytes(), engine.archive)
        self.assertFalse((self.directory(held) / CUSTODY_NAME).exists())
        self.assertIn(task['engine_uuid'], engine.tasks)
        self.assertFalse({call[0] for call in engine.calls} - {'initialize', 'upload', 'commit', 'read', 'download'})

    def stalled(self, identifier):
        origin = StalledOrigin()
        self.addCleanup(origin.close)
        self.engine(identifier).download = NativeEngine(origin.url).download
        return origin

    def blocked_in_recv(self, origin, threads, identifier):
        self.assertTrue(origin.sent.wait(10))
        fetch = threads[identifier]
        fetch.join(0.2)
        self.assertTrue(fetch.is_alive())
        return fetch

    def test_close_wakes_an_eager_fetch_blocked_on_a_real_socket(self):
        identifier = self.finished(wait=False)
        engine = self.engine(identifier)
        download = engine.download
        origin = self.stalled(identifier)
        self.service.tick()
        fetch = self.blocked_in_recv(origin, self.service._eager_threads, identifier)
        began = time.monotonic()
        self.service.close()
        self.assertLess(time.monotonic() - began, 5)  # The socket timeout is 60.
        self.assertFalse(fetch.is_alive())
        self.assertFalse(self.service.custody_writers_alive())
        self.assert_no_half_archive(identifier)
        self.assertFalse((self.directory(identifier) / 'all.zip.partial').exists())
        self.fixture.queue.close()
        self.fixture.make_queue_service()
        engine.download = download
        self.service.tick()
        self.join()
        self.assertEqual(self.custody(identifier), {'state': 'verified'})

    def test_close_wakes_an_adopt_fetch_blocked_on_a_real_socket(self):
        fixture = self.fixture
        identifier, _, engine_task = fixture.adoptable()
        fixture.report_finished(identifier, engine_task)
        origin = self.stalled(identifier)
        task = fixture.task(identifier)
        self.service._review_held_result(task, self.service._read_status(task))
        fetch = self.blocked_in_recv(origin, self.service._adopt_threads, identifier)
        began = time.monotonic()
        self.service.close()
        self.assertLess(time.monotonic() - began, 5)
        self.assertFalse(fetch.is_alive())
        fixture.assert_no_custody_files(identifier)
        self.assertEqual(fixture.custody(identifier)['state'], 'fetching')

    @unittest.skipIf(os.name == 'nt', "Winsock's shutdown never wakes a blocked recv; see the next test")
    def test_app_shutdown_closes_the_queue_only_after_a_woken_fetch_exits(self):
        identifier = self.finished(wait=False)
        origin = self.stalled(identifier)
        self.service.tick()
        fetch = self.blocked_in_recv(origin, self.service._eager_threads, identifier)
        queue = self.fixture.queue
        original, closed = queue.close, []
        queue.close = lambda: (closed.append(fetch.is_alive()), original())
        self.addCleanup(setattr, queue, 'close', original)
        stop = threading.Event()
        stop.set()
        self.assertEqual(coordinator_for(self.service, queue).run(stop), 0)
        self.assertEqual(closed, [False])

    @unittest.skipUnless(os.name == 'nt', 'only Winsock leaves this read unwoken')
    def test_a_read_windows_cannot_wake_keeps_authority_and_exits_nonzero(self):
        identifier = self.finished(wait=False)
        origin = self.stalled(identifier)
        self.service.tick()
        fetch = self.blocked_in_recv(origin, self.service._eager_threads, identifier)
        self.assertFalse(self.service._fetch_sockets)
        queue = self.fixture.queue
        original, closed = queue.close, []
        queue.close = lambda: (closed.append(fetch.is_alive()), original())
        self.addCleanup(setattr, queue, 'close', original)
        stop = threading.Event()
        stop.set()
        coordinator = coordinator_for(self.service, queue)
        with patch('processing.app.RESULT_WRITER_GRACE_SECONDS', 0.1):
            self.assertEqual(coordinator.run(stop), 2)
        self.assertEqual((coordinator.health()['reason'], closed), ('result_fetch_drain_failed', []))
        self.assertTrue(fetch.is_alive())
        origin.close()  # What the read's own timeout would do; authority is then released.
        fetch.join(10)
        self.assertFalse(self.service.custody_writers_alive())
        original()

    def test_shutdown_retains_authority_while_eager_fetch_is_blocked(self):
        identifier = self.finished(wait=False)
        blocked = Blocked(self.engine(identifier))
        self.addCleanup(blocked.undo)
        self.service.tick()
        self.assertTrue(blocked.reading.wait(5))
        fetch = self.service._eager_threads[identifier]
        original_join = threading.Thread.join

        def shorten_join(thread, timeout=None):
            if thread is fetch and timeout is not None:
                return original_join(thread, min(timeout, 0.05))
            return original_join(thread, timeout)

        with patch.object(threading.Thread, 'join', new=shorten_join):
            with self.assertRaisesRegex(RuntimeError, 'retain queue ownership'):
                self.service.close()
        done = threading.Event()
        waiter = threading.Thread(target=lambda: (self.service.wait_for_transfers(), done.set()))
        waiter.start()
        try:
            self.assertFalse(done.wait(0.2), 'shutdown wait ignored active eager fetch')
            blocked.undo()
            waiter.join(5)
            self.assertTrue(done.is_set(), 'shutdown wait did not finish after fetch exited')
        finally:
            blocked.undo()
            waiter.join(5)
        self.assertFalse(fetch.is_alive())

    def test_shutdown_waits_for_all_three_kinds_of_writer(self):
        for kind in ('dispatch', 'adopt', 'eager'):
            with self.subTest(kind):
                release, done = threading.Event(), threading.Event()
                writer = threading.Thread(target=release.wait, args=(10,), daemon=True)
                writer.start()
                registry = {'dispatch': self.service._dispatch_threads, 'adopt': self.service._adopt_threads,
                            'eager': self.service._eager_threads}[kind]
                registry['synthetic'] = ('job', writer) if kind == 'dispatch' else writer
                waiter = threading.Thread(target=lambda: (self.service.wait_for_transfers(), done.set()))
                waiter.start()
                self.assertFalse(done.wait(0.1), kind)
                release.set()
                writer.join(5)
                with self.service._lock:
                    del registry['synthetic']
                waiter.join(5)
                self.assertTrue(done.is_set(), kind)

    def test_writer_grace_ends_the_wait_only_for_a_result_fetch(self):
        release = threading.Event()
        self.addCleanup(release.set)
        writer = threading.Thread(target=release.wait, args=(10,), daemon=True)
        writer.start()
        self.service._eager_threads['synthetic'] = writer
        self.addCleanup(self.service._eager_threads.pop, 'synthetic', None)
        self.service.wait_for_transfers(0.05)
        self.assertTrue(self.service.custody_writers_alive())
        del self.service._eager_threads['synthetic']
        self.service._dispatch_threads['synthetic'] = ('job', writer)
        self.addCleanup(self.service._dispatch_threads.pop, 'synthetic', None)
        done = threading.Event()
        waiter = threading.Thread(target=lambda: (self.service.wait_for_transfers(0.05), done.set()), daemon=True)
        waiter.start()
        self.assertFalse(done.wait(0.3), 'a grace released a sender that can mutate an engine')
        release.set()
        writer.join(5)
        with self.service._lock:
            del self.service._dispatch_threads['synthetic']
        waiter.join(5)
        self.assertTrue(done.is_set())

    def test_close_reports_a_writer_it_could_not_stop(self):
        identifier = self.finished(wait=False)
        blocked = Blocked(self.engine(identifier))
        self.addCleanup(blocked.undo)
        self.service.tick()
        self.assertTrue(blocked.reading.wait(10))
        fetch = self.service._eager_threads[identifier]
        join = threading.Thread.join
        bounded = lambda thread, timeout=None: join(thread, min(timeout, 0.05) if thread is fetch and timeout else timeout)
        with patch.object(threading.Thread, 'join', new=bounded):
            with self.assertRaisesRegex(RuntimeError, 'retain queue ownership'):
                self.service.close()
        self.assertTrue(self.service.custody_writers_alive())
        blocked.undo()
        fetch.join(10)
        self.assertFalse(self.service.custody_writers_alive())

    def child(self, source):
        """Run a real process on its own state; its report ends at its first line."""
        home = self.fixture.root / 'child'
        home.mkdir()
        process = subprocess.Popen([sys.executable, '-c', source], cwd=ROOT, stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, text=True, env={**os.environ, 'TMPDIR': str(home)})
        self.addCleanup(process.stdout.close)
        self.addCleanup(process.stdin.close)
        self.addCleanup(process.wait)
        self.addCleanup(process.kill)
        watchdog = threading.Timer(60, process.kill)
        self.addCleanup(watchdog.cancel)
        watchdog.start()
        report = json.loads(process.stdout.readline())
        report['process'] = process
        if source is KILLED_AFTER_COMMIT:
            report['code'] = process.wait(10)
        return report

    def adopt_child_state(self, report):
        """Stand on a dead process's state, with an engine that kept its task."""
        fixture, root = self.fixture, Path(report['root'])
        self.service.close()
        fixture.queue.close()
        fixture.root, fixture.config.prototype_state_dir = root, str(root / 'state')
        fixture.engines[report['node']].tasks[report['engine']] = {
            'uuid': report['engine'], 'status': {'code': 40}, 'progress': 100, 'files': {}, 'cancel_requested': False,
            'processingTime': 5000, 'identity': report['identity']}
        fixture.make_queue_service()

    @unittest.skipIf(os.name == 'nt', 'proves a POSIX flock released by process death')
    def test_a_wedged_writer_keeps_authority_until_the_process_is_killed(self):
        report = self.child(WEDGED_WRITER)
        child = report['process']
        self.assertEqual((report['code'], report['alive'], report['state'], report['reason']),
                         (2, True, 'failed', 'result_fetch_drain_failed'))
        arguments = dict(node_ids=('cluster', 'main'), treedar_node_ids=('main',), viewer_enabled=True)
        with self.assertRaisesRegex(RuntimeError, 'already owned'):
            Queue(Path(report['root']) / 'state', **arguments)
        child.kill()
        self.assertEqual(child.wait(10), -9)
        self.adopt_child_state(report)
        identifier = report['identifier']
        names = {path.name for path in self.directory(identifier).iterdir()}
        self.assertFalse(names & {'all.zip', 'all.zip.receipt'}, names)  # The partial is what a kill leaves.
        self.service.tick()
        self.join()
        self.assertEqual(self.custody(identifier), {'state': 'verified'})
        self.assertFalse((self.directory(identifier) / 'all.zip.partial').exists())

    def test_sidecar_is_a_private_regular_file_the_census_allows(self):
        identifier = self.finished()
        path = self.directory(identifier) / CUSTODY_NAME
        info = path.lstat()
        self.assertEqual(info.st_nlink, 1)
        self.assertLess(info.st_size, 4096)
        for name in ('all.zip', 'all.zip.receipt', 'all.zip.intent'):
            self.assertEqual((self.directory(identifier) / name).lstat().st_nlink, 1)
        self.assertTrue(re.fullmatch(r'[a-f0-9]{64}', self.record(identifier)['archive']['sha256']))


if __name__ == '__main__':
    unittest.main()
