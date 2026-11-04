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
from types import SimpleNamespace
import threading
import time
import unittest
from unittest.mock import patch
import uuid
import zipfile

from processing.admission_drain import AdmissionClosed, AdmissionDrain
from processing.config import Config, from_environment
from processing.maintenance import Conflict
from processing.viewer_limits import EAGER_CUSTODY_MAX_BYTES, EAGER_CUSTODY_RESERVE_BYTES
from processing.viewer_result_adoption import CUSTODY_NAME, RESULT_CHANGED
from processing import viewer_service as service_module
from processing.viewer_service import (CENSUS_RETAINED_FILE_BYTES, EAGER_DEFERRED_RETRY_SECONDS, EAGER_FETCH_CONCURRENCY,
                                       EAGER_RESUME_SCAN_JOBS, ENGINE_MISSING_POLLS, EngineUnavailable, NODE_LOST_POLLS)
from processing.viewer_tasks import TaskStore
from tests import test_viewer_parallel_dispatch as startup_fixture, test_viewer_service as viewer_fixture

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
            self.assertEqual(config.viewer_eager_custody_reserve_bytes, EAGER_CUSTODY_RESERVE_BYTES)
            self.assertEqual(config.viewer_eager_custody_max_bytes, EAGER_CUSTODY_MAX_BYTES)
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


class EagerCustody(unittest.TestCase):
    def setUp(self):
        self.fixture = viewer_fixture.ViewerServiceTests('test_download_twice_matches_hash_and_retains_archive_and_inputs')
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.fixture.config.viewer_eager_result_custody = True
        self.fixture.config.viewer_eager_custody_reserve_bytes = 1
        self.fixture.config.viewer_eager_custody_max_bytes = 1024 ** 3
        self.restart()

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
        self.assertEqual((task['job']['status'], task['stage'], task['phase'], task['error']),
                         ('succeeded', 'succeeded', 'complete', None))
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
        self.assertEqual(self.custody(identifier), {'state': 'unavailable', 'reason': RESULT_CHANGED})
        self.assert_job_succeeded(identifier)
        self.settle()
        self.assertEqual(engine.download_calls, 1)  # Latched, and Viewer is unaffected.
        with self.service.download(identifier)[0] as stream:
            self.assertEqual(stream.read(), engine.archive)

    def test_result_changing_before_the_fetch_is_refused_without_a_transfer(self):
        identifier = self.finished(wait=False)
        task = self.fixture.task(identifier)
        engine = self.engine(identifier)
        with patch.object(self.service, '_advance_eager_custody', lambda jobs: None):
            self.service.tick()
        engine.tasks[task['engine_uuid']]['status']['code'] = 20  # A stale tasks.json reran it.
        self.settle()
        self.assertEqual(self.custody(identifier), {'state': 'unavailable', 'reason': RESULT_CHANGED})
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
        with patch.object(self.service, '_advance_eager_custody', lambda jobs: None):
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
        self.assertEqual(self.custody(identifier), {'state': 'unavailable', 'reason': 'archive_exceeds_census_limit'})
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

    def test_viewer_download_before_the_fetch_is_recorded_without_a_second_transfer(self):
        identifier = self.finished(wait=False)
        engine = self.engine(identifier)
        with patch.object(self.service, '_advance_eager_custody', lambda jobs: None):
            self.service.tick()
        with self.service.download(identifier)[0]:
            pass
        self.assertEqual(self.custody(identifier), {'state': 'pending'})
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

    def test_engine_answering_missing_ends_after_bounded_attempts(self):
        identifier = self.finished(wait=False)
        task = self.fixture.task(identifier)
        engine = self.engine(identifier)
        with patch.object(self.service, '_advance_eager_custody', lambda jobs: None):
            self.service.tick()
        del engine.tasks[task['engine_uuid']]
        for attempt in range(ENGINE_MISSING_POLLS):
            self.assertEqual(self.custody(identifier)['state'], 'pending')
            self.settle()
        self.assertEqual(self.custody(identifier), {'state': 'unavailable', 'reason': 'engine_task_missing'})
        self.assertNotIn(identifier, self.service._eager_due)
        self.assert_job_succeeded(identifier)

    def test_unreachable_node_retries_until_it_answers(self):
        identifier = self.finished(wait=False)
        engine = self.engine(identifier)
        with patch.object(self.service, '_advance_eager_custody', lambda jobs: None):
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
        with patch.object(self.service, '_advance_eager_custody', lambda jobs: None):
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
        with patch.object(self.service, '_advance_eager_custody', lambda jobs: None):
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

    def test_restart_scan_is_bounded_to_the_latest_successes(self):
        with patch.object(self.service, '_advance_eager_custody', lambda jobs: None):
            first = self.finished()
            second = self.finished()
        self.assertGreater(EAGER_RESUME_SCAN_JOBS, 1)
        self.restart()
        with patch.object(service_module, 'EAGER_RESUME_SCAN_JOBS', 1):
            self.service.tick()
            self.join()
        self.assertEqual(self.custody(second), {'state': 'verified'})
        self.assertEqual(self.custody(first), {'state': 'pending'})

    def test_invalid_record_is_reported_and_left_for_inspection(self):
        identifier = self.finished(wait=False)
        with patch.object(self.service, '_advance_eager_custody', lambda jobs: None):
            self.service.tick()
        path = self.directory(identifier) / CUSTODY_NAME
        path.write_text('{"state":"verified"}')
        self.settle()
        self.assertEqual(self.custody(identifier), {'state': 'unavailable', 'reason': 'custody_record_invalid'})
        self.assertEqual(path.read_text(), '{"state":"verified"}')
        self.assertEqual(self.engine(identifier).download_calls, 0)

    def test_unsafe_partial_stops_at_inspection_and_is_kept(self):
        identifier = self.finished(wait=False)
        with patch.object(self.service, '_advance_eager_custody', lambda jobs: None):
            self.service.tick()
        partial = self.directory(identifier) / 'all.zip.partial'
        partial.write_bytes(b'no intent was recorded for this')
        self.settle()
        self.assertEqual(self.custody(identifier), {'state': 'unavailable', 'reason': 'archive_requires_inspection'})
        self.assertTrue(partial.exists())

    def test_task_removed_before_its_fetch_is_dropped(self):
        identifier = self.finished(wait=False)
        engine = self.engine(identifier)
        with patch.object(self.service, '_advance_eager_custody', lambda jobs: None):
            self.service.tick()
        self.service.remove(identifier)
        self.settle()
        self.assertNotIn(identifier, self.service._eager_due)
        self.assertEqual(engine.download_calls, 0)

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
