"""Linux source descriptor custody; no NAS or deletion is exercised."""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from processing.maintenance import Conflict
from processing.prototype import Queue
from processing.reference_source_readers import SourceReaderError, SourceReaders
from processing.reference_store import ReferenceStore
from processing.reference_worker_api import _SourceBrokerStream, _SourceRangeBrokerStream


@contextmanager
def source_fixture(directory, *, range_v2=False):
    from tests.test_reference_source_intake import ReferenceSourceIntakeTests, IMAGE_ID
    case = ReferenceSourceIntakeTests('runTest')
    case.setUp()
    if range_v2:
        case.payload['manifest']['id'] = 'dataset-source-reader'
        case.rehash()
    root = Path(directory) / 'spool'
    root.mkdir(mode=0o700, exist_ok=True)
    marker = root / '.ltds-source-spool-id'
    marker.write_text('a' * 64)
    marker.chmod(0o600)
    from processing.source_spool import SourceSpool
    with patch('processing.source_spool.os.path.ismount', return_value=True):
        spool = SourceSpool(root, mount_id='a' * 64, safety_floor_bytes=0,
            max_object_bytes=1024, chunk_bytes=3)
        queue, store, control, job, _ = case._make_reserved_source_job(directory, spool, 'reader', source_range_v2=range_v2)
        try:
            grant = store.mint_source_transfer_grant(job['jobId'], source_spool=spool)
            store.update_phase(job['jobId'], 'fetching')
            yield queue, store, spool, job, grant, IMAGE_ID
        finally:
            queue.close()


def child_case(directory, kind):
    """Each intentionally uncertain lease dies with its own process."""
    with source_fixture(directory) as (queue, store, spool, job, grant, image):
        readers = store.source_readers()
        reader, _ = store.source_object_for_grant(grant['token'], image, source_spool=spool)
        if kind == 'duplicate':
            try:
                reader.attach(reader._reader, authorize=lambda db: None)
            except SourceReaderError:
                pass
        elif kind == 'uncertain_close':
            original = reader._reader
            class Uncertain:
                def fileno(self): return original.fileno()
                def close(self):
                    original.close()
                    raise OSError('synthetic close outcome unknown')
            reader._reader = Uncertain()
            try:
                reader.close()
            except OSError:
                pass
        elif kind == 'changed_lease':
            os.utime(readers.lock_path, ns=(1, 1))
        elif kind == 'changed_root':
            old = readers.root.with_name('source-readers-old')
            readers.root.rename(old)
            readers.root.mkdir(mode=0o700)
        elif kind == 'uncertain_commit':
            original = readers._db
            @contextmanager
            def ambiguous():
                with original() as db:
                    yield db
                raise OSError('synthetic lost commit acknowledgement')
            with patch.object(readers, '_db', side_effect=ambiguous):
                try:
                    reader.close()
                except OSError:
                    pass
        elif kind == 'fork_guard':
            entered, release = threading.Event(), threading.Event()
            def hold_mutexes():
                with queue._lifecycle_lock, readers._mutex:
                    entered.set()
                    release.wait(5)
            holder = threading.Thread(target=hold_mutexes)
            holder.start()
            if not entered.wait(2):
                os._exit(2)
            pid = os.fork()
            if pid == 0:
                # Both the descriptor and Store accessor must check PID before
                # acquiring either mutex inherited locked from the parent.
                for operation in (reader.close, store.source_readers):
                    try:
                        operation()
                    except SourceReaderError:
                        continue
                    os._exit(3)
                os._exit(0)
            try:
                deadline = time.monotonic() + 3
                while time.monotonic() < deadline:
                    observed, status = os.waitpid(pid, os.WNOHANG)
                    if observed:
                        if status != 0:
                            os._exit(4)
                        break
                    time.sleep(0.01)
                else:
                    import signal
                    os.kill(pid, signal.SIGKILL)
                    os.waitpid(pid, 0)
                    os._exit(5)
            finally:
                release.set()
                holder.join(timeout=3)
        try:
            queue.close()
        except (SourceReaderError, OSError):
            refused = True
        else:
            refused = False
        print(json.dumps(dict(refused=refused, authorityId=queue.authority_id,
            jobId=job['jobId'], generation=job['generation'])), flush=True)
        if kind == 'live':
            sys.stdin.readline()
        # Deliberately model process death; never silently unlock a live or
        # uncertain descriptor through test cleanup in this process.
        os._exit(0 if refused else 1)


@unittest.skipUnless(sys.platform.startswith('linux'), 'Linux source descriptor lease qualification')
class SourceReaderTests(unittest.TestCase):
    def test_full_stream_registers_then_releases_at_eof(self):
        with tempfile.TemporaryDirectory() as directory, source_fixture(directory) as (_, store, spool, job, grant, image):
            reader, receipt = store.source_object_for_grant(grant['token'], image, source_spool=spool)
            readers = store.source_readers()
            self.assertEqual(readers.active_count(job['jobId'], 1), 1)
            self.assertFalse(hasattr(reader, 'fileno'), 'no raw descriptor escapes the managed reader API')
            stream = _SourceBrokerStream(reader, byte_size=receipt['byteSize'])
            self.assertEqual(b''.join(stream.chunks()), b'abc')
            self.assertEqual(readers.active_count(job['jobId'], 1), 0)
            stream.close()

    def test_http_abort_and_read_error_release_known_closed_descriptor(self):
        with tempfile.TemporaryDirectory() as directory, source_fixture(directory) as (_, store, spool, job, grant, image):
            for fail_read in (False, True):
                reader, receipt = store.source_object_for_grant(grant['token'], image, source_spool=spool)
                stream = _SourceBrokerStream(reader, byte_size=receipt['byteSize'])
                if fail_read:
                    with patch.object(reader, 'read', side_effect=OSError('synthetic read error')):
                        with self.assertRaises(OSError):
                            list(stream.chunks())
                else:
                    stream.close()
                self.assertEqual(store.source_readers().active_count(job['jobId'], 1), 0)

    def test_range_reader_is_registered_until_verified_eof_and_blocks_fenced_renewal(self):
        from processing.source_range_transfer import validate_source_grant, SourceIdentity, _request_id
        with tempfile.TemporaryDirectory() as directory, source_fixture(directory, range_v2=True) as (_, store, spool, job, grant, image):
            checked = validate_source_grant(grant, job=store.get(job['jobId'], private=True))
            obj = next(item for item in checked.objects if item.id == image)
            identity = SourceIdentity(grant['environment'], grant['jobId'], grant['reservationId'],
                grant['generation'], grant['manifestSha256'], grant['sourceVersion'])
            request = _request_id(json.dumps(identity.canonical(), sort_keys=True, separators=(',', ':')), obj, 0, obj.byte_size)
            reader, receipt = store.source_range_for_grant(grant['token'], image, range_start=0,
                range_length=obj.byte_size, request_id=request, if_match='"sha256-' + obj.sha256 + '"', source_spool=spool)
            readers = store.source_readers()
            self.assertEqual(readers.active_count(job['jobId'], 1), 1)
            readers.fence(job['jobId'], 1, 'range-release')
            stream = _SourceRangeBrokerStream(reader, byte_size=receipt['rangeLength'], file_identity=receipt['fileIdentity'])
            self.assertEqual(b''.join(stream.chunks()), b'abc')
            self.assertEqual(readers.active_count(job['jobId'], 1), 0)
            from processing import reference_protocol as protocol
            renewal = dict(protocol='source-range-v2-renewal', renewalId='fenced-renewal', sequence=1,
                previousGrantSha256=protocol.digest(grant), previousGrant=grant)
            with self.assertRaises(Conflict):
                store.renew_source_transfer_grant(renewal, source_spool=spool)

    def test_queue_close_racing_reader_close_never_deadlocks_or_unlocks_live_reader(self):
        with tempfile.TemporaryDirectory() as directory, source_fixture(directory) as (queue, store, spool, job, grant, image):
            reader, _ = store.source_object_for_grant(grant['token'], image, source_spool=spool)
            outcome = []
            def attempt_close():
                try:
                    queue.close()
                except SourceReaderError:
                    outcome.append('refused')
            thread = threading.Thread(target=attempt_close)
            thread.start(); thread.join(timeout=3)
            self.assertFalse(thread.is_alive())
            self.assertEqual(outcome, ['refused'])
            self.assertEqual(reader.read(), b'abc')
            reader.close()

    def test_expired_grant_after_open_never_exposes_descriptor(self):
        with tempfile.TemporaryDirectory() as directory, source_fixture(directory) as (_, store, spool, job, grant, image):
            from datetime import datetime
            expiry = datetime.fromisoformat(grant['expiresAt'].replace('Z', '+00:00')).timestamp()
            now = [time.time()]
            original = spool.open_verified_object
            opened = []
            def race(**kwargs):
                result = original(**kwargs)
                opened.append(result[0])
                now[0] = expiry + 1
                return result
            with patch.object(spool, 'open_verified_object', side_effect=race):
                with self.assertRaises(Conflict):
                    store.source_object_for_grant(grant['token'], image, source_spool=spool, clock=lambda: now[0])
            self.assertTrue(opened[0].closed)
            self.assertEqual(store.source_readers().active_count(job['jobId'], 1), 0)

    def test_clock_rollback_during_open_closes_the_registered_descriptor(self):
        with tempfile.TemporaryDirectory() as directory, source_fixture(directory) as (_, store, spool, job, grant, image):
            now = [time.time()]
            original = spool.open_verified_object
            opened = []
            def race(**kwargs):
                result = original(**kwargs)
                opened.append(result[0])
                now[0] -= 1
                return result
            with patch.object(spool, 'open_verified_object', side_effect=race):
                with self.assertRaises(Conflict):
                    store.source_object_for_grant(grant['token'], image, source_spool=spool, clock=lambda: now[0])
            self.assertTrue(opened[0].closed)
            self.assertEqual(store.source_readers().active_count(job['jobId'], 1), 0)

    def test_slow_source_hash_does_not_hold_queue_but_shutdown_keeps_authority(self):
        with tempfile.TemporaryDirectory() as directory, source_fixture(directory) as (queue, store, spool, job, grant, image):
            entered, release, wrote = threading.Event(), threading.Event(), threading.Event()
            original = spool.open_verified_object
            outcome = []
            def slow(**kwargs):
                entered.set()
                if not release.wait(4):
                    raise AssertionError('source hash fixture was not released')
                return original(**kwargs)
            def fetch():
                try:
                    outcome.append(store.source_object_for_grant(grant['token'], image, source_spool=spool)[0])
                except Exception as error:
                    outcome.append(error)
            def write():
                with queue._db() as db:
                    db.execute('CREATE TABLE IF NOT EXISTS reader_queue_probe(value INTEGER)')
                    db.execute('INSERT INTO reader_queue_probe VALUES(1)')
                wrote.set()
            with patch.object(spool, 'open_verified_object', side_effect=slow):
                fetch_thread = threading.Thread(target=fetch)
                fetch_thread.start()
                try:
                    self.assertTrue(entered.wait(2))
                    writer = threading.Thread(target=write)
                    writer.start()
                    self.assertTrue(wrote.wait(2), 'queue blocked behind a source hash')
                    writer.join(timeout=2)
                    self.assertFalse(writer.is_alive())
                    with self.assertRaises(SourceReaderError):
                        queue.close()
                finally:
                    release.set()
                    fetch_thread.join(timeout=5)
                self.assertFalse(fetch_thread.is_alive())
            self.assertEqual(len(outcome), 1)
            self.assertFalse(isinstance(outcome[0], Exception), repr(outcome[0]))
            outcome[0].close()

    def test_fence_drains_existing_reader_and_blocks_new_reads_and_grants(self):
        with tempfile.TemporaryDirectory() as directory, source_fixture(directory) as (_, store, spool, job, grant, image):
            reader, _ = store.source_object_for_grant(grant['token'], image, source_spool=spool)
            readers = store.source_readers()
            readers.fence(job['jobId'], 1, 'release-one')
            readers.fence(job['jobId'], 1, 'release-one')
            self.assertEqual(readers.active_count(job['jobId'], 1), 1)
            self.assertEqual(reader.read(), b'abc')
            reader.close()
            self.assertEqual(readers.active_count(job['jobId'], 1), 0)
            with self.assertRaises(Conflict):
                store.source_object_for_grant(grant['token'], image, source_spool=spool)
            with self.assertRaises(Conflict):
                store.mint_source_transfer_grant(job['jobId'], source_spool=spool)
            with self.assertRaises(Conflict):
                readers.fence(job['jobId'], 1, 'different-release')
            self.assertTrue((spool.root / spool.storage_key(job['jobId'], 1, image)).exists())

    def test_fence_winning_during_open_closes_without_exposing_descriptor(self):
        with tempfile.TemporaryDirectory() as directory, source_fixture(directory) as (_, store, spool, job, grant, image):
            readers = store.source_readers()
            original = spool.open_verified_object
            opened = []
            def race(**kwargs):
                result = original(**kwargs)
                opened.append(result[0])
                readers.fence(job['jobId'], 1, 'release-open-race')
                return result
            with patch.object(spool, 'open_verified_object', side_effect=race):
                with self.assertRaises(Conflict):
                    store.source_object_for_grant(grant['token'], image, source_spool=spool)
            self.assertTrue(opened[0].closed)
            self.assertEqual(readers.active_count(job['jobId'], 1), 0)

    def test_grant_change_during_open_cannot_escape_post_open_check(self):
        with tempfile.TemporaryDirectory() as directory, source_fixture(directory) as (_, store, spool, job, grant, image):
            original = spool.open_verified_object
            def race(**kwargs):
                result = original(**kwargs)
                store.update_phase(job['jobId'], 'held', error='synthetic_owner_change')
                return result
            with patch.object(spool, 'open_verified_object', side_effect=race):
                with self.assertRaises(Conflict):
                    store.source_object_for_grant(grant['token'], image, source_spool=spool)
            self.assertEqual(store.source_readers().active_count(job['jobId'], 1), 0)

    def test_pin_and_fence_quota_fail_without_leaking_active_reader(self):
        with tempfile.TemporaryDirectory() as directory, source_fixture(directory) as (_, store, spool, job, grant, image):
            readers = store.source_readers()
            with store.queue._db() as db:
                used = store._metadata_used_bytes(db)
            store.max_metadata_bytes = used
            with self.assertRaisesRegex(Conflict, 'metadata quota'):
                store.source_object_for_grant(grant['token'], image, source_spool=spool)
            with self.assertRaisesRegex(Conflict, 'metadata quota'):
                readers.fence(job['jobId'], 1, 'release-quota')
            self.assertEqual(readers.active_count(job['jobId'], 1), 0)

    def test_unsupported_platform_is_explicit(self):
        with tempfile.TemporaryDirectory() as directory, source_fixture(directory) as (_, store, _, _, _, _):
            with patch('processing.reference_source_readers.sys.platform', 'win32'):
                with self.assertRaisesRegex(SourceReaderError, 'linux_source_readers_required'):
                    store.source_readers()

    def test_process_death_retires_only_registered_pins_and_live_lease_refuses_successor(self):
        with tempfile.TemporaryDirectory() as directory:
            command = [sys.executable, '-c', 'from tests.test_reference_source_readers import child_case; import sys; child_case(sys.argv[1],sys.argv[2])', directory, 'live']
            child = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                result = json.loads(child.stdout.readline())
                self.assertTrue(result['refused'])
                from types import SimpleNamespace
                fake = SimpleNamespace(queue=SimpleNamespace(path=Path(directory) / 'jobs.sqlite3', authority_id=result['authorityId']))
                with self.assertRaisesRegex(SourceReaderError, 'source_reader_session_busy'):
                    SourceReaders(fake).start()
                child.stdin.write('exit\n'); child.stdin.flush()
                self.assertEqual(child.wait(timeout=10), 0)
            finally:
                if child.poll() is None:
                    child.kill(); child.wait(timeout=5)
                child.stdin.close(); child.stdout.close(); child.stderr.close()
            queue = Queue(directory, reference_enabled=True)
            try:
                store = ReferenceStore(queue)
                readers = store.source_readers()
                self.assertEqual(readers.active_count(result['jobId'], 1), 0)
                with queue._db() as db:
                    self.assertEqual(db.execute("SELECT state FROM source_reader_pins").fetchone()[0], 'superseded')
            finally:
                queue.close()

    def test_uncertain_or_changed_ownership_never_unlocks_authority(self):
        for scenario in ('duplicate', 'uncertain_close', 'uncertain_commit', 'changed_lease', 'changed_root', 'fork_guard'):
            with self.subTest(scenario=scenario), tempfile.TemporaryDirectory() as directory:
                command = [sys.executable, '-c', 'from tests.test_reference_source_readers import child_case; import sys; child_case(sys.argv[1],sys.argv[2])', directory, scenario]
                completed = subprocess.run(command, capture_output=True, text=True, timeout=15)
                self.assertEqual(completed.returncode, 0, completed.stderr[-2000:])
                self.assertTrue(json.loads(completed.stdout)['refused'])


if __name__ == '__main__':
    unittest.main()
