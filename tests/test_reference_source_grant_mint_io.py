"""The initial source grant's NAS reads under the coordinator's own lifecycle.

Mirrors RenewalSourceIoTests for ``mint_source_transfer_grant`` on both transfer
protocols: whole-object grants hash through ``verify_object`` and range grants
through ``open_verified_object``. The coordinator and spool are real; only the
spool's own open, read and close calls are stalled or failed.
"""
from contextlib import contextmanager
import hashlib
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import uuid

from processing import source_spool as spool_module
from processing.coordinator_freeze import CoordinatorFrozen
from processing.maintenance import Conflict
from processing.reference_source_intake_store import _ensure_source_range_schema
from processing.reference_source_readers import SourceReaderError
from processing.source_spool import SourceSpool, SourceSpoolCloseUncertain
from tests.test_reference_source_grant_renewal import _BlockedClose, _UnknownClose


class MintHarness:
    source_range_v2 = None

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        from tests.test_reference_source_intake import ReferenceSourceIntakeTests
        case = ReferenceSourceIntakeTests('runTest')
        case.setUp()
        case.payload['manifest']['id'] = 'dataset-grant-mint'
        case.rehash()
        spool_root = self.root / 'spool'
        spool_root.mkdir(mode=0o700)
        marker = spool_root / '.ltds-source-spool-id'
        marker.write_text('a' * 64, encoding='ascii')
        if os.name != 'nt':
            marker.chmod(0o600)
        mount = patch('processing.source_spool.os.path.ismount', return_value=True)
        mount.start()
        self.addCleanup(mount.stop)
        self.spool = SourceSpool(spool_root, mount_id='a' * 64, safety_floor_bytes=0,
            max_object_bytes=1024, chunk_bytes=3)
        self.queue, self.store, _control, job, _claim_token = case._make_reserved_source_job(
            self.directory.name, self.spool, 'mint', source_range_v2=self.source_range_v2)
        self.addCleanup(lambda: self.queue.close())
        self.job_id = job['jobId']
        self.generation = self.store.get(self.job_id, private=True)['transferRequest']['generation']
        # A live coordinator has minted before. Without this, a freeze refuses
        # the first mint's schema DDL and hides what the reads themselves do.
        _ensure_source_range_schema(self.queue)
        self.readers = self.store.source_readers()
        self.plan = str(uuid.uuid4())
        self.retained = self.evidence()

    def mint(self):
        return self.store.mint_source_transfer_grant(self.job_id, source_spool=self.spool)

    def mint_in_background(self):
        outcome = []

        def run():
            try:
                outcome.append(self.mint())
            except BaseException as error:
                outcome.append(error)
        thread = threading.Thread(target=run)
        thread.start()
        self.addCleanup(thread.join, 10)
        return thread, outcome

    def finish(self, thread, outcome):
        thread.join(10)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(outcome), 1)
        return outcome[0]

    def freeze(self, action, **kwargs):
        gate = self.queue._coordinator_freeze
        return gate.transition(dict(requestId=str(uuid.uuid4()), planId=self.plan, action=action,
            expectedRevision=gate.status()['revision'], startupId=self.queue.startup_id),
            owner_secret='synthetic-freeze-owner', **kwargs)

    def seal_when_drained(self):
        deadline = time.monotonic() + 5
        while True:
            try:
                return self.freeze('seal')
            except Conflict:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.02)

    def execute(self, statement, *values):
        with self.queue._db() as db:
            db.execute(statement, values)

    def table(self, name):
        with self.queue._db() as db:
            return [dict(row) for row in db.execute(f'SELECT * FROM {name} WHERE job_id=?', (self.job_id,))]

    def object_paths(self):
        return [self.spool.root / row['storage_key'] for row in self.table('reference_source_object_receipts')]

    def evidence(self):
        """What a refused mint must leave exactly as it found it."""
        return dict(receipts=self.table('reference_source_object_receipts'),
            intake=self.table('reference_source_intakes'),
            objects={str(path): path.read_bytes() for path in self.object_paths()})

    def grants(self):
        return len(self.table('reference_transfer_grants'))

    def active(self):
        return self.readers.active_count(self.job_id, self.generation)

    def assert_nothing_minted_and_all_retained(self):
        self.assertEqual(self.grants(), 0)
        self.assertEqual(self.table('reference_source_grant_tokens'), [])
        self.assertEqual(self.table('reference_source_object_pins'), [])
        self.assertEqual(self.evidence(), self.retained)

    def assert_outcome_retained(self, result):
        if isinstance(result, BaseException):
            self.assert_nothing_minted_and_all_retained()
        else:
            self.assertEqual(self.grants(), 1)
            self.assertEqual(self.evidence(), self.retained)

    @contextmanager
    def spool_calls(self, *, before_open=None, after_open=None, before_read=None):
        """Hook the spool's real descriptor calls; anything else passes through."""
        real_open, real_read = os.open, os.read
        paths, descriptors = {str(path) for path in self.object_paths()}, set()

        def opening(path, *args, **kwargs):
            if str(path) not in paths:
                return real_open(path, *args, **kwargs)
            if before_open:
                before_open()
            descriptor = real_open(path, *args, **kwargs)
            descriptors.add(descriptor)
            if after_open:
                after_open(descriptor)
            return descriptor

        def reading(descriptor, size):
            if before_read and descriptor in descriptors:
                before_read()
            return real_read(descriptor, size)
        with patch.object(os, 'open', opening), patch.object(os, 'read', reading):
            yield

    @contextmanager
    def blocked(self, stage):
        entered, release = threading.Event(), threading.Event()

        def stall():
            if not entered.is_set():
                entered.set()
                if not release.wait(5):
                    raise AssertionError('blocked NAS call was not released')
        with self.spool_calls(**{stage: stall}):
            try:
                yield entered, release
            finally:
                release.set()


class MintLifecycleCases:
    """Each case fails while mint touches the NAS outside the registered readers."""

    def held_handle(self):
        held = [handle for handle in self.readers._handles.values() if handle.uncertain]
        self.assertEqual(len(held), 1)
        return held[0]

    def reconcile(self, held):
        # Only the process owner's exit may retire this in production. Reconcile
        # by hand so cleanup can close the coordinator this test still owns.
        held.uncertain, held._known_closed = False, True
        held.close()

    def assert_authority_retained(self):
        self.assertEqual(self.active(), 1)
        with self.assertRaises(SourceReaderError):
            self.queue.close()
        self.readers.fence(self.job_id, self.generation, 'mint-release')
        with self.assertRaisesRegex(SourceReaderError, 'source_operation_busy'):
            self.readers.reserve_release(self.job_id, self.generation, 'mint-release', authorize=lambda _db: None)

    def refused_after_open(self, change, error, *, on_open=1, opens=1, restore=None):
        """A claim change seen after one open refuses it and reaches no later object."""
        seen = []

        def opened(descriptor):
            seen.append((descriptor, os.fstat(descriptor).st_ino))
            if len(seen) == on_open:
                change()
        with self.spool_calls(after_open=opened):
            with self.assertRaisesRegex(Conflict, error):
                self.mint()
        self.assertEqual(len(seen), opens, 'NAS opens after the lost claim')
        for descriptor, inode in seen:
            try:
                self.assertNotEqual(os.fstat(descriptor).st_ino, inode, 'source descriptor left open')
            except OSError:
                pass
        self.assertEqual(self.active(), 0)
        if restore:
            restore()
        self.assert_nothing_minted_and_all_retained()

    # Every descriptor is registered.

    def test_each_open_happens_under_an_active_pin(self):
        seen = []
        with self.spool_calls(before_open=lambda: seen.append(self.active())):
            self.mint()
        self.assertEqual(seen, [1, 1])
        with self.queue._db() as db:
            pins = db.execute('''SELECT state FROM source_reader_pins
                WHERE job_id=? AND operation='read' ''', (self.job_id,)).fetchall()
        self.assertEqual([pin['state'] for pin in pins], ['released', 'released'])

    # A stalled open or read against freeze, release and Queue.close.

    def stalled_against_freeze(self, stage):
        with self.blocked(stage) as (entered, release):
            thread, outcome = self.mint_in_background()
            self.assertTrue(entered.wait(2))
            self.freeze('intent')
            self.freeze('drain')
            with self.assertRaises(Conflict):
                self.freeze('seal')
            release.set()
            self.assertIsInstance(self.finish(thread, outcome), CoordinatorFrozen)
        self.assert_nothing_minted_and_all_retained()
        self.assertEqual(self.seal_when_drained()['phase'], 'frozen')
        self.freeze('resume', healthy=True)

    def stalled_against_release(self, stage):
        with self.blocked(stage) as (entered, release):
            thread, outcome = self.mint_in_background()
            self.assertTrue(entered.wait(2))
            self.readers.fence(self.job_id, self.generation, 'mint-release')
            with self.assertRaisesRegex(SourceReaderError, 'source_operation_busy'):
                self.readers.reserve_release(self.job_id, self.generation, 'mint-release', authorize=lambda _db: None)
            release.set()
            self.assertIsInstance(self.finish(thread, outcome), SourceReaderError)
        self.assert_nothing_minted_and_all_retained()
        self.assertEqual(self.active(), 0)
        self.readers.reserve_release(self.job_id, self.generation, 'mint-release', authorize=lambda _db: None).close()

    def stalled_against_queue_close(self, stage):
        with self.blocked(stage) as (entered, release):
            thread, outcome = self.mint_in_background()
            self.assertTrue(entered.wait(2))
            with self.assertRaises(SourceReaderError):
                self.queue.close()
            release.set()
            self.assertIsInstance(self.finish(thread, outcome), dict)
        self.assertEqual(self.grants(), 1)
        self.assertEqual(self.evidence(), self.retained)

    def test_blocked_open_keeps_freeze_from_sealing(self):
        self.stalled_against_freeze('before_open')

    def test_blocked_open_keeps_release_from_starting(self):
        self.stalled_against_release('before_open')

    def test_blocked_open_keeps_coordinator_authority(self):
        self.stalled_against_queue_close('before_open')

    def test_blocked_read_keeps_freeze_from_sealing(self):
        self.stalled_against_freeze('before_read')

    def test_blocked_read_keeps_release_from_starting(self):
        self.stalled_against_release('before_read')

    def test_blocked_read_keeps_coordinator_authority(self):
        self.stalled_against_queue_close('before_read')

    # A stalled close against the same three.

    def test_blocked_close_is_drained_before_freeze_seals(self):
        with self.blocked_close() as (entered, release, finished):
            thread, outcome = self.mint_in_background()
            self.assertTrue(entered.wait(2))
            sealed = []

            def freeze():
                self.freeze('intent')
                self.freeze('drain')
                self.seal_when_drained()
                sealed.append(finished.is_set())
            freezer = threading.Thread(target=freeze)
            freezer.start()
            self.addCleanup(freezer.join, 10)
            freezer.join(0.5)
            self.assertTrue(freezer.is_alive(), 'freeze sealed with a source close outstanding')
            release.set()
            result = self.finish(thread, outcome)
            freezer.join(10)
        self.assertEqual(sealed, [True])
        self.assert_outcome_retained(result)
        self.freeze('resume', healthy=True)

    def test_blocked_close_is_drained_before_release_starts(self):
        with self.blocked_close() as (entered, release, finished):
            thread, outcome = self.mint_in_background()
            self.assertTrue(entered.wait(2))
            started = []

            def run_release():
                self.readers.fence(self.job_id, self.generation, 'mint-release')
                deadline = time.monotonic() + 5
                while True:
                    try:
                        handle = self.readers.reserve_release(self.job_id, self.generation, 'mint-release',
                            authorize=lambda _db: None)
                    except SourceReaderError:
                        if time.monotonic() > deadline:
                            raise
                        time.sleep(0.02)
                        continue
                    started.append(finished.is_set())
                    handle.close()
                    return
            releaser = threading.Thread(target=run_release)
            releaser.start()
            self.addCleanup(releaser.join, 10)
            releaser.join(0.5)
            self.assertTrue(releaser.is_alive(), 'release started with a source close outstanding')
            release.set()
            result = self.finish(thread, outcome)
            releaser.join(10)
        self.assertEqual(started, [True])
        self.assert_outcome_retained(result)

    def test_blocked_close_is_not_outlived_by_queue_close(self):
        """Queue.close may wait or refuse; it may never finish ahead of the close."""
        with self.blocked_close() as (entered, release, finished):
            thread, outcome = self.mint_in_background()
            self.assertTrue(entered.wait(2))
            closing = []

            def close_queue():
                try:
                    self.queue.close()
                    closing.append(('closed', finished.is_set()))
                except (SourceReaderError, RuntimeError, CoordinatorFrozen):
                    closing.append(('refused', finished.is_set()))
            closer = threading.Thread(target=close_queue)
            closer.start()
            self.addCleanup(closer.join, 10)
            closer.join(0.5)
            release.set()
            self.finish(thread, outcome)
            closer.join(10)
        self.assertEqual(len(closing), 1)
        self.assertNotEqual(closing[0], ('closed', False), 'Queue.close finished with a source close outstanding')

    # An unknown close keeps its authority.

    def test_unknown_close_retains_the_pin_and_coordinator_authority(self):
        with self.unknown_close():
            with self.assertRaisesRegex(*self.unknown_close_error):
                self.mint()
        self.assert_nothing_minted_and_all_retained()
        held = self.held_handle()
        self.assert_authority_retained()
        self.reconcile(held)
        self.assertEqual(self.evidence(), self.retained)

    def test_exact_retry_after_an_unknown_close_keeps_that_pin(self):
        with self.unknown_close():
            with self.assertRaisesRegex(*self.unknown_close_error):
                self.mint()
        grant = self.mint()
        self.assertEqual(self.store.private_transfer_grant(self.job_id), grant)
        self.assertEqual(self.active(), 1)
        self.reconcile(self.held_handle())

    # Reauthorization of the exact claim after each open.

    def test_cancelled_job_after_open_closes_the_descriptor(self):
        self.refused_after_open(lambda: self.execute('UPDATE jobs SET status=? WHERE id=?',
            'cancelled', self.job_id), 'source grant authority changed during verification')

    def test_replaced_claim_after_open_closes_the_descriptor(self):
        self.refused_after_open(lambda: self.execute(
            'UPDATE reference_claims SET instance_id=? WHERE job_id=?', 'other-instance', self.job_id),
            'source grant authority changed during verification')

    def test_source_no_longer_accepted_after_open_closes_the_descriptor(self):
        self.refused_after_open(lambda: self.execute(
            "UPDATE reference_source_intakes SET source_state='held' WHERE job_id=?", self.job_id),
            'source grant authority changed during verification',
            restore=lambda: self.execute(
                "UPDATE reference_source_intakes SET source_state='accepted' WHERE job_id=?", self.job_id))

    def test_held_node_after_open_closes_the_descriptor(self):
        self.refused_after_open(lambda: self.execute("UPDATE nodes SET state='held' WHERE id='cluster'"),
            'renewal_authority_unavailable')

    def test_held_node_after_the_last_open_mints_nothing(self):
        self.refused_after_open(lambda: self.execute("UPDATE nodes SET state='held' WHERE id='cluster'"),
            'renewal_authority_unavailable', on_open=2, opens=2)

    def test_fence_after_open_closes_the_descriptor(self):
        self.refused_after_open(lambda: self.readers.fence(self.job_id, self.generation, 'mint-release'),
            'source_release_fenced')

    # Admission refuses before any NAS open.

    def test_coordinator_freeze_refuses_before_any_open(self):
        self.freeze('intent')

        def refuse():
            raise AssertionError('NAS opened during a freeze')
        with self.spool_calls(before_open=refuse):
            with self.assertRaises(CoordinatorFrozen):
                self.mint()
        self.assert_nothing_minted_and_all_retained()
        self.freeze('abort', healthy=True)
        self.mint()
        self.assertEqual(self.grants(), 1)

    def test_running_source_audit_refuses_before_any_open(self):
        audit = self.readers.reserve(self.job_id, self.generation, 'source-audit',
            authorize=lambda _db: None, operation='source-audit')

        def refuse():
            raise AssertionError('NAS opened during a source audit')
        with self.spool_calls(before_open=refuse):
            with self.assertRaisesRegex(SourceReaderError, 'source_operation_busy'):
                self.mint()
        audit.close()
        self.assert_nothing_minted_and_all_retained()
        self.mint()
        self.assertEqual(self.grants(), 1)


class MintControlCases:
    """Controls: expected to pass with or without registered reads."""

    def test_identical_mint_that_lost_the_race_returns_the_winners_grant(self):
        """Guards the one custody field a mint lets change; see the mutation run."""
        reached, release = threading.Event(), threading.Event()
        winner_thread, loser_opens = threading.current_thread(), []

        def opening():
            if threading.current_thread() is not winner_thread:
                loser_opens.append(True)
                if len(loser_opens) == 2:
                    reached.set()
                    if not release.wait(5):
                        raise AssertionError('blocked verifier was not released')
        with self.spool_calls(before_open=opening):
            thread, outcome = self.mint_in_background()
            self.assertTrue(reached.wait(2))
            winner = self.mint()
            release.set()
            self.assertEqual(self.finish(thread, outcome), winner)
        self.assertEqual(self.grants(), 1)

    def test_draining_node_still_mints_for_the_existing_claim(self):
        self.execute("UPDATE nodes SET state='draining' WHERE id='cluster'")
        self.mint()
        self.assertEqual(self.grants(), 1)

    def test_every_byte_of_every_granted_object_is_read(self):
        counted, real_read = [], os.read
        tracked = set()

        def opened(descriptor):
            tracked.add(descriptor)

        def reading(descriptor, size):
            data = real_read(descriptor, size)
            if descriptor in tracked:
                counted.append(len(data))
                if not data:  # This number may be reused once the hash ends.
                    tracked.discard(descriptor)
            return data
        with self.spool_calls(after_open=opened), patch.object(os, 'read', reading):
            self.mint()
        self.assertEqual(sum(counted), self.expected_bytes())

    def test_changed_last_byte_of_the_same_size_is_refused(self):
        with self.queue._db() as db:
            first = db.execute('''SELECT object_id FROM reference_source_object_receipts WHERE job_id=?
                AND object_kind='source' ORDER BY object_id''', (self.job_id,)).fetchone()[0]
        target = self.spool.root / self.spool.storage_key(self.job_id, self.generation, first)
        body = bytearray(target.read_bytes())
        body[-1] ^= 1
        target.write_bytes(bytes(body))
        with self.assertRaisesRegex(Conflict, 'source transfer grant could not be validated'):
            self.mint()
        self.assertEqual(self.grants(), 0)
        self.assertEqual(self.active(), 0)

    def test_intake_already_needs_linux_readers_before_a_job_can_reach_mint(self):
        for operation in ('source-put', 'source-receipt', 'source-complete'):
            with patch('processing.reference_source_readers.sys.platform', 'win32'):
                with self.assertRaisesRegex(SourceReaderError, 'linux_source_readers_required'):
                    with self.store.source_operation(self.job_id, generation=self.generation, operation=operation):
                        pass


class WholeObjectHooks(MintHarness):
    source_range_v2 = False
    unknown_close_error = (Conflict, 'source transfer grant could not be validated')

    def expected_bytes(self):
        return sum(len(body) for body in self.granted_bodies())

    def granted_bodies(self):
        manifest = self.table('reference_jobs')[0]
        import json
        objects = json.loads(manifest['input_json'])['manifest']['objects']
        return [(self.spool.root / self.spool.storage_key(self.job_id, self.generation, item['id'])).read_bytes()
                for item in objects]

    @contextmanager
    def blocked_close(self):
        real = spool_module._close_owned_fd
        entered, release, finished = threading.Event(), threading.Event(), threading.Event()
        stalled = []

        def closing(descriptor):
            if stalled:
                return real(descriptor)
            stalled.append(True)
            entered.set()
            if not release.wait(5):
                raise AssertionError('blocked close was not released')
            real(descriptor)
            finished.set()
        with patch.object(spool_module, '_close_owned_fd', closing):
            try:
                yield entered, release, finished
            finally:
                release.set()

    def unknown_close(self):
        real, failed = spool_module._close_owned_fd, []

        def closing(descriptor):
            real(descriptor)
            if not failed:
                failed.append(True)
                raise SourceSpoolCloseUncertain()
        return patch.object(spool_module, '_close_owned_fd', closing)


class RangeHooks(MintHarness):
    source_range_v2 = True
    unknown_close_error = (OSError, 'synthetic close outcome unknown')

    def expected_bytes(self):
        return sum(len(body) for body in self.granted_bodies())

    granted_bodies = WholeObjectHooks.granted_bodies

    def spool_open(self, side_effect):
        return patch.object(self.spool, 'open_verified_object', side_effect=side_effect)

    @contextmanager
    def blocked_close(self):
        real = self.spool.open_verified_object
        entered, release, finished = threading.Event(), threading.Event(), threading.Event()
        wrapped = []

        def opened(**kwargs):
            reader, metadata = real(**kwargs)
            if not wrapped:
                wrapped.append(True)
                reader = _BlockedClose(reader, entered, release, finished)
            return reader, metadata
        with self.spool_open(opened):
            try:
                yield entered, release, finished
            finally:
                release.set()

    def unknown_close(self):
        real, wrapped = self.spool.open_verified_object, []

        def opened(**kwargs):
            reader, metadata = real(**kwargs)
            if not wrapped:
                wrapped.append(True)
                reader = _UnknownClose(reader)
            return reader, metadata
        return self.spool_open(opened)


class WholeObjectMintIoTests(MintLifecycleCases, WholeObjectHooks, unittest.TestCase):
    def test_uncertain_close_after_a_failed_hash_retains_the_pin(self):
        real, failed = spool_module._close_owned_fd, []

        def closing(descriptor):
            real(descriptor)
            if not failed:
                failed.append(True)
                raise SourceSpoolCloseUncertain()
        with patch.object(self.spool, 'chunk_bytes', 1), patch.object(spool_module, '_close_owned_fd', closing):
            with self.assertRaisesRegex(Conflict, 'source transfer grant could not be validated'):
                self.mint()
        held = self.held_handle()
        self.assert_authority_retained()
        self.reconcile(held)


class RangeMintIoTests(MintLifecycleCases, RangeHooks, unittest.TestCase):
    def test_uncertain_close_inside_the_spool_open_retains_the_pin(self):
        real, raised = self.spool.open_verified_object, []

        def opened(**kwargs):
            if not raised:
                raised.append(True)
                raise SourceSpoolCloseUncertain()
            return real(**kwargs)
        with self.spool_open(opened):
            with self.assertRaisesRegex(Conflict, 'source transfer grant could not be validated'):
                self.mint()
        self.assert_nothing_minted_and_all_retained()
        held = self.held_handle()
        self.assert_authority_retained()
        self.reconcile(held)


class WholeObjectMintControls(MintControlCases, WholeObjectHooks, unittest.TestCase):
    def test_normal_mint_returns_the_same_grant(self):
        grant = self.mint()
        self.assertEqual(set(grant), {'reservationId', 'generation', 'expiresAt', 'manifestSha256', 'byteBudget',
            'nonce', 'jobId', 'environment', 'objectIds', 'token'})
        self.assertEqual(self.store.private_transfer_grant(self.job_id), grant)
        self.assertEqual(self.table('reference_source_object_pins'), [])
        self.assertEqual(self.active(), 0)

    def test_inode_changed_during_the_hash_is_refused(self):
        """verify_object's stability check survives; open_verified_object has none."""
        path, touched = self.object_paths()[0], []

        def touch():
            if not touched:
                touched.append(True)
                for candidate in self.object_paths():
                    os.utime(candidate, ns=(1, 1))
        with self.spool_calls(before_read=touch):
            with self.assertRaisesRegex(Conflict, 'source transfer grant could not be validated'):
                self.mint()
        self.assertEqual(self.grants(), 0)
        self.assertEqual(self.active(), 0)
        self.assertEqual(path.read_bytes(), self.retained['objects'][str(path)])

    def test_verification_still_goes_through_verify_object(self):
        real, calls = self.spool.verify_object, []

        def verified(**kwargs):
            calls.append(kwargs['object_id'])
            return real(**kwargs)
        with patch.object(self.spool, 'verify_object', side_effect=verified):
            self.mint()
        self.assertEqual(len(calls), 2)


class RangeMintControls(MintControlCases, RangeHooks, unittest.TestCase):
    def test_normal_mint_returns_the_same_grant_and_pins_the_real_inodes(self):
        grant = self.mint()
        self.assertEqual(set(grant), {'protocol', 'reservationId', 'generation', 'manifestSha256', 'sourceVersion',
            'objectIds', 'chunkBytes', 'requestBudget', 'byteBudget', 'nonce', 'attemptBudget', 'expiresAt',
            'jobId', 'environment', 'token'})
        self.assertEqual(self.store.private_transfer_grant(self.job_id), grant)
        pins = self.table('reference_source_object_pins')
        self.assertEqual({pin['object_id'] for pin in pins}, set(grant['objectIds']))
        for pin in pins:
            info = os.stat(self.spool.root / self.spool.storage_key(self.job_id, pin['generation'], pin['object_id']))
            self.assertEqual((pin['device'], pin['inode'], pin['byte_size'], pin['mode'], pin['link_count'],
                pin['owner_uid'], pin['mtime_ns'], pin['ctime_ns']),
                (str(info.st_dev), str(info.st_ino), info.st_size, info.st_mode, info.st_nlink,
                 info.st_uid, info.st_mtime_ns, info.st_ctime_ns))
        self.assertEqual(self.active(), 0)

    def test_inode_shared_after_open_is_refused_and_closed(self):
        real = self.spool.open_verified_object
        link = self.root / 'second-link'

        def opened(**kwargs):
            result = real(**kwargs)
            os.link(self.spool.root / self.spool.storage_key(self.job_id, self.generation, kwargs['object_id']), link)
            return result
        with self.spool_open(opened):
            with self.assertRaisesRegex(Conflict, 'source object inode is not privately retained'):
                self.mint()
        self.assertEqual(self.grants(), 0)
        self.assertEqual(self.active(), 0)


if __name__ == '__main__':
    unittest.main()
