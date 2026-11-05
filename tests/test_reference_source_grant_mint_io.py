"""The initial source grant's NAS reads under the coordinator's own lifecycle.

Mirrors RenewalSourceIoTests for ``mint_source_transfer_grant``. The coordinator
and spool are real; only the descriptor's open and close are stalled or failed.
"""
from contextlib import contextmanager
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import uuid

from processing.coordinator_freeze import CoordinatorFrozen
from processing.maintenance import Conflict
from processing.reference_source_readers import SourceReaderError
from processing.source_spool import SourceSpool, SourceSpoolCloseUncertain
from tests.test_reference_source_grant_renewal import _BlockedClose, _UnknownClose


class MintHarness(unittest.TestCase):
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
            self.directory.name, self.spool, 'mint', source_range_v2=True)
        self.addCleanup(lambda: self.queue.close())
        self.job_id = job['jobId']
        self.generation = self.store.get(self.job_id, private=True)['transferRequest']['generation']
        self.readers = self.store.source_readers()
        self.plan = str(uuid.uuid4())

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

    def grants(self):
        with self.queue._db() as db:
            return db.execute('SELECT COUNT(*) FROM reference_transfer_grants WHERE job_id=?',
                (self.job_id,)).fetchone()[0]

    def active(self):
        return self.readers.active_count(self.job_id, self.generation)

    def assert_one_grant_or_none(self, result):
        self.assertEqual(self.grants(), 0 if isinstance(result, BaseException) else 1, repr(result))

    def spool_open(self, side_effect):
        return patch.object(self.spool, 'open_verified_object', side_effect=side_effect)


class MintSourceIoTests(MintHarness):
    """Each of these fails while mint opens the NAS outside the registered readers."""

    @contextmanager
    def blocked_open(self):
        real = self.spool.open_verified_object
        entered, release = threading.Event(), threading.Event()

        def stalled(**kwargs):
            if not entered.is_set():
                entered.set()
                if not release.wait(5):
                    raise AssertionError('blocked open was not released')
            return real(**kwargs)
        with self.spool_open(stalled):
            try:
                yield entered, release
            finally:
                release.set()

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

    def refused_after_first_open(self, change, error):
        """A claim change seen after the open refuses that object and opens no other."""
        real, opened_files = self.spool.open_verified_object, []

        def opened(**kwargs):
            reader, metadata = real(**kwargs)
            opened_files.append(reader)
            change()
            return reader, metadata
        with self.spool_open(opened):
            with self.assertRaisesRegex(Conflict, error):
                self.mint()
        self.assertEqual(len(opened_files), 1, 'a further NAS open followed the lost claim')
        self.assertTrue(opened_files[0].closed)
        self.assertEqual(self.active(), 0)
        self.assertEqual(self.grants(), 0)

    # Every descriptor is registered.

    def test_each_open_happens_under_an_active_pin(self):
        real, seen = self.spool.open_verified_object, []

        def opened(**kwargs):
            seen.append((kwargs['object_id'], self.active()))
            return real(**kwargs)
        with self.spool_open(opened):
            self.mint()
        self.assertEqual([count for _object_id, count in seen], [1, 1])
        with self.queue._db() as db:
            pins = db.execute('''SELECT object_id,state,operation FROM source_reader_pins
                WHERE job_id=? AND operation='read' ''', (self.job_id,)).fetchall()
        self.assertEqual(sorted(tuple(pin) for pin in pins),
            sorted((object_id, 'released', 'read') for object_id, _count in seen))

    # Open and close against freeze, release and Queue.close.

    def test_blocked_open_keeps_freeze_from_sealing(self):
        with self.blocked_open() as (entered, release):
            thread, outcome = self.mint_in_background()
            self.assertTrue(entered.wait(2))
            self.freeze('intent')
            self.freeze('drain')
            with self.assertRaises(Conflict):
                self.freeze('seal')
            release.set()
            self.assertIsInstance(self.finish(thread, outcome), CoordinatorFrozen)
        self.assertEqual(self.grants(), 0)
        self.assertEqual(self.seal_when_drained()['phase'], 'frozen')
        self.freeze('resume', healthy=True)

    def test_blocked_open_keeps_release_from_starting(self):
        with self.blocked_open() as (entered, release):
            thread, outcome = self.mint_in_background()
            self.assertTrue(entered.wait(2))
            self.readers.fence(self.job_id, self.generation, 'mint-release')
            with self.assertRaisesRegex(SourceReaderError, 'source_operation_busy'):
                self.readers.reserve_release(self.job_id, self.generation, 'mint-release', authorize=lambda _db: None)
            release.set()
            self.assertIsInstance(self.finish(thread, outcome), SourceReaderError)
        self.assertEqual(self.grants(), 0)
        self.assertEqual(self.active(), 0)
        self.readers.reserve_release(self.job_id, self.generation, 'mint-release', authorize=lambda _db: None).close()

    def test_blocked_open_keeps_coordinator_authority(self):
        with self.blocked_open() as (entered, release):
            thread, outcome = self.mint_in_background()
            self.assertTrue(entered.wait(2))
            with self.assertRaises(SourceReaderError):
                self.queue.close()
            release.set()
            self.assertIsInstance(self.finish(thread, outcome), dict)
        self.assertEqual(self.grants(), 1)

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
        self.assert_one_grant_or_none(result)
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
        self.assert_one_grant_or_none(result)

    def test_blocked_close_is_drained_before_queue_close(self):
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
            self.assertTrue(closer.is_alive(), 'Queue.close returned with a source close outstanding')
            release.set()
            self.finish(thread, outcome)
            closer.join(10)
        self.assertTrue(closing[0][1], closing)

    # An unknown close keeps its authority.

    def test_unknown_close_retains_the_pin_and_coordinator_authority(self):
        with self.unknown_close():
            with self.assertRaisesRegex(OSError, 'synthetic close outcome unknown'):
                self.mint()
        self.assertEqual(self.grants(), 0)
        held = self.held_handle()
        self.assert_authority_retained()
        self.reconcile(held)

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
        self.assertEqual(self.grants(), 0)
        held = self.held_handle()
        self.assert_authority_retained()
        self.reconcile(held)

    def test_exact_retry_after_an_unknown_close_keeps_that_pin(self):
        with self.unknown_close():
            with self.assertRaises(OSError):
                self.mint()
        grant = self.mint()
        self.assertEqual(self.store.private_transfer_grant(self.job_id), grant)
        self.assertEqual(self.active(), 1)
        self.reconcile(self.held_handle())

    # Reauthorization of the exact claim after each open.

    def test_cancelled_job_after_open_closes_the_descriptor(self):
        self.refused_after_first_open(lambda: self.execute('UPDATE jobs SET status=? WHERE id=?',
            'cancelled', self.job_id), 'source grant authority changed during verification')

    def test_replaced_claim_after_open_closes_the_descriptor(self):
        self.refused_after_first_open(lambda: self.execute(
            'UPDATE reference_claims SET instance_id=? WHERE job_id=?', 'other-instance', self.job_id),
            'source grant authority changed during verification')

    def test_held_node_after_open_closes_the_descriptor(self):
        self.refused_after_first_open(lambda: self.execute("UPDATE nodes SET state='held' WHERE id='cluster'"),
            'renewal_authority_unavailable')

    def test_fence_after_open_closes_the_descriptor(self):
        self.refused_after_first_open(lambda: self.readers.fence(self.job_id, self.generation, 'mint-release'),
            'source_release_fenced')

    # Admission refuses before any NAS open.

    def test_running_source_audit_refuses_before_any_open(self):
        audit = self.readers.reserve(self.job_id, self.generation, 'source-audit',
            authorize=lambda _db: None, operation='source-audit')
        with self.spool_open(AssertionError('NAS opened during a source audit')):
            with self.assertRaisesRegex(SourceReaderError, 'source_operation_busy'):
                self.mint()
        audit.close()
        self.assertEqual(self.grants(), 0)
        self.mint()
        self.assertEqual(self.grants(), 1)


class MintSourceIoControls(MintHarness):
    """Controls: expected to pass with or without registered reads."""

    def test_normal_mint_returns_the_same_grant_and_pins_the_real_inodes(self):
        grant = self.mint()
        self.assertEqual(set(grant), {'protocol', 'reservationId', 'generation', 'manifestSha256', 'sourceVersion',
            'objectIds', 'chunkBytes', 'requestBudget', 'byteBudget', 'nonce', 'attemptBudget', 'expiresAt',
            'jobId', 'environment', 'token'})
        self.assertEqual(self.store.private_transfer_grant(self.job_id), grant)
        with self.queue._db() as db:
            pins = db.execute('SELECT * FROM reference_source_object_pins WHERE job_id=?', (self.job_id,)).fetchall()
        self.assertEqual({pin['object_id'] for pin in pins}, set(grant['objectIds']))
        for pin in pins:
            info = os.stat(self.spool.root / self.spool.storage_key(self.job_id, pin['generation'], pin['object_id']))
            self.assertEqual((pin['device'], pin['inode'], pin['byte_size'], pin['mode'], pin['link_count'],
                pin['owner_uid'], pin['mtime_ns'], pin['ctime_ns']),
                (str(info.st_dev), str(info.st_ino), info.st_size, info.st_mode, info.st_nlink,
                 info.st_uid, info.st_mtime_ns, info.st_ctime_ns))
        self.assertEqual(self.active(), 0)

    def test_identical_mint_that_lost_the_race_returns_the_winners_grant(self):
        """Guards the one custody field a mint lets change; see the mutation run."""
        real = self.spool.open_verified_object
        reached, release = threading.Event(), threading.Event()
        last = self.store.get(self.job_id, private=True)['transferRequest']['objectIds'][-1]
        winner_thread = threading.current_thread()

        def opened(**kwargs):
            if threading.current_thread() is not winner_thread and kwargs['object_id'] == last:
                reached.set()
                if not release.wait(5):
                    raise AssertionError('blocked verifier was not released')
            return real(**kwargs)
        with self.spool_open(opened):
            thread, outcome = self.mint_in_background()
            self.assertTrue(reached.wait(2))
            winner = self.mint()
            release.set()
            self.assertEqual(self.finish(thread, outcome), winner)
        self.assertEqual(self.grants(), 1)

    def test_coordinator_freeze_refuses_before_any_open(self):
        self.freeze('intent')
        with self.spool_open(AssertionError('NAS opened during a freeze')):
            with self.assertRaises(CoordinatorFrozen):
                self.mint()
        self.assertEqual(self.grants(), 0)
        self.freeze('abort', healthy=True)
        self.mint()
        self.assertEqual(self.grants(), 1)

    def test_draining_node_still_mints_for_the_existing_claim(self):
        self.execute("UPDATE nodes SET state='draining' WHERE id='cluster'")
        self.mint()
        self.assertEqual(self.grants(), 1)

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
