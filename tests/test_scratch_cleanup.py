import hashlib
import concurrent.futures
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from processing.scratch_cleanup import ScratchCleanupExecutor, CleanupRefused, CleanupUnsupported
from processing.scratch_policy import ScratchLedger, PolicyConflict


class CleanupPlatformTests(unittest.TestCase):
    def test_unsupported_platform_refuses_before_touching_ledger(self):
        class UnusedLedger:
            root = Path('/unused-synthetic-root')
            def begin_cleanup(self, *args, **kwargs):
                raise AssertionError('unsupported platform touched ledger')
        with patch('processing.scratch_cleanup.sys.platform', 'win32'):
            with self.assertRaises(CleanupUnsupported):
                ScratchCleanupExecutor(UnusedLedger()).execute('r1', 'o1', generation=1,
                    sha256='a'*64, byte_size=3, relative_path='jobs/r1/o1.bin')


@unittest.skipUnless(sys.platform.startswith('linux'), 'executor requires Linux descriptor and fsync semantics')
class ScratchCleanupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.now = 100
        self.ledger = self.open_ledger()
        self.executor = ScratchCleanupExecutor(self.ledger)
        self.folder = self.root / 'jobs' / 'r1'
        self.folder.mkdir(parents=True)
        self.first = self.folder / 'o1.bin'
        self.second = self.folder / 'o2.bin'
        self.first.write_bytes(b'abc')
        self.second.write_bytes(b'defg')
        self.sha = hashlib.sha256(b'abc').hexdigest()
        self.ledger.reserve(job_id='j1', generation=1, reservation_id='r1',
            manifest_sha256='a'*64, input_bytes=0, output_budget=100,
            environment='staging', ttl_seconds=100, worker_ready=True)
        self.ledger.set_engine_state('r1', 'engine_started')
        self.ledger.register_result('r1', job_id='j1', generation=1,
            manifest_sha256='a'*64, pipeline_sha256='b'*64,
            result_manifest_sha256='c'*64, objects=[
                dict(object_id='o1', bytes=3, sha256=self.sha, relative_path='jobs/r1/o1.bin'),
                dict(object_id='o2', bytes=4, sha256=hashlib.sha256(b'defg').hexdigest(), relative_path='jobs/r1/o2.bin')])
        self.ledger.set_engine_state('r1', 'terminal', terminal_attested=True)
        self.ledger.accept_receipt('r1', receipt_id='ack1', storage_receipt_id='nas1',
            job_id='j1', generation=1, manifest_sha256='a'*64, pipeline_sha256='b'*64,
            result_manifest_sha256='c'*64, accepted_object_ids=['o1'])

    def open_ledger(self):
        return ScratchLedger(self.root / 'private' / 'ledger.sqlite', self.root,
            capacity_probe=lambda _: 10000, clock=lambda: self.now,
            safety_floor_bytes=0, cleanup_grace_seconds=0)

    def execute(self, **updates):
        values = dict(generation=1, sha256=self.sha, byte_size=3, relative_path='jobs/r1/o1.bin')
        values.update(updates)
        return self.executor.execute('r1', 'o1', **values)

    def test_partial_acceptance_removes_only_exact_object(self):
        result = self.execute()
        self.assertEqual(result['state'], 'cleaned')
        self.assertFalse(self.first.exists())
        self.assertEqual(self.second.read_bytes(), b'defg')
        self.assertTrue(self.folder.is_dir())
        with self.assertRaises(PolicyConflict):
            self.executor.execute('r1', 'o2', generation=1,
                sha256=hashlib.sha256(b'defg').hexdigest(), byte_size=4, relative_path='jobs/r1/o2.bin')

    def test_active_pin_prevents_fence_and_any_file_mutation(self):
        self.ledger.set_pin('r1', 'o1', 'reader', kind='read', state='active')
        with self.assertRaises(PolicyConflict):
            self.execute()
        self.assertEqual(self.first.read_bytes(), b'abc')
        self.assertFalse((self.root / '.cleanup-journal').exists())

    def test_guarded_failure_hold_survives_restart_and_blocks_real_executor(self):
        # This fixture already has a terminal reservation and an accepted
        # object: model a ledger incorrectly marked terminal by an older build.
        self.assertEqual(len(self.ledger.cleanup_candidates()), 1)
        self.ledger.hold_guarded_failure_cleanup('r1', job_id='j1', generation=1)
        self.ledger = self.open_ledger()
        self.executor = ScratchCleanupExecutor(self.ledger)
        self.assertEqual(self.ledger.cleanup_candidates(), [])
        with self.assertRaises(PolicyConflict):
            self.execute()
        self.assertEqual(self.first.read_bytes(), b'abc')

    def test_guarded_failure_hold_refuses_existing_cleanup_intent(self):
        intent = self.ledger.begin_cleanup('r1', 'o1', generation=1, sha256=self.sha)
        with self.assertRaisesRegex(PolicyConflict, 'existing cleanup intent'):
            self.ledger.hold_guarded_failure_cleanup('r1', job_id='j1', generation=1)
        self.assertEqual(self.first.read_bytes(), b'abc')
        self.assertEqual(self.ledger.cleanup_intent('r1', 'o1')['intent_token'], intent['intent_token'])

    def test_wrong_bytes_or_generation_refuses_without_unlink(self):
        with self.assertRaises(CleanupRefused):
            self.execute(byte_size=4)
        token = self.ledger.cleanup_intent('r1', 'o1')['intent_token']
        with self.assertRaises(CleanupRefused):
            self.execute(generation=2, intent_token=token)
        self.assertEqual(self.first.read_bytes(), b'abc')

    def test_content_mismatch_retains_original_and_fence(self):
        self.first.write_bytes(b'xyz')
        with self.assertRaises(CleanupRefused):
            self.execute()
        self.assertEqual(self.first.read_bytes(), b'xyz')
        self.assertNotEqual(self.ledger.cleanup_intent('r1', 'o1')['state'], 'cleaned')

    def test_symlink_and_hardlink_refused(self):
        outside = self.root / 'unrelated.bin'
        outside.write_bytes(b'abc')
        self.first.unlink()
        self.first.symlink_to(outside)
        with self.assertRaises(ValueError):
            self.execute()
        self.assertEqual(outside.read_bytes(), b'abc')
        self.first.unlink()
        self.first.hardlink_to(outside)
        with self.assertRaises(CleanupRefused):
            self.execute()
        self.assertEqual(self.first.read_bytes(), b'abc')
        self.assertEqual(outside.read_bytes(), b'abc')

    def test_source_name_swap_is_quarantined_but_never_deleted(self):
        original_persist = self.executor._persist
        replacement = self.folder / 'replacement.bin'
        replacement.write_bytes(b'xyz')
        def swap(*args):
            original_persist(*args)
            if args[-1] == 'verified':
                os.replace(replacement, self.first)
        with patch.object(self.executor, '_persist', swap):
            with self.assertRaises(CleanupRefused):
                self.execute()
        token = self.ledger.cleanup_intent('r1', 'o1')['intent_token']
        retained = self.root / '.cleanup-journal' / (token + '.object')
        self.assertEqual(retained.read_bytes(), b'xyz')
        self.assertNotEqual(self.ledger.cleanup_intent('r1', 'o1')['state'], 'cleaned')

    def test_missing_source_without_verified_journal_is_not_cleanup(self):
        intent = self.ledger.begin_cleanup('r1', 'o1', generation=1, sha256=self.sha)
        self.first.unlink()
        with self.assertRaises(FileNotFoundError):
            self.execute(intent_token=intent['intent_token'])
        self.assertNotEqual(self.ledger.cleanup_intent('r1', 'o1')['state'], 'cleaned')

    def test_directory_fsync_failure_preflight_prevents_deletion(self):
        with patch.object(self.executor, '_sync', side_effect=CleanupUnsupported('synthetic unsupported fsync')):
            with self.assertRaises(CleanupUnsupported):
                self.execute()
        self.assertEqual(self.first.read_bytes(), b'abc')

    def test_restart_after_unlink_before_ledger_ack_reconciles_verified_journal(self):
        with patch.object(self.ledger, 'mark_cleaned', side_effect=RuntimeError('synthetic crash before ledger ACK')):
            with self.assertRaises(RuntimeError):
                self.execute()
        self.assertFalse(self.first.exists())
        self.ledger = self.open_ledger()
        self.executor = ScratchCleanupExecutor(self.ledger)
        token = self.ledger.cleanup_intent('r1', 'o1')['intent_token']
        self.assertEqual(self.execute(intent_token=token)['state'], 'cleaned')
        self.assertEqual(self.second.read_bytes(), b'defg')

    def test_restart_at_each_durable_preunlink_phase(self):
        # Separate fresh synthetic trees ensure each replay exercises real I/O.
        for crash_phase in ('verified', 'quarantined', 'unlinking'):
            with self.subTest(phase=crash_phase):
                if crash_phase != 'verified':
                    self.setUp()
                original_persist = self.executor._persist
                def crash(*args):
                    original_persist(*args)
                    if args[-1] == crash_phase:
                        raise RuntimeError('synthetic crash at durable phase')
                with patch.object(self.executor, '_persist', crash):
                    with self.assertRaises(RuntimeError):
                        self.execute()
                token = self.ledger.cleanup_intent('r1', 'o1')['intent_token']
                self.ledger = self.open_ledger()
                self.executor = ScratchCleanupExecutor(self.ledger)
                self.execute(intent_token=token)
                self.assertFalse(self.first.exists())
                self.assertEqual(self.second.read_bytes(), b'defg')

    def test_replaced_parent_directory_cannot_reconcile_old_journal(self):
        with patch.object(self.ledger, 'mark_cleaned', side_effect=RuntimeError('synthetic crash')):
            with self.assertRaises(RuntimeError):
                self.execute()
        token = self.ledger.cleanup_intent('r1', 'o1')['intent_token']
        old_folder = self.folder.with_name('retained-original-directory')
        self.folder.rename(old_folder)
        self.folder.mkdir()
        with self.assertRaises(CleanupRefused):
            self.execute(intent_token=token)
        self.assertEqual((old_folder / 'o2.bin').read_bytes(), b'defg')

    def test_same_intent_concurrent_replays_unlink_exact_object_once(self):
        token = self.ledger.begin_cleanup('r1', 'o1', generation=1, sha256=self.sha)['intent_token']
        original_unlink = os.unlink
        deleted = []
        def unlink(name, **kwargs):
            deleted.append(name)
            return original_unlink(name, **kwargs)
        def execute(_):
            executor = ScratchCleanupExecutor(self.open_ledger())
            return executor.execute('r1', 'o1', generation=1, sha256=self.sha,
                byte_size=3, relative_path='jobs/r1/o1.bin', intent_token=token)
        with patch('processing.scratch_cleanup.os.unlink', unlink):
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(execute, range(2)))
        self.assertEqual([r['state'] for r in results], ['cleaned', 'cleaned'])
        self.assertEqual(deleted, [token + '.object'])
        self.assertEqual(self.second.read_bytes(), b'defg')

    def test_restart_after_unlink_before_fsync_reissues_directory_sync(self):
        original_unlink = os.unlink
        original_sync = self.executor._sync
        unlinked = False
        def unlink(*args, **kwargs):
            nonlocal unlinked
            original_unlink(*args, **kwargs)
            unlinked = True
        def sync(*args):
            if unlinked:
                raise RuntimeError('synthetic crash after unlink')
            original_sync(*args)
        with patch('processing.scratch_cleanup.os.unlink', unlink), patch.object(self.executor, '_sync', sync):
            with self.assertRaises(RuntimeError):
                self.execute()
        self.assertFalse(self.first.exists())
        self.ledger = self.open_ledger()
        self.executor = ScratchCleanupExecutor(self.ledger)
        token = self.ledger.cleanup_intent('r1', 'o1')['intent_token']
        sync_calls = []
        original_sync = self.executor._sync
        def observe_sync(*args):
            sync_calls.append(True)
            original_sync(*args)
        original_mark = self.ledger.mark_cleaned
        def mark(*args, **kwargs):
            self.assertTrue(sync_calls)
            return original_mark(*args, **kwargs)
        with patch.object(self.executor, '_sync', observe_sync), patch.object(self.ledger, 'mark_cleaned', mark):
            self.execute(intent_token=token)
        self.assertEqual(self.ledger.cleanup_intent('r1', 'o1')['state'], 'cleaned')


if __name__ == '__main__':
    unittest.main()
