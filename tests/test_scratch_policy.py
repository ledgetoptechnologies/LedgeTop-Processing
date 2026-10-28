import concurrent.futures
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import sqlite3

from processing.scratch_policy import ScratchLedger, PolicyConflict


class ScratchPolicyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.now = 100
        self.free = 1000
        self.ledger = self.open()

    def open(self):
        return ScratchLedger(self.root / 'private' / 'ledger.sqlite', self.root,
            capacity_probe=lambda _: self.free, clock=lambda: self.now,
            safety_floor_bytes=100, cleanup_grace_seconds=10)

    def reserve(self, **kwargs):
        values = dict(job_id='j1', generation=1, reservation_id='r1',
            manifest_sha256='a'*64, input_bytes=100, output_budget=300,
            environment='production', ttl_seconds=20, worker_ready=True)
        values.update(kwargs)
        return self.ledger.reserve(**values)

    def register(self):
        self.reserve()
        self.ledger.set_engine_state('r1', 'engine_started')
        self.ledger.register_result('r1', job_id='j1', generation=1,
            manifest_sha256='a'*64, pipeline_sha256='b'*64,
            result_manifest_sha256='c'*64, objects=[
                dict(object_id='o1', bytes=100, sha256='d'*64, relative_path='jobs/r1/o1.bin'),
                dict(object_id='o2', bytes=150, sha256='e'*64, relative_path='jobs/r1/o2.bin')])

    def accept(self, **kwargs):
        values = dict(receipt_id='ack1', storage_receipt_id='nas1', job_id='j1',
            generation=1, manifest_sha256='a'*64, pipeline_sha256='b'*64,
            result_manifest_sha256='c'*64, accepted_object_ids=['o1'])
        values.update(kwargs)
        self.ledger.accept_receipt('r1', **values)

    def test_readiness_identity_and_environment(self):
        with self.assertRaises(PolicyConflict):
            self.reserve(worker_ready=False)
        first = self.reserve()
        self.assertEqual(first, self.reserve())
        with self.assertRaises(PolicyConflict):
            self.reserve(environment='staging')
        with self.assertRaises(ValueError):
            self.reserve(input_bytes=True)
        with self.assertRaises(ValueError):
            self.reserve(generation=0)
        with self.assertRaises(ValueError):
            self.reserve(job_id='_invalid')

    def test_expired_retry_preserves_job_environment_after_restart(self):
        self.reserve()
        self.now = 120
        self.ledger = self.open()
        with self.assertRaises(PolicyConflict):
            self.reserve(reservation_id='r2', environment='staging')
        self.reserve(reservation_id='r2')
        with self.assertRaises(PolicyConflict):
            self.reserve(reservation_id='r3')
        self.ledger = self.open()
        with self.assertRaises(PolicyConflict):
            self.reserve(reservation_id='r4', generation=2, environment='staging')

    def test_startup_schema_and_config_are_one_full_sync_transaction(self):
        real_connect = sqlite3.connect
        observations = []

        class ObservedConnection(sqlite3.Connection):
            def execute(self, sql, parameters=()):
                if sql.strip().startswith(('CREATE', 'DROP', 'INSERT OR IGNORE INTO metadata')):
                    observations.append((self.in_transaction, super().execute('PRAGMA synchronous').fetchone()[0]))
                return super().execute(sql, parameters)

        def connect(*args, **kwargs):
            return real_connect(*args, **kwargs, factory=ObservedConnection)
        with patch('processing.scratch_policy.sqlite3.connect', connect):
            self.open()
        self.assertTrue(observations)
        self.assertTrue(all(in_transaction and sync == 2 for in_transaction, sync in observations))

    def test_concurrent_startup_rejects_conflicting_config(self):
        path = self.root / 'private' / 'concurrent.sqlite'
        def start(grace):
            try:
                ScratchLedger(path, self.root, safety_floor_bytes=100, cleanup_grace_seconds=grace)
                return True
            except PolicyConflict:
                return False
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            self.assertEqual(sum(pool.map(start, [10, 11])), 1)

    def test_concurrent_capacity(self):
        def request(index):
            try:
                self.reserve(job_id=f'j{index}', reservation_id=f'r{index}')
                return True
            except PolicyConflict:
                return False
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            outcomes = list(pool.map(request, range(8)))
        self.assertEqual(sum(outcomes), 2)

    def test_allocation_is_not_double_counted(self):
        self.reserve()
        self.free = 900  # 100 allocated bytes no longer appear in free snapshot.
        self.ledger.record_allocated('r1', 100)
        self.reserve(job_id='j2', reservation_id='r2')
        with self.assertRaises(PolicyConflict):
            self.ledger.record_allocated('r1', 99)

    def test_expiry_before_vs_after_engine(self):
        self.reserve()
        self.now = 119
        with self.assertRaises(PolicyConflict):
            self.reserve(job_id='j2', reservation_id='r2', input_bytes=600)
        self.now = 120
        self.reserve(job_id='j2', reservation_id='r2', input_bytes=600)
        with self.assertRaises(PolicyConflict):
            self.ledger.set_engine_state('r1', 'engine_started')

    def test_engine_and_uncertain_reservations_do_not_expire(self):
        self.reserve()
        self.ledger.set_engine_state('r1', 'engine_started')
        self.now = 1000
        with self.assertRaises(PolicyConflict):
            self.reserve(job_id='j2', reservation_id='r2', input_bytes=600)
        self.ledger.set_engine_state('r1', 'uncertain')
        with self.assertRaises(PolicyConflict):
            self.reserve(job_id='j2', reservation_id='r2', input_bytes=600)

    def test_clock_rollback_does_not_reopen_expired_lease(self):
        self.reserve()
        self.now = 130
        self.ledger.cleanup_candidates()  # Commit the observed clock high water mark.
        self.now = 110
        with self.assertRaises(PolicyConflict):
            self.open().set_engine_state('r1', 'engine_started')

    def test_rejected_expired_engine_start_persists_clock_and_expiry(self):
        self.reserve()  # t=100, expires=120
        self.now = 130
        with self.assertRaises(PolicyConflict):
            self.ledger.set_engine_state('r1', 'engine_started')
        self.now = 110
        with self.assertRaises(PolicyConflict):
            self.ledger.set_engine_state('r1', 'engine_started')
        self.ledger = self.open()
        with self.assertRaises(PolicyConflict):
            self.ledger.set_engine_state('r1', 'engine_started')
        self.reserve(reservation_id='r2')  # Durable safe expiry allows new identity.

    def test_ledger_cannot_be_rebound_to_different_policy_or_root(self):
        with self.assertRaises(PolicyConflict):
            ScratchLedger(self.root / 'private' / 'ledger.sqlite', self.root,
                capacity_probe=lambda _: self.free, cleanup_grace_seconds=0)

    def test_partial_acceptance_lost_ack_restart_and_tombstone(self):
        self.register()
        self.ledger.record_allocated('r1', 350)
        self.free = 650  # Allocated bytes remain occupied after unused budget release.
        self.ledger.set_engine_state('r1', 'terminal', terminal_attested=True)
        self.now = 200
        self.assertEqual(self.open().cleanup_candidates(), [])
        self.accept()
        self.accept()  # Lost response/retry preserves original accepted timestamp.
        self.now = 209
        self.assertEqual(self.ledger.cleanup_candidates(), [])
        self.now = 210
        plans = self.open().cleanup_candidates()
        self.assertEqual([p['id'] for p in plans], ['o1'])
        intent = self.ledger.begin_cleanup('r1', 'o1', generation=1, sha256='d'*64)
        with self.assertRaises(PolicyConflict):
            self.accept(result_manifest_sha256='f'*64)
        with self.assertRaises(PolicyConflict):
            self.accept(accepted_object_ids=['o2'])
        with self.assertRaises(PolicyConflict):
            self.ledger.mark_cleaned('r1', 'o1', generation=1, sha256='f'*64,
                intent_token=intent['intent_token'], executor_attested=True)
        self.ledger.mark_cleaned('r1', 'o1', generation=1, sha256='d'*64,
            intent_token=intent['intent_token'], executor_attested=True)
        self.assertEqual(self.open().cleanup_candidates(), [])
        self.accept()  # Receipt and object tombstone survive restart.
        with self.assertRaises(PolicyConflict):
            self.reserve(job_id='j2', reservation_id='r2', input_bytes=600)

    def test_terminal_attestation_releases_only_unused_budget(self):
        self.reserve()
        self.ledger.set_engine_state('r1', 'engine_started')
        self.ledger.record_allocated('r1', 100)
        self.free = 900
        with self.assertRaises(PolicyConflict):
            self.ledger.set_engine_state('r1', 'terminal')
        with self.assertRaises(PolicyConflict):
            self.reserve(job_id='j2', reservation_id='r2', input_bytes=400)
        self.ledger.set_engine_state('r1', 'terminal', terminal_attested=True)
        self.ledger = self.open()
        self.reserve(job_id='j2', reservation_id='r2', input_bytes=400)
        with self.assertRaises(PolicyConflict):
            self.ledger.record_allocated('r1', 101)
        with self.assertRaises(PolicyConflict):
            self.ledger.set_engine_state('r1', 'engine_started')
        with self.assertRaises(PolicyConflict):
            self.ledger.set_engine_state('r1', 'uncertain')

    def test_case_aliases_and_unsafe_windows_names_are_rejected(self):
        self.reserve()
        self.ledger.set_engine_state('r1', 'engine_started')
        for paths in [('jobs/r1/o.bin', 'jobs/r1/O.BIN'),
                      ('jobs/r1/a/o.bin', 'jobs/r1/A/O.BIN')]:
            with self.assertRaises(PolicyConflict):
                self.ledger.register_result('r1', job_id='j1', generation=1,
                    manifest_sha256='a'*64, pipeline_sha256='b'*64,
                    result_manifest_sha256='c'*64, objects=[
                        dict(object_id=f'o{i}', bytes=10, sha256='d'*64, relative_path=p)
                        for i, p in enumerate(paths)])
        for name in ('o.bin.', 'o.bin ', 'CON', 'nul.txt', 'LPT1.dat', 'bad?file', 'x\x00'):
            with self.assertRaises(ValueError):
                self.ledger._owned_path('r1', f'jobs/r1/{name}')

    def test_existing_hardlinks_cannot_be_registered_as_distinct_objects(self):
        self.reserve()
        self.ledger.set_engine_state('r1', 'engine_started')
        folder = self.root / 'jobs' / 'r1'
        folder.mkdir(parents=True)
        original = folder / 'original.bin'
        original.write_bytes(b'synthetic')
        alias = folder / 'alias.bin'
        try:
            alias.hardlink_to(original)
        except OSError:
            self.skipTest('hardlink creation is unavailable')
        with self.assertRaises(PolicyConflict):
            self.ledger.register_result('r1', job_id='j1', generation=1,
                manifest_sha256='a'*64, pipeline_sha256='b'*64,
                result_manifest_sha256='c'*64, objects=[
                    dict(object_id=f'o{i}', bytes=9, sha256='d'*64,
                         relative_path=f'jobs/r1/{name}')
                    for i, name in enumerate(('original.bin', 'alias.bin'))])

    def test_uncertain_pins_survive_restart_and_time(self):
        self.register()
        self.accept()
        self.ledger.set_engine_state('r1', 'terminal', terminal_attested=True)
        self.ledger.set_pin('r1', 'o1', 'reader1', kind='read', state='uncertain')
        self.now = 10000
        ledger = self.open()
        self.assertEqual(ledger.cleanup_candidates(), [])
        ledger.set_pin('r1', 'o1', 'reader1', kind='read', state='released')
        self.assertEqual(len(ledger.cleanup_candidates()), 1)
        ledger.set_pin('r1', 'o1', 'worker1', kind='worker', state='active')
        with self.assertRaises(PolicyConflict):
            ledger.begin_cleanup('r1', 'o1', generation=1, sha256='d'*64)

    def test_bounded_plans_and_confirmation(self):
        self.register()
        self.accept(accepted_object_ids=['o1', 'o2'])
        self.now = 200
        self.assertEqual(self.ledger.cleanup_candidates(), [])  # Engine still running.
        self.ledger.set_engine_state('r1', 'terminal', terminal_attested=True)
        self.now = 210
        self.assertEqual(len(self.ledger.cleanup_candidates(max_count=1)), 1)
        self.assertEqual(self.ledger.cleanup_candidates(max_bytes=99), [])
        self.assertEqual(len(self.ledger.cleanup_candidates(max_bytes=150)), 1)
        with self.assertRaises(PolicyConflict):
            self.ledger.mark_cleaned('r1', 'o1', generation=1, sha256='d'*64,
                intent_token='unfenced', executor_attested=False)

    def test_cleanup_intent_fences_racing_pins_and_survives_uncertainty(self):
        self.register()
        self.accept()
        self.ledger.set_engine_state('r1', 'terminal', terminal_attested=True)
        self.now = 200
        intent = self.ledger.begin_cleanup('r1', 'o1', generation=1, sha256='d'*64, lease_seconds=10)
        self.assertEqual(self.open().cleanup_candidates(), [])
        with self.assertRaises(PolicyConflict):
            self.open().set_pin('r1', 'o1', 'lateReader', kind='read', state='active')
        self.now = 210
        self.assertEqual(self.open().cleanup_intent('r1', 'o1')['state'], 'uncertain')
        with self.assertRaises(PolicyConflict):
            self.open().set_pin('r1', 'o1', 'lateWorker', kind='worker', state='active')
        with self.assertRaises(PolicyConflict):
            self.ledger.mark_cleaned('r1', 'o1', generation=1, sha256='d'*64,
                intent_token='wrong', executor_attested=True)
        self.open().mark_cleaned('r1', 'o1', generation=1, sha256='d'*64,
            intent_token=intent['intent_token'], executor_attested=True)
        self.assertEqual(self.open().cleanup_intent('r1', 'o1')['state'], 'cleaned')

    def test_pin_winning_race_blocks_cleanup_intent(self):
        self.register()
        self.accept()
        self.ledger.set_engine_state('r1', 'terminal', terminal_attested=True)
        self.now = 200
        plan = self.ledger.cleanup_candidates()[0]
        self.open().set_pin('r1', 'o1', 'reader', kind='read', state='active')
        with self.assertRaises(PolicyConflict):
            self.ledger.begin_cleanup('r1', 'o1', generation=plan['generation'], sha256=plan['sha'])

    def test_concurrent_pin_and_cleanup_cannot_both_win(self):
        self.register()
        self.accept()
        self.ledger.set_engine_state('r1', 'terminal', terminal_attested=True)
        self.now = 200
        def attempt(action):
            try:
                if action == 'pin':
                    self.open().set_pin('r1', 'o1', 'racer', kind='read', state='active')
                else:
                    self.open().begin_cleanup('r1', 'o1', generation=1, sha256='d'*64)
                return True
            except PolicyConflict:
                return False
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            self.assertEqual(sum(pool.map(attempt, ['pin', 'cleanup'])), 1)

    def test_paths_and_changed_results(self):
        self.register()
        for invalid in ('/nas/source', 'jobs/r1/../x', 'jobs/r1//x', 'C:/history/x', 'jobs/r2/x'):
            with self.assertRaises(ValueError):
                self.ledger._owned_path('r1', invalid)
        with self.assertRaises(PolicyConflict):
            self.ledger.register_result('r1', job_id='j1', generation=1,
                manifest_sha256='a'*64, pipeline_sha256='b'*64,
                result_manifest_sha256='f'*64, objects=[dict(object_id='o1', bytes=100,
                    sha256='d'*64, relative_path='jobs/r1/o1.bin')])

    def test_cleanup_rejects_symlink_created_after_registration(self):
        self.register()
        self.accept()
        self.ledger.set_engine_state('r1', 'terminal', terminal_attested=True)
        self.now = 200
        jobs = self.root / 'jobs'
        try:
            jobs.symlink_to(self.root / 'elsewhere', target_is_directory=True)
        except OSError:
            self.skipTest('symlink creation is unavailable to this Windows account')
        with self.assertRaises(ValueError):
            self.ledger.cleanup_candidates()


if __name__ == '__main__':
    unittest.main()
