"""Actual pinned Node24/TLS workflow with a fresh canonical indexed worker.

The fixture backend processes tiny synthetic bytes; no installed worker, NAS,
client data, image publication or source-custody/release authority is used.
"""
from contextlib import closing
import copy
import hashlib
import json
import sqlite3
from unittest.mock import patch

from ops.bootstrap_worker_input_accounting import bootstrap_input_copy_accounting
from processing import reference_protocol as p
from processing.reference_indexed_fetch import compiled_profile_descriptor
from tests import test_reference_input_worker_joint as input_fixture
from tests import test_reference_worker_service as service_fixture


PROFILE = {'schemaVersion': 2, 'fetchJournal': {
    'schemaVersion': 1, 'policyId': 'indexed-worker-fetch-v1'}}


def assert_unindexed_driver(test, driver, *, input_accounting):
    """The ordinary/v1 schemas must not acquire the new profile or tables."""
    with closing(sqlite3.connect(driver.path)) as db:
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        binding = json.loads(db.execute('SELECT binding FROM driver_binding WHERE singleton=1').fetchone()[0])
    test.assertFalse(any(name.startswith('indexed_') for name in tables))
    test.assertNotIn('indexedFetchJournal', binding)
    test.assertIsNone(driver.indexed_fetch_context)
    if input_accounting:
        test.assertEqual(binding['inputCopyAccounting'], {'schemaVersion': 1})
    else:
        test.assertNotIn('inputCopyAccounting', binding)


class CanonicalIndexedWorkerJointTests(service_fixture.CanonicalWorkerJointTests):
    def setUp(self):
        original = service_fixture.private_config

        def indexed(*args, **kwargs):
            config = original(*args, **kwargs)
            config['inputCopyAccounting'] = copy.deepcopy(PROFILE)
            bootstrap_input_copy_accounting(config)
            return config

        # Only choose a fresh private composition in the fixture; leave actual
        # bootstrap/factory/service/journal and both HTTPS transports unmocked.
        with patch.object(service_fixture, 'private_config', side_effect=indexed):
            super().setUp()
        self.assertTrue(self.driver.input_copy_accounting)
        self.assertIsNotNone(self.driver.indexed_fetch_context)
        self.assertIsNone(self.ledger.release_authority)
        assert_unindexed_driver(self, self.unused_driver, input_accounting=False)

    def wait_stage(self, target, *, pump=True):
        super().wait_stage(target, pump=pump)
        # Parent assertions expect a full observational envelope at a *phase
        # boundary*. Hydrate that test observation from the actual sealed
        # journal; do not replace _save, service steps or hot fetch behavior.
        with self.driver._db() as db:
            row = db.execute('SELECT * FROM driver_jobs WHERE job=?', (self.job,)).fetchone()
        return self.driver._load_driver_data(row)

    def assert_normalized_original_inputs_retained(self):
        with closing(sqlite3.connect(self.driver.path)) as db:
            row = db.execute('SELECT stage,data,claim_request FROM driver_jobs WHERE job=?', (self.job,)).fetchone()
            envelope, envelope_sha, count, byte_size, closure = db.execute(
                'SELECT envelope,sha,count,bytes,closure FROM indexed_claims WHERE job=?', (self.job,)).fetchone()
            header = json.loads(db.execute('SELECT header FROM indexed_claims WHERE job=?', (self.job,)).fetchone()[0])
            objects = db.execute('SELECT seq,id,descriptor FROM indexed_objects WHERE job=? ORDER BY seq', (self.job,)).fetchall()
            grant, grant_sha = db.execute('SELECT record,sha FROM indexed_grants WHERE job=?', (self.job,)).fetchone()
            next_seq, confirmed_bytes, attempts, revision = db.execute(
                'SELECT next,bytes,attempts,revision FROM indexed_fetch WHERE job=?', (self.job,)).fetchone()
            attempt_rows = db.execute('SELECT seq,attempt,state FROM indexed_attempts WHERE job=?', (self.job,)).fetchall()
            profile = json.loads(db.execute('SELECT record FROM indexed_profile WHERE singleton=1').fetchone()[0])
            quota = db.execute('SELECT charge FROM indexed_quota WHERE job=?', (self.job,)).fetchone()[0]
            binding = json.loads(db.execute('SELECT binding FROM driver_binding WHERE singleton=1').fetchone()[0])
        original = json.loads(envelope)
        projection = json.loads(row[1])
        self.assertEqual(row[0], 'completed')
        self.assertLessEqual(len(row[1].encode('utf-8')), 8192)
        self.assertFalse({'grant', 'inputPaths', 'fetchAttempts'} & set(projection))
        self.assertNotIn('submission', projection['claim'])
        self.assertEqual(envelope_sha, hashlib.sha256(envelope.encode()).hexdigest())
        self.assertEqual(header['claimRequestId'], row[2])
        self.assertEqual(original['jobId'], self.job)
        self.assertEqual(original['environment'], 'staging')
        self.assertEqual((count, byte_size, next_seq, confirmed_bytes, attempts), (1, 3, 1, 3, 1))
        self.assertGreaterEqual(revision, 2)
        self.assertEqual(attempt_rows, [(0, 1, 'confirmed')])
        self.assertEqual(objects[0][0], 0)
        self.assertEqual(json.loads(objects[0][2]), original['submission']['manifest']['objects'][0])
        self.assertEqual(closure, p.digest(original['submission']['manifest']['objects']))
        self.assertEqual(grant_sha, hashlib.sha256(grant.encode()).hexdigest())
        self.assertEqual(json.loads(grant)['jobId'], self.job)
        descriptor = compiled_profile_descriptor()
        self.assertEqual(profile['descriptor'], descriptor)
        self.assertEqual(binding['indexedFetchJournal'], descriptor)
        self.assertEqual(binding['inputCopyAccounting'], {'schemaVersion': 1})
        self.assertEqual(quota, descriptor['limits']['jobControlBytes'])
        with closing(sqlite3.connect(self.ledger.path)) as db:
            rows = db.execute('SELECT path,bytes,sha,fetched,cleaned FROM input_copies').fetchall()
        self.assertEqual(len(rows), 1)
        for relative, size, digest, fetched, cleaned in rows:
            self.assertIsNotNone(fetched)
            self.assertIsNone(cleaned)
            actual = (self.ledger.root / relative).read_bytes()
            self.assertEqual((len(actual), hashlib.sha256(actual).hexdigest()), (size, digest))
            self.assertEqual(actual, b'abc')
        self.assertIsNone(self.ledger.release_authority)
        self.assertEqual(self.ledger.input_cleanup_candidates(), [])
        assert_unindexed_driver(self, self.unused_driver, input_accounting=False)

    def test_actual_canonical_factory_service_runs_and_acceptance_precedes_cleanup(self):
        super().test_actual_canonical_factory_service_runs_and_acceptance_precedes_cleanup()
        self.assert_normalized_original_inputs_retained()

    def test_expired_original_grant_after_commit_and_coordinator_outage_keeps_original_work(self):
        super().test_expired_original_grant_after_commit_and_coordinator_outage_keeps_original_work()
        self.assert_normalized_original_inputs_retained()


class CanonicalV1CompatibilityJointTests(input_fixture.CanonicalInputAccountingJointTests):
    def test_actual_canonical_factory_service_runs_and_acceptance_precedes_cleanup(self):
        super().test_actual_canonical_factory_service_runs_and_acceptance_precedes_cleanup()
        assert_unindexed_driver(self, self.driver, input_accounting=True)
        assert_unindexed_driver(self, self.unused_driver, input_accounting=False)

    def test_expired_original_grant_after_commit_and_coordinator_outage_keeps_original_work(self):
        super().test_expired_original_grant_after_commit_and_coordinator_outage_keeps_original_work()
        assert_unindexed_driver(self, self.driver, input_accounting=True)
        assert_unindexed_driver(self, self.unused_driver, input_accounting=False)
