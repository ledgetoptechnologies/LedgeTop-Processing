"""Opt-in worker composition with the pinned real Node broker/protocol fixtures.

This uses the normal bootstrap and factory, not a release-authority test double.
No source-custody authority is supplied and fetched inputs must remain retained.
"""
from contextlib import closing
import hashlib
import sqlite3
from unittest.mock import patch

from ops.bootstrap_worker_input_accounting import bootstrap_input_copy_accounting
from tests import test_reference_worker_service as service_fixture


class CanonicalInputAccountingJointTests(service_fixture.CanonicalWorkerJointTests):
    def setUp(self):
        original = service_fixture.private_config

        def opted_in(*args, **kwargs):
            config = original(*args, **kwargs)
            config['inputCopyAccounting'] = {'schemaVersion': 1}
            bootstrap_input_copy_accounting(config)
            return config

        # Change only the fixture's selected configuration. The actual bootstrap,
        # factory, ledger, driver, TLS transports and Node broker remain in use.
        with patch.object(service_fixture, 'private_config', side_effect=opted_in):
            super().setUp()
        self.assertTrue(self.driver.input_copy_accounting)
        self.assertIsNone(self.ledger.release_authority)

    def assert_fetched_inputs_retained_without_authority(self):
        with closing(sqlite3.connect(self.ledger.path)) as db:
            rows = db.execute('SELECT path,bytes,sha,fetched,cleaned FROM input_copies').fetchall()
        self.assertTrue(rows)
        for relative, size, sha, fetched, cleaned in rows:
            self.assertIsNotNone(fetched)
            self.assertIsNone(cleaned)
            actual = (self.ledger.root / relative).read_bytes()
            self.assertEqual(len(actual), size)
            self.assertEqual(hashlib.sha256(actual).hexdigest(), sha)
        self.assertEqual(self.ledger.input_cleanup_candidates(), [])

    def test_actual_canonical_factory_service_runs_and_acceptance_precedes_cleanup(self):
        super().test_actual_canonical_factory_service_runs_and_acceptance_precedes_cleanup()
        self.assert_fetched_inputs_retained_without_authority()

    def test_expired_original_grant_after_commit_and_coordinator_outage_keeps_original_work(self):
        super().test_expired_original_grant_after_commit_and_coordinator_outage_keeps_original_work()
        self.assert_fetched_inputs_retained_without_authority()
