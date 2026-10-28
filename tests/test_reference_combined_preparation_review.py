"""Independent prepare-only dependency drift checks using actual TLS fixtures."""
import os
from dataclasses import replace
from unittest.mock import patch
import unittest

from tests import test_reference_combined_preparation as fixtures


@unittest.skipUnless(os.name == 'posix', 'actual Linux preparation')
class CombinedPreparationReviewTests(unittest.TestCase):
    setUp = fixtures.CombinedPreparationTests.setUp
    api = fixtures.CombinedPreparationTests.api
    make_driver = fixtures.CombinedPreparationTests.make_driver
    drive = fixtures.CombinedPreparationTests.drive
    intent = fixtures.CombinedPreparationTests.intent

    @classmethod
    def setUpClass(cls):
        fixtures.CombinedPreparationTests.setUpClass.__func__(cls)

    def test_prepared_missing_published_control_is_held(self):
        self.drive()
        path = self.driver.combined_preparation.recovery.path
        path.unlink()
        self.assertIn(self.driver.step()['state'], ('held', 'retry_wait'))
        self.assertFalse(path.exists(), 'missing published control was silently recreated')
        self.assertEqual(self.native_fixture.counts()['initialize'], 0)

    def test_background_dependency_drift_refuses_before_cleanup_call(self):
        self.drive()
        calls = []
        class ForeignCleanup:
            def reconcile_one(self):
                calls.append('unexpected-cleanup')
        self.driver.engine_cleanup = ForeignCleanup()
        result = self.driver.step()
        self.assertEqual(calls, [], 'prepare-only step invoked foreign cleanup before checking profile')
        self.assertEqual(result['state'], 'held')

    def test_native_transport_replacement_is_held_before_use(self):
        self.drive('execution_pending')
        calls = []
        original = self.native.read
        def foreign_read(*args, **kwargs):
            calls.append(args[0])
            return original(*args, **kwargs)
        with patch.object(self.native, 'read', side_effect=foreign_read):
            result = self.driver.step()
        self.assertEqual(calls, [], 'prepare-only step used a replaced native transport')
        self.assertEqual(result['state'], 'held')

    def test_live_driver_page_policy_replacement_is_held(self):
        self.drive()
        recovery = replace(self.policy.recovery,
            preparation_allowance=3*(256+self.policy.pipeline_page_cap)*4096+65536)
        pipeline = replace(self.policy.pipeline, recovery_control_budget=recovery.control_allowance)
        self.driver._combined_policy = replace(self.policy, pipeline=pipeline,
            recovery=recovery, driver_page_cap=256)
        self.assertEqual(self.driver.step()['state'], 'held')

    def test_coordinator_transport_origin_drift_is_held(self):
        self.drive()
        self.driver.origin += '/unapproved'
        self.assertEqual(self.driver.step()['state'], 'held')

    def test_broker_transport_replacement_is_held(self):
        self.drive()
        original = self.broker._open
        with patch.object(self.broker, '_open', side_effect=original):
            self.assertEqual(self.driver.step()['state'], 'held')
