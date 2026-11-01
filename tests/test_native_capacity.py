import unittest
from types import SimpleNamespace
from unittest.mock import patch

from processing import native_capacity
from processing.viewer_limits import MAX_UPLOAD_BYTES


class NativeCapacityTests(unittest.TestCase):
    def test_disabled_before_inspecting_store(self):
        with self.assertRaises(RuntimeError):
            native_capacity.capability(None)

    def test_closed_object_reports_only_configured_independent_limits(self):
        with patch.object(native_capacity, 'NATIVE_CAPACITY_QUALIFIED', True):
            actual = native_capacity.capability(SimpleNamespace(max_input_bytes=17))
        self.assertEqual(actual, {
            'schemaVersion': 1, 'unit': 'bytes',
            'scope': {'environment': None, 'node': None, 'configRevision': None,
                      'fingerprint': None, 'freshness': 'unknown'},
            'input': {'legacyMaxTaskBytes': 17, 'manifestMaxTaskBytes': 17,
                      'maxObjects': None, 'maxAuxiliaryObjects': None},
            'request': {'maxMultipartBodyBytes': MAX_UPLOAD_BYTES, 'maxPartBytes': None},
            'temporary': {'strategy': 'per_part_spool', 'filesystemRole': 'coordinator_spool',
                          'maxConcurrentBytes': None},
            'expansion': {'coordinatorTaskBytes': None, 'engineWorkspaceBytes': None,
                          'resultBytes': None},
            'reservation': {'state': 'unavailable', 'physical': False},
            'resources': {'coordinatorSpool': None, 'coordinatorTaskInput': None,
                          'coordinatorArchive': None, 'engineWorkspace': None,
                          'engineOutput': None},
            'admission': 'unknown',
        })

    def test_invalid_limits_fail_closed(self):
        with patch.object(native_capacity, 'NATIVE_CAPACITY_QUALIFIED', True):
            for value in (True, None, 0, -1, 10**15 + 1, 1.0):
                with self.subTest(value=value), self.assertRaises(ValueError):
                    native_capacity.capability(SimpleNamespace(max_input_bytes=value))

    def test_manifest_ceiling_remains_lower_than_larger_legacy_configuration(self):
        ceiling = native_capacity.MANIFEST_MAX_INPUT_BYTES
        with patch.object(native_capacity, 'NATIVE_CAPACITY_QUALIFIED', True):
            limits = native_capacity.capability(SimpleNamespace(max_input_bytes=ceiling + 1))['input']
        self.assertEqual(limits['legacyMaxTaskBytes'], ceiling + 1)
        self.assertEqual(limits['manifestMaxTaskBytes'], ceiling)
