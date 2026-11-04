"""Synthetic normal-completion custody regression; no real engine or data.

The fake engine is configured to reject a download after its task is removed,
as a NodeODM terminal cleanup would. These tests do not simulate a real timer.
"""
import hashlib
import unittest
from unittest.mock import patch

from processing.viewer_service import EngineUnavailable
from tests import test_viewer_service as viewer_fixture


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

if __name__ == '__main__':
    unittest.main()
