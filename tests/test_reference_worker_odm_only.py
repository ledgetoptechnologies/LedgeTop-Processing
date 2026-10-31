import unittest
from unittest.mock import patch

from ops import reference_worker_daemon as daemon


class OdmOnlyTests(unittest.TestCase):
    def test_image_mode_refuses_treedar_before_opening_worker(self):
        config = {'executionRouting': {'treedar': {'imageId': 'synthetic'}}}
        with patch.object(daemon, 'load_config', return_value=config), \
                patch.object(daemon, '_execution_routing', return_value=config['executionRouting']), \
                patch.object(daemon, 'build_worker') as build:
            self.assertEqual(daemon.main(['--odm-only', '--config', '/unused']), 2)
        build.assert_not_called()

    def test_normal_daemon_mode_is_unchanged(self):
        config = {'executionRouting': {'treedar': {'imageId': 'synthetic'}}}
        with patch.object(daemon, 'load_config', return_value=config), \
                patch.object(daemon, '_execution_routing') as routing, \
                patch.object(daemon, 'build_worker', side_effect=ValueError()) as build:
            self.assertEqual(daemon.main(['--config', '/unused']), 2)
        routing.assert_not_called()
        build.assert_called_once_with(config)

    def test_image_mode_allows_odm_routing(self):
        for routing in (None, {'treedar': None}):
            with self.subTest(routing=routing), \
                    patch.object(daemon, 'load_config', return_value={}), \
                    patch.object(daemon, '_execution_routing', return_value=routing), \
                    patch.object(daemon, 'build_worker', side_effect=ValueError()) as build:
                self.assertEqual(daemon.main(['--odm-only', '--config', '/unused']), 2)
            build.assert_called_once()
