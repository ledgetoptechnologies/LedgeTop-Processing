"""Viewer-facing option discovery keeps compatible ODM flags across engines."""

from types import SimpleNamespace
import unittest
from unittest.mock import patch

from processing.viewer_service import ViewerService


def option(name, *, help_text, kind='bool', value=False, domain=None):
    result = {'name': name, 'type': kind, 'value': value, 'help': help_text}
    if domain is not None:
        result['domain'] = domain
    return result


class ViewerOptionIntersectionTests(unittest.TestCase):
    def setUp(self):
        self.service = object.__new__(ViewerService)
        self.service.nodes = (
            SimpleNamespace(node_id='cluster', origin='https://cluster.invalid', token='cluster-secret'),
            SimpleNamespace(node_id='main', origin='https://main.invalid', token='main-secret'),
        )
        self.service.probe = lambda role, origin, token: {'ready': True}

    def discover(self, cluster_options, main_options):
        def snapshot(role, origin, token, *, allow_dynamic=False):
            self.assertTrue(allow_dynamic)
            options = cluster_options if role == 'cluster' else main_options
            return {'role': role, 'available': True, 'options': options}

        with patch('processing.viewer_service.discover_node', side_effect=snapshot):
            return self.service._catalog()

    def test_help_text_difference_does_not_hide_compatible_live_engine_option(self):
        required = [option(name, help_text='required output')
                    for name in ('pc-ept', 'gltf', '3d-tiles')]
        cluster = [*required, option('fast-orthophoto', help_text='ODM wording A')]
        main = [*required, option('fast-orthophoto', help_text='ODX wording B')]

        result = self.discover(cluster, main)

        self.assertTrue(result['ready'])
        self.assertIn('fast-orthophoto', {item['name'] for item in result['commonOptions']})

    def test_execution_metadata_difference_still_hides_option(self):
        required = [option(name, help_text='required output')
                    for name in ('pc-ept', 'gltf', '3d-tiles')]
        cluster = [*required, option('pc-quality', help_text='cluster', kind='enum',
                                     value='medium', domain=['low', 'medium', 'high'])]
        main = [*required, option('pc-quality', help_text='main', kind='enum',
                                  value='medium', domain=['low', 'high'])]

        result = self.discover(cluster, main)

        self.assertNotIn('pc-quality', {item['name'] for item in result['commonOptions']})


if __name__ == '__main__':
    unittest.main()
