from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

from ops import qualify_retained_small_engine_swap as fixture
from ops import retained_small_engine_swap as production


class NativeFixtureSourceTests(unittest.TestCase):
    nonce = '0123456789abcdef0123456789abcdef'

    def test_exact_owner_and_namespace_required_before_cleanup(self):
        prefix = fixture.namespace(self.nonce)
        fixture.owned_object({'Name': '/' + prefix + '-engine',
                              'Config': {'Labels': {fixture.OWNER: self.nonce}}}, self.nonce)
        fixture.owned_object({'Name': prefix + '-data', 'Labels': {fixture.OWNER: self.nonce}}, self.nonce)
        for item in ({'Name': prefix + '-data', 'Labels': {fixture.OWNER: 'different'}},
                     {'Name': 'production-data', 'Labels': {fixture.OWNER: self.nonce}},
                     {'Name': prefix + '-engine', 'Config': {'Labels': {}}}):
            with self.subTest(item=item), self.assertRaises(RuntimeError):
                fixture.owned_object(item, self.nonce)
        for nonce in ('../elsewhere', '', 'A' * 32, self.nonce + '/suffix'):
            with self.subTest(nonce=nonce), self.assertRaises(RuntimeError):
                fixture.namespace(nonce)

    def test_only_exact_known_node_banner_is_removed(self):
        reply = {'status': 200, 'body': '{"synthetic":true}'}
        raw = json.dumps(reply)
        self.assertEqual(fixture.node_reply(raw), reply)
        self.assertEqual(fixture.node_reply(fixture.NODE_BANNER + '\r\n' + raw + '\r\n'), reply)
        for bad in ('unknown startup banner\n' + raw, raw + '\nextra',
                    fixture.NODE_BANNER + '\n' + fixture.NODE_BANNER + '\n' + raw,
                    '{"status":true,"body":"x"}', '[]', raw + '\n' + raw):
            with self.subTest(raw=bad), self.assertRaises((ValueError, RuntimeError)):
                fixture.node_reply(bad)

    def test_fixture_compose_has_bounded_resources_no_published_ports_build_or_pull(self):
        prefix = fixture.namespace(self.nonce)
        config = fixture.engine_compose(prefix, prefix + '-network', prefix + '-data',
                fixture.BASE, Path('/private/fixture-config.json'), 'synthetic-token', self.nonce)
        service = config['services']['nodeodm']
        self.assertEqual(service['labels'][fixture.OWNER], self.nonce)
        self.assertNotIn('ports', service)
        self.assertNotIn('build', service)
        self.assertEqual(service['pull_policy'], 'never')
        self.assertEqual(service['runtime'], 'runc')
        self.assertEqual(service['cpus'], .5)
        self.assertEqual(service['mem_limit'], service['memswap_limit'])
        self.assertEqual(service['environment']['NVIDIA_VISIBLE_DEVICES'], 'void')

    def test_private_helper_module_does_not_rebind_production_import(self):
        before = production.ENGINE
        helper = fixture.load_helper()
        self.assertEqual(fixture.sha(Path(helper.__file__).read_bytes()), fixture.HELPER_SHA)
        helper.ENGINE = 'fixture-only'
        self.assertEqual(production.ENGINE, before)

    def test_default_is_source_plan_and_never_starts_fixture(self):
        with patch('sys.argv', ['fixture']), patch.object(fixture, 'run_fixture') as run, \
                redirect_stdout(io.StringIO()) as output:
            self.assertEqual(fixture.main(), 0)
        run.assert_not_called()
        report = json.loads(output.getvalue())
        self.assertEqual(report['mode'], 'source-plan')
        self.assertFalse(report['writes'])
        self.assertEqual(report['publishedPorts'], [])

    def test_partial_setup_cleanup_is_bounded_and_not_automatically_replayed(self):
        runtime = Mock()
        def fail_early(recovery):
            recovery.update(runtime=runtime, nonce=self.nonce, network='owned-network', volume='owned-volume')
            raise RuntimeError('synthetic setup failure')
        with patch.object(fixture, '_run_fixture', side_effect=fail_early), \
                patch.object(fixture, 'cleanup_resources') as cleanup, self.assertRaises(RuntimeError):
            fixture.run_fixture()
        cleanup.assert_called_once_with(runtime, self.nonce, 'owned-network', 'owned-volume')
        def fail_during_cleanup(recovery):
            recovery.update(runtime=runtime, nonce=self.nonce, network='owned-network',
                            volume='owned-volume', cleanupStarted=True)
            raise RuntimeError('synthetic uncertain cleanup')
        with patch.object(fixture, '_run_fixture', side_effect=fail_during_cleanup), \
                patch.object(fixture, 'cleanup_resources') as cleanup, self.assertRaises(RuntimeError):
            fixture.run_fixture()
        cleanup.assert_not_called()

    def test_fixture_client_refuses_other_origins_and_every_http_mutation(self):
        runtime = Mock()
        with self.assertRaises(RuntimeError):
            fixture.FixtureClient(runtime, 'http://192.168.50.89:3001')
        client = fixture.FixtureClient(runtime, 'http://127.0.0.1:13001', 'synthetic', node=True)
        with self.assertRaises(RuntimeError):
            client.request('POST', '/task/new/init')
        runtime.internal_http.assert_not_called()


if __name__ == '__main__':
    unittest.main()
