from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import tempfile
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

    def test_readonly_nginx_config_places_every_temp_directory_in_tmp(self):
        for directive, directory in ((b'client_body_temp_path', b'body'), (b'proxy_temp_path', b'proxy'),
                (b'fastcgi_temp_path', b'fastcgi'), (b'uwsgi_temp_path', b'uwsgi'), (b'scgi_temp_path', b'scgi')):
            self.assertIn(directive + b' /tmp/' + directory + b';', fixture.NGINX_CONFIG)
        self.assertNotIn(b'/var/cache/nginx', fixture.NGINX_CONFIG)

    def test_failure_diagnostics_save_only_owned_state_and_private_bounded_logs(self):
        identifier = 'a' * 64
        runtime = Mock()
        runtime.docker.return_value = identifier
        runtime.inspect.return_value = {
            'Id': identifier, 'Name': '/' + fixture.namespace(self.nonce) + '-ingress', 'Image': fixture.NGINX_IMAGES[1],
            'Config': {'Labels': {fixture.OWNER: self.nonce}, 'Env': ['TOKEN=private-not-for-report']},
            'State': {'Status': 'exited', 'Running': False, 'ExitCode': 1, 'OOMKilled': False,
                      'Restarting': False, 'Error': 'private-not-for-report'}}
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / fixture.namespace(self.nonce)
            root.mkdir()
            (root / '.fixture-marker').write_text(self.nonce + '\n')
            with patch.object(fixture, 'bounded_logs', return_value={'bytes': 20}) as logs:
                self.assertTrue(fixture.record_failure_diagnostics(runtime, self.nonce, root,
                    RuntimeError('fixture proxy startup failed'), {'phase': 'proxy_startup'}))
            logs.assert_called_once_with(identifier, root / ('diagnostic-' + identifier[:12] + '.log'))
            raw = (root / 'failure-diagnostics.json').read_text()
            report = json.loads(raw)
            self.assertEqual(report['containers'][0]['state']['ExitCode'], 1)
            self.assertEqual(report['exception']['reason'], 'fixture proxy startup failed')
            self.assertNotIn('private-not-for-report', raw)
            self.assertNotIn('Env', raw)

    def test_log_reader_retains_only_bounded_prefix_without_printing_content(self):
        process = Mock()
        process.stdout = io.BytesIO(b'private log bytes' * 100)
        process.poll.return_value = 0
        process.wait.return_value = 0
        process.returncode = 0
        with tempfile.TemporaryDirectory() as temporary, \
                patch.object(fixture.subprocess, 'Popen', return_value=process) as popen, \
                redirect_stdout(io.StringIO()) as output:
            path = Path(temporary) / 'private.log'
            result = fixture.bounded_logs('b' * 64, path, max_bytes=64)
            self.assertEqual(path.stat().st_size, 64)
            self.assertTrue(result['truncated'])
            self.assertFalse(result['timedOut'])
            self.assertEqual(output.getvalue(), '')
            self.assertIn('--tail', popen.call_args.args[0])
            self.assertIn('80', popen.call_args.args[0])
        self.assertTrue(process.stdout.closed)

    def test_unknown_exception_text_is_not_written_to_proxy_failure_context(self):
        report = fixture.failure_reason(RuntimeError('token=private-not-for-output'))
        self.assertEqual(report, {'class': 'RuntimeError', 'reason': 'redacted_exception'})

    def test_binding_difference_summary_contains_paths_and_no_changed_values(self):
        before = {'HostConfig': {'NetworkMode': 'old-private-network', 'Memory': 123},
                  'Mounts': [{'Source': '/private/old', 'RW': True}], 'networkKeys': ['old-network']}
        after = {'HostConfig': {'NetworkMode': 'new-private-network', 'Memory': 123},
                 'Mounts': [{'Source': '/private/new', 'RW': True}], 'networkKeys': ['new-network']}
        paths = fixture.differing_field_paths(before, after)
        self.assertEqual(paths, ['HostConfig.NetworkMode', 'Mounts[0].Source', 'networkKeys[0]'])
        self.assertNotIn('private', json.dumps(paths))

    def test_owned_candidate_private_bindings_are_saved_before_cleanup_comparison(self):
        old_id, new_id = 'a' * 64, 'b' * 64
        common = {'Name': '/' + fixture.namespace(self.nonce) + '-engine',
                  'Config': {'Labels': {fixture.OWNER: self.nonce, 'com.docker.compose.service': 'nodeodm'},
                             'Env': ['TOKEN=private-not-for-safe-output']},
                  'Mounts': [{'Source': '/private/fixture-volume', 'RW': True}],
                  'NetworkSettings': {'Networks': {'owned-fixture-network': {}}}}
        old = dict(common, Id=old_id, Image=fixture.BASE, HostConfig={'Memory': 123})
        current = dict(common, Id=new_id, Image=fixture.TARGET, HostConfig={'Memory': 456},
                       State={'Status': 'created', 'Running': False, 'ExitCode': 0, 'OOMKilled': False})
        runtime = Mock()
        runtime.docker.return_value = new_id
        runtime.inspect.return_value = current
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / fixture.namespace(self.nonce)
            root.mkdir(); (root / '.fixture-marker').write_text(self.nonce + '\n')
            (root / 'backup').mkdir()
            (root / 'backup' / 'prepared.private.json').write_text(json.dumps({'oldContainer': old}))
            with patch.object(fixture, 'bounded_logs', return_value={'bytes': 20}):
                self.assertTrue(fixture.record_failure_diagnostics(runtime, self.nonce, root,
                    RuntimeError('candidate host configuration or volume changed'), {'phase': 'recreate_and_verify'}))
            binding = json.loads((root / ('diagnostic-' + new_id + '.binding.private.json')).read_text())
            self.assertEqual(binding['HostConfig']['Memory'], 456)
            self.assertNotIn('Config', binding)
            safe = (root / 'binding-differences.safe.json').read_text()
            report = json.loads(safe)
            self.assertEqual(report['differingFieldPaths'], ['HostConfig.Memory'])
            self.assertEqual(report['candidateBindingsCompared'], 1)
            self.assertTrue(report['comparisonAvailable'])
            self.assertNotIn('456', safe)
            self.assertNotIn('private', safe)
            self.assertNotIn('TOKEN', safe)


if __name__ == '__main__':
    unittest.main()
