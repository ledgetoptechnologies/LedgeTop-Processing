import json
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from ops import processingctl


TOKEN = 'synthetic-coordinator-token-' + 'x' * 32
ENGINE = '45bc889d-d645-442b-8fb9-d78b77b14fe9'
IMAGE = 'sha256:' + 'a' * 64


class ProcessingCtlTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / 'install'
        self.root.mkdir()
        (self.root / 'source' / 'scripts').mkdir(parents=True)
        (self.root / 'source' / 'scripts' / 'prototype_dispatcher.py').write_text('# fixture\n')
        (self.root / 'work').mkdir()
        (self.root / 'dispatch-state').mkdir()
        (self.root / 'runtime.env').write_text('PROCESSING_API_TOKEN=' + TOKEN + '\n')
        if processingctl.os.name != 'nt':
            self.root.chmod(0o700)
            for path in (self.root / 'runtime.env',):
                path.chmod(0o600)
            for path in (self.root / 'work', self.root / 'dispatch-state'):
                path.chmod(0o700)
        self.metadata = {
            'origin': 'http://127.0.0.1:18080',
            'engineId': ENGINE,
            'generation': 1,
        }
        self.write_metadata()

    def tearDown(self):
        self.temporary.cleanup()

    def write_metadata(self):
        path = self.root / 'installation.json'
        path.write_text(json.dumps(self.metadata), encoding='utf-8')
        if processingctl.os.name != 'nt':
            path.chmod(0o600)

    def test_legacy_installation_is_cluster_bound_and_bounded(self):
        config = processingctl.load_installation(self.root)
        self.assertEqual(config.node_id, 'cluster')
        self.assertEqual(config.resource_policy, 'bounded-fixture')
        self.assertEqual(config.image, processingctl.DEFAULT_WORKER_IMAGE)
        command = processingctl.dispatch_command(config, 2)
        self.assertEqual(command[command.index('--node-id') + 1], 'cluster')
        self.assertEqual(command[command.index('--resource-policy') + 1], 'bounded-fixture')

    def test_main_installation_selects_main_gate_and_unrestricted_policy(self):
        self.metadata.update(nodeId='main', workerImage=IMAGE,
                             workerId='main-worker-fixture')
        self.write_metadata()
        config = processingctl.load_installation(self.root)
        command = processingctl.dispatch_command(config, 1)
        self.assertEqual(config.node_id, 'main')
        self.assertEqual(config.resource_policy, 'main-unrestricted')
        self.assertEqual(config.worker_id, 'main-worker-fixture')
        self.assertEqual(config.image, IMAGE)
        self.assertEqual(command[command.index('--docker-engine-id') + 1], ENGINE)
        self.assertEqual(command[command.index('--node-id') + 1], 'main')
        self.assertEqual(command[command.index('--resource-policy') + 1], 'main-unrestricted')

    def test_dispatch_passes_only_api_token_and_returns_child_status(self):
        self.metadata['nodeId'] = 'main'
        self.write_metadata()
        args = processingctl.parser().parse_args(['--root', str(self.root), 'dispatch', '--max-jobs', '3'])
        observed = {}

        def runner(command, *, env, check):
            observed.update(command=command, env=env, check=check)
            return SimpleNamespace(returncode=7)

        with patch.dict(processingctl.os.environ,
                        {'PROCESSING_PROTOTYPE_OPERATOR_TOKEN': 'operator-secret',
                         'PROCESSING_PROTOTYPE_OBSERVER_TOKEN': 'observer-secret',
                         'MAIN_NODE_TOKEN': 'node-secret',
                         'PROCESSING_VIEWER_TOKEN': 'viewer-secret',
                         'PYTHONPATH': 'untrusted-python-path',
                         'UNRELATED_TOKEN': 'unrelated-secret',
                         'DOCKER_CONFIG': '/private/docker-config'}, clear=False):
            result = processingctl.run(args, process_runner=runner)
        self.assertEqual(result, 7)
        self.assertEqual(observed['check'], False)
        self.assertEqual(observed['command'][observed['command'].index('--max-jobs') + 1], '3')
        self.assertEqual(observed['env']['PROCESSING_API_TOKEN'], TOKEN)
        for name in ('PROCESSING_PROTOTYPE_OPERATOR_TOKEN',
                     'PROCESSING_PROTOTYPE_OBSERVER_TOKEN', 'MAIN_NODE_TOKEN',
                     'PROCESSING_VIEWER_TOKEN', 'PYTHONPATH', 'UNRELATED_TOKEN'):
            self.assertNotIn(name, observed['env'])
        self.assertEqual(observed['env']['DOCKER_CONFIG'], '/private/docker-config')
        self.assertNotIn(TOKEN, ' '.join(observed['command']))

    def test_non_loopback_origin_is_refused(self):
        self.metadata['origin'] = 'http://192.168.50.89:18080'
        self.write_metadata()
        with self.assertRaisesRegex(processingctl.ControlError, 'loopback'):
            processingctl.load_installation(self.root)

    def test_unknown_node_and_bad_worker_image_are_refused(self):
        self.metadata['nodeId'] = 'other'
        self.write_metadata()
        with self.assertRaises(processingctl.ControlError):
            processingctl.load_installation(self.root)
        self.metadata['nodeId'] = ['main']
        self.write_metadata()
        with self.assertRaises(processingctl.ControlError):
            processingctl.load_installation(self.root)
        self.metadata.update(nodeId='main', workerImage='ghcr.io/example:latest')
        self.write_metadata()
        with self.assertRaises(processingctl.ControlError):
            processingctl.load_installation(self.root)

    def test_paths_must_stay_under_private_installation(self):
        outside = Path(self.temporary.name) / 'outside'
        outside.mkdir()
        self.metadata['dispatchStatePath'] = str(outside)
        self.write_metadata()
        with self.assertRaisesRegex(processingctl.ControlError, 'child'):
            processingctl.load_installation(self.root)

    def test_help_does_not_read_installation_or_credentials(self):
        parser = processingctl.parser()
        with self.assertRaises(SystemExit) as caught:
            parser.parse_args(['--root', str(self.root / 'missing'), '--help'])
        self.assertEqual(caught.exception.code, 0)


if __name__ == '__main__':
    unittest.main()
