from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace
import os
import subprocess

from processing.app import make_handler
from processing.config import Config
from processing.container_update import execute, ComposeNode, UpdateError, Uncertain, PreflightError
from processing.odx_smoke_worker import Client, SmokeError
from tests.test_service import TOKEN, start_server

OLD = 'sha256:' + '1' * 64
NEW = 'sha256:' + '2' * 64
OPERATOR = 'maintenance-operator-private-test-credential'


class Adapter:
    def __init__(self):
        self.current, self.calls, self.busy = OLD, [], False
        self.bad_health, self.uncertain, self.missing = False, False, False

    def inspect(self):
        return {'Image': self.current}

    def idle(self):
        return not self.busy

    def image(self, image):
        if image == NEW and self.missing:
            raise UpdateError('image absent')

    def replace(self, image):
        self.calls.append(image)
        self.current = image
        if self.uncertain:
            raise Uncertain('lost mutation response')

    def prepare(self, image):
        self.image(image)

    def healthy(self, image):
        return self.current == image and not (self.bad_health and image == NEW)


class MaintenanceUpdateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.work = self.root / 'receipt'
        self.work.mkdir()
        handler = make_handler(Config(TOKEN, '', '', 800, 20000,
            prototype_state_dir=str(self.root / 'queue'), prototype_operator_token=OPERATOR,
            prototype_node_ids=('cluster', 'main')))
        self.queue = handler.prototype_queue
        self.server = start_server(handler)
        self.base = 'http://127.0.0.1:' + str(self.server.server_port)
        self.api, self.operator = Client(self.base, TOKEN), Client(self.base, OPERATOR)
        self.adapter = Adapter()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.queue.close()
        self.temp.cleanup()

    def plan(self, future=False):
        stamp = datetime.now(timezone.utc) + timedelta(seconds=60 if future else -1)
        return self.operator.json('POST', '/v1/prototype/nodes/cluster/schedule', {
            'expectedRevision': self.queue.get_node('cluster')['revision'],
            'drainAt': stamp.isoformat(), 'notBefore': stamp.isoformat(),
            'afterJobId': None, 'targetImage': NEW})['node']

    def run_update(self):
        return execute(self.api, self.operator, self.adapter, 'cluster', self.work,
                       wait_seconds=0, health_seconds=0)

    def test_real_http_success_reopens_capacity_only_after_health(self):
        self.plan()
        self.assertIsNone(self.queue.claim({'workerId': 'before'}))
        result = self.run_update()
        self.assertEqual(result['status'], 'succeeded')
        self.assertEqual(self.adapter.calls, [NEW])
        self.assertEqual(self.queue.get_node('cluster')['state'], 'active')
        self.assertEqual(json.loads((self.work / 'update.json').read_text())['stage'], 'completed')
        with self.assertRaises(UpdateError):
            self.run_update()
        self.assertEqual(self.adapter.calls, [NEW])

    def test_frozen_plan_refuses_a_cancelled_and_rescheduled_operation(self):
        original = self.plan()
        self.operator.json('POST', '/v1/prototype/nodes/cluster/cancel', {
            'planId': original['planId'], 'expectedRevision': original['revision']})
        replacement = self.plan()
        self.assertNotEqual(original['planId'], replacement['planId'])
        with self.assertRaises(UpdateError):
            execute(self.api, self.operator, self.adapter, 'cluster', self.work,
                    expected_plan_id=original['planId'], expected_target=NEW)
        self.assertEqual(self.adapter.calls, [])
        self.assertFalse((self.work / 'update.json').exists())
        self.assertEqual(self.queue.get_node('cluster')['planId'], replacement['planId'])

    def test_failed_health_rolls_back_but_keeps_node_blocked(self):
        self.plan()
        self.adapter.bad_health = True
        result = self.run_update()
        self.assertEqual(self.adapter.calls, [NEW, OLD])
        self.assertTrue(result['rollbackHealthy'])
        self.assertEqual(self.queue.get_node('cluster')['state'], 'failed')
        self.assertIsNone(self.queue.claim({'workerId': 'blocked'}))

    def test_unknown_mutation_never_rolls_back_or_replays(self):
        self.plan()
        self.adapter.uncertain = True
        with self.assertRaises(Uncertain):
            self.run_update()
        self.assertEqual(self.adapter.calls, [NEW])
        self.assertEqual(self.queue.get_node('cluster')['state'], 'updating')
        self.assertEqual(json.loads((self.work / 'update.json').read_text())['stage'], 'replace_pending')
        with self.assertRaises(UpdateError):
            self.run_update()
        self.assertEqual(self.adapter.calls, [NEW])

    def test_engine_busy_or_future_time_does_not_start_update(self):
        self.plan(future=True)
        self.assertEqual(self.run_update()['status'], 'waiting')
        self.assertFalse((self.work / 'update.json').exists())
        self.assertEqual(self.adapter.calls, [])

    def test_engine_inventory_busy_does_not_reserve_or_mutate(self):
        self.plan()
        self.adapter.busy = True
        self.assertEqual(self.run_update()['status'], 'waiting')
        self.assertEqual(self.queue.get_node('cluster')['state'], 'draining')
        self.assertFalse((self.work / 'update.json').exists())

    def test_held_job_blocks_even_when_engine_reports_idle(self):
        self.queue.submit({'requestId': 'held', 'environment': 'production', 'profile': 'treedar-synthetic-v1'})
        job = self.queue.claim({'workerId': 'worker'})
        self.queue.operator(job['id'], 'hold', {'note': 'worker stop uncertain'})
        self.plan()
        self.assertEqual(self.run_update()['status'], 'waiting')
        self.assertEqual(self.adapter.calls, [])

    def test_missing_target_prevents_reservation_and_docker_action(self):
        self.plan()
        self.adapter.missing = True
        with self.assertRaises(UpdateError):
            self.run_update()
        self.assertEqual(self.queue.get_node('cluster')['state'], 'draining')
        self.assertFalse((self.work / 'update.json').exists())

    def test_config_preflight_failure_does_not_reserve_or_recreate(self):
        self.plan()
        with patch.object(self.adapter, 'prepare', side_effect=UpdateError('invalid configuration')):
            with self.assertRaises(UpdateError):
                self.run_update()
        self.assertEqual(self.adapter.calls, [])
        self.assertFalse((self.work / 'update.json').exists())
        self.assertEqual(self.queue.get_node('cluster')['state'], 'draining')

    def test_changed_preflight_after_reservation_does_not_rollback_recreate(self):
        self.plan()
        with patch.object(self.adapter, 'replace', side_effect=PreflightError('configuration changed')):
            result = self.run_update()
        self.assertEqual(result['status'], 'failed')
        self.assertFalse(result['rollbackPerformed'])
        self.assertEqual(self.adapter.calls, [])
        self.assertEqual(self.queue.get_node('cluster')['state'], 'failed')

    def test_worker_credential_cannot_plan_or_update_node(self):
        with self.assertRaises(SmokeError):
            self.api.json('POST', '/v1/prototype/nodes/cluster/begin', {})
        self.assertEqual(self.api.json('GET', '/v1/prototype/nodes/cluster')['node']['state'], 'active')

    def test_lost_finish_response_keeps_receipt_and_never_recreates_twice(self):
        self.plan()
        original = self.operator.json
        def lost(method, route, payload=None):
            original(method, route, payload)
            raise SmokeError('response lost')
        with patch.object(self.operator, 'json', side_effect=lambda method, route, payload=None:
                          lost(method, route, payload) if route.endswith('/finish') else original(method, route, payload)):
            with self.assertRaises(SmokeError):
                self.run_update()
        self.assertEqual(self.queue.get_node('cluster')['state'], 'active')
        self.assertEqual(json.loads((self.work / 'update.json').read_text())['stage'], 'finish_pending')
        with self.assertRaises(UpdateError):
            self.run_update()
        self.assertEqual(self.adapter.calls, [NEW])


class ComposeBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.args = SimpleNamespace(project='test-project', service='node', compose_file='fixture.json',
            node_origin='http://127.0.0.1:12345', docker_engine_id='expected-engine', node_port=3000)
        self.adapter = ComposeNode(self.args, Path(self.temp.name))
        self.item = {'Id': 'exact-container', 'Image': OLD, 'State': {'Running': True},
            'Config': {'Labels': {'com.docker.compose.project': 'test-project', 'com.docker.compose.service': 'node'}},
            'NetworkSettings': {'Ports': {'3000/tcp': [{'HostIp': '127.0.0.1', 'HostPort': '12345'}]}}}
        self.service = {'image': NEW}
        self.commands = []

    def tearDown(self):
        self.temp.cleanup()

    def docker(self, command, **_):
        self.commands.append(command)
        if 'context' in command:
            return json.dumps('unix:///var/run/docker.sock')
        if 'info' in command:
            return 'expected-engine\n'
        if 'ps' in command:
            return 'exact-container\n'
        if command[1:3] == ['image', 'inspect']:
            return NEW + '\n'
        if command[1] == 'inspect':
            return json.dumps([self.item])
        if 'config' in command:
            return json.dumps({'services': {'node': self.service}})
        return ''

    def test_exact_loopback_port_and_compose_ownership_required(self):
        with patch.dict(os.environ, {'DOCKER_HOST': ''}), patch.object(self.adapter, 'docker', side_effect=self.docker):
            self.assertEqual(self.adapter.inspect()['Id'], 'exact-container')
            self.item['NetworkSettings']['Ports']['3000/tcp'][0]['HostPort'] = '54321'
            with self.assertRaises(UpdateError):
                self.adapter.inspect()
            self.item['NetworkSettings']['Ports']['3000/tcp'][0]['HostPort'] = '12345'
            self.item['Config']['Labels']['com.docker.compose.project'] = 'other'
            with self.assertRaises(UpdateError):
                self.adapter.inspect()

    def test_public_binding_or_remote_daemon_refused(self):
        with patch.dict(os.environ, {'DOCKER_HOST': 'ssh://other-host'}):
            with self.assertRaises(UpdateError):
                self.adapter.engine()
        with patch.dict(os.environ, {'DOCKER_HOST': ''}), patch.object(self.adapter, 'docker', side_effect=self.docker):
            self.item['NetworkSettings']['Ports']['3000/tcp'][0]['HostIp'] = '0.0.0.0'
            with self.assertRaises(UpdateError):
                self.adapter.inspect()

    def test_explicit_context_takes_precedence_over_local_host_variable(self):
        with patch.dict(os.environ, {'DOCKER_HOST': 'unix:///local.sock', 'DOCKER_CONTEXT': 'remote'}), \
                patch.object(self.adapter, 'docker', return_value=json.dumps('ssh://remote')) as command:
            with self.assertRaises(UpdateError):
                self.adapter.engine()
            self.assertIn('remote', command.call_args.args[0])

    def test_orphan_processing_child_blocks_idle_census(self):
        base = 'PID COMMAND\n1 /sbin/docker-init -- /usr/bin/node /var/www/index.js\n7 node /var/www/index.js\n'
        with patch.object(self.adapter, 'docker', return_value=base):
            self.assertTrue(self.adapter.processing_idle('exact-container'))
        with patch.object(self.adapter, 'docker', return_value=base + '12 python /code/opensfm.py\n'):
            self.assertFalse(self.adapter.processing_idle('exact-container'))
        with patch.object(self.adapter, 'docker', return_value='COMMAND\n'):
            self.assertFalse(self.adapter.processing_idle('exact-container'))

    def test_only_exact_regular_nodeodx_placeholder_is_not_an_upload(self):
        with patch.object(self.adapter, 'docker', side_effect=['.gitignore\n', 'regular file 1\n', '*']):
            self.assertTrue(self.adapter.uploads_idle('exact-container'))
        for listing in ('.gitignore\nupload\n', '.other\n', 'upload\n'):
            with patch.object(self.adapter, 'docker', return_value=listing) as command:
                self.assertFalse(self.adapter.uploads_idle('exact-container'))
                self.assertEqual(command.call_count, 1)
        for kind in ('symbolic link 1\n', 'regular file 2\n', 'directory 1\n'):
            with patch.object(self.adapter, 'docker', side_effect=['.gitignore\n', kind]):
                self.assertFalse(self.adapter.uploads_idle('exact-container'))
        with patch.object(self.adapter, 'docker', side_effect=['.gitignore\n', 'regular file 1\n', '\n']):
            self.assertFalse(self.adapter.uploads_idle('exact-container'))
        with patch.object(self.adapter, 'docker', return_value=''):
            self.assertTrue(self.adapter.uploads_idle('exact-container'))

    def test_compose_hooks_or_replication_refused_before_replacement(self):
        with patch.dict(os.environ, {'DOCKER_HOST': ''}), patch.object(self.adapter, 'docker', side_effect=self.docker):
            for extra in ({'post_start': [{'command': 'arbitrary'}]}, {'deploy': {'replicas': 2}}, {'deploy': {'mode': 'global'}}):
                self.service = {'image': NEW, **extra}
                with self.assertRaises(UpdateError):
                    self.adapter.replace(NEW)
            self.assertFalse(any('up' in command for command in self.commands))

    def test_replace_only_selected_service_without_pull_build_or_dependencies(self):
        with patch.dict(os.environ, {'DOCKER_HOST': ''}), patch.object(self.adapter, 'docker', side_effect=self.docker):
            self.adapter.replace(NEW)
        command = self.commands[-1]
        self.assertIn('--no-deps', command)
        self.assertIn('--no-build', command)
        self.assertEqual(command[command.index('--pull') + 1], 'never')
        self.assertEqual(command[-1], 'node')

    def test_docker_timeout_is_uncertain_not_safe_rollback_signal(self):
        with patch('processing.container_update.subprocess.run', side_effect=subprocess.TimeoutExpired('docker', 1)):
            with self.assertRaises(Uncertain):
                self.adapter.docker(['docker', 'compose', 'up'])


if __name__ == '__main__':
    unittest.main()
