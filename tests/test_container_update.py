from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace
import os
import subprocess
import sys
from urllib.parse import urlsplit

from processing.app import make_handler
from processing.config import Config
from processing.container_update import execute,ComposeNode,UpdateError,Uncertain,PreflightError,finalize_deferred
from processing.odx_smoke_worker import Client, SmokeError
from ops.native_node_fixture import NativeNodeFixture
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

    def deferred(self):
        plan=self.plan()
        result=execute(self.api,self.operator,self.adapter,'cluster',self.work,health_seconds=0,defer_finish=True)
        self.assertEqual(self.queue.get_node('cluster')['state'],'updating')
        self.assertNotIn('updateToken',result)
        return plan,dict(healthy=True,image=NEW,previousImage=OLD,composePinUpdateRequired=False,
            persistentComposePin=NEW,ingress='open')

    def test_deferred_success_retains_capacity_until_exact_idempotent_finish(self):
        plan,proof=self.deferred()
        self.assertIsNone(self.queue.claim({'workerId':'blocked'}))
        receipt=json.loads((self.work/'update.json').read_text())
        self.assertEqual(receipt['stage'],'finalization_pending')
        for _ in range(2):
            result=finalize_deferred(self.api,self.operator,'cluster',self.work,
                expected_plan_id=plan['planId'],expected_target=NEW,result=proof)
            self.assertNotIn(receipt['updateToken'],json.dumps(result))
        self.assertEqual(self.queue.get_node('cluster')['state'],'active')
        self.assertEqual(self.adapter.calls,[NEW])

    def test_deferred_lost_finish_ack_is_read_back_without_docker_replay(self):
        plan,proof=self.deferred();original=self.operator.json
        calls=[]
        def lost(method,route,payload=None):
            calls.append(route)
            value=original(method,route,payload)
            if route.endswith('/finish'):raise SmokeError('synthetic lost finish ACK')
            return value
        with patch.object(self.operator,'json',side_effect=lost):
            result=finalize_deferred(self.api,self.operator,'cluster',self.work,
                expected_plan_id=plan['planId'],expected_target=NEW,result=proof)
        self.assertEqual(result['status'],'succeeded')
        self.assertEqual(sum(route.endswith('/finish') for route in calls),1)
        self.assertEqual(self.adapter.calls,[NEW])

    def test_deferred_changed_plan_or_completion_never_activates(self):
        plan,proof=self.deferred()
        with self.assertRaises(UpdateError):
            finalize_deferred(self.api,self.operator,'cluster',self.work,
                expected_plan_id='different-plan',expected_target=NEW,result=proof)
        with self.assertRaises(UpdateError):
            finalize_deferred(self.api,self.operator,'cluster',self.work,
                expected_plan_id=plan['planId'],expected_target=NEW,result=dict(proof,updateToken='must-not-be-public'))
        node=self.queue.get_node('cluster')
        self.operator.json('POST','/v1/prototype/nodes/cluster/hold',dict(planId=node['planId'],expectedRevision=node['revision'],note='inspection'))
        with self.assertRaises(UpdateError):
            finalize_deferred(self.api,self.operator,'cluster',self.work,
                expected_plan_id=plan['planId'],expected_target=NEW,result=proof)
        self.assertEqual(self.queue.get_node('cluster')['state'],'held')
        self.assertEqual(self.adapter.calls,[NEW])

    def test_busy_after_reserved_callback_never_replaces_or_rolls_back(self):
        self.plan()
        def drained(reserved):
            self.assertEqual(self.queue.get_node('cluster')['state'],'updating')
            receipt=json.loads((self.work/'update.json').read_text())
            self.assertIn('updateToken',receipt)
            self.assertNotIn('updateToken',reserved)
            self.adapter.busy=True
        with self.assertRaises(UpdateError):
            execute(self.api,self.operator,self.adapter,'cluster',self.work,health_seconds=0,
                defer_finish=True,on_reserved=drained)
        self.assertEqual(self.adapter.calls,[])
        self.assertEqual(self.queue.get_node('cluster')['state'],'updating')


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

    def test_standalone_cli_refuses_unverified_admission_without_mutation(self):
        # Use the actual entry point; no coordinator, Docker daemon or secrets
        # are required or contacted even when the historical flag is supplied.
        args=[sys.executable,'-m','processing.container_update',
              '--coordinator','http://127.0.0.1:9','--node-id','cluster',
              '--node-origin','http://127.0.0.1:9','--compose-file','unused.yaml',
              '--project','fixture','--service','fixture','--docker-engine-id','fixture',
              '--work',self.temp.name,'--exclusive-admission']
        result=subprocess.run(args,capture_output=True,text=True,timeout=5)
        self.assertEqual(result.returncode,2)
        self.assertIn('Standalone maintenance is disabled',result.stdout)
        self.assertIn('managed node maintenance',result.stdout)
        self.assertFalse((Path(self.temp.name)/'updater.lock').exists())
        self.assertFalse((Path(self.temp.name)/'update.json').exists())
        empty=subprocess.run([sys.executable,'-m','processing.container_update'],capture_output=True,text=True,timeout=5)
        self.assertEqual(empty.returncode,2);self.assertIn('Standalone maintenance is disabled',empty.stdout)
        help_text=subprocess.run([sys.executable,'-m','processing.container_update','--help'],capture_output=True,text=True,timeout=5)
        self.assertEqual(help_text.returncode,0);self.assertIn('Standalone node maintenance is disabled',help_text.stdout)
        self.assertIn('does not verify ingress',help_text.stdout)

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
        for listing in ('unexpected header\n7 node /var/www/index.js\n',
                        'PID COMMAND\n7 node /var/www/index.js\n7 /sbin/docker-init\n',
                        'PID COMMAND\n0 node /var/www/index.js\n',
                        'PID COMMAND\n7 node /var/www/index.js\n007 /sbin/docker-init\n',
                        'PID COMMAND\n² node /var/www/index.js\n',
                        'PID COMMAND\n99999999999 node /var/www/index.js\n'):
            with self.subTest(listing=listing), patch.object(self.adapter, 'docker', return_value=listing):
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

    def test_existing_cluster_resources_are_preserved_and_none_added_for_main(self):
        with patch.dict(os.environ,{'DOCKER_HOST':''}),patch.object(self.adapter,'docker',side_effect=self.docker):
            for service in ({'image':NEW},{'image':NEW,'mem_limit':'4g','cpus':2,'pids_limit':128}):
                self.service=service
                self.adapter.replace(NEW)
                resolved=json.loads((Path(self.temp.name)/'resolved-compose.json').read_text())
                self.assertEqual(resolved['services']['node'],service)

    def test_docker_timeout_is_uncertain_not_safe_rollback_signal(self):
        with patch('processing.container_update.subprocess.run', side_effect=subprocess.TimeoutExpired('docker', 1)):
            with self.assertRaises(Uncertain):
                self.adapter.docker(['docker', 'compose', 'up'])

    def test_actual_http_odm_requires_explicit_verified_layout_and_rejects_odx(self):
        backend=NativeNodeFixture(Path(self.temp.name)/'node.sqlite',synthetic_enabled=True)
        backend.start();self.addCleanup(backend.close)
        self.args.node_origin=backend.origin
        self.args.expected_engine='odm'
        with self.assertRaises(UpdateError):ComposeNode(self.args,Path(self.temp.name))
        self.args.upload_layout='var-www-tmp-v1';self.args.process_layout='node-var-www-v1'
        adapter=ComposeNode(self.args,Path(self.temp.name))
        self.item['NetworkSettings']['Ports']['3000/tcp'][0]['HostPort']=str(urlsplit(backend.origin).port)
        def docker(command,**kwargs):
            if command[1]=='exec':
                if '/bin/ls' in command:return '.gitignore\n'
                if '/usr/bin/stat' in command:return 'regular file 1\n'
                if '/bin/cat' in command:return '*'
            if command[1]=='top':return 'PID COMMAND\n7 node /var/www/index.js\n'
            return self.docker(command,**kwargs)
        with patch.dict(os.environ,{'DOCKER_HOST':''}),patch.object(adapter,'docker',side_effect=docker):
            self.assertTrue(adapter.healthy(OLD))
            adapter.expected_engine='odx'
            self.assertFalse(adapter.healthy(OLD))
        self.assertFalse(any(backend.counts().values()))


if __name__ == '__main__':
    unittest.main()
