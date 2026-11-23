from contextlib import contextmanager, ExitStack
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import tempfile
import unittest
import uuid
from unittest.mock import MagicMock, patch

from ops import auto_deploy as agent
from ops.auto_deploy import DeployError
from processing.container_update import save
from processing import release_packages

REPO = release_packages.repository('coordinator')
OLD = 'sha256:' + 'a' * 64
NEW = 'sha256:' + 'b' * 64
DIGEST = 'sha256:' + 'c' * 64
NOW = datetime(2026, 11, 22, 12, 0, tzinfo=timezone.utc)
AUTHORITY = str(uuid.UUID(int=7))


def image(identifier, run, *, ref='refs/heads/main', event='push', package='coordinator',
          source=release_packages.SOURCE_URL, digests=()):
    return {'Id': identifier, 'Os': 'linux', 'Architecture': 'amd64', 'RepoDigests': list(digests),
            'Config': {'User': '10001:10001', 'Labels': {
                release_packages.SOURCE_LABEL: source, release_packages.PACKAGE_LABEL: package,
                release_packages.REVISION_LABEL: format(run, 'x').rjust(40, '0'),
                release_packages.RUN_LABEL: str(run), release_packages.REF_LABEL: ref,
                release_packages.EVENT_LABEL: event}}}


class AutoDeployTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.settings = {'nodeId': 'main', 'project': 'owned', 'service': 'node',
                         'autoDeploy': {'package': 'coordinator'}}
        save(self.root / 'compose.json', {'services': {'node': {'image': OLD}}})
        self.node = {'state': 'active', 'revision': 4, 'planId': None, 'targetImage': None}
        self.api = MagicMock()
        self.api.json.side_effect = lambda method, path: (
            {'authorityId': AUTHORITY} if path.endswith('/authority') else {'node': dict(self.node)})
        self.owned = {}
        self.images = {OLD: image(OLD, 100)}
        self.publish(image(NEW, 200, digests=[REPO + '@' + DIGEST]))
        self.pulls = []
        self.schedule = MagicMock(side_effect=self.scheduled)
        self.run = MagicMock(return_value={'status': 'succeeded', 'image': NEW})

    def tearDown(self):
        self.temp.cleanup()

    def publish(self, item, by_digest=None):
        self.images[REPO + ':verified'] = item
        self.images[REPO + '@' + DIGEST] = by_digest or item

    def scheduled(self, request):
        # The coordinator's owned schedule: identical replay returns the stored
        # receipt; otherwise a compare-and-swap on an active node's revision.
        if request['requestId'] in self.owned:
            stored_request, receipt = self.owned[request['requestId']]
            if stored_request != request:
                raise RuntimeError('owned schedule requestId is already bound')
            return dict(receipt)
        if self.node['state'] != 'active' or self.node['revision'] != request['expectedRevision']:
            raise RuntimeError('HTTP request rejected')
        plan = str(uuid.uuid4())
        self.node.update(state='draining', planId=plan, targetImage=request['targetImage'],
                         revision=self.node['revision'] + 1)
        receipt = {'schemaVersion': 1, 'requestId': request['requestId'], 'nodeId': 'main', 'planId': plan,
                   'expectedRevision': request['expectedRevision'], 'targetImage': request['targetImage'],
                   'revision': self.node['revision'], 'state': 'draining'}
        self.owned[request['requestId']] = (dict(request), receipt)
        return dict(receipt)

    def inspect(self, reference):
        if reference not in self.images:
            raise DeployError('missing local image')
        return json.loads(json.dumps(self.images[reference]))

    def docker(self, args, timeout=900):
        self.pulls.append(args[-1])
        return ''

    @contextmanager
    def patched(self):
        @contextmanager
        def acquired(directory):
            yield True
        with ExitStack() as stack:
            stack.enter_context(patch.object(agent, 'docker', side_effect=self.docker))
            stack.enter_context(patch.object(agent, 'inspect', side_effect=self.inspect))
            stack.enter_context(patch.object(agent, 'agent_lock', acquired))
            yield

    def deploy(self):
        with self.patched():
            return agent.auto_deploy(self.root, self.settings, self.api, schedule=self.schedule, run=self.run,
                                     wait_seconds=60, drain_seconds=60, now=NOW)

    def state(self):
        return json.loads((self.root / 'auto-deploy' / 'state.json').read_text())

    def last(self):
        return json.loads((self.root / 'auto-deploy' / 'last-run.json').read_text())

    def test_newer_main_candidate_is_pulled_by_digest_then_drained_through_runner(self):
        result = self.deploy()
        self.assertEqual(result['status'], 'succeeded')
        self.assertEqual(self.pulls, [REPO + ':verified', REPO + '@' + DIGEST])
        request = self.schedule.call_args.args[0]
        self.assertEqual({key: request[key] for key in ('schemaVersion', 'authorityId', 'expectedRevision',
                                                         'targetImage', 'drainAt', 'notBefore', 'afterJobId')},
                         {'schemaVersion': 1, 'authorityId': AUTHORITY, 'expectedRevision': 4, 'targetImage': NEW,
                          'drainAt': '2026-11-22T12:00:00Z', 'notBefore': '2026-11-22T12:00:00Z',
                          'afterJobId': None})
        self.schedule.assert_called_once()
        receipt, wait, drain, plan, target = self.run.call_args.args
        self.assertEqual((receipt.parent.name, wait, drain), ('receipts', 60, 60))
        self.assertEqual((plan, target), (self.node['planId'], NEW))
        self.assertEqual(self.state()['applied']['planId'], plan)
        self.assertIsNone(self.state()['scheduling'])
        self.assertEqual(self.state()['applied']['imageId'], NEW)
        self.assertIsNone(self.state()['inProgress'])
        self.assertEqual(self.state()['highestRunId'], 200)
        self.assertEqual(self.last()['status'], 'succeeded')

    def test_drain_delay_from_host_binding(self):
        self.settings['autoDeploy']['drainDelaySeconds'] = 900
        self.deploy()
        request = self.schedule.call_args.args[0]
        self.assertEqual((request['drainAt'], request['notBefore']), ('2026-11-22T12:15:00Z', '2026-11-22T12:15:00Z'))

    def test_hold_file_pauses_before_any_pull_or_schedule(self):
        (self.root / agent.HOLD_FILE).write_text('hold\n')
        self.assertEqual(self.deploy(), {'status': 'paused'})
        self.assertEqual(self.pulls, [])
        self.api.json.assert_not_called()
        self.schedule.assert_not_called()
        self.run.assert_not_called()
        self.assertEqual(self.last()['status'], 'paused')

    def test_hold_also_stops_resuming_a_scheduled_window(self):
        self.run.return_value = {'status': 'waiting', 'nodeState': 'draining'}
        self.deploy()
        (self.root / agent.HOLD_FILE).write_text('')
        self.assertEqual(self.deploy()['status'], 'paused')
        self.assertEqual(self.run.call_count, 1)

    def test_host_without_binding_is_not_enabled(self):
        del self.settings['autoDeploy']
        self.assertEqual(self.deploy()['status'], 'not_enabled')
        self.assertEqual(self.pulls, [])

    def test_non_main_push_candidate_is_ignored(self):
        for changes in ({'ref': 'refs/heads/feature'}, {'event': 'workflow_dispatch'}, {'event': 'pull_request'}):
            with self.subTest(changes=changes):
                self.publish(image(NEW, 200, digests=[REPO + '@' + DIGEST], **changes))
                result = self.deploy()
                self.assertEqual((result['status'], result['reason']), ('ignored', 'not_a_main_push'))
        self.schedule.assert_not_called()
        self.run.assert_not_called()

    def test_older_or_equal_candidate_is_a_no_op(self):
        for run in (100, 99):
            with self.subTest(run=run):
                self.publish(image(NEW, run, digests=[REPO + '@' + DIGEST]))
                self.assertEqual(self.deploy()['status'], 'not_newer')
        self.publish(image(OLD, 100, digests=[REPO + '@' + DIGEST]))
        self.assertEqual(self.deploy()['status'], 'up_to_date')
        self.schedule.assert_not_called()
        self.run.assert_not_called()

    def test_success_then_same_candidate_again_is_idempotent(self):
        self.deploy()
        save(self.root / 'compose.json', {'services': {'node': {'image': NEW}}})
        self.node.update(state='active', targetImage=None)
        self.images[NEW] = self.images[REPO + ':verified']
        self.assertEqual(self.deploy()['status'], 'up_to_date')
        self.schedule.assert_called_once()
        self.run.assert_called_once()

    def test_digest_mismatch_or_wrong_identity_is_refused(self):
        cases = {
            'pulled_id_differs': (image(NEW, 200, digests=[REPO + '@' + DIGEST]),
                                  image('sha256:' + 'd' * 64, 200, digests=[REPO + '@' + DIGEST])),
            'digest_not_on_pulled_image': (image(NEW, 200, digests=[REPO + '@' + DIGEST]),
                                           image(NEW, 200, digests=[])),
            'ambiguous_tag_digest': (image(NEW, 200, digests=[REPO + '@' + DIGEST, REPO + '@sha256:' + 'e' * 64]), None),
            'no_tag_digest': (image(NEW, 200, digests=[]), None),
            'other_source': (image(NEW, 200, digests=[REPO + '@' + DIGEST], source='https://github.com/other/repo'), None),
            'other_package': (image(NEW, 200, digests=[REPO + '@' + DIGEST], package='treedar'), None),
        }
        for name, (tagged, pulled) in cases.items():
            with self.subTest(name=name):
                self.publish(tagged, pulled)
                result = self.deploy()
                self.assertEqual(result['status'], 'refused')
                self.assertEqual(self.last()['status'], 'refused')
        self.schedule.assert_not_called()
        self.run.assert_not_called()

    def test_wrong_platform_or_root_user_is_refused(self):
        for field, value in (('Architecture', 'arm64'), ('Os', 'windows')):
            with self.subTest(field=field):
                item = image(NEW, 200, digests=[REPO + '@' + DIGEST])
                item[field] = value
                self.publish(item)
                self.assertEqual(self.deploy()['status'], 'refused')
        item = image(NEW, 200, digests=[REPO + '@' + DIGEST])
        item['Config']['User'] = 'root'
        self.publish(item)
        self.assertEqual(self.deploy()['status'], 'refused')
        self.schedule.assert_not_called()

    def test_service_not_already_running_the_package_needs_manual_adoption(self):
        self.images[OLD] = {'Id': OLD, 'Config': {'Labels': {}}}
        result = self.deploy()
        self.assertEqual(result['status'], 'refused')
        self.assertIn('adopt it manually', result['reason'])
        self.schedule.assert_not_called()

    def test_failure_rolls_back_through_runner_and_is_never_retried(self):
        self.run.return_value = {'status': 'failed', 'healthy': False, 'rollbackHealthy': True,
                                 'restoredImage': OLD, 'ingress': 'closed'}
        result = self.deploy()
        self.assertEqual((result['status'], result['restoredImage']), ('failed', OLD))
        self.assertEqual(self.state()['failed']['imageId'], NEW)
        self.assertIsNone(self.state()['inProgress'])
        self.node.update(state='failed')
        self.assertEqual(self.deploy()['status'], 'not_eligible')
        # After an operator resumes the node, the same failed run stays skipped.
        self.node.update(state='active', targetImage=None)
        self.assertEqual(self.deploy()['status'], 'not_newer')
        self.run.assert_called_once()
        self.schedule.assert_called_once()

    def test_waiting_window_resumes_the_same_plan_without_rescheduling(self):
        self.run.return_value = {'status': 'waiting', 'nodeState': 'draining'}
        self.assertEqual(self.deploy()['status'], 'waiting')
        self.assertEqual(self.state()['inProgress']['imageId'], NEW)
        self.run.return_value = {'status': 'succeeded', 'image': NEW}
        self.assertEqual(self.deploy()['status'], 'succeeded')
        self.schedule.assert_called_once()
        self.assertEqual(self.run.call_args_list[0].args[0], self.run.call_args_list[1].args[0])
        self.assertEqual(self.pulls.count(REPO + '@' + DIGEST), 1)

    def test_same_image_plan_created_by_someone_else_is_never_adopted(self):
        self.node.update(state='draining', planId=str(uuid.uuid4()), targetImage=NEW, revision=9)
        result = self.deploy()
        self.assertEqual((result['status'], result['reason']), ('not_eligible', 'other_plan'))
        self.assertEqual(self.pulls, [])
        self.schedule.assert_not_called()
        self.run.assert_not_called()
        self.assertFalse((self.root / 'auto-deploy' / 'state.json').exists())

    def test_lost_schedule_response_replays_identical_request_and_binds_its_plan(self):
        created = []
        def lost(request):
            created.append(self.scheduled(request))
            raise RuntimeError('HTTP response uncertain')
        self.schedule.side_effect = lost
        with self.assertRaises(RuntimeError):
            self.deploy()
        self.assertEqual(self.last()['status'], 'stopped')
        self.assertIsNone(self.state().get('inProgress'))
        first = self.state()['scheduling']['request']
        self.schedule.side_effect = self.scheduled
        self.assertEqual(self.deploy()['status'], 'succeeded')
        self.assertEqual(self.schedule.call_args.args[0], first)
        self.assertEqual(self.run.call_args.args[3], created[0]['planId'])

    def test_schedule_conflict_keeps_intent_and_never_runs(self):
        def conflict(request):
            self.node.update(revision=self.node['revision'] + 1)
            raise RuntimeError('HTTP request rejected')
        self.schedule.side_effect = conflict
        for _ in range(2):
            with self.assertRaises(RuntimeError):
                self.deploy()
        requests = [call.args[0] for call in self.schedule.call_args_list]
        self.assertEqual(requests[0], requests[1])
        self.assertIsNotNone(self.state()['scheduling'])
        self.run.assert_not_called()

    def test_receipt_for_another_target_or_request_is_refused(self):
        for change in ({'targetImage': 'sha256:' + 'f' * 64}, {'requestId': str(uuid.uuid4())},
                       {'planId': 'not-a-plan'}, {'nodeId': 'cluster'}):
            with self.subTest(change=change):
                self.tearDown()
                self.setUp()
                self.schedule.side_effect = lambda request, change=change: {**self.scheduled(request), **change}
                self.assertEqual(self.deploy()['status'], 'refused')
                self.assertIsNone(self.state().get('inProgress'))
                self.run.assert_not_called()

    def test_plan_replaced_while_waiting_is_never_run(self):
        self.run.return_value = {'status': 'waiting', 'nodeState': 'draining'}
        self.deploy()
        mine = self.state()['inProgress']['planId']
        # Cancelled and rescheduled by someone else for the very same image.
        self.node.update(planId=str(uuid.uuid4()), revision=self.node['revision'] + 2)
        result = self.deploy()
        self.assertEqual((result['status'], result['reason']), ('not_eligible', 'plan_replaced'))
        self.assertEqual(self.run.call_count, 1)
        self.assertEqual(self.state()['cancelled']['planId'], mine)
        self.assertEqual(self.deploy()['status'], 'not_eligible')
        self.assertEqual(self.run.call_count, 1)

    def test_another_plan_is_never_adopted_or_replaced(self):
        self.node.update(state='scheduled', targetImage='sha256:' + 'f' * 64)
        result = self.deploy()
        self.assertEqual((result['status'], result['reason']), ('not_eligible', 'other_plan'))
        self.schedule.assert_not_called()
        self.run.assert_not_called()

    def test_operator_cancel_keeps_floor_and_is_not_retried(self):
        self.run.return_value = {'status': 'waiting', 'nodeState': 'scheduled'}
        self.deploy()
        self.node.update(state='active', targetImage=None)
        self.assertEqual(self.deploy()['status'], 'cancelled')
        self.assertEqual(self.deploy()['status'], 'not_newer')
        self.schedule.assert_called_once()

    def test_failed_or_held_node_is_left_for_inspection(self):
        for state in ('failed', 'held', 'updating'):
            with self.subTest(state=state):
                self.node.update(state=state)
                self.assertEqual(self.deploy()['status'], 'not_eligible')
        self.assertEqual(self.pulls, [])
        self.schedule.assert_not_called()

    def test_runner_stop_is_recorded_and_raised_not_replayed(self):
        self.run.side_effect = RuntimeError('existing operation receipt')
        with self.assertRaises(RuntimeError):
            self.deploy()
        self.assertEqual(self.last()['status'], 'stopped')
        self.assertEqual(self.state()['inProgress']['imageId'], NEW)


@unittest.skipIf(os.name == 'nt', 'Installed node automation uses Linux host locks')
class AutoDeployRunnerTests(unittest.TestCase):
    """The agent's failure path goes through the real drain-window runner."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        (self.root / 'ingress').mkdir()
        save(self.root / 'maintenance.json', {'nodeId': 'main', 'project': 'owned', 'service': 'node'})
        save(self.root / 'compose.json', {'services': {'node': {'image': OLD}}})

    def tearDown(self):
        self.temp.cleanup()

    def test_lock_is_exclusive_between_timer_passes(self):
        directory = agent.state_directory(self.root)
        with agent.agent_lock(directory) as first:
            self.assertTrue(first)
            pid = os.fork()
            if pid == 0:
                with agent.agent_lock(directory) as second:
                    os._exit(0 if second is False else 1)
            self.assertEqual(os.waitpid(pid, 0)[1], 0)

    def fixtures(self, node):
        api = MagicMock(origin='http://127.0.0.1:18080')
        api.json.side_effect = lambda method, path: (
            {'jobs': []} if path.endswith('/jobs') else
            {'authorityId': AUTHORITY} if path.endswith('/authority') else {'node': dict(node)})
        ingress = MagicMock()
        ingress.config = {'backendPort': 13004, 'backendName': 'owned', 'engineId': 'engine'}
        ingress.docker.return_value = json.dumps([{'Id': 'exact-backend'}])
        adapter = MagicMock()
        adapter.inspect.return_value = {'Id': 'exact-backend'}
        gate = MagicMock()
        gate.snapshot.return_value = {'reservation': None, 'nodeId': 'main',
                                      'coordinatorOrigin': api.origin, 'authorityId': AUTHORITY}
        return api, ingress, adapter, gate

    def patched_runner(self, ingress, adapter, gate, execute_result):
        stack = ExitStack()
        stack.enter_context(patch('ops.node_maintenance.Ingress', return_value=ingress))
        stack.enter_context(patch('ops.node_maintenance.ComposeNode', return_value=adapter))
        host = stack.enter_context(patch('ops.node_maintenance.NodeGate'))
        host.return_value.__enter__.return_value = gate
        execute = stack.enter_context(patch('ops.node_maintenance.execute', return_value=execute_result))
        return stack, execute

    def test_failed_candidate_rolls_back_and_keeps_old_pin(self):
        from ops.node_maintenance import run, schedule_owned
        node = {'state': 'active', 'revision': 1, 'planId': None, 'targetImage': None,
                'notBefore': '2020-01-01T00:00:00Z'}
        api, ingress, adapter, gate = self.fixtures(node)
        operator = MagicMock()
        def coordinator(method, path, payload):
            self.assertTrue(path.endswith('/main/schedule-owned'))
            self.assertEqual(payload['expectedRevision'], node['revision'])
            plan = str(uuid.uuid4())
            node.update(state='draining', planId=plan, targetImage=payload['targetImage'], revision=2)
            return {'receipt': {'requestId': payload['requestId'], 'nodeId': 'main', 'planId': plan,
                                'expectedRevision': payload['expectedRevision'], 'targetImage': payload['targetImage']}}
        operator.json.side_effect = coordinator
        images = {OLD: image(OLD, 100), REPO + ':verified': image(NEW, 200, digests=[REPO + '@' + DIGEST]),
                  REPO + '@' + DIGEST: image(NEW, 200, digests=[REPO + '@' + DIGEST])}
        rolled_back = {'status': 'failed', 'nodeId': 'main', 'healthy': False,
                       'rollbackHealthy': True, 'restoredImage': OLD}
        settings = {'nodeId': 'main', 'project': 'owned', 'service': 'node', 'autoDeploy': {'package': 'coordinator'}}
        stack, execute = self.patched_runner(ingress, adapter, gate, rolled_back)
        with stack, patch.object(agent, 'docker', return_value=''), \
                patch.object(agent, 'inspect', side_effect=lambda ref: json.loads(json.dumps(images[ref]))):
            result = agent.auto_deploy(self.root, settings, api,
                schedule=lambda request: schedule_owned(self.root, settings, operator, request),
                run=lambda receipt, wait, drain, plan, target: run(
                    self.root, api, operator, receipt, wait, drain, expected_plan_id=plan, expected_target=target),
                wait_seconds=0, drain_seconds=60)
        self.assertEqual((result['status'], result['ingress']), ('failed', 'closed'))
        self.assertEqual(execute.call_args.kwargs['expected_target'], NEW)
        self.assertEqual(execute.call_args.kwargs['expected_plan_id'], node['planId'])
        self.assertEqual(json.loads((self.root / 'compose.json').read_text())['services']['node']['image'], OLD)
        state = json.loads((self.root / 'auto-deploy' / 'state.json').read_text())
        self.assertEqual(state['failed']['planId'], node['planId'])

    def test_runner_stops_before_ingress_when_the_bound_plan_changes_while_waiting(self):
        from ops.node_ingress import GateError
        from ops.node_maintenance import run
        mine, theirs = str(uuid.uuid4()), str(uuid.uuid4())
        polls = [{'state': 'scheduled', 'planId': mine, 'targetImage': NEW, 'notBefore': '2999-01-01T00:00:00Z'},
                 {'state': 'draining', 'planId': theirs, 'targetImage': NEW, 'notBefore': '2020-01-01T00:00:00Z'}]
        api, ingress, adapter, gate = self.fixtures({})
        api.json.side_effect = lambda method, path: {'node': polls.pop(0) if len(polls) > 1 else polls[0]}
        receipt = self.root / 'receipt'
        receipt.mkdir()
        stack, execute = self.patched_runner(ingress, adapter, gate, {'status': 'succeeded', 'image': NEW})
        with stack, patch('ops.node_maintenance.time.sleep'):
            with self.assertRaisesRegex(GateError, 'bound plan'):
                run(self.root, api, MagicMock(), receipt, 60, 60, expected_plan_id=mine, expected_target=NEW)
        ingress.change.assert_not_called()
        execute.assert_not_called()
        self.assertFalse((receipt / 'managed-update.json').exists())
        self.assertEqual(json.loads((self.root / 'compose.json').read_text())['services']['node']['image'], OLD)

    def test_runner_never_processes_another_plan_with_the_same_target(self):
        from ops.node_ingress import GateError
        from ops.node_maintenance import run
        node = {'state': 'draining', 'planId': str(uuid.uuid4()), 'targetImage': NEW,
                'notBefore': '2020-01-01T00:00:00Z'}
        api, ingress, adapter, gate = self.fixtures(node)
        receipt = self.root / 'receipt'
        receipt.mkdir()
        stack, execute = self.patched_runner(ingress, adapter, gate, {'status': 'succeeded', 'image': NEW})
        with stack:
            with self.assertRaises(GateError):
                run(self.root, api, MagicMock(), receipt, 0, 60, expected_plan_id=str(uuid.uuid4()),
                    expected_target=NEW)
            with self.assertRaises(GateError):
                run(self.root, api, MagicMock(), receipt, 0, 60, expected_plan_id=node['planId'])
        ingress.change.assert_not_called()
        execute.assert_not_called()


if __name__ == '__main__':
    unittest.main()
