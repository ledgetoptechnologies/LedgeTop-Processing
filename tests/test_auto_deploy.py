from contextlib import contextmanager, ExitStack
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import socket
import tempfile
import threading
import unittest
import uuid
from unittest.mock import MagicMock, patch

from ops import auto_deploy as agent
from ops.auto_deploy import DeployError
from processing.odx_smoke_worker import Client, HTTPRefused, HTTPUncertain
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
        self.lookup = MagicMock(side_effect=lambda request: (
            dict(self.owned[request['requestId']][1]) if request['requestId'] in self.owned else None))
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
            raise HTTPRefused('HTTP request rejected or redirected; inspect private receipt', 409)
        plan = str(uuid.uuid4())
        self.node.update(state='draining', planId=plan, targetImage=request['targetImage'],
                         revision=self.node['revision'] + 1)
        receipt = {'schemaVersion': 1, 'requestId': request['requestId'], 'nodeId': 'main', 'planId': plan,
                   'authorityId': request['authorityId'],
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
            return agent.auto_deploy(self.root, self.settings, self.api, schedule=self.schedule,
                                     lookup=self.lookup, run=self.run,
                                     wait_seconds=60, drain_seconds=60, now=NOW)

    def intent(self):
        path = self.root / 'auto-deploy' / agent.INTENT_FILE
        return json.loads(path.read_text()) if path.exists() else None

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
        self.assertIsNone(self.intent())
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

    def test_lost_schedule_response_is_reconciled_by_lookup_never_by_another_post(self):
        created = []
        def lost(request):
            created.append(self.scheduled(request))
            raise HTTPUncertain('HTTP response uncertain; mutation must not be retried')
        self.schedule.side_effect = lost
        self.assertEqual(self.deploy()['reason'], 'schedule_response_uncertain')
        self.assertFalse((self.root / 'auto-deploy' / 'state.json').exists())
        saved = self.intent()['request']
        self.assertEqual(self.deploy()['status'], 'succeeded')
        self.schedule.assert_called_once()
        self.lookup.assert_called_once_with(saved)
        self.assertEqual(self.run.call_args.args[3], created[0]['planId'])

    def test_uncertain_lookup_holds_without_posting(self):
        self.schedule.side_effect = HTTPUncertain('HTTP response uncertain; mutation must not be retried')
        self.deploy()
        self.lookup.side_effect = HTTPUncertain('HTTP response uncertain; mutation must not be retried')
        for _ in range(2):
            self.assertEqual(self.deploy(), {'status': 'held', 'reason': 'ownership_unproven',
                                             'requestId': self.intent()['request']['requestId']})
        self.schedule.assert_called_once()
        self.run.assert_not_called()

    def test_server_error_on_schedule_is_uncertain_not_a_refusal(self):
        self.schedule.side_effect = HTTPRefused('HTTP request rejected or redirected; inspect private receipt', 500)
        self.assertEqual(self.deploy()['status'], 'held')
        self.assertFalse((self.root / 'auto-deploy' / agent.REFUSAL_FILE).exists())
        self.assertIsNotNone(self.intent())

    def test_typed_absence_keeps_the_intent_across_passes_until_a_person_deletes_it(self):
        self.schedule.side_effect = HTTPUncertain('HTTP response uncertain; mutation must not be retried')
        self.deploy()
        first = self.intent()['request']
        for _ in range(3):
            result = self.deploy()
            self.assertEqual((result['status'], result['reason'], result['requestId']),
                             ('held', 'schedule_unresolved', first['requestId']))
            self.assertIn('before deleting auto-deploy/schedule-intent.json to retry', result['action'])
            self.assertIn('a momentary absence is not enough', result['action'])
            self.assertEqual(self.intent()['request'], first)
        self.assertEqual(self.last()['reason'], 'schedule_unresolved')
        self.assertIn('unresolved', self.intent())
        self.assertEqual(self.schedule.call_count, 1)
        self.assertEqual(self.lookup.call_count, 3)
        self.run.assert_not_called()
        # A person checked the node and deleted the intent: a fresh request follows.
        (self.root / 'auto-deploy' / agent.INTENT_FILE).unlink()
        self.schedule.side_effect = self.scheduled
        self.assertEqual(self.deploy()['status'], 'succeeded')
        self.assertNotEqual(self.schedule.call_args.args[0]['requestId'], first['requestId'])

    def test_late_commit_after_typed_absence_is_bound_not_other_plan(self):
        self.schedule.side_effect = HTTPUncertain('HTTP response uncertain; mutation must not be retried')
        self.deploy()
        saved = self.intent()['request']
        self.assertEqual(self.deploy()['reason'], 'schedule_unresolved')
        # The timed-out request commits only now, after the first lookup.
        receipt = self.scheduled(saved)
        result = self.deploy()
        self.assertEqual(result['status'], 'succeeded')
        self.assertEqual(self.run.call_args.args[3:], (receipt['planId'], NEW))
        self.assertIsNone(self.intent())
        self.assertEqual(self.schedule.call_count, 1)

    def test_definite_refusal_latches_and_nothing_is_posted_until_cleared(self):
        for status in (403, 409):
            with self.subTest(status=status):
                self.tearDown()
                self.setUp()
                self.schedule.side_effect = HTTPRefused('HTTP request rejected or redirected', status)
                result = self.deploy()
                self.assertEqual((result['status'], result['httpStatus']), ('refused', status))
                self.assertIsNone(self.intent())
                latch = self.root / 'auto-deploy' / agent.REFUSAL_FILE
                self.assertTrue(latch.exists())
                for _ in range(2):
                    self.assertEqual(self.deploy(), {'status': 'held', 'reason': 'refusal_latched'})
                self.schedule.assert_called_once()
                self.lookup.assert_not_called()
                self.run.assert_not_called()
                latch.unlink()
                self.schedule.side_effect = self.scheduled
                self.assertEqual(self.deploy()['status'], 'succeeded')

    def failing_save(self, name):
        real = agent.save
        def save_or_fail(path, value):
            if path.name == name:
                raise OSError('synthetic write failure')
            return real(path, value)
        return patch.object(agent, 'save', side_effect=save_or_fail)

    def test_refusal_latch_write_failure_keeps_intent_and_never_reposts(self):
        self.schedule.side_effect = HTTPRefused('HTTP request rejected or redirected', 409)
        with self.failing_save(agent.REFUSAL_FILE):
            result = self.deploy()
        self.assertEqual((result['status'], result['reason']), ('held', 'refusal_latch_unsaved'))
        self.assertIsNotNone(self.intent())
        self.assertFalse((self.root / 'auto-deploy' / agent.REFUSAL_FILE).exists())
        # Next timer pass: only the read-only lookup; the request is never re-sent.
        for _ in range(2):
            self.assertEqual(self.deploy()['reason'], 'schedule_unresolved')
        self.schedule.assert_called_once()
        self.assertEqual(self.lookup.call_count, 2)
        self.run.assert_not_called()

    def test_interruption_after_durable_latch_leaves_both_files_and_never_reposts(self):
        # refused.json is durable, then the pass stops before removing the intent.
        self.schedule.side_effect = HTTPRefused('HTTP request rejected or redirected', 409)
        with self.failing_save('state.json'):
            self.assertEqual(self.deploy()['reason'], 'refusal_latch_unsaved')
        self.assertTrue((self.root / 'auto-deploy' / agent.REFUSAL_FILE).exists())
        self.assertIsNotNone(self.intent())
        self.assertEqual(self.deploy(), {'status': 'held', 'reason': 'refusal_latched'})
        self.schedule.assert_called_once()
        self.lookup.assert_not_called()

    def test_definite_lookup_refusal_latches(self):
        self.schedule.side_effect = HTTPUncertain('HTTP response uncertain; mutation must not be retried')
        self.deploy()
        self.lookup.side_effect = HTTPRefused('HTTP request rejected or redirected', 403)
        self.assertEqual(self.deploy()['status'], 'refused')
        self.assertEqual(self.deploy(), {'status': 'held', 'reason': 'refusal_latched'})
        self.lookup.assert_called_once()
        self.schedule.assert_called_once()

    def test_receipt_for_another_target_or_request_is_refused(self):
        for change in ({'targetImage': 'sha256:' + 'f' * 64}, {'requestId': str(uuid.uuid4())},
                       {'planId': 'not-a-plan'}, {'nodeId': 'cluster'}, {'authorityId': str(uuid.UUID(int=8))}):
            with self.subTest(change=change):
                self.tearDown()
                self.setUp()
                self.schedule.side_effect = lambda request, change=change: {**self.scheduled(request), **change}
                self.assertEqual(self.deploy()['status'], 'refused')
                self.assertIsNone(self.state().get('inProgress'))
                self.assertTrue((self.root / 'auto-deploy' / agent.REFUSAL_FILE).exists())
                self.assertEqual(self.deploy()['reason'], 'refusal_latched')
                self.schedule.assert_called_once()
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


class Coordinator(ThreadingHTTPServer):
    """A loopback stand-in for the coordinator's node and owned-schedule routes."""
    daemon_threads = True

    def __init__(self):
        super().__init__(('127.0.0.1', 0), CoordinatorHandler)
        self.node = {'state': 'active', 'revision': 4, 'planId': None, 'targetImage': None,
                     'notBefore': '2020-01-01T00:00:00Z'}
        self.mode = 'conflict'
        self.authority = AUTHORITY
        self.lookup_answer = 'typed'
        self.on_lookup = None
        self.posts, self.lookups, self.owned = [], [], {}


class CoordinatorHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def reply(self, status, value):
        body = json.dumps(value).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        server = self.server
        if self.path == '/v1/prototype/authority':
            return self.reply(200, {'authorityId': server.authority})
        if self.path == '/v1/prototype/nodes/main':
            return self.reply(200, {'node': server.node})
        prefix = '/v1/prototype/nodes/main/schedule-owned/'
        if self.path.startswith(prefix):
            request_id = self.path[len(prefix):]
            server.lookups.append(request_id)
            if server.on_lookup:
                server.on_lookup()
            if request_id in server.owned:
                if server.owned[request_id]['authorityId'] != server.authority:
                    return self.reply(409, {'error': 'conflict'})
                return self.reply(200, {'receipt': server.owned[request_id]})
            # The real handler's three 404 shapes; only the first proves absence.
            code = {'typed': 'owned_schedule_not_found', 'generic': 'not_found',
                    'disabled': 'prototype_disabled'}[server.lookup_answer]
            return self.reply(404, {'error': code})
        self.reply(404, {'error': 'not_found'})

    def do_POST(self):
        server = self.server
        request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        server.posts.append(request)
        if server.mode == 'conflict':
            return self.reply(409, {'error': 'conflict', 'detail': 'node already has maintenance'})
        plan = str(uuid.uuid4())
        server.node.update(state='draining', planId=plan, targetImage=request['targetImage'],
                           revision=server.node['revision'] + 1)
        server.owned[request['requestId']] = {
            'schemaVersion': 1, 'requestId': request['requestId'], 'authorityId': request['authorityId'],
            'nodeId': 'main', 'planId': plan, 'expectedRevision': request['expectedRevision'],
            'targetImage': request['targetImage'], 'revision': server.node['revision'], 'state': 'draining'}
        if server.mode == 'drop':
            # Created, but the answer is lost: the client sees no response.
            self.close_connection = True
            self.wfile.flush()
            self.connection.shutdown(socket.SHUT_RDWR)
            return
        self.reply(200, {'receipt': server.owned[request['requestId']]})


class RealClientTimerTests(unittest.TestCase):
    """Two timer passes against the real HTTP client's error classification."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        save(self.root / 'compose.json', {'services': {'node': {'image': OLD}}})
        self.server = Coordinator()
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        origin = 'http://127.0.0.1:' + str(self.server.server_address[1])
        self.api, self.operator = Client(origin, 'worker-token'), Client(origin, 'operator-token')
        self.settings = {'nodeId': 'main', 'project': 'owned', 'service': 'node',
                         'autoDeploy': {'package': 'coordinator'}}
        self.images = {OLD: image(OLD, 100), REPO + ':verified': image(NEW, 200, digests=[REPO + '@' + DIGEST]),
                       REPO + '@' + DIGEST: image(NEW, 200, digests=[REPO + '@' + DIGEST])}
        self.run = MagicMock(return_value={'status': 'waiting', 'nodeState': 'draining'})

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.temp.cleanup()

    def timer_pass(self):
        from ops import node_maintenance

        @contextmanager
        def acquired(directory):
            yield True
        ingress = MagicMock()
        ingress.config = {'engineId': 'engine'}
        with patch.object(agent, 'docker', return_value=''), \
                patch.object(agent, 'inspect', side_effect=lambda ref: json.loads(json.dumps(self.images[ref]))), \
                patch.object(agent, 'agent_lock', acquired), \
                patch('ops.node_maintenance.Ingress', return_value=ingress), \
                patch('ops.node_maintenance.ComposeNode'):
            return agent.auto_deploy(self.root, self.settings, self.api,
                schedule=lambda request: node_maintenance.schedule_owned(self.root, self.settings, self.operator, request),
                lookup=lambda request: node_maintenance.owned_receipt(self.settings, self.api, self.operator, request),
                run=self.run, wait_seconds=0, drain_seconds=60, now=NOW)

    def test_real_409_latches_and_the_next_timer_pass_sends_nothing(self):
        first = self.timer_pass()
        self.assertEqual((first['status'], first['httpStatus']), ('refused', 409))
        second = self.timer_pass()
        self.assertEqual(second, {'status': 'held', 'reason': 'refusal_latched'})
        self.assertEqual(len(self.server.posts), 1)
        self.assertEqual(self.server.lookups, [])
        self.run.assert_not_called()

    def test_real_lost_response_is_bound_by_lookup_on_the_next_timer_pass(self):
        self.server.mode = 'drop'
        first = self.timer_pass()
        self.assertEqual(first['reason'], 'schedule_response_uncertain')
        second = self.timer_pass()
        self.assertEqual(second['status'], 'waiting')
        self.assertEqual(len(self.server.posts), 1)
        self.assertEqual(self.server.lookups, [self.server.posts[0]['requestId']])
        plan = self.server.node['planId']
        self.assertEqual(self.run.call_args.args[3:], (plan, NEW))

    def lost_then_absent(self):
        # First pass: the POST is lost. The coordinator then holds no row for it.
        self.server.mode = 'drop'
        self.timer_pass()
        self.server.owned.clear()
        self.server.node.update(state='active', planId=None, targetImage=None)
        return self.root / 'auto-deploy' / agent.INTENT_FILE

    def assert_held_with_intent(self, state_path, saved):
        result = self.timer_pass()
        self.assertEqual((result['status'], result['reason']), ('held', 'ownership_unproven'))
        self.assertEqual(json.loads(state_path.read_text())['request'], saved)
        self.assertFalse((self.root / 'auto-deploy' / agent.REFUSAL_FILE).exists())
        self.assertEqual(len(self.server.posts), 1)
        self.run.assert_not_called()

    def test_typed_absence_at_the_same_authority_keeps_the_intent(self):
        intent_path = self.lost_then_absent()
        saved = json.loads(intent_path.read_text())['request']
        for _ in range(2):
            result = self.timer_pass()
            self.assertEqual((result['status'], result['reason']), ('held', 'schedule_unresolved'))
            self.assertEqual(json.loads(intent_path.read_text())['request'], saved)
        self.assertEqual(len(self.server.posts), 1)
        self.assertEqual(self.server.lookups, [saved['requestId']] * 2)
        self.run.assert_not_called()

    def test_real_commit_landing_after_the_first_lookup_is_bound_on_the_next_pass(self):
        intent_path = self.lost_then_absent()
        saved = json.loads(intent_path.read_text())['request']
        self.assertEqual(self.timer_pass()['reason'], 'schedule_unresolved')
        # The original timed-out POST commits late on the coordinator.
        plan = str(uuid.uuid4())
        self.server.node.update(state='draining', planId=plan, targetImage=saved['targetImage'],
                                revision=saved['expectedRevision'] + 1)
        self.server.owned[saved['requestId']] = {
            'schemaVersion': 1, 'requestId': saved['requestId'], 'authorityId': saved['authorityId'],
            'nodeId': 'main', 'planId': plan, 'expectedRevision': saved['expectedRevision'],
            'targetImage': saved['targetImage'], 'revision': saved['expectedRevision'] + 1, 'state': 'draining'}
        result = self.timer_pass()
        self.assertEqual(result['status'], 'waiting')
        self.assertNotEqual(result.get('reason'), 'other_plan')
        self.assertEqual(self.run.call_args.args[3:], (plan, NEW))
        self.assertFalse(intent_path.exists())
        self.assertEqual(len(self.server.posts), 1)

    def test_generic_404_holds_and_keeps_the_intent(self):
        state_path = self.lost_then_absent()
        saved = json.loads(state_path.read_text())['request']
        self.server.lookup_answer = 'generic'
        for _ in range(2):
            self.assert_held_with_intent(state_path, saved)

    def test_disabled_prototype_404_holds_and_keeps_the_intent(self):
        state_path = self.lost_then_absent()
        saved = json.loads(state_path.read_text())['request']
        self.server.lookup_answer = 'disabled'
        self.assert_held_with_intent(state_path, saved)

    def test_changed_authority_with_no_row_holds_even_with_typed_absence(self):
        state_path = self.lost_then_absent()
        saved = json.loads(state_path.read_text())['request']
        self.server.authority = str(uuid.UUID(int=9))
        for _ in range(2):
            self.assert_held_with_intent(state_path, saved)
        # The absence answer was never even asked for under the new authority.
        self.assertEqual(self.server.lookups, [])

    def test_authority_changing_during_the_lookup_holds(self):
        state_path = self.lost_then_absent()
        saved = json.loads(state_path.read_text())['request']
        self.server.on_lookup = lambda: setattr(self.server, 'authority', str(uuid.UUID(int=9)))
        self.assert_held_with_intent(state_path, saved)
        self.assertEqual(len(self.server.lookups), 1)

    def test_real_409_with_failed_latch_write_never_posts_again(self):
        real = agent.save
        def save_or_fail(path, value):
            if path.name == agent.REFUSAL_FILE:
                raise OSError('synthetic write failure')
            return real(path, value)
        with patch.object(agent, 'save', side_effect=save_or_fail):
            first = self.timer_pass()
        self.assertEqual((first['status'], first['reason']), ('held', 'refusal_latch_unsaved'))
        intent_path = self.root / 'auto-deploy' / agent.INTENT_FILE
        self.assertTrue(intent_path.exists())
        second = self.timer_pass()
        self.assertEqual((second['status'], second['reason']), ('held', 'schedule_unresolved'))
        self.assertEqual(len(self.server.posts), 1)
        self.assertEqual(self.server.lookups, [self.server.posts[0]['requestId']])
        self.run.assert_not_called()

    def test_changed_authority_with_a_row_holds(self):
        self.server.mode = 'drop'
        self.timer_pass()
        state_path = self.root / 'auto-deploy' / agent.INTENT_FILE
        saved = json.loads(state_path.read_text())['request']
        # The row exists but now belongs to another authority: 409 on lookup.
        self.server.owned[saved['requestId']]['authorityId'] = str(uuid.UUID(int=9))
        self.assert_held_with_intent(state_path, saved)


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
                                'authorityId': payload['authorityId'],
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
                lookup=lambda request: None,
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
