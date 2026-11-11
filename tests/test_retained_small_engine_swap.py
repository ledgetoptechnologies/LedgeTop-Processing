import copy
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
import uuid

from ops.retained_small_engine_swap import (
    BASE, TARGET, FLAG, Held, Runtime, apply, candidate_config, canonical, command_with_retention, main,
    digest, no_cluster_claims, snapshot_tree, terminal_inventory, verify_owned_receipt, verify_retained,
)


class FakeRuntime:
    def __init__(self, fail=None):
        self.fail = fail
        self.calls = []
        self.closed = False
        self.current = dict(id='cluster', state='active', revision=0, planId=None,
                            targetImage=None, result=None)
        self.prepared = dict(requestId=str(uuid.uuid4()), authorityId=str(uuid.uuid4()),
                             node=copy.deepcopy(self.current), command=['--max-images', '1000', '--token', 'synthetic'],
                             engine={'tasks': {str(uuid.uuid4()): 40}})
        self.operator = SimpleNamespace(json=self.operator_json)

    def step(self, name):
        self.calls.append(name)
        if self.fail == name:
            raise Held('synthetic uncertain operation')

    def authority(self):
        return self.prepared['authorityId']

    def node(self):
        return copy.deepcopy(self.current)

    def operator_json(self, method, route, payload):
        action = route.rsplit('/', 1)[-1]
        self.step(action)
        if action == 'schedule-owned':
            self.current.update(state='draining', revision=1, planId=str(uuid.uuid4()), targetImage=TARGET)
            receipt = dict(payload, nodeId='cluster', revision=1, state='draining', planId=self.current['planId'],
                           requestSha256=digest(canonical({'nodeId': 'cluster', 'request': payload})))
            receipt['receiptSha256'] = digest(canonical(receipt))
            return {'receipt': receipt}
        if action == 'begin':
            self.current.update(state='updating', revision=2)
            return {'node': self.node(), 'updateToken': 'synthetic-token'}
        if action == 'finish':
            self.current.update(state='active', revision=3, result=payload['result'])
            return {'node': self.node()}
        raise AssertionError('unexpected API mutation')

    def revalidate(self, _):
        self.step('revalidate')

    def engine_idle(self, *_):
        self.step('engine_idle')

    def claims(self):
        return [{'nodeId': 'main', 'status': 'running'}]

    def change_ingress(self, _, *, closed):
        self.step('close' if closed else 'open')
        self.closed = closed

    def stop_and_backup(self, *_):
        self.step('stop_and_backup')

    def recreate_and_verify(self, *_):
        self.step('recreate_and_verify')

    def persist_pin(self, _):
        self.step('persist_pin')

    def hold_closed(self, *_):
        self.calls.append('hold_closed')
        self.closed = True
        if self.current['state'] == 'updating':
            self.current['state'] = 'held'


class RetainedSwapTests(unittest.TestCase):
    def test_command_and_all_other_configuration_are_preserved(self):
        command = ['--max-images', '1000', '--token', 'synthetic-private']
        current = {'services': {'nodeodm': {'image': BASE, 'command': command,
                    'volumes': [{'source': 'owned-volume', 'target': '/var/www/data'}],
                    'networks': ['existing'], 'restart': 'unless-stopped', 'mem_limit': 123}},
                   'networks': {'existing': {'external': True}}}
        before = copy.deepcopy(current)
        candidate = candidate_config(current, command)
        self.assertEqual(candidate['services']['nodeodm']['image'], TARGET)
        self.assertEqual(candidate['services']['nodeodm']['command'], command + [FLAG])
        candidate['services']['nodeodm']['image'] = BASE
        candidate['services']['nodeodm']['command'] = command
        self.assertEqual(candidate, before)
        self.assertEqual(current, before)
        for bad in (command + [FLAG], ['--max-images', '1001', '--token', 'x'],
                    ['--max-images', '1000', '--token', ''], 'shell token'):
            with self.subTest(bad=bad), self.assertRaises(Held):
                command_with_retention(bad)

    def test_retained_terminal_tasks_allowed_but_active_unknown_or_duplicate_rejected(self):
        ids = [str(uuid.uuid4()) for _ in range(3)]
        codes = dict(zip(ids, (30, 40, 50)))
        records = [{'uuid': identifier} for identifier in ids]
        self.assertEqual(terminal_inventory(records, lambda i: {'status': {'code': codes[i]}}), codes)
        for code in (None, 0, 10, 20, 99, '40'):
            with self.subTest(code=code), self.assertRaises(Held):
                terminal_inventory(records[:1], lambda i: {'status': {'code': code}})
        with self.assertRaises(Held):
            terminal_inventory(records + records[:1], lambda i: {'status': {'code': codes[i]}})

    def test_main_claim_does_not_block_small_swap_and_small_claims_do(self):
        no_cluster_claims([{'nodeId': 'main', 'status': 'running'}, {'nodeId': None, 'status': 'queued'}])
        for state in ('queued', 'running', 'held'):
            with self.subTest(state=state), self.assertRaises(Held):
                no_cluster_claims([{'nodeId': 'cluster', 'status': state}])

    def test_success_orders_backup_before_recreate_and_finish_after_verified_open(self):
        runtime, checkpoints = FakeRuntime(), []
        with tempfile.TemporaryDirectory() as temporary:
            result = apply(runtime, Path(temporary), runtime.prepared,
                           lambda state: checkpoints.append(copy.deepcopy(state)))
        self.assertEqual(result['status'], 'succeeded')
        ordered = ['schedule-owned', 'begin', 'close', 'stop_and_backup',
                   'recreate_and_verify', 'persist_pin', 'open', 'finish']
        self.assertEqual([x for x in runtime.calls if x in ordered], ordered)
        self.assertEqual(checkpoints[-1]['stage'], 'completed')
        self.assertFalse(runtime.closed)

    def test_each_mutation_failure_stays_closed_never_boots_old_or_replays(self):
        for stage in ('schedule-owned', 'begin', 'close', 'stop_and_backup',
                      'recreate_and_verify', 'persist_pin', 'open', 'finish'):
            runtime, checkpoints = FakeRuntime(fail=stage), []
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as temporary:
                with self.assertRaises(Held):
                    apply(runtime, Path(temporary), runtime.prepared,
                          lambda state: checkpoints.append(copy.deepcopy(state)))
                self.assertTrue(runtime.closed)
                self.assertEqual(runtime.calls.count(stage), 1)
                self.assertNotIn('rollback', runtime.calls)
                self.assertNotIn('start_old', runtime.calls)
                self.assertEqual(checkpoints[-1]['stage'], 'held_for_inspection')
                if stage in ('close', 'stop_and_backup'):
                    self.assertNotIn('recreate_and_verify', runtime.calls)

    def test_existing_operation_and_changed_authority_never_schedule(self):
        runtime = FakeRuntime()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'operation.json').write_text('{}')
            with self.assertRaises(Held):
                apply(runtime, root, runtime.prepared, lambda _: None)
        self.assertEqual(runtime.calls, [])
        runtime.prepared['authorityId'] = 'different'
        runtime.authority = lambda: 'actual-authority'
        with tempfile.TemporaryDirectory() as temporary, self.assertRaises(Held):
            apply(runtime, Path(temporary), runtime.prepared, lambda _: None)
        self.assertEqual(runtime.calls, [])

    def test_owned_schedule_digest_mismatch_cannot_be_adopted(self):
        runtime = FakeRuntime()
        request = dict(schemaVersion=1, requestId=runtime.prepared['requestId'],
                       authorityId=runtime.authority(), expectedRevision=0,
                       drainAt='2026-11-11T00:00:00Z', notBefore='2026-11-11T00:00:00Z',
                       afterJobId=None, targetImage=TARGET)
        receipt = runtime.operator_json('POST', '/schedule-owned', request)['receipt']
        verify_owned_receipt(receipt, request)
        receipt['targetImage'] = BASE
        with self.assertRaises(Held):
            verify_owned_receipt(receipt, request)

    def test_retained_file_inventory_detects_changes_and_permits_index_reserialization(self):
        import time
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'source.jpg').write_bytes(b'synthetic retained original')
            (root / 'output.zip').write_bytes(b'synthetic retained result')
            (root / 'tasks.json').write_text('[{}]')
            before = snapshot_tree(root, deadline=time.monotonic() + 5)
            (root / 'tasks.json').write_text('[ { } ]')
            verify_retained(before, snapshot_tree(root, deadline=time.monotonic() + 5))
            (root / 'source.jpg').write_bytes(b'changed')
            with self.assertRaises(Held):
                verify_retained(before, snapshot_tree(root, deadline=time.monotonic() + 5))

    def test_failed_unowned_schedule_never_closes_somebody_elses_ingress(self):
        runtime = Runtime.__new__(Runtime)
        runtime.operator = Mock()
        runtime.operator.json.side_effect = Held('no owned historical schedule')
        runtime.change_ingress = Mock()
        outcome = runtime.hold_closed({'requestId': str(uuid.uuid4())}, {})
        self.assertFalse(outcome['ingressClosedVerified'])
        runtime.change_ingress.assert_not_called()

    def test_default_cli_inspects_only_and_does_not_print_private_configuration(self):
        prepared = FakeRuntime().prepared
        prepared['command'][-1] = 'private-not-for-output'
        runtime = Mock()
        runtime.snapshot.return_value = prepared
        with patch('sys.argv', ['swap', '--credentials', 'synthetic-private.env']), \
                patch('ops.retained_small_engine_swap.Runtime', return_value=runtime), \
                redirect_stdout(io.StringIO()) as output:
            self.assertEqual(main(), 0)
        report = json.loads(output.getvalue())
        self.assertEqual(report['mode'], 'inspect')
        self.assertFalse(report['writes'])
        self.assertNotIn('private-not-for-output', output.getvalue())
        self.assertEqual(runtime.method_calls, [('snapshot', (), {})])


if __name__ == '__main__':
    unittest.main()
