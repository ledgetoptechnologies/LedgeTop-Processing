import json
import os
from pathlib import Path
import tempfile
import unittest
from contextlib import nullcontext
from unittest.mock import MagicMock, patch

from ops.node_ingress import GateError, save
from ops.node_maintenance import run
from processing.node_gate import GateBusy, GateError as HostGateError


@unittest.skipIf(os.name == 'nt', 'Installed node automation uses Linux host locks')
class ManagedMaintenanceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        (self.root / 'ingress').mkdir()
        self.receipt = self.root / 'receipt'
        self.receipt.mkdir()
        save(self.root / 'maintenance.json', {'nodeId': 'main', 'project': 'owned', 'service': 'node'})
        self.old = 'sha256:' + 'a' * 64
        self.new = 'sha256:' + 'b' * 64
        save(self.root / 'compose.json', {'services': {'node': {'image': self.old}}})
        self.api = MagicMock(origin='http://127.0.0.1:18080')
        self.node = {'state': 'draining', 'notBefore': '2020-01-01T00:00:00Z',
                     'planId': 'original-plan', 'targetImage': self.new,'revision':1}
        self.api.json.side_effect = lambda method, path: (
            {'jobs': []} if path.endswith('/jobs') else
            {'authorityId': 'authority'} if path.endswith('/authority') else {'node': self.node})
        self.ingress = MagicMock()
        self.ingress.root=self.root/'ingress'
        self.ingress.config = {'backendPort': 13004,'backendName':'owned','engineId':'engine',
            'proxyName':'owned-proxy','proxyImage':'sha256:'+'c'*64,'listenPort':3001}
        self.ingress.docker.return_value = json.dumps([{'Id': 'exact-backend'}])
        self.ingress.inspect.return_value={'Id':'owned-proxy'}
        self.ingress.workers.return_value={2}
        self.ingress.backend.return_value={'engine':'odx','engineVersion':'pin','version':'2.4.0'}
        self.ingress.http.return_value=(200,self.ingress.backend.return_value)
        self.ingress.change.side_effect=self.change
        self.adapter = MagicMock()
        self.adapter.inspect.return_value = {'Id': 'exact-backend','Image':self.old}
        self.adapter.healthy.return_value=True
        self.adapter.serving.return_value=True
        self.gate = MagicMock()
        self.gate.snapshot.return_value = {'reservation': None, 'nodeId': 'main',
            'coordinatorOrigin': self.api.origin, 'authorityId': 'authority'}

    def tearDown(self):
        self.temp.cleanup()

    def context(self):
        from contextlib import ExitStack
        stack = ExitStack()
        stack.enter_context(patch('ops.node_maintenance.local_engine_id', return_value='engine'))
        stack.enter_context(patch('ops.node_maintenance.maintenance_authority', side_effect=lambda _: nullcontext()))
        stack.enter_context(patch('ops.node_maintenance.Ingress', return_value=self.ingress))
        stack.enter_context(patch('ops.node_maintenance.ComposeNode', return_value=self.adapter))
        gate = stack.enter_context(patch('ops.node_maintenance.NodeGate'))
        gate.return_value.__enter__.return_value = self.gate
        stack.enter_context(patch('ops.node_maintenance.deferred_receipt',return_value=dict(oldImage=self.old,reservedRevision=1,
            nodeId='main',planId='original-plan',targetImage=self.new)))
        stack.enter_context(patch('ops.node_maintenance.finalize_deferred',side_effect=lambda *args,**kwargs:
            dict(status='succeeded',nodeId='main',**kwargs['result'])))
        return stack

    def change(self,closed,**kwargs):
        state=dict(state='closed' if closed else 'open',containerId='owned-proxy',oldWorkers=[],
            expectedBackendImage=None if closed else self.new)
        save(self.ingress.root/'state.json',state)
        return dict(state=state['state'],containerId='owned-proxy',oldWorkersDrained=True)

    def executed(self,result):
        def invoke(*args,**kwargs):
            self.node.update(state='updating',revision=2)
            kwargs['on_reserved'](dict(nodeId='main',planId='original-plan',targetImage=self.new,reservedRevision=2))
            return result
        return invoke

    def test_unresolved_host_reservation_never_updates_or_reopens(self):
        self.gate.snapshot.return_value['reservation'] = {'receiptId': 'unresolved'}
        with self.context(), patch('ops.node_maintenance.execute') as execute:
            with self.assertRaises(GateError):
                run(self.root, self.api, MagicMock(), self.receipt, 0)
            execute.assert_not_called()
        self.ingress.change.assert_not_called()
        self.assertEqual(json.loads((self.root / 'compose.json').read_text())['services']['node']['image'], self.old)

    def test_mismatched_backend_never_closes_another_ingress(self):
        self.adapter.inspect.return_value['Id'] = 'another-backend'
        with self.context():
            with self.assertRaises(GateError):
                run(self.root, self.api, MagicMock(), self.receipt, 0)
        self.ingress.change.assert_not_called()

    def test_failed_update_keeps_ingress_closed_and_old_persistent_pin(self):
        with self.context(), patch('ops.node_maintenance.execute', side_effect=self.executed({'status': 'failed'})):
            self.assertEqual(run(self.root, self.api, MagicMock(), self.receipt, 0)['ingress'], 'closed')
        self.ingress.change.assert_called_once_with(True, seconds=60)
        self.assertEqual(json.loads((self.root / 'compose.json').read_text())['services']['node']['image'], self.old)

    def test_persistent_pin_is_saved_before_reopening_and_receipt_prevents_replay(self):
        def change(closed, **kwargs):
            if not closed:
                self.assertEqual(json.loads((self.root / 'compose.json').read_text())['services']['node']['image'], self.new)
            return self.change(closed,**kwargs)
        self.ingress.change.side_effect = change
        with self.context(), patch('ops.node_maintenance.execute', side_effect=self.executed({'status': 'succeeded', 'image': self.new})) as execute:
            self.assertEqual(run(self.root, self.api, MagicMock(), self.receipt, 0)['ingress'], 'open')
            with self.assertRaises(GateError):
                run(self.root, self.api, MagicMock(), self.receipt, 0)
            execute.assert_called_once()

    def test_busy_worker_and_lock_are_waited_for_before_same_planned_update(self):
        polls = []
        def api(method, path):
            if path.endswith('/jobs'):
                self.ingress.change.assert_not_called()
                polls.append(1)
                return {'jobs': [{'nodeId': 'main', 'status': 'running'}] if len(polls) == 1 else []}
            return {'authorityId': 'authority'} if path.endswith('/authority') else {'node': self.node}
        self.api.json.side_effect = api
        with self.context(), patch('ops.node_maintenance.NodeGate') as host, \
                patch('ops.node_maintenance.time.sleep') as sleep, \
                patch('ops.node_maintenance.execute', side_effect=self.executed({'status': 'succeeded', 'image': self.new})) as execute:
            host.return_value.__enter__.side_effect = [GateBusy('busy'), self.gate]
            self.assertEqual(run(self.root, self.api, MagicMock(), self.receipt, 60)['status'], 'succeeded')
            self.assertEqual(sleep.call_count, 2)
            self.assertEqual(execute.call_args.kwargs['expected_plan_id'], 'original-plan')

    def test_held_claim_timeout_never_starts_replacement(self):
        original = self.api.json.side_effect
        self.api.json.side_effect = lambda method, path: (
            {'jobs': [{'nodeId': 'main', 'status': 'held'}]} if path.endswith('/jobs') else original(method, path))
        with self.context(), patch('ops.node_maintenance.time.monotonic', side_effect=[0, 2]), \
                patch('ops.node_maintenance.execute') as execute, patch('ops.node_maintenance.NodeGate') as host:
            with self.assertRaises(GateError):
                run(self.root, self.api, MagicMock(), self.receipt, 1)
            execute.assert_not_called()
            host.assert_not_called()
        self.ingress.change.assert_not_called()

    def test_non_contention_gate_error_is_not_retried(self):
        with self.context(), patch('ops.node_maintenance.NodeGate') as host, \
                patch('ops.node_maintenance.time.sleep') as sleep:
            host.return_value.__enter__.side_effect = HostGateError('invalid binding/filesystem')
            with self.assertRaises(HostGateError):
                run(self.root, self.api, MagicMock(), self.receipt, 60)
            sleep.assert_not_called()


if __name__ == '__main__':
    unittest.main()
