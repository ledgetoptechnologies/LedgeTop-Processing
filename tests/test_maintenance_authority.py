"""Cross-process checks for the cooperative per-engine maintenance lease."""
from contextlib import redirect_stdout
import io
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch, MagicMock

from ops import node_ingress, node_maintenance, update_maintenance_adapter
from ops import update_service
from processing import maintenance_authority as authority


@unittest.skipIf(os.name == 'nt', 'the installed maintenance host is Linux')
class MaintenanceAuthorityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / 'lease'
        self.root.mkdir(mode=0o700)
        self.path_patch = patch.object(authority, 'authority_root', return_value=self.root)
        self.path_patch.start()
        self.addCleanup(self.path_patch.stop)

    def child(self, action):
        code = '''
import sys
from pathlib import Path
from processing import maintenance_authority as a
from ops import node_maintenance as m
a.authority_root = lambda _: Path(sys.argv[1])
m.local_engine_id = lambda: 'test-engine'
try:
    if sys.argv[2] == 'run':
        m.run(Path(sys.argv[1]) / 'nonexistent-managed-root', None, None, None, 0)
    else:
        with a.maintenance_authority('test-engine'):
            print('acquired')
except a.MaintenanceAuthorityBusy:
    print('busy-before-config-read')
'''
        return subprocess.run([sys.executable, '-c', code, str(self.root), action],
                              capture_output=True, text=True, timeout=10, check=True).stdout.strip()

    def other_thread(self, function):
        outcome = []
        def invoke():
            try: outcome.append(('result', function()))
            except BaseException as error: outcome.append(('error', error))
        thread = threading.Thread(target=invoke)
        thread.start();thread.join(timeout=10)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(outcome), 1)
        return outcome[0]

    def test_separate_process_cannot_read_managed_config_while_lease_held(self):
        with authority.maintenance_authority('test-engine'):
            self.assertEqual(self.child('run'), 'busy-before-config-read')
            self.assertEqual(self.child('lease'), 'busy-before-config-read')
        self.assertEqual(self.child('lease'), 'acquired')

    def test_reentrant_same_thread_keeps_other_process_excluded(self):
        with authority.maintenance_authority('test-engine') as outer:
            with authority.maintenance_authority('test-engine') as inner:
                self.assertIs(outer, inner)
                self.assertEqual(self.child('lease'), 'busy-before-config-read')
            self.assertEqual(self.child('lease'), 'busy-before-config-read')

    def test_replaced_lock_path_is_rejected_after_flock(self):
        import fcntl
        original = fcntl.flock
        lockfile = self.root / 'maintenance-authority.lock'
        def swap(fd, action):
            if action == fcntl.LOCK_EX | fcntl.LOCK_NB:
                lockfile.unlink()
                lockfile.touch(mode=0o600)
            return original(fd, action)
        with patch.object(fcntl, 'flock', side_effect=swap):
            with self.assertRaises(authority.MaintenanceAuthorityBusy):
                with authority.maintenance_authority('test-engine'):
                    self.fail('replaced lock must be rejected before use')

    def test_replaced_ancestor_or_lock_detected_on_scope_exit(self):
        with self.assertRaises(authority.MaintenanceAuthorityBusy):
            with authority.maintenance_authority('test-engine'):
                self.root.rename(self.root.with_name('old-lease'))
                self.root.mkdir(mode=0o700)

    def test_compose_uses_lease_before_reading_control_clients(self):
        from types import SimpleNamespace
        from contextlib import ExitStack
        raw={'codeBindingSha256':'test', 'maintenance':{'managedRoot':'/never-read','statePath':'/a','receiptRoot':'/b'},
             'registry':{'statePath':'/c'},'qualification':{'statePath':'/d'},
             'supervisor':{'statePath':'/e'},'service':{'statePath':'/f'}}
        cfg = SimpleNamespace(raw=raw, registry_policy=SimpleNamespace(engine_id='test-engine'),
            supervisor_policy=SimpleNamespace(qualification_policy_sha256='policy'),
            capacity_policy=None,startup_policy=None,window=None)
        # Prove contention prevents credential reads before reaching any managed
        # config. Other component setup is replaced with inert fixtures.
        with ExitStack() as stack:
            stack.enter_context(authority.maintenance_authority('test-engine'))
            clients=stack.enter_context(patch.object(update_service, 'control_clients'))
            stack.enter_context(patch.object(update_service,'private_path'))
            stack.enter_context(patch.object(update_service,'code_binding_sha256',return_value='test'))
            stack.enter_context(patch('processing.update_capacity.ImageStagingCapacity'))
            stack.enter_context(patch('processing.update_registry.DockerRegistry'))
            stack.enter_context(patch('processing.update_qualifier.StartupQualifier',
                                      return_value=SimpleNamespace(policy_sha256='policy')))
            kind,error=self.other_thread(lambda:update_service.compose(cfg))
            self.assertEqual(kind, 'error')
            self.assertIsInstance(error, authority.MaintenanceAuthorityBusy)
            clients.assert_not_called()

    def test_direct_run_reads_current_config_after_acquiring_lease(self):
        config = self.root / 'gateway.json'
        config.write_text('new', encoding='utf-8')
        def inspect(*args, **kwargs):
            self.assertEqual(config.read_text(encoding='utf-8'), 'new')
            self.assertEqual(self.child('lease'), 'busy-before-config-read')
            return 'checked'
        with patch.object(node_maintenance, 'local_engine_id', return_value='test-engine'), \
             patch.object(node_maintenance, '_run_locked', side_effect=inspect):
            self.assertEqual(node_maintenance.run(self.root, None, None, None, 0), 'checked')

    def test_adapter_direct_operations_use_same_lease(self):
        adapter = update_maintenance_adapter.UpdateMaintenanceAdapter.__new__(update_maintenance_adapter.UpdateMaintenanceAdapter)
        adapter.binding = {'engineId': 'test-engine'}
        adapter._check = lambda: None
        adapter._mutex = threading.RLock()
        adapter._busy = False
        with authority.maintenance_authority('test-engine'):
            def invoke():
                with adapter._operation(): self.fail('must not enter while lease is held')
            kind, error = self.other_thread(invoke)
            self.assertEqual(kind, 'error')
            self.assertIsInstance(error, update_maintenance_adapter.MaintenanceAdapterHeld)

    def test_adapter_construction_is_fenced_before_opening_managed_root(self):
        with authority.maintenance_authority('test-engine'):
            def invoke():
                update_maintenance_adapter.UpdateMaintenanceAdapter(
                    self.root / 'missing', state_path=self.root / 'missing.db', receipt_root=self.root / 'receipts',
                    api=None, operator=None, node_id='cluster', authority_id='invalid', engine_id='test-engine',
                    adapter_id='test', window=None)
            kind, error = self.other_thread(invoke)
            self.assertEqual(kind, 'error')
            self.assertIsInstance(error, update_maintenance_adapter.MaintenanceAdapterHeld)
            self.assertEqual(str(error), 'maintenance_authority_unavailable')

    def test_ingress_cli_cannot_construct_stale_gateway_under_contention(self):
        args = ['node_ingress', '--root', str(self.root), 'close']
        with authority.maintenance_authority('test-engine'), \
             patch.object(node_ingress, 'local_engine_id', return_value='test-engine'), \
             patch.object(node_ingress, 'Ingress') as ingress, patch.object(sys, 'argv', args), \
             redirect_stdout(io.StringIO()):
            self.assertEqual(self.other_thread(node_ingress.main), ('result', 2))
            ingress.assert_not_called()

    def test_readiness_does_not_read_gateway_or_call_api_under_contention(self):
        adapter=MagicMock()
        adapter._operation.side_effect=lambda:authority.maintenance_authority('test-engine')
        api=MagicMock()
        idle,_=update_service.readiness_probes(MagicMock(),adapter,api)
        with authority.maintenance_authority('test-engine'):
            kind,error=self.other_thread(lambda:idle({}))
        self.assertEqual(kind,'error')
        self.assertIsInstance(error,authority.MaintenanceAuthorityBusy)
        adapter._context.assert_not_called()
        api.json.assert_not_called()


if __name__ == '__main__':
    unittest.main()
