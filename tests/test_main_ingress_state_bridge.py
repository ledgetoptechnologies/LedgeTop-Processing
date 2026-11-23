"""Synthetic files and mocked ingress; no Docker or coordinator interaction."""
import json
from contextlib import nullcontext
import os
from pathlib import Path
import stat
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
import uuid

from ops import retained_main_engine_swap as swap
from ops.node_ingress import Ingress, configuration
from ops.combined_main_retention import safe_failure
from ops import combined_main_retention as combined
from processing.intermediate_mounts import MountTopologyHeld
from processing.intermediate_cleanup import CleanupHeld


@unittest.skipUnless(os.name == 'posix' and getattr(os, 'getuid', lambda: -1)() == 1000,
                     'requires actual Linux uid1000 canonical-file metadata')
class CanonicalIngressBridgeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        (self.root / 'public-config').mkdir()
        self.nginx = self.root / 'public-config/nginx.conf'
        self.state = self.root / 'state.json'
        self.nginx.write_text(configuration(3004, 13004, False)); self.nginx.chmod(0o644)
        self.initial_state = dict(state='open', containerId=swap.INGRESS, oldWorkers=[90], expectedBackendImage=swap.BASE)
        self.state.write_text(json.dumps(self.initial_state)); self.state.chmod(0o600)
        gateway = dict(engineId='fixture-daemon', proxyName='fixture-proxy', proxyImage=swap.INGRESS_IMAGE,
                       backendName='fixture-node', listenPort=3004, backendPort=13004)
        (self.root / 'gateway.json').write_text(json.dumps(gateway)); (self.root / 'gateway.json').chmod(0o600)
        (self.root / 'ingress.lock').touch(mode=0o600)
        self.runtime = object.__new__(swap.Runtime)
        self.runtime.ingress_inspect = Mock(return_value=dict(Id=swap.INGRESS, Name='/fixture-proxy', State=dict(Pid=100)))
        self.runtime.nginx_workers = Mock(side_effect=[(100, {101}), (100, {101}), (100, {102})])
        self.runtime.docker = Mock(return_value='')
        self.prepared = dict(requestId=str(uuid.uuid4()), dockerEngineId='fixture-daemon',
                             oldContainer=dict(Name='/fixture-node'), nginxHex=self.nginx.read_bytes().hex(),
                             ingressLockProfile=swap.ingress_lock_profile(self.root))
        self.patch = patch.object(swap, 'NGINX', self.nginx); self.patch.start()

    def tearDown(self):
        self.patch.stop(); self.temp.cleanup()

    def metadata(self, path):
        info = path.stat()
        return info.st_uid, info.st_gid, stat.S_IMODE(info.st_mode)

    def test_close_attests_canonical_state_and_bytes_after_workers_drained(self):
        metadata = [self.metadata(p) for p in (self.nginx, self.state)]
        with patch.object(Ingress, 'http', return_value=(503, None)):
            self.runtime.change_ingress(self.prepared, closed=True)
        state = json.loads(self.state.read_bytes())
        self.assertEqual(state['state'], 'closed')
        self.assertEqual(state['oldWorkers'], [101])
        self.assertEqual(state['retentionOperationId'], self.prepared['requestId'])
        self.assertEqual(state['retentionGatewayMasterPid'], 100)
        self.assertEqual(self.nginx.read_bytes(), configuration(3004, 13004, True).encode())
        self.assertNotEqual(self.nginx.read_bytes(), bytes.fromhex(self.prepared['nginxHex']).replace(swap.PASS, b'return 503;'))
        self.assertEqual(metadata, [self.metadata(p) for p in (self.nginx, self.state)])
        self.runtime.docker.assert_any_call('kill', '--signal', 'HUP', swap.INGRESS)
        self.assertEqual(sum(c.args[:3] == ('kill', '--signal', 'HUP') for c in self.runtime.docker.call_args_list), 1)

    def test_supported_mode664_lock_is_sealed_and_preserved_inside_private700(self):
        lock = self.root / 'ingress.lock'
        lock.chmod(0o664)
        self.prepared['ingressLockProfile'] = swap.ingress_lock_profile(self.root)
        with patch.object(Ingress, 'http', return_value=(503, None)):
            self.runtime.change_ingress(self.prepared, closed=True)
        self.assertEqual(stat.S_IMODE(lock.stat().st_mode), 0o664)
        self.assertEqual(swap.ingress_lock_profile(self.root), self.prepared['ingressLockProfile'])

    def test_lock_mode_drift_or_unsafe_parent_is_not_adopted(self):
        lock = self.root / 'ingress.lock'
        lock.chmod(0o664)
        with self.assertRaisesRegex(swap.Held, 'prepared_ingress_lock_profile_changed'):
            self.runtime.change_ingress(self.prepared, closed=True)
        lock.chmod(0o666)
        with self.assertRaisesRegex(swap.Held, 'canonical_ingress_lock_profile_changed'):
            swap.ingress_lock_profile(self.root)
        lock.chmod(0o664); self.root.chmod(0o750)
        with self.assertRaisesRegex(swap.Held, 'canonical_ingress_root_not_private'):
            swap.ingress_lock_profile(self.root)
        self.runtime.docker.assert_not_called()

    def test_open_requires_owned_closed_then_attests_actual_supported_backend(self):
        with patch.object(Ingress, 'http', return_value=(503, None)):
            self.runtime.change_ingress(self.prepared, closed=True)
        self.runtime.nginx_workers = Mock(side_effect=[(100, {102}), (100, {102}), (100, {103})])
        self.runtime.docker.reset_mock()
        info = dict(engine='odx', engineVersion='fixture', version='fixture')
        with patch.object(Ingress, 'http', return_value=(200, info)), patch.object(Ingress, 'backend', return_value=info) as backend:
            self.runtime.change_ingress(self.prepared, closed=False)
        self.assertEqual(json.loads(self.state.read_bytes())['state'], 'open')
        self.assertEqual(self.nginx.read_bytes(), bytes.fromhex(self.prepared['nginxHex']))
        self.assertEqual(backend.call_count, 2)
        backend.assert_called_with(swap.TARGET)

    def test_open_state_with_closed_config_is_not_silently_adopted(self):
        for raw in (configuration(3004, 13004, True).encode(),
                    bytes.fromhex(self.prepared['nginxHex']).replace(swap.PASS, b'return 503;')):
            self.nginx.write_bytes(raw)
            self.runtime.nginx_workers = Mock(return_value=(100, {102}))
            with self.subTest(config=raw[:20]):
                with self.assertRaisesRegex(swap.Held, 'canonical_ingress_source_mismatch'):
                    self.runtime.change_ingress(self.prepared, closed=True, recover_closed_intent=True)
                self.assertEqual(json.loads(self.state.read_bytes()), self.initial_state)
                self.assertEqual(self.nginx.read_bytes(), raw)
                self.runtime.docker.assert_not_called()

    def test_lost_hup_response_retains_intent_and_recovery_does_not_resignal(self):
        def docker(*args):
            if args[:3] == ('kill', '--signal', 'HUP'):
                raise TimeoutError('private path/token must never leave CLI')
            return ''
        self.runtime.docker.side_effect = docker
        with self.assertRaises(TimeoutError): self.runtime.change_ingress(self.prepared, closed=True)
        self.assertEqual(json.loads(self.state.read_bytes())['state'], 'closing')
        self.runtime.docker.reset_mock(); self.runtime.docker.side_effect = None
        self.runtime.nginx_workers = Mock(return_value=(100, {102}))
        with patch.object(Ingress, 'http', return_value=(503, None)):
            self.runtime.change_ingress(self.prepared, closed=True, recover_closed_intent=True)
        self.runtime.docker.assert_not_called()
        self.assertEqual(json.loads(self.state.read_bytes())['state'], 'closed')
        foreign = json.loads(self.state.read_bytes()); foreign['retentionOperationId'] = str(uuid.uuid4())
        self.state.write_text(json.dumps(foreign))
        with self.assertRaisesRegex(swap.Held, 'owned_closed_ingress_unproven_no_replay'):
            self.runtime.change_ingress(self.prepared, closed=True, recover_closed_intent=True)


class SafeFailureTests(unittest.TestCase):
    def test_consumed_phase_journal_retains_only_safe_failure_code(self):
        operation = object.__new__(combined.Operation)
        operation.root = Path('/unused-synthetic-operation')
        operation._executing = False
        operation.r = SimpleNamespace(hold_closed=Mock(return_value={'held': True}))
        operation.prepared = dict(cleanupTemplates=[])
        operation.plan_cleanup = Mock(side_effect=CleanupHeld('ingress_readers_not_drained'))
        with patch.object(swap, 'operation_lock', return_value=nullcontext()), patch.object(
                combined, 'private_json', return_value={'stage': 'stopped_tmp_preserved'}), patch.object(swap, 'save') as save:
            with self.assertRaises(CleanupHeld): operation.run_phase('plan-cleanup')
        saved = save.call_args.args[1]
        self.assertEqual(saved['stage'], 'held_for_independent_inspection')
        self.assertEqual(saved['errorClass'], 'CleanupHeld')
        self.assertEqual(saved['errorCode'], 'ingress_readers_not_drained')
        self.assertFalse(saved['automaticReplay'])

    def test_only_typed_static_underscore_codes_are_released(self):
        self.assertEqual(safe_failure(swap.Held('ingress_source_changed')),
                         dict(errorClass='Held', errorCode='ingress_source_changed'))
        self.assertEqual(safe_failure(MountTopologyHeld('relative_mount_identity'))['errorCode'], 'relative_mount_identity')
        self.assertEqual(safe_failure(CleanupHeld('ingress_readers_not_drained')),
                         dict(errorClass='CleanupHeld', errorCode='ingress_readers_not_drained'))
        for error in (swap.Held('/private/path token=do-not-echo'), RuntimeError('secret_token')):
            result = safe_failure(error)
            self.assertNotIn('secret_token', json.dumps(result))
            self.assertNotIn('/private', json.dumps(result))
