"""Synthetic remote-bootstrap path check for the guarded helper recovery CLI.

Runs the real controller, BOOTSTRAP source, CLI parser, updater method, gate
protocol, and journal against temporary private files and a test-only Docker
executable. It never connects to SSH or a Docker daemon.
"""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from ops import run_coordinator_update_bridge as controller
from ops.coordinator_gate_bridge import helper_digest
from ops.durable_helper_journal import HelperJournal
from ops import update_reference_coordinator as updater
from processing.node_gate import NodeGate


class RemoteRecoverHelperCliReview(unittest.TestCase):
    @unittest.skipUnless(os.name == 'posix', 'POSIX journal, gate and relay required')
    def test_controller_reaches_guarded_remote_cli_and_preserves_checkpoint(self):
        with tempfile.TemporaryDirectory(prefix='lt-recover-cli-') as temp:
            root = Path(temp)
            operation_root = root / 'operation'
            operation_root.mkdir(mode=0o700)
            lock_root = root / 'locks'
            lock_root.mkdir(mode=0o700)
            gate_state = root / 'gate-state'
            os.environ['XDG_STATE_HOME'] = str(gate_state)
            os.environ['LT_TEST_BOOTSTRAP'] = '1'
            os.environ['LT_TEST_ROOT'] = str(root)
            marker = root / 'helper-running'
            marker.write_text('owned fixture helper\n')
            docker_bin = root / 'bin'
            docker_bin.mkdir(mode=0o700)
            docker_script = docker_bin / 'docker'
            docker_script.write_text(DOCKER_STUB)
            docker_script.chmod(0o700)
            os.environ['LT_TEST_HELPER_MARKER'] = str(marker)
            os.environ['LT_TEST_CLUSTER_ENGINE'] = controller.ENGINES['cluster']
            os.environ['LT_TEST_MAIN_ENGINE'] = controller.ENGINES['main']
            os.environ['PATH'] = str(docker_bin) + os.pathsep + os.environ['PATH']
            shim_root = root / 'shim'
            shim_root.mkdir(mode=0o700)
            (shim_root / 'sitecustomize.py').write_text(SITECUSTOMIZE)
            child_pythonpath = os.pathsep.join((str(shim_root), str(ROOT)))

            config = test_configuration()
            main_gate = NodeGate(controller.ENGINES['main'])
            with main_gate:
                main_gate.provision('main', 'http://127.0.0.1:18080', controller.AUTHORITY)
            cluster_gate = NodeGate(controller.ENGINES['cluster'])
            with cluster_gate:
                cluster_gate.provision('cluster', 'http://127.0.0.1:18080', controller.AUTHORITY)

            gate_config = root / 'bridge.json'
            gate_config.write_text(json.dumps(config, sort_keys=True, separators=(',', ':')))
            gate_config.chmod(0o600)
            operation = 'e' * 32
            helper_operation = '1' * 32
            helper_id = 'a' * 64
            state = dict(schemaVersion=2, mode=updater.MODE, operation=operation,
                phase='needs_inspection', image='sha256:' + 'b' * 64,
                sourceCommit='c' * 40, oldSourceCommit=updater.OLD_SOURCE,
                old={'containerId': updater.OLD_ID, 'image': updater.OLD_IMAGE},
                authorityId=updater.AUTHORITY,
                gateBinding={node: dict(config[node]) for node in ('cluster', 'main')},
                engineImages={'main': 'sha256:' + 'd' * 64, 'cluster': 'sha256:' + 'f' * 64},
                clusterContainerId='9' * 64,
                rollbackName=updater.NAME + '-pre-reference-update-' + operation,
                failedName=updater.NAME + '-failed-reference-update-' + operation)
            updater.legacy.save(operation_root, state)
            checkpoint_before = (operation_root / 'checkpoint.json').read_bytes()
            with HelperJournal(operation_root, updater.helper_binding(state)) as journal:
                journal.write(helper_operation, 'create_pending')
                journal.write(helper_operation, 'created', helper_id)
                journal.write(helper_operation, 'start_pending', helper_id)

            spec = controller.RunSpec(action='recover-helper', source_commit=state['sourceCommit'],
                image=state['image'], main_image=state['engineImages']['main'],
                operation_id=operation, receipt_dir=controller.BASE + '/review/operation',
                gate_config=controller.BASE + '/review/bridge.json')
            evidence = root / 'evidence'
            calls, children = [], []

            def launch(argv, **kwargs):
                calls.append(argv)
                payload = json.loads(shlex_split(argv[-1])[-1])
                role = payload['role']
                payload['source'] = str(ROOT)
                if role == 'cluster':
                    arguments = payload['arguments']
                    arguments[arguments.index('--receipt-dir') + 1] = str(operation_root)
                    arguments[arguments.index('--gate-config') + 1] = str(gate_config)
                    payload['arguments'] = arguments
                env = os.environ.copy()
                env['LT_TEST_BOOTSTRAP'] = '1'
                env['LT_TEST_ROOT'] = str(root)
                env['LT_TEST_HELPER_MARKER'] = str(marker)
                env['LT_TEST_CLUSTER_ENGINE'] = controller.ENGINES['cluster']
                env['LT_TEST_MAIN_ENGINE'] = controller.ENGINES['main']
                env['LT_TEST_ROLE'] = role
                env['XDG_STATE_HOME'] = str(gate_state)
                env['PATH'] = str(docker_bin) + os.pathsep + env['PATH']
                env['PYTHONPATH'] = child_pythonpath
                child = subprocess.Popen([sys.executable, '-B', '-c', controller.BOOTSTRAP,
                    json.dumps(payload, sort_keys=True, separators=(',', ':'))], env=env, **kwargs)
                children.append(child)
                return child

            result = controller.run(spec, config, evidence, popen=launch)
            self.assertEqual(result, 0)
            self.assertEqual(len(calls), 2)
            self.assertTrue(all(child.poll() is not None and child.returncode == 0 for child in children))
            self.assertFalse(marker.exists())
            self.assertEqual((operation_root / 'checkpoint.json').read_bytes(), checkpoint_before)
            with HelperJournal(operation_root, updater.helper_binding(state)) as journal:
                self.assertEqual(journal.read()['phase'], 'resolved')


def test_configuration():
    config = dict(schemaVersion=1, sessionNonce='a' * 64, timeoutSeconds=3)
    for node in ('main', 'cluster'):
        config[node] = dict(engineId=controller.ENGINES[node], nodeId=node,
            authorityId=controller.AUTHORITY, coordinatorOrigin='http://127.0.0.1:18080',
            generation=1, helperSha256=helper_digest())
    return config


def shlex_split(value):
    import shlex
    return shlex.split(value)


SITECUSTOMIZE = r'''import os, pwd, socket
if os.environ.get('LT_TEST_BOOTSTRAP') == '1':
    from pathlib import Path
    class _Passwd:
        pw_name = 'bkoltz'
    pwd.getpwuid = lambda uid: _Passwd()
    socket.gethostname = lambda: 'node' if os.environ.get('LT_TEST_ROLE') == 'main' else 'cluster'
    from ops import coordinator_gate_bridge as bridge
    from ops import install_reference_coordinator as install
    from ops import update_reference_coordinator as updater
    install.LOCK_PATH = Path(os.environ['LT_TEST_ROOT']) / 'locks' / 'coordinator-install.lock'
    updater.ACTIVATION_REVIEWED = True
    bridge.MainEngineProbe = lambda expected_image: (lambda: os.environ['LT_TEST_MAIN_ENGINE'])
'''


DOCKER_STUB = r'''#!/usr/bin/env python3
import json, os, pathlib, sys
args=sys.argv[1:]
if args[:1]==['--host']:
    args=args[2:]
marker=pathlib.Path(os.environ['LT_TEST_HELPER_MARKER'])
helper='a'*64; operation='1'*32
if args[:1]==['info']:
    print(os.environ['LT_TEST_CLUSTER_ENGINE'])
elif args[:1]==['ps']:
    if marker.exists():print(helper)
elif args[:1]==['inspect']:
    print(json.dumps([{'Id':helper,'Name':'/ledgetop-update-helper-'+operation,
        'Config':{'Labels':{'io.ledgetop.update-helper':operation}}}]))
elif args[:1]==['rm']:
    assert args[-1]==helper
    marker.unlink(missing_ok=True)
else:
    raise SystemExit('unexpected docker stub action: '+repr(args))
'''


if __name__ == '__main__':
    unittest.main()
