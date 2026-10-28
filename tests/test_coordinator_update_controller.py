"""Controller tests use real local children/pipes, never SSH or a Docker daemon."""
import json
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import unittest

from ops import run_coordinator_update_bridge as controller
from ops.coordinator_gate_bridge import BridgeError, helper_digest
from processing.node_gate import GateError
from ops.incremental_coordinator_binding import IncrementalBinding


def configuration():
    config = dict(schemaVersion=1, sessionNonce='a' * 64, timeoutSeconds=2)
    for node in ('main', 'cluster'):
        config[node] = dict(engineId=controller.ENGINES[node], nodeId=node,
            authorityId=controller.AUTHORITY, coordinatorOrigin='http://127.0.0.1:18080',
            generation=1, helperSha256=helper_digest())
    return config


def specification(**changes):
    values = dict(action='activate', source_commit='b' * 40, image='sha256:' + 'c' * 64,
        main_image='sha256:' + 'd' * 64, operation_id='e' * 32,
        receipt_dir=controller.BASE + '/test-update/receipt',
        gate_config=controller.BASE + '/test-update/bridge.json')
    values.update(changes)
    return controller.RunSpec(**values)


class ControllerTests(unittest.TestCase):
    def test_incremental_helper_bundle_and_current_cid_are_separate_from_candidate_app(self):
        profile=IncrementalBinding('1'*64,'sha256:'+'2'*64,'3'*40,'4'*40,controller.AUTHORITY,'5'*64,'6'*64,'7'*64,'sha256:'+'8'*64,'9'*64,'sha256:'+'a'*64,'b'*64,'c'*64,'d'*64)
        spec=specification(source_commit=profile.candidate_source,main_image=profile.main_image,helper_source_commit='e'*40,incremental_binding=profile)
        config=configuration()
        for node in ('cluster','main'):config[node]['helperSha256']=controller.incremental_helper_digest()
        for node in ('cluster','main'):
            argv=controller.ssh_argv(node,spec,config);payload=json.loads(shlex.split(argv[-1])[4])
            self.assertTrue(payload['source'].endswith('/reference-'+'e'*40+'/source'))
            self.assertNotIn('/reference-'+profile.candidate_source+'/',payload['source'])
            self.assertEqual(payload['incrementalBinding']['predecessor_container'],profile.predecessor_container)
            if node=='main':self.assertEqual(payload['expectedMainContainer'],profile.main_container)
            else:
                arguments=payload['arguments'];self.assertEqual(arguments[arguments.index('--source-commit')+1],profile.candidate_source)
                self.assertEqual(arguments[arguments.index('--helper-source-commit')+1],'e'*40)
                self.assertIn('--incremental-profile',arguments)
        with self.assertRaises(BridgeError):specification(incremental_binding=profile)

    def test_remote_commands_are_one_quoted_static_program_and_closed_json(self):
        spec = specification(); config = configuration()
        for node in ('main', 'cluster'):
            argv = controller.ssh_argv(node, spec, config)
            self.assertEqual(argv[-2], controller.HOSTS[node])
            self.assertIn('StrictHostKeyChecking=yes', argv)
            self.assertIn('ForwardAgent=no', argv)
            self.assertIn('ClearAllForwardings=yes', argv)
            self.assertIn('ControlPath=none', argv)
            program = shlex.split(argv[-1])
            self.assertEqual(program[:4], ['python3', '-B', '-c', controller.BOOTSTRAP])
            payload = json.loads(program[4])
            self.assertEqual(payload['source'], spec.source)
            self.assertEqual(payload['role'], node)
            self.assertEqual(payload['operation'], spec.operation_id)
            if node == 'main': self.assertEqual(payload['expectedMainImage'], spec.main_image)
            self.assertNotIn('token', json.dumps(payload).lower())

    def test_rejects_unprepared_actions_tags_shell_text_and_other_remote_roots(self):
        invalid = [dict(action='prepare'), dict(source_commit='main'), dict(image='latest'),
            dict(operation_id='e' * 32 + ';whoami'), dict(receipt_dir='/tmp/receipt'),
            dict(receipt_dir=controller.BASE + '/../receipt'),
            dict(gate_config=controller.BASE + '/$(whoami).json'),
            dict(gate_config=controller.BASE + '/x//bridge.json')]
        for changes in invalid:
            with self.subTest(changes=changes), self.assertRaises(BridgeError):
                specification(**changes)

    def test_rejects_binding_drift_and_unknown_fields(self):
        cases = []
        for node in ('main', 'cluster'):
            for key, value in [('engineId', 'other'), ('nodeId', 'other'),
                    ('generation', True), ('helperSha256', 'f' * 64),
                    ('coordinatorOrigin', 'http://192.168.50.89:18080')]:
                config = configuration(); config[node][key] = value; cases.append(config)
        config = configuration(); config['extra'] = True; cases.append(config)
        for config in cases:
            with self.subTest(config=config), self.assertRaises((BridgeError, GateError)):
                controller.validate_config(config)

    def children(self, calls, processes, fail_at=None):
        def launch(argv, **kwargs):
            calls.append(argv)
            if len(calls) == fail_at:
                raise OSError('injected_child_launch_failure')
            self.assertFalse(kwargs['shell'])
            self.assertEqual(kwargs['bufsize'], 0)
            child = subprocess.Popen([sys.executable, '-B', '-c',
                'import sys;sys.stderr.write("owned child\\n");sys.stderr.flush();sys.stdin.buffer.read()'], **kwargs)
            processes.append(child)
            return child
        return launch

    def test_second_launch_failure_reaps_only_its_actual_first_child_without_retry(self):
        calls = []; processes = []
        with tempfile.TemporaryDirectory() as root:
            evidence = Path(root) / 'new'
            with self.assertRaises(OSError):
                controller.run(specification(), configuration(), evidence,
                    popen=self.children(calls, processes, fail_at=2))
            self.assertEqual(len(calls), 2)
            self.assertEqual(len(processes), 1)
            self.assertIsNotNone(processes[0].poll())
            self.assertTrue(processes[0].stdout.closed)
            self.assertTrue((evidence / 'main-ssh.stderr').exists())

    def test_relay_failure_closes_both_real_transports_and_keeps_logs(self):
        calls = []; processes = []; relays = []
        class FailedRelay:
            def __init__(self, *args, **kwargs): relays.append((args, kwargs))
            def run(self): raise BridgeError('injected_main_holder_loss')
        with tempfile.TemporaryDirectory() as root:
            evidence = Path(root) / 'new'
            with self.assertRaisesRegex(BridgeError, 'injected_main_holder_loss'):
                controller.run(specification(), configuration(), evidence,
                    popen=self.children(calls, processes), relay_factory=FailedRelay)
            self.assertEqual(len(calls), 2); self.assertEqual(len(relays), 1)
            for child in processes:
                self.assertIsNotNone(child.poll()); self.assertTrue(child.stdout.closed)
            self.assertEqual({p.name for p in evidence.iterdir()}, {'main-ssh.stderr', 'cluster-ssh.stderr'})

    def test_relay_success_requires_both_actual_zero_exits(self):
        for exit_code in (0, 7):
            calls = []; processes = []
            class CompletedRelay:
                def __init__(self, main, cluster, **kwargs): self.children = (main, cluster)
                def run(self):
                    for child in self.children: child.stdin.close()
                    for child in self.children: child.wait(timeout=3)
                    return 0
            def launch(argv, **kwargs):
                calls.append(argv)
                code = exit_code if len(calls) == 2 else 0
                child = subprocess.Popen([sys.executable, '-B', '-c',
                    'import sys;sys.stdin.buffer.read();sys.exit(' + str(code) + ')'], **kwargs)
                processes.append(child); return child
            with self.subTest(exit_code=exit_code), tempfile.TemporaryDirectory() as root:
                if exit_code:
                    with self.assertRaisesRegex(BridgeError, 'owned_ssh_exit_not_successful'):
                        controller.run(specification(), configuration(), Path(root) / 'new',
                            popen=launch, relay_factory=CompletedRelay)
                else:
                    self.assertEqual(controller.run(specification(), configuration(), Path(root) / 'new',
                        popen=launch, relay_factory=CompletedRelay), 0)
                self.assertEqual(len(calls), 2)
                for child in processes: self.assertIsNotNone(child.poll())

    def test_evidence_directory_is_never_reused_or_overwritten(self):
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaises(FileExistsError):
                controller.run(specification(), configuration(), Path(root),
                    popen=lambda *a, **k: self.fail('must not launch a child'))

    def test_real_relay_with_portable_children_accepts_only_fresh_protocol(self):
        # Synthetic holder identity only: this checks actual Windows/Linux root
        # pipe handling, not the Linux NodeGate ownership tested separately.
        main_program = '''import json,sys
from ops.coordinator_gate_bridge import BridgeBinding,_common
p=json.loads(sys.argv[1]);r=p['binding']
b=BridgeBinding(engine_id=r['engineId'],node_id=r['nodeId'],authority_id=r['authorityId'],coordinator_origin=r['coordinatorOrigin'],gate_generation=r['generation'],helper_sha256=r['helperSha256'])
i=dict(pid=123,rootDevice=1,rootInode=2,lockDevice=1,lockInode=3,stateDevice=1,stateInode=4)
base=_common(b,p['operation'],p['sessionNonce'],'f'*64,i)
print(json.dumps(dict(base,type='main-gate-ready')),flush=True)
for line in sys.stdin.buffer:
 q=json.loads(line);q['type']='main-gate-checked'
 if p['corrupt']:q['nonce']='0'*64
 print(json.dumps(q),flush=True)
'''
        cluster_program = '''import json,sys
from ops.coordinator_gate_bridge import BridgeBinding,BridgeGateClient
p=json.loads(sys.argv[1]);r=p['binding']
b=BridgeBinding(engine_id=r['engineId'],node_id=r['nodeId'],authority_id=r['authorityId'],coordinator_origin=r['coordinatorOrigin'],gate_generation=r['generation'],helper_sha256=r['helperSha256'])
with BridgeGateClient(sys.stdin.buffer,sys.stdout.buffer,binding=b,operation_id=p['operation'],session_nonce=p['sessionNonce'],timeout_seconds=2) as gate:
 gate.check();gate.check()
'''
        for corrupt in (False, True):
            calls = []; children = []; config = configuration(); spec = specification()
            payload = dict(binding=config['main'], operation=spec.operation_id,
                sessionNonce=config['sessionNonce'], corrupt=corrupt)
            def launch(argv, **kwargs):
                calls.append(argv)
                program = main_program if len(calls) == 1 else cluster_program
                child = subprocess.Popen([sys.executable, '-B', '-c', program, json.dumps(payload)], **kwargs)
                children.append(child); return child
            with self.subTest(corrupt=corrupt), tempfile.TemporaryDirectory() as root:
                if corrupt:
                    with self.assertRaises(BridgeError):
                        controller.run(spec, config, Path(root) / 'new', popen=launch)
                else:
                    self.assertEqual(controller.run(spec, config, Path(root) / 'new', popen=launch), 0)
                self.assertEqual(len(calls), 2)
                for child in children:
                    self.assertIsNotNone(child.poll()); self.assertTrue(child.stdout.closed)


if __name__ == '__main__': unittest.main()
