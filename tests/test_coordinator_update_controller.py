"""Controller tests use real local children/pipes, never SSH or a Docker daemon."""
import json
import hashlib
import io
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import tarfile
import unittest

from ops import run_coordinator_update_bridge as controller
from ops.make_cutover_manifest import manifest_from_archive
from ops.coordinator_gate_bridge import BridgeError, helper_digest
from processing.node_gate import GateError


def configuration():
    config = dict(schemaVersion=1, sessionNonce='a' * 64, timeoutSeconds=2)
    for node in ('main', 'cluster'):
        config[node] = dict(engineId=controller.ENGINES[node], nodeId=node,
            authorityId=controller.AUTHORITY, coordinatorOrigin='http://127.0.0.1:18080',
            generation=1, helperSha256=helper_digest())
    return config


def specification(**changes):
    values = dict(action='activate', source_commit='b' * 40, bundle_commit='f' * 40,bundle_manifest_sha256='a'*64, image='sha256:' + 'c' * 64,
        main_image='sha256:' + 'd' * 64, operation_id='e' * 32,
        receipt_dir=controller.BASE + '/test-update/receipt',
        gate_config=controller.BASE + '/test-update/bridge.json',predecessor_id='1'*64,
        predecessor_image='sha256:'+'2'*64,predecessor_source='3'*40,lease_dir=controller.BASE+'/test-update/lease')
    values.update(changes)
    return controller.RunSpec(**values)


class ControllerTests(unittest.TestCase):
    def test_manifest_builder_pins_content_and_rejects_links(self):
        def archive(entries):
            out=io.BytesIO()
            with tarfile.open(fileobj=out,mode='w') as t:
                for name,data,kind in entries:
                    item=tarfile.TarInfo(name);item.type=kind;item.size=len(data) if kind==tarfile.REGTYPE else 0
                    if kind==tarfile.SYMTYPE:item.linkname='elsewhere'
                    t.addfile(item,io.BytesIO(data) if kind==tarfile.REGTYPE else None)
            return out.getvalue()
        result=json.loads(manifest_from_archive(archive([('ops/a.py',b'hello',tarfile.REGTYPE)])))
        self.assertEqual(result,{'ops/a.py':hashlib.sha256(b'hello').hexdigest()})
        with self.assertRaises(ValueError):manifest_from_archive(archive([('ops/a',b'',tarfile.SYMTYPE)]))
        with self.assertRaises(ValueError):manifest_from_archive(archive([('ops/../bad',b'x',tarfile.REGTYPE)]))
    @unittest.skipUnless(os.name=='posix','Linux source verification')
    def test_bootstrap_verifies_manifest_before_import(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);(root/'ops').mkdir();(root/'processing').mkdir()
            (root/'ops'/'__init__.py').write_text('')
            (root/'ops'/'coordinator_gate_bridge.py').write_text("print('IMPORTED')\ndef helper_digest(): return 'a'*64\nclass BridgeBinding:\n def __init__(self,**kwargs): pass\nclass MainEngineProbe:\n def __init__(self,**kwargs): pass\ndef serve_main(*args,**kwargs): print('READY')\n")
            files={p.relative_to(root).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in (root/'ops').rglob('*') if p.is_file()}
            manifest=json.dumps(files,sort_keys=True,separators=(',',':')).encode();(root/'cutover-source-manifest.json').write_bytes(manifest)
            payload=dict(source=str(root),bundleManifestSha256=hashlib.sha256(manifest).hexdigest(),hostname='test',role='main',helperSha256='a'*64,operation='b'*32,sessionNonce='c'*64,timeoutSeconds=2,expectedMainImage='sha256:'+'d'*64,binding=dict(engineId='x',nodeId='main',authorityId='y',coordinatorOrigin='http://127.0.0.1:18080',generation=1,helperSha256='a'*64))
            program=controller.BOOTSTRAP.replace("if os.geteuid()!=1000 or pwd.getpwuid(os.geteuid()).pw_name!='bkoltz':raise RuntimeError('account mismatch')","if False:raise RuntimeError('account mismatch')").replace("if socket.gethostname().split('.')[0].lower()!=p['hostname']:raise RuntimeError('host mismatch')","if False:raise RuntimeError('host mismatch')")
            def run():return subprocess.run([sys.executable,'-I','-B','-c',program,json.dumps(payload)],capture_output=True,text=True,timeout=10)
            good=run();self.assertEqual(good.returncode,0,good.stderr);self.assertIn('READY',good.stdout)
            (root/'ops'/'coordinator_gate_bridge.py').write_text("print('TAMPERED')\n")
            bad=run();self.assertNotEqual(bad.returncode,0);self.assertNotIn('TAMPERED',bad.stdout)
            (root/'ops'/'coordinator_gate_bridge.py').write_text("print('IMPORTED')\ndef helper_digest(): return 'a'*64\nclass BridgeBinding:\n def __init__(self,**kwargs): pass\nclass MainEngineProbe:\n def __init__(self,**kwargs): pass\ndef serve_main(*args,**kwargs): print('READY')\n")
            (root/'processing'/'extra.py').write_text("print('EXTRA')\n")
            extra=run();self.assertNotEqual(extra.returncode,0);self.assertNotIn('IMPORTED',extra.stdout)
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
            self.assertEqual(program[:5], ['python3', '-I', '-B', '-c', controller.BOOTSTRAP])
            payload = json.loads(program[5])
            self.assertEqual(payload['source'], spec.source)
            self.assertEqual(payload['role'], node)
            self.assertEqual(payload['operation'], spec.operation_id)
            if node == 'main': self.assertEqual(payload['expectedMainImage'], spec.main_image)
            else:
                arguments=payload['arguments']
                for flag,value in [('--predecessor-id',spec.predecessor_id),('--predecessor-image',spec.predecessor_image),
                                   ('--predecessor-source',spec.predecessor_source),('--lease-dir',spec.lease_dir)]:
                    self.assertEqual(arguments[arguments.index(flag)+1],value)
            self.assertNotIn('token', json.dumps(payload).lower())

    def test_rejects_unprepared_actions_tags_shell_text_and_other_remote_roots(self):
        invalid = [dict(action='prepare'), dict(source_commit='main'), dict(image='latest'),
            dict(operation_id='e' * 32 + ';whoami'), dict(receipt_dir='/tmp/receipt'),
            dict(receipt_dir=controller.BASE + '/../receipt'),
            dict(gate_config=controller.BASE + '/$(whoami).json'),
            dict(gate_config=controller.BASE + '/x//bridge.json'),dict(bundle_commit='main'),dict(bundle_manifest_sha256='bad'),
            dict(predecessor_id='latest'),dict(predecessor_image='latest'),dict(predecessor_source='main'),
            dict(lease_dir='/tmp/lease')]
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
