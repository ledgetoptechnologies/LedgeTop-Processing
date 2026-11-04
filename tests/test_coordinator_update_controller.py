"""Controller tests use real local children/pipes, never SSH or a Docker daemon."""
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import unittest

from ops import run_coordinator_update_bridge as controller
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
    values = dict(action='activate', source_commit='b' * 40, image='sha256:' + 'c' * 64,
        main_image='sha256:' + 'd' * 64, operation_id='e' * 32,
        receipt_dir=controller.BASE + '/test-update/receipt',
        gate_config=controller.BASE + '/test-update/bridge.json')
    values.update(changes)
    return controller.RunSpec(**values)


class ControllerTests(unittest.TestCase):
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

    def test_recover_helper_is_explicitly_supported_by_cli_and_cluster_payload(self):
        output=subprocess.run([sys.executable,'-B','-m','ops.run_coordinator_update_bridge','--help'],
            capture_output=True,text=True,check=True).stdout
        self.assertIn('recover-helper',output)
        spec=specification(action='recover-helper'); config=configuration()
        main=json.loads(shlex.split(controller.ssh_argv('main',spec,config)[-1])[4])
        cluster=json.loads(shlex.split(controller.ssh_argv('cluster',spec,config)[-1])[4])
        self.assertEqual(cluster['arguments'][0],'recover-helper')
        self.assertEqual(cluster['operation'],spec.operation_id)
        self.assertEqual(main['operation'],spec.operation_id)
        self.assertEqual(cluster['arguments'][cluster['arguments'].index('--receipt-dir')+1],spec.receipt_dir)
        self.assertEqual(cluster['arguments'][cluster['arguments'].index('--gate-config')+1],spec.gate_config)

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

    @unittest.skipUnless(os.name=='posix','actual POSIX journal and child locks')
    def test_recover_helper_through_real_relay_and_updater_method(self):
        # Real controller and framed pipes; synthetic local Docker and gate
        # holder. Separate updater-method tests cover the engine/gate failures.
        holder='''import json,sys
from ops.coordinator_gate_bridge import BridgeBinding,_common
p=json.loads(sys.argv[1]); r=p['binding']; b=BridgeBinding(engine_id=r['engineId'],node_id=r['nodeId'],authority_id=r['authorityId'],coordinator_origin=r['coordinatorOrigin'],gate_generation=r['generation'],helper_sha256=r['helperSha256'])
i=dict(pid=123,rootDevice=1,rootInode=2,lockDevice=1,lockInode=3,stateDevice=1,stateInode=4)
base=_common(b,p['operation'],p['sessionNonce'],'f'*64,i)
print(json.dumps(dict(base,type='main-gate-ready')),flush=True)
for line in sys.stdin.buffer:
 q=json.loads(line);q['type']='main-gate-checked';print(json.dumps(q),flush=True)
'''
        executor='''import contextlib,fcntl,json,os,sys
from pathlib import Path
from ops import update_reference_coordinator as u
from ops.coordinator_gate_bridge import BridgeBinding,BridgeGateClient
from ops.durable_helper_journal import HelperJournal
p=json.loads(sys.argv[1]);root=Path(p['root']);root.mkdir(mode=0o700)
r=p['binding'];b=BridgeBinding(engine_id=r['engineId'],node_id=r['nodeId'],authority_id=r['authorityId'],coordinator_origin=r['coordinatorOrigin'],gate_generation=r['generation'],helper_sha256=r['helperSha256'])
binding={'cluster':{'engineId':'fixture-cluster'},'main':r};op=p['operation'];cid='a'*64;hop='1'*32
state=dict(schemaVersion=2,mode=u.MODE,operation=op,phase='needs_inspection',image='sha256:'+'b'*64,sourceCommit='c'*40,oldSourceCommit=u.OLD_SOURCE,old={'containerId':u.OLD_ID,'image':u.OLD_IMAGE},authorityId=u.AUTHORITY,gateBinding=binding,engineImages={'cluster':'sha256:'+'d'*64,'main':'sha256:'+'e'*64},clusterContainerId='f'*64,rollbackName=u.NAME+'-pre-reference-update-'+op,failedName=u.NAME+'-failed-reference-update-'+op)
u.legacy.save(root,state);marker=root/'owned-helper';marker.write_text('running')
with HelperJournal(root,u.helper_binding(state)) as journal:
 journal.write(hop,'create_pending');journal.write(hop,'created',cid);journal.write(hop,'start_pending',cid)
class T:
 save=staticmethod(u.legacy.save);private_file=staticmethod(u.legacy.private_file)
 @contextlib.contextmanager
 def operation_lock(self):
  f=open(root/'global.lock','w');fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)
  try:yield
  finally:f.close()
 def docker(self,*args,**kw):
  if args[0]=='info':return 'fixture-cluster'
  if args[0]=='ps':return cid if marker.exists() else ''
  if args[0]=='rm':assert args[-1]==cid;marker.unlink();return ''
  raise AssertionError(args)
 def inspected(self,identifier,**kw):return {'Id':cid,'Name':'/ledgetop-update-helper-'+hop,'Config':{'Labels':{'io.ledgetop.update-helper':hop}}}
class Factory:
 binding=binding
 def __call__(self):return BridgeGateClient(sys.stdin.buffer,sys.stdout.buffer,binding=b,operation_id=op,session_nonce=p['sessionNonce'],timeout_seconds=2)
u.ReferenceCoordinatorUpdate(root,gate_factory=Factory(),transport=T()).recover_helper(state)
assert not marker.exists()
with HelperJournal(root,u.helper_binding(state)) as journal:assert journal.read()['phase']=='resolved'
'''
        with tempfile.TemporaryDirectory() as temp:
            config=configuration();spec=specification(action='recover-helper');calls=[];children=[]
            payload=dict(binding=config['main'],operation=spec.operation_id,sessionNonce=config['sessionNonce'],root=str(Path(temp)/'operation'))
            def launch(argv,**kwargs):
                calls.append(argv);routed=json.loads(shlex.split(argv[-1])[4])
                if len(calls)==2:self.assertEqual(routed['arguments'][0],'recover-helper')
                child=subprocess.Popen([sys.executable,'-B','-c',holder if len(calls)==1 else executor,json.dumps(payload)],**kwargs)
                children.append(child);return child
            self.assertEqual(controller.run(spec,config,Path(temp)/'evidence',popen=launch),0)
            self.assertEqual(len(children),2)
            for child in children:self.assertIsNotNone(child.poll())

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
        for action,corrupt in (('activate',False),('activate',True),('recover-helper',False),('recover-helper',True)):
            calls = []; children = []; config = configuration(); spec = specification(action=action)
            payload = dict(binding=config['main'], operation=spec.operation_id,
                sessionNonce=config['sessionNonce'], corrupt=corrupt)
            def launch(argv, **kwargs):
                calls.append(argv)
                routed=json.loads(shlex.split(argv[-1])[4])
                if len(calls)==2:self.assertEqual(routed['arguments'][0],action)
                program = main_program if len(calls) == 1 else cluster_program
                child = subprocess.Popen([sys.executable, '-B', '-c', program, json.dumps(payload)], **kwargs)
                children.append(child); return child
            with self.subTest(action=action,corrupt=corrupt), tempfile.TemporaryDirectory() as root:
                if corrupt:
                    with self.assertRaises(BridgeError):
                        controller.run(spec, config, Path(root) / 'new', popen=launch)
                else:
                    self.assertEqual(controller.run(spec, config, Path(root) / 'new', popen=launch), 0)
                self.assertEqual(len(calls), 2)
                for child in children:
                    self.assertIsNotNone(child.poll()); self.assertTrue(child.stdout.closed)


if __name__ == '__main__': unittest.main()
