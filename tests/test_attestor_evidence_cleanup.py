import contextlib
import io
import json
import os
import socket
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch


@unittest.skipUnless(os.name=='posix','Unix socket fixture required')
class ProbeCleanupTests(unittest.TestCase):
    def test_inspect_failure_retains_socket_and_recovery_metadata(self):
        source=(Path(__file__).resolve().parents[1]/'docs/evidence/pr205-host-attestor-probe-reviewed.py.txt').read_text()
        fake_module=b'class DockerProcessAttestor: pass\nclass HandoffHeld(Exception): pass\ndef _peer(x): pass\n'
        with tempfile.TemporaryDirectory() as parent:
            root=Path(parent)/'owned';root.mkdir()
            calls=[]
            def docker(args,**_kwargs):
                calls.append(args)
                if args[3:5]==['info','--format']:return subprocess.CompletedProcess(args,0,stdout='synthetic-daemon\n')
                if args[3:5]==['run','-d']:return subprocess.CompletedProcess(args,0,stdout='a'*64+'\n')
                if args[3]=='inspect':raise subprocess.CalledProcessError(1,args)
                self.fail('unexpected Docker action')
            output=io.StringIO()
            with patch('tempfile.mkdtemp',return_value=str(root)),patch('base64.b64decode',return_value=fake_module), \
                 patch('subprocess.run',side_effect=docker),contextlib.redirect_stdout(output):
                with self.assertRaises(subprocess.CalledProcessError):exec(compile(source,'probe','exec'),{'__name__':'__main__'})
            self.assertTrue((root/'gate.sock').exists())
            recovery=json.loads((root/'recovery.json').read_text())
            self.assertTrue(recovery['name'].startswith('codex-fd-attestor-'))
            self.assertEqual(json.loads((root/'container-id.json').read_text()),{'id':'a'*64})
            self.assertIn('owned_directory_retained=',output.getvalue())
            self.assertFalse(any('rm' in args for args in calls))

    def test_create_timeout_retains_name_and_socket(self):
        source=(Path(__file__).resolve().parents[1]/'docs/evidence/pr205-host-attestor-probe-reviewed.py.txt').read_text()
        fake_module=b'class DockerProcessAttestor: pass\nclass HandoffHeld(Exception): pass\ndef _peer(x): pass\n'
        with tempfile.TemporaryDirectory() as parent:
            root=Path(parent)/'owned';root.mkdir();calls=[]
            def docker(args,**_kwargs):
                calls.append(args)
                if args[3:5]==['info','--format']:return subprocess.CompletedProcess(args,0,stdout='synthetic-daemon\n')
                if args[3:5]==['run','-d']:raise subprocess.TimeoutExpired(args,12)
                self.fail('unexpected Docker action')
            with patch('tempfile.mkdtemp',return_value=str(root)),patch('base64.b64decode',return_value=fake_module), \
                 patch('subprocess.run',side_effect=docker),contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaises(subprocess.TimeoutExpired):exec(compile(source,'probe','exec'),{'__name__':'__main__'})
            self.assertTrue((root/'gate.sock').exists());self.assertTrue((root/'recovery.json').exists())
            self.assertFalse((root/'container-id.json').exists());self.assertFalse(any('rm' in args for args in calls))

    def test_daemon_switch_before_stop_retains_container_and_recovery(self):
        source=(Path(__file__).resolve().parents[1]/'docs/evidence/pr205-host-attestor-probe-reviewed.py.txt').read_text()
        fake_module=b'class DockerProcessAttestor: pass\nclass HandoffHeld(Exception): pass\ndef _peer(x): pass\n'
        with tempfile.TemporaryDirectory() as parent:
            root=Path(parent)/'owned';root.mkdir();calls=[];state={'infos':0,'label':None}
            def docker(args,**_kwargs):
                calls.append(args);command=args[3]
                if command=='info':
                    state['infos']+=1
                    return subprocess.CompletedProcess(args,0,stdout='daemon-a\n' if state['infos']==1 else 'daemon-b\n')
                if command=='run':
                    state['label']=args[args.index('--label')+1].split('=',1)[1]
                    return subprocess.CompletedProcess(args,0,stdout='a'*64+'\n')
                if command=='inspect':
                    details={'Id':'a'*64,'Image':'sha256:b655ce507f3ea3924bae9bdb93a6a8ecfa9e55941ab0f9f34f540dc101796a0c',
                        'Config':{'Labels':{'codex.task':state['label']}},'HostConfig':{'NetworkMode':'none','Privileged':False},
                        'State':{'Running':True}}
                    return subprocess.CompletedProcess(args,0,stdout=json.dumps([details]))
                self.fail('mutating cleanup called after daemon switch')
            original_accept=socket.socket.accept;accepts=[]
            def accept(sock):
                accepts.append(True)
                if len(accepts)==2:raise RuntimeError('synthetic accept failure')
                return original_accept(sock)
            with patch('tempfile.mkdtemp',return_value=str(root)),patch('base64.b64decode',return_value=fake_module), \
                 patch('subprocess.run',side_effect=docker),patch.object(socket.socket,'accept',accept), \
                 contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(RuntimeError,'synthetic accept failure'):
                    exec(compile(source,'probe','exec'),{'__name__':'__main__'})
            self.assertTrue((root/'gate.sock').exists());self.assertTrue((root/'recovery.json').exists())
            self.assertEqual(json.loads((root/'container-id.json').read_text()),{'id':'a'*64})
            self.assertFalse(any(args[3] in ('stop','rm') for args in calls))

    def test_daemon_switch_before_remove_retains_container_and_recovery(self):
        source=(Path(__file__).resolve().parents[1]/'docs/evidence/pr205-host-attestor-probe-reviewed.py.txt').read_text()
        fake_module=b'class DockerProcessAttestor: pass\nclass HandoffHeld(Exception): pass\ndef _peer(x): pass\n'
        with tempfile.TemporaryDirectory() as parent:
            root=Path(parent)/'owned';root.mkdir();calls=[];state={'infos':0,'label':None}
            def docker(args,**_kwargs):
                calls.append(args);command=args[3]
                if command=='info':
                    state['infos']+=1
                    return subprocess.CompletedProcess(args,0,stdout='daemon-a\n' if state['infos']==1 else 'daemon-b\n')
                if command=='run':
                    state['label']=args[args.index('--label')+1].split('=',1)[1]
                    return subprocess.CompletedProcess(args,0,stdout='a'*64+'\n')
                if command=='inspect':
                    details={'Id':'a'*64,'Image':'sha256:b655ce507f3ea3924bae9bdb93a6a8ecfa9e55941ab0f9f34f540dc101796a0c',
                        'Config':{'Labels':{'codex.task':state['label']}},'HostConfig':{'NetworkMode':'none','Privileged':False},
                        'State':{'Running':False}}
                    return subprocess.CompletedProcess(args,0,stdout=json.dumps([details]))
                self.fail('mutating cleanup called after daemon switch')
            original_accept=socket.socket.accept;accepts=[]
            def accept(sock):
                accepts.append(True)
                if len(accepts)==2:raise RuntimeError('synthetic accept failure')
                return original_accept(sock)
            with patch('tempfile.mkdtemp',return_value=str(root)),patch('base64.b64decode',return_value=fake_module), \
                 patch('subprocess.run',side_effect=docker),patch.object(socket.socket,'accept',accept), \
                 contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(RuntimeError,'synthetic accept failure'):
                    exec(compile(source,'probe','exec'),{'__name__':'__main__'})
            self.assertTrue((root/'gate.sock').exists());self.assertTrue((root/'recovery.json').exists())
            self.assertFalse(any(args[3] in ('stop','rm') for args in calls))


if __name__=='__main__':unittest.main()
