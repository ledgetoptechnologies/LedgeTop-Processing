import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace
from unittest.mock import patch

ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT))
spec=importlib.util.spec_from_file_location('pilot_launcher',ROOT/'run_real_viewer_pilot.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


class LauncherTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();root=Path(self.tmp.name).resolve()
        names=('processing','viewer','cache','converter','photos','registry','parent')
        self.args=SimpleNamespace(**{n:root/n for n in names})
        for n in names:getattr(self.args,n).mkdir()
    def tearDown(self):self.tmp.cleanup()
    def exercise(self, failure=False):
        calls=[]; state={}; secret='stage-secret-'+'a'*40
        def prepare(source,data,count):
            data.mkdir();state['data']=data
            return {'owner':'b'*32,'bytes':100,'count':count}
        def run(cmd,**kw):
            calls.append((cmd,kw))
            if cmd[:3]==['docker','image','inspect']:return SimpleNamespace(returncode=0)
            if cmd[:2]==['docker','create']:
                state['name']=cmd[cmd.index('--name')+1]; state['owner']=state['name'].split('-')[-1]
                return SimpleNamespace(stdout='c'*64,returncode=0)
            if cmd[:2]==['docker','start']:
                if failure:raise subprocess.TimeoutExpired(cmd,1)
                receipt={'owner':'b'*32,'attemptId':'attempt','providerTaskId':'provider'}
                complete={**receipt,'files':6,'archiveManifestSha256':'d'*64,'tilesSha256':'e'*64}
                (state['data']/'pilot-receipt.json').write_text(json.dumps(receipt))
                (state['data']/'pilot-complete.json').write_text(json.dumps(complete))
                return SimpleNamespace(returncode=0)
            if cmd[:2]==['git','archive']:
                kw['stdout'].write(b'synthetic git archive')
                return SimpleNamespace(returncode=0)
            raise AssertionError(cmd)
        def output(cmd,**kw):
            return json.dumps([{'Id':'c'*64,'Name':'/'+state['name'],'Image':m.IMAGE,
                'Config':{'Labels':{'ltds.pilot.owner':state['owner']}}}])
        preflight=SimpleNamespace(source_status=lambda _:True,converter_status=lambda _:True,sample_size=lambda _:100,REMOTE='fake')
        pinned={'preflight':preflight,'input':SimpleNamespace(prepare=prepare),'worker':b'fake'}
        patches=[patch.object(m,'FIXED_REGISTRY',self.args.registry),patch.object(m,'verify_registry_acl'),
                 patch.object(m,'load_pinned',return_value=pinned),patch.object(m,'remote_snapshot',return_value={}),
                 patch.object(m,'read_staging_token',return_value=secret),
                 patch.object(m.subprocess,'run',side_effect=run),patch.object(m.subprocess,'check_output',side_effect=output)]
        for p in patches:p.start()
        self.addCleanup(lambda:[p.stop() for p in reversed(patches)])
        return calls,state,secret
    def test_pinned_inline_entrypoint_reaches_help_without_launch(self):
        bootstrap = '''import hashlib,subprocess
commit='49a4f3b70870d1d275799a99b70c28991a362fb6'
expected='9fb0b1299c55a40ffe041c71b04b3c3e5d57ced5935ef6d20eccba749d5c6dc8'
code=subprocess.check_output(['git','show',commit+':tests/run_real_viewer_pilot.py'])
if hashlib.sha256(code).hexdigest()!=expected: raise SystemExit('pinned launcher mismatch')
exec(compile(code,'<verified-pilot-launcher>','exec'),{'__name__':'__main__'})'''
        result=subprocess.run([sys.executable,'-c',bootstrap,'--help'],cwd=ROOT.parent,
                              capture_output=True,text=True,timeout=15)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertIn('--processing',result.stdout)
        self.assertFalse((self.args.registry/'launcher.json').exists())

    def test_other_registry_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError,'fixed_registry_required'):
            m.verify_registry_acl(self.args.registry)

    @unittest.skipUnless(os.name=='nt','Windows ACL check only')
    def test_private_windows_registry_accepts_owner_and_rejects_everyone(self):
        sid=subprocess.check_output(['pwsh','-NoProfile','-NonInteractive','-Command',
            '[Security.Principal.WindowsIdentity]::GetCurrent().User.Value'],text=True,timeout=15).strip()
        directory=self.args.registry
        subprocess.run(['icacls.exe',str(directory),'/inheritance:r','/grant:r',
            f'*{sid}:(OI)(CI)F','*S-1-5-18:(OI)(CI)F','*S-1-5-32-544:(OI)(CI)F'],
            capture_output=True,text=True,timeout=15,check=True)
        with patch.object(m,'FIXED_REGISTRY',directory):
            m.verify_registry_acl(directory)
            subprocess.run(['icacls.exe',str(directory),'/grant:r','*S-1-1-0:(OI)(CI)F'],
                capture_output=True,text=True,timeout=15,check=True)
            with self.assertRaisesRegex(RuntimeError,'registry_acl_unverified'):
                m.verify_registry_acl(directory)

    def test_pinned_git_objects_match_expected_hashes(self):
        pinned=m.load_pinned(ROOT.parent)
        self.assertIn(b'live_pilot_not_qualified',pinned['worker'])

    def test_mutated_pinned_object_rejected(self):
        with patch.object(m.subprocess,'check_output',return_value=b'altered'):
            with self.assertRaisesRegex(RuntimeError,'processing_source_mismatch'):
                m.load_pinned(self.args.processing)

    def test_viewer_archive_excludes_ignored_node_modules(self):
        import tarfile
        repo=self.args.viewer
        subprocess.run(['git','init','-q'],cwd=repo,check=True)
        subprocess.run(['git','config','user.name','Synthetic'],cwd=repo,check=True)
        subprocess.run(['git','config','user.email','synthetic@example.invalid'],cwd=repo,check=True)
        app=repo/'apps'/'viewer';app.mkdir(parents=True)
        (app/'package.json').write_text('{}')
        (repo/'.gitignore').write_text('node_modules/\n')
        subprocess.run(['git','add','.'],cwd=repo,check=True)
        subprocess.run(['git','commit','-qm','fixture'],cwd=repo,check=True)
        rev=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
        ignored=app/'node_modules';ignored.mkdir();(ignored/'evil.js').write_text('not in committed tree')
        out=self.args.parent/'archive.tar'
        with patch.object(m,'PINNED_VIEWER',rev):m.write_viewer_archive(repo,out)
        with tarfile.open(out) as tar:
            names=tar.getnames()
        self.assertIn('apps/viewer/package.json',names)
        self.assertFalse(any('node_modules' in x for x in names))

    def test_one_shot_token_stdin_and_no_automatic_cleanup(self):
        calls,state,secret=self.exercise()
        with redirect_stdout(io.StringIO()):m.launch(self.args)
        self.assertTrue((self.args.registry/'launcher.json').is_file())
        self.assertTrue((state['data']/'container.json').is_file())
        start=[(cmd,kw) for cmd,kw in calls if cmd[:2]==['docker','start']][0]
        self.assertEqual(start[1]['input'],secret+'\n')
        self.assertNotIn(secret,repr(start[0]))
        self.assertFalse(any(cmd[:2] in (['docker','rm'],['docker','stop']) for cmd,_ in calls))
        with self.assertRaises(FileExistsError):m.launch(self.args)
    def test_interrupted_attach_preserves_owned_state_and_container(self):
        calls,state,_=self.exercise(failure=True)
        with self.assertRaisesRegex(RuntimeError,'monitor_interrupted_preserve_all'):m.launch(self.args)
        self.assertTrue((self.args.registry/'launcher.json').is_file())
        self.assertTrue((state['data']/'container.json').is_file())
        self.assertFalse(any(cmd[:2] in (['docker','rm'],['docker','stop']) for cmd,_ in calls))


if __name__=='__main__':unittest.main()
