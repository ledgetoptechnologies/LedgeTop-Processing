import importlib.util
import io
import json
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
            raise AssertionError(cmd)
        def output(cmd,**kw):
            return json.dumps([{'Id':'c'*64,'Name':'/'+state['name'],'Image':m.IMAGE,
                'Config':{'Labels':{'ltds.pilot.owner':state['owner']}}}])
        patches=[patch.object(m,'source_status',return_value=True),patch.object(m,'converter_status',return_value=True),
                 patch.object(m,'sample_size',return_value=100),patch.object(m,'remote_snapshot',return_value={}),
                 patch.object(m,'read_staging_token',return_value=secret),patch.object(m,'prepare',side_effect=prepare),
                 patch.object(m.subprocess,'run',side_effect=run),patch.object(m.subprocess,'check_output',side_effect=output)]
        for p in patches:p.start()
        self.addCleanup(lambda:[p.stop() for p in reversed(patches)])
        return calls,state,secret
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
