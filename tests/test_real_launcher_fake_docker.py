"""Opt-in real-Docker launcher integration; invented data, network disabled.

The test deliberately terminates the test worker after the fake coordinator
accepts its task, then verifies the launcher retains its receipt/container and
refuses another attempt. It does not access a live host or real credentials.
"""
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tests'))
spec=importlib.util.spec_from_file_location('real_pilot_launcher',ROOT/'tests'/'run_real_viewer_pilot.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

RUNNER=r'''const fs=require('node:fs'),{spawn}=require('node:child_process'),readline=require('node:readline'),{once}=require('node:events');
async function main(){
 const secret=fs.readFileSync(0,'utf8').trim();
 const server=spawn('python3',['-u','/processing/tests/legacy_viewer_pair_server.py','/tmp/pilot-coordinator','result'],{stdio:['pipe','pipe','inherit'],env:{...process.env,PYTHONPATH:'/processing'}});
 const serverExit=once(server,'exit');const lines=readline.createInterface({input:server.stdout})[Symbol.asyncIterator]();let worker,workerExit;
 try{
  const first=await lines.next();if(first.done)throw new Error('fake_server_unavailable');const {port}=JSON.parse(first.value);
  worker=spawn(process.execPath,['/tmp/pilot-worker.js'],{stdio:['pipe','pipe','pipe'],env:{...process.env,VIEWER_SOURCE:'/tmp/deps',PILOT_MODE:'fake',PILOT_ROOT:'/pilot',PILOT_REGISTRY:'/pilot-registry',PILOT_ENDPOINT:`http://127.0.0.1:${port}`,PILOT_DEADLINE_SECONDS:'120'}});
  workerExit=once(worker,'exit');worker.stdin.end(secret+'\n');worker.stderr.resume();
  const events=readline.createInterface({input:worker.stdout})[Symbol.asyncIterator]();let committed=false;
  for(;;){const next=await events.next();if(next.done)break;const e=JSON.parse(next.value);if(e.event==='status'&&e.submission==='committed'){committed=true;worker.kill('SIGTERM');break;}}
  await workerExit;if(!committed)throw new Error('fake_commit_missing');
  console.log(JSON.stringify({fake:'committed_then_interrupted'}));
 }finally{if(worker&&worker.exitCode===null&&worker.signalCode===null){worker.kill();await workerExit;}server.stdin.end(JSON.stringify({op:'stop'})+'\n');await serverExit;}
 process.exitCode=23;
}
main().catch(e=>{console.error('fake-runner-failed',e.message);process.exitCode=1});'''


@unittest.skipUnless(os.environ.get('RUN_PILOT_DOCKER_TEST')=='1','explicit isolated Docker test only')
class RealDockerFakeLaunch(unittest.TestCase):
    def test_fake_submission_interruption_retains_identity(self):
        viewer=Path(os.environ['PILOT_TEST_VIEWER']).resolve()
        cache=Path(os.environ['PILOT_TEST_NPM_CACHE']).resolve()
        if not viewer.is_dir() or not cache.is_dir():self.fail('viewer/cache required')
        original_run=subprocess.run
        with tempfile.TemporaryDirectory(prefix='lt-pilot-launch-fake-') as temp:
            root=Path(temp).resolve(); registry=root/'registry';registry.mkdir()
            photos=root/'photos';photos.mkdir();converter=root/'converter';converter.mkdir()
            for n in range(25):(photos/f'{n:03}.jpg').write_bytes(f'invented-{n}'.encode())
            runner=root/'fake-runner.js';runner.write_text(RUNNER,encoding='utf-8')
            args=SimpleNamespace(processing=ROOT,viewer=viewer,cache=cache,converter=converter,photos=photos,registry=registry)
            pinned=m.load_pinned(ROOT);preflight=pinned['preflight'];preflight.converter_status=lambda _: True
            created=[]
            def run(cmd,**kw):
                if isinstance(cmd,list) and cmd[:2]==['docker','create']:
                    cmd=list(cmd);cmd[cmd.index('bridge')]='none'
                    at=cmd.index('--entrypoint')
                    cmd[at:at]=['--mount',f'type=bind,src={ROOT},dst=/processing,readonly',
                                '--mount',f'type=bind,src={runner},dst=/test/fake-runner.js,readonly']
                    cmd[-1]=cmd[-1].replace('exec node /tmp/pilot-worker.js','exec node /test/fake-runner.js')
                    result=original_run(cmd,**kw);created.append(result.stdout.strip());return result
                if isinstance(cmd,list) and cmd[:2]==['docker','start']:
                    kw['timeout']=45  # bounded fake-only monitoring
                result=original_run(cmd,**kw)
                if isinstance(cmd,list) and cmd[:2]==['docker','start'] and result.returncode:
                    print('synthetic docker stdout:',result.stdout,'stderr:',result.stderr,file=sys.stderr)
                return result
            try:
                with patch.object(m,'FIXED_REGISTRY',registry),patch.object(m,'verify_registry_acl'),\
                     patch.object(m,'load_pinned',return_value=pinned),patch.object(m,'remote_snapshot',return_value={}),\
                     patch.object(m,'read_staging_token',return_value='synthetic-viewer-'+'b'*32),\
                     patch.object(m.subprocess,'run',side_effect=run):
                    with self.assertRaisesRegex(RuntimeError,'pilot_failed_preserve_all'):m.launch(args)
                    marker=json.loads((registry/'launcher.json').read_text())
                    pilot=registry/('pilot-'+marker['owner'])
                    receipt=json.loads((pilot/'pilot-receipt.json').read_text())
                    self.assertTrue(receipt.get('attemptId'));self.assertTrue(receipt.get('providerTaskId'))
                    self.assertTrue((pilot/'container.json').is_file())
                    self.assertFalse((pilot/'pilot-complete.json').exists())
                    self.assertEqual(len(list((pilot/'input').glob('*.jpg'))),25)
                    with self.assertRaises(FileExistsError):m.launch(args)
                self.assertEqual(len(created),1)
            finally:
                # Exact task-owned container only. Never sweep other resources.
                for cid in created:
                    inspection=json.loads(subprocess.check_output(['docker','inspect',cid],text=True,timeout=15))[0]
                    owner=json.loads((registry/'launcher.json').read_text())
                    self.assertEqual(inspection['Id'],cid)
                    self.assertEqual(inspection['Name'],'/'+owner['containerName'])
                    self.assertEqual(inspection['Image'],m.IMAGE)
                    self.assertEqual(inspection['Config']['Labels'].get('ltds.pilot.owner'),owner['owner'])
                    if inspection['State']['Running']:
                        subprocess.run(['docker','stop','--time','3',cid],capture_output=True,check=True,timeout=15)
                    subprocess.run(['docker','rm',cid],capture_output=True,check=True,timeout=15)


if __name__=='__main__':unittest.main()
