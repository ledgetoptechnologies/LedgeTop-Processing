"""Hard-kill the coordinator mid-job and restart it on the same state.

Synthetic only: two in-process synthetic engines, the coordinator in a child
process on a temporary state directory, and the actual Viewer NodeOdmProvider
driving one 20-file job. The coordinator is killed without cleanup (SIGKILL /
TerminateProcess) and restarted on the same state: after the engine commit, after
upload but before commit, after completion but before any result download, or
after the first download. Checks that the job then completes, the engines received
exactly one commit, and every download after the restart is byte-identical. Does not contact LAN nodes or modify Viewer. Requires Node.js
and the trusted current Viewer adapter path:

    python scripts/viewer_restart_smoke.py --viewer-adapter PATH [--kill-at running|uploaded|completed|downloaded]
"""
import argparse
import hashlib
from http.server import ThreadingHTTPServer
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import tempfile
import threading
import time

CHILD = r'''
import json, sys, threading
from http.server import ThreadingHTTPServer
from processing.app import make_handler
from processing.config import Config
spec = json.loads(sys.argv[1])
config = Config(spec['api'], spec['local'], spec['main'], 20, 1024 ** 2,
    prototype_state_dir=spec['state'], prototype_operator_token=spec['operator'],
    prototype_node_ids=('cluster', 'main'), treedar_node_ids=('main',),
    local_node_token=spec['node'], main_node_token=spec['node'],
    viewer_enabled=True, viewer_token=spec['viewer'], viewer_staging_token=spec['staging'])
handler = make_handler(config)
server = ThreadingHTTPServer(('127.0.0.1', spec['port']), handler)
handler.viewer_service.start()
print('READY', flush=True)
server.serve_forever()
'''

JAVASCRIPT = r'''
const assert=require('node:assert/strict'),crypto=require('node:crypto'),fs=require('node:fs');
const {NodeOdmProvider}=require(process.env.VIEWER_ADAPTER);
const p=new NodeOdmProvider({endpoint:process.env.SMOKE_ENDPOINT,token:process.env.SMOKE_TOKEN});
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
async function retry(fn,label){const end=Date.now()+60000;for(;;){try{return await fn();}catch(e){if(Date.now()>end)throw Error(label+': '+e.message);await sleep(200);}}}
(async()=>{
 const uuid=crypto.randomUUID();
 await p.initialize({uuid,name:'restart-probe',options:{'pc-ept':true}});
 const files=Array.from({length:20},(_,i)=>({relativePath:`image-${i}.jpg`,buffer:Buffer.from(`SYNTHETIC RESTART ${i}`)}));
 await p.upload(uuid,files);
 fs.writeFileSync(process.env.SMOKE_STAGE,'uploaded');
 await retry(()=>p.commit(uuid),'commit');
 fs.writeFileSync(process.env.SMOKE_STAGE,'committed');
 const hold=async stage=>{fs.writeFileSync(process.env.SMOKE_STAGE,stage);if(process.env.SMOKE_KILL_AT!==stage)return;
  const end=Date.now()+90000;while(!fs.existsSync(process.env.SMOKE_RESUME)){if(Date.now()>end)throw Error('restart never signalled');await sleep(50);}};
 const end=Date.now()+90000;let last;
 for(;;){let s;try{s=await p.status(uuid);}catch(e){s={status:'unreachable'};}
  if(s.status!==last){console.error('status',s.status);last=s.status;}
  if(s.status==='completed')break;
  assert(!['failed','cancelled','unknown'].includes(s.status),JSON.stringify(s));
  if(Date.now()>end)throw Error('timeout waiting for completion, last '+JSON.stringify(s));await sleep(150);}
 await hold('completed');
 const a=Buffer.from(await (await retry(()=>p.downloadAll(uuid),'download')).arrayBuffer());
 await hold('downloaded');
 const b=Buffer.from(await (await retry(()=>p.downloadAll(uuid),'download')).arrayBuffer());
 assert.deepEqual(a,b);
 console.log(JSON.stringify({uuid,bytes:a.length,sha256:crypto.createHash('sha256').update(a).digest('hex')}));
})().catch(e=>{console.error(e.message);process.exitCode=1;});
'''


def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--viewer-adapter', required=True)
    parser.add_argument('--kill-at', choices=('running', 'uploaded', 'completed', 'downloaded'), default='running')
    parser.add_argument('--node', default='node')
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo))
    from processing.synthetic_engine import SyntheticEngine, handler_for

    servers, children, js = [], [], None
    with tempfile.TemporaryDirectory(prefix='viewer-restart-synthetic-') as directory:
        root = Path(directory)
        try:
            node_token = secrets.token_urlsafe(32)
            engines = [SyntheticEngine(20, 4), SyntheticEngine(None, 4)]
            origins = []
            for engine in engines:
                server = ThreadingHTTPServer(('127.0.0.1', 0), handler_for(engine, node_token))
                servers.append(server)
                threading.Thread(target=server.serve_forever, daemon=True).start()
                origins.append('http://127.0.0.1:' + str(server.server_port))
            spec = {'api': secrets.token_urlsafe(32), 'local': origins[0], 'main': origins[1],
                    'state': str(root / 'state'), 'operator': secrets.token_urlsafe(32), 'node': node_token,
                    'viewer': secrets.token_urlsafe(32), 'staging': secrets.token_urlsafe(32), 'port': free_port()}

            def start_child():
                child = subprocess.Popen([sys.executable, '-c', CHILD, json.dumps(spec)], cwd=repo,
                                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                children.append(child)
                line = child.stdout.readline()
                if line.strip() != 'READY':
                    raise RuntimeError('coordinator did not start: ' + child.stderr.read()[-800:])
                return child

            coordinator = start_child()
            stage = root / 'stage'
            script = root / 'restart.cjs'
            script.write_text(JAVASCRIPT)
            env = {**os.environ, 'VIEWER_ADAPTER': str(Path(args.viewer_adapter).resolve()),
                   'SMOKE_ENDPOINT': 'http://127.0.0.1:' + str(spec['port']),
                   'SMOKE_TOKEN': spec['viewer'], 'SMOKE_STAGE': str(stage),
                   'SMOKE_KILL_AT': args.kill_at, 'SMOKE_RESUME': str(root / 'resume')}
            js = subprocess.Popen([args.node, str(script)], env=env, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, text=True)

            deadline = time.monotonic() + 60
            if args.kill_at == 'uploaded':
                ready = lambda: stage.exists() and stage.read_text() in ('uploaded', 'committed')
            elif args.kill_at in ('completed', 'downloaded'):
                ready = lambda: stage.exists() and stage.read_text() == args.kill_at
            else:
                ready = lambda: sum(e.commits for e in engines) >= 1
            while not ready() and js.poll() is None and time.monotonic() < deadline:
                time.sleep(.02)
            if not ready():
                raise RuntimeError('job never reached kill point; adapter: ' + (js.stderr.read() if js.poll() is not None else 'running'))
            commits_at_kill = sum(e.commits for e in engines)
            coordinator.kill(); coordinator.wait()  # SIGKILL / TerminateProcess: no cleanup runs
            killed_rc = coordinator.returncode
            time.sleep(1)
            start_child()
            (root / 'resume').write_text('restarted')

            stdout, stderr = js.communicate(timeout=150)
            if js.returncode:
                raise RuntimeError('adapter workflow failed after restart: ' + stderr[-800:])
            result = json.loads(stdout.strip().splitlines()[-1])
            total_commits = sum(e.commits for e in engines)
            assert total_commits == 1, f'engine commits {total_commits}, expected exactly 1'
            assert all(e.maximum_running <= 1 for e in engines)
            print(json.dumps({'killAt': args.kill_at, 'killedReturnCode': killed_rc,
                              'engineCommitsAtKill': commits_at_kill, 'engineCommitsTotal': total_commits,
                              'adapterStatusTrail': [l for l in stderr.splitlines() if l.startswith('status ')],
                              'download': result, 'repeatDownloadIdentical': True, 'syntheticOnly': True},
                             sort_keys=True))
            return 0
        except Exception as error:
            print('restart smoke FAILED: ' + str(error), file=sys.stderr)
            for child in children:
                if child.poll() is not None and child.stderr:
                    print('coordinator stderr tail: ' + child.stderr.read()[-600:], file=sys.stderr)
            return 1
        finally:
            if js and js.poll() is None:
                js.kill(); js.wait()
            for child in children:
                if child.poll() is None:
                    child.kill(); child.wait()
            for server in servers:
                server.shutdown(); server.server_close()


if __name__ == '__main__':
    raise SystemExit(main())
