"""Run the actual Viewer JS adapter against isolated synthetic HTTP engines.

All inputs are invented bytes, no photogrammetry is run. Does not contact LAN
nodes, modify Viewer, or publish packages. Requires the actual adapter path.
"""
import argparse
from contextlib import ExitStack
import hashlib
from http.server import ThreadingHTTPServer
import json
from pathlib import Path
import secrets
import subprocess
import sys
import tempfile
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from processing.app import make_handler
from processing.config import Config
from processing.prototype import PROFILE
from processing.synthetic_engine import SyntheticEngine, handler_for


JAVASCRIPT = r'''
const assert=require('node:assert/strict'),crypto=require('node:crypto'),fs=require('node:fs');
const {NodeOdmProvider}=require(process.env.VIEWER_ADAPTER);
const production=new NodeOdmProvider({endpoint:process.env.SMOKE_ENDPOINT,token:process.env.SMOKE_TOKEN});
const staging=new NodeOdmProvider({endpoint:process.env.SMOKE_ENDPOINT,token:process.env.SMOKE_STAGING_TOKEN});
async function submit(provider,name,count){
 const uuid=crypto.randomUUID();
 await assert.rejects(()=>provider.status(uuid),e=>e.code==='provider_task_not_found');
 await provider.initialize({uuid,name,options:{'pc-ept':true,gltf:true,'3d-tiles':true}});
 const files=Array.from({length:count},(_,i)=>({relativePath:`image-${i}.jpg`,buffer:Buffer.from(`SYNTHETIC TRANSFER ONLY ${name} ${i}`)}));
 for(let start=0;start<files.length;start+=20)await provider.upload(uuid,files.slice(start,start+20));
 assert.equal((await provider.status(uuid)).imagesCount,count);
 await provider.commit(uuid);await provider.commit(uuid);
 return uuid;
}
(async()=>{
 const caps=await production.capabilities();assert.equal(caps.capabilities.providerType,'nodeodm');
 const cancelled=await submit(production,'cancelled-before-dispatch',1);
 await production.cancel(cancelled);assert.equal((await production.status(cancelled)).status,'cancelled');
 await production.remove(cancelled);
 const a=await submit(staging,'staging-small',20);
 const b=await submit(production,'production-small',20);
 const c=await submit(production,'production-large',21);
 await assert.rejects(()=>production.status(a),e=>e.code==='provider_task_not_found');
 fs.writeFileSync(process.env.SMOKE_READY,JSON.stringify({a,b,c}));
 for(const [provider,id] of [[production,b],[production,c],[staging,a]]){
  const deadline=Date.now()+60000;
  for(;;){const state=await provider.status(id);if(state.status==='completed')break;
   assert(!['failed','cancelled','unknown'].includes(state.status),JSON.stringify(state));
   if(Date.now()>deadline)throw Error('synthetic workflow timeout');await new Promise(r=>setTimeout(r,100));}
 }
 const first=Buffer.from(await (await production.downloadAll(b)).arrayBuffer());
 const second=Buffer.from(await (await production.downloadAll(b)).arrayBuffer());assert.deepEqual(first,second);
 const output=await production.output(b);assert.equal(output.lines[1],'');assert.equal(output.nextLine,3);
 for(const [provider,id] of [[production,b],[production,c],[staging,a]])await provider.remove(id);
 console.log(JSON.stringify({adapter:'actual Viewer NodeOdmProvider',uploads:62,completed:3,cancelled:1,
  repeatedCommit:'idempotent',repeatedDownloadSha256:crypto.createHash('sha256').update(first).digest('hex'),
  stagingIsolation:true,outputCursorPreserved:true}));
})().catch(e=>{console.error(e.message);process.exitCode=1;});
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--viewer-adapter', required=True)
    parser.add_argument('--node', default='node')
    args = parser.parse_args()
    adapter = Path(args.viewer_adapter).resolve()
    if not adapter.is_file():
        parser.error('actual Viewer adapter source file required')
    servers, threads = [], []
    service = queue = process = None
    try:
        with ExitStack() as cleanup:
            directory = cleanup.enter_context(tempfile.TemporaryDirectory(prefix='processing-viewer-synthetic-'))
            root = Path(directory)
            node_token, viewer_token, staging_token = (secrets.token_urlsafe(32) for _ in range(3))
            engines = [SyntheticEngine(20, .25), SyntheticEngine(None, .25)]
            origins = []
            for engine in engines:
                server = ThreadingHTTPServer(('127.0.0.1', 0), handler_for(engine, node_token))
                servers.append(server)
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                threads.append(thread);thread.start()
                origins.append('http://127.0.0.1:' + str(server.server_port))
            config = Config(secrets.token_urlsafe(32), *origins, 20, 1024 ** 2,
                prototype_state_dir=str(root / 'state'), prototype_operator_token=secrets.token_urlsafe(32),
                prototype_node_ids=('cluster', 'main'), treedar_node_ids=('main',),
                local_node_token=node_token, main_node_token=node_token,
                viewer_enabled=True, viewer_token=viewer_token, viewer_staging_token=staging_token)
            handler = make_handler(config)
            queue, service = handler.prototype_queue, handler.viewer_service
            cleanup.callback(queue.close)
            cleanup.callback(service.close)
            guard = queue.submit({'requestId': 'synthetic-slot-guard', 'environment': 'production', 'profile': PROFILE})[0]
            claim = queue.claim({'workerId': 'synthetic-guard', 'nodeId': 'main'})
            assert claim['id'] == guard['id']
            server = ThreadingHTTPServer(('127.0.0.1', 0), handler)
            servers.append(server)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            threads.append(thread);thread.start();service.start()
            script, ready = root / 'viewer-smoke.cjs', root / 'ready.json'
            script.write_text(JAVASCRIPT)
            import os
            env = {**os.environ, 'VIEWER_ADAPTER': str(adapter),
                'SMOKE_ENDPOINT': 'http://127.0.0.1:' + str(server.server_port),
                'SMOKE_TOKEN': viewer_token, 'SMOKE_STAGING_TOKEN': staging_token, 'SMOKE_READY': str(ready)}
            process = subprocess.Popen([args.node, str(script)], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            deadline = time.monotonic() + 60
            while not ready.exists() and process.poll() is None and time.monotonic() < deadline:
                time.sleep(.05)
            if not ready.exists():
                raise RuntimeError('adapter could not enqueue fixture jobs')
            assert sum(e.commits for e in engines) == 0, 'photo execution bypassed global TreeDar slot'
            queue.complete(claim['id'], {'claimToken': claim['claimToken'], 'status': 'succeeded',
                                         'result': {'syntheticSlotGuard': True}})
            stdout, stderr = process.communicate(timeout=75)
            if process.returncode:
                raise RuntimeError('actual adapter workflow rejected: ' + stderr[-500:])
            receipt = json.loads(stdout.strip())
            jobs = [j for j in queue.list() if j['profile'] == 'odm-viewer-v1']
            assert [j['nodeId'] for j in jobs] == [None, 'cluster', 'cluster', 'main']
            assert sum(e.commits for e in engines) == 3
            assert all(e.maximum_running == 1 for e in engines)
            completed = sorted((task for engine in engines for task in engine.tasks.values()
                                if task['code'] == 40), key=lambda task: task['end'])
            assert [task['name'] for task in completed] == ['production-small', 'production-large', 'staging-small']
            with queue._db() as db:
                rows = db.execute('SELECT uuid,archive_path FROM viewer_tasks WHERE archive_path IS NOT NULL').fetchall()
                assert len(rows) == 1 and Path(rows[0]['archive_path']).is_file(), 'download was deleted on remove'
            receipt.update(smallNodeFirst=True, globalTreeDarExclusion=True,
                           localCommits=engines[0].commits, mainCommits=engines[1].commits,
                           adapterSha256=hashlib.sha256(adapter.read_bytes()).hexdigest(),
                           executionOrder=[task['name'] for task in completed],
                           syntheticOnly=True, realViewerIngestionValidated=False)
            print(json.dumps(receipt, sort_keys=True))
    except Exception as error:
        print('Synthetic adapter acceptance failed: ' + str(error), file=sys.stderr)
        return 1
    finally:
        if process and process.poll() is None:
            process.kill();process.wait()
        if service:
            service.close()
        for server in reversed(servers):
            server.shutdown();server.server_close()
        if queue:
            queue.close()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
