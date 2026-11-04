'use strict';
// Isolated fake-engine verification only. Never points at a live provider.
const fs=require('node:fs'),{spawn,spawnSync}=require('node:child_process'),readline=require('node:readline'),assert=require('node:assert/strict');
const {once}=require('node:events');
async function main(){
  fs.mkdirSync('/pilot/source');
  for(let i=0;i<25;i++)fs.writeFileSync(`/pilot/source/${String(i).padStart(3,'0')}.jpg`,`invented-photo-${i}`);
  const prep=spawnSync('python3',['tests/prepare_pilot_input.py','/pilot/source','/pilot/run'],{encoding:'utf8'});
  if(prep.status!==0)throw new Error('fake_prepare_failed');
  const server=spawn('python3',['-u','tests/legacy_viewer_pair_server.py','/tmp/pilot-coordinator','result'],{stdio:['pipe','pipe','inherit'],env:{...process.env,PYTHONPATH:'/processing'}});
  const lines=readline.createInterface({input:server.stdout})[Symbol.asyncIterator](),serverExit=once(server,'exit');
  let worker,workerExit;
  try{
    const first=await lines.next();if(first.done)throw new Error('fake_server_unavailable');
    const {port}=JSON.parse(first.value);
    worker=spawn(process.execPath,['tests/real_viewer_pilot.js'],{stdio:['pipe','pipe','inherit'],env:{...process.env,VIEWER_SOURCE:'/tmp/deps',PILOT_MODE:'fake',PILOT_ROOT:'/pilot/run',PILOT_REGISTRY:'/pilot-registry',PILOT_ENDPOINT:`http://127.0.0.1:${port}`,PILOT_DEADLINE_SECONDS:'120'}});
    workerExit=once(worker,'exit');worker.stdin.end('synthetic-viewer-'+'b'.repeat(32)+'\n');
    const events=readline.createInterface({input:worker.stdout})[Symbol.asyncIterator]();
    let id,finished=false,ready=false;
    for(;;){
      const next=await events.next();if(next.done)break;
      const e=JSON.parse(next.value);
      if(e.event==='created')id=e.providerTaskId;
      if(e.event==='status'&&e.submission==='committed'&&!finished){
        assert.ok(id);
        let state;
        for(let i=0;i<100;i++){
          server.stdin.write(JSON.stringify({op:'inspect',uuid:id})+'\n');
          state=JSON.parse((await lines.next()).value);
          if(state.phase==='processing')break;
          await new Promise(r=>setTimeout(r,50));
        }
        assert.equal(state.phase,'processing');
        server.stdin.write(JSON.stringify({op:'finish',uuid:id})+'\n');
        assert.equal(JSON.parse((await lines.next()).value).finished,true);finished=true;
      }
      if(e.event==='ready_for_review'){ready=true;assert.ok(e.files>0);}
    }
    const [code,signal]=await workerExit;
    assert.equal(signal,null);assert.equal(code,0);assert.equal(finished,true);assert.equal(ready,true);
    assert.ok(fs.existsSync('/pilot/run/pilot-complete.json'));
    // Re-entering the same owned root must never initialize a second task.
    const again=spawn(process.execPath,['tests/real_viewer_pilot.js'],{stdio:['pipe','pipe','pipe'],env:{...process.env,VIEWER_SOURCE:'/tmp/deps',PILOT_MODE:'fake',PILOT_ROOT:'/pilot/run',PILOT_REGISTRY:'/pilot-registry',PILOT_ENDPOINT:`http://127.0.0.1:${port}`,PILOT_DEADLINE_SECONDS:'120'}});
    let repeatedOutput='',repeatedError='';again.stdout.on('data',v=>{repeatedOutput+=v});again.stderr.on('data',v=>{repeatedError+=v});
    const againExit=once(again,'exit');again.stdin.end('synthetic-viewer-'+'b'.repeat(32)+'\n');
    const [againCode,againSignal]=await againExit;
    assert.equal(againSignal,null);assert.equal(againCode,1);assert.equal(repeatedOutput,'');assert.match(repeatedError,/pilot_already_started/);
    const secondPrep=spawnSync('python3',['tests/prepare_pilot_input.py','/pilot/source','/pilot/second'],{encoding:'utf8'});
    assert.equal(secondPrep.status,0);
    const second=spawn(process.execPath,['tests/real_viewer_pilot.js'],{stdio:['pipe','pipe','pipe'],env:{...process.env,VIEWER_SOURCE:'/tmp/deps',PILOT_MODE:'fake',PILOT_ROOT:'/pilot/second',PILOT_REGISTRY:'/pilot-registry',PILOT_ENDPOINT:`http://127.0.0.1:${port}`,PILOT_DEADLINE_SECONDS:'120'}});
    let secondOutput='',secondError='';second.stdout.on('data',v=>{secondOutput+=v});second.stderr.on('data',v=>{secondError+=v});
    const secondExit=once(second,'exit');second.stdin.end('synthetic-viewer-'+'b'.repeat(32)+'\n');
    const [secondCode,secondSignal]=await secondExit;
    assert.equal(secondSignal,null);assert.equal(secondCode,1);assert.equal(secondOutput,'');assert.match(secondError,/EEXIST/);
    console.log(JSON.stringify({fakePilot:'passed',inputCount:25,retained:true}));
  }finally{
    if(worker&&worker.exitCode===null&&worker.signalCode===null){worker.kill();await workerExit;}
    server.stdin.end(JSON.stringify({op:'stop'})+'\n');
    const [code,signal]=await serverExit;if(code!==0||signal)throw new Error('fake_server_cleanup_failed');
  }
}
main().catch(e=>{console.error('fake pilot failed:',e.message);process.exitCode=1;});
