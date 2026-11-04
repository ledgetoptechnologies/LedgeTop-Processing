'use strict';
// Run only inside a disposable network-none container, under an outer watchdog.
const assert=require('node:assert/strict');
const {spawn}=require('node:child_process');
const readline=require('node:readline');
const {once}=require('node:events');

async function run(mode) {
  const child=spawn('python3',['-u','tests/legacy_viewer_pair_server.py',`/tmp/coordinator-${mode}`,mode],{stdio:['pipe','pipe','inherit'],env:{...process.env,PYTHONPATH:'/processing'}});
  const lines=readline.createInterface({input:child.stdout})[Symbol.asyncIterator]();
  const exited=once(child,'exit');
  try {
    const first=await lines.next(); if(first.done)throw new Error('server exited before readiness');
    const {port}=JSON.parse(first.value);
    const client=spawn(process.execPath,['tests/legacy_viewer_pair_client.js',`/tmp/viewer-${mode}`,String(port),mode],{stdio:['pipe','pipe','inherit'],env:{...process.env,VIEWER_SOURCE:'/tmp/deps'}});
    const clientExited=once(client,'exit');
    const clientLines=readline.createInterface({input:client.stdout})[Symbol.asyncIterator]();
    let result;
    try {
      let next=await clientLines.next(); if(next.done)throw new Error('client exited before event');
      result=JSON.parse(next.value);
      if(mode==='result'||mode==='result-retry') {
        assert.equal(result.event,'submitted');
        let state;
        for(let tries=0;tries<100;tries++) {
          child.stdin.write(JSON.stringify({op:'inspect',uuid:result.uuid})+'\n');
          const line=await lines.next(); if(line.done)throw new Error('server exited during dispatch');
          state=JSON.parse(line.value);
          if(state.phase==='processing') break;
          await new Promise(resolve=>setTimeout(resolve,50));
        }
        assert.equal(state.phase,'processing');
        child.stdin.write(JSON.stringify({op:'finish',uuid:result.uuid})+'\n');
        assert.equal(JSON.parse((await lines.next()).value).finished,true);
        // Wait until the real coordinator observes completion before Viewer reconciliation.
        for(let tries=0;tries<100;tries++) {
          child.stdin.write(JSON.stringify({op:'inspect',uuid:result.uuid})+'\n');
          const line=await lines.next(); if(line.done)throw new Error('server exited during completion');
          state=JSON.parse(line.value);
          if(state.stage==='succeeded') break;
          await new Promise(resolve=>setTimeout(resolve,50));
        }
        assert.equal(state.stage,'succeeded');
        if(mode==='result-retry')client.stdin.write('finished\n'); else client.stdin.end('finished\n');
        next=await clientLines.next(); if(next.done)throw new Error('client exited before ingestion');
        let ingested=JSON.parse(next.value);
        if(mode==='result-retry') {
          assert.equal(ingested.event,'download-interrupted');
          assert.equal(ingested.uuid,result.uuid);
          child.stdin.write(JSON.stringify({op:'inspect',uuid:result.uuid})+'\n');
          const interrupted=JSON.parse((await lines.next()).value);
          assert.equal(interrupted.intent_exists,true);
          assert.equal(interrupted.partial_bytes,7);
          assert.equal(interrupted.archive_sha256,null);
          client.stdin.end('retry\n');
          next=await clientLines.next(); if(next.done)throw new Error('client exited before retry');
          ingested=JSON.parse(next.value);
        }
        assert.equal(ingested.event,'ingested');
        assert.equal(ingested.uuid,result.uuid);
        const [code,signal]=await clientExited;
        if(code!==0||signal)throw new Error(`client failed: ${code}/${signal}`);
        child.stdin.write(JSON.stringify({op:'inspect',uuid:result.uuid})+'\n');
        const retained=JSON.parse((await lines.next()).value);
        assert.equal(retained.archive_sha256,ingested.receipt.archiveSha256);
        console.log(JSON.stringify({mode,attemptStatus:ingested.attemptStatus,receiptFiles:ingested.receipt.archiveFiles.length}));
        return;
      }
      if(mode==='partial'||mode==='overlap') {
        assert.equal(result.event,'interrupted');
        child.stdin.write(JSON.stringify({op:'inspect',uuid:result.uuid})+'\n');
        const partialLine=await lines.next(); if(partialLine.done)throw new Error('server exited before partial inspection');
        const partial=JSON.parse(partialLine.value);
        assert.deepEqual(partial.files,[{name:result.files[0].relativePath,sha256:result.files[0].sha256}]);
        assert.equal(partial.phase,'uploading');
        client.stdin.end('resume\n');
        next=await clientLines.next(); if(next.done)throw new Error('client exited before retry');
        result=JSON.parse(next.value);
      } else client.stdin.end();
      assert.equal(result.event,'complete');
      const [code,signal]=await clientExited;
      if(code!==0||signal)throw new Error(`client failed: ${code}/${signal}`);
    } finally {
      if(client.exitCode===null&&client.signalCode===null)client.kill();
      await clientExited;
    }
    child.stdin.write(JSON.stringify({op:'inspect',uuid:result.uuid})+'\n');
    const line=await lines.next(); if(line.done)throw new Error('server exited before inspection');
    const actual=JSON.parse(line.value);
    assert.deepEqual(actual.files.map(x=>[x.name,x.sha256]).sort(),result.files.map(x=>[x.relativePath,x.sha256]).sort());
    assert.ok(['queued','waiting_node','initialized','upload_pending','uploaded','commit_pending','processing'].includes(actual.phase),`unexpected coordinator phase ${actual.phase}`);
    assert.equal(result.dropped,mode==='lost');
    if(mode==='overlap') {
      assert.equal(actual.old_active,true,'old multipart handler must still be blocked after retry');
      child.stdin.write(JSON.stringify({op:'release'})+'\n');
      const released=await lines.next(); if(released.done)throw new Error('server exited before old request completed');
      assert.equal(JSON.parse(released.value).old_done,true);
      child.stdin.write(JSON.stringify({op:'inspect',uuid:result.uuid})+'\n');
      const afterLine=await lines.next(); if(afterLine.done)throw new Error('server exited before final inspection');
      const after=JSON.parse(afterLine.value);
      assert.deepEqual(after.files.map(x=>[x.name,x.sha256]).sort(),result.files.map(x=>[x.relativePath,x.sha256]).sort());
    }
    console.log(JSON.stringify({mode,files:actual.files.length,phase:actual.phase}));
  } finally {
    child.stdin.end(JSON.stringify({op:'stop'})+'\n');
    const [code,signal]=await exited;
    if(code!==0||signal)throw new Error(`server failed: ${code}/${signal}`);
  }
}
(async()=>{await run('result-retry');})().catch(error=>{console.error(error.stack);process.exitCode=1;});
