'use strict';
// Synthetic-only localhost bridge; never accepts endpoint or bearer input.
const assert=require('node:assert/strict');
const {ReferenceProvider}=require(process.argv[2]);
const fixture=require(process.argv[3]);
const port=Number(process.argv[4]);
if(!Number.isInteger(port)||port<1||port>65535) throw Error('invalid fixture port');
const origin='https://fixture.invalid', expected=fixture.expected, receipt=fixture.acceptance;
const allowed=new Set([`/ltds/v1/jobs/${expected.jobId}/results`,`/ltds/v1/jobs/${expected.jobId}/acceptance`]);
const bodies=[];let lost=false;
const provider=new ReferenceProvider({endpoint:origin,token:'synthetic-viewer-token-only-00001',environment:'staging',enabled:true,
  fetchImpl:async(url,init)=>{
    const parsed=new URL(url);if(parsed.origin!==origin||!allowed.has(parsed.pathname)||parsed.search||parsed.hash)throw Error('fixture route refused');
    if(parsed.pathname.endsWith('/acceptance'))bodies.push(init.body);
    const response=await fetch(`http://127.0.0.1:${port}${parsed.pathname}`,{...init,redirect:'error'});
    // Discard a fully delivered, already committed response to simulate the
    // caller observing uncertainty. This is not a real network interruption.
    if(parsed.pathname.endsWith('/acceptance')&&!lost){lost=true;await response.arrayBuffer();throw Error('synthetic lost response');}
    return response;
  }});
(async()=>{
  await provider.results(expected.jobId,expected);
  await assert.rejects(provider.acceptResult(expected.jobId,receipt,expected),{code:'UNCERTAIN'});
  const replay=await provider.acceptResult(expected.jobId,receipt,expected);
  assert.equal(replay.created,false);assert.equal(replay.fullyAccepted,true);
  assert.equal(replay.receiptId,receipt.receiptId);assert.equal(replay.receiptSha256,fixture.acceptanceSha256);
  assert.deepEqual(replay.acceptedObjectIds,receipt.acceptedObjectIds);
  assert.equal(bodies.length,2);assert.equal(bodies[0],bodies[1]);
  process.stdout.write(JSON.stringify({replayed:true,requestBodiesIdentical:true}));
})().catch(e=>{process.stderr.write(String(e.code||e.name||'fixture failure'));process.exitCode=1;});
