'use strict';
// Synthetic-only, run inside network-none container with owned data volume.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {ReferenceProvider}=require('/viewer/referenceProvider');
const {ReferenceAcceptanceLedger}=require('/viewer/referenceAcceptance');
const fixture=require('/scope/contracts/reference-v1-acceptance-sender.golden.json');
const mode=process.argv[2],port=Number(process.argv[3]),root='/owned/paired-state';
if(!['first','retry'].includes(mode)||!Number.isInteger(port)||port<1||port>65535)throw Error('fixture arguments refused');
const expected={...fixture.expected,objectIds:fixture.resultManifest.objects.map(o=>o.id)};
if(mode==='first'){
  fs.mkdirSync(root,{mode:0o700});fs.mkdirSync(path.join(root,'models'),{mode:0o700});
  fs.writeFileSync(path.join(root,'models','synthetic.bin'),Buffer.from('abc'),{flag:'wx',mode:0o600});
}
const ledger=new ReferenceAcceptanceLedger({databasePath:path.join(root,'ledger.sqlite'),managedRoot:path.join(root,'models')});
const origin='https://fixture.invalid',allowed=new Set([`/ltds/v1/jobs/${expected.jobId}/results`,`/ltds/v1/jobs/${expected.jobId}/acceptance`]);
const provider=new ReferenceProvider({endpoint:origin,token:'synthetic-viewer-token-only-00001',environment:'staging',enabled:true,
  fetchImpl:async(url,init)=>{const u=new URL(url);if(u.origin!==origin||!allowed.has(u.pathname)||u.search||u.hash)throw Error('fixture route refused');return fetch(`http://127.0.0.1:${port}${u.pathname}`,{...init,redirect:'error'});}});
(async()=>{
  if(mode==='first'){
    ledger.accept({expected,resultManifest:fixture.resultManifest,acceptance:fixture.acceptance,
      objects:[{id:'archive-1',relativePath:'synthetic.bin'}],reconstruction:{syntheticFixture:true}});
    assert.equal(ledger.pendingOutbox().length,1);
  }
  await provider.results(expected.jobId,expected);
  const pending=ledger.pendingOutbox();assert.equal(pending.length,1);
  assert.deepEqual(pending[0].payload,fixture.acceptance);
  if(mode==='first'){
    await assert.rejects(provider.acceptResult(expected.jobId,pending[0].payload,expected),{code:'UNCERTAIN'});
    assert.equal(ledger.pendingOutbox().length,1);
    process.stdout.write('SYNTHETIC_PARTIAL_RESPONSE_UNCERTAIN\n');
    process.exit(86); // Deliberate process exit; no DB close or simulated power loss.
  }
  const result=await provider.acceptResult(expected.jobId,pending[0].payload,expected);
  assert.equal(result.created,false);assert.equal(result.fullyAccepted,true);
  assert.equal(result.receiptId,fixture.acceptance.receiptId);assert.equal(result.receiptSha256,fixture.acceptanceSha256);
  ledger.markAcked(pending[0].receiptId,pending[0].payload);
  assert.equal(ledger.pendingOutbox().length,0);ledger.close();
  process.stdout.write('SYNTHETIC_REOPEN_EXACT_REPLAY_ACKED\n');
})().catch(e=>{process.stderr.write(String(e.code||e.name||'fixture failure'));try{ledger.close();}catch{}process.exitCode=1;});
