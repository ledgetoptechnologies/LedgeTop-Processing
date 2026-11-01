'use strict';
// Entirely synthetic: real Viewer candidate storage/held outbox and HTTPS sender.
// The SQLite database and staged files remain in the retained container.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const assert=require('node:assert/strict'),{DatabaseSync}=require('node:sqlite');
const root='/tmp/acceptance-rehearsal',viewer='/viewer/apps/viewer/server';
const sha=x=>crypto.createHash('sha256').update(x).digest('hex');
const fixture=JSON.parse(fs.readFileSync('/processing/contracts/reference-v1.contract.json','utf8'));
const receipts=require(path.join(viewer,'nativeReferenceReceiptCandidate'));
const {stageAndHold}=require(path.join(viewer,'nativeReferenceConsumerCandidate'));
const {sendHeld}=require(path.join(viewer,'nativeReferenceAcceptanceCandidate'));

async function main(){
  fs.mkdirSync(root,{mode:0o700});fs.mkdirSync(path.join(root,'results'),{mode:0o700});
  const db=new DatabaseSync(path.join(root,'viewer.sqlite'));
  try {
    db.exec('CREATE TABLE processing_tasks (id TEXT PRIMARY KEY, active_attempt_id TEXT NOT NULL); CREATE TABLE processing_attempts (id TEXT PRIMARY KEY, task_id TEXT NOT NULL)');
    const attempt=crypto.randomUUID();db.prepare('INSERT INTO processing_tasks VALUES (?,?)').run('task-1',attempt);
    db.prepare('INSERT INTO processing_attempts VALUES (?,?)').run(attempt,'task-1');
    console.log('STAGE install');receipts.install(db,true);
    const r=fixture.acceptance, manifest=fixture.resultManifest;
    console.log('STAGE bind');receipts.bind(db,{attemptId:attempt,bindingId:crypto.randomUUID(),jobId:r.jobId,generation:r.generation,
      environment:r.environment,inputManifestSha256:r.inputManifestSha256,pipelineDigest:r.pipelineDigest},{authorizeBinding:()=>true});
    const target=path.join(root,'results');
    console.log('STAGE capacity');receipts.configureCapacity(db,{rootDigest:sha(Buffer.from(target)),limitBytes:10485760},{authorizeCapacity:()=>true});
    const storageReceiptId=crypto.randomUUID(),receiptId=crypto.randomUUID();
    console.log('STAGE hold');await stageAndHold({db,attemptId:attempt,root:target,storageReceiptId,receiptId,manifest,
      openObject:async object=>{const bytes=object.id==='result-1'?Buffer.from('abc'):Buffer.alloc(0);
        return {status:200,headers:new Headers({'content-length':String(bytes.length),
          'x-result-sha256':object.sha256,'x-result-manifest-sha256':manifest.resultManifestSha256}),
          source:(async function*(){yield bytes;})(),close:async()=>{}};},
      maxBytes:1024,acceptedAt:'2026-11-01T00:00:00.000Z',authorizeBinding:()=>true,authorizeReceipt:()=>true,enabled:true});
    const dir='/viewer/apps/viewer/test/fixtures/readiness-tls',ca=fs.readFileSync(path.join(dir,'synthetic-only.crt'));
    const pin=sha(new crypto.X509Certificate(ca).raw);
    const options={db,attemptId:attempt,authorizeBinding:()=>true,authorizeSend:()=>true,port:17864,
      serverName:'viewer-test.invalid',ca,pin,token:'synthetic-viewer-token',timeoutMs:3000,maxAttempts:1,syntheticOnly:true,enabled:true};
    console.log('STAGE send');const first=await sendHeld(options), second=await sendHeld(options);
    assert.equal(first.observation,'matching_receipt_response');assert.equal(second.observation,'matching_receipt_response');
    assert.equal(first.state,'held');assert.equal(second.state,'held');
    assert.equal(db.prepare('SELECT state FROM native_reference_outbox_candidate WHERE attempt_id=?').get(attempt).state,'held');
    console.log('VIEWER_SEND_AND_REPLAY_OK held=true synthetic-only');
  } finally {db.close();}
}
main().catch(error=>{console.error('SYNTHETIC_REHEARSAL_FAILED',error.stack||error.code||error.message);process.exitCode=1;});
