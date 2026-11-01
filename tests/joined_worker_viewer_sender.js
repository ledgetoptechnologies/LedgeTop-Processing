'use strict';
// UNREVIEWED. Synthetic-only Viewer candidate consumer over actual result HTTP.
const fs=require('node:fs'),path=require('node:path'),https=require('node:https'),crypto=require('node:crypto');
const assert=require('node:assert/strict'),{DatabaseSync}=require('node:sqlite');
const root=process.env.JOINED_ROOT,viewer='/viewer/apps/viewer/server';
const receipts=require(path.join(viewer,'nativeReferenceReceiptCandidate'));
const {stageAndHold}=require(path.join(viewer,'nativeReferenceConsumerCandidate'));
const {sendBound}=require(path.join(viewer,'nativeReferenceBoundSenderCandidate'));
const sha=x=>crypto.createHash('sha256').update(x).digest('hex');

async function main(){
  const ready=JSON.parse(fs.readFileSync(path.join(root,'viewer-ready.json'),'utf8'));
  const ca=fs.readFileSync(path.join(root,'cert.pem')),pin=sha(new crypto.X509Certificate(ca).raw);
  const resultRoot=path.join(root,'viewer-results');fs.mkdirSync(resultRoot,{mode:0o700});
  const db=new DatabaseSync(path.join(root,'viewer-candidate.sqlite'));
  try{
    db.exec('CREATE TABLE processing_tasks (id TEXT PRIMARY KEY, active_attempt_id TEXT NOT NULL); CREATE TABLE processing_attempts (id TEXT PRIMARY KEY, task_id TEXT NOT NULL, provider_id TEXT NOT NULL)');
    const attemptId=crypto.randomUUID();db.prepare('INSERT INTO processing_tasks VALUES (?,?)').run('task-1',attemptId);
    db.prepare('INSERT INTO processing_attempts VALUES (?,?,?)').run(attemptId,'task-1','synthetic-provider');
    receipts.install(db,true);receipts.attest(db);
    const payload=ready.payload,manifest=payload.resultManifest;
    receipts.bind(db,{attemptId,bindingId:crypto.randomUUID(),jobId:payload.jobId,generation:payload.generation,
      environment:payload.environment,inputManifestSha256:payload.inputManifestSha256,pipelineDigest:payload.pipelineDigest},{authorizeBinding:()=>true});
    receipts.configureCapacity(db,{rootDigest:sha(Buffer.from(resultRoot)),limitBytes:10485760},{authorizeCapacity:()=>true});
    const openObject=async object=>new Promise((resolve,reject)=>{
      const request=https.request({hostname:'127.0.0.1',port:ready.port,
        path:`/ltds/v1/jobs/${encodeURIComponent(payload.jobId)}/results/${encodeURIComponent(object.id)}`,
        method:'GET',ca,rejectUnauthorized:true,agent:false,headers:{authorization:`Bearer ${ready.viewerToken}`}},response=>{
          const headers=new Headers();for(const [name,value] of Object.entries(response.headers))if(value!==undefined)headers.set(name,String(value));
          resolve({status:response.statusCode,headers,source:response,close:async()=>{response.destroy();}});
      });
      request.setTimeout(5000,()=>request.destroy(new Error('synthetic result timeout')));
      request.on('error',reject);request.end();
    });
    const stage=await stageAndHold({db,attemptId,root:resultRoot,storageReceiptId:crypto.randomUUID(),receiptId:crypto.randomUUID(),
      manifest,openObject,maxBytes:1048576,acceptedAt:'2026-11-01T00:00:00.000Z',authorizeBinding:()=>true,authorizeReceipt:()=>true,enabled:true});
    assert.equal(stage.state,'held');
    const held=receipts.readHeld(db,attemptId,{authorizeBinding:()=>true,authorizeSend:()=>true});
    const result=await sendBound({db,attemptId,authorizeBinding:()=>true,authorizeSend:()=>true,
      loadConfiguration:async()=>({environment:'staging',origin:`https://127.0.0.1:${ready.port}`,secretRef:'synthetic/credential',revision:1}),
      authorizeConfiguration:()=>true,resolveSecret:async()=>({revision:1,token:ready.viewerToken}),ca,pin,
      timeoutMs:3000,maxAttempts:1,syntheticOnly:true,enabled:true});
    assert.equal(result.observation,'matching_receipt_response');assert.equal(result.state,'held');
    const output={receipt:JSON.parse(held.payload),payloadSha256:sha(held.payload),state:result.state,observation:result.observation};
    const tmp=path.join(root,'viewer-complete.tmp');fs.writeFileSync(tmp,JSON.stringify(output),{mode:0o600});
    fs.renameSync(tmp,path.join(root,'viewer-complete.json'));
    console.log('VIEWER_ACTUAL_PROXY_STAGED_AND_SENT synthetic-only');
  }finally{db.close();}
}
main().catch(error=>{console.error('JOINED_VIEWER_FAILED',error.stack||error.message);process.exitCode=1;});
