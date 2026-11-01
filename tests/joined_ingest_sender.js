'use strict';
// UNREVIEWED. Synthetic-only Viewer candidate consumer over actual result HTTP.
const fs=require('node:fs'),path=require('node:path'),https=require('node:https'),crypto=require('node:crypto');
const assert=require('node:assert/strict');
const root=process.env.JOINED_ROOT,viewer='/viewer/apps/viewer/server';
const receipts=require(path.join(viewer,'nativeReferenceReceiptCandidate'));
const {stageAndHold}=require(path.join(viewer,'nativeReferenceConsumerCandidate'));
const {sendBound}=require(path.join(viewer,'nativeReferenceBoundSenderCandidate'));
const {openDatabase}=require(path.join(viewer,'database'));
const {ProcessingRepository}=require(path.join(viewer,'processingRepository'));
const {ViewerRepository}=require(path.join(viewer,'repository'));
const {StorageManager}=require(path.join(viewer,'storageManager'));
const {ingestHeld}=require(path.join(viewer,'nativeReferenceIngestCandidate'));
const sha=x=>crypto.createHash('sha256').update(x).digest('hex');

async function main(){
  const ready=JSON.parse(fs.readFileSync(path.join(root,'viewer-ready.json'),'utf8'));
  const ca=fs.readFileSync(path.join(root,'cert.pem')),pin=sha(new crypto.X509Certificate(ca).raw);
  const resultRoot=path.join(root,'viewer-results');fs.mkdirSync(resultRoot,{mode:0o700});
  const db=openDatabase(path.join(root,'viewer-candidate.sqlite'));
  try{
    const processing=new ProcessingRepository(db),repository=new ViewerRepository(db);
    const project=processing.createProject({displayName:'Synthetic'}),dataset=processing.createDataset({projectId:project.id,displayName:'Data',rootKey:'datasets',relativePath:'input'});
    processing.finalizeDataset(dataset.id,[{relativePath:'photo.jpg',byteSize:4,sha256:sha('jpeg'),processingRole:'image'}],sha('input'));
    const provider=processing.upsertProvider({type:'nodeodm',displayName:'Synthetic',endpoint:'http://127.0.0.1:1234',enabled:true});
    const task=processing.createTask({projectId:project.id,datasetId:dataset.id,displayName:'Synthetic'}),attempt=processing.createAttempt({taskId:task.id,providerId:provider.id});
    const attemptId=attempt.id,owner='fixture-owner',submit=processing.claimJob(owner);
    assert.equal(processing.setSubmissionStateForJob(submit.id,owner,'committed',1),true);
    assert.ok(processing.transitionAttemptForJob(submit.id,owner,'ingesting'));
    processing.completeJob(submit.id,owner);processing.enqueueJob(attemptId,'ingest');
    const job=processing.claimJob(owner);assert.equal(job.job_type,'ingest');
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
    const config={datasetsMount:path.join(root,'datasets'),modelsMount:process.env.JOINED_MODELS_ROOT,
      cacheMount:path.join(root,'cache'),trashMount:path.join(root,'trash'),storageReserveBytes:0,
      storageReservePercent:0,meshDerivativesEnabled:true,opsBaseUrl:'http://operations.test'};
    for(const key of ['datasetsMount','cacheMount','trashMount'])fs.mkdirSync(config[key],{mode:0o700});
    if(config.modelsMount!=='/synthetic-models')throw Error('synthetic volume required');
    fs.chmodSync(config.modelsMount,0o700);
    const capacity=fs.statfsSync(config.modelsMount,{bigint:true});
    console.log('SYNTHETIC_MODELS_STATFS',JSON.stringify({bsize:String(capacity.bsize),bavail:String(capacity.bavail),
      freeBytes:String(capacity.bsize*capacity.bavail),freeInodes:String(capacity.ffree),
      requestedBytes:String(32*1024*1024+64*1024*1024),requestedInodes:'1000000',floorInodes:'1024'}));
    const storage=new StorageManager(config);storage.initialize();
    console.log('SYNTHETIC_MODELS_RESERVE_BYTES',String(storage.space('models').reserve));
    const ingest=await ingestHeld({db,job,processing,repository,storage,config,root:resultRoot,
      maxBytes:8*1024*1024,maxDiskBytes:32*1024*1024,authorizeBinding:()=>true,authorizeIngest:()=>true,enabled:true});
    assert.equal(ingest.state,'ingested_without_producer_proof');
    assert.ok(db.prepare('SELECT 1 FROM native_reference_model_registration_candidate WHERE attempt_id=?').get(attemptId));
    const derivative=db.prepare("SELECT id,status FROM derivative_jobs WHERE attempt_id=? ORDER BY id LIMIT 1").get(attemptId);
    assert.ok(derivative);const derivativeId=derivative.id;
    assert.equal(processing.claimDerivative('synthetic-worker'),null);
    assert.equal(db.prepare('SELECT status FROM derivative_jobs WHERE id=?').get(derivativeId).status,'pending');
    const otherTask=processing.createTask({projectId:project.id,datasetId:dataset.id,displayName:'Unrelated synthetic task'});
    const otherAttempt=processing.createAttempt({taskId:otherTask.id,providerId:provider.id});
    const otherId=processing.enqueueDerivative(otherAttempt.id,'ept');
    assert.equal(processing.claimDerivative('synthetic-worker').id,otherId);
    console.log('REAL_VIEWER_INGEST_DERIVATIVE_HELD_UNRELATED_CLAIMED synthetic-only');
    const output={receipt:JSON.parse(held.payload),payloadSha256:sha(held.payload),state:result.state,observation:result.observation};
    const tmp=path.join(root,'viewer-complete.tmp');fs.writeFileSync(tmp,JSON.stringify(output),{mode:0o600});
    fs.renameSync(tmp,path.join(root,'viewer-complete.json'));
    console.log('VIEWER_ACTUAL_PROXY_STAGED_AND_SENT synthetic-only');
  }finally{db.close();}
}
main().catch(error=>{console.error('JOINED_VIEWER_FAILED',error.stack||error.message);process.exitCode=1;});
