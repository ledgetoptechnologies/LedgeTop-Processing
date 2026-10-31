'use strict';
// Synthetic rehearsal only: ZIP entries below are placeholders, not a model.
const assert=require('node:assert/strict');
const crypto=require('node:crypto');
const fs=require('node:fs');
const os=require('node:os');
const path=require('node:path');
const base=`${process.env.VIEWER_SOURCE_ROOT||'/viewer'}/apps/viewer/server`;
const {openDatabase}=require(`${base}/database`);
const {ViewerRepository}=require(`${base}/repository`);
const {ProcessingRepository}=require(`${base}/processingRepository`);
const {StorageManager}=require(`${base}/storageManager`);
const {processReconcile,processIngest}=require(`${base}/processingWorker`);
const {NO_AUTH_MARKER}=require(`${base}/providerAuth`);

async function main(){
  const bridge=JSON.parse(fs.readFileSync(process.env.BRIDGE_READY,'utf8'));
  const root=fs.mkdtempSync(path.join(os.tmpdir(),'viewer-processing-bridge-'));
  const database=openDatabase(path.join(root,'viewer.sqlite'));
  try {
    const processing=new ProcessingRepository(database),repository=new ViewerRepository(database);
    const config=Object.fromEntries(['datasets','models','cache','trash'].map(key=>{
      const value=path.join(root,key);fs.mkdirSync(value);return [`${key}Mount`,value];
    }));
    Object.assign(config,{storageReserveBytes:0,storageReservePercent:0,meshDerivativesEnabled:true,
      processingProviderTransferTimeoutMs:5000,opsBaseUrl:'http://operations.test'});
    const storage=new StorageManager(config);storage.initialize();
    const project=processing.createProject({displayName:'Synthetic bridge'});
    const dataset=processing.createDataset({projectId:project.id,displayName:'Synthetic photos',storageMode:'managed',rootKey:'datasets',relativePath:'source'});
    fs.mkdirSync(path.join(config.datasetsMount,'source'));
    const photo=Buffer.from('synthetic');fs.writeFileSync(path.join(config.datasetsMount,'source','photo.jpg'),photo);
    const hash=crypto.createHash('sha256').update(photo).digest('hex');
    processing.finalizeDataset(dataset.id,[{id:'synthetic-photo',relativePath:'photo.jpg',byteSize:photo.length,sha256:hash,contentType:'image/jpeg',metadata:{width:100,height:100}}],hash);
    const provider=processing.upsertProvider({authEnvKey:NO_AUTH_MARKER,type:'nodeodm',displayName:'Synthetic Processing',endpoint:`http://127.0.0.1:${bridge.port}`,enabled:true});
    const task=processing.createTask({projectId:project.id,datasetId:dataset.id,displayName:'Synthetic bridge task'});
    const attempt=processing.createAttempt({taskId:task.id,providerId:provider.id,providerTaskId:bridge.task,options:{},createdBy:'fixture'});
    const submit=processing.claimJob('fixture');assert.equal(processing.completeJob(submit.id,'fixture'),true);
    processing.transitionAttempt(attempt.id,'queued_upstream');processing.enqueueJob(attempt.id,'reconcile');
    const credentials={resolve:()=>bridge.token};const deps={processing,repository,storage,config,providerCredentials:credentials,signal:new AbortController().signal};
    const reconcile=processing.claimJob('fixture');assert.equal(reconcile.job_type,'reconcile');
    await processReconcile(reconcile,deps);
    const ingest=processing.claimJob('fixture');assert.equal(ingest.job_type,'ingest');
    await processIngest(ingest,deps);
    const provenance=processing.getAttemptTransferProvenance(attempt.id);
    assert.equal(provenance.archive.archiveSha256,bridge.archiveSha256);
    assert.equal(processing.getAttempt(attempt.id).status,'derivatives');
    assert.equal(processing.getVerifiedAttemptProducerReceipt(attempt.id),null);
    const pending=database.prepare("SELECT COUNT(*) AS n FROM derivative_jobs WHERE attempt_id=? AND status='pending'").get(attempt.id).n;
    assert.ok(pending>0);
    console.log(JSON.stringify({result:'synthetic_ingest_complete',providerTaskId:bridge.task,archiveSha256:provenance.archive.archiveSha256,
      viewerAttemptStatus:processing.getAttempt(attempt.id).status,pendingDerivatives:pending,viewerCustodyAcknowledged:false}));
  } finally {database.close();fs.rmSync(root,{recursive:true,force:true});}
}
main().catch(error=>{console.error(JSON.stringify({code:error.code||'error',message:error.message}));process.exitCode=1;});
