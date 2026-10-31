'use strict';
// Real Viewer submit/reconcile/ingest entry points; synthetic inputs only.
const assert=require('node:assert/strict'),crypto=require('node:crypto'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const base=`${process.env.VIEWER_SOURCE_ROOT||'/viewer'}/apps/viewer/server`;
const {openDatabase}=require(`${base}/database`),{ViewerRepository}=require(`${base}/repository`),{ProcessingRepository}=require(`${base}/processingRepository`);
const {StorageManager}=require(`${base}/storageManager`),{processSubmit,processReconcile,processIngest}=require(`${base}/processingWorker`);
const {NodeOdmProvider}=require(`${base}/nodeOdmProvider`),{NO_AUTH_MARKER}=require(`${base}/providerAuth`);
const {ProviderCredentials}=require(`${base}/providerCredentials`),{spawnSync}=require('node:child_process');
const sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function main(){
  const bridge=JSON.parse(fs.readFileSync(process.env.BRIDGE_READY,'utf8'));
  const root=fs.mkdtempSync(path.join(os.tmpdir(),'viewer-full-processing-bridge-')),database=openDatabase(path.join(root,'viewer.sqlite'));
  try{
    const processing=new ProcessingRepository(database),repository=new ViewerRepository(database);
    const config=Object.fromEntries(['datasets','models','cache','trash'].map(key=>{const value=path.join(root,key);fs.mkdirSync(value);return [`${key}Mount`,value]}));
    Object.assign(config,{storageReserveBytes:0,storageReservePercent:0,meshDerivativesEnabled:true,processingProviderTransferTimeoutMs:5000,opsBaseUrl:'http://operations.test'});
    const storage=new StorageManager(config);storage.initialize();
    const project=processing.createProject({displayName:'Synthetic full bridge'}),dataset=processing.createDataset({projectId:project.id,displayName:'Synthetic photos',storageMode:'managed',rootKey:'datasets',relativePath:'source'});
    fs.mkdirSync(path.join(config.datasetsMount,'source'));const photo=Buffer.from('synthetic');fs.writeFileSync(path.join(config.datasetsMount,'source','photo.jpg'),photo);
    const hash=crypto.createHash('sha256').update(photo).digest('hex');
    processing.finalizeDataset(dataset.id,[{id:'synthetic-photo',relativePath:'photo.jpg',byteSize:photo.length,sha256:hash,contentType:'image/jpeg',metadata:{width:100,height:100}}],hash);
    const provider=processing.upsertProvider({authEnvKey:NO_AUTH_MARKER,type:'nodeodm',displayName:'Synthetic Processing',endpoint:`http://127.0.0.1:${bridge.port}`,enabled:true,capabilities:{reservations:{version:1}}});
    const task=processing.createTask({projectId:project.id,datasetId:dataset.id,displayName:'Synthetic full bridge task'});
    const attempt=processing.createAttempt({taskId:task.id,providerId:provider.id,providerTaskId:bridge.task,options:{dsm:true},createdBy:'fixture'});
    const sealed=new ProviderCredentials({processing,activeKeyId:'fixture',keys:{fixture:Buffer.alloc(32,3).toString('base64')}});
    const credentials={resolve:()=>bridge.token,sealAttemptCapability:sealed.sealAttemptCapability.bind(sealed),openAttemptCapability:sealed.openAttemptCapability.bind(sealed),resolveStagedCommitmentCredential:async()=>bridge.bearer};
    config.stagedCommitmentProviders={[provider.id]:{origin:`https://127.0.0.1:${bridge.ackPort}`,principalId:'viewer-stage',authorityId:bridge.authority,environment:'staging',credentialRef:'synthetic'}};
    const deps={processing,repository,storage,config,providerCredentials:credentials,signal:new AbortController().signal};
    const submit=processing.claimJob('fixture');assert.equal(submit.job_type,'submit');await processSubmit(submit,deps);
    assert.equal(processing.getAttempt(attempt.id).status,'queued_upstream');
    const reservation=processing.getAttemptReservation(attempt.id);assert.ok(reservation);
    const capability=sealed.openAttemptCapability(provider.id,attempt.id,bridge.task,reservation);
    const adapter=new NodeOdmProvider({endpoint:provider.endpoint,token:bridge.token,providerType:'nodeodm',attemptCapability:capability,transferTimeoutMs:5000});
    let status=null;const deadline=Date.now()+45000;
    while(Date.now()<deadline){status=await adapter.status(bridge.task);if(status.status==='completed')break;await sleep(100);}
    assert.equal(status?.status,'completed');assert.equal(status.imagesCount,1);
    let reconcile=null;
    while(Date.now()<deadline){reconcile=processing.claimJob('fixture');if(reconcile)break;await sleep(100);}
    assert.equal(reconcile?.job_type,'reconcile');await processReconcile(reconcile,deps);
    const ingest=processing.claimJob('fixture');assert.equal(ingest?.job_type,'ingest');await processIngest(ingest,deps);
    const provenance=processing.getAttemptTransferProvenance(attempt.id);assert.equal(provenance.archive.archiveSha256,bridge.archiveSha256);
    assert.equal(processing.getAttempt(attempt.id).status,'derivatives');
    const producer=processing.getVerifiedAttemptProducerReceipt(attempt.id);
    assert.equal(producer?.receipt?.providerTaskId,bridge.task);assert.equal(producer?.receipt?.archiveSha256,bridge.archiveSha256);
    assert.equal(producer?.inputFiles?.[0]?.sha256,hash);
    const pending=database.prepare("SELECT COUNT(*) AS n FROM derivative_jobs WHERE attempt_id=? AND status='pending'").get(attempt.id).n;assert.ok(pending>0);
    assert.equal(crypto.createHash('sha256').update(fs.readFileSync(path.join(config.datasetsMount,'source','photo.jpg'))).digest('hex'),hash);
    const row=database.prepare('SELECT assertion_json FROM processing_staged_byte_ack_outbox WHERE attempt_id=?').get(attempt.id);assert.ok(row);
    const ackConfig={origin:`https://127.0.0.1:${bridge.ackPort}`,authority:bridge.authority,bearer:bridge.bearer,capability,task:bridge.task,attempt:attempt.id,providerId:provider.id};
    const ack=spawnSync(process.execPath,['/processing/tests/viewer_staged_clients_synthetic.cjs','existing'],{env:{...process.env,VIEWER_SOURCE:process.env.VIEWER_SOURCE_ROOT||'/viewer',SYNTHETIC_BRIDGE_CONFIG:JSON.stringify(ackConfig),SYNTHETIC_DB_PATH:path.join(root,'viewer.sqlite')},encoding:'utf8',timeout:30000});
    assert.equal(ack.status,0,ack.stderr);const ackResult=JSON.parse(ack.stdout);assert.equal(ackResult.replay,'lookup');
    assert.equal(database.prepare('SELECT assertion_json FROM processing_staged_byte_ack_outbox WHERE attempt_id=?').get(attempt.id).assertion_json,row.assertion_json);
    console.log(JSON.stringify({result:'synthetic_full_submit_and_ingest',providerTaskId:bridge.task,sourceSha256:hash,archiveSha256:provenance.archive.archiveSha256,
      viewerAttemptStatus:processing.getAttempt(attempt.id).status,pendingDerivatives:pending,producerProvenanceRecorded:true,stagedAckReplay:'lookup',viewerCustodyAcknowledged:false}));
  }finally{database.close();fs.rmSync(root,{recursive:true,force:true});}
}
main().catch(error=>{console.error(JSON.stringify({code:error.code||'error',message:error.message}));process.exitCode=1;});
