'use strict';
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const root=process.env.VIEWER_SOURCE+'/apps/viewer/server',c=JSON.parse(process.env.SYNTHETIC_BRIDGE_CONFIG),work=fs.mkdtempSync(path.join(process.env.SYNTHETIC_INGEST_VOLUME,'run-'));
const {openDatabase}=require(root+'/database'),{ProcessingRepository}=require(root+'/processingRepository'),{ViewerRepository}=require(root+'/repository'),{StorageManager}=require(root+'/storageManager'),{ProviderCredentials}=require(root+'/providerCredentials'),{processIngest}=require(root+'/processingWorker');
(async()=>{
 const db=openDatabase(path.join(work,'viewer.sqlite'));try{
 const processing=new ProcessingRepository(db),repository=new ViewerRepository(db),config={};for(const key of ['datasets','models','cache','trash']){config[key+'Mount']=path.join(work,key);fs.mkdirSync(config[key+'Mount'],{recursive:true});}
 Object.assign(config,{storageReserveBytes:0,storageReservePercent:0,meshDerivativesEnabled:true,processingProviderTransferTimeoutMs:5000,opsBaseUrl:'http://ops.invalid'});
 const storage=new StorageManager(config);storage.initialize();const project=processing.createProject({displayName:'synthetic'}),dataset=processing.createDataset({projectId:project.id,displayName:'synthetic',rootKey:'datasets',relativePath:'source'});fs.mkdirSync(path.join(config.datasetsMount,'source'));fs.writeFileSync(path.join(config.datasetsMount,'source','a.jpg'),'a');const digest=crypto.createHash('sha256').update('a').digest('hex');processing.finalizeDataset(dataset.id,[{relativePath:'a.jpg',byteSize:1,sha256:digest}],digest);
 const provider=processing.upsertProvider({type:'nodeodm',displayName:'synthetic',endpoint:c.viewerOrigin,authEnvKey:require(root+'/providerAuth').NO_AUTH_MARKER,enabled:true});const task=processing.createTask({projectId:project.id,datasetId:dataset.id,displayName:'synthetic'}),attempt=processing.createAttempt({taskId:task.id,providerId:provider.id,providerTaskId:c.task,options:{},createdBy:'fixture'});
 const sealed=new ProviderCredentials({processing,activeKeyId:'fixture',keys:{fixture:Buffer.alloc(32,3).toString('base64')}}),job0=processing.claimJob('setup');
 const reservation=processing.ensureAttemptReservation(job0.id,'setup',(providerId,attemptId,taskId)=>sealed.sealAttemptCapability(providerId,attemptId,taskId,c.capability));if(!reservation)throw Error('reservation unavailable');
 db.prepare('UPDATE processing_attempts SET reservation_required=1 WHERE id=?').run(attempt.id);processing.completeJob(job0.id,'setup');processing.transitionAttempt(attempt.id,'ingesting');processing.enqueueJob(attempt.id,'ingest');const job=processing.claimJob('worker');
 config.stagedCommitmentProviders={[provider.id]:{origin:c.origin,principalId:'viewer-stage',authorityId:c.authority,environment:'staging',credentialRef:'synthetic'}};
 const providerCredentials={resolve:()=>c.viewerToken,openAttemptCapability:sealed.openAttemptCapability.bind(sealed),resolveStagedCommitmentCredential:async()=>c.bearer};
 const {NodeOdmProvider}=require(root+'/nodeOdmProvider');const probe=new NodeOdmProvider({endpoint:c.viewerOrigin,token:c.viewerToken,providerType:'nodeodm',attemptCapability:c.capability,transferTimeoutMs:5000});const caps=await probe.capabilities();if(caps.capabilities?.reservations?.version!==1)throw Error('real provider reservation discovery unavailable: '+JSON.stringify(caps.capabilities));
 await processIngest(job,{processing,repository,storage,config,providerCredentials,signal:new AbortController().signal});
 const row=db.prepare('SELECT assertion_json FROM processing_staged_byte_ack_outbox WHERE attempt_id=?').get(attempt.id);if(!row)throw Error('normal ingest did not write outbox');console.log(JSON.stringify({ok:true,attempt:attempt.id,providerId:provider.id,dbPath:path.join(work,'viewer.sqlite')}));
 }finally{db.close();}
})().catch(e=>{console.error(e.stack);process.exitCode=1});
