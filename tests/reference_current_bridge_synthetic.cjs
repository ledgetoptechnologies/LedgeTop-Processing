'use strict';
// Synthetic-only cross-language fixture: actual staging importer, no ACK.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const root=process.env.VIEWER_STAGED_ROOT;
const p=require(path.join(root,'server/referenceProtocol'));
const {ReferenceProvider}=require(path.join(root,'server/referenceProvider'));
const {ReferenceResultJournal,validateResultBinding}=require(path.join(root,'server/referenceResultJournal'));
const {ReferenceAcceptanceLedger}=require(path.join(root,'server/referenceAcceptance'));
const {handleReferenceResult}=require(path.join(root,'server/referenceResultBridge'));
const {withConfiguredStagingCustody}=require(path.join(root,'server/referenceConfiguredStagingCustody'));
const {ingestStagingModelUnderAuthority,verifyStagingModelUnderAuthority}=require(path.join(root,'server/referenceStagingModelIngest'));
const {openDatabase}=require(path.join(root,'server/database'));
const {ViewerRepository}=require(path.join(root,'server/repository'));
const {ProcessingRepository}=require(path.join(root,'server/processingRepository'));
const {StorageManager}=require(path.join(root,'server/storageManager'));
const {NO_AUTH_MARKER}=require(path.join(root,'server/providerAuth'));
const c=JSON.parse(fs.readFileSync(0,'utf8'));
(async()=>{
 const base=path.join(c.custody,'synthetic-viewer');fs.mkdirSync(base);
 const config=Object.fromEntries(['datasets','models','cache','trash'].map(n=>{const d=path.join(base,n);fs.mkdirSync(d);return [`${n}Mount`,d]}));
 Object.assign(config,{production:false,referenceCustodyLabOnly:true,storageReserveBytes:0,storageReservePercent:0,meshDerivativesEnabled:true,processingProviderTransferTimeoutMs:5000,opsBaseUrl:'http://operations.test'});
 const db=openDatabase(path.join(base,'viewer.sqlite'));
 try {
  const processing=new ProcessingRepository(db),repository=new ViewerRepository(db),storage=new StorageManager(config);storage.initialize();
  const project=processing.createProject({displayName:'Synthetic project'}),dataset=processing.createDataset({projectId:project.id,displayName:'Synthetic photos',storageMode:'managed',rootKey:'datasets',relativePath:'source'});
  fs.mkdirSync(path.join(config.datasetsMount,'source'));const photo=Buffer.from('synthetic-photo'),hash=crypto.createHash('sha256').update(photo).digest('hex');fs.writeFileSync(path.join(config.datasetsMount,'source','photo.jpg'),photo);
  processing.finalizeDataset(dataset.id,[{id:'synthetic-photo',relativePath:'photo.jpg',byteSize:photo.length,sha256:hash,contentType:'image/jpeg'}],hash);
  const upstream=processing.upsertProvider({type:'nodeodm',displayName:'Synthetic provider',endpoint:'http://127.0.0.1:9',authEnvKey:NO_AUTH_MARKER,enabled:true});
  const task=processing.createTask({projectId:project.id,datasetId:dataset.id,displayName:'Synthetic task'}),attempt=processing.createAttempt({taskId:task.id,providerId:upstream.id,options:{},createdBy:'synthetic'}),submit=processing.claimJob('fixture');
  if(!processing.recordAttemptProviderResult(submit.id,'fixture',{uuid:attempt.providerTaskId,status:'completed',statusCode:40,imagesCount:1}))throw Error('setup');
  processing.completeJob(submit.id,'fixture');processing.transitionAttempt(attempt.id,'ingesting');processing.enqueueJob(attempt.id,'ingest');
  const job=processing.claimJob('fixture'),deps={processing,repository,storage,config,providerCredentials:{resolve:()=>''}};
  await withConfiguredStagingCustody(c.configPath,async authority=>{
   const journal=new ReferenceResultJournal({authority,maxBytes:1024*1024});
   const ledger=new ReferenceAcceptanceLedger({databasePath:path.join(c.custody,'acceptance.sqlite'),managedRoot:c.custody});
   try{
    let acceptancePosts=0;
    const transport=(url,init)=>{if(init.method==='POST'&&String(url).endsWith('/acceptance'))acceptancePosts++;return fetch(url,init)};
    const provider=new ReferenceProvider({endpoint:c.origin,token:c.token,environment:'staging',enabled:true,maxObjectBytes:1024*1024,fetchImpl:transport});
    const expected=c.expected,manifest=await provider.results(expected.jobId,expected);
    if(manifest.objects.length!==1||manifest.objects[0].role!=='odm_archive')throw Error('synthetic_manifest');
    const obj=manifest.objects[0];if(obj.byteSize<=0)throw Error('synthetic_empty');
    const storageReceiptId=p.sha256(p.canonicalBytes(validateResultBinding(expected,manifest,obj.id)));
    const acceptance={schemaVersion:1,receiptId:'synthetic-receipt-1',storageReceiptId,jobId:expected.jobId,generation:expected.generation,environment:'staging',inputManifestSha256:expected.inputManifestSha256,pipelineDigest:expected.pipelineDigest,resultManifestSha256:expected.resultManifestSha256,acceptedObjectIds:[obj.id],acceptedAt:'2026-10-29T12:00:00.000Z',validationVersion:'ltds-reference-v1'};
    const modelArgs={authority,storageKey:storageReceiptId,expectedSha256:obj.sha256,expectedBytes:obj.byteSize,job,deps,signal:new AbortController().signal};
    const result=await handleReferenceResult({provider,expected,journal,ledger,acceptance,deliverAcceptance:false,
      assertAuthority:authority.assertCurrent,
      prepareIngest:({expected,manifest,object,acceptance})=>journal.prepareIngest({expected,manifest,objectId:object.id,acceptance,taskId:attempt.taskId,attemptId:attempt.id}),
      ingestRetained:()=>ingestStagingModelUnderAuthority(modelArgs),verifyIngest:()=>verifyStagingModelUnderAuthority(modelArgs)});
    const pending=ledger.pendingOutbox(Date.now()+300000),final=processing.getAttempt(attempt.id),output=final.resultModelVersionId&&processing.getModelOutput(final.resultModelVersionId);
    if(result.acknowledgement!=='pending'||pending.length!==1||acceptancePosts!==0||!output||output.attemptId!==attempt.id||db.prepare('SELECT COUNT(*) AS n FROM model_outputs WHERE attempt_id=?').get(attempt.id).n!==1)throw Error('synthetic_pending');
    const retained=fs.readFileSync(path.join(c.custody,`${storageReceiptId}.zip`));
    if(retained.length!==obj.byteSize||crypto.createHash('sha256').update(retained).digest('hex')!==obj.sha256)throw Error('synthetic_bytes');
    console.log(JSON.stringify({acknowledgement:result.acknowledgement,pending:pending.length,acceptancePosts,bytes:retained.length,sha256:obj.sha256,modelStatus:output.status}));
   }finally{journal.close();ledger.close()}
  });
 }finally{db.close()}
})().catch(e=>{console.error('synthetic_bridge_failed',e.code||e.name);process.exitCode=1});
