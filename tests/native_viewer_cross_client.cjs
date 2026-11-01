'use strict';
// Synthetic-only client; source and SQLite live in the fixture's private root.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),assert=require('node:assert/strict');
const base=process.env.VIEWER_SOURCE+'/apps/viewer/server';
const {openDatabase}=require(base+'/database'),{ViewerRepository}=require(base+'/repository'),{ProcessingRepository}=require(base+'/processingRepository');
const {StorageManager}=require(base+'/storageManager'),{processOne}=require(base+'/processingWorker'),{NO_AUTH_MARKER}=require(base+'/providerAuth');
const {NodeOdmProvider}=require(base+'/nodeOdmProvider'),multipart=require(base+'/providerMultipart');
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
async function main(){
 const root=process.env.FIXTURE_ROOT,port=process.env.COORDINATOR_PORT,control=process.env.CONTROL_PORT;
 const db=openDatabase(path.join(root,'viewer.sqlite')),processing=new ProcessingRepository(db),repository=new ViewerRepository(db);
 try{
  const config=Object.fromEntries(['datasets','models','cache','trash'].map(k=>{const p=path.join(root,k);fs.mkdirSync(p);return [k+'Mount',p]}));
  Object.assign(config,{storageReserveBytes:0,storageReservePercent:0,meshDerivativesEnabled:true,processingProviderTransferTimeoutMs:5000,opsBaseUrl:'http://fixture.invalid'});
  const storage=new StorageManager(config);storage.initialize();
  const project=processing.createProject({displayName:'Synthetic project'}),dataset=processing.createDataset({projectId:project.id,displayName:'Synthetic data',storageMode:'managed',rootKey:'datasets',relativePath:'source'});
  fs.mkdirSync(path.join(config.datasetsMount,'source'));const bytes=Buffer.from('synthetic image');fs.writeFileSync(path.join(config.datasetsMount,'source','image.jpg'),bytes);
  const hash=crypto.createHash('sha256').update(bytes).digest('hex');processing.finalizeDataset(dataset.id,[{id:'one',relativePath:'image.jpg',byteSize:bytes.length,sha256:hash,contentType:'image/jpeg',metadata:{width:10,height:10}}],hash);
  const providers={};for(const env of ['staging','production'])providers[env]=processing.upsertProvider({authEnvKey:NO_AUTH_MARKER,type:'nodeodm',displayName:'fixture-'+env,endpoint:`http://127.0.0.1:${port}`,enabled:true,admissionLimit:2});
  const credentials={resolve:id=>id===providers.production.id?'synthetic-production-token-pppppppp':'synthetic-staging-token-ssssssss'};
  const deps={processing,repository,storage,config,providerCredentials:credentials};
  const controlCall=async(pathname)=>{const r=await fetch(`http://127.0.0.1:${control}${pathname}`),body=await r.json();if(r.status!==200)throw Error(`fixture control ${pathname.split('/')[1]}: ${JSON.stringify(body)}`);return body};
  async function create(name,env){const task=processing.createTask({projectId:project.id,datasetId:dataset.id,displayName:name});return processing.createAttempt({taskId:task.id,providerId:providers[env].id,options:{},createdBy:'fixture'})}
  const first=await create('first','staging');assert.equal(await processOne(deps,'fixture-first'),true);
  const initial=processing.getAttempt(first.id),submission=processing.getAttemptSubmission(first.id);
  if(initial.status!=='queued_upstream')throw Error('first synthetic submission: '+JSON.stringify({status:initial.status,errorCode:initial.errorCode,errorMessage:initial.errorMessage,phase:submission.submissionPhase}));
  await controlCall('/wait-running/'+first.providerTaskId);
  const second=await create('waiting','staging'),third=await create('production','production');
  assert.equal(await processOne(deps,'fixture-second'),true);assert.equal(await processOne(deps,'fixture-third'),true);
  await controlCall('/assert-queued/'+first.providerTaskId+'/'+second.providerTaskId+'/'+third.providerTaskId);
  await controlCall('/coordinator-stop');
  await sleep(5100);
  const offlineJob=db.prepare("SELECT id,error_code,attempt_count FROM processing_jobs WHERE job_type='reconcile' AND status='pending' AND available_at<=? ORDER BY created_at LIMIT 1").get(new Date().toISOString());
  assert.ok(offlineJob,'expected a due reconcile job');assert.equal(offlineJob.error_code,null);assert.equal(await processOne(deps,'fixture-offline'),true);
  const retry=db.prepare('SELECT status,error_code,attempt_count FROM processing_jobs WHERE id=?').get(offlineJob.id);
  assert.equal(retry.status,'pending');assert.equal(retry.error_code,'provider_observation_unavailable');assert.equal(retry.attempt_count,offlineJob.attempt_count+1);
  await controlCall('/coordinator-start');await controlCall('/finish/'+first.providerTaskId);
  await controlCall('/expect-next/'+third.providerTaskId+'/'+second.providerTaskId);
  await controlCall('/finish/'+third.providerTaskId);await controlCall('/expect-next/'+second.providerTaskId+'/none');await controlCall('/finish/'+second.providerTaskId);
  const deadline=Date.now()+60000;
  while(Date.now()<deadline){await processOne(deps,'fixture-reconcile');const missing=db.prepare("SELECT error_message FROM processing_jobs WHERE error_code='ERR_MODULE_NOT_FOUND' LIMIT 1").get();if(missing)throw Error('synthetic module resolution: '+missing.error_message);if([first,second,third].every(a=>processing.getAttempt(a.id).status==='derivatives'))break;await sleep(200)}
  for(const a of [first,second,third]){const attempt=processing.getAttempt(a.id);if(attempt.status!=='derivatives'){const jobs=db.prepare('SELECT job_type,status,error_code,attempt_count FROM processing_jobs WHERE attempt_id=? ORDER BY created_at').all(a.id);throw Error('synthetic ingestion incomplete: '+JSON.stringify({status:attempt.status,errorCode:attempt.errorCode,jobs}))}assert.equal(db.prepare('SELECT COUNT(*) AS n FROM model_outputs WHERE attempt_id=?').get(a.id).n,1)}
  assert.equal(db.prepare('SELECT status FROM processing_jobs WHERE id=?').get(offlineJob.id).status,'complete');
  assert.equal(crypto.createHash('sha256').update(fs.readFileSync(path.join(config.datasetsMount,'source','image.jpg'))).digest('hex'),hash);
  await controlCall('/assert-retained/'+first.providerTaskId+'/'+second.providerTaskId+'/'+third.providerTaskId);
  const detected=await new NodeOdmProvider({endpoint:providers.production.endpoint,token:credentials.resolve(providers.production.id),providerType:'nodeodm'}).capabilities();
  assert.equal(detected.capabilities.ledgetopMaxInputBytes,30);assert.equal(detected.capabilities.ledgetopMaxUploadRequestBytes,400);
  processing.updateProviderCapabilities(providers.production.id,{...detected,health:'healthy'});
  let serial=0;
  async function scenario(names){
    const key='capacity-'+(++serial),dir=path.join(config.datasetsMount,key);fs.mkdirSync(dir);
    const entries=names.map(([name,body])=>{const bytes=Buffer.from(body);fs.writeFileSync(path.join(dir,name),bytes);return{relativePath:name,byteSize:bytes.length,sha256:crypto.createHash('sha256').update(bytes).digest('hex'),contentType:'image/jpeg'}});
    const ds=processing.createDataset({projectId:project.id,displayName:key,storageMode:'managed',rootKey:'datasets',relativePath:key});processing.finalizeDataset(ds.id,entries,crypto.createHash('sha256').update(JSON.stringify(entries)).digest('hex'));
    const task=processing.createTask({projectId:project.id,datasetId:ds.id,displayName:key});return {attempt:processing.createAttempt({taskId:task.id,providerId:providers.production.id,options:{},createdBy:'fixture'}),entries,dir};
  }
  for(const [name,body,code] of [['bad#name.jpg','image','provider_filename_unsupported'],['unsupported.raw','image','provider_input_type_unsupported'],['large.jpg','x'.repeat(31),'provider_input_limit_exceeded']]){
    const sample=await scenario([[name,body]]),before=await controlCall('/stats');assert.equal(await processOne(deps,'preflight-'+serial),true);
    const after=processing.getAttempt(sample.attempt.id);assert.equal(after.status,'failed');assert.equal(after.errorCode,code);
    await controlCall('/assert-no-init/'+sample.attempt.providerTaskId);assert.deepEqual(await controlCall('/stats'),before);assert.equal(fs.readFileSync(path.join(sample.dir,name),'utf8'),body);
  }
  const batch=await scenario([['image0.jpg','aaaaa'],['image1.jpg','bbbbb'],['image2.jpg','ccccc']]);let lost=false;
  const adapterFactory=(provider,_config,creds)=>new NodeOdmProvider({endpoint:provider.endpoint,token:creds.resolve(provider.id),providerType:'nodeodm',fetchImpl:async(url,init)=>{
    const response=await fetch(url,init);if(!lost&&new URL(url).pathname.includes('/task/new/upload/')){lost=true;await response.arrayBuffer();throw Object.assign(new Error('synthetic lost upload response'),{code:'ECONNRESET'})}return response;
  }});
  assert.equal(await processOne({...deps,adapterFactory},'batch-lost'),true);assert.ok(lost);
  let saved=processing.getAttemptSubmission(batch.attempt.id);assert.equal(saved.submissionPhase,'uploading');assert.equal(saved.uploadedFileCount,0);
  let evidence=await controlCall('/upload-evidence/'+batch.attempt.providerTaskId);assert.equal(evidence.images,2);
  await sleep(11000);assert.equal(await processOne(deps,'batch-resume'),true);
  saved=processing.getAttemptSubmission(batch.attempt.id);assert.equal(saved.submissionPhase,'committed');assert.equal(saved.uploadedFileCount,3);
  evidence=await controlCall('/upload-evidence/'+batch.attempt.providerTaskId);
  assert.deepEqual(evidence.names,['image0.jpg','image1.jpg','image2.jpg']);assert.equal(evidence.images,3);
  assert.deepEqual(evidence.hashes,batch.entries.map(entry=>entry.sha256));
  assert.deepEqual(evidence.storedHashes,batch.entries.map(entry=>entry.sha256));
  for(const entry of batch.entries)assert.equal(crypto.createHash('sha256').update(fs.readFileSync(path.join(batch.dir,entry.relativePath))).digest('hex'),entry.sha256);
  assert.deepEqual(evidence.lengths,[multipart.wireBytes(batch.entries.slice(0,2)),multipart.wireBytes(batch.entries.slice(2))]);
  await controlCall('/wait-running/'+batch.attempt.providerTaskId);await controlCall('/finish/'+batch.attempt.providerTaskId);
  console.log(JSON.stringify({synthetic:true,attempts:3,resultsPerAttempt:1}));
 }finally{db.close()}
}
main().catch(e=>{console.error(e.stack);process.exitCode=1});
