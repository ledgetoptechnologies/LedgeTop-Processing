'use strict';
// Synthetic-only client; source and SQLite live in the fixture's private root.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),assert=require('node:assert/strict');
const base=process.env.VIEWER_SOURCE+'/apps/viewer/server';
const {openDatabase}=require(base+'/database'),{ViewerRepository}=require(base+'/repository'),{ProcessingRepository}=require(base+'/processingRepository');
const {StorageManager}=require(base+'/storageManager'),{processOne}=require(base+'/processingWorker'),{NO_AUTH_MARKER}=require(base+'/providerAuth');
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
  const providers={};for(const env of ['staging','production'])providers[env]=processing.upsertProvider({authEnvKey:NO_AUTH_MARKER,type:'nodeodm',displayName:'fixture-'+env,endpoint:`http://127.0.0.1:${port}`,enabled:true});
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
  await sleep(5100);await processOne(deps,'fixture-offline');
  await controlCall('/coordinator-start');await controlCall('/finish/'+first.providerTaskId);
  await controlCall('/expect-next/'+third.providerTaskId+'/'+second.providerTaskId);
  await controlCall('/finish/'+third.providerTaskId);await controlCall('/expect-next/'+second.providerTaskId+'/none');await controlCall('/finish/'+second.providerTaskId);
  const deadline=Date.now()+60000;
  while(Date.now()<deadline){await processOne(deps,'fixture-reconcile');if([first,second,third].every(a=>processing.getAttempt(a.id).status==='derivatives'))break;await sleep(200)}
  for(const a of [first,second,third]){const attempt=processing.getAttempt(a.id);assert.equal(attempt.status,'derivatives');assert.equal(db.prepare('SELECT COUNT(*) AS n FROM model_outputs WHERE attempt_id=?').get(a.id).n,1)}
  console.log(JSON.stringify({synthetic:true,attempts:3,resultsPerAttempt:1}));
 }finally{db.close()}
}
main().catch(e=>{console.error(e.stack);process.exitCode=1});
