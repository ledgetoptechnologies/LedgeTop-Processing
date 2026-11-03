'use strict';
// Actual Viewer repository/worker/provider against synthetic local coordinator.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
let input='';process.stdin.on('data',x=>input+=x);process.stdin.on('end',async()=>{
 const c=JSON.parse(input),server=path.join(c.viewer,'apps/viewer/server');
 const {processSubmit,processOne}=require(path.join(server,'processingWorker.js'));
 const {NodeOdmProvider}=require(path.join(server,'nodeOdmProvider.js'));
 const {openDatabase}=require(path.join(server,'database.js'));
 const {ProcessingRepository}=require(path.join(server,'processingRepository.js'));
 const database=openDatabase(path.join(c.root,'viewer.sqlite'));const processing=new ProcessingRepository(database);
 try{
  let attempt,job,id;
  if(['outage-recover','upload-recover'].includes(c.mode)){
   const row=database.prepare('SELECT id FROM processing_attempts').get();attempt=processing.getAttempt(row.id);id=attempt.providerTaskId;job=null;
   if(c.mode==='upload-recover')database.prepare("UPDATE processing_jobs SET lease_expires_at='2000-01-01T00:00:00.000Z' WHERE attempt_id=? AND job_type='submit'").run(attempt.id);else database.prepare("UPDATE processing_jobs SET available_at='2000-01-01T00:00:00.000Z' WHERE attempt_id=? AND job_type='submit'").run(attempt.id);
  } else {
   const dir=path.join(c.root,'dataset');fs.mkdirSync(dir);
   const entries=[['a.jpg','aaa','image'],['b.jpg','bbbb','image'],['aux.txt','xyz','provider_input'],['raw.csv','raw-excluded','gcp_source']];
   const files=entries.map(([name,content,role])=>{const data=Buffer.from(content);fs.writeFileSync(path.join(dir,name),data);return{id:name.replace('.','-'),relativePath:name,byteSize:data.length,sha256:crypto.createHash('sha256').update(data).digest('hex'),processingRole:role,contentType:name.endsWith('.jpg')?'image/jpeg':name.endsWith('.csv')?'text/csv':'text/plain'}});
   const project=processing.createProject({displayName:'synthetic'}),dataset=processing.createDataset({projectId:project.id,displayName:'test',storageMode:'managed',rootKey:'datasets',relativePath:'dataset'});processing.finalizeDataset(dataset.id,files,'d'.repeat(64));
   const task=processing.createTask({projectId:project.id,datasetId:dataset.id,displayName:'synthetic'});const provider=processing.upsertProvider({type:'nodeodm',displayName:'test',endpoint:c.origin,enabled:true});
   attempt=processing.createAttempt({taskId:task.id,providerId:provider.id,options:{}});job=['outage-retry','outage-first','upload-die'].includes(c.mode)?null:processing.claimJob('synthetic-worker');id=attempt.providerTaskId;
   if(c.mode==='lost')processing.declareUploadAdmissionForJob(job.id,job.lease_owner,1,10);
  }
  const storage={resolve:(_root,relative)=>path.join(c.root,relative),requireProcessingHeadroom:()=>{}};
  const observations=[];let outage=true;
  const intercept=async(url,init)=>{const headers=new Headers(init.headers||{}),route=new URL(url).pathname,actual=headers.get('X-LTDS-Declared-Input-Bytes');if(route==='/task/new/init')observations.push({route});if(route.includes('/upload/')||route.includes('/commit/')){observations.push({route,total:actual});if(c.mode==='missing')headers.delete('X-LTDS-Declared-Input-Bytes');if(c.mode==='changed'&&route.includes('/commit/'))headers.set('X-LTDS-Declared-Input-Bytes',String(Number(actual)+1));}if((c.mode==='probe-error'||['outage-retry','outage-first'].includes(c.mode)&&outage)&&route==='/info')return new Response('{}',{status:503,headers:{'content-type':'application/json'}});const response=await fetch(url,{...init,headers});if(c.mode==='upload-die'&&route.includes('/upload/')&&response.ok){process.stdout.write('UPLOAD_ACCEPTED\n');setInterval(()=>{},1000);await new Promise(()=>{});}if(c.mode==='malformed'&&route==='/info'&&response.ok){const info=await response.json();info.ledgetopUploadAdmission={version:99};return new Response(JSON.stringify(info),{status:200,headers:{'content-type':'application/json'}});}if(c.mode==='lost'&&route==='/info'&&response.ok){const info=await response.json();delete info.ledgetopUploadAdmission;return new Response(JSON.stringify(info),{status:200,headers:{'content-type':'application/json'}});}if(!response.ok)observations.push({route,status:response.status});return response;};
  const adapter=new NodeOdmProvider({endpoint:c.origin,token:c.token,fetchImpl:intercept,providerType:'nodeodm'});let completed=false,error=null,replay=false;
  let retry=null;const deps={processing,storage,config:{},providerCredentials:{},adapterFactory:()=>adapter};
  try{if(['outage-retry','outage-first','outage-recover','upload-die','upload-recover'].includes(c.mode)){
   if(['outage-recover','upload-recover'].includes(c.mode)){await processOne(deps,'worker-restarted');const current=processing.getAttempt(attempt.id);retry={afterStatus:current.status,afterPhase:current.submissionPhase,afterError:current.errorCode,afterJob:database.prepare("SELECT status FROM processing_jobs WHERE attempt_id=? AND job_type='submit'").get(attempt.id)?.status};completed=current.submissionPhase==='committed';}
   else if(c.mode==='upload-die'){await processOne(deps,'worker-killed');throw new Error('expected process exit after upload');}
   else {
   await processOne(deps,'worker-first');const after=processing.getAttempt(attempt.id);const pending=database.prepare("SELECT status FROM processing_jobs WHERE attempt_id=? AND job_type='submit'").get(attempt.id);
   retry={firstStatus:after.status,firstPhase:after.submissionPhase,firstJob:pending?.status,firstUploadCount:observations.filter(x=>x.route?.includes('/upload/')).length};
   if(c.mode==='outage-retry'){database.prepare("UPDATE processing_jobs SET available_at='2000-01-01T00:00:00.000Z' WHERE attempt_id=? AND job_type='submit'").run(attempt.id);outage=false;
   await processOne(deps,'worker-second');const afterRetry=processing.getAttempt(attempt.id);retry.afterStatus=afterRetry.status;retry.afterPhase=afterRetry.submissionPhase;completed=afterRetry.submissionPhase==='committed';}
   }
  } else {await processSubmit(job,deps);completed=true;await adapter.commit(id,{declaredInputBytes:10});replay=true;}}catch(e){error={code:e.code||null,status:e.status||null,message:e.message};}
  const current=processing.getAttempt(attempt.id);process.stdout.write(JSON.stringify({uuid:id,attemptId:attempt.id,jobId:job?.id||null,retry,expectedTotal:10,completed,commitReplay:replay,observations,error,declaration:current.declaredInputBytes,phase:current.submissionPhase}));
 } finally {database.close();}
}).on('error',e=>{process.stderr.write(String(e));process.exitCode=1});
