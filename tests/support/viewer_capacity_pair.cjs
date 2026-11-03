'use strict';
// Actual Viewer repository/worker/provider against synthetic local coordinator.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
let input='';process.stdin.on('data',x=>input+=x);process.stdin.on('end',async()=>{
 const c=JSON.parse(input),server=path.join(c.viewer,'apps/viewer/server');
 const {processSubmit}=require(path.join(server,'processingWorker.js'));
 const {NodeOdmProvider}=require(path.join(server,'nodeOdmProvider.js'));
 const {openDatabase}=require(path.join(server,'database.js'));
 const {ProcessingRepository}=require(path.join(server,'processingRepository.js'));
 const database=openDatabase(path.join(c.root,'viewer.sqlite'));const processing=new ProcessingRepository(database);
 try{
  const dir=path.join(c.root,'dataset');fs.mkdirSync(dir);
  const entries=[['a.jpg','aaa','image'],['b.jpg','bbbb','image'],['aux.txt','xyz','provider_input'],['raw.csv','raw-excluded','gcp_source']];
  const files=entries.map(([name,content,role])=>{const data=Buffer.from(content);fs.writeFileSync(path.join(dir,name),data);return{id:name.replace('.','-'),relativePath:name,byteSize:data.length,sha256:crypto.createHash('sha256').update(data).digest('hex'),processingRole:role,contentType:name.endsWith('.jpg')?'image/jpeg':name.endsWith('.csv')?'text/csv':'text/plain'}});
  const project=processing.createProject({displayName:'synthetic'}),dataset=processing.createDataset({projectId:project.id,displayName:'test',storageMode:'managed',rootKey:'datasets',relativePath:'dataset'});processing.finalizeDataset(dataset.id,files,'d'.repeat(64));
  const task=processing.createTask({projectId:project.id,datasetId:dataset.id,displayName:'synthetic'}),provider=processing.upsertProvider({type:'nodeodm',displayName:'test',endpoint:c.origin,enabled:true});
  const attempt=processing.createAttempt({taskId:task.id,providerId:provider.id,options:{}}),job=processing.claimJob('synthetic-worker'),id=attempt.providerTaskId;
  if(c.mode==='lost')processing.declareUploadAdmissionForJob(job.id,job.lease_owner,1,10);
  const storage={resolve:(_root,relative)=>path.join(c.root,relative),requireProcessingHeadroom:()=>{}};
  const observations=[];
  const intercept=async(url,init)=>{const headers=new Headers(init.headers||{}),route=new URL(url).pathname,actual=headers.get('X-LTDS-Declared-Input-Bytes');if(route.includes('/upload/')||route.includes('/commit/')){observations.push({route,total:actual});if(c.mode==='missing')headers.delete('X-LTDS-Declared-Input-Bytes');if(c.mode==='changed'&&route.includes('/commit/'))headers.set('X-LTDS-Declared-Input-Bytes',String(Number(actual)+1));}if(c.mode==='probe-error'&&route==='/info')return new Response('{}',{status:503,headers:{'content-type':'application/json'}});const response=await fetch(url,{...init,headers});if(c.mode==='malformed'&&route==='/info'&&response.ok){const info=await response.json();info.ledgetopUploadAdmission={version:99};return new Response(JSON.stringify(info),{status:200,headers:{'content-type':'application/json'}});}if(c.mode==='lost'&&route==='/info'&&response.ok){const info=await response.json();delete info.ledgetopUploadAdmission;return new Response(JSON.stringify(info),{status:200,headers:{'content-type':'application/json'}});}if(!response.ok)observations.push({route,status:response.status});return response;};
  const adapter=new NodeOdmProvider({endpoint:c.origin,token:c.token,fetchImpl:intercept,providerType:'nodeodm'});let completed=false,error=null,replay=false;
  try{await processSubmit(job,{processing,storage,config:{},providerCredentials:{},adapterFactory:()=>adapter});completed=true;await adapter.commit(id,{declaredInputBytes:10});replay=true;}catch(e){error={code:e.code||null,status:e.status||null,message:e.message};}
  const current=processing.getAttempt(attempt.id);process.stdout.write(JSON.stringify({uuid:id,attemptId:attempt.id,jobId:job.id,expectedTotal:10,completed,commitReplay:replay,observations,error,declaration:current.declaredInputBytes,phase:current.submissionPhase}));
 } finally {database.close();}
}).on('error',e=>{process.stderr.write(String(e));process.exitCode=1});
