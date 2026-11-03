'use strict';
// Synthetic collaborators only; real Viewer worker and NodeOdmProvider.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
let input='';process.stdin.on('data',x=>input+=x);process.stdin.on('end',async()=>{
 const c=JSON.parse(input),server=path.join(c.viewer,'apps/viewer/server');
 const {processSubmit}=require(path.join(server,'processingWorker.js'));
 const {NodeOdmProvider}=require(path.join(server,'nodeOdmProvider.js'));
 const id='11111111-2222-4333-8444-555555555555',dir=path.join(c.root,'dataset');fs.mkdirSync(dir);
 const entries=[['a.jpg','aaa','image'],['b.jpg','bbbb','image'],['aux.txt','xyz','provider_input'],['raw.csv','raw-excluded','gcp_source']];
 const files=entries.map(([name,content,role])=>{const data=Buffer.from(content);fs.writeFileSync(path.join(dir,name),data);return{relativePath:name,byteSize:data.length,sha256:crypto.createHash('sha256').update(data).digest('hex'),processingRole:role}});
 const attempt={id:'attempt',taskId:'task',datasetId:'dataset',providerId:'provider',providerTaskId:id,options:{},submissionPhase:'new',uploadedFileCount:0,...(c.mode==='lost'?{uploadAdmissionVersion:1,declaredInputBytes:10}:{})};
 const processing={hasReferenceAttemptProtocol:()=>false,hasReferenceAttemptJournal:()=>false,beginAttemptInitialization:()=>({requestSnapshot:{request:{uuid:id,name:'synthetic',options:{}}},generation:1,historical:false}),acknowledgeAttemptInitialization:()=>true,deferHeldSubmit:(_a,_b,_c,reason)=>{throw new Error('deferred:'+reason)},getAttempt:()=>attempt,getAttemptSubmission:()=>({}),getTask:()=>({datasetId:'dataset',displayName:'synthetic'}),getDataset:()=>({rootKey:'datasets',relativePath:'dataset',files,byteSize:files.reduce((n,x)=>n+x.byteSize,0)}),getProvider:()=>({endpoint:c.origin,type:'nodeodm',capabilities:{}}),activeProcessingReservationBytes:()=>0,activeDerivativeReservationBytes:()=>0,transitionAttemptForJob:()=>true,setSubmissionStateForJob:()=>true,completeAndEnqueueJob:()=>true,declareUploadAdmissionForJob:(_job,_owner,version,declaredInputBytes)=>({version,declaredInputBytes})};
 const storage={resolve:(_root,relative)=>path.join(c.root,relative),requireProcessingHeadroom:()=>{}};
 const observations=[];
 const intercept=async(url,init)=>{const headers=new Headers(init.headers||{});let actual=headers.get('X-LTDS-Declared-Input-Bytes');const route=new URL(url).pathname;if(route.includes('/upload/')||route.includes('/commit/')){observations.push({route,total:actual});if(c.mode==='missing')headers.delete('X-LTDS-Declared-Input-Bytes');if(c.mode==='changed'&&route.includes('/commit/'))headers.set('X-LTDS-Declared-Input-Bytes',String(Number(actual)+1));}const response=await fetch(url,{...init,headers});if(c.mode==='lost'&&route==='/info'&&response.ok){const info=await response.json();delete info.ledgetopUploadAdmission;return new Response(JSON.stringify(info),{status:200,headers:{'content-type':'application/json'}});}if(!response.ok)observations.push({route,status:response.status});return response;};
 const adapter=new NodeOdmProvider({endpoint:c.origin,token:c.token,fetchImpl:intercept,providerType:'nodeodm'});
 const originalStatus=adapter.status.bind(adapter);adapter.status=async(...args)=>{try{return await originalStatus(...args)}catch(e){observations.push({statusError:e.code||null});throw e}};
 let completed=false,error=null,replay=false;
 try{await processSubmit({id:'job',attempt_id:'attempt',lease_owner:'fixture'},{processing,storage,config:{},providerCredentials:{},adapterFactory:()=>adapter});completed=true;await adapter.commit(id,{declaredInputBytes:10});replay=true;}catch(e){error={code:e.code||null,status:e.status||null,message:e.message};}
 process.stdout.write(JSON.stringify({uuid:id,expectedTotal:10,completed,commitReplay:replay,observations,error}));
}).on('error',e=>{process.stderr.write(String(e));process.exitCode=1});
