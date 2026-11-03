'use strict';
// Synthetic harness that invokes Viewer commit 7dbd175d's actual outbox/client.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const [viewer,root,mode]=process.argv.slice(2);
const {openDatabase}=require(path.join(viewer,'apps/viewer/server/database'));
const {ProcessingRepository}=require(path.join(viewer,'apps/viewer/server/processingRepository'));
const {PreparedSubmissionOutbox,PreparedSubmissionClient,PROTOCOL}=require(path.join(viewer,'apps/viewer/server/preparedSubmissionOutbox'));
let raw='';process.stdin.setEncoding('utf8');process.stdin.on('data',x=>raw+=x);
process.stdin.on('end',async()=>{
 let db;
 try{
  const arg=JSON.parse(raw);db=openDatabase(path.join(root,'viewer.sqlite'));const outbox=new PreparedSubmissionOutbox(db);
  if(mode==='create'){
   const repo=new ProcessingRepository(db),project=repo.createProject({displayName:'Synthetic project'});
   const dataset=repo.createDataset({projectId:project.id,displayName:'Synthetic source',storageMode:'managed',rootKey:'datasets'});
   const digest=crypto.createHash('sha256').update('source').digest('hex');
   repo.finalizeDataset(dataset.id,[{relativePath:'photo.jpg',byteSize:6,sha256:digest}],digest);
   const provider=repo.upsertProvider({type:'nodeodm',displayName:'Synthetic node',endpoint:'https://node.invalid',enabled:true});
   const task=repo.createTask({projectId:project.id,datasetId:dataset.id,displayName:'Synthetic task'});
   const attempt=repo.createAttempt({taskId:task.id,providerId:provider.id,capabilityFingerprint:'synthetic'});
   const job=repo.claimJob('paired-synthetic-owner');
   const submission=structuredClone(arg.submission);submission.submissionId=attempt.id;
   const envelope={schemaVersion:3,protocol:PROTOCOL,submission,viewerTaskId:task.id,viewerAttemptId:attempt.id,activationId:crypto.randomUUID()};
   const intent=outbox.persistIntent({localJobId:job.id,owner:job.lease_owner,origin:arg.origin,authorityId:arg.authority,principalId:'viewer-a',environment:'production',envelope});
   console.log(JSON.stringify({attemptId:attempt.id,localJobId:job.id,owner:job.lease_owner,envelope,intentState:intent.state}));
  }else{
   const intent=outbox.getIntent(arg.attemptId);if(!intent)throw Error('missing synthetic intent');
   let posts=0,gets=0;
   const client=new PreparedSubmissionClient({origin:arg.origin,authorityId:arg.authority,bearerToken:arg.token,principalId:'viewer-a',environment:'production',outbox,
    fetchImpl:async(url,options)=>{if(options.method==='POST')posts++;else gets++;const response=await fetch(url,options);if(options.method==='POST'&&arg.dropResponse){await response.body?.cancel();throw Error('synthetic lost reply');}return response;}});
   if(mode==='lease-expired'){
    db.prepare('UPDATE processing_jobs SET lease_expires_at=? WHERE id=?').run(new Date(Date.now()-1000).toISOString(),arg.localJobId);
    let code=null;try{await client.dispatch(intent,{owner:arg.owner})}catch(e){code=e.code}
    console.log(JSON.stringify({code,posts,gets,state:outbox.getIntent(arg.attemptId).state}));
   }else{
    const accepted=await client.dispatch(intent,{owner:arg.owner});
    const replay=await client.dispatch(outbox.getIntent(arg.attemptId),{owner:arg.owner});
    console.log(JSON.stringify({state:accepted.state,remoteBinding:accepted.remoteBinding,replayState:replay.state,posts,gets}));
   }
  }
 }catch(error){console.error(JSON.stringify({code:error.code||'synthetic_harness_error'}));process.exitCode=1}
 finally{if(db)db.close()}
});
