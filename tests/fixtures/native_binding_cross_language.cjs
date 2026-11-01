'use strict';
// Synthetic client fixture; all Viewer modules come from an immutable export.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const [viewer,root,endpoint,phase]=process.argv.slice(2),server=path.join(viewer,'apps/viewer/server');
const {openDatabase}=require(path.join(server,'database'));
const {ProcessingRepository}=require(path.join(server,'processingRepository'));
const {ProviderCredentials}=require(path.join(server,'providerCredentials'));
const {canonical}=require(path.join(server,'nativeManifestCandidate'));
const {installCandidateSchema,createCandidate,createLoopbackTransport,capabilityContext}=require(path.join(server,'nativeManifestBindingCandidate'));
const {installPlanCandidateSchema,createPlanCandidate,createPlanBindingCandidate}=require(path.join(server,'nativePlanCandidate'));
const source=JSON.parse(fs.readFileSync(path.join(viewer,'apps/viewer/test/fixtures/native-input-manifest-v1-candidate.json'))).vectors[0];
const catalog=crypto.createHash('sha256').update('ltds-viewer-selected-catalog-v1\0[]').digest('hex');
const db=openDatabase(path.join(root,'viewer.sqlite')),processing=new ProcessingRepository(db);
const vault=new ProviderCredentials({processing,activeKeyId:'fixture',keys:{fixture:'ab'.repeat(32)}});
const transport=createLoopbackTransport({endpoint,environmentToken:'synthetic-staging-token-'+'s'.repeat(32)});
const counts={bindPlan:0,planStatus:0};
for(const name of Object.keys(counts)){const actual=transport[name];transport[name]=async(...args)=>{counts[name]++;return actual(...args);};}
async function attempt(options){
  const project=processing.createProject({displayName:'Synthetic '+crypto.randomUUID()});
  const dataset=processing.createDataset({projectId:project.id,displayName:'Data',storageMode:'managed',rootKey:'datasets',relativePath:'dataset-'+project.id});
  const digest=crypto.createHash('sha256').update('test').digest('hex');
  processing.finalizeDataset(dataset.id,[{relativePath:'photo.jpg',byteSize:4,sha256:digest}],digest);
  const task=processing.createTask({projectId:project.id,datasetId:dataset.id,displayName:'Plan'});
  const provider=processing.upsertProvider({type:'nodeodm',displayName:'Synthetic',endpoint,enabled:true});
  const attempt=processing.createAttempt({taskId:task.id,providerId:provider.id,options});
  const manifest=JSON.parse(source.canonical);manifest.id=attempt.id;delete manifest.sha256;
  manifest.sha256=crypto.createHash('sha256').update(canonical(manifest)).digest('hex');
  const client=createCandidate({db,vault,transport,enabled:true});
  client.prepare({attemptId:attempt.id,providerId:provider.id,taskUuid:attempt.providerTaskId,environment:'staging',manifestBytes:Buffer.from(canonical(manifest))});
  await client.bindOrRecover(attempt.id);
  createPlanCandidate({db,processing,expectedEnvironment:'staging',enabled:true}).freeze({attemptId:attempt.id,outputs:null,catalogDigest:catalog});
  return attempt.id;
}
async function run(){
  if(phase==='lose'){
    installCandidateSchema(db,true);installPlanCandidateSchema(db,true);
    const id=await attempt({});
    const post=transport.bindPlan;transport.bindPlan=async(...args)=>{await post(...args);throw new Error('synthetic post-response loss');};
    try{await createPlanBindingCandidate({db,processing,vault,transport,expectedEnvironment:'staging',enabled:true}).bindOrRecover(id);throw new Error('expected hold');}
    catch(e){if(e.code!=='native_plan_hold')throw e;}
    const row=db.prepare('SELECT * FROM native_plan_candidate WHERE attempt_id=?').get(id);
    if(row.phase!=='binding'||row.receipt!==null||counts.bindPlan!==1||counts.planStatus!==0)throw new Error('lost-response state mismatch');
  }else{
    const rows=db.prepare('SELECT attempt_id FROM native_plan_candidate ORDER BY created_at').all();
    if(rows.length!==1)throw new Error('unexpected candidate count');const id=rows[0].attempt_id;
    const manifestRow=db.prepare('SELECT * FROM native_binding_candidate WHERE attempt_id=?').get(id);
    const capability=vault.open(capabilityContext(manifestRow),{credentialCiphertext:manifestRow.capability_ciphertext,credentialKeyId:manifestRow.key_id});
    const wrongEnvironment=createLoopbackTransport({endpoint,environmentToken:'synthetic-production-token-'+'p'.repeat(32)});
    for(const [candidate,cap] of [[wrongEnvironment,capability],[createLoopbackTransport({endpoint,environmentToken:'synthetic-staging-token-'+'s'.repeat(32)}),'A'.repeat(43)]]){
      try{await candidate.planStatus(manifestRow.task_uuid,cap);throw new Error('wrong authority accepted');}
      catch(e){if(e.code!=='native_binding_hold')throw e;}
    }
    const result=await createPlanBindingCandidate({db,processing,vault,transport,expectedEnvironment:'staging',enabled:true}).bindOrRecover(id);
    const row=db.prepare('SELECT * FROM native_plan_candidate WHERE attempt_id=?').get(id);
    if(row.phase!=='bound'||!row.receipt||JSON.parse(row.receipt).planSha256!==result.planSha256||result.capacityReady!==false||result.dispatchReady!==false||counts.bindPlan!==0||counts.planStatus!==1)throw new Error('recovery mismatch');
    if(phase==='nonempty'){
      const other=await attempt({fixture:true});const before=counts.bindPlan;
      try{await createPlanBindingCandidate({db,processing,vault,transport,expectedEnvironment:'staging',enabled:true}).bindOrRecover(other);throw new Error('nonempty plan advanced');}
      catch(e){if(e.code!=='native_plan_hold')throw e;}
      if(counts.bindPlan!==before||db.prepare('SELECT phase FROM native_plan_candidate WHERE attempt_id=?').get(other).phase!=='intent')throw new Error('nonempty hold mismatch');
    }
  }
}
run().then(()=>db.close(),error=>{db.close();console.error(error.message);process.exitCode=1;});
