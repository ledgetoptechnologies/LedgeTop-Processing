'use strict';
// Synthetic persisted outbox: real staged Viewer migrations and sender.
const assert=require('node:assert/strict');
const crypto=require('node:crypto');
const fs=require('node:fs');
const {spawnSync}=require('node:child_process');
const {DatabaseSync}=require('node:sqlite');
const path=require('node:path');
const root=process.env.VIEWER_SOURCE;
const {getStagedCommitment}=require(path.join(root,'apps/viewer/server/stagedCommitmentClient'));
const {deliverOneStagedAck}=require(path.join(root,'apps/viewer/server/stagedAckDelivery'));
const {ProcessingRepository}=require(path.join(root,'apps/viewer/server/processingRepository'));
const config=JSON.parse(process.env.SYNTHETIC_BRIDGE_CONFIG);
const binding={origin:config.origin,principalId:'viewer-stage',authorityId:config.authority,environment:'staging',credentialRef:'synthetic'};
const dbPath=process.env.SYNTHETIC_DB_PATH;
const sha=v=>crypto.createHash('sha256').update(v).digest('hex');
function migration(number){
 const source=fs.readFileSync(path.join(root,'apps/viewer/server/database.js'),'utf8');
 const match=source.match(new RegExp('\\{version:'+number+',name:\\x27[^\\x27]+\\x27,sql:`([\\s\\S]*?)`\\}'));
 if(!match)throw Error('expected exact Viewer migration absent');return match[1];
}
function setup(db,clock,fetchImpl=fetch){return {db,enabled:true,bindings:{[config.providerId||'provider']:binding},resolveCredential:async()=>config.bearer,resolveCapability:async()=>config.capability,clock:()=>clock,fetchImpl};}
(async()=>{
 const mode=process.argv[2];
 if(mode==='kill'||mode==='replay'){
  const db=new DatabaseSync(dbPath);
  const fetchImpl=async(url,init)=>{fs.appendFileSync(dbPath+'.requests',init.method+' '+url.pathname+'\n');const response=await fetch(url,init);if(mode==='kill'&&init.method==='POST'){await response.arrayBuffer();process.kill(process.pid,'SIGKILL');}return response;};
  await deliverOneStagedAck(setup(db,mode==='kill'?100000:200000,fetchImpl));db.close();return;
 }
 const opts={enabled:true,...binding,bearer:config.bearer,capability:config.capability,taskId:config.task};
 const commitment=await getStagedCommitment(opts);
 await assert.rejects(getStagedCommitment({...opts,bearer:config.capability}));
 const id=()=>crypto.randomUUID();let assertion={...commitment,attemptId:id(),modelId:id(),modelVersionId:id(),stagingCommitId:id()};
 let db=new DatabaseSync(dbPath),body;
 if(mode!=='existing'){
 db.exec('CREATE TABLE processing_attempts(id TEXT PRIMARY KEY); CREATE TABLE processing_providers(id TEXT PRIMARY KEY); CREATE TABLE models(id TEXT PRIMARY KEY,display_name TEXT); CREATE TABLE model_outputs(id TEXT PRIMARY KEY,model_id TEXT,attempt_id TEXT,status TEXT); CREATE TABLE processing_attempt_transfer_provenance(attempt_id TEXT PRIMARY KEY,initialization_json TEXT,archive_json TEXT);');
 db.exec(migration(46));db.exec(migration(47));
 db.prepare('INSERT INTO processing_attempts VALUES(?)').run(assertion.attemptId);db.prepare('INSERT INTO processing_providers VALUES(?)').run('provider');
 db.prepare('INSERT INTO models VALUES(?,?)').run(assertion.modelId,'synthetic');
 db.prepare('INSERT INTO model_outputs(id,model_id,attempt_id,status) VALUES(?,?,?,?)').run(assertion.modelVersionId,assertion.modelId,assertion.attemptId,'staged');
 db.prepare('INSERT INTO processing_attempt_transfer_provenance VALUES(?,?,?)').run(assertion.attemptId,null,JSON.stringify({providerTaskId:config.task,archiveSha256:commitment.archiveSha256,archiveByteSize:commitment.archiveBytes}));
 db.prepare('INSERT INTO processing_staged_commitments VALUES(?,?,?)').run(assertion.attemptId,JSON.stringify(commitment),'2026');
 const attempt={id:assertion.attemptId,providerId:'provider',providerTaskId:config.task,resultModelId:assertion.modelId,resultModelVersionId:assertion.modelVersionId};
 const repository=Object.create(ProcessingRepository.prototype);repository.database=db;db.exec('BEGIN IMMEDIATE');
 repository.enqueueDisabledStagedByteAck(attempt,commitment,{origin:config.origin,credentialRef:'synthetic',capabilitySha256:sha(config.capability)});db.exec('COMMIT');
 body=db.prepare('SELECT assertion_json FROM processing_staged_byte_ack_outbox').get().assertion_json;
 const saved=JSON.parse(body);assert.equal(saved.attemptId,assertion.attemptId);assert.equal(saved.archiveSha256,commitment.archiveSha256);
 }else{body=db.prepare('SELECT assertion_json FROM processing_staged_byte_ack_outbox WHERE attempt_id=?').get(config.attempt).assertion_json;assertion=JSON.parse(body);assert.equal(assertion.archiveSha256,commitment.archiveSha256);}
 db.close();
 const first=spawnSync(process.execPath,[__filename,'kill'],{env:process.env,encoding:'utf8',timeout:8000});
 assert.equal(first.signal,'SIGKILL',`sender was not killed: ${first.status} ${first.stderr}`);
 db=new DatabaseSync(dbPath);let row=db.prepare('SELECT * FROM processing_staged_ack_delivery').get();
 assert.equal(row.state,'uncertain');assert.ok(row.lease_token);assert.equal(row.lease_expires_at,160000);db.close();
 const second=spawnSync(process.execPath,[__filename,'replay'],{env:process.env,encoding:'utf8',timeout:8000});
 assert.equal(second.status,0,`sender restart failed: ${second.stderr}`);
 db=new DatabaseSync(dbPath);row=db.prepare('SELECT * FROM processing_staged_ack_delivery').get();
 assert.equal(row.state,'accepted');assert.equal(db.prepare('SELECT assertion_json FROM processing_staged_byte_ack_outbox').get().assertion_json,body);db.close();
 const requests=fs.readFileSync(dbPath+'.requests','utf8').trim().split('\n');
 assert.deepEqual(requests,[`POST /ltds/v1/viewer-staged-acks/${config.task}`,`GET /ltds/v1/viewer-staged-acks/${config.task}/generations/${commitment.generation}`]);
 process.stdout.write(JSON.stringify({ok:true,environment:commitment.environment,loss:'sigkill_after_post_commit',replay:'lookup',task:commitment.taskId}));
})().catch(error=>{process.stderr.write(error.stack+'\n');process.exitCode=1});
