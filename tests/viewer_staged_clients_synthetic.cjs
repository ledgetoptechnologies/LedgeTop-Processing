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
const config=JSON.parse(process.env.SYNTHETIC_BRIDGE_CONFIG);
const binding={origin:config.origin,principalId:'viewer-stage',authorityId:config.authority,environment:'staging',credentialRef:'synthetic'};
const dbPath=process.env.SYNTHETIC_DB_PATH;
const sha=v=>crypto.createHash('sha256').update(v).digest('hex');
function migration(number){
 const source=fs.readFileSync(path.join(root,'apps/viewer/server/database.js'),'utf8');
 const match=source.match(new RegExp('\\{version:'+number+',name:\\x27[^\\x27]+\\x27,sql:`([\\s\\S]*?)`\\}'));
 if(!match)throw Error('expected exact Viewer migration absent');return match[1];
}
function setup(db,clock,fetchImpl=fetch){return {db,enabled:true,bindings:{provider:binding},resolveCredential:async()=>config.bearer,resolveCapability:async()=>config.capability,clock:()=>clock,fetchImpl};}
(async()=>{
 const mode=process.argv[2];
 if(mode==='kill'||mode==='replay'){
  const db=new DatabaseSync(dbPath);
  const fetchImpl=async(url,init)=>{const response=await fetch(url,init);if(mode==='kill'&&init.method==='POST'){await response.arrayBuffer();process.kill(process.pid,'SIGKILL');}return response;};
  await deliverOneStagedAck(setup(db,mode==='kill'?100000:200000,fetchImpl));db.close();return;
 }
 const opts={enabled:true,...binding,bearer:config.bearer,capability:config.capability,taskId:config.task};
 const commitment=await getStagedCommitment(opts);
 await assert.rejects(getStagedCommitment({...opts,bearer:config.capability}));
 const id=()=>crypto.randomUUID();const assertion={...commitment,attemptId:id(),modelId:id(),modelVersionId:id(),stagingCommitId:id()};
 let db=new DatabaseSync(dbPath);
 db.exec('CREATE TABLE processing_attempts(id TEXT PRIMARY KEY); CREATE TABLE processing_providers(id TEXT PRIMARY KEY);');
 db.exec(migration(46));db.exec(migration(47));
 db.prepare('INSERT INTO processing_attempts VALUES(?)').run(assertion.attemptId);db.prepare('INSERT INTO processing_providers VALUES(?)').run('provider');
 const body=JSON.stringify(assertion);
 db.prepare('INSERT INTO processing_staged_byte_ack_outbox(attempt_id,provider_id,staging_commit_id,assertion_json,created_at) VALUES(?,?,?,?,?)').run(assertion.attemptId,'provider',assertion.stagingCommitId,body,'2026');
 db.prepare("INSERT INTO processing_staged_ack_delivery(attempt_id,state,assertion_sha256,expected_origin,credential_ref,capability_sha256,updated_at) VALUES(?,'pending',?,?,?,?,0)").run(assertion.attemptId,sha(body),config.origin,'synthetic',sha(config.capability));db.close();
 const first=spawnSync(process.execPath,[__filename,'kill'],{env:process.env,encoding:'utf8',timeout:20000});
 assert.equal(first.signal,'SIGKILL',`sender was not killed: ${first.status} ${first.stderr}`);
 db=new DatabaseSync(dbPath);let row=db.prepare('SELECT * FROM processing_staged_ack_delivery').get();
 assert.equal(row.state,'uncertain');assert.ok(row.lease_token);assert.equal(row.lease_expires_at,160000);db.close();
 const second=spawnSync(process.execPath,[__filename,'replay'],{env:process.env,encoding:'utf8',timeout:20000});
 assert.equal(second.status,0,`sender restart failed: ${second.stderr}`);
 db=new DatabaseSync(dbPath);row=db.prepare('SELECT * FROM processing_staged_ack_delivery').get();
 assert.equal(row.state,'accepted');assert.equal(db.prepare('SELECT assertion_json FROM processing_staged_byte_ack_outbox').get().assertion_json,body);db.close();
 process.stdout.write(JSON.stringify({ok:true,environment:commitment.environment,loss:'sigkill_after_post_commit',replay:'lookup',task:commitment.taskId}));
})().catch(error=>{process.stderr.write(error.stack+'\n');process.exitCode=1});
