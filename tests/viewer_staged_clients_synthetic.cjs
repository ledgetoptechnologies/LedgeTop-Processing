'use strict';
// Test-only outbox: sender and commitment client are the staged Viewer source.
const assert=require('node:assert/strict');
const crypto=require('node:crypto');
const {DatabaseSync}=require('node:sqlite');
const path=require('node:path');
const root=process.env.VIEWER_SOURCE;
const {getStagedCommitment}=require(path.join(root,'apps/viewer/server/stagedCommitmentClient'));
const {deliverOneStagedAck}=require(path.join(root,'apps/viewer/server/stagedAckDelivery'));
const config=JSON.parse(process.env.SYNTHETIC_BRIDGE_CONFIG);
(async()=>{
 const binding={origin:config.origin,principalId:'viewer-stage',authorityId:config.authority,environment:'staging',credentialRef:'synthetic'};
 const opts={enabled:true,...binding,bearer:config.bearer,capability:config.capability,taskId:config.task};
 const commitment=await getStagedCommitment(opts);
 await assert.rejects(getStagedCommitment({...opts,bearer:config.capability}));
 const id=()=>crypto.randomUUID();const assertion={...commitment,attemptId:id(),modelId:id(),modelVersionId:id(),stagingCommitId:id()};
 const db=new DatabaseSync(':memory:');
 db.exec(`CREATE TABLE processing_staged_byte_ack_outbox(attempt_id TEXT PRIMARY KEY,provider_id TEXT,staging_commit_id TEXT,assertion_json TEXT,created_at TEXT); CREATE TABLE processing_staged_ack_delivery(attempt_id TEXT PRIMARY KEY,state TEXT,assertion_sha256 TEXT NOT NULL,binding_version INTEGER NOT NULL DEFAULT 1,expected_origin TEXT,credential_ref TEXT,capability_sha256 TEXT,lease_token TEXT,lease_expires_at INTEGER,next_attempt_at INTEGER DEFAULT 0,attempts INTEGER DEFAULT 0,error_code TEXT,updated_at INTEGER);`);
 const body=JSON.stringify(assertion),sha=v=>crypto.createHash('sha256').update(v).digest('hex');
 db.prepare('INSERT INTO processing_staged_byte_ack_outbox VALUES(?,?,?,?,?)').run(assertion.attemptId,'provider',assertion.stagingCommitId,body,'2026');
 db.prepare("INSERT INTO processing_staged_ack_delivery(attempt_id,state,assertion_sha256,expected_origin,credential_ref,capability_sha256,updated_at) VALUES(?,'pending',?,?,?,?,0)").run(assertion.attemptId,sha(body),config.origin,'synthetic',sha(config.capability));
 let clock=100000;let drop=true;
 const fetchImpl=async(url,init)=>{const response=await fetch(url,init);if(drop&&init.method==='POST'){drop=false;await response.arrayBuffer();throw Error('synthetic lost response after coordinator commit');}return response;};
 const setup={db,enabled:true,bindings:{provider:binding},resolveCredential:async()=>config.bearer,resolveCapability:async()=>config.capability,clock:()=>clock,fetchImpl};
 await deliverOneStagedAck(setup);assert.equal(db.prepare('SELECT state FROM processing_staged_ack_delivery').get().state,'uncertain');
 clock+=100000;await deliverOneStagedAck(setup);assert.equal(db.prepare('SELECT state FROM processing_staged_ack_delivery').get().state,'accepted');
 db.close();process.stdout.write(JSON.stringify({ok:true,environment:commitment.environment,loss:'post_commit_response_discarded',replay:'lookup',task:commitment.taskId}));
})().catch(error=>{process.stderr.write(error.stack+'\n');process.exitCode=1});
