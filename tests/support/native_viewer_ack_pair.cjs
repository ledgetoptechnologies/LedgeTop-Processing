'use strict';
// A synthetic Viewer outbox using the real release repositories and sender.
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const crypto = require('node:crypto');
const readline = require('node:readline');
const assert = require('node:assert/strict');
const viewer = process.env.PINNED_VIEWER_ROOT;
if (!viewer || !process.env.ACK_TEST_ORIGIN || !process.env.ACK_TEST_ARCHIVE_SHA) throw new Error('missing isolated test inputs');
const {openDatabase} = require(path.join(viewer, 'apps/viewer/server/database'));
const {ProcessingRepository} = require(path.join(viewer, 'apps/viewer/server/processingRepository'));
const {ViewerRepository} = require(path.join(viewer, 'apps/viewer/server/repository'));
const {deliverOneViewerAcceptanceAck} = require(path.join(viewer, 'apps/viewer/server/viewerAcceptanceAckDelivery'));
const sha = v => crypto.createHash('sha256').update(v).digest('hex');
const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'native-viewer-pair-'));
const database = path.join(directory, 'db.sqlite');
let db;
try {
  db = openDatabase(database);
  let processing = new ProcessingRepository(db);
  const repository = new ViewerRepository(db);
  const project = processing.createProject({displayName:'Synthetic ACK test'});
  const dataset = processing.createDataset({projectId:project.id, displayName:'Synthetic dataset', rootKey:'datasets', relativePath:'source'});
  const files = [{relativePath:'image.jpg', byteSize:4, sha256:sha('jpeg'), processingRole:'image'}];
  processing.finalizeDataset(dataset.id, files, sha(JSON.stringify(files.map(({relativePath,byteSize,sha256})=>({relativePath,byteSize,sha256})))));
  const marker = {version:1, transport:'https', pathTemplate:'/task/{taskId}/viewer-acceptance'};
  const provider = processing.upsertProvider({type:'nodeodm', displayName:'Synthetic TLS provider', endpoint:process.env.ACK_TEST_ORIGIN, enabled:true, capabilities:{viewerAcceptanceAck:marker}});
  const task = processing.createTask({projectId:project.id, datasetId:dataset.id, displayName:'Synthetic task'});
  const attempt = processing.createAttempt({taskId:task.id, providerId:provider.id});
  const job = processing.claimJob('synthetic-submit');
  const entries = [{relativePath:'odm_dem/dsm.tif', byteSize:4, sha256:sha('tiff')}];
  db.prepare("UPDATE processing_jobs SET job_type='ingest' WHERE id=?").run(job.id);
  assert.ok(processing.recordAttemptArchiveReceipt(job.id, 'synthetic-submit', {archiveSha256:process.env.ACK_TEST_ARCHIVE_SHA, archiveFiles:entries, archiveManifestSha256:sha(JSON.stringify(entries))}));
  const model = repository.upsertModelVersion({provider:'nodeodm', providerModelId:attempt.providerTaskId, providerVersionId:attempt.id, displayName:'Synthetic result', status:'ready', assets:[], makeActive:false});
  const versionId = db.prepare('SELECT id FROM model_versions WHERE model_id=?').get(model.id).id;
  processing.setAttemptResult(attempt.id, model.id, versionId);
  processing.registerModelOutput({versionId, modelId:model.id, taskId:task.id, attemptId:attempt.id, projectId:project.id, relativePath:`${task.id}/${attempt.id}`, status:'ready', byteSize:4, assetCount:0});
  const now = new Date().toISOString();
  db.prepare("UPDATE processing_attempts SET status='ready_for_review',result_model_id=?,result_model_version_id=?,ingested_at=?,updated_at=? WHERE id=?").run(model.id, versionId, now, now, attempt.id);
  db.prepare("UPDATE processing_tasks SET status='ready_for_review',updated_at=? WHERE id=?").run(now, task.id);
  assert.equal(processing.enqueueViewerAcceptanceAck(attempt), true);
  const initial = db.prepare('SELECT event_id,payload_json,status FROM viewer_acceptance_ack_outbox').get();
  assert.equal(initial.status, 'pending');
  process.stdout.write(JSON.stringify({state:'ready', providerTaskId:attempt.providerTaskId, eventId:initial.event_id})+'\n');
  const commands = readline.createInterface({input:process.stdin, crlfDelay:Infinity});
  let step = 0;
  commands.on('line', async line => {
    try {
      if (line !== 'go' || step > 1) throw new Error('invalid synthetic test command');
      const credentials = {resolve:()=> 'synthetic-staging-token'};
      if (step === 1) {
        db.prepare("UPDATE viewer_acceptance_ack_outbox SET available_at='2000-01-01T00:00:00.000Z'").run();
        db.close();
        db = openDatabase(database);
        processing = new ProcessingRepository(db);
      }
      assert.equal(await deliverOneViewerAcceptanceAck(processing, credentials, step ? 'synthetic-worker-after' : 'synthetic-worker-before'), true);
      const row = db.prepare('SELECT event_id,payload_json,status,attempt_count FROM viewer_acceptance_ack_outbox').get();
      assert.equal(db.prepare('SELECT COUNT(*) AS n FROM viewer_acceptance_ack_outbox').get().n, 1);
      assert.equal(row.event_id, initial.event_id);
      assert.equal(row.payload_json, initial.payload_json);
      assert.equal(row.status, step ? 'delivered' : 'pending');
      assert.equal(row.attempt_count, step + 1);
      process.stdout.write(JSON.stringify({state:step ? 'delivered' : 'pending', eventId:row.event_id, attemptCount:row.attempt_count})+'\n');
      if (++step === 2) { commands.close(); db.close(); db = null; fs.rmSync(directory,{recursive:true,force:true}); }
    } catch (error) { console.error(error.stack); commands.close(); if(db)db.close(); fs.rmSync(directory,{recursive:true,force:true}); process.exitCode=1; }
  });
} catch (error) { if(db)db.close(); fs.rmSync(directory,{recursive:true,force:true}); throw error; }
