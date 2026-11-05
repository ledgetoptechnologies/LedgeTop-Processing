'use strict';

const { createRequire } = require('node:module');
const viewerRequire = createRequire(process.env.LTDS_VIEWER_APP_ROOT + '/package.json');
const { openDatabase } = viewerRequire('./server/database');
const { ProcessingRepository } = viewerRequire('./server/processingRepository');

const config = JSON.parse(require('node:fs').readFileSync(process.env.LTDS_PAIRING_CONFIG, 'utf8'));
const db = openDatabase(config.databasePath);
try {
  const processing = new ProcessingRepository(db);
  const row = db.prepare('SELECT id FROM processing_attempts').get();
  if (!row) throw new Error('synthetic attempt missing');
  const attempt = processing.getAttempt(row.id);
  const submit = db.prepare("SELECT id,lease_owner FROM processing_jobs WHERE attempt_id=? AND job_type='submit' AND status='leased'").get(row.id);
  if (!submit || !submit.lease_owner) throw new Error('synthetic submit lease missing');
  if (!processing.transitionAttemptForJob(submit.id, submit.lease_owner, 'queued_upstream', { progress: 0 }))
    throw new Error('synthetic submit transition failed');
  if (!processing.completeAndEnqueueJob(submit.id, submit.lease_owner, row.id, 'reconcile',
      new Date(Date.now() - 1000).toISOString())) throw new Error('synthetic reconcile enqueue failed');
  const reconcile = processing.claimJob('roundtrip-reconcile-owner');
  if (!reconcile || reconcile.job_type !== 'reconcile') throw new Error('synthetic reconcile claim failed');
  process.stdout.write(JSON.stringify({ attemptId: row.id, taskId: attempt.taskId,
    providerTaskId: attempt.providerTaskId, reconcileJobId: reconcile.id,
    reconcileOwner: reconcile.lease_owner }) + '\n');
} finally { db.close(); }
