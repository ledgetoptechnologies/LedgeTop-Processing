'use strict';

const { createRequire } = require('node:module');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const viewerRequire = createRequire(process.env.LTDS_VIEWER_APP_ROOT + '/package.json');
const { openDatabase } = viewerRequire('./server/database');
const { ProcessingRepository } = viewerRequire('./server/processingRepository');
const { PreparedSubmissionOutbox } = viewerRequire('./server/preparedSubmissionOutbox');
const { SourceIntakeClientV2 } = viewerRequire('./server/sourceIntakeClientV2');
const { processSubmit } = viewerRequire('./server/processingWorker');

const config = JSON.parse(require('node:fs').readFileSync(process.env.LTDS_PAIRING_CONFIG, 'utf8'));
const environment = config.environment || 'staging';
const principalId = config.principalId || 'viewer-staging';
const db = openDatabase(config.databasePath);
async function main() {
try {
  const processing = new ProcessingRepository(db);
  const row = db.prepare('SELECT id FROM processing_attempts').get();
  if (!row) throw new Error('synthetic attempt missing');
  const attempt = processing.getAttempt(row.id);
  const submit = db.prepare("SELECT id,lease_owner FROM processing_jobs WHERE attempt_id=? AND job_type='submit' AND status='leased'").get(row.id);
  if (!submit || !submit.lease_owner) throw new Error('synthetic submit lease missing');
  const dataset = processing.getDataset(attempt.datasetId || processing.getTask(attempt.taskId).datasetId, true);
  assert.equal(dataset.files.length, config.withGcp ? 2 : 1);
  const file = dataset.files.find(item => item.relativePath === 'photo.jpg');
  assert.ok(file);
  const outbox = new PreparedSubmissionOutbox(db);
  const client = new SourceIntakeClientV2({ origin: config.origin, bearerToken: config.viewerBearer,
    environment, outbox, timeoutMs: 5000, transferTimeoutMs: 10000 });
  const snapshotPrivate = crypto.createPrivateKey(config.snapshotPrivateKeyPem);
  const attestationPrivate = crypto.createPrivateKey(config.attestationPrivateKeyPem);
  await processSubmit({ id: submit.id, attempt_id: row.id, lease_owner: submit.lease_owner }, {
    processing,
    config: { referenceSourceIntake: { enabled: true, providerId: attempt.providerId,
      origin: config.origin, authorityId: config.authorityId, principalId,
      environment, snapshotIssuerId: 'pairing-snapshot-issuer', snapshotKeyId: 'pairing-snapshot-key' } },
    sourceIntake: { client, outbox,
      snapshotPublicKey: crypto.createPublicKey(config.snapshotPublicKeyPem),
      snapshotSigner: { issuerId: 'pairing-snapshot-issuer', keyId: 'pairing-snapshot-key',
        sign: bytes => crypto.sign(null, bytes, snapshotPrivate) },
      attestationSigner: { issuerId: 'pairing-source-issuer', keyId: 'pairing-source-key',
        publicKey: crypto.createPublicKey(config.attestationPublicKeyPem),
        sign: bytes => crypto.sign(null, bytes, attestationPrivate) } },
    storage: { resolve(root, relative) {
      assert.equal(root, dataset.rootKey);
      const source = dataset.files.find(item => relative === `${dataset.relativePath}/${item.relativePath}`);
      assert.ok(source);
      return require('node:path').join(require('node:path').dirname(config.sourceFilePath), source.relativePath);
    } },
    providerCredentials: { resolve() { throw new Error('legacy submission must not run'); } }
  });
  assert.equal(db.prepare('SELECT status FROM processing_jobs WHERE id=?').get(submit.id).status, 'complete');
  assert.equal(db.prepare("SELECT COUNT(*) AS count FROM processing_jobs WHERE attempt_id=? AND job_type='reconcile' AND status='pending'").get(row.id).count, 1);
  // Use the application's normal enqueue delay and claim rather than forging
  // a reconcile row or editing its due time in the fixture.
  await new Promise(resolve => setTimeout(resolve, 5200));
  const reconcile = processing.claimJob('roundtrip-reconcile-owner');
  if (!reconcile || reconcile.job_type !== 'reconcile') throw new Error('synthetic reconcile claim failed');
  process.stdout.write(JSON.stringify({ attemptId: row.id, taskId: attempt.taskId,
    providerTaskId: attempt.providerTaskId, reconcileJobId: reconcile.id,
    reconcileOwner: reconcile.lease_owner }) + '\n');
} finally { db.close(); }
}

main().catch(error => {
  process.stderr.write((error.code || error.name) + ': ' + error.message + '\n');
  process.exitCode = 1;
});
