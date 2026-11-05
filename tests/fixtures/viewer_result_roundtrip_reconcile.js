'use strict';

const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const { createRequire } = require('node:module');

const viewerRequire = createRequire(process.env.LTDS_VIEWER_APP_ROOT + '/package.json');
const { openDatabase } = viewerRequire('./server/database');
const { ProcessingRepository } = viewerRequire('./server/processingRepository');
const { PreparedSubmissionOutbox } = viewerRequire('./server/preparedSubmissionOutbox');
const { SourceIntakeClientV2 } = viewerRequire('./server/sourceIntakeClientV2');
const { processReconcile } = viewerRequire('./server/processingWorker');
const { createViewerReceiptV2SignerCandidate } = viewerRequire('./server/referenceReceiptV2Candidate');

const config = JSON.parse(fs.readFileSync(process.env.LTDS_PAIRING_CONFIG, 'utf8'));
const environment = config.environment || 'staging';
const expectedBytes = Buffer.from(config.resultBytesBase64, 'base64');
const modelRoot = path.resolve(config.modelsRoot);
fs.mkdirSync(modelRoot, { recursive: true });
let db = openDatabase(config.databasePath);
try {
  let processing = new ProcessingRepository(db);
  let outbox = new PreparedSubmissionOutbox(db);
  const attemptRow = db.prepare('SELECT id FROM processing_attempts').get();
  assert.ok(attemptRow);
  const attempt = processing.getAttempt(attemptRow.id);
  const task = processing.getTask(attempt.taskId);
  const provider = processing.getProvider(attempt.providerId);
  const client = new SourceIntakeClientV2({ origin: config.origin, bearerToken: config.viewerBearer,
    environment, outbox, timeoutMs: 5000, transferTimeoutMs: 10000 });
  const privateKey = crypto.createPrivateKey(config.copyPrivateKeyPem);
  const signer = createViewerReceiptV2SignerCandidate({ issuerId: 'pairing-copy-issuer',
    keyId: 'pairing-copy-key', privateKeyPem: config.copyPrivateKeyPem });
  signer.issuerId = 'pairing-copy-issuer';
  signer.keyId = 'pairing-copy-key';
  assert.ok(privateKey);
  const sourceIntake = { client, outbox, copySigner: signer, copyPublicKeyPem: config.copyPublicKeyPem };
  const storage = {
    roots: { models: modelRoot },
    requireSpace(rootKey, bytes) { assert.equal(rootKey, 'models'); assert.ok(Number.isSafeInteger(bytes)); },
    resolve(rootKey, relative, { mustExist = false } = {}) {
      assert.equal(rootKey, 'models');
      const target = path.resolve(modelRoot, ...relative.split('/'));
      if (target !== modelRoot && !target.startsWith(modelRoot + path.sep)) throw new Error('storage escape');
      if (mustExist) return fs.realpathSync(target);
      return target;
    }
  };
  const job = { id: config.reconcileJobId, attempt_id: attempt.id, lease_owner: config.reconcileOwner };
  const dependencies = { processing, config: { referenceSourceIntake: {
    enabled: true, providerId: provider.id, origin: provider.endpoint, environment } }, sourceIntake, storage };
  (async () => {
    let firstCode = null;
    try { await processReconcile(job, dependencies); }
    catch (error) { firstCode = error.code || error.name; }
    assert.equal(firstCode, 'source_intake_transport_uncertain');
    const intentAfterLostReply = outbox.getIntent(attempt.id);
    assert.ok(intentAfterLostReply.copyRequestCanonical);
    assert.equal(intentAfterLostReply.copyAckCanonical, null);
    const object = JSON.parse(intentAfterLostReply.resultManifestCanonical).objects[0];
    const localPath = path.join(modelRoot, task.id, attempt.id, 'reference-v2-results', 'accepted_objects', object.id + '.bin');
    assert.deepEqual(fs.readFileSync(localPath), expectedBytes, 'Viewer must durably retain exact bytes before ACK retry');

    await processReconcile(job, dependencies);
    const accepted = outbox.getIntent(attempt.id);
    assert.ok(accepted.copyAckCanonical);
    const ack = JSON.parse(accepted.copyAckCanonical);
    assert.equal(ack.fullyAccepted, true);
    assert.equal(ack.job.phase, 'completed');
    assert.deepEqual(fs.readFileSync(localPath), expectedBytes, 'accepted Viewer copy remains after ACK');
    process.stdout.write(JSON.stringify({ firstReplyLost: true, exactRequestPersisted: true,
      resultBytesRetained: true, accepted: true, receiptId: ack.viewerReceipt.receiptId,
      storageReceiptId: ack.viewerReceipt.storageReceiptId }) + '\n');
  })().catch(error => { process.stderr.write((error.code || error.name) + ': ' + error.message + '\n'); process.exitCode = 1; })
    .finally(() => { try { db.close(); } catch {} });
} finally {
  // The async reconciliation owns the DB until its promise settles.
}
