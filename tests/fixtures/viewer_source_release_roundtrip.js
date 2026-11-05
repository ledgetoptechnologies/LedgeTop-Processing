'use strict';

// Real application repositories/client/worker, reopened for every Python phase.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const { createRequire } = require('node:module');
const viewerRequire = createRequire(process.env.LTDS_VIEWER_APP_ROOT + '/package.json');
const { openDatabase } = viewerRequire('./server/database');
const { ProcessingRepository } = viewerRequire('./server/processingRepository');
const { PreparedSubmissionOutbox } = viewerRequire('./server/preparedSubmissionOutbox');
const { StorageManager } = viewerRequire('./server/storageManager');
const { SourceReleaseOutboxV1 } = viewerRequire('./server/sourceReleaseOutboxV1');
const { SourceReleaseClientV1 } = viewerRequire('./server/sourceReleaseClientV1');
const { processSourceReleaseAttempt } = viewerRequire('./server/sourceReleaseWorkerV1');
const config = JSON.parse(fs.readFileSync(process.env.LTDS_PAIRING_CONFIG, 'utf8'));

(async () => {
  const db = openDatabase(config.databasePath);
  try {
    const processing = new ProcessingRepository(db);
    const sourceIntake = { outbox: new PreparedSubmissionOutbox(db) };
    const outbox = new SourceReleaseOutboxV1(db);
    const storage = new StorageManager({ datasetsMount: config.datasetMount, modelsMount: config.modelsRoot,
      cacheMount: config.cacheMount, trashMount: config.trashMount, storageReserveBytes: 0, storageReservePercent: 0 });
    storage.initialize();
    const attemptId = db.prepare('SELECT id FROM processing_attempts ORDER BY created_at,id LIMIT 1').get().id;
    const signer = { issuerId: 'pairing-release-issuer', keyId: 'pairing-release-key',
      principalId: 'viewer-staging', privateKeyPem: config.releasePrivateKeyPem };
    const sourceRelease = { enabled: true, outbox, signer,
      trustedReleaseKeys: [{ environment: 'staging', issuerId: signer.issuerId, keyId: signer.keyId,
        principalId: signer.principalId, publicKeyPem: config.releasePublicKeyPem }],
      client: new SourceReleaseClientV1({ origin: config.origin, bearerToken: config.viewerBearer,
        environment: 'staging', timeoutMs: 5000 }) };
    const before = outbox.getIntent(attemptId);
    const run = () => processSourceReleaseAttempt({ attemptId, processing, storage, sourceIntake, sourceRelease });
    if (config.sourceReleasePhase === 'lost-post') {
      assert.equal(before, null);
      await assert.rejects(run(), { code: 'source_release_transport_uncertain' });
      const intent = outbox.getIntent(attemptId);
      assert.equal(intent.state, 'prepared');
      assert.ok(intent.request && intent.requestSha256);
      process.stdout.write(JSON.stringify({ lostReply: true, requestSha256: intent.requestSha256 }) + '\n');
      return;
    }
    assert.ok(before.requestSha256);
    const result = await run();
    const intent = outbox.getIntent(attemptId);
    assert.equal(intent.requestSha256, before.requestSha256, 'durable signed request cannot change across restart');
    if (config.sourceReleasePhase === 'resume') {
      assert.equal(result.state, 'waiting_policy');
      assert.equal(intent.state, 'submitted');
    } else {
      assert.equal(result.status, 'released');
      assert.equal(intent.state, 'released');
      assert.ok(intent.receipt && intent.receiptSha256);
      assert.equal(intent.receipt.removedObjectCount, intent.authorization.eligibleObjects.length);
      assert.equal(intent.receipt.removedBytes,
        intent.authorization.eligibleObjects.reduce((sum, obj) => sum + obj.byteSize, 0));
    }
    process.stdout.write(JSON.stringify({ state: intent.status.state,
      requestSha256: intent.requestSha256, receiptSha256: intent.receiptSha256,
      preservedClosureSha256: intent.receipt?.preservedClosureSha256 }) + '\n');
  } finally { db.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
