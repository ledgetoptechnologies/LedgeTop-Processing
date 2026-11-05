'use strict';

// Real application repositories/client/worker, reopened for every Python phase.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const crypto = require('node:crypto');
const path = require('node:path');
const { createRequire } = require('node:module');
const viewerRequire = createRequire(process.env.LTDS_VIEWER_APP_ROOT + '/package.json');
const { openDatabase } = viewerRequire('./server/database');
const { ProcessingRepository } = viewerRequire('./server/processingRepository');
const { PreparedSubmissionOutbox } = viewerRequire('./server/preparedSubmissionOutbox');
const { StorageManager } = viewerRequire('./server/storageManager');
const { SourceReleaseOutboxV1 } = viewerRequire('./server/sourceReleaseOutboxV1');
const { SourceReleaseClientV1 } = viewerRequire('./server/sourceReleaseClientV1');
const { processSourceReleaseAttempt, reconcileSourceReleaseQueue } = viewerRequire('./server/sourceReleaseWorkerV1');
const config = JSON.parse(fs.readFileSync(process.env.LTDS_PAIRING_CONFIG, 'utf8'));

(async () => {
  const db = openDatabase(config.databasePath);
  try {
    const processing = new ProcessingRepository(db);
    const sourceIntake = { outbox: new PreparedSubmissionOutbox(db),
      snapshotPublicKey: crypto.createPublicKey(config.snapshotPublicKeyPem),
      snapshotSigner: { issuerId: 'pairing-snapshot-issuer', keyId: 'pairing-snapshot-key' } };
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
    if (config.sourceReleasePhase === 'rerun') {
      const original = processing.getAttempt(attemptId);
      const next = processing.createAttempt({ taskId: original.taskId, providerId: original.providerId,
        options: original.options, capabilityFingerprint: config.capabilityFingerprint });
      assert.notEqual(next.id, original.id);
      assert.equal(processing.getTask(original.taskId).activeAttemptId, next.id);
      assert.equal(processing.getAttempt(original.id).status, 'published');
      process.stdout.write(JSON.stringify({ superseded: true }) + '\n');
      return;
    }
    if (config.sourceReleasePhase === 'terminal-crash') {
      const prepare = db.prepare.bind(db);
      db.prepare = sql => {
        if (sql.includes("SET status='released',released_at=? WHERE snapshot_id=?")) {
          return { run() {
            const terminal = outbox.getIntent(attemptId);
            assert.equal(terminal.state, 'released', 'receipt is durable before snapshot retirement begins');
            assert.equal(prepare('SELECT status FROM viewer_source_snapshots WHERE attempt_id=?').get(attemptId).status, 'accepted');
            fs.writeSync(1, JSON.stringify({ snapshotReleaseInterrupted: true, receiptSha256: terminal.receiptSha256 }) + '\n');
            process.exit(42); // Actual process death at the durable local handoff boundary.
          } };
        }
        return prepare(sql);
      };
      await run();
      throw new Error('synthetic snapshot-retirement crash seam was not reached');
    }
    if (config.sourceReleasePhase === 'lost-post') {
      assert.equal(before, null);
      await assert.rejects(run(), error => {
        if (error?.code !== 'source_release_transport_uncertain') console.error(error?.stack);
        return error?.code === 'source_release_transport_uncertain';
      });
      const intent = outbox.getIntent(attemptId);
      assert.equal(intent.state, 'prepared');
      assert.equal(db.prepare('SELECT status FROM viewer_source_snapshots WHERE attempt_id=?').get(attemptId).status, 'accepted');
      const datasetId = db.prepare('SELECT dataset_id FROM viewer_source_snapshots WHERE attempt_id=?').get(attemptId).dataset_id;
      assert.equal(processing.hasHeldSourceSnapshot(datasetId), true, 'accepted snapshots stay operationally held until terminal receipt');
      assert.ok(intent.request && intent.requestSha256);
      process.stdout.write(JSON.stringify({ lostReply: true, requestSha256: intent.requestSha256 }) + '\n');
      return;
    }
    assert.ok(before.requestSha256);
    let outputOpens = 0;
    const originalOpen = fs.openSync;
    fs.openSync = function(target, ...args) {
      if (typeof target === 'string' && path.resolve(target).startsWith(path.resolve(config.modelsRoot) + path.sep)) outputOpens++;
      return originalOpen.call(this, target, ...args);
    };
    let result;
    try {
      if (config.sourceReleasePhase === 'terminal') {
        assert.equal(before.state, 'released');
        assert.equal(db.prepare('SELECT status FROM viewer_source_snapshots WHERE attempt_id=?').get(attemptId).status, 'accepted');
        const repaired = await reconcileSourceReleaseQueue({ processing, storage, sourceIntake, sourceRelease,
          now: () => new Date(Date.now() + 31_000).toISOString() });
        assert.equal(repaired.scanned, 1, 'ordinary bounded background reconciliation repairs the interrupted hold');
        result = repaired.results[0];
      } else result = await run();
    } finally { fs.openSync = originalOpen; }
    const intent = outbox.getIntent(attemptId);
    assert.equal(intent.requestSha256, before.requestSha256, 'durable signed request cannot change across restart');
    if (config.sourceReleasePhase === 'resume') {
      assert.equal(result.state, 'waiting_policy');
      assert.equal(intent.state, 'submitted');
      assert.equal(outputOpens, 0, 'policy polling must not rehash every large Viewer output for 24 hours');
    } else {
      assert.equal(result.status, 'released');
      assert.equal(intent.state, 'released');
      assert.equal(db.prepare('SELECT status FROM viewer_source_snapshots WHERE attempt_id=?').get(attemptId).status, 'released');
      const datasetId = db.prepare('SELECT dataset_id FROM viewer_source_snapshots WHERE attempt_id=?').get(attemptId).dataset_id;
      assert.equal(processing.hasHeldSourceSnapshot(datasetId), false, 'only the matching terminal receipt retires the operational hold');
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
