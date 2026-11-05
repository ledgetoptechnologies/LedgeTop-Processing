'use strict';

const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const { createRequire } = require('node:module');
const viewerRequire = createRequire(path.join(process.env.LTDS_VIEWER_APP_ROOT, 'package.json'));
const { openDatabase } = viewerRequire('./server/database');
const { ProcessingRepository } = viewerRequire('./server/processingRepository');
const { ViewerRepository } = viewerRequire('./server/repository');
const { StorageManager } = viewerRequire('./server/storageManager');
const { PreparedSubmissionOutbox } = viewerRequire('./server/preparedSubmissionOutbox');
const { processIngest } = viewerRequire('./server/processingWorker');

const config = JSON.parse(fs.readFileSync(process.env.LTDS_PAIRING_CONFIG, 'utf8'));
const database = openDatabase(config.databasePath);

async function main() {
  try {
    const processing = new ProcessingRepository(database);
    const repository = new ViewerRepository(database);
    const attemptId = database.prepare('SELECT id FROM processing_attempts').get()?.id;
    assert.ok(attemptId, 'the V2 pairing attempt exists');
    const attempt = processing.getAttempt(attemptId);
    const provider = processing.getProvider(attempt.providerId);
    assert.equal(attempt.status, 'ingesting', 'accepted result is awaiting local ingestion');
    const job = processing.claimJob('roundtrip-ingest-owner');
    assert.ok(job && job.job_type === 'ingest' && job.attempt_id === attemptId,
      'the durable ingest queue supplied the job');

    const storageConfig = {
      datasetsMount: config.datasetMount,
      modelsMount: config.modelsRoot,
      cacheMount: config.cacheMount,
      trashMount: config.trashMount,
      storageReserveBytes: 0,
      storageReservePercent: 0,
    };
    const storage = new StorageManager(storageConfig);
    storage.initialize();
    const sourceIntake = { outbox: new PreparedSubmissionOutbox(database) };
    await processIngest(job, {
      processing,
      repository,
      storage,
      config: {
        referenceSourceIntake: { enabled: true, providerId: provider.id,
          origin: provider.endpoint, environment: 'staging' },
        meshDerivativesEnabled: false,
        opsBaseUrl: config.ingestOpsBaseUrl,
      },
      providerCredentials: { resolve() { throw new Error('V2 ingest must not resolve legacy provider credentials'); } },
      sourceIntake,
      signal: new AbortController().signal,
    });

    const updated = processing.getAttempt(attemptId);
    assert.equal(updated.status, 'derivatives');
    const output = processing.getModelOutput(updated.resultModelVersionId);
    assert.equal(output.status, 'staged');
    const model = repository.getModelVersion(updated.resultModelId, updated.resultModelVersionId);
    const assets = model.activeVersion.assets;
    assert.deepEqual(assets.map(asset => asset.kind).sort(), ['ept', 'glb', 'obj']);
    assert.ok(assets.every(asset => asset.published === false && /^[a-f0-9]{64}$/.test(asset.sha256)));
    for (const asset of assets) {
      const absolute = storage.resolve(asset.rootKey, asset.relativePath, { mustExist: true });
      assert.equal(crypto.createHash('sha256').update(fs.readFileSync(absolute)).digest('hex'), asset.sha256,
        `registered hash matches managed bytes for ${asset.kind}`);
    }
    const ept = assets.find(asset => asset.kind === 'ept');
    const eptRow = database.prepare('SELECT id FROM model_assets WHERE version_id=? AND kind=\'ept\'')
      .get(updated.resultModelVersionId);
    const eptFileCount = database.prepare('SELECT COUNT(*) AS count FROM model_asset_files WHERE asset_id=?')
      .get(eptRow.id).count;
    assert.equal(eptFileCount, 3, 'one-point EPT closure includes header, data and hierarchy');
    assert.equal(JSON.parse(fs.readFileSync(storage.resolve(ept.rootKey, ept.relativePath, { mustExist: true }), 'utf8')).points, 1);

    const derivative = database.prepare(`SELECT id,status,derivative_type,request_json FROM derivative_jobs
      WHERE attempt_id=? ORDER BY created_at DESC,id DESC LIMIT 1`).get(attemptId);
    assert.ok(derivative);
    assert.equal(derivative.derivative_type, 'mesh_tiles');
    assert.equal(derivative.status, 'pending');
    assert.equal(JSON.parse(derivative.request_json).optional, false,
      'the required mesh derivative remains mandatory even when optional mesh derivatives are disabled');
    const snapshot = processing.derivativeInputSnapshot(derivative.id);
    assert.ok(snapshot && snapshot.fileCount === 4);
    assert.deepEqual(new Set(snapshot.files.map(file => file.role)), new Set(['mesh_glb', 'mesh_mtl', 'mesh_obj', 'mesh_texture']));
    assert.equal(database.prepare('SELECT active_version_id FROM models WHERE id=?').get(updated.resultModelId).active_version_id,
      null, 'ingest does not publish the staged model');
    process.stdout.write(JSON.stringify({ attemptStatus: updated.status, outputStatus: output.status,
      derivativeStatus: derivative.status, derivativeType: derivative.derivative_type,
      derivativeOptional: JSON.parse(derivative.request_json).optional,
      trustedInputSnapshot: Boolean(snapshot), assetKinds: assets.map(asset => asset.kind),
      eptFileCount, assetsHaveDigests: true, published: false }) + '\n');
  } finally {
    database.close();
  }
}

main().catch(error => {
  process.stderr.write(`${error.code || error.name}: ${error.message}\n`);
  process.exitCode = 1;
});
