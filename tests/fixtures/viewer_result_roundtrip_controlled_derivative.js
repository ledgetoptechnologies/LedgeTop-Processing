'use strict';

const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const express = require('express');
const fs = require('node:fs');
const path = require('node:path');
const { createRequire } = require('node:module');
const viewerRequire = createRequire(path.join(process.env.LTDS_VIEWER_APP_ROOT, 'package.json'));
const { openDatabase } = viewerRequire('./server/database');
const { ProcessingRepository } = viewerRequire('./server/processingRepository');
const { ViewerRepository } = viewerRequire('./server/repository');
const { StorageManager } = viewerRequire('./server/storageManager');
const { processOneDerivative } = viewerRequire('./server/derivativeWorker');
const { config: baseConfig } = viewerRequire('./server/config');
const { createProcessingApi } = viewerRequire('./server/processingApi');
const auth = viewerRequire('./server/auth');
const converterPolicy = viewerRequire('./lod-converter-policy.cjs');

const pairing = JSON.parse(fs.readFileSync(process.env.LTDS_PAIRING_CONFIG, 'utf8'));
const database = openDatabase(pairing.databasePath);

async function main() {
  let server;
  try {
    assert.equal(pairing.controlledConversionQualification, true,
      'this helper is only valid for an explicitly requested real-converter qualification');
    const binary = pairing.obj2TilesBin;
    assert.equal(typeof binary, 'string');
    assert.ok(path.isAbsolute(binary), 'converter path must be absolute');
    fs.accessSync(binary, fs.constants.X_OK);
    const binaryHash = crypto.createHash('sha256').update(fs.readFileSync(binary)).digest('hex');
    assert.ok(converterPolicy.CONTROLLED_CONVERTER_BINARY_SHA256.includes(binaryHash),
      `converter binary is not approved by the mounted Viewer build-info policy: ${binaryHash}`);

    const processing = new ProcessingRepository(database);
    const repository = new ViewerRepository(database);
    const attemptRow = database.prepare('SELECT id FROM processing_attempts ORDER BY created_at,id LIMIT 1').get();
    assert.ok(attemptRow, 'accepted V2 attempt exists');
    const attempt = processing.getAttempt(attemptRow.id);
    const outputBefore = processing.getModelOutput(attempt.resultModelVersionId);
    assert.equal(attempt.status, 'derivatives');
    assert.equal(outputBefore.status, 'staged');
    const jobBefore = database.prepare(`SELECT id,status,derivative_type,request_json FROM derivative_jobs
      WHERE attempt_id=? ORDER BY created_at DESC,id DESC LIMIT 1`).get(attempt.id);
    assert.ok(jobBefore);
    assert.equal(jobBefore.status, 'pending');
    assert.equal(jobBefore.derivative_type, 'mesh_tiles');
    assert.equal(JSON.parse(jobBefore.request_json).optional, false, 'mesh tiles are a required derivative');

    const storage = new StorageManager({
      datasetsMount: pairing.datasetMount,
      modelsMount: pairing.modelsRoot,
      cacheMount: pairing.cacheMount,
      trashMount: pairing.trashMount,
      storageReserveBytes: 0,
      storageReservePercent: 0,
    });
    storage.initialize();
    const runtimeConfig = {
      ...baseConfig,
      meshDerivativesEnabled: true,
      obj2TilesBin: binary,
      opsBaseUrl: pairing.ingestOpsBaseUrl,
    };

    assert.equal(await processOneDerivative({ processing, storage, config: runtimeConfig },
      'controlled-conversion-qualification-worker'), true, 'real derivative worker claimed the queued job');

    const derivative = database.prepare(`SELECT status,result_json FROM derivative_jobs WHERE id=?`).get(jobBefore.id);
    assert.equal(derivative.status, 'complete', `real derivative did not complete: ${derivative.result_json || ''}`);
    const result = JSON.parse(derivative.result_json || '{}');
    assert.equal(result.verified, true);
    assert.equal(result.phase, 'complete');
    const readyAttempt = processing.getAttempt(attempt.id);
    const readyOutput = processing.getModelOutput(attempt.resultModelVersionId);
    assert.equal(readyAttempt.status, 'ready_for_review');
    assert.equal(readyOutput.status, 'ready');
    const stagedVersion = repository.getModelVersion(attempt.resultModelId, attempt.resultModelVersionId);
    assert.ok(stagedVersion.activeVersion.lodVerifierReceipt, 'verified derivative receipt is persisted privately');
    const tiles = stagedVersion.activeVersion.assets.find((asset) => asset.kind === 'tiles');
    assert.ok(tiles && tiles.manifestSha256 && tiles.sha256);
    const tilesetPath = storage.resolve(tiles.rootKey, tiles.relativePath, { mustExist: true });
    const tileset = JSON.parse(fs.readFileSync(tilesetPath, 'utf8'));
    assert.ok(tileset.root && Array.isArray(tileset.root.children) && tileset.root.children.length > 0,
      'actual Obj2Tiles output has a non-empty 3D Tiles root');
    assert.equal(tiles.published, false, 'verified output remains staged until explicit publication');

    const adminToken = crypto.randomBytes(32).toString('base64url');
    processing.createAdminSession({
      tokenHash: auth.hashToken(adminToken),
      subject: 'ops:controlled-conversion-qualification',
      permissions: ['viewer.processing.publish'],
      displayUnits: 'metric',
      expiresAt: new Date(Date.now() + 60_000).toISOString(),
    });
    const app = express();
    app.use(express.json({ verify: (req, _res, bytes) => { req.rawBody = Buffer.from(bytes); } }));
    app.use(createProcessingApi({ repository, processing, storage, config: runtimeConfig }));
    server = await new Promise((resolve) => {
      const listener = app.listen(0, '127.0.0.1', () => resolve(listener));
    });
    const response = await fetch(`http://127.0.0.1:${server.address().port}/api/v1/attempts/${encodeURIComponent(attempt.id)}/publish`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${adminToken}`,
        'content-type': 'application/json',
        'idempotency-key': 'controlled-conversion-publish',
      },
      body: JSON.stringify({ selectedAssetKinds: ['glb', 'tiles'] }),
    });
    assert.equal(response.status, 200, `actual publish endpoint rejected verified model: ${await response.text()}`);

    const published = repository.getModel(attempt.resultModelId);
    const publicConfig = viewerRequire('./server/apiV1').toViewerConfig(published);
    const publishedAttempt = processing.getAttempt(attempt.id);
    const publishedOutput = processing.getModelOutput(attempt.resultModelVersionId);
    assert.equal(publishedAttempt.status, 'published');
    assert.equal(publishedOutput.status, 'published');
    assert.equal(published.activeVersion.id, attempt.resultModelVersionId);
    assert.equal(published.activeVersion.assets.find((asset) => asset.kind === 'tiles')?.published, true);
    assert.equal(published.activeVersion.assets.some((asset) => asset.kind === 'obj'), false,
      'the public model omits the source OBJ used only as private provenance evidence');
    const persistedVersion = repository.getModelVersion(attempt.resultModelId, attempt.resultModelVersionId);
    assert.equal(persistedVersion.activeVersion.assets.find((asset) => asset.kind === 'obj')?.published, false,
      'the source OBJ remains registered privately while supporting provenance verification');
    assert.ok(publicConfig.assets.tiles);
    assert.equal(publicConfig.assets.obj, null);
    assert.equal(publicConfig.lodProvenanceVerified, true);
    const audit = database.prepare(`SELECT COUNT(*) AS count FROM audit_events
      WHERE action='derivative.tiles_verified' AND entity_id=?`).get(jobBefore.id);
    assert.equal(audit.count, 1, 'publication is backed by the actual derivative verification receipt');

    process.stdout.write(JSON.stringify({ derivativeStatus: derivative.status,
      verifiedReceipt: Boolean(stagedVersion.activeVersion.lodVerifierReceipt),
      attemptStatus: publishedAttempt.status, outputStatus: publishedOutput.status,
      publishedTilesVerified: publicConfig.lodProvenanceVerified,
      publishedObj: Boolean(publicConfig.assets.obj) }) + '\n');
  } finally {
    if (server) await new Promise((resolve) => server.close(resolve));
    database.close();
  }
}

main().catch((error) => {
  process.stderr.write(`${error.code || error.name}: ${error.message}\n`);
  process.exitCode = 1;
});
