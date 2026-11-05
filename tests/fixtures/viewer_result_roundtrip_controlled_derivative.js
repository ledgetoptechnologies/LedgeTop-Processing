'use strict';

const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const express = require('express');
const fs = require('node:fs');
const path = require('node:path');
const { createRequire } = require('node:module');
const pairing = JSON.parse(fs.readFileSync(process.env.LTDS_PAIRING_CONFIG, 'utf8'));
const root = path.dirname(pairing.databasePath);
process.env.DATA_DIR = root;
process.env.PROCESSING_ROOT = root;
process.env.DATASETS_MOUNT = pairing.datasetMount;
process.env.MODELS_MOUNT = pairing.modelsRoot;
process.env.CACHE_MOUNT = pairing.cacheMount;
process.env.TRASH_MOUNT = pairing.trashMount;
process.env.STORAGE_RESERVE_BYTES = '0';
process.env.STORAGE_RESERVE_PERCENT = '0';
const viewerRequire = createRequire(path.join(process.env.LTDS_VIEWER_APP_ROOT, 'package.json'));
const { openDatabase } = viewerRequire('./server/database');
const { ProcessingRepository } = viewerRequire('./server/processingRepository');
const { ViewerRepository } = viewerRequire('./server/repository');
const { StorageManager } = viewerRequire('./server/storageManager');
const { processOneDerivative } = viewerRequire('./server/derivativeWorker');
const { config: baseConfig } = viewerRequire('./server/config');
const { createProcessingApi } = viewerRequire('./server/processingApi');
const { createApiV1 } = viewerRequire('./server/apiV1');
const assetsRouter = viewerRequire('./server/assets');
const auth = viewerRequire('./server/auth');
const converterPolicy = viewerRequire('./lod-converter-policy.cjs');

const database = openDatabase(pairing.databasePath);

function tileContentUris(tile, values = []) {
  const content = tile?.content;
  if (content && typeof content.uri === 'string') values.push(content.uri);
  for (const item of Array.isArray(tile?.contents) ? tile.contents : []) {
    if (typeof item?.uri === 'string') values.push(item.uri);
  }
  for (const child of Array.isArray(tile?.children) ? tile.children : []) tileContentUris(child, values);
  return values;
}

async function findServedBinaryChild(tilesetUrl, maximumDepth = 8) {
  let currentUrl = tilesetUrl;
  for (let depth = 0; depth <= maximumDepth; depth += 1) {
    const response = await fetch(currentUrl);
    assert.equal(response.status, 200, `Viewer asset route rejected tileset at depth ${depth}`);
    const bytes = Buffer.from(await response.arrayBuffer());
    const document = JSON.parse(bytes.toString('utf8'));
    const uris = tileContentUris(document.root);
    assert.ok(uris.length > 0, 'published 3D Tiles document references content');
    const binaryUri = uris.find((uri) => /\.(?:b3dm|glb|pnts|i3dm)(?:[?#]|$)/i.test(uri));
    if (binaryUri) {
      const binaryUrl = new URL(binaryUri, currentUrl);
      assert.equal(binaryUrl.origin, new URL(tilesetUrl).origin, 'tile content stays on the scoped Viewer origin');
      const child = await fetch(binaryUrl);
      assert.equal(child.status, 200, 'Viewer served a real binary child referenced by the published tileset');
      const body = Buffer.from(await child.arrayBuffer());
      assert.ok(body.byteLength > 0, 'published binary child is non-empty');
      return { url: binaryUrl, bytes: body };
    }
    const nestedUri = uris.find((uri) => /\.json(?:[?#]|$)/i.test(uri));
    assert.ok(nestedUri, 'tileset must reference a binary child or a nested tileset');
    currentUrl = new URL(nestedUri, currentUrl);
    assert.equal(currentUrl.origin, new URL(tilesetUrl).origin, 'nested tileset stays on the scoped Viewer origin');
  }
  throw new Error('published tileset did not resolve to a binary child within the bounded depth');
}

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
      permissions: ['viewer.processing.publish', 'viewer.processing.read'],
      displayUnits: 'metric',
      expiresAt: new Date(Date.now() + 60_000).toISOString(),
    });
    const app = express();
    app.use(express.json({ verify: (req, _res, bytes) => { req.rawBody = Buffer.from(bytes); } }));
    app.use(createProcessingApi({ repository, processing, storage, config: runtimeConfig }));
    app.use(createApiV1(repository));
    assetsRouter.setRepository(repository);
    assetsRouter.setProcessingRepository(processing);
    app.use(assetsRouter);
    server = await new Promise((resolve) => {
      const listener = app.listen(0, '127.0.0.1', () => resolve(listener));
    });
    const base = `http://127.0.0.1:${server.address().port}`;
    const publishUrl = `${base}/api/v1/attempts/${encodeURIComponent(attempt.id)}/publish`;
    const publishRequest = {
      method: 'POST',
      headers: {
        authorization: `Bearer ${adminToken}`,
        'content-type': 'application/json',
        'idempotency-key': 'controlled-conversion-publish',
      },
      body: JSON.stringify({ selectedAssetKinds: ['glb', 'tiles'] }),
    };
    const response = await fetch(publishUrl, publishRequest);
    const firstPublicationText = await response.text();
    assert.equal(response.status, 200, `actual publish endpoint rejected verified model: ${firstPublicationText}`);
    const firstPublication = JSON.parse(firstPublicationText);
    const replayResponse = await fetch(publishUrl, publishRequest);
    assert.equal(replayResponse.status, 200, 'same publication idempotency key replays the committed response');
    const replayPublication = await replayResponse.json();
    assert.deepEqual(replayPublication, firstPublication, 'publication replay returns the identical model/task DTO');
    const publicationAuditCount = database.prepare(`SELECT COUNT(*) AS count FROM audit_events
      WHERE action='processing_attempt.published' AND entity_id=?`).get(attempt.id).count;
    assert.equal(publicationAuditCount, 1, 'publication replay does not create a second publication audit');

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

    const sessionResponse = await fetch(`${base}/api/v1/processing/outputs/${encodeURIComponent(attempt.resultModelVersionId)}/view-sessions`, {
      method: 'POST', headers: { authorization: `Bearer ${adminToken}`, 'content-type': 'application/json',
        'idempotency-key': 'controlled-conversion-published-session' }, body: '{}',
    });
    const sessionGrantText = await sessionResponse.text();
    assert.equal(sessionResponse.status, 201, `actual published Viewer session issuance failed: ${sessionGrantText}`);
    const sessionGrant = JSON.parse(sessionGrantText);
    const redeemResponse = await fetch(`${base}/api/v1/sessions/redeem`, {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ grant: sessionGrant.grant }),
    });
    const redeemText = await redeemResponse.text();
    assert.equal(redeemResponse.status, 200, `actual Viewer session redemption failed: ${redeemText}`);
    const session = JSON.parse(redeemText);
    const token = session.accessToken;
    assert.equal(session.sessionMode, 'published');
    assert.equal(session.model.lodProvenanceVerified, true);
    assert.equal(session.model.assets.obj, null, 'review session DTO omits private source OBJ');
    assert.match(session.model.assets.tiles, /^\/session-assets\//, 'tiles URL uses a scoped session capability');
    const tilesetUrl = new URL(session.model.assets.tiles, base);
    const tilesetResponse = await fetch(tilesetUrl);
    assert.equal(tilesetResponse.status, 200, 'normal Viewer asset router serves the published tileset');
    assert.match(tilesetResponse.headers.get('content-type') || '', /application\/json/i);
    const servedTileset = JSON.parse(await tilesetResponse.text());
    assert.ok(servedTileset.root);
    const child = await findServedBinaryChild(tilesetUrl);

    const invalidTokenUrl = new URL(tilesetUrl);
    invalidTokenUrl.pathname = invalidTokenUrl.pathname.replace(`/session-assets/${encodeURIComponent(token)}/`,
      `/session-assets/${'x'.repeat(43)}/`);
    assert.notEqual(invalidTokenUrl.pathname, tilesetUrl.pathname, 'invalid-token URL changed only the scoped token segment');
    assert.equal((await fetch(invalidTokenUrl)).status, 403, 'an invalid session token cannot read a published asset');
    const obj = persistedVersion.activeVersion.assets.find((asset) => asset.kind === 'obj');
    assert.ok(obj, 'private source OBJ remains registered for provenance verification');
    const privateObjUrl = new URL(`/session-assets/${encodeURIComponent(token)}/${encodeURIComponent(published.id)}/${encodeURIComponent(obj.rootKey)}/${obj.relativePath.split(/[\\/]+/).map(encodeURIComponent).join('/')}`, base);
    assert.equal((await fetch(privateObjUrl)).status, 404, 'review session cannot fetch the private provenance OBJ');

    assert.equal(repository.revokeViewerSession(session.sessionId), true, 'persisted published session can be revoked');
    const revokedAsset = await fetch(child.url);
    assert.equal(revokedAsset.status, 403, 'revoked Viewer session cannot reuse a previously valid asset URL');

    process.stdout.write(JSON.stringify({ derivativeStatus: derivative.status,
      verifiedReceipt: Boolean(stagedVersion.activeVersion.lodVerifierReceipt),
      attemptStatus: publishedAttempt.status, outputStatus: publishedOutput.status,
      publishedTilesVerified: publicConfig.lodProvenanceVerified,
      publishedObj: Boolean(publicConfig.assets.obj), actualTilesetHttp: tilesetResponse.status,
      actualBinaryChildHttp: 200, publicationReplayAuditCount: publicationAuditCount,
      invalidTokenRejected: true, revokedTokenRejected: revokedAsset.status === 403,
      privateObjRejected: true }) + '\n');
  } finally {
    if (server) await new Promise((resolve) => server.close(resolve));
    database.close();
  }
}

main().catch((error) => {
  process.stderr.write(`${error.code || error.name}: ${error.message}\n`);
  process.exitCode = 1;
});
