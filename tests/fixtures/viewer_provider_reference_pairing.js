'use strict';

const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const express = require('express');
const fs = require('node:fs');
const path = require('node:path');
const { createRequire } = require('node:module');

const viewerRequire = createRequire(path.join(process.env.LTDS_VIEWER_APP_ROOT, 'package.json'));
const config = JSON.parse(fs.readFileSync(process.env.LTDS_PAIRING_CONFIG, 'utf8'));
const { openDatabase } = viewerRequire('./server/database');
const { ViewerRepository } = viewerRequire('./server/repository');
const { ProcessingRepository } = viewerRequire('./server/processingRepository');
const { StorageManager } = viewerRequire('./server/storageManager');
const { PreparedSubmissionOutbox } = viewerRequire('./server/preparedSubmissionOutbox');
const { SourceIntakeClientV2 } = viewerRequire('./server/sourceIntakeClientV2');
const ReferenceProtocol = viewerRequire('./server/referenceProtocol');
const { createHeldSourceSnapshot } = viewerRequire('./server/heldSourceSnapshot');
const { createPreclaimSourceAttestation } = viewerRequire('./server/preclaimSourceAttestation');
const { processOne } = viewerRequire('./server/processingWorker');
const auth = viewerRequire('./server/auth');
const { ProviderCredentials } = viewerRequire('./server/providerCredentials');
const { createProcessingApi } = viewerRequire('./server/processingApi');

async function json(response) {
  const body = await response.json();
  assert.ok(response.ok, `${response.status} ${JSON.stringify(body)}`);
  return body;
}

async function main() {
  const root = path.dirname(config.databasePath);
  for (const name of ['datasets', 'models', 'cache', 'trash', 'imports'])
    fs.mkdirSync(path.join(root, name), { recursive: true });
  const database = openDatabase(config.databasePath);
  const repository = new ViewerRepository(database);
  const processing = new ProcessingRepository(database);
  const storage = new StorageManager({ datasetsMount: path.join(root, 'datasets'),
    modelsMount: path.join(root, 'models'), cacheMount: path.join(root, 'cache'),
    trashMount: path.join(root, 'trash'), datasetImportMount: path.join(root, 'imports'),
    storageReserveBytes: 0, storageReservePercent: 0 });
  storage.initialize();
  const adminToken = 'synthetic-provider-admin-token-' + 'a'.repeat(32);
  const subject = 'ops:reference-provider-pairing';
  processing.createAdminSession({ tokenHash: auth.hashToken(adminToken), subject,
    permissions: ['viewer.providers.read', 'viewer.providers.write', 'viewer.processing.read',
      'viewer.processing.write'], displayUnits: 'metric',
    expiresAt: new Date(Date.now() + 120000).toISOString() });
  const providerCredentials = new ProviderCredentials({ processing,
    activeKeyId: 'pairing-v1', keys: { 'pairing-v1': crypto.randomBytes(32).toString('hex') } });
  const runtimeConfig = { ...viewerRequire('./server/config').config,
    production: false, referenceSourceIntake: { enabled: true,
      providerId: config.providerId, origin: config.origin, bearerToken: config.viewerBearer,
      environment: 'staging' },
    processingProviderOrigins: [config.origin], processingProviderAllowedCidrs: [],
    defaultUnits: 'metric', meshDerivativesEnabled: true };
  const app = express();
  app.use(express.json({ verify: (req, _res, buffer) => { req.rawBody = Buffer.from(buffer); } }));
  app.use(createProcessingApi({ repository, processing, storage, providerCredentials, config: runtimeConfig }));
  const server = await new Promise((resolve, reject) => {
    const value = app.listen(0, '127.0.0.1', () => resolve(value));
    value.once('error', reject);
  });
  try {
    const base = `http://127.0.0.1:${server.address().port}`;
    const headers = { authorization: `Bearer ${adminToken}`, 'content-type': 'application/json' };
    const post = (route, payload, key) => fetch(`${base}${route}`, { method: 'POST', headers: {
      ...headers, ...(key ? { 'idempotency-key': key } : {}) }, body: JSON.stringify(payload) });
    const patch = (route, payload, key) => fetch(`${base}${route}`, { method: 'PATCH', headers: {
      ...headers, ...(key ? { 'idempotency-key': key } : {}) }, body: JSON.stringify(payload) });

    const creation = await json(await post('/api/v1/processing/providers', {
      displayName: 'Synthetic Reference Coordinator', endpoint: config.origin,
    }, 'pairing-provider-create-0001'));
    const provider = creation.provider;
    assert.equal(provider.id, config.providerId);
    assert.equal(provider.enabled, false);
    assert.equal(provider.capabilities.detectionMethod, 'pinned-reference-coordinator');
    assert.equal(provider.credential.mode, 'managed-reference');
    assert.equal(provider.capabilities.engineVersion, 'unknown');
    assert.equal(provider.capabilities.taskQueueCount, null);
    const optionsByName = new Map(provider.capabilities.options.map(option => [option.name, option]));
    assert.equal(optionsByName.get('pc-ept').value, false);
    assert.equal(optionsByName.get('pc-ept').rawDefault, 'false');
    assert.equal(optionsByName.get('orthophoto-resolution').value, 2);
    assert.equal(optionsByName.get('orthophoto-resolution').rawDefault, '2');
    assert.deepEqual(optionsByName.get('feature-quality').domain, ['low', 'medium', 'high']);

    const enabled = await json(await patch(`/api/v1/processing/providers/${encodeURIComponent(provider.id)}`, {
      enabled: true, endpoint: config.origin, displayName: provider.displayName,
    }, 'pairing-provider-enable-0001'));
    assert.equal(enabled.provider.enabled, true);

    const refreshed = await json(await post(`/api/v1/processing/providers/${encodeURIComponent(provider.id)}/capabilities/refresh`, {},
      'pairing-provider-refresh-0001'));
    assert.equal(refreshed.capabilities.providerType, 'nodeodm');
    assert.equal(refreshed.capabilities.detectionMethod, 'pinned-reference-coordinator');
    assert.equal(refreshed.capabilities.reference.environment, 'staging');
    assert.equal(processing.providerProbeCurrent(provider.id), true);

    const project = processing.createProject({ displayName: 'Synthetic provider pairing project' });
    const options = { 'pc-ept': true, gltf: true, '3d-tiles': true, 'orthophoto-resolution': 1e-7 };
    const photoRecords = [];
    for (let index = 0; index < 2; index++) {
      const relativePath = `source-${index + 1}`;
      const dataset = processing.createDataset({ projectId: project.id,
        displayName: `Synthetic one-photo set ${index + 1}`, storageMode: 'managed',
        rootKey: 'datasets', relativePath });
      const photo = Buffer.from(`synthetic photo bytes ${index + 1} for Viewer provider admission`);
      const absolute = storage.resolve('datasets', `${relativePath}/photo.jpg`);
      fs.mkdirSync(path.dirname(absolute), { recursive: true });
      fs.writeFileSync(absolute, photo);
      const file = { id: crypto.randomUUID(), relativePath: 'photo.jpg', byteSize: photo.length,
        sha256: crypto.createHash('sha256').update(photo).digest('hex'), contentType: 'image/jpeg' };
      processing.finalizeDataset(dataset.id, [file],
        crypto.createHash('sha256').update(`synthetic dataset manifest ${index + 1}`).digest('hex'));
      const taskSubmission = await json(await post('/api/v1/task-submissions', {
        submissionId: crypto.randomUUID(), projectId: project.id, datasetId: dataset.id,
        taskDisplayName: `Synthetic coordinator task ${index + 1}`, providerId: provider.id, options,
      }, `pairing-task-submit-000${index + 1}`));
      assert.equal(taskSubmission.replayed, false);
      assert.equal(taskSubmission.attempt.providerId, provider.id);
      assert.equal(taskSubmission.attempt.options['orthophoto-resolution'], 1e-7);
      photoRecords.push({ datasetId: dataset.id, taskId: taskSubmission.task.id,
        attemptId: taskSubmission.attempt.id });
    }

    const snapshotPrivate = crypto.createPrivateKey(config.snapshotPrivateKeyPem);
    const snapshotPublic = crypto.createPublicKey(config.snapshotPublicKeyPem);
    const attestationPrivate = crypto.createPrivateKey(config.attestationPrivateKeyPem);
    const attestationPublic = crypto.createPublicKey(config.attestationPublicKeyPem);
    const sourceConfig = { enabled: true, providerId: provider.id, origin: config.origin,
      authorityId: config.authorityId, principalId: 'viewer-staging', environment: 'staging',
      snapshotIssuerId: 'pairing-snapshot-issuer', snapshotKeyId: 'pairing-snapshot-key' };
    const outbox = new PreparedSubmissionOutbox(database);
    const client = new SourceIntakeClientV2({ origin: config.origin, bearerToken: config.viewerBearer,
      environment: 'staging', outbox, timeoutMs: 5000, transferTimeoutMs: 10000 });
    const sourceIntakeCalls = [];
    const originalCreateOrReplay = client.createOrReplay.bind(client);
    client.createOrReplay = async (...args) => {
      sourceIntakeCalls.push('createOrReplay');
      return originalCreateOrReplay(...args);
    };
    const originalUploadClosure = client.uploadClosure.bind(client);
    client.uploadClosure = async (args) => {
      sourceIntakeCalls.push(`uploadClosure:${args.manifests.sourceManifest.objects.length}`);
      try { return await originalUploadClosure(args); }
      catch (error) { sourceIntakeCalls.push(`uploadClosureError:${error.code || error.name}`); throw error; }
    };
    const capabilities = ReferenceProtocol.createCapabilities('staging');
    capabilities.capabilityFingerprint = config.capabilityFingerprint;
    capabilities.engines.odm.optionsFingerprint = config.optionsFingerprint;
    const sourceIntake = { client, outbox,
      snapshotPublicKey: snapshotPublic,
      snapshotSigner: { issuerId: 'pairing-snapshot-issuer', keyId: 'pairing-snapshot-key',
        sign: bytes => crypto.sign(null, bytes, snapshotPrivate) },
      attestationSigner: { issuerId: 'pairing-source-issuer', keyId: 'pairing-source-key',
        publicKey: attestationPublic, sign: bytes => crypto.sign(null, bytes, attestationPrivate) } };
    const sourceRuntimeConfig = { referenceSourceIntake: sourceConfig,
      processingProviderTransferTimeoutMs: 10000, opsBaseUrl: 'https://ops.synthetic.invalid' };
    const runOne = async (owner) => processOne({ processing, storage, config: sourceRuntimeConfig,
      providerCredentials, sourceIntake }, owner);
    assert.equal(await runOne('pairing-submit-owner-1'), true);
    assert.equal(processing.getAttempt(photoRecords[0].attemptId).status, 'queued_upstream');
    assert.equal(await runOne('pairing-submit-owner-2'), true,
      'second normal submit claim must progress while first is queued upstream');
    assert.equal(processing.getAttempt(photoRecords[1].attemptId).status, 'queued_upstream');
    for (const record of photoRecords) {
      const intent = outbox.getIntent(record.attemptId);
      assert.equal(intent.state, 'accepted');
      assert.equal(intent.remoteBinding.environment, 'staging');
    }
    const attemptDiagnostics = photoRecords.map(record => {
      const attempt = processing.getAttempt(record.attemptId);
      const intent = outbox.getIntent(record.attemptId);
      return { status: attempt.status, errorCode: attempt.errorCode,
        submissionPhase: attempt.submissionPhase, uploadedFileCount: attempt.uploadedFileCount,
        sourceIntakeCompletionPersisted: Boolean(intent.sourceIntakeCompletionCanonical) };
    });
    process.stdout.write(JSON.stringify({ providerCreated: true, providerHealthy: true,
      capabilitiesFromCoordinator: true, providerEnabled: true, taskAdmitted: true,
      queuedUpstreamAttempts: photoRecords.map(record => record.attemptId), sourceIntakeCalls,
      attemptDiagnostics, sourceManifestObjectCounts: sourceIntakeCalls
        .filter(value => value.startsWith('uploadClosure:')).map(value => Number(value.split(':')[1])) }) + '\n');
  } finally {
    await new Promise(resolve => server.close(resolve));
    database.close();
  }
}

main().catch(error => {
  process.stderr.write(`${error.code || error.name}: ${error.message}\n`);
  process.exitCode = 1;
});
