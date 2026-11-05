'use strict';

const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const { createRequire } = require('node:module');

const viewerRequire = createRequire(path.join(process.env.LTDS_VIEWER_APP_ROOT, 'package.json'));
const { openDatabase } = viewerRequire('./server/database');
const { ProcessingRepository } = viewerRequire('./server/processingRepository');
const { createHeldSourceSnapshot } = viewerRequire('./server/heldSourceSnapshot');
const { PreparedSubmissionOutbox, canonicalizeJcs } = viewerRequire('./server/preparedSubmissionOutbox');
const { SourceIntakeClientV2 } = viewerRequire('./server/sourceIntakeClientV2');
const ReferenceProtocol = viewerRequire('./server/referenceProtocol');
const { createPreclaimSourceAttestation } = viewerRequire('./server/preclaimSourceAttestation');

const config = JSON.parse(fs.readFileSync(process.env.LTDS_PAIRING_CONFIG, 'utf8'));
const owner = 'pairing-submit-owner';
const inputBytes = Buffer.from('synthetic-loopback-photo-bytes');
const sha256 = (value) => crypto.createHash('sha256').update(value).digest('hex');
const out = { discovery: false, lostPostReplay: false, lostUploadStatusResume: false,
  lostCompletionReplay: false, strictCompletionPersisted: false,
  numericOptionExact: false, sourceSignerPinned: false };
let database = null;

function open() {
  database = openDatabase(config.databasePath);
  return { processing: new ProcessingRepository(database), outbox: new PreparedSubmissionOutbox(database) };
}
function close() { if (database) { database.close(); database = null; } }
function makeClient(outbox) {
  return new SourceIntakeClientV2({ origin: config.origin, bearerToken: config.viewerBearer,
    environment: 'staging', outbox, timeoutMs: 5000, transferTimeoutMs: 10000 });
}

async function main() {
  try {
    let { processing, outbox } = open();
    const project = processing.createProject({ displayName: 'Synthetic pairing project' });
    const dataset = processing.createDataset({ projectId: project.id, displayName: 'Synthetic photo source',
      storageMode: 'managed', rootKey: 'datasets' });
    const photoHash = sha256(inputBytes);
    const fileId = crypto.randomUUID();
    processing.finalizeDataset(dataset.id, [{ id: fileId, relativePath: 'photo.jpg', byteSize: inputBytes.length,
      sha256: photoHash, contentType: 'image/jpeg' }], sha256(Buffer.from('synthetic-dataset-manifest')));
    const provider = processing.upsertProvider({ type: 'nodeodm', displayName: 'Synthetic coordinator',
      endpoint: config.origin, enabled: true });
    const task = processing.createTask({ projectId: project.id, datasetId: dataset.id,
      displayName: 'Synthetic source intake task' });
    const options = { 'numeric-probe': 1e-7 };
    const attempt = processing.createAttempt({ taskId: task.id, providerId: provider.id, options,
      capabilityFingerprint: config.capabilityFingerprint });
    const localJob = processing.claimJob(owner);
    assert.equal(localJob.attempt_id, attempt.id);
    const localJobId = localJob.id;
    const capabilities = ReferenceProtocol.createCapabilities('staging');
    capabilities.capabilityFingerprint = config.capabilityFingerprint;
    capabilities.engines.odm.optionsFingerprint = config.optionsFingerprint;
    const snapshotPrivateKey = crypto.createPrivateKey(config.snapshotPrivateKeyPem);
    const snapshotPublicKey = crypto.createPublicKey(config.snapshotPublicKeyPem);
    const attestationPrivateKey = crypto.createPrivateKey(config.attestationPrivateKeyPem);
    const attestationPublicKey = crypto.createPublicKey(config.attestationPublicKeyPem);
    const snapshotSigner = { issuerId: 'pairing-snapshot-issuer', keyId: 'pairing-snapshot-key',
      sign: (bytes) => crypto.sign(null, bytes, snapshotPrivateKey) };
    const attestationSigner = { issuerId: 'pairing-source-issuer', keyId: 'pairing-source-key',
      publicKey: attestationPublicKey, sign: (bytes) => crypto.sign(null, bytes, attestationPrivateKey) };
    const manifest = { id: dataset.id, revision: 1, totalBytes: inputBytes.length, objectCount: 1,
      inputCounts: { images: 1, lidarObjects: 0, points: 0 }, objects: [{ id: fileId, name: 'photo.jpg',
        byteSize: inputBytes.length, sha256: photoHash, kind: 'image', mediaType: 'image/jpeg' }] };
    manifest.sha256 = ReferenceProtocol.manifestSha256(manifest);
    const pipeline = { odm: { requested: true, options, optionsFingerprint: config.optionsFingerprint },
      treedar: { requested: false, inputObjectIds: [], provenanceReceiptId: null },
      gaussian_splat: { requested: false } };
    const envelope = { schemaVersion: 3, protocol: 'ltds-viewer-prepared-envelope-jcs-v3',
      activationId: crypto.randomUUID(), viewerTaskId: task.id, viewerAttemptId: attempt.id,
      submission: { schemaVersion: 1, submissionId: attempt.id, manifest, pipeline,
        capabilityFingerprint: config.capabilityFingerprint } };
    const intent = outbox.persistIntent({ localJobId, owner, origin: config.origin,
      authorityId: config.authorityId, principalId: 'viewer-staging', environment: 'staging', envelope });
    createHeldSourceSnapshot(processing, { datasetId: dataset.id, taskId: task.id, attemptId: attempt.id,
      signer: snapshotSigner });
    const preclaim = createPreclaimSourceAttestation({ processing, outbox, attemptId: attempt.id,
      snapshotPublicKey, expectedSnapshotIssuerId: snapshotSigner.issuerId,
      expectedSnapshotKeyId: snapshotSigner.keyId, signer: attestationSigner,
      authorityId: config.authorityId, principalId: 'viewer-staging', environment: 'staging', capabilities });
    assert.equal(preclaim.sourceIntakeRequest.pipeline.odm.options['numeric-probe'], 1e-7);
    assert.equal(preclaim.sourceAttestation.proof.issuerId, attestationSigner.issuerId);
    assert.equal(preclaim.sourceAttestation.proof.keyId, attestationSigner.keyId);
    out.sourceSignerPinned = true;
    const prepared = outbox.persistSourceIntakeRequest(attempt.id, preclaim.sourceIntakeCanonical,
      { localJobId, owner });
    assert.equal(prepared.sourceIntakeCanonical, preclaim.sourceIntakeCanonical);
    out.numericOptionExact = JSON.parse(prepared.sourceIntakeCanonical).pipeline.odm.options['numeric-probe'] === 1e-7;

    const firstClient = makeClient(outbox);
    const discovered = await firstClient.getCapabilities();
    assert.equal(discovered.capabilities.environment, 'staging');
    assert.equal(discovered.sourceIntake.enabled, true);
    assert.equal(discovered.viewerAcceptance.enabled, true);
    out.discovery = true;
    await assert.rejects(firstClient.createOrReplay(prepared, { localJobId, owner }),
      { code: 'source_intake_transport_uncertain' });
    const afterLostPost = outbox.getIntent(attempt.id);
    assert.equal(afterLostPost.state, 'prepared');
    assert.equal(afterLostPost.sourceIntakeCanonical, preclaim.sourceIntakeCanonical);
    close();

    ({ processing, outbox } = open());
    const replayedIntent = outbox.getIntent(attempt.id);
    const resumedClient = makeClient(outbox);
    const binding = await resumedClient.createOrReplay(replayedIntent, { localJobId, owner });
    const persistedBinding = outbox.getIntent(attempt.id).remoteBinding;
    assert.deepEqual(binding, persistedBinding,
      'createOrReplay returns the durable binding directly, matching the outbox replay state');
    assert.equal(binding.jobId, attempt.providerTaskId);
    assert.equal(binding.coordinatorAuthorityId, config.authorityId);
    assert.equal(binding.attemptId, attempt.id);
    assert.equal(binding.providerTaskId, attempt.providerTaskId);
    assert.equal(binding.generation, persistedBinding.generation);
    assert.equal(binding.requestSha256, afterLostPost.sourceIntakeSha256);
    assert.equal(outbox.getIntent(attempt.id).sourceIntakeCanonical, preclaim.sourceIntakeCanonical);
    out.lostPostReplay = true;

    const manifests = { sourceManifest: preclaim.sourceManifest,
      sourceManifestSha256: preclaim.sourceManifestSha256,
      derivedManifest: preclaim.derivedManifest,
      derivedManifestSha256: preclaim.derivedManifestSha256,
      pipelineManifest: preclaim.pipelineManifest,
      pipelineManifestSha256: preclaim.pipelineManifestSha256 };
    const derivedBytes = preclaim.derivedBytes;
    const openSourceStream = async (object) => {
      assert.equal(object.id, fileId);
      return fs.createReadStream(config.sourceFilePath);
    };
    await assert.rejects(resumedClient.uploadClosure({ binding, manifests, derivedBytes,
      openSourceStream, localJobId, owner }), { code: 'source_intake_upload_uncertain' });
    close();

    ({ processing, outbox } = open());
    const restartedIntent = outbox.getIntent(attempt.id);
    const restartedClient = makeClient(outbox);
    const resumedBinding = restartedIntent.remoteBinding;
    await assert.rejects(restartedClient.uploadClosure({ binding: resumedBinding, manifests,
      derivedBytes, openSourceStream, localJobId, owner }), { code: 'source_intake_transport_uncertain' });
    close();

    ({ processing, outbox } = open());
    const completion = await makeClient(outbox).uploadClosure({
      binding: outbox.getIntent(attempt.id).remoteBinding, manifests, derivedBytes,
      openSourceStream, localJobId, owner });
    assert.equal(completion.state, 'accepted');
    const saved = outbox.getIntent(attempt.id);
    assert.ok(saved.sourceIntakeCompletionCanonical);
    const savedCompletion = JSON.parse(saved.sourceIntakeCompletionCanonical);
    assert.equal(savedCompletion.sourceState, 'retained');
    assert.equal(savedCompletion.derivedState, 'retained');
    assert.equal(savedCompletion.sourceReceipt.objects.length, 1);
    assert.equal(savedCompletion.sourceReceipt.objects[0].id, fileId);
    assert.equal(savedCompletion.derivedReceipt.objects.length, 0);
    assert.equal(savedCompletion.sourceReceiptSha256, sha256(canonicalizeJcs(savedCompletion.sourceReceipt)));
    assert.equal(savedCompletion.derivedReceiptSha256, sha256(canonicalizeJcs(savedCompletion.derivedReceipt)));
    out.strictCompletionPersisted = true;
    out.lostCompletionReplay = true;
    close();

    ({ outbox } = open());
    const durable = outbox.getIntent(attempt.id);
    assert.equal(durable.sourceIntakeCanonical, preclaim.sourceIntakeCanonical);
    assert.equal(durable.sourceIntakeCompletionCanonical, saved.sourceIntakeCompletionCanonical);
    const replayAfterCompletion = await makeClient(outbox).createOrReplay(durable, { localJobId, owner });
    assert.deepEqual(replayAfterCompletion, durable.remoteBinding);
    out.lostUploadStatusResume = true;
    close();
    process.stdout.write(`${JSON.stringify(out)}\n`);
  } finally { close(); }
}

main().catch((error) => {
  process.stderr.write(`${error?.code || error?.name || 'Error'}: ${error?.message || 'pairing failed'}\n`);
  process.exitCode = 1;
});
