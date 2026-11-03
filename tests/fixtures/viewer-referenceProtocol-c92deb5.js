'use strict';

const { createHash, randomBytes } = require('node:crypto');
const PROTOCOL = 'ltds-reference-v1';
const PREFIX = '/ltds/v1';
const LIMITS = Object.freeze({ metadataBytes: 4 * 1024 * 1024, objects: 10000, integer: Number.MAX_SAFE_INTEGER });
class ReferenceProtocolError extends Error {
  constructor(code, path) { super(`${code}: ${path}`); this.name = 'ReferenceProtocolError'; this.code = code; this.path = path; }
}
const fail = (path, code = 'INVALID_PROTOCOL') => { throw new ReferenceProtocolError(code, path); };
function record(v, keys, path) {
  if (!v || typeof v !== 'object' || Array.isArray(v) || Object.getPrototypeOf(v) !== Object.prototype) fail(path);
  if (Object.keys(v).length !== keys.length || keys.some(k => !Object.hasOwn(v, k)) || Object.keys(v).some(k => !keys.includes(k))) fail(path);
}
function text(v, path, max = 256) { if (typeof v !== 'string' || !v.length || v.length > max || /[\x00-\x1f\x7f]/u.test(v)) fail(path); }
function id(v, path) { text(v, path, 128); if (!/^[A-Za-z0-9][A-Za-z0-9_-]*$/.test(v)) fail(path); }
function integer(v, path) { if (!Number.isSafeInteger(v) || v < 0 || Object.is(v, -0)) fail(path); }
function hash(v, path) { if (typeof v !== 'string' || !/^[a-f0-9]{64}$/.test(v)) fail(path); }
function bool(v, path) { if (typeof v !== 'boolean') fail(path); }
function environment(v) { if (!['staging', 'production'].includes(v)) fail('environment'); }
function timestamp(v, path) { text(v, path, 24); const date = new Date(v); if (!/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$/.test(v) || !Number.isFinite(date.getTime()) || date.toISOString() !== v) fail(path); }
function array(v, path) { if (!Array.isArray(v) || v.length > LIMITS.objects) fail(path); }
function uniqueIds(v, path) { array(v, path); v.forEach(x => id(x, path)); if (new Set(v).size !== v.length) fail(path); }
function canonicalBytes(value) {
  function encode(v) {
    if (v === null || typeof v === 'boolean' || typeof v === 'string') return JSON.stringify(v);
    if (typeof v === 'number') { integer(v, 'canonical.integer'); return String(v); }
    if (Array.isArray(v)) return '[' + v.map(encode).join(',') + ']';
    if (!v || typeof v !== 'object' || Object.getPrototypeOf(v) !== Object.prototype) fail('canonical.object');
    return '{' + Object.keys(v).sort().map(k => { if (!/^[\x20-\x7e]+$/.test(k)) fail('canonical.key'); return JSON.stringify(k) + ':' + encode(v[k]); }).join(',') + '}';
  }
  return Buffer.from(encode(value), 'utf8');
}
const sha256 = bytes => createHash('sha256').update(bytes).digest('hex');
function canonicalManifestBytes(manifest) { const { sha256: ignored, ...payload } = manifest; return canonicalBytes(payload); }
const manifestSha256 = manifest => sha256(canonicalManifestBytes(manifest));
function canonicalResultManifestBytes(manifest) { const { resultManifestSha256: ignored, ...payload } = manifest; return canonicalBytes(payload); }
const resultManifestSha256 = manifest => sha256(canonicalResultManifestBytes(manifest));
function transferByteBudgetLimit(totalBytes) {
  integer(totalBytes, 'manifest.totalBytes');
  const maximum = BigInt(Number.MAX_SAFE_INTEGER), retryBudget = BigInt(totalBytes) * 3n;
  return Number(retryBudget < maximum ? retryBudget : maximum);
}
function validateTransferByteBudget(totalBytes, byteBudget) {
  const maximum = transferByteBudgetLimit(totalBytes); integer(byteBudget, 'byteBudget');
  if (byteBudget < totalBytes || byteBudget > maximum) fail('byteBudget', 'TRANSFER_BUDGET');
  return byteBudget;
}
function metadata(v) { let bytes; try { bytes = Buffer.byteLength(JSON.stringify(v), 'utf8'); } catch { fail('metadata'); } if (bytes > LIMITS.metadataBytes) fail('metadata', 'METADATA_LIMIT'); }
function jsonOptions(v, depth = 0) {
  if (depth > 32) fail('odm.options.depth');
  if (v === null || typeof v === 'boolean' || typeof v === 'string') return;
  if (typeof v === 'number') { if (!Number.isFinite(v) || Math.abs(v) > Number.MAX_SAFE_INTEGER) fail('odm.options.number'); return; }
  if (Array.isArray(v)) { if (v.length > LIMITS.objects) fail('odm.options.array'); v.forEach(x => jsonOptions(x, depth + 1)); return; }
  if (!v || typeof v !== 'object' || Object.getPrototypeOf(v) !== Object.prototype) fail('odm.options');
  for (const [key, value] of Object.entries(v)) { if (['__proto__', 'prototype', 'constructor'].includes(key)) fail('odm.options.key'); jsonOptions(value, depth + 1); }
}
function basename(v) {
  text(v, 'manifest.objects.name', 255);
  if (!/^[\x20-\x7e]+$/.test(v) || /[\\/:<>"|?*]/.test(v) || /[. ]$/.test(v) || v.startsWith('.') || !/^.+\.[^.]+$/.test(v) || /^(con|prn|aux|nul|com[1-9]|lpt[1-9])(?:\.|$)/i.test(v)) fail('manifest.objects.name');
  return v.toLowerCase();
}
function validateManifest(m) {
  metadata(m);
  record(m, ['id', 'revision', 'sha256', 'totalBytes', 'objectCount', 'inputCounts', 'objects'], 'manifest');
  id(m.id, 'manifest.id'); integer(m.revision, 'manifest.revision'); hash(m.sha256, 'manifest.sha256'); integer(m.totalBytes, 'manifest.totalBytes'); integer(m.objectCount, 'manifest.objectCount');
  record(m.inputCounts, ['images', 'lidarObjects', 'points'], 'manifest.inputCounts'); Object.values(m.inputCounts).forEach(v => integer(v, 'manifest.inputCounts'));
  array(m.objects, 'manifest.objects'); const names = new Set(), ids = new Set(); let bytes = 0, images = 0, lidar = 0;
  for (const o of m.objects) {
    record(o, ['id', 'name', 'byteSize', 'sha256', 'kind', 'mediaType'], 'manifest.objects'); id(o.id, 'object.id'); integer(o.byteSize, 'object.byteSize'); hash(o.sha256, 'object.sha256');
    const name = basename(o.name); if (names.has(name) || ids.has(o.id)) fail('manifest.objects.duplicate'); names.add(name); ids.add(o.id);
    if (!['image', 'lidar', 'auxiliary'].includes(o.kind)) fail('object.kind');
    const media = o.kind === 'image' ? ['image/jpeg', 'image/png', 'image/tiff'] : o.kind === 'lidar' ? ['application/vnd.las', 'application/vnd.laz'] : ['application/octet-stream', 'application/json', 'text/plain', 'text/csv'];
    if (!media.includes(o.mediaType)) fail('object.mediaType');
    if (o.kind === 'image') images++; else if (o.kind === 'lidar') lidar++;
    bytes += o.byteSize; integer(bytes, 'manifest.totalBytes');
  }
  if (m.objectCount !== m.objects.length || bytes !== m.totalBytes || images !== m.inputCounts.images || lidar !== m.inputCounts.lidarObjects || (!lidar && m.inputCounts.points !== 0)) fail('manifest.counts');
  if (manifestSha256(m) !== m.sha256) fail('manifest.sha256', 'DIGEST_MISMATCH'); return m;
}
function createCapabilities(env, overrides = {}) {
  environment(env); const opaque = () => randomBytes(32).toString('hex');
  const c = { protocol: PROTOCOL, schemaVersion: 1, environment: env, capabilityFingerprint: opaque(), limits: { ...LIMITS }, engines: { odm: { available: true, optionsFingerprint: opaque() }, treedar: { available: false, admissionVersion: 'treedar-lidar-v1' }, gaussian_splat: { available: false, enabled: false } } };
  if (overrides.treedarAvailable === true) c.engines.treedar.available = true;
  return c;
}
function validateCapabilities(c) {
  record(c, ['protocol', 'schemaVersion', 'environment', 'capabilityFingerprint', 'limits', 'engines'], 'capabilities');
  if (c.protocol !== PROTOCOL || c.schemaVersion !== 1) fail('capabilities.version'); environment(c.environment); hash(c.capabilityFingerprint, 'capabilityFingerprint');
  record(c.limits, ['metadataBytes', 'objects', 'integer'], 'limits'); for (const k of Object.keys(LIMITS)) if (c.limits[k] !== LIMITS[k]) fail('limits.' + k);
  record(c.engines, ['odm', 'treedar', 'gaussian_splat'], 'engines'); record(c.engines.odm, ['available', 'optionsFingerprint'], 'odm'); bool(c.engines.odm.available, 'odm.available'); hash(c.engines.odm.optionsFingerprint, 'optionsFingerprint');
  record(c.engines.treedar, ['available', 'admissionVersion'], 'treedar'); bool(c.engines.treedar.available, 'treedar.available'); if (c.engines.treedar.admissionVersion !== 'treedar-lidar-v1') fail('admissionVersion');
  record(c.engines.gaussian_splat, ['available', 'enabled'], 'gaussian_splat'); if (c.engines.gaussian_splat.available !== false || c.engines.gaussian_splat.enabled !== false) fail('gaussian_splat'); return c;
}
function validateSubmission(s, { capabilities, trustedLidarEvidence = null } = {}) {
  metadata(s); validateCapabilities(capabilities);
  const submissionFields = ['schemaVersion', 'submissionId', 'manifest', 'pipeline', 'capabilityFingerprint'];
  if (Object.hasOwn(s, 'viewerAttemptId')) submissionFields.push('viewerAttemptId');
  record(s, submissionFields, 'submission');
  if (s.schemaVersion !== 1 || typeof s.submissionId !== 'string' || !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(s.submissionId)) fail('submission.versionOrId');
  if (Object.hasOwn(s, 'viewerAttemptId') && (typeof s.viewerAttemptId !== 'string' || !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(s.viewerAttemptId))) fail('submission.viewerAttemptId');
  hash(s.capabilityFingerprint, 'capabilityFingerprint'); if (s.capabilityFingerprint !== capabilities.capabilityFingerprint) fail('capabilityFingerprint', 'STALE_CAPABILITIES'); validateManifest(s.manifest);
  record(s.pipeline, ['odm', 'treedar', 'gaussian_splat'], 'pipeline'); const { odm, treedar, gaussian_splat: gs } = s.pipeline;
  record(odm, ['requested', 'options', 'optionsFingerprint'], 'odm'); bool(odm.requested, 'odm.requested'); hash(odm.optionsFingerprint, 'optionsFingerprint');
  if (!odm.options || typeof odm.options !== 'object' || Array.isArray(odm.options)) fail('odm.options');
  jsonOptions(odm.options);
  // Dynamic option values are validated by the engine adapter. No client digest is derived from their JSON serialization.
  if (odm.optionsFingerprint !== capabilities.engines.odm.optionsFingerprint) fail('optionsFingerprint', 'STALE_OPTIONS');
  if (odm.requested && !capabilities.engines.odm.available) fail('odm', 'UNSUPPORTED_ENGINE');
  record(treedar, ['requested', 'inputObjectIds', 'provenanceReceiptId'], 'treedar'); bool(treedar.requested, 'treedar.requested'); uniqueIds(treedar.inputObjectIds, 'treedar.inputObjectIds');
  if (treedar.provenanceReceiptId !== null) id(treedar.provenanceReceiptId, 'treedar.provenanceReceiptId');
  record(gs, ['requested'], 'gaussian_splat'); if (gs.requested !== false) fail('gaussian_splat', 'UNSUPPORTED_ENGINE');
  if (treedar.requested) {
    if (!capabilities.engines.treedar.available || !treedar.inputObjectIds.length || !trustedLidarEvidence || trustedLidarEvidence.receiptId !== treedar.provenanceReceiptId || trustedLidarEvidence.admissionVersion !== 'treedar-lidar-v1') fail('treedar', 'UNSUPPORTED_LIDAR');
    for (const oid of treedar.inputObjectIds) { const o = s.manifest.objects.find(o => o.id === oid), e = trustedLidarEvidence.objects?.find(e => e.id === oid);
      if (!o || o.kind !== 'lidar' || !e || e.sha256 !== o.sha256 || e.genuineLasLaz !== true || e.photogrammetryDerived !== false) fail('treedar.evidence', 'UNSUPPORTED_LIDAR'); }
  } else if (treedar.inputObjectIds.length || treedar.provenanceReceiptId !== null) fail('treedar.disabled');
  if (!odm.requested && !treedar.requested) fail('pipeline.empty'); return s;
}
function binding(v, expected, keys) { for (const k of keys) if (JSON.stringify(v[k]) !== JSON.stringify(expected[k])) fail(k, 'BINDING_MISMATCH'); }
function validateTransferRequest(v, expected = null) {
  record(v, ['reservationId', 'generation', 'expiresAt', 'manifestSha256', 'byteBudget', 'nonce'], 'transferRequest'); id(v.reservationId, 'reservationId'); integer(v.generation, 'generation'); timestamp(v.expiresAt, 'expiresAt'); hash(v.manifestSha256, 'manifestSha256'); integer(v.byteBudget, 'byteBudget'); id(v.nonce, 'nonce');
  if (Date.parse(v.expiresAt) <= Date.now()) fail('expiresAt', 'EXPIRED_TRANSFER');
  if (expected) binding(v, expected, Object.keys(v)); return v;
}
function validateTransferGrant(v, expected) {
  record(v, ['jobId', 'generation', 'reservationId', 'manifestSha256', 'environment', 'objectIds', 'nonce', 'byteBudget', 'expiresAt', 'token'], 'transferGrant');
  id(v.jobId, 'jobId'); integer(v.generation, 'generation'); id(v.reservationId, 'reservationId'); hash(v.manifestSha256, 'manifestSha256'); environment(v.environment); uniqueIds(v.objectIds, 'objectIds'); id(v.nonce, 'nonce'); integer(v.byteBudget, 'byteBudget'); timestamp(v.expiresAt, 'expiresAt'); text(v.token, 'token', 4096);
  if (Date.parse(v.expiresAt) <= Date.now()) fail('expiresAt', 'EXPIRED_TRANSFER');
  if (!expected) fail('transferGrant.binding'); binding(v, expected, Object.keys(v).filter(k => k !== 'token')); return v;
}
function validateResultManifest(v, expected) {
  metadata(v);
  record(v, ['schemaVersion', 'jobId', 'generation', 'environment', 'pipelineDigest', 'engineVersions', 'objects', 'resultManifestSha256'], 'resultManifest'); if (v.schemaVersion !== 1) fail('schemaVersion'); id(v.jobId, 'jobId'); integer(v.generation, 'generation'); environment(v.environment); hash(v.pipelineDigest, 'pipelineDigest'); hash(v.resultManifestSha256, 'resultManifestSha256'); array(v.objects, 'objects'); const ids = new Set();
  if (!v.engineVersions || typeof v.engineVersions !== 'object' || Array.isArray(v.engineVersions) || Object.getPrototypeOf(v.engineVersions) !== Object.prototype || !Object.keys(v.engineVersions).length) fail('engineVersions');
  for (const [engine, version] of Object.entries(v.engineVersions)) { if (!['odm', 'treedar'].includes(engine)) fail('engineVersions.engine'); text(version, 'engineVersions.version', 128); if (!/^[\x21-\x7e]+$/.test(version)) fail('engineVersions.version'); }
  for (const o of v.objects) { record(o, ['id', 'role', 'byteSize', 'sha256'], 'resultObject'); id(o.id, 'object.id'); if (ids.has(o.id)) fail('object.duplicate'); ids.add(o.id); if (!['pointcloud', 'mesh', 'orthophoto', 'dem', 'report', 'tree_inventory', 'odm_archive'].includes(o.role)) fail('object.role'); integer(o.byteSize, 'object.byteSize'); if (o.byteSize === 0 && o.role !== 'report') fail('object.byteSize'); hash(o.sha256, 'object.sha256'); }
  if (resultManifestSha256(v) !== v.resultManifestSha256) fail('resultManifestSha256', 'DIGEST_MISMATCH');
  if (!expected) fail('resultManifest.binding'); binding(v, expected, ['jobId', 'generation', 'environment', 'pipelineDigest']);
  if (Object.hasOwn(expected, 'requestedEngines')) {
    const engines = expected.requestedEngines;
    if (!Array.isArray(engines) || !engines.length || engines.some(e => !['odm', 'treedar'].includes(e)) || new Set(engines).size !== engines.length) fail('requestedEngines');
    if (JSON.stringify([...engines].sort()) !== JSON.stringify(Object.keys(v.engineVersions).sort())) fail('engineVersions', 'BINDING_MISMATCH');
    const archives = v.objects.filter(o => o.role === 'odm_archive');
    if (engines.includes('odm') ? archives.length !== 1 : archives.length !== 0) fail('odm_archive', 'BINDING_MISMATCH');
  }
  return v;
}
function validateAcceptance(v, expected) {
  metadata(v);
  record(v, ['schemaVersion', 'receiptId', 'storageReceiptId', 'jobId', 'generation', 'environment', 'inputManifestSha256', 'pipelineDigest', 'resultManifestSha256', 'acceptedObjectIds', 'acceptedAt', 'validationVersion'], 'acceptance');
  if (v.schemaVersion !== 1 || v.validationVersion !== PROTOCOL) fail('acceptance.version'); for (const k of ['receiptId', 'storageReceiptId', 'jobId']) id(v[k], k); integer(v.generation, 'generation'); environment(v.environment); for (const k of ['inputManifestSha256', 'pipelineDigest', 'resultManifestSha256']) hash(v[k], k); uniqueIds(v.acceptedObjectIds, 'acceptedObjectIds'); timestamp(v.acceptedAt, 'acceptedAt');
  if (!expected) fail('acceptance.binding'); binding(v, expected, ['jobId', 'generation', 'environment', 'inputManifestSha256', 'pipelineDigest', 'resultManifestSha256']);
  if (!Array.isArray(expected.objectIds) || v.acceptedObjectIds.some(id => !expected.objectIds.includes(id))) fail('acceptedObjectIds', 'BINDING_MISMATCH'); return v;
}
module.exports = { PROTOCOL, PREFIX, LIMITS, ReferenceProtocolError, canonicalBytes, canonicalManifestBytes, manifestSha256, canonicalResultManifestBytes, resultManifestSha256, transferByteBudgetLimit, validateTransferByteBudget, sha256, createCapabilities, validateCapabilities, validateManifest, validateSubmission, validateTransferRequest, validateTransferGrant, validateResultManifest, validateAcceptance };
