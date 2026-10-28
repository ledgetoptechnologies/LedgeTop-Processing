'use strict';

// Explicit offline paired-repository probe. Never contacts Viewer or an engine.
// Usage: node tests/reference_pipeline_wire_probe.cjs <fixture.json>
//        <trusted-viewer-referenceProtocol-module> <canonical-source-sha256>
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { createHash } = require('node:crypto');

function readBounded(file, limit) {
  const metadata = fs.statSync(file);
  assert.ok(metadata.isFile() && metadata.size <= limit, 'bounded regular fixture/source file required');
  const value = fs.readFileSync(file);
  assert.ok(value.length <= limit);
  return value;
}

function main() {
  assert.equal(process.argv.length, 5, 'fixture, trusted protocol module and pinned source hash required');
  const [fixturePath, modulePath, expectedSourceSha256] = process.argv.slice(2);
  assert.match(expectedSourceSha256, /^[a-f0-9]{64}$/);
  const source = readBounded(modulePath, 256 * 1024).toString('utf8').replace(/\r\n/g, '\n');
  const actualSha256 = createHash('sha256').update(source).digest('hex');
  assert.equal(actualSha256, expectedSourceSha256, 'actual Viewer validator source must match the reviewed pin');
  const protocol = require(path.resolve(modulePath));
  const fixture = JSON.parse(readBounded(fixturePath, 4 * 1024 * 1024).toString('utf8'));
  assert.deepEqual(Object.keys(fixture).sort(), ['binding', 'resultManifest']);
  assert.deepEqual(fixture.binding.requestedEngines, ['odm', 'treedar']);
  const manifest = fixture.resultManifest;
  protocol.validateResultManifest(manifest, fixture.binding);
  assert.equal(manifest.objects.filter(object => object.role === 'odm_archive').length, 1);
  const archive = manifest.objects.find(object => object.role === 'odm_archive');
  const existingIds = new Set(manifest.objects.map(object => object.id));
  let extraArchiveId = 'odm-wire-probe-extra';
  for (let index = 1; existingIds.has(extraArchiveId); index += 1) extraArchiveId = `odm-wire-probe-extra-${index}`;
  const mutations = {
    missingArchive: { path: 'odm_archive', mutate: value => { value.objects = value.objects.filter(object => object.role !== 'odm_archive'); } },
    duplicateArchive: { path: 'odm_archive', mutate: value => { value.objects.push({ ...archive, id: extraArchiveId }); } },
    zeroArchive: { path: 'object.byteSize', mutate: value => { value.objects.find(object => object.role === 'odm_archive').byteSize = 0; } },
    unsupportedRole: { path: 'object.role', mutate: value => { value.objects[0].role = 'unsupported-stage-output'; } },
    missingTreeDarVersion: { path: 'engineVersions', mutate: value => { delete value.engineVersions.treedar; } },
  };
  const rejected = [];
  for (const [name, scenario] of Object.entries(mutations)) {
    const changed = structuredClone(manifest);
    scenario.mutate(changed);
    changed.resultManifestSha256 = protocol.resultManifestSha256(changed);
    assert.throws(() => protocol.validateResultManifest(changed, fixture.binding),
      error => error instanceof protocol.ReferenceProtocolError && error.path === scenario.path, name);
    rejected.push(name);
  }
  console.log(JSON.stringify({ status: 'accepted', sourceSha256: actualSha256,
    engineVersions: manifest.engineVersions, objectCount: manifest.objects.length,
    resultManifestSha256: manifest.resultManifestSha256, negativeCasesRejected: rejected,
    scope: 'Actual Python synthetic stage closure and pinned Viewer validator only; no processing/runtime acceptance.' }));
}

try { main(); } catch (error) {
  console.error(JSON.stringify({ status: 'held', category: error.name }));
  process.exitCode = 2;
}
