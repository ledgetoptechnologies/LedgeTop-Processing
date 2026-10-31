'use strict';
// Independent cross-language check of provisional canonical bytes and hashes.
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const assert = require('node:assert/strict');
const fixture = JSON.parse(fs.readFileSync(path.join(__dirname, '../contracts/source-proof-v2-golden-draft.json'), 'utf8'));
function canonical(value) {
  if (Array.isArray(value)) return '[' + value.map(canonical).join(',') + ']';
  if (value !== null && typeof value === 'object')
    return '{' + Object.keys(value).sort().map(k => JSON.stringify(k) + ':' + canonical(value[k])).join(',') + '}';
  if (typeof value === 'number') assert(Number.isSafeInteger(value) && !Object.is(value, -0));
  return JSON.stringify(value);
}
function hash(domain, bytes) {
  return crypto.createHash('sha256').update(Buffer.from(domain + '\0', 'ascii')).update(Buffer.from(bytes, 'utf8')).digest('hex');
}
for (const [key, domain] of Object.entries({root:'ltds-source-root-v2', emptyObject:'ltds-source-object-v2', object:'ltds-source-object-v2', page:'ltds-source-page-v2'})) {
  const document = fixture.documents[key];
  assert.equal(canonical(JSON.parse(document.utf8)), document.utf8);
  assert.equal(hash(domain, document.utf8), document.sha256);
}
const manifest = {...fixture.inputManifest};
delete manifest.sha256;
assert.equal(crypto.createHash('sha256').update(Buffer.from(canonical(manifest), 'utf8')).digest('hex'), fixture.inputManifest.sha256);
const boundaries = JSON.parse(fs.readFileSync(path.join(__dirname, '../contracts/source-proof-v2-page-boundaries-draft.json'), 'utf8'));
for (const [key, domain] of Object.entries({firstPage:'ltds-source-page-v2', lastPage:'ltds-source-page-v2', object:'ltds-source-object-v2'})) {
  const document = boundaries.documents[key];
  assert.equal(canonical(JSON.parse(document.utf8)), document.utf8);
  assert.equal(hash(domain, document.utf8), document.sha256);
}
const first = JSON.parse(boundaries.documents.firstPage.utf8);
const last = JSON.parse(boundaries.documents.lastPage.utf8);
assert.equal(first.chunks.length, 256);
assert.equal(last.pageIndex, 1);
assert.equal(last.chunks.length, 1);
console.log('provisional source-proof-v2 golden bytes and hashes match');
