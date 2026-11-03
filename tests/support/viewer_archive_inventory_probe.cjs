'use strict';
// Load the real Viewer module and expose its private pure helpers only in this
// process. ZIP extraction is not exercised; stub its unrelated dependency.
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const Module = require('node:module');
const viewer = process.env.LTDS_VIEWER_SOURCE_DIR;
if (!viewer) throw new Error('LTDS_VIEWER_SOURCE_DIR is required');
const filename = path.join(path.resolve(viewer), 'apps/viewer/server/processingArchiveEvidence.js');
const mod = new Module(filename, module);
mod.filename = filename;
mod.paths = Module._nodeModulePaths(path.dirname(filename));
mod.require = function (name) {
  if (name === './safeZip') return { extractZipDescriptor() { throw new Error('not used'); } };
  return Module.prototype.require.call(this, name);
};
mod._compile(fs.readFileSync(filename, 'utf8') + '\nmodule.exports.__inventoryProbe = { canonical, validateInventory };\n', filename);
const input = JSON.parse(fs.readFileSync(0, 'utf8'));
const canonical = mod.exports.__inventoryProbe.canonical(input);
let accepted = true;
try { mod.exports.__inventoryProbe.validateInventory(input, 100000, Number.MAX_SAFE_INTEGER); }
catch (error) { if (error.code !== 'invalid_archive') throw error; accepted = false; }
const json = JSON.stringify(canonical);
process.stdout.write(JSON.stringify({ canonicalJson: json, sha256: crypto.createHash('sha256').update(json, 'utf8').digest('hex'), accepted }));
