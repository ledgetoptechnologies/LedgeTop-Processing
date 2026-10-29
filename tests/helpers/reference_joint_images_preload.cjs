'use strict';
// Synthetic fixture transport only. No credentials, budgets, capabilities,
// manifests or validators are replaced; delegate exactly once to real mintGrant.
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const count = Number(process.env.LTDS_REFERENCE_SYNTHETIC_IMAGES ?? '1');
if (!Number.isSafeInteger(count) || count < 1 || count > 50) throw Error('bounded_synthetic_image_count');
if (count !== 1) {
  if (process.env.REFERENCE_SYNTHETIC_FIXTURE !== '1') throw Error('synthetic_fixture_required');
  const root = process.env.VIEWER_REFERENCE_ROOT;
  if (!root || !path.isAbsolute(root)) throw Error('explicit_pinned_viewer_fixture_required');
  const {ReferenceTransferBroker} = require(path.join(root,'apps/viewer/server/referenceTransferBroker.js'));
  const mint = ReferenceTransferBroker.prototype.mintGrant;
  const bytes = Buffer.from('abc'); const digest = crypto.createHash('sha256').update(bytes).digest('hex');
  ReferenceTransferBroker.prototype.mintGrant = function(input) {
    if (!input || input.manifest?.objects.length !== count || input.manifest.inputCounts.images !== count) throw Error('synthetic_manifest_count');
    const first = input.objectFiles?.[input.manifest.objects[0].id];
    if (typeof first !== 'string' || !path.isAbsolute(first)) throw Error('synthetic_original_source_required');
    const parent = path.dirname(first); const stat = fs.lstatSync(parent);
    if (!stat.isDirectory() || stat.isSymbolicLink() || (stat.mode & 0o077)) throw Error('private_synthetic_source_required');
    const files = {...input.objectFiles};
    for (const object of input.manifest.objects) {
      if (!/^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/.test(object.id) || object.kind !== 'image' || object.byteSize !== 3 || object.sha256 !== digest) throw Error('synthetic_abc_source_only');
      if (!Object.hasOwn(files,object.id)) {
        const target = path.join(parent,object.id+'.JPG'); let fd;
        try { fd = fs.openSync(target,'wx',0o600); fs.writeFileSync(fd,bytes); fs.fsyncSync(fd); }
        catch (error) { if (error.code !== 'EEXIST') throw error; }
        finally { if (fd !== undefined) fs.closeSync(fd); }
        const source = fs.lstatSync(target);
        if (!source.isFile() || source.isSymbolicLink() || source.nlink !== 1 || source.size !== 3 || !fs.readFileSync(target).equals(bytes)) throw Error('synthetic_source_changed');
        files[object.id] = target;
      }
    }
    const directory = fs.openSync(parent,fs.constants.O_RDONLY | fs.constants.O_DIRECTORY);
    try { fs.fsyncSync(directory); } finally { fs.closeSync(directory); }
    return mint.call(this,{...input,objectFiles:files});
  };
}
