'use strict';

const fs = require('node:fs');
const path = require('node:path');
const zlib = require('node:zlib');
const assert = require('node:assert/strict');
const { createRequire } = require('node:module');
const viewerRequire = createRequire(path.join(process.env.LTDS_VIEWER_APP_ROOT, 'package.json'));
const { crc32, makeZip } = viewerRequire('./test/helpers/zipFixture');

function pngChunk(type, bytes) {
  const name = Buffer.from(type, 'ascii'), length = Buffer.alloc(4), checksum = Buffer.alloc(4);
  length.writeUInt32BE(bytes.length);
  checksum.writeUInt32BE(crc32(Buffer.concat([name, bytes])));
  return Buffer.concat([length, name, bytes, checksum]);
}

function tinyPng() {
  const header = Buffer.alloc(13);
  const pixel = Buffer.from([0, 255, 0, 0, 255]), compressed = zlib.deflateSync(pixel);
  header.writeUInt32BE(1, 0); header.writeUInt32BE(1, 4);
  header[8] = 8; header[9] = 6;
  const png = Buffer.concat([Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]),
    pngChunk('IHDR', header), pngChunk('IDAT', compressed),
    pngChunk('IEND', Buffer.alloc(0))]);
  assert.deepEqual(png.subarray(0, 8), Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]));
  let offset = 8, chunks = 0, decodedPixel = null;
  while (offset < png.length) {
    const length = png.readUInt32BE(offset), type = png.subarray(offset + 4, offset + 8);
    const data = png.subarray(offset + 8, offset + 8 + length), checksum = png.readUInt32BE(offset + 8 + length);
    assert.equal(crc32(Buffer.concat([type, data])), checksum, `PNG ${type.toString('ascii')} CRC`);
    if (type.toString('ascii') === 'IDAT') decodedPixel = zlib.inflateSync(data);
    offset += 12 + length; chunks++;
  }
  assert.equal(offset, png.length); assert.equal(chunks, 3);
  assert.deepEqual(decodedPixel, pixel);
  return png;
}

function makeOneTriangleGlb(texture) {
  const positions = Buffer.alloc(36), normals = Buffer.alloc(36), texcoords = Buffer.alloc(24);
  const vertices = [[0, 0, 0], [1, 0, 0], [0, 1, 0]];
  vertices.forEach(([x, y, z], index) => {
    positions.writeFloatLE(x, index * 12); positions.writeFloatLE(y, index * 12 + 4); positions.writeFloatLE(z, index * 12 + 8);
    normals.writeFloatLE(0, index * 12); normals.writeFloatLE(0, index * 12 + 4); normals.writeFloatLE(1, index * 12 + 8);
    texcoords.writeFloatLE(x, index * 8); texcoords.writeFloatLE(y, index * 8 + 4);
  });
  const indices = Buffer.alloc(8);
  indices.writeUInt16LE(0, 0); indices.writeUInt16LE(1, 2); indices.writeUInt16LE(2, 4);
  const chunks = [positions, normals, texcoords, indices];
  let offset = 0;
  const views = chunks.map((bytes, index) => {
    const view = { buffer: 0, byteOffset: offset, byteLength: index === 3 ? 6 : bytes.length,
      target: index === 3 ? 34963 : 34962 };
    offset += bytes.length;
    return view;
  });
  const imageOffset = offset;
  chunks.push(texture);
  views.push({ buffer: 0, byteOffset: imageOffset, byteLength: texture.length });
  const imageBufferView = views.length - 1;
  const bin = Buffer.concat(chunks);
  const document = {
    asset: { version: '2.0', generator: 'LedgeTop deterministic ingest test' }, scene: 0,
    scenes: [{ nodes: [0] }], nodes: [{ mesh: 0 }],
    meshes: [{ primitives: [{ attributes: { POSITION: 0, NORMAL: 1, TEXCOORD_0: 2 }, indices: 3, material: 0 }] }],
    materials: [{ pbrMetallicRoughness: { baseColorTexture: { index: 0 }, metallicFactor: 0, roughnessFactor: 1 } }],
    textures: [{ source: 0 }], images: [{ bufferView: imageBufferView, mimeType: 'image/png' }],
    accessors: [
      { bufferView: 0, componentType: 5126, count: 3, type: 'VEC3', min: [0, 0, 0], max: [1, 1, 0] },
      { bufferView: 1, componentType: 5126, count: 3, type: 'VEC3' },
      { bufferView: 2, componentType: 5126, count: 3, type: 'VEC2' },
      { bufferView: 3, componentType: 5123, count: 3, type: 'SCALAR' },
    ],
    bufferViews: views, buffers: [{ byteLength: bin.length }],
  };
  const json = Buffer.from(JSON.stringify(document));
  const paddedJson = Buffer.alloc((json.length + 3) & ~3, 0x20); json.copy(paddedJson);
  const paddedBin = Buffer.alloc((bin.length + 3) & ~3); bin.copy(paddedBin);
  const output = Buffer.alloc(12 + 8 + paddedJson.length + 8 + paddedBin.length);
  output.writeUInt32LE(0x46546c67, 0); output.writeUInt32LE(2, 4); output.writeUInt32LE(output.length, 8);
  output.writeUInt32LE(paddedJson.length, 12); output.writeUInt32LE(0x4e4f534a, 16); paddedJson.copy(output, 20);
  const binHeader = 20 + paddedJson.length;
  output.writeUInt32LE(paddedBin.length, binHeader); output.writeUInt32LE(0x004e4942, binHeader + 4);
  paddedBin.copy(output, binHeader + 8);
  assert.equal(output.readUInt32LE(0), 0x46546c67);
  assert.equal(output.readUInt32LE(4), 2);
  assert.equal(output.readUInt32LE(8), output.length);
  const jsonLength = output.readUInt32LE(12);
  assert.equal(output.readUInt32LE(16), 0x4e4f534a);
  const parsed = JSON.parse(output.subarray(20, 20 + jsonLength).toString('utf8').trim());
  assert.deepEqual(parsed.meshes[0].primitives[0].attributes, { POSITION: 0, NORMAL: 1, TEXCOORD_0: 2 });
  assert.equal(parsed.meshes[0].primitives[0].indices, 3);
  const emittedBinHeader = 20 + jsonLength;
  assert.equal(output.readUInt32LE(emittedBinHeader + 4), 0x004e4942);
  const emittedBinLength = output.readUInt32LE(emittedBinHeader);
  const emittedBin = output.subarray(emittedBinHeader + 8, emittedBinHeader + 8 + emittedBinLength);
  assert.equal(emittedBin.length, (parsed.buffers[0].byteLength + 3) & ~3,
    'GLB BIN chunk has only spec-permitted four-byte padding');
  const imageView = parsed.bufferViews[parsed.images[0].bufferView];
  assert.ok(imageView, 'GLB image buffer view exists');
  assert.deepEqual(emittedBin.subarray(imageView.byteOffset, imageView.byteOffset + imageView.byteLength), texture,
    'GLB image view points to the exact embedded PNG bytes');
  assert.deepEqual(parsed.accessors[0].min, [0, 0, 0]);
  assert.deepEqual(parsed.accessors[0].max, [1, 1, 0]);
  const indexView = parsed.bufferViews[parsed.accessors[3].bufferView];
  assert.deepEqual([...emittedBin.subarray(indexView.byteOffset, indexView.byteOffset + indexView.byteLength)], [0, 0, 1, 0, 2, 0]);
  return output;
}

async function main() {
  const png = tinyPng();
  const ept = {
    version: '1.1.0', bounds: [0, 0, 0, 1, 1, 1], boundsConforming: [0, 0, 0, 1, 1, 1],
    dataType: 'binary', hierarchyType: 'json', points: 1, span: 128,
    schema: ['X', 'Y', 'Z'].map(name => ({ name, type: 'signed', size: 4, scale: 0.01, offset: 0 })),
  };
  const entries = [
    { path: 'odm_texturing/odm_textured_model_geo.obj', data: [
      'mtllib odm_textured_model_geo.mtl', 'o synthetic-triangle', 'v 0 0 0', 'v 1 0 0', 'v 0 1 0',
      'vt 0 0', 'vt 1 0', 'vt 0 1', 'vn 0 0 1', 'usemtl surface', 'f 1/1/1 2/2/1 3/3/1', '',
    ].join('\n') },
    { path: 'odm_texturing/odm_textured_model_geo.mtl', data: [
      'newmtl surface', 'Ka 1 1 1', 'Kd 1 1 1', 'Ks 0 0 0', 'd 1', 'illum 1',
      'map_Kd odm_textured_model_geo_0.png', '',
    ].join('\n') },
    { path: 'odm_texturing/odm_textured_model_geo_0.png', data: png },
    { path: 'odm_texturing/odm_textured_model_geo.glb', data: makeOneTriangleGlb(png) },
    { path: 'entwine_pointcloud/ept.json', data: JSON.stringify(ept) },
    { path: 'entwine_pointcloud/ept-data/0-0-0-0.bin', data: Buffer.alloc(12) },
    { path: 'entwine_pointcloud/ept-hierarchy/0-0-0-0.json', data: JSON.stringify({ '0-0-0-0': 1 }) },
  ];
  fs.writeFileSync(process.argv[2], makeZip(entries));
}

main().catch(error => { process.stderr.write(`${error.name}: ${error.message}\n`); process.exitCode = 1; });
