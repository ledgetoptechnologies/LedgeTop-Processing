'use strict';
// Test-only interoperability check; this is not a production validator.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const viewer=process.env.VIEWER_SOURCE;
if(!viewer||!path.isAbsolute(viewer))throw new Error('absolute Viewer checkout required');
const p=require(path.join(viewer,'apps/viewer/server/referenceProtocol'));
const vector=JSON.parse(fs.readFileSync(process.argv[2],'utf8'));
const {signature,...body}=vector.receipt,bytes=p.canonicalBytes(body);
const digest=crypto.createHash('sha256').update(bytes).digest('hex');
const ok=crypto.verify(null,Buffer.concat([Buffer.from('LTDS-VIEWER-ACCEPTED-COPY-ACK-V2\0','ascii'),bytes]),vector.publicKeyPem,Buffer.from(signature,'base64url'));
if(!ok||digest!==vector.bodyCanonicalSha256)throw new Error('proposal vector mismatch');
process.stdout.write(JSON.stringify({verified:true,bodyCanonicalSha256:digest}));
