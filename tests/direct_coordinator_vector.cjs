'use strict';
// Fixture fields are fixed ASCII keys/values and safe integers. This deliberately
// is not a generic JCS implementation; production must use a qualified library.
const fs=require('node:fs'),crypto=require('node:crypto'),assert=require('node:assert/strict');
const vector=JSON.parse(fs.readFileSync(process.argv[2],'utf8'));
function canonical(value){
  if(Array.isArray(value))return '['+value.map(canonical).join(',')+']';
  if(value&&typeof value==='object')return '{'+Object.keys(value).sort().map(k=>JSON.stringify(k)+':'+canonical(value[k])).join(',')+'}';
  if(typeof value==='string'&&!/^[\x20-\x7e]*$/.test(value))throw Error('ASCII fixture only');
  if(typeof value==='number'&&!Number.isSafeInteger(value))throw Error('safe fixture integers only');
  return JSON.stringify(value);
}
const {signature,...unsigned}=vector.receipt;
const text=canonical(unsigned);
assert.equal(text,vector.unsignedCanonicalUtf8);
assert.equal(crypto.createHash('sha256').update(text).digest('hex'),vector.unsignedSha256);
assert.equal(crypto.verify(null,Buffer.concat([Buffer.from(vector.domainHex,'hex'),Buffer.from(text)]),
  vector.publicKeyPem,Buffer.from(signature,'base64url')),true);
const changed=structuredClone(unsigned);changed.body.generation++;
assert.equal(crypto.verify(null,Buffer.concat([Buffer.from(vector.domainHex,'hex'),Buffer.from(canonical(changed))]),
  vector.publicKeyPem,Buffer.from(signature,'base64url')),false);
console.log('synthetic Python/Node receipt vector verified');
