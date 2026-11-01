'use strict';
// Review-only wire codec. It authenticates neither sender nor receipt.
const crypto = require('node:crypto');
const { TextDecoder } = require('node:util');
const MAX_ENVELOPE = 4 * 1024 * 1024, MAX_RECEIPT = 2 * 1024 * 1024, MAX_DEPTH = 8, MAX_NODES = 50000;
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
function fail(){ throw new Error('invalid candidate receipt wire'); }
function canonical(value,depth=0,budget={remaining:MAX_NODES}){
  if(depth>MAX_DEPTH || --budget.remaining<0) fail();
  if(value === null || typeof value === 'boolean') return JSON.stringify(value);
  if(typeof value === 'string') { if(/[\uD800-\uDFFF]/u.test(value)) fail(); return JSON.stringify(value); }
  if(typeof value === 'number') { if(!Number.isSafeInteger(value) || value < 0 || Object.is(value,-0)) fail(); return JSON.stringify(value); }
  if(Array.isArray(value)) return '[' + value.map(item=>canonical(item,depth+1,budget)).join(',') + ']';
  if(typeof value === 'object' && Object.getPrototypeOf(value) === Object.prototype){
    const keys=Object.keys(value).sort(); if(keys.some(k => /[^\x00-\x7f]/.test(k))) fail();
    return '{' + keys.map(k => JSON.stringify(k)+':'+canonical(value[k],depth+1,budget)).join(',') + '}';
  }
  fail();
}
function parse(raw){
  try { const s=new TextDecoder('utf-8',{fatal:true}).decode(raw), value=JSON.parse(s);
    if(!Buffer.from(canonical(value),'utf8').equals(raw)) fail(); return value;
  } catch { fail(); }
}
function decode(raw){
  if(!Buffer.isBuffer(raw) || raw.length < 1 || raw.length > MAX_ENVELOPE) fail();
  const env=parse(raw);
  if(!env || Array.isArray(env) || typeof env !== 'object' || Object.keys(env).sort().join('|') !== 'attemptId|bindingId|receiptBase64url|receiptSha256|schemaVersion') fail();
  if(env.schemaVersion !== 1 || typeof env.attemptId !== 'string' || typeof env.bindingId !== 'string' || env.attemptId.length !== 36 || env.bindingId.length !== 36 || !uuid.test(env.attemptId) || !uuid.test(env.bindingId)) fail();
  const e=env.receiptBase64url;
  if(typeof e!=='string' || !/^[A-Za-z0-9_-]+$/.test(e) || e.length>Math.floor((MAX_RECEIPT*4+2)/3)) fail();
  const b=Buffer.from(e,'base64url'); if(!b.length || b.length>MAX_RECEIPT || b.toString('base64url')!==e) fail();
  if(typeof env.receiptSha256 !== 'string' || !/^[a-f0-9]{64}$/.test(env.receiptSha256) || crypto.createHash('sha256').update(b).digest('hex')!==env.receiptSha256) fail();
  const receipt=parse(b); if(!receipt || Array.isArray(receipt) || typeof receipt!=='object') fail();
  return {envelope:env,receipt,receiptBytes:b};
}
module.exports={decode,canonical};
