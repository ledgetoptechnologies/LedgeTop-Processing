'use strict';
const crypto = require('node:crypto');
const fs = require('node:fs');
const https = require('node:https');
const { createReferenceAcceptanceApi } = require('/viewer/apps/viewer/server/referenceAcceptanceApi');
const { ReferenceAcceptanceLedger } = require('/viewer/apps/viewer/server/referenceAcceptance');
const { createViewerReceiptSigner } = require('/viewer/apps/viewer/server/referenceReceipt');
const PORT = Number(process.env.PORT || 44443);
const KEY_ID = 'synthetic-service', SECRET = 'synthetic-viewer-service-key-material';
const nonceSet = new Set(), counters = { chunkResponsesDropped: 0, commitResponsesDropped: 0, chunksAcknowledged: 0, commits: 0 };
const repository = { consumeServiceNonce(key, nonce) { const id = key + ':' + nonce; if (nonceSet.has(id)) return false; nonceSet.add(id); return true; } };
const pair = crypto.generateKeyPairSync('ed25519');
const publicKeyPem = pair.publicKey.export({ type: 'spki', format: 'pem' }).toString();
const receiptSigner = createViewerReceiptSigner({ issuerId: 'synthetic-viewer-issuer', keyId: 'synthetic-viewer-key',
  privateKeyPem: pair.privateKey.export({ type: 'pkcs8', format: 'pem' }).toString() });
fs.mkdirSync('/data/managed', { recursive: true });
const ledger = new ReferenceAcceptanceLedger({ databasePath: '/data/ledger.sqlite', managedRoot: '/data/managed', maxManagedBytes: 1024 * 1024 });
class MiniRouter {
  constructor() { this.layers = []; }
  use(handler) { this.layers.push({ method: null, pattern: null, handler }); }
  get(pattern, handler) { this.layers.push({ method: 'GET', pattern, handler }); }
  put(pattern, handler) { this.layers.push({ method: 'PUT', pattern, handler }); }
  post(pattern, handler) { this.layers.push({ method: 'POST', pattern, handler }); }
}
function match(pattern, pathname) {
  const a = pattern.split('/').filter(Boolean), b = pathname.split('/').filter(Boolean);
  if (a.length !== b.length) return null;
  const params = {};
  for (let i = 0; i < a.length; i++) {
    if (a[i].startsWith(':')) { try { params[a[i].slice(1)] = decodeURIComponent(b[i]); } catch { return null; } }
    else if (a[i] !== b[i]) return null;
  }
  return params;
}
const router = createReferenceAcceptanceApi({ repository, ledger, serviceKeyId: KEY_ID,
  serviceAuthKeys: { [KEY_ID]: SECRET }, receiptSigner, environment: 'staging',
  chunkBytes: 4 * 1024 * 1024, routerFactory: () => new MiniRouter() });
const server = https.createServer({ key: fs.readFileSync('/harness/server.key'), cert: fs.readFileSync('/harness/server.crt') }, async (req, res) => {
  const url = new URL(req.url, 'https://localhost');
  req.originalUrl = req.url; req.path = url.pathname; req.query = Object.fromEntries(url.searchParams.entries());
  req.get = name => req.headers[String(name).toLowerCase()];
  req.is = type => (req.headers['content-type'] || '').split(';')[0].trim().toLowerCase() === type.toLowerCase();
  req.params = {};
  res.status = function(code) { this.statusCode = code; return this; };
  res.json = function(value) {
    const path = req.path;
    if (path === '/_test/receipt-public-key') {
      const body = Buffer.from(JSON.stringify({ issuerId: 'synthetic-viewer-issuer', keyId: 'synthetic-viewer-key', publicKeyPem }));
      this.setHeader('Content-Type', 'application/json'); this.setHeader('Content-Length', String(body.length)); this.end(body); return this;
    }
    if (path === '/_test/stats') {
      const body = Buffer.from(JSON.stringify(counters));
      this.setHeader('Content-Type', 'application/json'); this.setHeader('Content-Length', String(body.length)); this.end(body); return this;
    }
    if (path.includes('/objects/') && counters.chunkResponsesDropped === 0) {
      counters.chunksAcknowledged++; counters.chunkResponsesDropped++; this.socket.destroy(); return this;
    }
    if (path.endsWith('/commit') && counters.commitResponsesDropped === 0) {
      counters.commits++; counters.commitResponsesDropped++; this.socket.destroy(); return this;
    }
    if (path.includes('/objects/')) counters.chunksAcknowledged++;
    if (path.endsWith('/commit')) counters.commits++;
    const body = Buffer.from(JSON.stringify(value));
    this.setHeader('Content-Type', 'application/json'); this.setHeader('Content-Length', String(body.length)); this.end(body); return this;
  };
  if (req.is('application/json') && ['PUT', 'POST'].includes(req.method)) {
    const parts = []; let total = 0;
    for await (const part of req) { total += part.length; if (total > 5 * 1024 * 1024) { res.statusCode = 413; res.end(); return; } parts.push(part); }
    req.rawBody = Buffer.concat(parts);
    if (req.rawBody.length) { try { req.body = JSON.parse(req.rawBody.toString('utf8')); } catch { res.statusCode = 400; res.end(); return; } }
  }
  let i = 0;
  const run = error => {
    if (res.writableEnded || res.destroyed) return;
    if (error) { res.statusCode = 500; res.end('harness failure'); return; }
    while (i < router.layers.length) {
      const layer = router.layers[i++];
      if (!layer.method) {
        try { const result = layer.handler(req, res, run); if (result && typeof result.then === 'function') result.catch(run); }
        catch (cause) { run(cause); }
        return;
      }
      if (layer.method !== req.method) continue;
      const params = match(layer.pattern, req.path);
      if (!params) continue;
      req.params = params;
      try { const result = layer.handler(req, res, run); if (result && typeof result.then === 'function') result.catch(run); }
      catch (cause) { run(cause); }
      return;
    }
    res.statusCode = 404; res.end();
  };
  if (req.path === '/_test/receipt-public-key' || req.path === '/_test/stats') return res.json({});
  run();
});
server.listen(PORT, '0.0.0.0', () => process.stdout.write('READY ' + PORT + '\n'));
process.on('SIGTERM', () => server.close(() => { try { ledger.close(); } catch {} process.exit(0); }));

