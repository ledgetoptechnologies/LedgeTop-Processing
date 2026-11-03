'use strict';

// Synthetic loopback bridge for the opt-in Processing/Viewer source renewal test.
// It exercises the real Viewer route handler while accepting only loopback HTTP
// marked by the paired test's private in-process adapter. It never binds beyond
// 127.0.0.1 and uses no production configuration or credentials.
const crypto = require('node:crypto');
const fs = require('node:fs');
const http = require('node:http');
const path = require('node:path');

const viewerRoot = process.env.LEDGETOP_VIEWER_SOURCE_DIR;
const readyPath = process.env.LEDGETOP_PAIRED_READY_FILE;
if (typeof viewerRoot !== 'string' || !path.isAbsolute(viewerRoot)
    || typeof readyPath !== 'string' || !path.isAbsolute(readyPath)) {
  throw new Error('paired Viewer paths are required');
}

const protocol = require(path.join(viewerRoot, 'apps/viewer/server/referenceProtocol.js'));
const { createSourceRangeV2Handler } = require(path.join(viewerRoot, 'apps/viewer/server/sourceRangeV2Candidate.js'));
const pending = new Map();
const digest = value => crypto.createHash('sha256').update(value).digest('hex');
const canonical = value => protocol.canonicalBytes(value).toString('utf8');
const fail = code => { throw Object.assign(new Error(code), { code }); };

const handler = createSourceRangeV2Handler({
  broker: { readAuthorizedRange() { throw new Error('range route not used by this test'); } },
  enabled: true,
  isProtectedTransport: req => req.socket?.remoteAddress === '127.0.0.1'
    && req.headers?.['x-ledgetop-paired-test'] === 'loopback-only',
  renewGrant(request) {
    const fields = ['protocol', 'renewalId', 'sequence', 'previousGrantSha256', 'previousGrant'];
    if (!request || Object.getPrototypeOf(request) !== Object.prototype
        || Object.keys(request).length !== fields.length || fields.some(key => !Object.hasOwn(request, key))
        || request.protocol !== 'source-range-v2-renewal'
        || !Number.isSafeInteger(request.sequence) || request.sequence < 1) {
      fail('source_grant_renewal_request_invalid');
    }
    if (digest(Buffer.from(canonical(request.previousGrant))) !== request.previousGrantSha256) {
      fail('source_grant_renewal_binding');
    }
    const requestBytes = canonical(request);
    const previous = pending.get(request.renewalId);
    if (previous) {
      if (previous.requestBytes !== requestBytes) fail('source_grant_renewal_conflict');
      return previous.response;
    }
    const grant = {
      ...request.previousGrant,
      expiresAt: new Date(Date.now() + 120_000).toISOString(),
      token: crypto.randomBytes(32).toString('base64url'),
    };
    if (Date.parse(grant.expiresAt) <= Date.parse(request.previousGrant.expiresAt)) {
      fail('source_grant_renewal_binding');
    }
    const response = {
      protocol: request.protocol,
      renewalId: request.renewalId,
      sequence: request.sequence,
      previousGrantSha256: request.previousGrantSha256,
      grant,
    };
    pending.set(request.renewalId, { requestBytes, response, acknowledgementBytes: null, acknowledgement: null });
    return response;
  },
  acknowledgeGrantRenewal(ack) {
    const record = pending.get(ack?.renewalId);
    if (!record || ack.protocol !== 'source-range-v2-renewal-ack'
        || ack.sequence !== record.response.sequence
        || ack.grantSha256 !== digest(Buffer.from(canonical(record.response.grant)))
        || ack.token !== record.response.grant.token) {
      fail('source_grant_renewal_ack_binding');
    }
    const ackBytes = canonical(ack);
    if (record.acknowledgementBytes !== null) {
      if (record.acknowledgementBytes !== ackBytes) fail('source_grant_renewal_ack_conflict');
      return record.acknowledgement;
    }
    record.acknowledgementBytes = ackBytes;
    record.acknowledgement = {
      protocol: ack.protocol,
      renewalId: ack.renewalId,
      sequence: ack.sequence,
      grantSha256: ack.grantSha256,
    };
    return record.acknowledgement;
  },
});

const server = http.createServer((req, res) => {
  Promise.resolve(handler(req, res)).then(handled => {
    if (!handled && !res.headersSent) {
      res.statusCode = 404;
      res.end();
    }
  }).catch(() => {
    if (!res.headersSent) {
      res.statusCode = 500;
      res.end();
    } else {
      res.destroy();
    }
  });
});

server.listen(0, '127.0.0.1', () => {
  fs.writeFileSync(readyPath, JSON.stringify({ port: server.address().port }), { flag: 'wx', mode: 0o600 });
});

function stop() {
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 2_000).unref();
}
process.once('SIGTERM', stop);
process.once('SIGINT', stop);
