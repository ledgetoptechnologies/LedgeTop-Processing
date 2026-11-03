'use strict';
// Synthetic test host for the Viewer source-range v2 broker candidate (Ops PR #429).
// Loopback HTTP only: protected transport is asserted by the test, not proven, so
// this host says nothing about production TLS or proxy mounting. Never deploy it.
//
//   node broker_host.cjs '{"serverDir": "...", "databasePath": "...", "managedRoot": "...",
//                          "grantSecret": "...", "allowCreate": true, "nowMs": null}'
//
// Prints one line "READY <port>". Control requests are POST /__control JSON:
//   {"op": "mint", "job": {...}, "paths": {"<objectId>": "<absolute path>"}}
//   {"op": "usage"}  {"op": "revoke", "identityKey": "..."}  {"op": "now", "nowMs": 123}
const http = require('node:http');
const path = require('node:path');

const config = JSON.parse(process.argv[2]);
const {
  SourceRangeV2BrokerCandidate, createSourceRangeV2Handler, deriveSourceRangeGrantTokenKey,
} = require(path.join(config.serverDir, 'sourceRangeV2Candidate.js'));

let nowMs = config.nowMs;
const broker = new SourceRangeV2BrokerCandidate({
  databasePath: config.databasePath, managedRoot: config.managedRoot,
  grantTokenKey: deriveSourceRangeGrantTokenKey(config.grantSecret),
  now: () => (nowMs === null || nowMs === undefined ? Date.now() : nowMs),
  allowCreate: config.allowCreate === true,
});
const handler = createSourceRangeV2Handler({ broker, enabled: true, isProtectedTransport: () => true });

function control(body) {
  if (body.op === 'mint') {
    const resolveSource = request => ({
      jobId: request.jobId, generation: request.generation, environment: request.environment,
      manifestSha256: request.manifestSha256, sourceVersion: request.sourceVersion,
      objects: request.manifest.objects.map(object => ({
        id: object.id, path: body.paths[object.id], byteSize: object.byteSize, sha256: object.sha256,
      })),
    });
    return { grant: broker.mintGrant({ job: body.job, resolveSource }) };
  }
  if (body.op === 'usage') {
    return {
      usage: broker.db.prepare('SELECT identity_key,request_budget,byte_budget,attempt_budget,requests_used,bytes_used,attempts_used FROM source_range_usage').all(),
      ranges: broker.db.prepare('SELECT object_id,start_byte,byte_length FROM source_range_requests ORDER BY object_id,start_byte').all(),
    };
  }
  if (body.op === 'revoke') { broker.revoke(body.identityKey); return {}; }
  if (body.op === 'now') { nowMs = body.nowMs; return {}; }
  throw new Error('unknown control op');
}

const server = http.createServer(async (req, res) => {
  if (req.url === '/__control' && req.method === 'POST') {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    try {
      const result = control(JSON.parse(Buffer.concat(chunks).toString('utf8')));
      res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify(result));
    } catch (error) {
      res.writeHead(409, { 'content-type': 'application/json' }).end(JSON.stringify({ error: error.code || error.message }));
    }
    return;
  }
  if (!(await handler(req, res))) res.writeHead(404).end();
});
server.listen(0, '127.0.0.1', () => process.stdout.write(`READY ${server.address().port}\n`));
const stop = () => { server.close(); broker.close(); process.exit(0); };
process.on('SIGTERM', stop);
process.on('SIGINT', stop);
