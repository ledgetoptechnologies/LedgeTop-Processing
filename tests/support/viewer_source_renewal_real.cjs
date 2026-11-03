'use strict';
// Synthetic loopback host for the end-to-end source-range renewal test. It runs
// the real Viewer database migrations, reference dispatcher, source broker and
// grant renewal service from LEDGETOP_VIEWER_SOURCE_DIR against file databases
// in a private test directory, so a kill and restart keeps all durable state.
// Provider HTTP is replaced by an in-process fake that returns the transfer
// request Processing built. Loopback only; protected transport is asserted, not
// proven. Never deploy it.
//
// Prints one line "READY <port>". Control requests are POST /__control JSON:
//   {"op":"submission"} -> the prepared Viewer submission and attempt ID
//   {"op":"dispatch","transferRequest":{...}} -> mints the initial grant through the real dispatcher
//   {"op":"usage"} | {"op":"head"} | {"op":"clock","offsetMs":n}
const crypto = require('node:crypto');
const fs = require('node:fs');
const http = require('node:http');
const path = require('node:path');
const { DatabaseSync } = require('node:sqlite');

const server = path.join(process.env.LEDGETOP_VIEWER_SOURCE_DIR, 'apps/viewer/server');
const need = name => require(path.join(server, name));
const { applyMigrations } = need('database.js');
const p = need('referenceProtocol.js');
const { prepareReferenceSubmission } = need('referenceSubmissionDraft.js');
const { ReferenceProvider } = need('referenceProvider.js');
const { ReferenceAppJournal, createSyntheticReferenceDispatcher } = need('referenceAppDispatch.js');
const { ReferenceTransferOutbox } = need('referenceTransferOutbox.js');
const { ReferenceTransferGrantVault, deriveReferenceTransferGrantKey } = need('referenceTransferGrantVault.js');
const { createSourceRangeV2TransferDispatcher } = need('sourceRangeV2TransferDispatcher.js');
const { ReferenceTransferGrantRenewals, ReferenceTransferRenewalVault } = need('referenceTransferGrantRenewals.js');
const { SourceRangeV2BrokerCandidate, createSourceRangeV2Handler, deriveSourceRangeGrantTokenKey } = need('sourceRangeV2Candidate.js');

const root = process.env.PAIRED_STATE_DIR;
const ATTEMPT = '00000000-0000-4000-8000-000000000001';
const SECRET = 'synthetic paired renewal secret 0123456789abcdef';
let offsetMs = 0;
const now = () => Date.now() + offsetMs;
const source = path.join(root, 'source', 'a.jpg');
const sourceBytes = fs.readFileSync(source);
const viewerPath = path.join(root, 'viewer.sqlite');
const fresh = !fs.existsSync(viewerPath);
const db = new DatabaseSync(viewerPath);
// The fixture drops the Viewer's reference admission triggers (as the Viewer's
// own dispatcher tests do), which the protected-migration guard then rejects on
// a second applyMigrations. A restart therefore reopens the migrated file as-is:
// this tests durable broker and renewal state, not migration or startup guards.
if (fresh) applyMigrations(db);

const caps = p.createCapabilities('staging'), definitions = [{ name: 'dsm', type: 'bool' }];
caps.engines.odm.optionsFingerprint = p.sha256(p.canonicalBytes({ protocol: 'reference-options-v1', definitions }));
if (fresh) {
  db.exec('PRAGMA foreign_keys=OFF');
  db.exec(`INSERT INTO projects(id,display_name,status,created_at,updated_at) VALUES('p','P','active','t','t');
    INSERT INTO datasets(id,project_id,display_name,storage_mode,root_key,relative_path,status,created_at,updated_at) VALUES('d','p','D','managed','datasets','d','finalized','t','t');
    INSERT INTO processing_tasks(id,project_id,dataset_id,display_name,status,created_at,updated_at) VALUES('t','p','d','T','draft','t','t');
    INSERT INTO processing_attempts(id,task_id,dataset_id,attempt_number,options_json,status,created_at,updated_at) VALUES('${ATTEMPT}','t','d',1,'{}','pending','t','t');
    UPDATE processing_tasks SET active_attempt_id='${ATTEMPT}' WHERE id='t';
    UPDATE reference_catalog_projection_state SET scan_complete=1;
    DROP TRIGGER viewer_reference_attempt_admission_unavailable;DROP TRIGGER viewer_reference_provider_unavailable;
    DROP TRIGGER viewer_reference_protocol_unavailable;DROP TRIGGER viewer_reference_genesis_unavailable;
    DROP TRIGGER viewer_reference_genesis_no_existing;
    INSERT INTO viewer_reference_providers VALUES('r','Ref','https://example.test',0);
    INSERT INTO viewer_reference_attempt_genesis VALUES('${ATTEMPT}','r');
    INSERT INTO viewer_reference_attempt_protocols VALUES('${ATTEMPT}','r','reference-v1');`);
  const options = { schemaVersion: 1, environment: 'staging', capabilityFingerprint: caps.capabilityFingerprint,
    optionsFingerprint: caps.engines.odm.optionsFingerprint, options: definitions };
  const request = prepareReferenceSubmission({ enabled: true, dataset: { id: 'd', status: 'finalized', storageMode: 'managed',
    files: [{ id: 'i1', relativePath: 'a.jpg', byteSize: sourceBytes.length, sha256: p.sha256(sourceBytes) }] },
    submissionId: '56f7e8af-73c7-4a51-9903-c59cc68c033b', capabilities: caps, optionsCatalog: options, odmOptions: { dsm: true } }).submission;
  const text = JSON.stringify(request);
  db.prepare('INSERT INTO viewer_reference_attempt_journal(attempt_id,submission_id,environment,request_json,request_sha256,input_manifest_sha256) VALUES(?,?,?,?,?,?)')
    .run(ATTEMPT, request.submissionId, 'staging', text, crypto.createHash('sha256').update(text).digest('hex'), request.manifest.sha256);
  db.exec('PRAGMA foreign_keys=ON');
}
const request = JSON.parse(db.prepare('SELECT request_json FROM viewer_reference_attempt_journal WHERE attempt_id=?').get(ATTEMPT).request_json);
const journal = new ReferenceAppJournal(db);

const brokerDir = path.join(root, 'broker');
if (!fs.existsSync(brokerDir)) fs.mkdirSync(brokerDir, { mode: 0o700 });
const broker = new SourceRangeV2BrokerCandidate({ databasePath: path.join(brokerDir, 'broker.sqlite'),
  managedRoot: path.join(root, 'source'), grantTokenKey: deriveSourceRangeGrantTokenKey(SECRET), now, allowCreate: true });
const resolveSource = binding => ({ jobId: binding.jobId, generation: binding.generation, environment: binding.environment,
  manifestSha256: binding.manifestSha256, sourceVersion: binding.sourceVersion,
  objects: binding.manifest.objects.map(o => ({ id: o.id, path: source, byteSize: o.byteSize, sha256: o.sha256 })) });
// Known Viewer gap at c92deb5: sourceRangeV2TransferDispatcher mints with
// `submission: { manifest }`, dropping the viewerAttemptId the broker requires,
// so the real broker refuses the initial grant with INVALID_MANIFEST. With
// PAIRED_INJECT_DISPATCH_ATTEMPT=1 this host restores it from the transfer
// request so the rest of the lifecycle can be exercised; without it the gap shows.
const mintGrant = (job, options = {}) => {
  if (process.env.PAIRED_INJECT_DISPATCH_ATTEMPT === '1' && job.submission && !('viewerAttemptId' in job.submission))
    job = { ...job, submission: { ...job.submission, viewerAttemptId: job.transferRequest.viewerAttemptId } };
  return broker.mintGrant({ job, resolveSource, ...options });
};
const renewalVault = new ReferenceTransferRenewalVault(db, { key: deriveReferenceTransferGrantKey(SECRET) });
const renewals = new ReferenceTransferGrantRenewals(db, { now, vault: renewalVault });
const grantVault = new ReferenceTransferGrantVault(db, { key: deriveReferenceTransferGrantKey(SECRET) });

const handler = createSourceRangeV2Handler({ broker, enabled: true, isProtectedTransport: () => true,
  renewGrant: value => renewals.renewSourceGrant(value, { mintGrant }),
  acknowledgeGrantRenewal: value => renewals.acknowledgeSourceGrant(value) });

async function dispatch(transferRequest) {
  const job = phase => ({ jobId: 'job-1', generation: 1, environment: 'staging', phase, manifestSha256: request.manifest.sha256,
    pipelineDigest: 'b'.repeat(64), status: phase === 'queued' ? 'queued' : 'running', nodeId: null, transferRequest: null,
    transferGrantAcknowledgement: null, error: null, resultManifestSha256: null });
  let sent = null, ack = null;
  const reply = value => new Response(JSON.stringify(value), { status: 200, headers: { 'content-type': 'application/json' } });
  const previous = global.fetch;
  global.fetch = async (url, init = {}) => {
    if (url.endsWith('/capabilities')) return reply(caps);
    if (url.endsWith('/jobs') && init.method === 'POST') return reply(job('queued'));
    if (url.endsWith('/jobs/job-1')) return reply({ ...job('awaiting_transfer'), transferRequest, transferGrantAcknowledgement: ack });
    if (url.endsWith('/transfer-grants')) {
      sent = JSON.parse(init.body);
      ack = { reservationId: sent.reservationId, generation: sent.generation, manifestSha256: sent.manifestSha256,
        nonce: sent.nonce, accepted: true, grantSha256: p.sha256(p.canonicalBytes(sent)) };
      return reply(ack);
    }
    throw Error('unexpected provider request ' + url);
  };
  try {
    const provider = new ReferenceProvider({ endpoint: 'https://example.test', token: 'synthetic-token-only', environment: 'staging', enabled: true });
    const outbox = new ReferenceTransferOutbox(db, { journal, grantVault, grantRenewals: renewals });
    const transfer = createSourceRangeV2TransferDispatcher({ runtime: { mintGrant, grantRenewals: renewals }, outbox, journal });
    const dispatcher = createSyntheticReferenceDispatcher({ scope: 'synthetic-fixture-only', journal, provider,
      custody: { assertCurrent: async () => {} }, transfer });
    if (await dispatcher({ attemptId: ATTEMPT, owner: 'paired-initial' }) !== true) throw Error('dispatch did not complete');
  } finally { global.fetch = previous; }
  return sent;
}

async function control(body) {
  if (body.op === 'submission') return { attemptId: ATTEMPT, submission: request };
  if (body.op === 'dispatch') return { grant: await dispatch(body.transferRequest) };
  if (body.op === 'head') return { head: renewals.head(ATTEMPT) ?? null };
  if (body.op === 'clock') { offsetMs = body.offsetMs; return {}; }
  if (body.op === 'usage') return {
    usage: broker.db.prepare('SELECT identity_json,request_budget,byte_budget,attempt_budget,requests_used,bytes_used,attempts_used FROM source_range_usage').all(),
    grants: broker.db.prepare('SELECT revoked,expires_ms FROM source_range_grants ORDER BY rowid').all() };
  throw Error('unknown control op');
}

const httpServer = http.createServer(async (req, res) => {
  if (req.url === '/__control' && req.method === 'POST') {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    try {
      const result = await control(JSON.parse(Buffer.concat(chunks).toString('utf8')));
      res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify(result));
    } catch (error) {
      res.writeHead(409, { 'content-type': 'application/json' }).end(JSON.stringify({ error: error.code || error.message }));
    }
    return;
  }
  if (!(await handler(req, res))) res.writeHead(404).end();
});
httpServer.listen(0, '127.0.0.1', () => process.stdout.write(`READY ${httpServer.address().port}\n`));
