'use strict';
// Synthetic native-path matrix driver: actual, unmodified Viewer modules from a
// pinned read-only tree (VIEWER_SOURCE_ROOT) against the real Processing native
// facade on loopback. Writes only under MATRIX_ROOT. Invented bytes only.
//
// Simulated time: Viewer schedules retries/polls through processing_jobs.
// available_at. The driver advances that column to "now" between steps instead
// of sleeping; backoff arithmetic and attempt counting are left untouched.
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const http = require('node:http');
const path = require('node:path');

const source = path.join(process.env.VIEWER_SOURCE_ROOT, 'apps', 'viewer', 'server');
const { openDatabase } = require(path.join(source, 'database'));
const { ProcessingRepository } = require(path.join(source, 'processingRepository'));
const { ViewerRepository } = require(path.join(source, 'repository'));
const { StorageManager } = require(path.join(source, 'storageManager'));
const { NodeOdmProvider } = require(path.join(source, 'nodeOdmProvider'));
const worker = require(path.join(source, 'processingWorker'));
const { processingJobCounts } = require(path.join(source, 'processingOverview'));

const root = process.env.MATRIX_ROOT;
const signal = (name) => fs.writeFileSync(path.join(root, name), '1');
const waitFor = async (name, ms = 60000) => {
  const deadline = Date.now() + ms;
  while (!fs.existsSync(path.join(root, name))) {
    if (Date.now() > deadline) throw new Error('timeout waiting for ' + name);
    await new Promise((r) => setTimeout(r, 25));
  }
};

// A loopback relay whose outage refuses connections, modelling an unreachable
// coordinator while the real coordinator (and its engine job) keeps running.
function relay(target) {
  const upstream = new URL(target);
  let up = true;
  const server = http.createServer((req, res) => {
    if (!up) { req.socket.destroy(); return; }
    const forward = http.request({ host: upstream.hostname, port: upstream.port, method: req.method, path: req.url,
      headers: req.headers }, (reply) => { res.writeHead(reply.statusCode, reply.headers); reply.pipe(res); });
    forward.on('error', () => req.socket.destroy());
    req.pipe(forward);
  });
  return new Promise((resolve) => server.listen(0, '127.0.0.1', () => resolve({
    origin: `http://127.0.0.1:${server.address().port}`,
    set(state) { up = state; },
    close: () => new Promise((r) => { server.closeAllConnections?.(); server.close(r); }),
  })));
}

function viewer(endpoint) {
  const state = path.join(root, 'viewer');
  for (const name of ['datasets', 'models', 'cache', 'trash', 'imports']) fs.mkdirSync(path.join(state, name), { recursive: true });
  const db = openDatabase(path.join(state, 'viewer.sqlite'));
  const processing = new ProcessingRepository(db), repository = new ViewerRepository(db);
  const storage = new StorageManager({ datasetsMount: path.join(state, 'datasets'), modelsMount: path.join(state, 'models'),
    cacheMount: path.join(state, 'cache'), trashMount: path.join(state, 'trash'), datasetImportMount: path.join(state, 'imports'),
    storageReserveBytes: 0, storageReservePercent: 0 });
  storage.initialize();
  const provider = processing.upsertProvider({ type: 'nodeodm', displayName: 'Synthetic coordinator', endpoint, enabled: true });
  const tokens = new Map([[provider.id, process.env.MATRIX_TOKEN]]);
  const deps = { processing, repository, storage, providerCredentials: { resolve: (id) => tokens.get(id) },
    config: { processingProviderTransferTimeoutMs: 15000, meshDerivativesEnabled: false, opsBaseUrl: 'http://127.0.0.1:9' } };
  return { db, processing, storage, provider, deps };
}

function submitAttempt(v, name, count) {
  const project = v.processing.createProject({ displayName: 'Matrix ' + name });
  const relative = crypto.randomUUID();
  const directory = path.join(root, 'viewer', 'datasets', relative);
  fs.mkdirSync(directory, { recursive: true });
  const files = Array.from({ length: count }, (_, i) => {
    const body = Buffer.from(`SYNTHETIC TRANSFER ONLY ${name} ${i}`);
    fs.writeFileSync(path.join(directory, `image-${i}.jpg`), body);
    return { relativePath: `image-${i}.jpg`, byteSize: body.length, sha256: crypto.createHash('sha256').update(body).digest('hex') };
  });
  const dataset = v.processing.createDataset({ projectId: project.id, displayName: name, storageMode: 'managed', rootKey: 'datasets', relativePath: relative });
  v.processing.finalizeDataset(dataset.id, files, crypto.createHash('sha256').update(JSON.stringify(files)).digest('hex'));
  const task = v.processing.createTask({ projectId: project.id, datasetId: dataset.id, displayName: name });
  return v.processing.createAttempt({ taskId: task.id, providerId: v.provider.id, options: {} });
}

const advanceClock = (v) => v.db.prepare("UPDATE processing_jobs SET available_at='2000-01-01T00:00:00.000Z' WHERE status='pending'").run();
const expireLeases = (v) => v.db.prepare("UPDATE processing_jobs SET lease_expires_at='2000-01-01T00:00:00.000Z' WHERE status='leased'").run();
const warnings = (v) => Number(processingJobCounts(v.db).observationWarnings ?? NaN);
const pendingIngest = (v, attemptId) => v.db.prepare("SELECT COUNT(*) AS n FROM processing_jobs WHERE attempt_id=? AND job_type='ingest' AND status IN ('pending','leased')").get(attemptId).n;
const jobs = (v, attemptId) => v.db.prepare('SELECT job_type,status,attempt_count,error_code FROM processing_jobs WHERE attempt_id=? ORDER BY created_at,rowid').all(attemptId);

// One worker step at a time; returns the attempt after the step.
async function step(v, attemptId) {
  advanceClock(v);
  await worker.processOne(v.deps, 'matrix-worker');
  return v.processing.getAttempt(attemptId);
}

async function untilStatus(v, attemptId, wanted, ms = 90000) {
  const seen = [], deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    const attempt = await step(v, attemptId);
    if (seen[seen.length - 1] !== attempt.status) seen.push(attempt.status);
    if (wanted.includes(attempt.status)) return { attempt, seen };
    await new Promise((r) => setTimeout(r, 20));
  }
  throw new Error(`attempt did not reach ${wanted.join('|')}; saw ${seen.join('>')}`);
}

const cases = {
  // T1 + T12: submit, poll, download/ingest once; outputs retained upstream.
  async success(v) {
    const attempt = submitAttempt(v, 'success', 20);
    // 'derivatives' means ingest committed the model version and handed off to
    // Viewer's separate derivative worker (EPT/mesh conversion), not run here.
    const done = ['derivatives', 'ready_for_review', 'published', 'failed'];
    const reached = await untilStatus(v, attempt.id, [...done, 'ingesting']);
    let final = reached.attempt, seen = reached.seen;
    if (final.status === 'ingesting') {
      const more = await untilStatus(v, attempt.id, done);
      final = more.attempt; seen = seen.concat(more.seen.slice(1));
    }
    const versions = v.db.prepare('SELECT COUNT(*) AS n FROM model_versions WHERE provider_version_id=?').get(attempt.id).n;
    return { attemptId: attempt.id, providerTaskId: attempt.providerTaskId, statuses: seen, finalStatus: final.status,
      errorCode: final.errorCode || null, errorMessage: final.errorMessage || null, modelVersions: versions,
      jobs: jobs(v, attempt.id).map((j) => `${j.job_type}:${j.status}:${j.attempt_count}:${j.error_code || ''}`),
      lastLogs: v.processing.listLogs(attempt.id, 8).map((l) => `${l.level}: ${l.message}`.slice(0, 400)) };
  },

  // T7: submit commit acknowledgement lost, lease expires, worker resumes.
  async replay(v) {
    const attempt = submitAttempt(v, 'replay', 3);
    const original = v.deps;
    let lost = 0;
    const adapterFactory = (provider, config, credentials) => {
      const real = worker.adapterFor(provider, config, credentials);
      return new Proxy(real, { get(target, key) {
        if (key === 'commit') return async (...args) => { const out = await target.commit(...args); if (!lost++) throw Object.assign(new Error('synthetic lost commit response'), { code: 'ECONNRESET' }); return out; };
        const value = target[key]; return typeof value === 'function' ? value.bind(target) : value;
      } });
    };
    advanceClock(v);
    const job = v.processing.claimJob('matrix-crashing-worker');
    await assert.rejects(worker.processSubmit(job, { ...original, adapterFactory, signal: undefined }));
    expireLeases(v);
    const reached = await untilStatus(v, attempt.id, ['queued_upstream', 'running', 'ingesting', 'ready_for_review', 'failed']);
    const settled = await untilStatus(v, attempt.id, ['ingesting', 'derivatives', 'ready_for_review', 'failed']);
    return { attemptId: attempt.id, providerTaskId: attempt.providerTaskId, lostCommitResponses: lost, statusAfterResume: reached.attempt.status,
      statusAfterUpstreamCompletion: settled.attempt.status,
      errorCode: reached.attempt.errorCode || null, jobs: jobs(v, attempt.id) };
  },

  // T9/T10: coordinator unreachable for OUTAGE_FAILURES reconcile attempts.
  async outage(v, link) {
    const failures = Number(process.env.OUTAGE_FAILURES);
    const attempt = submitAttempt(v, 'outage', 3);
    const started = await untilStatus(v, attempt.id, ['queued_upstream', 'running']);
    link.set(false);
    const during = [];
    for (let i = 0; i < failures; i++) {
      const a = await step(v, attempt.id);
      during.push({ status: a.status, jobs: jobs(v, attempt.id).filter((j) => j.job_type === 'reconcile').slice(-1)[0],
        observationWarnings: warnings(v) });
      if (a.status === 'failed') break;
    }
    link.set(true);
    const afterOutage = v.processing.getAttempt(attempt.id).status;
    let final = afterOutage;
    if (afterOutage !== 'failed') final = (await untilStatus(v, attempt.id, ['ingesting', 'derivatives', 'ready_for_review', 'failed'])).attempt.status;
    signal('outage-finished');
    return { attemptId: attempt.id, providerTaskId: attempt.providerTaskId, startedAs: started.attempt.status, outageSteps: during,
      statusAfterOutage: afterOutage, finalStatus: final, errorCode: v.processing.getAttempt(attempt.id).errorCode || null,
      observationWarningsAfterRecovery: warnings(v) };
  },

  // T13: the provider reports completion but enqueueing the ingest job fails
  // once (a temporary trigger in this temp DB only). Records whether the
  // attempt is left 'ingesting' with no ingest job, then lets it recover.
  async atomic_ingest(v) {
    const attempt = submitAttempt(v, 'atomic', 2);
    await untilStatus(v, attempt.id, ['running']);
    const provider = new NodeOdmProvider({ endpoint: v.provider.endpoint, token: process.env.MATRIX_TOKEN });
    const deadline = Date.now() + 60000;
    while ((await provider.status(attempt.providerTaskId)).status !== 'completed') {
      if (Date.now() > deadline) throw new Error('upstream did not complete');
      await new Promise((r) => setTimeout(r, 50));
    }
    v.db.exec("CREATE TEMP TRIGGER matrix_fail_ingest_enqueue BEFORE INSERT ON processing_jobs WHEN NEW.job_type='ingest' BEGIN SELECT RAISE(ABORT,'matrix injected ingest enqueue failure'); END");
    const injected = await step(v, attempt.id);
    const afterInjection = { status: injected.status, pendingIngestJobs: pendingIngest(v, attempt.id),
      reconcile: jobs(v, attempt.id).filter((j) => j.job_type === 'reconcile').slice(-1)[0], observationWarnings: warnings(v) };
    v.db.exec('DROP TRIGGER matrix_fail_ingest_enqueue');
    const recovered = await untilStatus(v, attempt.id, ['derivatives', 'ready_for_review', 'failed']);
    return { attemptId: attempt.id, afterInjection,
      strandedIngesting: afterInjection.status === 'ingesting' && afterInjection.pendingIngestJobs === 0,
      finalStatus: recovered.attempt.status, errorCode: recovered.attempt.errorCode || null,
      modelVersions: v.db.prepare('SELECT COUNT(*) AS n FROM model_versions WHERE provider_version_id=?').get(attempt.id).n };
  },

  // T14: a leftover reconcile job for an attempt that already advanced to
  // 'derivatives' (enqueued through Viewer's own repository API). Records how
  // the worker settles it and whether it keeps retrying.
  async stale_reconcile(v) {
    const attempt = submitAttempt(v, 'stale', 2);
    const done = await untilStatus(v, attempt.id, ['derivatives', 'ready_for_review', 'failed']);
    const staleJob = v.processing.enqueueJob(attempt.id, 'reconcile');
    const observed = [];
    for (let i = 0; i < 4; i++) {
      advanceClock(v); expireLeases(v);
      await worker.processOne(v.deps, 'matrix-worker');
      const row = v.db.prepare('SELECT status,attempt_count,error_code FROM processing_jobs WHERE id=?').get(staleJob);
      observed.push(row);
      if (!['pending', 'leased'].includes(row.status)) break;
    }
    return { attemptId: attempt.id, statusBefore: done.attempt.status, staleJob: observed,
      attemptStatusAfter: v.processing.getAttempt(attempt.id).status, observationWarnings: warnings(v) };
  },

  // T5: running cancellation through the provider call the Viewer route makes.
  async cancel(v) {
    const attempt = submitAttempt(v, 'cancel', 2);
    await untilStatus(v, attempt.id, ['running', 'queued_upstream']);
    const provider = new NodeOdmProvider({ endpoint: v.provider.endpoint, token: process.env.MATRIX_TOKEN });
    v.processing.cancelAttempt(attempt.id, 'matrix');
    await provider.cancel(attempt.providerTaskId);
    await provider.cancel(attempt.providerTaskId);
    const deadline = Date.now() + 30000; let upstream;
    for (;;) { upstream = await provider.status(attempt.providerTaskId); if (['cancelled', 'failed', 'completed'].includes(upstream.status) || Date.now() > deadline) break; await new Promise((r) => setTimeout(r, 50)); }
    return { attemptId: attempt.id, providerTaskId: attempt.providerTaskId, viewerStatus: v.processing.getAttempt(attempt.id).status, upstreamStatus: upstream.status };
  },

  // T2/T3/T4: queued order and routing; Python holds the global slot meanwhile.
  async priority(v) {
    const production = new NodeOdmProvider({ endpoint: v.provider.endpoint, token: process.env.MATRIX_TOKEN });
    const staging = new NodeOdmProvider({ endpoint: v.provider.endpoint, token: process.env.MATRIX_STAGING_TOKEN });
    const submit = async (provider, name, count) => {
      const uuid = crypto.randomUUID();
      await provider.initialize({ uuid, name, options: {} });
      const files = Array.from({ length: count }, (_, i) => ({ relativePath: `image-${i}.jpg`, buffer: Buffer.from(`SYNTHETIC ${name} ${i}`) }));
      for (let s = 0; s < files.length; s += 20) await provider.upload(uuid, files.slice(s, s + 20));
      await provider.commit(uuid);
      return uuid;
    };
    const ids = { staging: await submit(staging, 'staging-small', 3), productionSmall: await submit(production, 'production-small', 3),
      productionLarge: await submit(production, 'production-large', 21) };
    signal('priority-ready');
    for (const [provider, id] of [[production, ids.productionSmall], [production, ids.productionLarge], [staging, ids.staging]]) {
      const deadline = Date.now() + 60000;
      for (;;) { const s = await provider.status(id); if (s.status === 'completed') break; assert(!['failed', 'cancelled'].includes(s.status), JSON.stringify(s)); if (Date.now() > deadline) throw new Error('priority timeout'); await new Promise((r) => setTimeout(r, 50)); }
    }
    return { ids };
  },

  // T6: a draining node gets no new dispatch; Python reopens it.
  async maintenance(v) {
    const provider = new NodeOdmProvider({ endpoint: v.provider.endpoint, token: process.env.MATRIX_TOKEN });
    const uuid = crypto.randomUUID();
    await provider.initialize({ uuid, name: 'drained-small', options: {} });
    await provider.upload(uuid, [{ relativePath: 'image-0.jpg', buffer: Buffer.from('SYNTHETIC drained 0') }]);
    await provider.commit(uuid);
    signal('maintenance-submitted');
    await waitFor('maintenance-reopened');
    const deadline = Date.now() + 60000; let s;
    for (;;) { s = await provider.status(uuid); if (s.status === 'completed' || Date.now() > deadline) break; await new Promise((r) => setTimeout(r, 50)); }
    return { uuid, finalStatus: s.status };
  },

  // T8b: an interrupted download is retried with identical bytes.
  async download(v) {
    const attempt = submitAttempt(v, 'download', 2);
    await untilStatus(v, attempt.id, ['ingesting', 'derivatives', 'ready_for_review', 'failed']);
    const provider = new NodeOdmProvider({ endpoint: v.provider.endpoint, token: process.env.MATRIX_TOKEN });
    const partial = await provider.downloadAll(attempt.providerTaskId);
    const reader = partial.body.getReader(); await reader.read(); await reader.cancel();
    const hash = async () => crypto.createHash('sha256').update(Buffer.from(await (await provider.downloadAll(attempt.providerTaskId)).arrayBuffer())).digest('hex');
    const first = await hash(), second = await hash();
    return { providerTaskId: attempt.providerTaskId, firstSha256: first, secondSha256: second };
  },
};

// Evidence of exactly which Viewer code ran: every CommonJS module loaded from
// the pinned tree, with the sha256 of its bytes on disk. ES modules imported
// lazily by ingest are not in require.cache and are listed separately.
function viewerModules() {
  const tree = path.resolve(process.env.VIEWER_SOURCE_ROOT);
  const digest = (file) => crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');
  const loaded = Object.keys(require.cache).filter((file) => file.startsWith(tree + path.sep)).sort()
    .map((file) => ({ path: path.relative(tree, file).split(path.sep).join('/'), sha256: digest(file) }));
  return { tree, loaded, esmNote: 'ES modules (.mjs) imported during ingest are not enumerated here' };
}

(async () => {
  const name = process.env.MATRIX_CASE;
  const link = await relay(process.env.MATRIX_ENDPOINT);
  const v = viewer(link.origin);
  try {
    const result = await cases[name](v, link);
    console.log(JSON.stringify({ case: name, ok: true, result, viewerModules: viewerModules() }));
  } catch (error) {
    console.log(JSON.stringify({ case: name, ok: false, error: String(error && error.stack || error).slice(0, 2000), viewerModules: viewerModules() }));
    process.exitCode = 1;
  } finally {
    v.db.close();
    await link.close();
  }
})();
