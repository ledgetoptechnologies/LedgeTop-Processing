'use strict';
// Actual Viewer provider, generated test CA and synthetic metadata only.
const assert = require('node:assert/strict');
const { ReferenceProvider } = require('/viewer/apps/viewer/server/referenceProvider');
const fixture = require('/source/contracts/reference-v1.contract.json');
async function main() {
  const config = { endpoint:'https://127.0.0.1:9443', enabled:true };
  const provider = new ReferenceProvider({...config, token:'synthetic-reference-staging-token', environment:'staging'});
  const capabilities = await provider.capabilities();
  assert.deepEqual(capabilities, fixture.capabilities);
  const job = await provider.submit(fixture.submission);
  assert.equal(job.phase, 'queued');
  assert.equal(job.generation, 1);
  const binding = {jobId:job.jobId, generation:job.generation, environment:'staging',
    manifestSha256:job.manifestSha256, pipelineDigest:job.pipelineDigest};
  assert.deepEqual(await provider.status(job.jobId,binding),job);
  assert.deepEqual(await provider.lookupSubmission(fixture.submission.submissionId,binding),job);
  assert.deepEqual(await provider.submit(fixture.submission),job);
  const production = new ReferenceProvider({...config, token:'synthetic-reference-production-token', environment:'production'});
  await assert.rejects(production.status(job.jobId), error => error.code === 'NOT_FOUND');
  console.log(JSON.stringify({passed:true, transport:'actual TLS with explicit generated test CA',
    actualViewerProvider:true, capabilities:true, submission:true, status:true,
    lostResponseLookup:true, idempotentSubmission:true, environmentIsolation:true,
    engineExecution:false, completePipeline:false}));
}
main().catch(() => { console.error('synthetic Viewer/Python TLS probe failed'); process.exitCode=1; });
