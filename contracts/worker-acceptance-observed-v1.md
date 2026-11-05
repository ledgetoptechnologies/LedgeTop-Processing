# Worker observation of Viewer accepted copies

Implemented candidate prerequisite only. This acknowledgement does not release
NAS source custody, retire Viewer holds, authorize file deletion or replace the
existing signed-copy scratch cleanup policy.

## Private worker request

`POST /ltds/worker/v1/jobs/{jobId}/acceptance-observed` uses the configured worker
Bearer and existing `X-Claim-Token`. The original assigned node, worker and
instance are checked again inside the journal transaction, including when the
job has already completed. No request supplies an alternate owner or path.

The closed request contains:

- `schemaVersion: 1`, `protocol: ltds-worker-acceptance-observed-v1`;
- canonical UUID `observationId`, persisted by the worker before the first POST;
- `coordinatorAuthorityId`, `jobId`, `generation`, `environment`;
- `inputManifestSha256`, `pipelineDigest`, `resultManifestSha256`;
- `receipts`, sorted by unique `acceptanceReceiptId`, each containing exactly
  `acceptanceReceiptId`, `acceptanceSha256`, `viewerReceiptId` and
  `viewerReceiptSha256`.

Receipt hashes cover the exact canonical Processing acceptance and signed V2
Viewer accepted-copy receipt. Only source-intake V2 jobs qualify. The coordinator
requires its immutable completed result and accepted raw/derived custody, matches
every selected pair to persisted accepted/signed receipt rows and their digests,
and derives full result-object coverage from those selected receipts. A request
boolean or the job's completed phase alone is insufficient. The selected set may
omit other receipt pairs only if it still covers the entire result. V2 copy
ingestion already rejects partial copy proofs; the observation independently
checks coverage rather than trusting damaged or historical journal rows.

Accepted source/derived custody is required for the first observation. An exact
committed observation remains replayable after a later custody hold or release
transition, while preserving all immutable job, owner, result and receipt
bindings. That acknowledgement records a historical fact; it does not make
currently held custody eligible for a new release operation.

The request is bounded by the ordinary 4 MiB metadata envelope and existing
object-count limit. One journal row is permitted per `(jobId, generation)` and
`observationId` is globally unique. Conflicting replay cannot replace a row.
The entire request, assigned identity, claim-token digest and acknowledgement
count against the coordinator's existing metadata quota.

## Stable acknowledgement and Viewer lookup

The closed acknowledgement has `schemaVersion: 1`, protocol
`ltds-worker-acceptance-observed-ack-v1`, `coordinatorAuthorityId`, `jobId`,
`generation`, `observationId`, `requestSha256`, `nodeId`, `workerId`,
`workerInstanceId`, `observedAt` and `ackSha256`. `observedAt` is minted from the
coordinator's aware UTC clock when creating the journal row; no client time is
accepted. Exact replay returns the stored acknowledgement before consulting a
new clock value or consuming more quota. `ackSha256` binds the canonical body
without itself; it is an integrity binding, not a digital signature.

`GET /ltds/v2/jobs/{jobId}/source/acceptance-observation` returns only that
acknowledgement, or 404 while absent. It uses the existing Viewer Bearer/job
environment boundary, returns 404 across environments and introduces no job
listing or new multi-client principal model. It exposes no claim token, source
path or full acceptance/copy receipt. A future release operation must consult
the authoritative journal and separately verify its authenticated release
policy; a client-supplied acknowledgement is not deletion authority.

Read-only lookup also validates the saved canonical request and acknowledgement,
current immutable environment/input/pipeline/result binding, and retained
original claim owner/token digest. A self-consistent stale journal hash is not
sufficient. These checks do not require current source custody to be accepted:
the original acknowledgement remains a historical fact after a real custody hold.

This is an authenticated assertion from the assigned trusted worker, crosschecked
against coordinator records. It is not hardware attestation of remote execution.

## Worker recovery and cleanup boundaries

After locally verifying and registering the signed Viewer receipts, the driver
stores exact canonical observation request bytes in its existing job data. Both
foreground and background acceptance paths require a matching acknowledgement
before local driver completion or native engine-cache removal.

The driver persists attempt count and its next retry time before each POST.
Retries begin at two seconds, double to a maximum of 300 seconds and saturate the
counter at 32. An outage, old coordinator's 404, conflicting/malformed reply or
restart preserves the original request and ID. Both this job's receipt-relay GET
and observation POST are suppressed until due. Other jobs and existing generic
scratch cleanup continue; no global idle requirement or new result pin is added.
The original V1 acceptance path is unchanged. Install the coordinated API before
enabling this driver version, otherwise V2 observations remain pending.

Pending observation replay uses retained outbox/receipt/result-manifest metadata,
including after eligible scratch result bytes were removed by the existing
signed-copy policy. It does not refetch NAS inputs or recreate engine tasks.
Retry clock checks are local scheduling checks, not a global durable-clock or
power-loss guarantee.
