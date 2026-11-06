# Job presentation contract `ltds-job-presentation-v1`

**Status: unqualified.** This wire is not qualified until root and the Viewer
owner have reviewed this final DTO and cross-language fixtures, and real NodeODM
output timing has passed as an installed check. Until then, the scan bounds and the
blank-line rule are provisional. The Viewer owner must also agree the wording below
before release.

This is display only. Internal phase, claim, gate, custody, budgets and recovery
are unchanged. No witness, retry or projection lifts a hold or changes a budget.

Code: `processing/reference_presentation.py`.

## What a witness means

A start witness means **engine command output was observed** for this attempt:
native status 20, then non-blank task output, then status 20 again. On a fresh task,
non-blank output normally comes from the child's stdout or stderr, and init logging
does not append to task output.

It does not prove that ODM initialized successfully, because the output is launcher
or shell output (`run.sh`, `shell:true`). It is not a heartbeat, a job status, an
engine incarnation or a maintenance signal, and it does not show current or
uninterrupted CPU work.

Known exceptions, from the NodeODM source:

- Timeout text is appended after `dateStarted` (`TaskManager:325-327`), so it can
  appear with no child output.
- `config.test` branches generate output without a spawn. `config.test` is false on
  both current native containers, which closes this exception for those containers
  only. Any other container must be checked.

**Activity is historical, and current activity is unknown.** There is no
`activity` field in v1. Whatever `state: processing` or `processingConfirmedAt`
suggests about activity describes the past. The Viewer must say engine output was
observed, and must not imply work is happening now. Dead-worker and maintenance
visibility remain a separate monitoring requirement.

## Public projection

`$.presentation` appears on these reads only:

- `GET /ltds/v2/jobs/{jobId}`
- `GET /ltds/v1/jobs/{jobId}`
- `GET /ltds/v1/jobs/by-submission/{submissionId}`

POST replies and every worker reply omit it. The source phase stays at `$.phase`.

| Field | Type | Meaning |
|---|---|---|
| `schemaVersion` | int, `1` | |
| `protocol` | `ltds-job-presentation-v1` | |
| `state` | `queued`, `preparing`, `processing` or `null` | `null` means another state dominates; see below. `processing` means engine command output was observed, not that work is current |
| `processingConfirmedAt` | `YYYY-MM-DDTHH:MM:SS.mmmZ` or `null` | When this coordinator first durably accepted the observation that engine command output existed |

`processingConfirmedAt` is coordinator time. It is neither engine spawn time, nor
worker time, nor proof of successful ODM initialization, and a report delayed by a
coordinator outage makes it late. It may stay populated while `state` is `null`, as
history only.

The object is closed: an extra field makes it malformed. `processing` requires a
time, and `queued` and `preparing` require `null`.

### Precedence, first match wins

| Rank | Condition | `state` | Viewer shows |
|---|---|---|---|
| 1 | Viewer cannot reach the coordinator | n/a | its own disconnected or maintenance state, never a cached `processing` |
| 2 | `status` failed, cancelled or succeeded | `null` | terminal |
| 3 | `status` held | `null` | held |
| 4 | `queueReason` present | `null` | `queueMessage` |
| 5 | `error` non-null | `null` | error |
| 6 | `phase` waiting_for_delivery, awaiting_acceptance or completed | `null` | delivery |
| 7 | otherwise | as derived below | state plus `$.phase` |

### Derivation at rank 7

| `status` | `phase` | Witness | `state` | Viewer detail |
|---|---|---|---|---|
| queued | queued, waiting_capacity | none | `queued` | waiting for node |
| running | queued, waiting_capacity | none | `preparing` | reservation |
| running | awaiting_transfer, fetching | none | `preparing` | transfer |
| running | processing | none | `preparing` | engine preparation |
| running | processing | yes | `processing` | engine output observed |

Any other combination is `null`.

### Omitted, unknown or malformed projection

On Reference V2 the Viewer must treat an absent `$.presentation`, or an unknown or
malformed `protocol`, `schemaVersion`, `state` or time, like this:

- Do not fall back to inferring processing from `phase` or logs.
- Never infer `queued` from missing data.
- Keep the attempt active and visible with an honest preparing or
  status-unavailable detail, or keep a previously validated latch for the same
  attempt and generation.
- Terminal, held and existing concrete failure or control states still dominate.

Native and legacy provider behaviour is separate and unchanged. The coordinator
omits the field when a stored latch fails validation, because a damaged latch
proves nothing either way.

### Shared fixtures

`tests/fixtures/job-presentation-v1.json` covers precedence, malformed,
attempt/generation and disconnected cases, and is pinned to LF so its SHA-256 is
stable. Coordinator cases are checked against the real projection.
`tests/test_job_presentation_fixtures.py` holds a short reference reader for the rule
above. `display` in the fixtures is a proposed label, not a wire field.

## Worker to coordinator

`POST /ltds/worker/v1/jobs/{jobId}/start-witness`, with `Authorization: Bearer
<worker>`, `X-Claim-Token` and `Content-Type: application/json`. Protected
transport is required. The body is closed and at most 2 KiB.

| Field | Type and bound |
|---|---|
| `schemaVersion` | int, `1` |
| `protocol` | `ltds-worker-start-witness-v1` |
| `witnessId` | UUID, minted once at latch |
| `sequence` | int, exactly `1` |
| `coordinatorAuthorityId`, `claimRequestId` | slug |
| `jobId`, `executionUuid` | UUID, equal to each other and to the path |
| `generation`, `gateGeneration` | positive int |
| `environment` | `production` or `staging` |
| `inputManifestSha256`, `pipelineDigest`, `claimTokenSha256` | SHA-256 hex |
| `gateReceiptId` | 32 lowercase hex |
| `nativeStatusCode` | int, exactly `20` |
| `outputLineCount` | int, 1 to 8, lines seen from the first non-blank line, capped at 8 |
| `outputPrefixSha256` | SHA-256 hex of the first non-blank line; text is never sent |
| `observedAt` | `YYYY-MM-DDTHH:MM:SS.mmmZ`, worker clock |
| `witnessSha256` | digest of all fields above; the coordinator recomputes it |

Receipt, closed, protocol `ltds-worker-start-witness-ack-v1`: `schemaVersion`,
`protocol`, `jobId`, `generation`, `witnessId`, `sequence`, `requestSha256`,
`witnessSha256`, `workerObservedAt`, `acceptedAt`, `ackSha256`. It is built only
from the stored record, so every replay returns the same bytes.

| Case | Response |
|---|---|
| Same `requestSha256`, any status including terminal | 200, stored receipt, no write |
| Record exists with another digest or `witnessId` | 409, first witness wins |
| Row phase before `processing` | 409 |
| Claim or binding mismatch, non-ODM job, stored row unreadable, quota reached | 409 `worker_conflict` |
| No claim header | 401 `claim_authentication_required` |
| Malformed body | 400 `invalid_request` |
| Freeze | 503 `coordinator_maintenance` |

A first report on a held or terminal job at or past `processing` is recorded. It
changes no status and lifts no hold. Ownership is checked before replay.

The two clocks are never compared, so no report is refused for being late or for
worker clock skew. `observedAt` is kept as `workerObservedAt`, an attributed
worker-clock observation, and is never public. Exact replay returns both original
values.

## Worker observation

`ReferenceEngineWorker.start_witness` runs under `NodeGate` and requires all of:

- the gate binding matches, `receiptId` equals the execution row, `completionDigest`
  is null and the bound job is this job;
- the row phase is `commit_pending` or `processing`;
- `/task/{uuid}/info` names the exact UUID with integer code 20;
- a bounded forward scan of the output finds a non-blank line (see below);
- a second `/info` still reports 20;
- the engine identity matches before and after.

`processingTime`, `engine_started` and the row phase never stand in for these.

### Output scan

NodeODM appends blank strings for whitespace chunks, and its output route ignores
`limit` and returns the whole suffix from `line`. So the worker does not sample line
0. It reads pages of at most 200 lines, at most 2 pages per observation, from a
cursor private to the worker. The cursor is never sent, and the DTO is unchanged.

- **Bytes:** each read stops after the page plus one lookahead line, and refuses a
  prefix over 256 KiB, so a long response is never read whole. The engine still
  builds the whole suffix for each request, which this cannot bound.
- **Blanks:** a run of blanks of any length is crossed a bounded step at a time, so it
  cannot hide a later line. State stays one integer however long the run is.
- **All blanks:** the scan reaches the end, restarts from line 0 and never witnesses.
- **Shrinking output:** an empty page past line 0 means the end or output that shrank.
  The two look alike, so the cursor resets to 0 in the same observation.
- **Worker restart:** the cursor is in memory and is lost, so the scan starts again
  from line 0 within the same bounds. A saved latch is unaffected.
- **Status not 20:** no output is read and the cursor does not move.
- **Second status read fails:** the cursor stays on the line found.
- **One line over 256 KiB:** that read fails each time, so the scan stops there and the
  job shows `preparing`. It fails towards no witness and never stops polling.

The driver saves `data['startWitness']` with the exact request before any POST,
then sends it once per step, including on held rows. Failures never stop polling or
raise a hold. `DriverWait` retries next step. A 400 to 404, or a 409, stops the
sending and keeps the latch. The latch feeds no proof, recovery, hold or budget.

## Storage

One `reference_outbox` row per job: identity `presentation-start:{jobId}`, kind
`presentation_start`, `acknowledged=1`, payload at most 1 KiB, never sent, no schema
change. Payload fields: `witnessId`, `sequence`, `requestSha256`, `witnessSha256`,
`executionUuid`, `generation`, `claimRequestId`, `gateGeneration`, `gateReceiptId`,
`workerObservedAt`, `acceptedAt`.

The row counts against the metadata quota and is kept for the life of the job. The
predecessor census, candidate projection and update volume census all refuse a
database that holds this row. A real row always has a claim row beside it, and each
of those checks already refuses that.

## Known limits

- **Restored output:** status 20 with output restored after a NodeODM restart cannot
  be rejected, because the worker cannot see an engine restart, so the output may be
  earlier output for this task. The live check decides whether this matters.
- **Gate evidence is worker-attested.** The coordinator has no gate data, so it
  checks digest consistency, not the receipt.
- **A dead worker leaves `processing` displayed** until something else changes. Only
  `node_lost` and `engine_task_missing` reach the DTO.
- **TreeDar and combined jobs never witness.** They stay `preparing`, or `null` once
  dominated.
- **Delivery has no state.** A job finishing between polls never reaches `processing`.
- **Rows past the driver's active stages do not flush.** A latch not yet acknowledged
  when its row reaches `awaiting_acceptance` stays unreported. Delivery dominates
  there, but `processingConfirmedAt` stays null.
- **The native facade** `/task/{uuid}/info` is outside v1.
- **Rollout order:** deploy coordinator, then workers. A new worker on an old
  coordinator gets a 404, stops sending for that job and keeps its latch. An old
  worker on a new coordinator shows `preparing` for the whole run.
- `preparing` cannot split init, upload, commit and native 10.

## Installed acceptance still owed

On real NodeODM, record `/info` and `/output` from commit through child start and
after a NodeODM restart: first output relative to 10 to 20, whether any non-blank line
precedes the child, whether output is restored, and how long a blank run can be.
