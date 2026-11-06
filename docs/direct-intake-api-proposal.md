# Proposed direct-intake protocol and recovery contract

**Design only.** No route, receipt, Viewer acknowledgement, automatic fallback or
release described here is implemented by the current direct-intake prototypes.
The existing V2 spool-first API and its stored requests and receipts remain
unchanged. Direct is a separately versioned, capability-negotiated contract;
an internal V3-shaped Viewer envelope does not mean this protocol is enabled.

## Admission and scheduler

1. An authenticated Viewer or PC client requests capabilities. The coordinator
   advertises `directIntake` only when the installed coordinator, worker,
   source-inventory contract and recovery paths support the exact negotiated
   version. A client that does not see it uses the existing qualified route.
2. The client persists an immutable, signed request before its first network
   write. Its binding includes coordinator authority and principal, environment,
   submission/task/attempt IDs, source snapshot and selected path-inventory
   digest, separate source and derived manifest digests, pipeline and engine
   mapping digests, sizes, output budget and capability fingerprint. An object
   is identified by immutable ID, size and hash; original paths remain in the
   independently verifiable source inventory, not as storage paths. GCP-derived
   bytes, raw source CSV/image provenance, CRS and explicit height source must
   bind to the same attempt and engine filename mapping. The current prototype's
   V1-style combined manifest is insufficient for this full contract.
3. A single durable coordinator transaction identifies the current eligible
   queue position and reserves at most one physical node for the attempt. The
   scheduler orders production ahead of waiting staging, FIFO among eligible
   jobs of the same priority, and chooses the first configured eligible idle
   node; a large job must not run on a node above its image limit. An earlier
   eligible queued job cannot be bypassed by a new direct request. Running or
   already transferring work is not preempted. Draining or unavailable nodes
   cannot be selected; node capacity and worker readiness must be freshly
   verified, not inferred from a stale observation.
4. Direct mode is provisional until the specifically pinned worker has durably
   reserved scratch capacity and bound its physical node gate to the exact
   authority, node, worker instance, operation, job and generation. Neither a
   coordinator database slot nor a scratch-capacity observation proves worker
   custody or permits processing. If a node cannot be reserved before any byte
   is accepted, select local spool mode only after atomically reserving the
   full required coordinator disk capacity and free-space floor. If neither
   mode can safely accept the input, return retryable waiting capacity and
   leave all original data at the client. The PC uploader must retain its own
   source until the contract says it is safe; Viewer retains its originals.

## Candidate API shapes

Names below are proposed for review, not installed URLs. Every call uses TLS,
authenticated scoped identities and an independently pinned authority. Limit
request and object sizes, timeouts, concurrent streams and response bodies.
Secrets and credentials are never placed in URLs, receipts or logs.

- `GET /ltds/direct/v1/capabilities`: version, supported inventory/receipt
  protocols, object and byte limits, and current availability. Availability is
  advisory; admission must recheck it atomically.
- `POST /ltds/direct/v1/submissions`: submit the exact immutable signed intent.
  Return an authenticated binding with stable `submissionId`, `jobId`,
  `attemptId`, generation, environment, authority, intent digest and mode
  revision. Same key and identical bytes reconcile to the same job; any changed
  bytes or binding conflict. A lost reply is reconciled with the lookup below,
  never by guessing whether a second job should be created.
- `GET /ltds/direct/v1/submissions/{submissionId}`: authenticate and return
  only that principal/environment's exact binding, mode, phase and object
  acknowledgements. Unknown or unauthorized identities disclose no private
  job details. Persist and compare the intent digest and mode revision.
- `PUT /ltds/direct/v1/jobs/{jobId}/objects/{objectId}`: stream exactly the
  expected object for the bound generation and mode. A direct relay returns a
  per-object worker receipt only after complete hash verification, file and
  namespace durability barriers and durable receipt recording; a spool relay
  returns an equivalent coordinator-spool receipt only after its own barriers.
  An object receipt is not a complete-source acknowledgement. Replays must
  compare exact identity, bytes, digest and receipt; conflicts never replace
  previously accepted bytes. Start with whole-object retry unless a separately
  negotiated, durably verified range protocol exists; never report unverified
  partial offsets as committed.
- `POST /ltds/direct/v1/jobs/{jobId}/source/complete`: require every named
  source and derived object, exact immutable source/derived/path/mapping hashes,
  closed local inventory and verified physical custody. Persist the exact
  authenticated complete-source receipt before sending it. A lost reply is
  resolved by GET and exact receipt comparison; a receipt identifier alone is
  insufficient. Only then may the coordinator advance toward processing.
- `GET /ltds/direct/v1/jobs/{jobId}` and object/receipt lookups: return the
  authenticated bound state, node assignment, manifest and receipt identities,
  current processing logs where authorized, and durable result/acknowledgement
  state. Do not expose a worker private path, bearer or client secret.

The coordinator-to-worker relay uses a separately authenticated private
worker protocol with a closed request binding and a unique durable writer pin.
The worker reports its exact local object or closure receipt to the coordinator;
the coordinator validates worker identity, gate, scratch, manifest, attempt
and signed receipt before persisting and relaying any acknowledgement. A
local scratch or closure observation alone must never be passed off as such a
verified network receipt. Signing and verification keys, key rotation and
version migration require their own reviewed design.

## Status and recovery

Persist mode and phase; do not infer successful work from process liveness:

| State | Viewer presentation | Meaning |
|---|---|---|
| `waiting_capacity` | Queued, waiting for an eligible node or disk space | No bytes acknowledged for the new mode. |
| `receiving_direct` | Receiving on assigned node | Node and scratch reserved; individual verified objects may be acknowledged. |
| `receiving_spool` | Receiving into coordinator queue | Disk budget reserved; objects are being durably stored locally. |
| `waiting_dispatch` | Queued, waiting for an eligible node | Complete spool custody verified; engine has not started. |
| `preparing` | Preparing on assigned node | Source closure verified and worker setup underway; engine has not started. |
| `processing` | Processing | Authenticated worker confirms the engine has actually started. |
| `awaiting_viewer` | Processing complete, delivering results | Result remains retained until Viewer verifies and acknowledges the exact copy. |
| `held` | Needs inspection or reconnection | Identity, storage, transfer, writer retirement or outcome is uncertain. |

If a stream, worker, client or coordinator disconnects, keep immutable intent,
node/scratch reservation, writer pins and verified receipts. Never use time
alone to free an active or uncertain writer or rerun an engine. Reconcile the
same operation/worker/generation and verified object state. A worker that
finishes while the coordinator is offline keeps a durable completion outbox and
retries; the coordinator retains results while Viewer is unavailable. Duplicate
completion and accepted-copy messages return the exact previously committed
receipt only after authentication and identity verification.

A mid-transfer direct-to-spool fallback is a **new durable mode revision**,
not an implicit second upload: first prove the old writer and in-flight request
have retired and fence the old node/attempt, reserve the entire missing spool
capacity, then journal the new destination and replay verified objects from the
retained source. Unknown writer retirement, ambiguous RPC or conflicting data
holds the job for inspection. No mode change may claim a partial direct upload
as spool custody or allow two nodes to process the same attempt.

Keep cleanup disabled for the first installed test. After the complete result
is durably copied and Viewer outputs are verified ready, redundant processing
copies become eligible only after the agreed 24 hours and after all active
readers, uploads, retries and required evidence are closed. Viewer originals,
accepted outputs and raw GCP CSVs are retained. Capacity exhaustion must stop
new admission; disk usage and pending holds remain visible.

## Required qualification before enabling

Agree the exact schema and receipt signature domains with Viewer; implement
the full path-preserving inventory and per-attempt GCP provenance contract.
Integrate direct writer pin accounting/precharge and explicit recovery; current
prototype pins remain inspection-only after uncertain retirement. Prove
per-node physical fencing, freeze/custody migration, disk reservation and
private engine ingress on the actual machines. Test concurrent priority and
node eligibility, interruption and restart at every intent/object/closure/ACK
boundary, duplicate/conflicting replay, mount replacement, full disk, failed
sync, client loss and worker/coordinator/Viewer outages. Verify cross-language
canonical/signature vectors and old V2 compatibility. Then run small real
jobs and verify both node routing and returned Viewer outputs before larger
client datasets. A synthetic receipt or successful local spool test alone is
not installed or end-to-end proof.
