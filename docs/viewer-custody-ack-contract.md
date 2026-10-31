# Viewer custody acknowledgement: proposed contract (not implemented)

This is a design boundary for the disabled synthetic direct-worker path. It
adds no HTTP endpoint, schema, acceptance, dispatch authority, or cleanup.
A successful archive download is not custody acceptance.

## Why the current endpoint cannot safely authenticate an acceptance

`ViewerFacade._environment` in `processing/viewer_http.py` authenticates one
shared bearer per environment; it identifies production versus staging, not
which Viewer installation or principal has accepted custody. Existing task
capabilities are optional for unreserved legacy tasks, and possession alone does
not establish that its holder is the intended durable custody authority. The
staged Viewer `server/ProcessingWorker.js` has no end-to-end acknowledgement
for this direct-worker route. Its `server/referenceResultBridge.js` explicitly
leaves its separate reference acceptance flow unmounted and acknowledgement
disabled by default. Its reference receipt schema and `/jobs/.../acceptance`
protocol cannot be assumed compatible with the NodeODM-compatible task API.

## Required boundary before implementation

* Provision an authenticated, individually identified Viewer principal with
  explicit environment and task authority. Reject shared legacy bearer alone;
  prohibit production/staging principal or key overlap. Bind the identity and
  authorization policy durably to the task incarnation before accepting a
  receipt; authenticate over a protected transport. Credential rotation must
  preserve an auditable mapping without treating an arbitrary new principal as
  the former custodian.
* Viewer must first durably accept and verify the exact result, including its
  own crash-recovery and storage guarantees, and persist an immutable
  acknowledgement intent. The coordinator cannot infer these facts from a
  request, a downloaded byte count, or a self-computed hash. A receipt is an
  authenticated assertion by the trusted Viewer, not independent proof of its
  hardware or filesystem durability.
* Define a canonical versioned request including authenticated principal,
  environment, Viewer task UUID and generation, Processing job and claim/grant
  identity, archive byte count and SHA-256, exact coordinator spool receipt
  digest, and immutable Viewer acceptance identifier. Reject missing,
  contradictory, oversized, unknown-version or noncanonical fields.
* Serialize acceptance with task replacement, cancellation and hold state in a
  single coordinator write transaction. Revalidate the exact task incarnation,
  eligible completion attestation, sealed spool receipt and authorized
  principal under that barrier. Specify whether holds deny new acceptance and
  preserve previously committed receipt lookup; do not use a precheck that can
  race the transaction. Archive verification and slot locking must remain
  coordinated with the transaction without unbounded hashing under the write
  lock. A hold or uncertain state fails closed.
* Persist the full canonical request and deterministic response atomically;
  repeat of the identical authorized request returns the same receipt after a
  restart or lost response. Same acceptance identifier with different content,
  or a different identifier for the same immutable acceptance, must conflict.
  Schema installation, migration and feature settings must fail closed with
  partial tables; legacy scheduling must not bypass these markers.
* No acceptance request or response may delete input, worker results or the
  coordinator spool, finish a job, or trigger cleanup. Those operations require
  a separate reviewed retention policy and explicit authority. Rate-limit
  concurrent archive verification and bound request size and execution.

## Validation required

Use the real protected HTTP/TLS assembly and a real isolated Viewer custody
implementation, not an in-memory stand-in claiming persistence. Exercise wrong
principal, environment, task, generation, digest and spool identity; cancellation
and hold races; concurrent requests; database rollback; process kill after commit
before response; lost-response retry; disk/fsync failures; restart; and retained
node and spool files. Verify idempotent exact replay with the same stored receipt.
All tests must use synthetic data and never activate the production feature.

## Staged Viewer reference protocol compatibility

The staged Viewer reference provider calls `POST
/ltds/v1/jobs/{jobId}/acceptance` and validates an exact response containing
`job`, `created`, `fullyAccepted`, and `acceptedObjectIds`. It requires
`fullyAccepted` to agree with `job.phase === 'completed'`. The request validator
also requires a reference input manifest digest, pipeline digest, result
manifest digest, and accepted object IDs. It is bound to the previously fetched
reference job and result manifest. The direct Viewer task spool currently binds
an archive hash and byte count to its own claim, metadata and spool receipt;
it does not publish that reference result manifest or the required trusted
engine version provenance. The staged reference bridge is explicitly unmounted
and its acknowledgement disabled.

A new credential alone therefore does not make that existing reference route
compatible with the direct path. Fabricating a reference manifest or engine
version, or reporting a completed reference job merely to satisfy the response
validator, would cross the stated trust and lifecycle boundary. A compatible
integration needs an agreed source-backed mapping and a Viewer-side custody
implementation before an authenticated route can be meaningfully tested.

## Staged-byte route is a distinct, narrower option

The staged Viewer also contains `stagedCommitmentClient.js` and
`stagedAckDelivery.js`, and Processing contains a disabled `ViewerStagedAck`.
That protocol is narrower than reference acceptance: it records an authenticated
Viewer assertion about staging exact bytes, with no cleanup or queue completion.
It uses a separately scoped bearer principal, environment and task capability;
the earlier shared-bearer limitation applies to `ViewerFacade`, not to that
already-existing staged-byte handler.

The existing staged-byte handler cannot yet attest a direct-worker spool. Its
commitment and POST require a `succeeded` job and legacy `all.zip.receipt`; the
direct-worker job stays `running` and its spool uses a different sealed receipt.
The runtime configuration also allows staged acknowledgement principals only in
schema 3 and the direct-worker spool only in schema 4. Staged ACK schema
qualification remains explicitly disabled. These checks must not be bypassed.

A safe integration must preserve the exact existing wire fields while holding a
verified spool slot and binding the live task, generation, capability, scoped
principal, environment and spool receipt in a coordinator transaction. Recheck
hold/cancel and claim state under the same write barrier; use bounded hashing
outside it, then retain the cooperative slot lock while revalidating identity.
The stored assertion is the Viewer's claim, not proof of Viewer retention.
A new opt-in runtime configuration needs independent schema/migration and
credential-separation qualification before activation; no completion or cleanup
may follow this assertion. Exercise actual Viewer staged commitment and ACK
clients against an isolated TLS coordinator with lost response/replay,
wrong-authority and task-generation tests and retained upstream copies.

The isolated staged-ACK spool draft adds a held-reader transactional
revalidation and routes the existing handler through it. It remains unassembled:
runtime schema 3 and schema 4 are mutually exclusive, spool settings explicitly
reject staged principals, and both schema qualification flags remain false.
There is no cross-language staged-ACK or crash/replay verification yet. Synthetic
handler replay is not proof of process-crash durability or real Viewer custody.

The disabled combined schema-5 draft accepts a separately scoped staged-byte
principal and provisioned spool together only after the existing schema and
credential preflight succeeds. Runtime qualification flags remain false. Tests
patch a guard only inside isolated fixtures; the combined assembled TLS runtime,
staging-only Viewer clients and process-kill/lost-response replay have not yet
been validated. This is not a production activation or a custody guarantee.

An isolated staging fixture has exercised the actual staged Viewer commitment
and acknowledgement clients against the assembled TLS coordinator, using a
synthetic in-memory Viewer outbox. The client discards a successful POST response
after coordinator commit, marks delivery uncertain, and resolves it through the
existing authenticated lookup. This is a simulated lost response, not a
process-kill test, and the in-memory outbox provides no durable Viewer custody.
The runtime fixture patches qualification flags only for the test. It does not
wire the spool into ViewerService or enable completion projection in deployment.
The staging harness verifies SHA-256 of both Viewer client source files before
starting the client process. This detects changes present at verification time;
a concurrently writable source could still change afterward, so a future
release-grade rehearsal needs an immutable export. Outer fixture cleanup is
registered before setup, but nested legacy fixture setup is not independently
proven to clean every resource if it fails before registering its own cleanup.
The pinned hashes now match the raw Git blobs in staged tree
`baaaf2439d5a641ae59c3f550f77dc7512e3b84d`, not the CRLF working checkout.
The cross-language rehearsal used separate files written from `git cat-file`
blob bytes; the export was mounted read-only in the test container. An ordinary
`git archive` on this checkout applied line-ending attributes and was not
byte-identical, so its extracted copies were not used for the final test. The
host-side export is still writable by its owner; hashing followed by execution
is not an atomic immutable snapshot against concurrent host modification.

An isolated schema-6 draft adds a separately qualified, staging-only runtime
binding for Viewer status and download. The binding publishes the actual
coordinator spool as one scoped unit and is removed on runtime close. The
projection verifies completion evidence before streaming; it does not revoke a
stream already in flight when a hold arrives. The new qualification guard is
false outside tests. Existing production Viewer behavior remains unchanged.
The current fixture creates the synthetic task through TaskStore before testing
NodeODM-compatible status and download; it does not yet test submission through
the Viewer HTTP interface or prove Viewer custody.
The runtime serializes start and close on one lifecycle lock so concurrent
starts cannot create untracked listeners and close cannot miss a starting
listener or leave its projection bound. Thread creation or `thread.start()`
does not prove that a listener remains alive or healthy afterward; independent
health monitoring and recovery remain necessary.

A later isolated rehearsal uses an on-disk synthetic SQLite database with the
actual Viewer migration 46 and 47 SQL, and the actual staged sender. A separate
sender process is killed after the coordinator response arrives but before the
Viewer records it; after the recorded lease expires, a new process looks up
the exact assertion and marks it accepted. The fixture directly inserts its
synthetic outbox rows and does not exercise Viewer ingest or its real outbox
writer. The database resides on container tmpfs, so the test verifies process
restart, not host power loss, Viewer custody, or real storage durability.
The later staging rehearsal calls the actual pinned Viewer
`ProcessingRepository.enqueueDisabledStagedByteAck` method against migrated
outbox/delivery tables, with synthetic staged commitment, output and transfer
provenance rows and a frozen delivery binding. It invokes the repository
prototype directly without its normal constructor and does not run the normal
Viewer ingest transaction or prove its upstream provenance. It logs and checks
exactly one POST followed by one GET during sender restart; the GET response is
validated by the actual Viewer sender. The parent process starts the Node
harness in a separate process group and kills and waits for that group on timeout.
Viewer dependency files besides the four pinned files come from a separate Git
archive export, and its host copy remains writable.
