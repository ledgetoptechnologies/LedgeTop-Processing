# Original worker claim recovery

`processing.reference_worker_recovery.OriginalClaimRecovery` is an inactive,
explicit client of the optional worker claim-recovery service. It is not wired
into any daemon/factory, advertised capability, processing node or runtime
update. It does not claim a job, renew a lease, acquire another reservation,
start an engine, create an outbox, release a physical receipt or clean files.

The client currently applies to the original **main-node combined ODM→TreeDar
PipelineJournal**. General server routes do not establish automatic recovery for
standalone ODM-only or TreeDar-only workers.

## Required preparation and bounded control storage

PipelinePolicy has an immutable `recovery_control_budget` (default zero).
`output_budget` adds that component to both unchanged engine-stage maxima.
Zero retains legacy guard behavior and disables this client. An enabled client
requires exact equality with RecoveryPolicy's computed control allowance; it
cannot borrow promised ODM/TreeDar bytes.

Call `prepare(parent_binding)` before any stage intent is issued. It holds a
concrete PipelineJournal operation lease (PID, thread, actual lock descriptor,
inode and current owner), proves both stages are new with no intent/proof, and
uses the narrow preexecution guard. That guard verifies the exact original
Driver input-path closure and bounded byte/SHA reads of every original file,
the whole reserved parent, physical receipt and fresh authenticated processing
claim. Ordinary engine mutation checks require a typed published control charge;
calling stage advance cannot bypass preparation. Parent binding observes the
existing claim so that this order has no circular execution grant.

ScratchLedger atomically commits an exact input-baseline CAS, control allowance,
immutable receipt and full retained capacity hold. That receipt is the initial
durable setup intent: construction creates no client database before it. Replay
compares all immutable arguments and returns history; it never infers a charge
from an allocated counter or increments it again. Admission includes full
retained control holds because precharged metadata may not yet have materialized
in the free-space probe. Keeping the hold after files grow is conservative.
TTL, ordinary terminal settlement, a result ACK or missing files cannot release
it. A future release must qualify no future writers, retention, acceptance and
cleanup; no release API exists here.

Only exclusively created empty files may initialize new client state. Existing
exact committed schema/binding can resume setup; foreign empty files,
partial/corrupt state and missing published files hold. After FULL commit and
directory fsync, a typed publication CAS binds database/lock inode, root and
policy. All earlier published parents in the shared scope must name the same
identity. An unpublished later parent cannot authorize replacing old history.
The ledger retains the full bounded immutable descriptor, not only its hash.
Before normal stage/Native mutation, a concrete read-only validator checks the
current database and lock paths/inodes, private root, exact schema/binding,
page/row/aggregate/sidecar bounds, policy/transport, typed anchor and every prior
scope publication. Missing, replaced or corrupt metadata holds execution without
recreating it. If a stage intent was already issued, it stays issued and held;
no engine mutation is replayed and no job/artifact is dropped.
Read-only attachment and admission use the same exact 4096-byte page-size and
page-count checks as the client, plus current-parent intent/history validation.
An in-place SQLite format change cannot bypass admission by retaining its inode.
Lost charge/publication replies reconcile the exact typed receipt. State and
ScratchLedger must share a filesystem/capacity domain.

Operational defaults are eight attempts per job, 64 globally, 8 KiB per row,
128 KiB total serialized records, 4096-byte pages and 64 database pages. Control
allowance is `4 × maximum database bytes + 64 KiB` (1,114,112 bytes by default),
covering database, rollback coexistence and retained typed publication metadata.
SQL COUNT/SUM/MAX preflight precedes row fetch; rows, mutations, SQLite pages and
rollback sidecars are bounded. Independent limits can hold before the attempt
maximum. Tests provision this additive budget up front. No history is pruned;
at a cap preserve state and hold. Qualified operator export/handoff or storage
policy migration remains future work.
The same default also caps **prepared jobs** at 64 in a scope, even if none ever
needs recovery: their retained control receipts consume the scope census.
This prototype therefore stops accepting preparation after 64 jobs absent a
qualified retirement/rotation lifecycle. Manual weekly rotation is not an
intended production solution. Final-ACK/control-history retirement and no-future-
writer proof remain required work before production readiness; nothing is
silently deleted to bypass these caps.

This changes the private source ABI and policy/schema bindings. Do not reset or
silently reinterpret published state; deployment migration is unfinished. The
paired NativeODM change captures actual allocation before its own writes and
adds archive bytes to that baseline, retaining controls. TreeDar later freezes
that retained baseline. Neither component is deployed by this qualification.
Host allocator accounting now has a monotonic cold-open fence. The first typed
control charge atomically adds `accountingVersion: 1` to the existing strict
`metadata.config`, together with its receipt, counter movement and retained hold.
Ordinary legacy/default-zero activity leaves the original config unchanged.
There is no cached accounting-version attribute: new transactions and read-only
control/guard snapshots check current exact config and stream-qualify every
retained receipt/reservation allowance before trusting its capacity hold.
Unknown versions, missing markers/config, removed records or lowered holds fail
before admission; attempted in-transaction marker removal rolls back. Future
control retirement must preserve a qualified monotonic accounting history and
cannot erase the marker to reopen an old allocator.

The actual pre-control ScratchLedger from commit
`c60834cd156b7ec073ab381c0d9cde4ecee12bd3` is archived byte-for-byte under
`tests/fixtures/scratch_policy_c60834c`, with SHA-pinned provenance and Git
attributes preserving its original bytes. Its unchanged strict config comparison
refuses a coherent charged ledger on cold reopen; regression tests confirm the
entire database bytes/schema/rows remain unchanged. The same old module still
opens an ordinary uncharged legacy ledger. New code cold-reopens charged state
and reconciles an exact first-charge receipt after a lost reply without charging
twice.

This proves ordinary **cold reopen of coherent marked state**, not universal
rollback safety after database corruption or external marker removal. Unmodified
old code cannot recognize a marker it never understood once someone removes it.
Installed owner/supervisor checks, source migration and corruption handling must
still be qualified before deployment; new code refuses those incoherent states.
An already-loaded old allocator cannot be retrofitted by this marker. Actual
DaemonStateLock ownership must exclude mixed old/new daemons, and host allocator
rollback remains a controlled deployment operation. A NodeODM/TreeDar backend
image rollback is separate from rolling back the host ledger allocator.

## Concrete authority and local evidence

Construction requires the concrete ProductionParentGuard and its actual
PipelineJournal, ScratchLedger, DaemonStateLock, DriverClaimTokens and
ClaimStateClient. Generic truth-returning callbacks and configured hashes are
not accepted as authority. The durable private root must already exist and
remain owned by the actual daemon OS lease. The new private rollback SQLite
journal and operation lock pin file/root identity, exact schema, worker policy,
transport policy and creator process. It does not provision directories or
modify existing driver credential state.

Each operation reads the original private driver claim and exact original
transfer request. The transfer digest must match the authenticated coordinator
challenge and every replayed intent. Changed token, generation, manifest,
pipeline, worker instance, authority, physical receipt or transfer holds work.
Fresh read-only ParentGuard or narrow preexecution validation precedes a recovery POST. The client also
takes the real **existing** physical flock and rechecks the original bound
receipt and whole started/uncertain parent (or strictly proved preexecution reservation) while holding it across
the POST. No new gate reservation is acquired. All original inputs and retained
stage outputs remain allocated. ParentGuard itself is called outside that gate
context to avoid nested lease acquisition.

The worker evidence digest records actual local fence/reservation facts and
their original parent binding. This is authenticated worker attestation, not
remote hardware attestation or proof against an administrator controlling the
worker. The coordinator checks its configured authority/physical binding and
exact original claim/transfer. It cannot independently inspect the worker disk.

## Durable retry and reset rules

`recover(parent_binding)` performs one bounded explicit attempt; there is no
background retry loop or scheduled inbox polling. It first reconciles any
outstanding immutable request using authenticated GET
`recovery-receipts/{requestId}`, then reads fresh `recovery-state`.

Before the first POST, SQLite FULL-commits a random stable request UUID, current
coordinator epoch, node maintenance revision, exact body and evidence. The same
request ID is always replayed with exactly the same body. A lost HTTP response
leaves that intent unresolved. A later call reads its receipt first: a matching
receipt is saved locally, while a confirmed missing receipt with an unchanged
eligible epoch/revision permits only the original same-ID replay. Transport,
authentication, malformed responses and old-server 404s hold. There is no claim
or POST fallback for unavailable GET routes.

Missing does **not** generally mean that a POST failed. A racing original POST
can commit between receipt lookup and the fresh state read. If fresh state is
already running, the client issues no competing resume, keeps the unresolved
intent for later historical lookup and evaluates the current ParentGuard only.

A request with no receipt may be retired only when fresh authenticated state is
still eligible `held/coordinator_restart` and either reports a different current
startup epoch or a strictly larger maintenance revision within the same epoch.
The server's epoch/revision CAS then fences the old body from a future successful
mutation. The immutable original intent and explicit eligible retirement fence
remain in the journal, with no invented success receipt. A new immutable request
may then target that newly fenced epoch/revision. An unchanged or regressed
revision, operator hold, maintenance hold or uncertain GET does not reset it.

Receipted attempts from earlier restarts remain historical. They never clear a
later restart hold or operator hold. Every later restart needs a fresh current
challenge and a new request only after the outstanding prior attempt has been
reconciled or definitively fenced as above. No API is provided to delete/reset
an unresolved intent manually. Corrupt state or an unqualified authority/endpoint
migration requires explicit operator review; this client does not silently
discard records or recreate its binding.

## Permission after recovery

Even a successful POST and matching durable receipt do not imply permission to
execute. Before returning `executable: true`, the client invokes the concrete
ParentGuard again with `mutation=True`, forcing a fresh authenticated claim-state
read plus local ownership/fence/reservation checks. A later restart, operator hold
or uncertain local engine state holds instead. Historical receipts cannot replace
this check. The returned value is an observation at that instant, not an engine
action or reusable grant: each engine controller must still perform its own
immediate admission/runtime/fence checks before any mutation.

Restart before the first stage is separate. Normal ParentGuard still refuses
held-restart engine evidence on a merely reserved job. Only the narrow
preexecution method, under the active journal lease with both stages unissued,
full original input hashes and an existing exact charge, can finish local
publication and attest original-claim continuation. It never marks an engine
started. Server resume must still be followed by a fresh normal mutation guard
before stage issue. A held job without a prior charge cannot invent preparation;
already-issued legacy work cannot retrofit it. Reserved expiry and other
issued-but-not-started uncertainty hold for explicit reconciliation.

## Transport and verification

The existing fixed ClaimStateClient provides verified HTTPS hostname/certificate
checks, pinned trust-root digest, fixed worker role/identity and immutable
transport objects. Recovery uses only fixed paths under its origin, disables
proxies and redirects through that client, bounds each request/response and wall
deadline, requires finite duplicate-free JSON and no-store responses, and accepts
only an exact closed receipt for its saved request. Credentials are sent only in
protected headers and never enter new journal records, returned values or error
messages. A trusted factory still needs to qualify HTTPS/credential provisioning;
no plaintext loopback escape or production configuration is installed here.

`tests.test_reference_worker_recovery` uses a temporary synthetic certificate and
the actual TLS worker API, ClaimRecoveryService, Queue/ReferenceStore,
ProductionParentGuard, journal, ledger and OS leases. Faults simulate lost replies,
restart, maintenance revision, race/concurrency and corruption without client
data or live nodes. These tests qualify this source contract, not installed
acceptance or complete processing-pipeline recovery. Engine stage reconciliation,
outer-v2 TreeDar delivery, Viewer acceptance and cleanup remain separate work.
