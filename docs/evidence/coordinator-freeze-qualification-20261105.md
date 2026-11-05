# Coordinator freeze contract checkpoint

Base: `18e37ac`. This is an additive Queue-authority primitive and HTTP/lifecycle
qualification. Current-V2 updater profile/census/backup/rollback integration is
still in progress; no deployment or installed upgrade is claimed here.

## Linearization and ownership

The operator endpoint is `/v1/prototype/coordinator-freeze`, protected by the
existing separate operator credential. GET returns authority/startup/plan,
revision, phase and outstanding operation count. POST has exactly
`requestId`, `planId`, `expectedRevision`, `action`, `startupId`. UUIDs are
canonical; owner credentials are stored only as a fingerprint. Each exact
transition request/receipt is retained, and a conflicting replay is rejected.

Transitions are `open -> intent -> draining -> frozen -> open`, requested as
`intent`, `drain`, `seal`, `resume`. A new plan cannot reuse a previous plan ID.
The original healthy startup can explicitly `abort` its own intent/draining
plan using the same owner/plan/revision. It reopens admission without retiring
an active stream or source pin. It cannot abort across restart or unknown hold,
and does not cancel any node plan; the updater must cancel only its exact owned
node drains separately. Intent accepts an already-draining node only when its
durable owned schedule request ID is UUIDv5(freezePlanId,
`coordinator-freeze:` + nodeId), and its canonical request/receipt and current
node plan/revision match exactly. Foreign drains and updates are refused.
Intent and operation admission serialize through the existing Queue lifecycle
mutex and SQLite transaction. A pre-intent operation owns an actual same-thread
opaque token and durable pin; nested synchronous receipt closure inherits that
token. New operations fail retryably once intent commits. Seal requires zero
active/uncertain coordinator operation pins and source reader pins. Node update
ownership other than active/draining must resolve before intent. Resume uses
the same plan/owner and CAS and requires the actual server health predicate;
the update controller must separately bind its candidate identity and census.

No lifecycle mutex remains held during a request body, network operation or NAS
operation. It covers short admission/DB/retirement transitions only. HTTP scope
spans the whole body and response stream. The reference and native wrappers
reject before reading a large body with `503 coordinator_maintenance` and
`Retry-After: 2`; they do not acknowledge acceptance. Producers retain pending
outboxes. Closed pure status paths remain readable. Other GETs, including source
and result streams, require operation admission. The Queue DB authorizer blocks
new ordinary mutations while closed, including background writers that use
Queue._db. Existing ViewerDispatcher loop uses the same operation scope.

Source release/Viewer transfer loop and source-pin lifetime integration are
owned by the source-release branch and must be merged/qualified together. The
typed gate contexts for actual SourceReadHandle retirement and actual
SourceReaders shutdown permit only their pin state/epoch state updates, so a
frozen coordinator can finish a verified handle close and graceful shutdown.
They accept no caller-selected bypass flag or reference string.

## Process death and exact original lease

The journal registers each Queue startup against authority plus exact original
database, root and main coordinator-lock paths/device/inodes, private numeric
ownership and the actual open main-lock descriptor. The Queue must first
acquire that original OS lease. A recognized old active epoch can then retire
only its well-formed closed-kind active pins, retaining their rows and epoch
retirement link. Unknown epochs, changed bindings, malformed states or copied
files remain held. No PID timeout or copied lock proves original ownership.
Frozen maintenance remains frozen across restart; unsealed prior-owner plans
remain held. Per-job source/worker journals retain their separate uncertain
effects and recovery rules; operation retirement does not acknowledge those.

Queue.close refuses live/uncertain operation pins, closes real source reader
ownership, seals its exact epoch and only then releases the main lease. Real
runtime startup has a typed, closed-table metadata scope for its known additive
schema/recovery overlay and source-reader epoch startup. Ordinary job bodies,
receipt payloads and new work cannot use this scope. Frozen startup skips source
spool reconciliation. New source-release tables are deliberately absent from
that reviewed startup table set until their separate review is integrated.

## Bounded control reserve

Active freeze state/plan receipts, active epochs and operation pins have an
independent 256 KiB control ceiling, with 512 bytes reserved for state closure/
hold and at most 128 active pins. Completed plan receipts and closed/retired
epochs/pins remain charged to the ordinary total metadata budget; retained
history cannot consume active emergency capacity. Combined measured metadata
plus reserved future control cannot exceed the ordinary metadata ceiling plus
256 KiB. A new plan has a bounded emergency path even at a full ordinary payload
budget: intent reserves 8 KiB for all future finish/reopen records and known
startup metadata, with each finish step bounded at 2 KiB. Unknown outcomes keep
their reservation; exact replay allocates nothing. A verified frozen runtime's
metadata startup may spend that reservation, while payload mutation stays
denied and rollback restores the spent amount. Total headroom exhaustion holds
new plans without discarding history or blocking existing ACK/status replay.
A 120-cycle native regression retains 480 receipts and more than 256 KiB
of history while active control uses less than 4 KiB and HTTP admission works.
Operation kinds are a closed constant set; paths, raw
query strings, headers and tokens are never persisted as operation metadata.
All actual control bytes still appear in ReferenceStore's measured total.
Payload allocation remains subject to the ordinary quota and cannot borrow
control capacity. Exact stored transition ACK/read recovery and sealing remain
possible when the payload quota is full. Control exhaustion is a bounded hold,
not permission to discard history. Census/physical headroom must account for
both the ordinary metadata budget and this reserved control capacity.

## Evidence and remaining boundaries

Bounded Linux, one CPU/1 GiB, read-only source, no network:

```text
python -m unittest tests.test_coordinator_freeze tests.test_reference_runtime tests.test_node_inventory tests.test_node_roles
Ran 74 tests in 2.171s — OK
```

Image: `sha256:57f01267c88439d872f6782b54511e314614a76145e32cfe2fad003e5ca0fac8`.
Owned test container `ltds-coordinator-freeze-runtime` auto-removed.
The final quota/abort/owned-drain followup passed **80/80 tests in 3.917s** with
the same bounded command. It additionally checks full ordinary quota through
new intent/drain/seal/resume cycles until total headroom holds the next plan,
typed startup reservation rollback, and authorizer-denial typing without the
Python 3.11 exception attributes (a simulated legacy error object, not a full
Python 3.10 interpreter qualification).

Evidence includes actual local HTTP rejection before body read, pre-intent
nested closure and seal waiting, direct background DB refusal/pure reads,
owner/CAS/exact replay/health, full payload quota, real frozen Runtime restart
with source reconciliation forbidden, and a real child HTTP process hard exit.
The crash case proves a copied-state process remains held with the pin intact
and leaves the original unchanged; the actual original lease restart retires
that registered pin and keeps the journal. Child processes are waited/reaped.

Still required: coupled source-effect crash tests with the source-release
branch, actual Viewer outbox 503 recovery, active response-stream drain tests,
exact startup/close census transformations, reviewed installed 693cb7e profile,
pure no-NAS preflight, and bounded copy/backup/rollback fault qualification.
This checkpoint does not authorize a live coordinator replacement.

## Prior candidate comparison

Read-only review covered `f1acf77`/`4aff372` AdmissionDrain, `9c46659` HTTP drain,
`98b41ea` joined actual-runtime evidence and `e5bb4f7` enrolled-updater hold.
Their authority/generation/exact-replay, reserved reopen capacity and adversarial
stream/shutdown test patterns are relevant here. Current CoordinatorService
already contains the mature stop-both-listeners and actual dispatcher/runtime
join behavior, so this patch does not add another shutdown controller.

Direct merging is unsuitable: AdmissionDrain retains one in-memory RLock across
admitted file/network work and has no current-V2 registered I/O/background DB
fence; the HTTP-only gate is explicitly disabled and in-memory. Joined actual
runtime evidence is real TLS/proxy/process restart coverage, but schema V1,
direct fixture admission, acceptance-before-ingest and cleanup disabled.
Guarded native dispatch checkpoints use a separate older NativeEngine transport
and explicitly do not implement automatic resume/old-writer exclusion. The
enrolled updater explicitly requires a new reviewed checkpoint ABI rather than
mount/flag additions. These older guarantees must not be relabeled as current
V2 qualification.

The new gate refuses existing `coordinator_admission`,
`coordinator_admission_operations` or `legacy_fence_requirement` table markers
until an explicit reviewed conversion exists. It cannot silently run alongside
or bypass those older barriers. A native test confirms that refusal and safe
lease cleanup. The first installed profile under consideration, revision
693cb7e, remains separately pinned and is not inferred from these old branches.
