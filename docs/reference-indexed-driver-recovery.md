# Completed-v2 indexed driver rollback recovery

This source candidate adds a narrow startup recovery for an already completed,
physically bound version-2 worker's `driver.sqlite`. It is not a migration,
bootstrap, journal adoption, cleanup authority, or proof that every worker
power-loss case is automatically recoverable. Default and version-1 factories
retain their prior behavior and never invoke this helper. The indexed schema,
compiled descriptor, limits and policy hashes are unchanged.

The factory runs recovery under `DaemonStateLock`, before its ordinary read-only
profile checks, gate/engine contact, ScratchLedger construction, or outbox/worker
construction. Normal `read_only_profile` itself remains read-only and refuses
rollback sidecars. `completed_indexed_recovery_authority` checks the closed
configuration and full external bootstrap/checkpoint/completion/scratch-claim
chain, pinned state/scratch/driver identities and cold scratch ledger without
opening driver SQLite. An incomplete bootstrap or hot scratch ledger still
holds; this helper does not recover other worker databases.

## Copy validation before original writes

Only an owned private single-link bounded driver/rollback pair is eligible.
WAL/SHM, symlinks, hardlinks, foreign or changed roots/checkpoints, malformed
journals and multi-database super-journals hold. A genuine zero-header journal
left by a crash before pager spill also deliberately holds unchanged. No manual
unlink fallback exists. This residual requires separately reviewed recovery;
it must not be described as complete automatic power-loss recovery.

A fixed private `indexed-driver-recovery/` capsule contains one closed state
record and at most one copied database/journal pair. Raw descriptor copies are
made only after original identities, sizes, times and SHA-256 fingerprints are
pinned; the original pair must be unchanged after copying. SQLite itself rolls
back the copy. The copied result must pass bounded integrity, exact schema,
original physical/profile identity, full driver binding and a read-only business
closure census. That census verifies original claims, normalized submission,
quota/seal/header/object closure, mutable claim/reservation projection,
grant-to-reservation/object identity, and cursor/attempt history. Grant closure
uses only its persisted clock high-water to check historical consistency; no
current transfer capability, renewal, expiration reset, or new clock is created.
Normal driver time/TTL checks remain mandatory before future work.

Only a durable `original-started` checkpoint permits normal SQLite rollback on
the original path. Its inode is never replaced. On restart that phase permits
partially restored database pages but requires the recorded journal identity
and fingerprint. A disappeared journal is accepted only when the original
matches the previously validated recovered-copy hash. The recovered original
must pass the same checks and byte digest before completion is recorded.

The phases are `copying`, `validated`, `original-started`, and `complete`.
Atomic checkpoint replacement and directory fsync bound their transitions.
Before the first state rename, a bounded `state.tmp` alone may be overwritten
because no SQLite copy or original recovery has begun. Unknown copy files
without a state record are retained and refused. Partial known copies may be
rebuilt only against identical original fingerprints. Once complete, only the
two fixed, owned copy basenames are removed; original data, source objects,
results and bootstrap receipts are never deleted. The small completed record
remains, and the next eligible journal reuses this one bounded capsule with a
new operation identity. No growing history of recovery copies is created.

## Capacity and stopped census

Before copying, the helper validates the real scratch ledger read-only and sums
all outstanding `reservations.remaining` and all retained `control_charges.bytes`
without expiring rows or observing a new ledger clock. Noninteger/negative
charges and SUM overflow hold. When scratch shares the state device, all those
commitments plus the configured floor remain protected. The existing global
192 MiB metadata allowance is preserved minus already allocated original
DB/journal blocks. An additional conservative 192 MiB copy bound plus 64 KiB
control allowance must fit; allocated private-copy blocks are counted once.
Separate-device scratch holds are not subtracted from unrelated state-disk free
space. Insufficient space holds the original pair unchanged. Space is rechecked
before original rollback and on its restart. No existing indexed policy pin is
silently rewritten to fund recovery.

`inspect_completed_recovery_capsule` is a read-only maintenance seam accepting
an actual mapped host root and explicit original bootstrap/binding/root/driver
identities. It requires a closed COMPLETE record with fully typed fingerprints
and no remaining copy files, and returns only a bounded summary/hash. It grants
no recovery, job acceptance, current-journal adoption or deletion authority.
Maintenance must separately verify its mount mapping, ordinary DB/profile
census and absence of current journals; pending capsules hold.

## Source qualification boundary

Tests run production source read-only in an existing cached Python container,
with read-only root, network disabled and synthetic tmpfs state. Actual child
`os._exit` after SQLite dirty-page spills produces a nonzero hot journal and
changed original pages. Another test SIGKILLs a recovery child only after
observing some sampled original pages restored and others still dirty while
the journal exists, then verifies restart restores exact committed bytes/inode.
Tests also cover checkpoint/copy interruptions, the first temporary-write
window, committed claim/clock preservation, corrupted business rows, full
binding/config mismatch, journal replacement, retained capacity, unknown
capsules, and default/v1/bootstrap compatibility. These are source tests, not a
new image build, installed worker activation, hardware power-cut test or NAS
operation. Independent patch review is required before release.

## Closed projection and stage correction

The recovered-copy census uses the actual driver's fixed and mutable claim-key
constants. A projected `claim` must contain every fixed identity key and may
contain only the driver's permitted mutable keys; full `submission` or
`resultManifest` overlays are rejected before original rollback. The submission
continues to come exclusively from the sealed indexed claim envelope.

Only the explicit ODM version-2 stages emitted/consumed by the driver are
recognized. An unsealed claim is allowed only in `claim_pending`, whose mutable
control contains at most the existing execution advice. A sealed claim cannot
remain in `claim_pending`; pre-grant and granted stages must match the actual
indexed grant/progress footprint. `combined_prepared_held` remains forbidden
because the indexed worker ABI refuses combined/TreeDar composition.

The reservation-intent window is preserved: in `claimed` or
`reservation_pending`, the local reservation may exist before the coordinator
has returned a public `transferRequest`. Later phases require exact equality.
This matches the driver's reconstruction checks and avoids rejecting that legal
power-loss checkpoint. Genuine hot-journal regression cases cover rejected
unsealed claim overlays/unknown stages and accepted legal mutable fields and
reservation-intent windows, with unchanged original/journal/scratch bytes on
rejection. The earlier 75-test packet is superseded for this closure defect;
zero-header recovery remains separate deferred work.

## Fetch projection and phase/prefix correction

The fetching-only projection guard is now a shared pure function extracted from
`ReferenceDriver._indexed_fetch_step`; runtime uses the same comparison and error
semantics. Recovery supplies a manifest/reservation-derived input identity,
rejects extra hot-fetch control keys, and checks the typed input-accounting
checkpoint. A raw `fetching` checkpoint remains anchored at zero while actual
progress lives in the indexed cursor. In particular `fetching` with a complete
cursor is legal until the next step records `processing_pending`.

Every post-fetch ODM stage requires the complete indexed prefix and matching
materialized checkpoint count/bytes: processing pending, execution pending,
executing, result/failure delivery, either acceptance wait and completed. An
incomplete prefix cannot gain original-rollback authorization merely by changing
a recognized stage string. Fetching-pending remains at the initial empty
prefix. Tests cover both refusal and legal completion boundaries across all
these stages, preserving early claim/local reservation windows. This narrows
recovery authorization without altering the normal runtime's fetching guard or
introducing cold-journal cleanup.

The phase/prefix correction's local qualification is deliberately split: all
29 focused recovery tests passed, and four actual runtime regressions passed.
The separate real 10,000-step runtime oracle held at object 5,367 with
`indexed_clock_rollback` (129.611-second five-test runtime group); it did not
pass. Its original failure log is retained. No clock reset, injected clock,
guard relaxation or automatic rerun was used. Prior local backwards-wall-clock
evidence makes clock behavior a candidate explanation, but this failed run did
not capture a failure-time clock sample. Independent source review and actual
main-Linux packaged-image qualification remain required; local WSL tests do not
clear an activation gate.
