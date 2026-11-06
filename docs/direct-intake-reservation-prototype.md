# Direct-intake reservation prototype

This is an unwired queue-only prototype, not a source transfer, worker scratch
reservation, API, custody receipt, deployment, or authorization to process. The
caller must already authenticate and create a direct-only job in the same trusted
queue; the reserved `directIntakeVersion` marker is not part of the current
Viewer/Processing V1 or V2 contract. Existing V2 source-intake jobs are refused.

Within one SQLite write transaction the prototype uses the existing eligible
node/priority selector, records an immutable attempt, authority, environment,
manifest, node and worker binding, and holds the existing queue slot. A retry
may observe the same binding; changed identities fail closed. The slot stays
held across restart and cannot expire or be released by this prototype. The
ordinary reference claim path refuses the direct marker, so it cannot start an
engine while direct custody is incomplete. The internal queue state is `held`;
it must not be exposed as a completed upload, processing status or result.

Before any runtime wiring, implement and verify an authenticated worker-side
physical-node gate, durable per-object transfer receipts; crash recovery,
stream/fallback and cancellation reconciliation; safe releases; coordinator
freeze, custody/migration and complete metadata/disk accounting for this table;
durable pinned worker inventory and fresh health; a Viewer receiving/waiting
presentation; and independent cross-language protocol tests. In particular,
no bytes may be acknowledged or processing started merely because this node
slot exists. A worker or coordinator disconnect must retain the source at the
Viewer or in qualified cluster-local spool until verified custody. No production
resource, customer data, deployed service or V2 behavior is changed here.

The separate unwired `direct_worker_scratch.prepare` prototype derives a local
scratch reservation identity from the complete supplied binding, then uses
`ScratchLedger.reserve` and its durable pre-transfer fence. A crash between
those transactions must never result in a byte transfer; an unfenced reservation
can expire, while a fenced one remains held across restart. Its return value is
only a local capacity observation: it neither authenticates the caller nor proves
physical node-gate ownership, accepts a source object, permits transfer, or
acknowledges custody. A verified caller, durable receiver, capacity/mount
identity, reconciliation and cleanup proof remain necessary before any wiring.

The separate unwired `direct_worker_receiver.receive` prototype accepts one
closed, hash-bound manifest object only when the exact scratch reservation is
fenced and an already-provisioned physical NodeGate is locked and bound to the
same authority, worker, job and deterministic receipt. It confines the spool
to that reservation and delegates atomic publication, hashing, file and
directory sync, and lost-reply replay to `SourceSpool`. Its observation proves
only that a local object was verified; it explicitly does not acknowledge the
source closure or authorize engine execution. The entire scratch reservation
remains charged (conservatively including already written bytes). Authentication,
physical gate creation/handover, transfer lifetime, bounded concurrency,
complete source/derived closure, quota reconciliation and network protocol are
not implemented. Its tests use real NodeGate and a real Linux tmpfs mount, but
do not qualify any installed disk, host, network or power-loss guarantee.
