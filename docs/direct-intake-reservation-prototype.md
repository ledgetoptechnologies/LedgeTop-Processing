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

The receiver installs a durable, bounded direct-writer pin atomically with the
scratch-fence check before reading. Generic unstarted settlement, allocation
accounting, engine lease and engine-state transitions reject while pinned. The
pin is retired only after the synchronous reader's `close` returns and reports
closed; an exception, uncertain close or process death retains it across restart
for inspection. No automatic reconciliation or release is implemented. This
prototype assumes a trusted owned reader; the future network adapter must prove
reader ownership and retirement rather than accepting a caller assertion.
The first writer permanently latches a ledger version marker. The exact earlier
`649b` scratch source was tested to reject the ledger after retirement; that
does not qualify rollback across all old versions. A physical upper bound for
pin metadata growth and production upgrade/rollback handling remain open.

An unwired closure prototype binds a domain-separated digest of the selected
derived-object inventory into the coordinator reservation and worker scratch
identity. It rehashes all objects in its combined V1-style manifest under the
node gate and writer pin, checks the closed spool directory, syncs it, and
persists an immutable local receipt. Exact replay revalidates the objects and
receipt. A missing object before verification does not strand the upload;
unknown files, changed bytes, receipt drift or uncertain publication hold the
pin for inspection. This local receipt explicitly says source custody has not
been accepted and processing is not authorized. It does not yet represent the
full Viewer source snapshot, raw GCP provenance, V3 path mapping, or a signed
and authenticated cross-service closure. No Viewer acknowledgement or source
release may rely on it.
Writer admission now includes a 64 KiB logical SQLite/WAL allowance per active
or uncertain writer in the scratch free-space calculation. It is a conservative
planning hold, not a filesystem quota or a proven upper bound on physical
database growth. The writer accounting marker is version 2; a version 1 ledger
is refused rather than silently adopting weaker accounting.

Recovery remains deliberately unavailable. A process-ID or death check does
not prove that another process lacks a duplicated reader descriptor. The future
transport must enforce a lifetime lease and reader ownership with no untracked
descriptor delegation, and prove the old lease and reader are retired before
an authenticated recovery can remove a pin. Stale, missing, or uncertain
identity must retain the reservation. Do not implement timeout-only release.
