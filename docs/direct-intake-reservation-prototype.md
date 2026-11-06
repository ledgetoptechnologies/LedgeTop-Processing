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
scratch reservation and durable per-object transfer receipts; crash recovery,
stream/fallback and cancellation reconciliation; safe releases; coordinator
freeze, custody/migration and complete metadata/disk accounting for this table;
durable pinned worker inventory and fresh health; a Viewer receiving/waiting
presentation; and independent cross-language protocol tests. In particular,
no bytes may be acknowledged or processing started merely because this node
slot exists. A worker or coordinator disconnect must retain the source at the
Viewer or in qualified cluster-local spool until verified custody. No production
resource, customer data, deployed service or V2 behavior is changed here.
