# Reference listener to ordered Viewer inventory: rehearsal checklist

This is a Viewer-only migration proposal, not a way to enable the reference
pipeline with inventory. `PROCESSING_REFERENCE_CONFIG` and ordered inventory
remain mutually exclusive in the current candidate, because mixed reference
and Viewer claim paths have not been qualified together. Removing that check
would not establish shared physical-node authority or safe per-node claims.
The old reference listener will become unavailable while this proposal is in
effect. TreeDar, reference workers, partner submission, NAS source-range
delivery, and cleanup are not enabled by this migration.

The 2026-11-04 observation on the cluster found the reference runtime configured
for staging and two workers; eight present reference tables contained no rows,
and the two worker result endpoints were unreachable. There were no established
reference-listener connections in one sample. This is historical, point-in-time
evidence: offline clients, other state, and future submissions cannot be ruled
out. Preserve the reference config and credentials without displaying them.

## Before any candidate switch

1. Coordinate the existing Viewer/staging owner. Identify every reference
   client, worker, listener and ingress, including offline retrying clients.
   Drain intake, hold production and staging scheduling, and verify both
   physical nodes are idle under their existing gates. An empty queue, TCP
   connection sample or process list alone is not a fence.
2. Preserve the exact existing coordinator image digest, container settings,
   port and private mount mapping, reference config/certificates, authority
   lease and database volume. Do not print secrets. Confirm that rollback can
   use these exact artifacts and that no other controller can own either node.
3. After writers are stopped and fenced, create a consistent self-contained
   SQLite **backup** in a task-owned private location; do not copy a live DB
   while omitting its WAL. Inventory retained Viewer artifacts and every
   reference worker/outbox root separately. Unknown, inaccessible, nonempty or
   changing reference state holds the cutover; do not delete it. Record the
   immutable image and backup identities, not client data.
4. Run `python -m ops.reference_inventory_preflight <offline-sqlite-backup>`
   on that backup. It is read-only and advisory. Review every returned count,
   unknown reference table, terminal Viewer state and authority/lease row; it
   cannot attest file custody, network clients, the active gates, a stable
   daemon, or that the backup came from the intended live volume. A missing
   table, WAL/journal beside the backup, or inspection error is a hold.
5. Recheck node identity and inventory order. Explicitly map the existing
   `cluster` physical host and preserve its 1,000-photo maximum; configure the
   main node's own capacity and capabilities. The candidate's durable queue,
   lease and existing Viewer task history must remain on the original volume.
   Viewer admission must allow at least two jobs if both nodes are to run.
6. Rehearse a flags-off update and exact-volume rollback with completed retained
   tasks and realistic archive sizes. The current updater rejects files above
   64 MiB and may leave the old coordinator stopped after a timeout; helper
   crash recovery is still disabled and live dual-host gate behavior is not
   qualified. Do not raise the limit or assume an automatic rollback.

## Candidate acceptance

Only after the above evidence is reviewed, use one coordinated container swap
to test a Viewer-only candidate with reference runtime absent and retained
config/mounts preserved. Verify the original authority lease and sole volume
owner, running image identity, existing Viewer tasks, dynamic options,
production/staging priority, node gates, health, logs and queue visibility.
With invented data first, test the ordered small-node preference, one task per
node, an oversized queued job waiting for an eligible node, shutdown/restart,
archive delivery and retained-result recovery. Qualify both actual engines and
the Viewer display before client work. The current NodeODM status 20 can appear
before execution, and retained logs lack a qualified incarnation signal.

For rollback, stop new admission, drain or hold active work, reconcile node
and coordinator custody, verify the same authority and gates, then use the
reviewed rollback path only if it proves compatibility with all state created
since cutover. Otherwise retain the candidate and held data for inspection.
An old config alone is not proof the old image can safely read newer state.

## Separate future work

Re-enabling reference with ordered inventory requires actual shared atomic
per-node claims, worker readiness, compatible protocol/credentials, durable
result custody and recovery, update/rollback census, concurrent Viewer and
reference contention tests, and one-controller/physical-gate qualification.
The presence of partial inventory-aware `ReferenceStore` code is not evidence
that this combined runtime is ready.
