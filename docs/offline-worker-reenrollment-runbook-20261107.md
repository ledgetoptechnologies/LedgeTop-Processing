# Offline worker re-enrollment: bounded rehearsal

This runbook describes a **staging rehearsal**, not an authorization or a
production-ready cutover. The current first installer cannot enroll over an
existing unit, including a masked one. The procedure deliberately stops both
coordinator authority and engine ingress before moving the old files and does
not reopen them on any error. Confirm all paths and identities against the
private reviewed manifests, not the examples in this document. Beau controls
any TrueNAS changes separately.

## Required inputs and hold conditions

Prepare two *fresh*, independently validated enrollment manifests and their
root-owned dependencies/configuration: the candidate and a rollback operation
derived from the old settings. Each must have a distinct new operation UUID,
installation root, matching state/scratch paths, exact host and boot IDs,
coordinator authority, canonical physical gate, engine container/image/port and
source/dependency/TLS pins. The rollback operation is not the old manifest or
its receipt: moving its unit changes its identity. Verify both manifests with
`validate_manifest` and stage the artifacts before the window; do not print
secrets. Prepare the existing coordinator cutover/rollback plans and verify a
restorable data backup. Until both fresh enrollment bundles and their guarded
activation plans exist, **do not begin**.

Inventory all coordinator clients, worker processes, listeners and node engines.
Admission must be closed and all active work durably drained, including uploads,
results awaiting acceptance and ambiguous claims. A 503 response or an empty
gate alone is not proof. Confirm the native engine listens only on loopback,
there is no alternate scheduler or unmanaged direct engine caller, and a later
operator can inspect every gate/journal after a crash. If any item is unknown,
hold without changing the worker.

## Closure, then retirement

1. Record the exact old coordinator container ID, image, restart policy,
   state/lease mounts and backup verification. Using the supported coordinator
   cutover controls, close admission and prove drain; preserve its state and
   lease. Stop the *exact* predecessor only after its data and running jobs are
   safe. Inspect its ID and `State.Running=false` after stop. Disable its restart
   policy for the window only using the supported cutover journal; do not use a
   bare stop if Docker could restart it. The new coordinator remains stopped.
2. Use the existing `ops.node_ingress --root <reviewed-private-root> close`
   command for each affected engine ingress. Read the durable closed receipt,
   prove old nginx workers drained and the exact public ingress returns 503.
   Verify the private backend is unchanged and live task/upload/process census
   is idle. Check the original physical NodeGate binding and empty reservation.
   Do not alter gate generation or manufacture a `release_empty` response.
3. Run the old `Installer.inspect` using its root-private manifest and pinned
   root-owned installer. Verify the installed tree/receipt first. Run as the
   actual service user:

   ```sh
   systemctl --user stop ledgetop-processing-reference-worker.service
   systemctl --user disable ledgetop-processing-reference-worker.service
   systemctl --user show --no-pager \
     --property=LoadState,ActiveState,MainPID,UnitFileState \
     ledgetop-processing-reference-worker.service
   ```

   The returned unit must be loaded, inactive, disabled and PID zero. Inspect
   its cgroup and the host process table for any detached worker using the old
   installation or configuration. Do not kill an unidentified process. Repeat
   the closed-authority/closed-ingress and native-idle checks immediately before
   the next step.
4. Create a new private, empty, same-filesystem archive directory. As the
   service owner, capture hashes and metadata for the exact canonical unit and
   config, then move *only those two exact regular, single-link files* into that
   directory using no-clobber operations. Fsync both affected directories and
   verify the archive bytes and ownership. Preserve the original root-owned
   installation and journal untouched. Any uncertain move or mismatch is an
   inspection hold: do not retry, restore or reopen automatically.
5. Run `systemctl --user daemon-reload`, then verify `LoadState=not-found`,
   `ActiveState=inactive`, `MainPID=0`, empty `UnitFileState`, no old cgroup or
   detached worker, and both original canonical paths absent. The temporarily
   unmasked window can admit an explicit start of a cached unit. Coordinator
   authority remains stopped and ingress closed throughout; a detected process
   or uncertainty holds the window closed for inspection.

## Fresh enrollment, activation and rollback

Run the **existing pinned** `ops.enroll_reference_worker` `prepare`, `inspect`,
and `enroll` commands with the reviewed candidate manifest, as specified in
`docs/first-worker-enrollment.md`. They create new operation state and an exact
unit and config, but do not start or enable it. Retain and verify the fresh
install receipt and whole-tree digest. Confirm systemd remains inactive and the
authority and ingress closures still hold. Do not restore the old receipt or
adopt partial output if an operation is interrupted.

Use the existing first-worker activation and coordinator cutover procedures,
their exact journaled operation and fresh readiness checks. Before coordinator
admission or ingress reopens, recheck no old worker process, a single expected
new worker, physical gate ownership, engine task/upload census, no alternate
writer, and Viewer V2 identity/TLS readiness. Keep automatic cleanup disabled.
The cutover is not complete until a small installed job and two-node queue and
recovery qualification succeed.

If rollback is needed, keep authority and ingress closed. If any new worker
started, first stop it under the supported activation/cutover rollback and prove
its process and custody state; uncertain effects require inspection. Preserve
its installation and journal. Archive its canonical files only after proving
idle and reload to `not-found` as above. Use the *separately prepared fresh
rollback operation* with the standard installer and a new receipt, then the
supported activation and predecessor recovery procedures. Restore job service
only after the rollback worker and predecessor are independently verified. If
the coordinator has accepted new work or custody is ambiguous, do not silently
replay an old snapshot or reopen the predecessor.

## Simulation boundary

`tests/test_never_started_reenrollment_boundary.py` demonstrates on Linux that
the standard installer rejects a masked unit and that rename/restore changes its
proof identity. A separate synthetic-host test in
`tests/test_enroll_reference_worker.py` performs actual installer prepare/enroll
operations for an old install, a fresh candidate, and a fresh rollback operation,
and checks that their journals and archived files remain intact and the old
receipts do not authorize the new canonical files. On 2026-11-07 all three tests
passed in a bounded, network-disabled Linux container using candidate image
`sha256:98c37f4b7bcf2ac5626b9368852e8c1d2e9c2553d5606b26b26d3e37dccc7fdb`.

The host/engine/systemd boundaries in the fresh-enrollment test are mocked.
These tests do not prove a real systemd transition, ingress closure, crash
recovery, correct private inputs, or a complete maintenance rehearsal. A
host-isolated rehearsal with independent coordinator/engine observers remains
required before using the procedure on either processing PC.
