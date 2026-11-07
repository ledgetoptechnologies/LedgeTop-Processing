# External custody controller preparation — 2026-11-07

This is an inactive controller candidate, not an installed coordinator, cutover
bundle, worker readiness receipt, or permission to treat a live database copy as
a complete backup. The existing 4705 runtime image and its worker source archive
were not changed.

On the cluster PC, the private task-owned directory
`photo-pilot-coordinator-inputs-20261106/controller-external-custody-7cfb6075/`
under `/home/bkoltz/.local/share/ledgetop-processing-reference/` contains `source/`
and a retained `reviewed-source-updated.tar` with SHA-256
`4d970bea91011b438a6c6f0f9003c8c3279f8fd99926881cd74495b080dc9340`.
The archive includes the independently updated custody and driver model tests.
After correcting archive line-ending differences in `processing/`, recursive
readback of `ops/`, `processing/`, and `contracts/` against the existing LF
4705 source found exactly two differences, both in `ops/`:

- `enrolled_coordinator_custody_ops.py`: SHA-256
  `1072b09ae6d1390d6f19b71cad05798523a7b12ae3efd58a2d925eae4f390f8c`.
- `enrolled_coordinator_driver.py`: SHA-256
  `dab009d0544065a18b12d038779f05bfbcefb66812dd17020240b75cd5fbfcd5`.

Both changed hashes match the exact LF Git blobs from commit `7cfb6075`.
The unchanged serialized census, projection and startup-policy modules remain
those of 4705. The controller fix reads counts only from the hash-pinned,
reviewed counts artifact and still requires an empty predecessor reference-job
queue; it does not remove the historical predecessor node/custody fences.

For bounded verification on `.89`, a temporary task-owned Docker volume was
populated from this actual tree, then mounted read-only in the exact 4705 image
with network disabled, UID 10001, a 0.5 CPU limit, and 384 MiB memory limit.
The custody, predecessor-census, deployment, driver-model and worker-prestart
review suites passed **106 tests**. The driver-model suite mocks its external
worker-review boundary; the separate worker-prestart suite runs real review
validation. These are isolated tests, not a real coordinator cutover or worker
activation. The task-owned test volume and containers were removed afterward.

Before using this controller, independently review and bind the controller
source/archive and exact invocation, regenerate and pin fresh reviewed counts,
construct a complete validated operation plan and bundle, qualify both workers
and the spool against current state, retain an actual complete frozen-state
backup, and coordinate Viewer configuration and rollback. A separately captured
live SQLite online backup is only a point-in-time database copy; it excludes
the payload tree and cannot authorize rollback. Do not mix this external
controller with stale preparation receipts or claim the unchanged 4705 image
contains the controller change.
