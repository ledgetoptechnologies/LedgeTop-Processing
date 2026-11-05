# Reference worker maintenance handover

This extension uses the existing qualified immutable-image update supervisor, owned
coordinator plan, private ingress drain, physical NodeGate and deferred finalization.
It does not install a timer, provision credentials, perform OS updates or deploy itself.

## Reviewed configuration

A reference node requires explicit `workerMode: "reference"` and a `referenceWorker`
manifest in its private maintenance settings. The manifest has schemaVersion 1 and
fixed unitName, unitFile, unitSha256, pythonExecutable, configPath, sourceRoot,
sourceSha256, readinessOrigin (HTTPS), and readinessCAFile fields. No field is
inferred from a running container or a missing legacy marker.

The worker config additionally requires `maintenanceReadiness`: schemaVersion 1,
a separate bearer, sourceRoot, sourceSha256 and activation initially null. Keep this
credential separate from worker, result and engine credentials. The installed
source root must match the actual daemon source. SourceSha256 is computed by
`processing.reference_worker_readiness.source_digest`; it binds bounded Python
source files under ops/processing and rejects symlinks. This detects accidental
source drift; it is not hardware attestation against a privileged administrator.

The reviewed unit uses the exact Python `-B -m ops.reference_worker_daemon --config`
command, Type=simple, UMask=0077, NoNewPrivileges=yes, KillMode=mixed and
SendSIGKILL=no. The controller verifies the unit hash, process command, executable,
working directory and owner. Before each action and readiness process probe, it
also checks the loaded unit: no drop-ins or pending daemon reload, exact effective
command, no lifecycle hooks, and matching termination/security properties.
Unsupported systemctl command-record encodings hold closed. It only requests
graceful start/stop on that unit.
A stop needs both the original process gone and its cgroup empty; an active service
flag never proves readiness. No automatic forced kill is used.

## Durable transition

After per-node queue drain and ingress closure, the updater holds an empty physical
gate, authenticates the old worker and records its exact config/provenance. It
stops that worker and reuses the existing readonly migration census to retain
pending accepted-result/outbox history. Engine replacement remains subject to the
existing qualified image and exact version checks.

The updater CAS-replaces only engineVersion and the plan activation in the worker
config, durably journals both, and briefly yields the physical gate while the
coordinator is updating and ingress remains closed. The real worker factory then
uses its existing driver pin migration authority. No SQLite journal is replaced,
restored or reauthored. Old sealed result manifests retain their original versions.

A separate authenticated HTTPS nonce challenge must prove the actual worker's live
owner/read-session leases, committed driver pin, current source/config hashes,
boot/process identity, engine identity/version, actual options discovery and task
census. The updater reacquires the gate and repeats readiness plus engine/ingress
checks before reopening or marking the plan successful. The public completion
contains only a frozen handover hash, never credentials or private config bytes.

The private worker-handover.json contains the old config for rollback and therefore
contains secrets. It belongs beside the existing private maintenance receipts;
never publish it. Keep receipt and config directories private to their service user.

## Failure and recovery boundaries

Known pre-open startup/readiness failures stop the new worker, CAS-restore the exact
old config, replace the prior image, and start/authenticate the old factory. The
node remains failed with ingress closed. Unknown start/stop acknowledgements,
foreign config/lease changes, authority changes and uncertain ingress reloads hold
closed rather than repeating side effects. Lost config acknowledgement is recovered
only by exact-byte readback and fsync. Finished replay authenticates the current
serving worker without redoing stop/start/replacement. Some interrupted phases
intentionally require operator inspection; there is no blanket retry override.

## Evidence boundary

Tests exercise actual TLS, worker factories, SQLite journals, pin migration, OS
leases and physical gates with synthetic Docker/systemd transports. They prove
ordering, durable replay and refusal/rollback behavior. Installed systemd operation,
real image replacement and power-loss testing still require a separately reviewed
node rollout. This patch performs no such deployment.
