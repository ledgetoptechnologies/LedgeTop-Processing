# Canonical worker backend maintenance (source draft)

This opt-in host-side extension uses the existing owned node-maintenance operation.
It has not been installed on either processing PC. The installed legacy updater
and worker package do not acquire these protections simply by having this source
available. The synthetic tests exercise the host lifecycle and paired coordinator
publication. This remains an unreviewed source draft until both lanes freeze and
the coupled qualification completes; it is not an installed update mechanism.

## Explicit configuration and deployment boundary

Service schema 2 requires `maintenance.workerAware: true` and a private
`maintenance-worker-v2.json`. Its closed profile binds the exact worker CID,
immutable image, full Docker recipe, config directory, state/scratch directories,
private TLS certificate and CA fingerprints, Engine ID and exact hashed gate
directory. The semantic worker config base excludes **only** `engineVersion`;
credentials, endpoints, role identities, `optionEvidence`, cleanup flags, and all
other fields remain unchanged. A persistent current-publication receipt binds
the full published config bytes and filesystem identity for sequential updates.

The legacy `maintenance.json` and every declared legacy profile path must be
absent. Deployment must separately prove that the installed old updater/timer
cannot take its old path; merely installing a new flag is insufficient. The
archived actual 020c loader/CLI tests prove that the old implementation refuses
the new service schema, new filename and new flag. They do not qualify a deployed
timer change. No source in this lane changes timers, credentials or permissions.

Bind the config **directory**, not an individual `worker.json` inode, so atomic
replacement becomes visible to the restarted container. Bind only the exact
private hashed Engine gate root, not its parent. Host-side Docker inspection and
namespace inode readback verify the recipe and mounts; the worker receives no
Docker socket. Initial provisioning and real namespace qualification remain
installation gates.

## Lifecycle and retained work

After the existing owned drain/begin CAS, the helper verifies the exact authority,
plan, revision, target and unreserved physical gate. Reader-role private TLS
health must show an idle canonical ODM service. Graceful stop and thread joining
precede acquisition of the actual daemon state-owner lease and a bounded
read-only SQLite census. Pending acceptance, success/failure outboxes, uncertain
readers, unfinished cleanup or engine tasks hold the operation. Nothing is
discarded to manufacture idle status.

The existing Compose adapter still prepares both images and performs the native
replacement. The opt-in concrete adapter rechecks the original owned updating
CAS, actual host gate and exact stopped worker/state census after Compose
preparation, immediately before Docker mutation. Legacy Compose behavior is
unchanged. Config rebind records an intent before writing and atomically
publishes only the new engine version under exact old bytes/inode checks.

The helper releases the host gate during worker startup so the canonical factory
can acquire that same gate. Coordinator state stays owned `updating`, ingress and
operation locks stay held, and the worker cannot obtain a new claim. The helper
then reacquires the actual gate and verifies the immutable worker recipe, mount
inodes, private authenticated health, backend CID/image/Compose/loopback binding,
version and original completed job/manifests/outbox history. Reader epochs and
the global runtime pin may legitimately change across factory restart; original
per-job bindings do not.

Before final reopen, the coordinator must atomically publish the owned runtime
version and invalidate its catalog/cache generation. The helper sends the closed
11-field request to the existing operator-authenticated runtime-version route,
using the original begin-update token only in `X-LTDS-Update-Token`. The original
owned schedule request UUID is part of the durable operation binding. It is never
substituted with a newly scheduled plan. No token appears in the request body,
publication receipt or review record.

The request is journaled before its single POST. A lost reply permits only the
immutable receipt GET. An absent receipt holds rather than authorizing another
POST. Fresh current-state validation requires the same publication request,
per-node epoch, version, image, authority, Engine and gate generation, while
allowing unrelated global epoch progress on the other node. This HTTP observation
is configured service identity; host Docker proof supplies the separate actual
container/image checks.

Two completed sequential updates survive a real coordinator Queue/runtime cold
reopen between them. A restart **during** an update has a different existing
safety rule: Queue startup changes `updating` to `held`, advances its revision and
clears the begin token hash. The publication receipt and current binding survive,
but the helper cannot finish/reopen from historical evidence. Ingress remains
closed and the original operation is retained for owned reconciliation. This
profile does not add automatic recovery of that maintenance authority.

## Unknown outcomes and cleanup

Stop, config replacement, publication, start and finish have durable intents.
Unknown outcomes permit exact read-only reconciliation; an absent/uncommitted
effect is a hold, not permission to replay. No gate reset, historical state
rollback, forced process kill, result deletion or TTL extension is introduced.
The bounded stop command requests graceful shutdown and may time out locally
while Docker is still acting; it never escalates to killing the worker.

Completed history is retained. Automatic processing-copy cleanup after durable
acceptance requires the separately qualified worker cleanup configuration and
completed cleanup receipts. This profile does not silently enable those flags.
NAS original source files remain outside its cleanup authority.

## Synthetic verification scope

Tests exercise the actual canonical factory and service threads, Queue/native
maintenance API, private Worker API/Reader TLS, daemon state lock, NodeGate and
SQLite histories. Docker/Compose/namespace and ingress are explicit synthetic
boundaries; the backend is an isolated HTTP fixture, not scientific processing.
Cases cover sequential and same-version image updates, actual coordinator
restart, completed history, pending acceptance, outbox/reader holds, publication
receipt loss without replay, atomic inode publication and ownership changes
after Compose prepare. The coordinator is the actual configured
`ReferenceRuntime`/registry/catalog implementation and native operator handler;
the worker uses its actual private TLS listener. No real photos, NAS state or
live processing services are changed.
