# Native ODM intermediate controller

`processing/reference_odm_stage.py` implements the native-engine part of a
sequential parent pipeline. It is source-only: no worker factory or router
constructs it, no capability is enabled and no processing PC is changed by this
module. It does not create child jobs or call the standalone worker's finalizing
`poll()` path.

## Concrete runtime and ownership

`NativeRuntimePolicy` fixes the local Docker executable/socket/Engine ID,
authoritative Compose file and base digest, project/service/backend name, exact
cached backend image ID, literal private loopback origin, engine family/version,
container port and bounded inspection/transfer policies. Its `sha256` must equal
the parent `PipelinePolicy.odm_runtime_sha256`. This digest is a binding, not
authorization: the concrete `NativeRuntimeProbe` inspects the actual local Unix
socket, Docker Engine, running backend image/container/port and Compose pin/base.
The engine API must separately report the expected family and version. A new
image reporting the same version is rejected. Once an attempt starts, its actual
container ID is immutable too.

Construction requires the actual `NativeEngine`, `PipelineJournal`,
`ScratchLedger` and live `DaemonStateLock`. The journal and ledger must share the
same state-owner root. That real private UID-owned mode-0700 root must already be
durably provisioned; the constructor never creates it or its ancestors. It
publishes only new private journal/lock files, checks the dedicated schema and
bindings and fsyncs root/parent. File/root inode identities, process identity,
the real OS lease and fixed dependency objects are checked throughout.

The trusted `ParentGuard` must have the parent journal's exact binding and prove
the current claim, full reservation and physical fence. The trusted input
resolver has the ODM adapter binding and a `resolve(intent)` method returning
only original image-ID → reservation-relative-path entries. Resolved files are
independently hash/size verified inside scratch. Neither a boolean guard nor
submission-controlled executable/origin/path policy is accepted. Backend tokens
remain only in the actual engine client in memory; they are not serialized.
Both the engine origin and its mutation client's origin are pinned.

## Start, tick and lookup are different operations

`start(intent)` first verifies the exact **already-issued** ODM intent in the
parent journal. It checks fresh safe numeric/boolean option definitions, the
native task census and absence of that exact parent task UUID, and verifies the
full parent input bytes are already allocated. Only original image objects are
uploaded; the separately retained original LiDAR object is not sent to ODM.
After resolution and potentially long input hashing, it refreshes process/state
ownership and the live parent guard outside the physical gate. Under the gate's
OS lease it rechecks reservation, receipt and actual runtime, and checks them
again immediately before initialization. Guard/runtime drift during resolution
therefore cannot authorize a late engine mutation. Guard callbacks are never
called while the gate is held, avoiding reentrant physical-lock acquisition.

The caller must already own the parent physical-gate receipt and have durably
provisioned its private `jobs/<reservation>` directory. The controller adopts
that exact receipt under the physical lock; it never calls `gate.reserve()`.
It records its attempt before publishing its own `odm-stage` output directory,
then FULL-commits initialize/upload/commit intent before each corresponding HTTP
mutation. It makes one attempt at each mutation and leaves the parent reservation
`engine_started`. A duplicate start cannot repeat these calls.

`tick(job_id)` may read native task status, download an archive into its bounded
private cache and commit local artifact/proof checkpoints. It issues no native
initialize/upload/commit/remove/cancel calls. Unknown initialize or upload
outcomes remain held. A committed task with a lost commit reply can be observed
through GET status without another commit. Failed/cancelled terminal status
produces stage failure evidence, not a parent completion.
An exact coordinator-restart-held parent may collect already-running work's
terminal evidence when its trusted guard permits read-only recovery; that state
does not authorize another start or the next stage.

`lookup(intent)` uses a read-only SQLite connection to return the exact sealed
stage proof, or `None` while none exists. Apart from ownership/lease/file checks,
it performs no engine, Docker, guard-transport, reservation or SQL mutation.
Rollback-journal file format is checked before opening SQLite, so a WAL database
is refused before a reader could create shared-memory sidecars. It does not call
`tick()` or advance the parent pipeline.

`verify(intent, proof)` freshly checks the parent fence/reservation, concrete
runtime, exact terminal native task identity and owned archive bytes/hash.
`artifact_paths(intent)` is a private projection for a future final delivery
adapter; it is not an HTTP payload or acceptance receipt.

## Archive safety and parent capacity

The inert `reference_native_archive.py` helpers are extracted from the existing
standalone worker. Both controllers use the same regular-file/no-symlink,
single-link/inode cache checks, identity-before-first-byte checkpoint, bounded
archive reads, ZIP validation and size/hash verification. The standalone worker
still performs its original final result/outbox/gate steps; only its inert cache
and owned-file operations delegate to the shared helpers.

A new successful GET must open before existing partial bytes can be truncated.
Source errors retain those bytes. Only the exact recorded cache inode can be
rewritten. Complete artifact size/hash is durable before link/unlink publication
and directory sync; unbound files or uncertain publication remain held. Retrying
a safe archive GET never reruns reconstruction. The native stage additionally
has an explicit bounded archive-read deadline; expiry retains its cache and does
not stop the processing engine. No CPU/RAM/PID/thread or reconstruction timeout
is introduced.

Allocation is a monotonic watermark containing **all parent inputs**, including
LiDAR, plus the retained ODM archive. Retained partial bytes are accounted even
after a failed GET; retries cannot decrease allocated bytes. The controller never
calls `register_result`, sets the parent terminal, persists an outbox, prepares
gate completion, releases a gate or deletes inputs/results. Unused TreeDar
capacity remains reserved after the ODM proof. Unknown or failed work retains
its evidence and capacity until the outer pipeline handles it explicitly.

## Synthetic qualification and remaining work

Tests use actual `NativeEngine` HTTP against the isolated synthetic
`NativeNodeFixture`, actual Queue/ReferenceStore/ScratchLedger/NodeGate and real
SQLite/file/OS ownership. Only Docker inspection transport is simulated; no
Docker backend or reconstruction job is run. Three-byte inputs and an empty
valid ZIP do not prove imagery quality or genuine LiDAR acquisition.

Covered cases include one native attempt, lost initialize/upload/commit replies,
lost intent/proof commit acknowledgements, GET/cache restart and source outage,
SQL-only lookup, archive/proof corruption, same-version image/container changes,
mutation-client origin drift, current-option removal, active backend refusal,
foreign schema/WAL, owner/fork/lease refusal, and retention of the original parent
claim, gate and complete scratch reservation. Existing standalone worker/driver
tests qualify the helper extraction separately.

Remaining work is the TreeDar intermediate adapter, complete final delivery
facade, fresh claim/provenance admission, factory/router/driver composition,
runtime epoch adoption and installed acceptance. The factory must qualify durable
state/scratch ancestor publication and hold its owner lease throughout. This
controller does not authenticate a NAS acquisition issuer or renew an expired
transfer/provenance grant. Existing Viewer storage/provenance ownership is
unchanged. Combined processing must stay unadvertised until those paths pass.
