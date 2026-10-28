# Reference-to-reference coordinator updater

`ops/update_reference_coordinator.py` is a separate source executor for updating
the existing reference coordinator. It does not reuse the native predecessor
selection or change the historical installer/frozen operation artifacts. Import
does nothing. The activation CLI is enabled after independent source review,
the frozen 894-case Linux run (890 passed, four Node skips covered on Windows),
and the actual two-PC gate preflight recorded for source `890cd26` in
`docs/evidence/coordinator-gate-preflight-20261028.json`. An explicit new prepared
operation and every immutable binding/live fence are still required. These
checks do not themselves install the replacement coordinator.

## Exact operation binding

The reviewed predecessor is container
`7ebe34636518961ddb4398b545a6a73d0254a47a093b198cf5d2d8411345cd07`, image
`sha256:48651a4f4b0ab88f9c4969ed2536344842ff974d262e1aa9c5bdff5e27b38de9`,
source `e2ccf489ab41160acff3f90e2b4bcd568f72cf24`. The executor revalidates
these identities. The much older native container beginning `6300` is never
selected or modified. The sole authority and exact original state volume remain
unchanged. A new schema-version-2 checkpoint explicitly records mode
`reference-to-reference-v1`; historical schema-version-1 checkpoints are refused.

The CLI runs only as `bkoltz` UID 1000 on host `cluster`. It holds the existing
fixed private, nonblocking coordinator operation flock for its entire prepare,
activate, or rollback command. Each operation uses a new private receipt
directory; no backup is restored and no state is deleted.

Both predecessor and candidate use the reference recipe validator. Native ports
18080/3005, reference TLS port 18443, the exact read-only TLS/config mount,
application/native environment, UID 10001, security, CPU/memory/PID settings,
logging, network, and volume are preserved. Image defaults are compared against
each image separately: a changed image default does not authorize changing an
application override. Environment values remain in memory; the private checkpoint
contains a digest. Candidate source labels, command, user, work directory and
healthcheck are checked before any node drain.

Prepare runs the existing UID-10001 candidate settings/TLS loader with no network.
It also hashes the TLS/config files, modes, UIDs, device/inode identities and
mount-root identity; subsequent mutation checks require the same census. It does
not generate credentials or PKI. The initial operation is intentionally narrow:
exactly 12 terminal public jobs and no native or reference rows/files, with both
nodes active. Later workloads need a separately reviewed wider baseline policy.

## Live gate transport

The root operator launches authenticated, host-key-pinned SSH children on main
and cluster and runs `ops.coordinator_gate_bridge.RootRelay`. Cluster does not
SSH to main. The cluster executor's stdin/stdout are exclusively bridge frames
during activation/rollback. Its final sanitized summary goes to stderr; private
checkpoints hold detailed evidence. Prepare does not enter the bridge and may
return its public operation summary on stdout.

The private `--gate-config` JSON is closed:

```json
{
  "schemaVersion": 1,
  "cluster": {
    "engineId": "fixed-actual-cluster-engine",
    "nodeId": "cluster",
    "authorityId": "7131882e-38ce-4d3e-99cc-4ba5a94d2ee8",
    "coordinatorOrigin": "http://127.0.0.1:18080",
    "generation": 1,
    "helperSha256": "<reviewed 64 hex bridge/NodeGate digest>"
  },
  "main": {
    "engineId": "fixed-actual-main-engine",
    "nodeId": "main",
    "authorityId": "7131882e-38ce-4d3e-99cc-4ba5a94d2ee8",
    "coordinatorOrigin": "http://127.0.0.1:18080",
    "generation": 1,
    "helperSha256": "<same reviewed helper digest>"
  },
  "sessionNonce": "<fresh private 64 hex relay nonce>",
  "timeoutSeconds": 10
}
```

The root relay and main holder must use the prepared checkpoint's operation ID
and this invocation's nonce. Public gate bindings are checkpointed; the session
nonce is not. `GateFactory` composes the existing canonical cluster `LocalGate`
with a live main `BridgeGateClient` and `GateSet`. Fresh checks verify engine ID,
authority, origin, generation, holder/lock identity, and idle reservation. Both
scopes stay live through drain, stop, backup, candidate verification and reopen.
Each host/operator mutation and each node reopen rechecks them. Saved JSON is
never substituted for a live gate. The bridge does not provision or clear gates.

The local cluster check additionally pins the prepared full `node-odx-gpu`
container ID/image/name, exact Compose ownership and private loopback port 13001.
It reads the existing `LOCAL_NODE_TOKEN` from the native environment file in
memory, verifies actual NodeODM `/info` and an empty `/task/list`, and reuses the
fixed `ComposeNode` upload-directory and child-process inspectors. Queue count
alone is insufficient. The two fixed HTTP reads share one absolute ten-second
deadline across connect, request write, headers and body using nonblocking
sockets; trickle responses cannot extend it. Headers/body and JSON are bounded,
redirects are rejected, and the socket closes on every outcome without reader
threads. Main's separately configured holder must use the reviewed
`MainEngineProbe` with the same expected main image supplied to this operation;
the root relay controls that composition. Neither side deletes retained tasks.

The root SSH launcher and bridge are separate components; this updater neither
chooses credentials nor relaxes host-key verification. Direct library use must
retain the same fixed global operation lock and supply the live gate factory;
test factories are synthetic boundaries only.

## Census and update ordering

Prepare hashes the full public job history and captures every SQLite schema
object, table column and row digest, including private job fields and empty
receipt/claim/outbox tables. It also hashes each allowed state file. Symlinks,
hardlinks, unexpected files/tables/indexes/triggers/views, active jobs, native
rows, reference rows and owned-schedule rows fail closed. No job/receipt contents
are exported. Only system node rows are retained privately to prove exact CAS
transitions.

The old API lacks durable caller-correlated scheduling. Before each single
legacy schedule POST the executor FULL-saves the exact payload. A lost response
is held forever for explicit inspection: it never adopts a matching target,
reposts, or infers ownership. Successful ACKs save the exact plan and revision.
Only the prepared node row plus this exact acknowledged drain transition is
allowed to differ from the original census. Every other private field, table,
schema object and original file remains bound.

After both exact plans are draining, a full new census is frozen. The exact old
container is stopped with Docker's graceful `--time -1`; the CLI observation is
bounded to 60 seconds. An observation timeout is unknown and held, never an
automatic stop replay. There must then
be zero running volume owners, and a read-only network-none helper must match
the stopped volume to that post-drain census before backup or candidate creation.
Logical SQLite contents are compared exactly; raw SQLite file hashes are retained
as evidence, while expected page-layout differences do not substitute for logical
proof. Non-database file contents/modes are compared exactly.

The offline backup is newly created with numeric UID/GID 10001 and mode 0700.
The helper copies and hashes all bytes, rejects links, fsyncs files/directories,
runs SQLite quick-check, and compares full source and copied censuses including
raw database bytes. It does not restore, remove or rewrite original data.

Only the exact predecessor is retained under this operation's unique name. The
candidate is created once with an operation label and full immutable image ID.
Unknown create results may be recovered only from exact label/image/name/full-ID
readback; activation never repeats create/start/stop/rename. Every uncertain
response leaves a checkpoint for a separate explicit guarded operation.

Candidate verification requires sole volume ownership, unchanged authority,
history, full stopped-state logical census, native health, actual TLS role
separation, and the new operator-owned lookup API. The new API probe is only a
GET for a missing receipt: exact authenticated HTTP 404
`{"error":"owned_schedule_not_found"}` is required. Generic 404, 401, timeout,
malformed body or server error is not evidence of support. No plan is created
by this probe.

Immediately before the first reopen, the full idle/workload census is fenced
again. Each reopen checks both live gates and its exact own plan/revision/target,
including a fresh gate check after the readback and before terminal completion.
A durable cancel intent is saved before POST; a lost ACK can be resolved
only by the exact active state/revision/plan/target readback. From the first cancel
intent onward, rollback to the old coordinator is permanently refused, including
when a response is unknown or only one node has reopened.
After that intent, legitimate new admissions are preserved and do not have to
match the old idle baseline for the remaining exact plan to reopen. Gate checks
are fresh point-in-time proofs; loss after a final check remains a failed root
relay operation even if local terminal evidence was already written.

## Rollback boundary

Rollback is explicit, never automatic on activation failure. It can stop only
this operation's exact candidate and restore only the recorded reference
predecessor. Online proof must match the full drained census before stopping a
running candidate. A second stopped-volume census fences admissions that arrive
while shutdown drains. Any new work, private-field change, receipt, unknown
schema/progress, native metadata, or unverifiable census blocks downgrade.

The only additive schema exception is the exact reviewed empty
`owned_maintenance_schedules` table and its implicit primary-key index. Nonempty
rows, extra columns/indexes/triggers, or any other addition are refused. If a
stopped-volume fence fails, the executor may restart only the exact unchanged
candidate, verify its health and retain nodes fenced. It never starts the old
binary over incompatible state or restores the snapshot over admitted records.
Candidate retention additionally requires its canonical original name and zero
other running volume owners before a start. A lost old-start ACK is reconciled
only from the saved exact start intent plus the healthy, sole, exact restored
predecessor and matching online census; it does not start the failed-name
candidate beside the old coordinator.
Lost or unsafe early outcomes can require inspection rather than automatic
recovery; unknown legacy schedule outcomes and partially reopened operations
cannot use rollback.

## Source verification

`tests.test_reference_coordinator_update` uses real temporary SQLite and local
coordinator HTTP with fake Docker/host transports. It covers reference recipe
and environment-default preservation, full private-content and exact-node
censuses, byte-identical backup copying, late admissions, exact API probing,
lost schedule/create/stop/rename/start/cancel responses, foreign identity refusal,
gate loss, TLS-preserving rollback, and partial-reopen downgrade refusal. Bridge
process/OS gate tests are maintained separately. No live host/container, image
pull, coordinator cutover, real job or NAS action occurs in these tests.
