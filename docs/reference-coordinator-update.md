# Reference-to-reference coordinator updater

`ops/update_reference_coordinator.py` is a separate source executor for updating
the existing reference coordinator. It does not reuse the native predecessor
selection or change the historical installer/frozen operation artifacts. Import
does nothing. Source `890cd26` passed independent review, the frozen 894-case
Linux run and the two-PC gate preflight in
`docs/evidence/coordinator-gate-preflight-20261028.json`. Retargeting to the
installed `693cb7e` predecessor, retained Viewer tasks and the pinned additive
migration are not yet reviewed against the live host, so `ACTIVATION_REVIEWED`
is false: activation and rollback are refused; only prepare runs, and prepare
itself holds until `REVIEWED_PILOT_JOURNALS_SHA256` is pinned (see Pilot journals).

## Exact operation binding

The pinned predecessor is container
`525ca6e24ad3b9e57e2a3be36567471c10745a4aaa8ebc03c7f2d2f215dfb977`, image
`sha256:7fae9385fb5ca829783fe05eef2fdb1983abf4d491b2b9f795e55b617a5a0004`,
source `693cb7eca32295da959a4f42399f317577ef8a8a`, with exactly 20 terminal jobs
and no tmpfs. The candidate is created with only a private 64 MiB `/tmp` tmpfs
added. The executor revalidates these identities. The much older native container beginning `6300` is never
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

The local cluster check additionally pins the prepared full ID, image and name
of the adopted `lt-migration-1120ae14a8988e84513a4a8a46b78741-candidate` container
(a container name; its Compose service is `nodeodm`), exact Compose ownership and
private loopback port 13001.
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
hardlinks, unexpected files/tables/indexes/triggers/views, active jobs, reference
rows and reservations fail closed. Existing `owned_maintenance_schedules` rows are
kept: each must validate against its authority, node and digests, and is hashed
with every other row. Viewer tasks are kept
only when `phase='complete' AND stage IN ('succeeded','failed','cancelled')`
(`TERMINAL_VIEWER_TASK`, shared by install, update, backup and rollback); their
`viewer_files`/`viewer_incarnations`/`viewer_option_evidence` rows must not be
orphaned, and every file under `viewer-tasks/` (client inputs and results, up to
20 GiB each) is hashed by path, mode, size and SHA-256. No job/receipt contents
are exported. Only system node rows are retained privately to prove exact CAS
transitions.

The existing reference schema includes `reference_terminal_acceptances`, created
by the real `TerminalService`, including its three UNIQUE indexes. That known
table is included in the full schema/row census and must be empty, like every
other `reference_*` table. A terminal receipt blocks both fresh preparation and
downgrade; this allowance does not permit new receipt history or unknown schema.
The first actual prepare exposed its omission from the original synthetic
fixture and held before draining. That held checkpoint is retained unchanged;
the correction requires a fresh reviewed source/image/operation rather than
resetting or reinterpreting the held operation.

The updater schedules through the legacy `/schedule` route, which has no durable
caller correlation; it does not use the `schedule-owned` route that 693cb7e also
has. Before each single
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
proof. Non-database file contents/modes are compared exactly. Every volume
census runs under `CENSUS_TIMEOUT`, the reviewed `BACKUP_TIMEOUT` budget, because
it reads the same bytes and, online, at 0.5 CPU; the small TLS/configuration reads
keep the 60-second default. A timeout after stop leaves `needs_inspection`.

Admission is not fenced between the drained census and stop: the predecessor has
`ADMISSION_DRAIN_QUALIFIED = False`. A job admitted in that window fails the
stopped-volume census, so the operation ends in `needs_inspection` with the old
coordinator stopped, no candidate created and nothing running; rollback's offline
census refuses the same job. Automatic restart of the predecessor is not safe
(Codex audit) and is not attempted. The queued job stays on the volume for the
operator.

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
separation, and a working operator lookup. The lookup probe is only a GET for a
missing receipt: exact authenticated HTTP 404
`{"error":"owned_schedule_not_found"}` is required. Generic 404, 401, timeout,
malformed body or server error fails it. The 693cb7e predecessor already has this
route, so the probe proves that the running coordinator answers operator-authenticated
lookups, not that it is the candidate; candidate identity comes from the exact
container, image and sole-owner checks. No plan is created by this probe.

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

## Pilot journals

The live state root also holds `pilot-journals/`: operator pilot records with no
writer in this source. `volume_census` allows only that directory, its depth-1
directories and their depth-2 regular files, owned by the census UID, not
symlinks, with `nlink` 1. Directories must be mode 0700 and files must have no
group, other or special bits. Elsewhere the census only records modes; this tree
must meet them so the backup, under its umask, reproduces it. At most 64 files,
64 KiB each and
1 MiB in total. Anything else raises, so the census emits no digest for such a tree.

Bytes are opaque and never parsed. Files join the ordinary `files` census, and
`pilotJournals` adds a digest of the manifest sorted by relative path: rows of path,
mode, size and SHA-256 (size and SHA-256 null for directories, so an empty directory
or a directory mode is bound too), with file and byte counts. The same function runs
online, offline and inside the backup helper, and the generic backup copies these
files byte for byte, so `compatible()` has no allowance: every comparison, including
rollback's, is exact, and a changed, added or removed file is `full_census_changed`.
Rollback reuses the volume and restores nothing.

`REVIEWED_PILOT_JOURNALS_SHA256` is `None` in source. While it is `None`, prepare
holds with `reviewed_pilot_journals_required`; a different pinned digest holds with
`reviewed_pilot_journals_changed`, and activation rechecks the pin. A prepare hold
keeps the observed manifest in the private checkpoint (phase `pilot_journals_held`)
and leaves no baseline. A human reviewer pins the value from the live read-only
census. An absent directory is the digest of the empty manifest and is accepted only
if the pin is that digest. No live value is recorded here.

Before pinning, a one-time pre-live audit is required, because a hash cannot show a
pilot finished: no pilot supervisor is running, each `child-start` has a
`child-exit`, each `.cancel-intent` has a resolved node outcome, and the writer is
identified from operator history.

## Rollback boundary

Rollback is explicit, never automatic on activation failure. It can stop only
this operation's exact candidate and restore only the recorded reference
predecessor. Online proof must match the full drained census before stopping a
running candidate. A second stopped-volume census fences admissions that arrive
while shutdown drains. Any new work, private-field change, receipt, unknown
schema/progress, native metadata, or unverifiable census blocks downgrade.

The additive schema exceptions are the exact reviewed `owned_maintenance_schedules`
table and its implicit primary-key index, and the pinned candidate start-up
migration (`MIGRATION_TABLES`/`MIGRATION_COLUMNS`): empty
`reference_viewer_receipts`, `reference_viewer_transfers` (with
`verified_full_coverage`), `reference_released_claims`,
`viewer_full_option_evidence`, `viewer_guarded_dispatch_checkpoint` and
`viewer_guarded_dispatch_history`, plus
`viewer_tasks.full_option_evidence_required`, `viewer_tasks.node_guarded` and
`reference_jobs.viewer_receipt_required`. An owned table already in the baseline
keeps its rows, which are validated and hashed and must match exactly; only an
owned table absent from the baseline counts as an addition, and it must be empty. Only
items absent from the drained baseline count as additions, and a table must lack
all of its pinned columns. Column DDL is replayed in memory over the baseline
table DDL and must match exactly. New tables must be empty. Because retained
terminal `viewer_tasks` rows are nonzero, the census also emits, for each table
with pinned columns, a hash of its rows projected onto the non-pinned columns
and a NULL-aware count (`IS NOT default`) of rows whose pinned column differs
from its default (0, 0 and NULL). Row contents never leave the helper. A column
addition is accepted only when the observed projection equals the baseline
full-row hash, counts match and every non-default count is zero. The
predecessor names every INSERT column on those tables, so the census accepts
this migration for rollback (not the Viewer reservation tables). Nonempty rows,
extra columns/indexes/triggers, or any other addition are refused.

Legacy policy: rows the predecessor inserts on migrated state omit the pinned
columns. `viewer_tasks` rows get 0/0 (no full option evidence, not node
guarded). `reference_jobs.viewer_receipt_required` is NULL, which the candidate
treats as ambiguous: acceptance is refused ("legacy Viewer acceptance policy
requires reconciliation"), the job is never queued for Viewer transfer, and the
transfer worker holds with `viewer_acceptance_policy_requires_reconciliation`.
Any such row also fails a later update census.

The predecessor's loader accepts runtime configuration `schemaVersion` 1-3 and
the candidate's 1-5, but version 3 requires staged principals that the
predecessor's runtime refuses: the real predecessor image exits on a v3 or v4
config. Only versions 1 and 2 are accepted. Prepare records the exact `runtime.json` bytes, SHA-256 and
version (`predecessorConfig`) and refuses a version the predecessor cannot
read. Rollback reads it again before any mutation and before starting the
predecessor, and refuses unless it is byte-identical to the capture. These are
schema and configuration rules only; the real predecessor image on migrated
state still needs qualification before the gates open. If a
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
censuses, the pilot-journals tree, pin and census timeouts, byte-identical backup
copying, late admissions, exact API probing,
lost schedule/create/stop/rename/start/cancel responses, foreign identity refusal,
gate loss, TLS-preserving rollback, and partial-reopen downgrade refusal. Bridge
process/OS gate tests are maintained separately. No live host/container, image
pull, coordinator cutover, real job or NAS action occurs in these tests.
