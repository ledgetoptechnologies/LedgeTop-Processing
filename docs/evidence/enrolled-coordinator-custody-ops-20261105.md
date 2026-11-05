# Concrete first-cutover custody IO module

`ops/enrolled_coordinator_custody_ops.py` supplies concrete custody IO for the
existing journal/driver. It is not another scheduler/controller. No effect runs
on import. It creates/starts/stops no container, acquires no copy lease, changes
no live permission, performs no extraction/SQLite restore, and introduces no
sudo/chown requirement. The authenticated host operator owns the new private
backup evidence; original UID/GID10001 remains in its verified TAR manifest.

## Exact injected seams

`CustodyOps` consumes the existing exact holder ID/settings, reviewed in-memory
source bytes/hashes for the predecessor/projection/startup-policy modules,
reviewed693 schema bytes, immutable policy bytes/hash, private key/hash, bounded
budgets, and the already private700 operation directory. Dependencies are the
concrete `DockerCLI`, callable `TrackedExec`, and driver-owned `boundary_check`.
The body never discovers a schema/authorization allowlist from candidate rows.

- `capture(intent,dispatch=None,context=None)` runs the real full predecessor
  census through a serialized stdin-only capsule, under fresh original holder
  boundaries before/after, then persists its full private keyed evidence.
- `backup(baseline_ref,intent,dispatch=...,context=None)` requires unchanged
  original custody, streams one fixed Docker archive RPC to a NEW O_EXCL600 TAR,
  fsyncs it, verifies every content byte/namespace/UID/GID/mode against the full
  baseline (including exact SQLite bytes), and requires another unchanged
  original census plus current original owner boundary before sealing receipt.
- `projection(baseline_ref,intent,phase,binding,...)` calls the reviewed exact
  policy verifier in the existing holder for stopped/frozen/rollback checks, or
  the exact live frozen-final container for final readiness. It binds genuine
  runtime custody and independent physical data to the same original authority.
- `inspect_backup` and `inspect_projection` consume existing completion artifacts
  and inspect exact prior tracked requests without dispatch; they never start
  another archive RPC or treat a partial TAR as an acknowledgement.
- `validate_backup_artifact` only revalidates accepted historical DATA after its
  old holder retired/restarted. It makes no remote call/authority claim and
  explicitly forbids restore. The phase driver must separately verify the
  accepted journal chain and a NEW current owner/physical/frozen boundary.

The callable tracked envelope is closed and requires `bodySucceeded=true`, exact
request/container/source identities, result, and independently verified original
PID/start retirement. Body completion is not misrepresented as an observed OS
exit status. A local Docker-exec CLI result/timeout alone proves no remote child
closure. The maintenance-owned tracker persists reservation before dispatch;
repeat calls inspect existing reservations and never mistake failed inspection
for freshness. Module requests are deterministic per logical intent, purpose
and distinct dispatch nonce. A completed result is historical evidence: every
before/after driver boundary must use a NEW read-only current challenge. Cached
tracking results are never current owner/writer checks.

Boundary proofs contain actual container identity/image/start/PID, original
physical binding, real held descriptor/kernel proof or authenticated final
runtime custody, and stopped writer records. Bare booleans are refused. The
concrete driver owns completeness/actual-source verification of those proofs,
including current frozen Gate plan/owner, known helper retirement and all
potential writers. This module binds and retains their exact private digests;
it cannot authenticate an arbitrary caller's invented callback object.

## Immutable TAR and failure behavior

The host operator's TAR remains private600 beneath its original700 directory.
No privileged extraction is needed. The streaming parser bounds bytes/duration,
512-byte headers, extension sizes, path depth/length, entry counts and content
chunks before allocation. It supports bounded ordinary PAX paths/times and GNU
long names, rejects links/devices/traversal/unknown extensions/duplicate paths,
and requires the complete original keyed file AND empty-directory inventories.
Original UID/GID/modes and every content HMAC match. TAR checksum/framing and
zero trailer are checked. It does not call Python's unbounded PAX extraction
path and never extracts any entry. Clean EOF plus owned archive CLI exit0 and
matching retained TAR hash/length are mandatory before receipt. This CLI exit
status is actual archive-client completion, distinct from tracked Python body
completion described above.

Quota/disk-floor checks include every retained partial artifact before a new
copy. Unknown partial TARs are retained without receipt, overwrite, automatic
retry or deletion. A source observer that is still live/unretired, changed owner,
unknown archive RPC, mismatched bytes/schema, or failed proof stays held. Private
metadata references are operation/intent/dispatch bound, O_EXCL, fsynced and
reread with descriptor identity/mode/owner/link checks; no copied database or TAR
is ever original lock authority.

Same-incarnation pending inspection requires old remote observer records still
available. After known helper retirement, tmpfs records can disappear. Later
control phases use their accepted historical artifact/journal chain PLUS new
current owner/projection challenges, never pretend the old scratch became a
fresh runtime proof.

## Qualification boundary

15/15 Linux focused tests passed, no skips, in2.234 seconds. Eight stream cases
cover complete bytes, SQLite corruption/missing/extra files, UID/GID/modes,
empty directories, links/traversal, checksums/truncation/byte bounds/PAX/long UTF8
names. Seven orchestration cases use REAL serialized readonly census children
against the full synthetic693 fixture, with a SYNTHETIC owner/CLI boundary. They
prove private TAR creation, full original bytes preserved, lost-reply request
reuse, unretired/writer rejection, unknown partial retention/no second cp,
quota/disk-floor refusal, reference corruption, same-incarnation readback and
historical data-only validation after simulated owner restart. The first run
caught a missing test-only Path import; corrected focused runs passed.

These tests do NOT qualify actual Docker archive RPC/PID1/TrackedExec integration
or the projection wrapper against an actual frozen final container. Those must
run through the shared task-owned local Docker harness before live cutover.
The previously integrated independent policy's normal-runtime four-phase tests
are separate evidence, not silently substituted for this concrete transport test.
No live PC/NAS/client dataset/container publication or activation occurred.

The bounded runner used1CPU/768MiB, readonly source/image, network-none,
UID/GID10001, --init and128MiB task tmpfs. It exited0 and auto-removed; the exact
`ltds.task=cutover-custody-ops` container inventory was empty. Real census child
PIDs were communicated/waited/reaped in the test fixture. Production subprocess
and remote retirement ownership remains the reviewed tracked transport seam.
