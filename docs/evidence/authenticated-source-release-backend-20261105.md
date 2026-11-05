# Authenticated source release backend candidate

Disabled by default. No NAS/host/container deployment changes were made. All
physical removals in qualification used private task-owned temporary synthetic
files. The shared wire contract/vectors are `e2f6af6`; its raw fixture SHA-256 is
`1e59b47a50065f8aeae1d8c63b16ca0a57acbb38919fb5a111c04e479e5cd365`.

The candidate implements separately pinned release-role/principal settings,
protected owner-scoped submit/status routes, canonical signed intents, exact
original source/derived/copy/worker-observation binding, conservative 24-hour
monotonic policy, durable per-job fence/drain, exact Linux descriptor-relative
NOREPLACE quarantine/unlink/fsync/tombstone recovery, stable terminal receipt,
and full read-only restart custody verification. Failure/cancel/expiry never
authorize cleanup. Raw GCP/permanent source closure and all results/Viewer
originals remain protected. Historical acknowledgements survive later custody
holds without granting new effects.

Filesystem effects run outside Queue SQL/lifecycle locks. Each effect first
commits its exact intent under current authority, then executes under the
existing process lease and release pin. A concurrent hold cannot retroactively
cancel an already-issued effect; it stops the next unissued effect. Unknown
outcomes retain exact evidence and never mint success.

Cleanup uses one separately joined Runtime thread under the same Store/Queue
authority, allowing Viewer result delivery to continue while NAS I/O blocks.
Shutdown refuses to release authority or stop unrelated HTTP/delivery while the
cleanup thread still owns blocked I/O. The disabled feature starts no cleanup
thread and does not qualify/access NAS through its saved release settings.

Core synthetic fault qualification: **81/81 passed, no skips, 28.730 seconds,
Python 3.13.17**, task-owned bounded runner (1 CPU / 1 GiB, network none, read-only
code mount) removed after completion. Modules:

```
tests.test_source_release_backend tests.test_source_release_contract
tests.test_source_release_metadata tests.test_reference_source_operations
tests.test_reference_source_readers tests.test_coordinator_freeze
tests.test_source_spool tests.test_reference_source_intake
```

Coverage includes actual fresh processes exiting before/after rename, after
unlink, after removed-journal commit and after terminal receipt commit; exact
original lease retirement/restart/tombstone audit; unchanged ACK replay and an
unrelated next job claim/completion; actual TreeDar-only V2 coordinator identity
and release (three-byte transport fixture, not LiDAR geometry); active read/write
and maintenance gates; blocked rename/unlink/directory-fsync with unrelated DB
progress and refused shutdown; source hold after effect intent; raw GCP corruption
or disappearance after eligible removals withholding terminal ACK; independent
synthetic Viewer-original copies unchanged; and exact replay at exhausted payload
quota using the independent bounded coordinator CONTROL reserve.

Pure Windows contract/closed-size tests also passed: **4/4 in 0.075 seconds**.
The maximum physical JSON proof checks uint64 device/inode, meaningful POSIX
mode bits, uint32 UID, nonnegative int64 timestamps and safe-integer sizes. Both
prepared and future completed effect outcomes fit 1 KiB before an effect; each
object reserves 1,280 bytes, plus exact request/closure bytes and a separate
4,096-byte terminal/header reserve. A 10,000-photo / 50 GB **metadata-only**
projection fits the intended default payload budget; no 50 GB physical test is
claimed.

Followup synthetic qualification: **89/89 passed, no skips, 38.493 seconds,
Python 3.13.17**, using the same 1 CPU / 1 GiB / network-none / read-only-code
runner, verified removed after completion. It adds
`tests.test_source_release_namespace` to the module list above. Pure Windows
contract/size tests passed **5/5 in 0.058 seconds**.

The followup seals one initial namespace and ORIGINAL per-target inode inventory
under the permanent per-job fence and drained source pin. Each target then
rechecks its parents, original inode/content and NOREPLACE destination without
rescanning every other target. Full namespace and permanent-file verification
repeat at restart and before the terminal receipt. Immutable signed closures
validate once per authority epoch/checkpoint, with bounded scalar authority and
state comparisons before every effect; restart/terminal still revalidate the
full closure. Exact encoded per-object prepared and completed evidence remains
bounded before every journal update and effect. One real coordinator operation
admits at most 16 sequential turns, amortizing CONTROL admission without a quota
bypass; maintenance intent stops the next unissued effect or object and retires
the actual operation/source pin. Same-epoch release pins are reused only after
exact durable retirement and absence of a live or uncertain in-memory owner.

A separate joined metadata-only timer loop checkpoints all waiting releases in
one bounded batch, independently of the single physical I/O thread. The test
advances 151 waiting release headers through the full 24-hour minimum while the
I/O mutex stays held. The existing 300-second maximum credit gap, restart epoch
reset and clock rollback refusal remain conservative; downtime is not credited.
The timer fixture proves scheduling arithmetic, not 151 accepted physical jobs.

Additional fault checks cover a real process exiting after the unlinking phase
commit but BEFORE its unlink intent: disappearance of that quarantine file is
held and never treated as issued deletion. A retained exact unlink intent still
supports recovery after an actual unlink. The actual maintenance gate test
commits intent during rename, verifies no later unlink/object starts, drains and
seals with zero active pins, then resumes the same journal to completion.

The physical scale fixture creates **10,000 owned one-byte files**, seals their
inventory with fewer than N+256 stat calls and less than 64 MiB traced peak
allocation, and removes four representative targets with fewer than N+1024
total stat calls. A new unknown file is preserved and blocks the full audit.
This proves bounded inventory/per-target work; it does not claim a 50 GB physical
transfer or full 10,000-object SQL deletion throughput benchmark.

This remains a review checkpoint, not deployment or enablement evidence.
Frozen startup/updater census must explicitly
recognize the three source-release journal schemas before automated updates of
an installation containing these journals; unknown schemas remain blocked.
Viewer's server-verified readiness inventory/signer/outbox/operational-hold
retirement and actual NAS filesystem qualification are separate gates.
