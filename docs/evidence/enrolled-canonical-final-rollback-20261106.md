# Canonical final image: actual rollback and post-restart audit passed

Separate fresh operation `d9168b69-ba7d-4771-8457-12d707e79869` passed all
**30 actual Docker journal phases** through `rolled-back-rebaseline-required`,
with admission false, pending null, and the mandatory post-original-restart
stopped-writer custody audit accepted. No forward operation was rolled back
after admission, and no failed operation was repaired or reused.

## Exact reviewed inputs

- Actual final image:
  `sha256:7adf6e6ece0b1a5a6246cc5af4d8707d146b577b58b9df3ac6d27148032a20b4`.
- Packaged source: `63c8049b74e4e179e812f67b489f8f67f7d9d8b1`.
- Controller: `ae8e259ddbca9e24f62090ff190d291e3e187e33`, frozen archive SHA256
  `f99da7dd67cc27dfa1c839d69f6c1370f9e0cdeee288407484d4b09d79b154a5`.
- 744-file source manifest:
  `fbec5a44f01fff500112d9bf35b988b15228ba6a225738bd1cf2ba916e672ea8`.
- All 99 Processing files exactly match canonical LF image context, footprint
  `654fceb89d4dc114434053f14bbb503a53763401f458e1a628e2724979e9989d`.
- Genuine predecessor:
  `sha256:7fae9385fb5ca829783fe05eef2fdb1983abf4d491b2b9f795e55b617a5a0004`,
  source `693cb7eca32295da959a4f42399f317577ef8a8a`.
- Native CLI SHA256: `6c71cfe71faff949a602946a4df4799b563232e79d0b8b6fd343d0ef081c9621`.
- Reviewed one-shot launcher SHA256:
  `ee7d9644a5097e4c6e4903fb283de7ad46834b4b44c8e03d8e494ab0095ea728`.

Pins, all frozen bytes and absent marker were verified before one-shot dispatch
under the separately granted quiet slot. Fresh running inventory contained only
the shared builder. No build, pull, retry, authority repair or alternate image
was substituted. The canonical archive explicitly disables Git autocrlf export.

## Authoritative result

Outer `49530992ae2e5650db1aed4f84fad0d7de299d348a843e9d86e755388f50c922`
started `2026-11-06T03:08:09.511625365Z`, finished `03:11:29.297001142Z`:
**199.785 seconds**, exit0/PID0/no OOM. Handle12664 was tracked to terminal.
One CPU/768 MiB, UID10001, no host ports, immutable packaged app, frozen source,
private artifacts and unchanged 900-second outer cap.

The production journal completed 20 forward-prefix steps through final-ready,
then its 10 declared rollback steps. Actual dual kernel leases, original full
custody/TAR, quarantine Gate freeze, independently declared startup projections,
final readiness, candidate stop/name retention, original-name restoration,
original immutable image restart/health and restart policy were exercised.
Admission never opened, no SQLite backup was restored and no mutation was
retried. All 19 legacy table projections and retained files remained covered.

After that original restart and health, the qualification-only audit validated
the exact operation/plan-bound artifact intent BEFORE stopping anything. It
gracefully stopped only the synthetic original, acquired both original leases
using the existing holder with NEW start `03:11:19.536656392Z` inside NEW window
`03:11:19Z`–`03:11:20.622372Z`, obtained fresh actual socket/kernel/physical
boundary, ran unchanged strict `rollback-final` projection, retained its bound
receipt and verified holder release. No second original start occurred.

Accepted audit projection: 6,737 bytes, SHA256
`c81c3d281629bc5f9acadabf40d604befc1649992ebc2c3b5b21b69010e5fa02`.
Fresh boundary SHA256:
`767c6314afe01f21ef3d4e328beb4983c292ee7ad2431476b282a3108cf0a0b6`.
The result explicitly says qualification-only, original restart observed,
original stopped after restart, holder released, live SQLite audit false and
continuous availability false.

## Cold proof and cleanup boundary

Complete cold operation, sanitized result, controller logs, journal, audit
result/projection, baseline/TAR, config/plan/bundle and worker material retained
privately: 321 files/1,400,946 bytes, manifest SHA256
`82a4d60a6a0f4e7d73fc2548b1c683352fe74d6d1704d466bd91d219b2570042`.
Controller stdout SHA256:
`3a370536f54ba73d7badf0f311da4f4e8d50cb35abeb7bd0d3df86893f6ca64f`.
Cold readback checked journal30/pending-null/admission-false and the accepted
audit's bytes/hash/operation/19-table baseline binding. The original TAR was
independently revalidated: 202 files, 13 directories, 262,429 content bytes,
467,968 archive bytes, SHA256
`a309b7ff61f5567b6f582303dc0dda7a332cf741d8865e10e8a711a339a2fbf9`.
The TAR is retained evidence, never a restore or lock-authority source.

The explicitly authorized unchanged success-only inner cleanup ran after audit
success and worker closure. Inner task-label container and volume inventories
were empty. Root independently reviewed the result, accepted audit and all 321
cold files before exact outer retirement. Fresh full-image/operation/PID0/exit0/
no-OOM checks preceded removal of only the stopped outer. A new closed FULL
container inventory independently proved zero named-volume or bind-parent/child
alias users before the single owned artifact volume was removed without force.
Both inner and outer task-label container/volume inventories were empty. The
complete Windows private/cold evidence remains unchanged; no own runner,
container or volume remains active. All unknown/failure paths retain resources
until separately reviewed; no further tests/build/deployment were started.

Together with the separate canonical final-image forward, this proves those
local component paths for the exact image. Explicit fixture limitations remain:
Desktop mount/enrollment identity seams, synthetic original dataset and native
engine/coordinator transport, real factory/TLS workers with local process
control. It is not installed SSH/systemd, NAS activation, client processing,
continuous availability or whole-production deployment acceptance.
