# Retained-terminal main-node swap candidate

Status: SOURCE CANDIDATE, not fully native-swap-qualified or installed. Read-only
binding checks were made on 2026-11-11 while client processing continued. The
target was subsequently cached and a bounded native CREATE/INSPECT-only fixture
passed on 2026-11-12, as recorded below. No live engine replacement, maintenance
schedule, production data walk, backup or stop was performed. The qualified local
child-server and native create-time tests do not qualify the full host swap.

## Exact scope and bindings

`ops/retained_main_engine_swap.py` is one fixed-host operation. It imports the
unchanged small helper's pure comparison/inventory/lock functions; it does not
patch their globals or change that helper. Main's live CID is `a22ca003...`, base
image `a53446d5...`, target local image `d1b3a6af...`; complete immutable pins are
in source. The child retains its exact `ledgetop.retention.main-local-fixture`
label `607b560b94fee3c8ceb3f2bff78aacf6`. This label is provenance metadata, not
execution authority or a runtime resource limit. Source commit label is 984f8bc4.

Main differs from the small node: NodeODX `odx` 2.4.0 / engine 3.8.6, no photo
cap, null default command, root `0:0`, existing `privileged: true`, NVIDIA all GPUs, unlimited CPU/memory,
one RW bind `/dockerfiles/nodeodm/data` -> `/var/www/data`, writable root/TMP,
private 13004 and dedicated public ingress 3004. Only the image and bare
`--disable_scheduled_cleanup` are added. Main uses Docker 29.6.2 and Compose
5.3.1; the earlier small-node qualification used a different Compose version.

Current engine Compose is the exact two-file combination under
`/home/bkoltz/.local/state/processing-engine-update-20261109`:
`rollback-compose.json` plus `main-image.override.json`. Both are UID1000,
mode600, single-link files, have no interpolation, and the engine has no
Compose environment-file label. The old bootstrap role Compose under
`/home/bkoltz/.local/share/ledgetop-processing-node/compose.json` still points to
an older b9d9a1 image. A successful operation durably updates both forward entry
points (override and bootstrap role) to target image + bare flag, preserving
UID/GID/mode and all other service fields. The historical rollback file stays
byte-for-byte unchanged. No processing systemd unit was found; Docker's existing
unless-stopped restart behavior applies. This is not reboot automation proof.

The existing main maintenance configuration names the expected loopback
coordinator `http://127.0.0.1:18080`; an authenticated read through the existing
private tunnel succeeded. Do not create or replace a tunnel silently. Prepare
uses a fresh root-private copy of the existing worker/operator credentials and
seals the coordinator authority, Docker daemon ID and source configurations.
Credentials and the full preparation/journal remain private.

## Admission and one-shot behavior

Default CLI action is read-only inspection. `--prepare` explicitly creates a new
0700 receipt, 0600 preparation and empty environment file, only after idle
admission. The old missing environment-file label is verified as missing; it is
not treated as an existing empty-file binding. Inspection resolves the
non-interpolated source with explicit `/dev/null`; recreation uses the new
root-owned private empty.env. The resulting new environment-file label must
match exactly.

Both inspection and apply refuse a busy main before data hashing, backups,
stopping or scheduling. Apply rechecks the engine and main claims before its
first journal/schedule. A race after that check is fenced by the real owned
maintenance CAS and transactional queue drain. Queued client jobs remain queued;
no task is cancelled or resubmitted. The helper never schedules an idle-window
plan while main is already processing. Choose the idle window after the current
authorized client queue; this document is not approval to interrupt it.

After an independently reviewed preparation digest, apply records intent before
each mutation: reserve the exact main plan, close/drain only its dedicated
ingress, recheck engine/claims, stop cleanly, verify backups, recreate only
nodeodx, verify contents/configuration, persist forward pins, reopen ingress,
then finish maintenance. The target must already be cached by exact ID. No
pull/build, generic engine update, cleanup, old-image boot or automatic replay is
included. On uncertain failure, only the proven owned plan can be held/closed;
otherwise no ingress mutation is attempted.

The bind root's real path, device/inode, numeric owner and mode are sealed.
Active writable Docker aliases and nested host mounts are rejected. Admission
uses no CPU-utilization guess: taskQueueCount must be zero, every task must be
terminal, server processes must fit the reviewed Node/bash/init allowlist, and
TMP may contain only the exact shipped placeholder. Backups include full rootfs,
ACL/xattr/numeric-owner data tar with GNU tar comparison, and archived TMP.
Retained files/links/directories and numeric metadata are compared, task index
spelling may change only alongside an unchanged semantic inventory, and index
kind/owner/mode must remain unchanged. Bind-root and TMP-root metadata are also
verified. Backup copies remain private for operator recovery.

The 32 GiB base-rootfs allowance plus writable layer, retained data copy and
16 GiB reserve is conservative planning, not measured export qualification.
Docker image Size is not accepted as the rootfs tar size. Export beyond the
allowance holds before recreation. Native qualification must establish real
export size/capacity and bounded duration for this exact base.

## Required native fixture before live use

This is a plan, not a passing test or an executable deployment request.
The later bounded fixture establishes only the native create-time profile and
comparison checks in step 3; the remaining full-fixture steps are still required.

1. Freeze helper/tests by commit and LF Git archive; verify every packaged blob.
   Pin the actual Compose 5.3.1 binary and Docker version. Preserve the existing
   small helper and current client queue. Stage exact base/target images only in
   an approved cache window; no production replace or bulk data hashing.
2. Build a new, explicitly owned, synthetic fixture: one real data **bind**,
   writable root/TMP, three expired terminal statuses 30/40/50, source/result
   sentinels, nondefault numeric file/directory/TMP metadata, ACL/xattr sentinel,
   old two-file Compose with no env label, and stale bootstrap forward pin.
   Use isolated credentials/queue and only fixture ingress/network/resources.
   Declare every binding override; do not alter production helper constants or
   weaken its guards. No client mounts, uploads, jobs or outputs are involved.
3. Exercise the real backup/compare/rootfs export, recreation, exact host/app
   configuration comparison, tmp restoration, atomic ingress close/open with
   old workers drained, forward-pin preservation and owned finish. Test the
   new root-private environment label. Preserve all synthetic task/file hashes
   across startup and one clean restart. The actual main GPU/unlimited profile
   still needs exact resolved/create-time configuration proof: a CPU-limited
   fixture alone is not proof of that profile.
4. Cover failures at backup, candidate create/inspect/start, metadata comparison,
   first/second forward-pin write, ingress reopen and finish. Keep private
   forensic journals; demonstrate no old-image auto-start, no replay, no
   other-node ingress closure and no completed-data deletion. An uncertain
   state is held for inspection, not reset to an old image.
5. Reconcile every owned fixture container, network, bind root and subprocess
   by nonce/ID; retain small safe receipts and remove only verified fixture bulk
   data. Verify no task-owned process/container remains. No prune or shared
   service cleanup.
6. Only after independent source/native review, re-inspect the idle real main
   host, available space, full current terminal inventory, actual credentials,
   tunnel/authority, bindings and target cache. Seal a new live preparation and
   review its digest. Never reuse fixture receipts or a consumed operation.

## Unit evidence and remaining limits

Windows bounded unittest scope ran 45 cases: 39 passed, 6 explicit POSIX/root
skips (21 main cases plus the unchanged small-helper cases). Main-specific tests
include busy-before-schedule/backup, exact command/resource preservation, owned
main receipt binding, real Queue main drain/claim gap, permanent hold behavior,
metadata-sensitive task index, and exact new environment/fixture-label rules.
The exact a0d946d5 source then passed all 45 cases on cached Python 3.13 image
`ae14d4ce...` as Linux root, zero skips/failures. This executed the real numeric
ownership, private-umask, directory-fsync and dual-forward-pin metadata tests.
All 80 archived files matched frozen LF Git blobs. The isolated runner had one
CPU, 512 MiB memory, 128 MiB private tmpfs and 64 PIDs, no network/GPU/Docker
socket/host data, read-only root/source and bytecode writes disabled. Only CHOWN,
DAC_OVERRIDE and FOWNER were added to the otherwise dropped capabilities so
UID1000 metadata tests could execute inside its tmpfs. The container exited 0
without OOM and was removed, with absence verified. Safe hashes and limits are
in `retained-main-engine-swap-source-20261111.json`.

This is Linux **unit** proof. At this checkpoint no live apply or native fixture
had run. The later CREATE/INSPECT-only evidence below qualifies the exact native
create-time GPU/profile comparisons, not actual retained-data backup, writable
TMP restoration, ingress, export duration/capacity or live host integration.
Synthetic passes do not establish real job delivery, crash/power-loss safety,
large-data backup duration, autonomous reboot recovery, or a completed goal.

## Fixed-profile correction and actual busy refusal — 15:29 UTC

An initial default inspection safely held on a source mistake: the fixed main
GPU profile expected Options `{}` while Docker actually reports `null`. No
mutation or busy-admission proof was claimed for that attempt. Source 662a32e2
corrects only that fixed expectation, with a negative `{}` case; shared
HostConfig equivalence and every other GPU/resource field remain unchanged.

The corrected source passed all 46 main/small Linux-root unit cases, zero skips
or failures, under the same isolated limits and ownership cleanup. One new
default inspection on the real main host then exited 2 with `main owns unfinished
claim` before any data walk or target-image inspection. No `--prepare` or
`--apply` action was used, and no maintenance schedule, receipt, backup, container
or configuration mutation occurred. Main's exact container/image/PID, active
revision43/plan, broker/native and original running engine identities were
unchanged in before/after read-only API and Docker projections. Generation1 was
derived from the installed request-identity protocol, not directly read from
a database field.

The existing UID1000 maintenance credential file stayed unchanged. One separately
authorized root-private, O_EXCL0600 task-owned copy was verified in memory and
removed after the helper and its process group exited. No credential contents
or credential digest were printed or retained. Only source and safe evidence
remain in the private inspection root. This is actual **negative admission**
proof while main is busy, not native replacement or positive idle admission.

## Native CREATE/INSPECT-only qualification - 2026-11-12

Reviewed runner `930d5e4f639ed047459e6a11a7e5b6eb951f1cfb120de4683a5e2245ae1a28de`
passed once on actual Docker 29.6.2/Compose 5.3.1, using unchanged helper
`a8208f269269e4354b56ef8e6ccbcaced587ffd4d7bf9b9281db2a988b479ae6`.
See the redacted [machine-readable evidence](evidence/main-retention-native-create-inspect-20261112.json).
The private combined receipt has SHA256
`72e28029fc40f0dfb80b7e7bdca869a12fbc77309d12bf29700e45041d00eb9d`.

Caching the exact target transferred 5,667,816,960 stream bytes in 573.840 seconds.
The guarded tag was absent before load and identified the exact target afterward;
save/load children and pump thread exited. The live engine's full role remained
unchanged before/after caching, after the fixture and at independent readback.
Daemon-reported image size was not substituted for measured transfer or rootfs
export size. The cache is not a runtime installation.

The first owned attempt safely refused the existing `privileged: true` profile
before creating a fixture directory or container. Its consumed intent and evidence
remain held. A fresh reviewed nonce pinned canonical and live privilege to Boolean
`true`; it did not strip, lower or change the live setting.

Two sequential owned containers were created but **never started**: both had
status `created`, PID 0 and zero `StartedAt`. No engine or GPU workload ran. They
used only tiny synthetic data/TMP binds, network `none` and no published ports,
explicit host device mappings, Docker socket or client mounts. Both retained root `0:0`,
existing privilege, CPU/memory 0 and NVIDIA all-GPU metadata (Driver `nvidia`,
Count -1, Capabilities `[["gpu"]]`, DeviceIDs/Options `null`). The null default
command changed only to bare `--disable_scheduled_cleanup` in the child.

Actual source comparison functions passed, including **exact canonical HostConfig
equality**, application configuration, literal Mounts equality and the
missing-to-explicit private environment-file label. Synthetic bind metadata was
unchanged. Both create responses were acknowledged and exact CIDs recorded; the
base fixture was replaced and the child removed. Independent readback found no
owned container or matching fixture Compose process. Live engine role,
Compose/ingress bytes and helper source stayed unchanged. No pull/build/start,
maintenance invocation or production-data cleanup occurred.

Sixteen pure guards passed on Windows and Linux, including lost-create-ACK
faults. These do not inject an actual Docker timeout: that failure retains unknown
Compose-plugin/daemon disposition and `cleanupUncertain`, even after empty
discovery or removal of a late owned CID. It is never replayed or reported as a
full process/future-create absence proof.

Still open before live use: native retained-bind backup/comparison and ACL/xattr
preservation; measured rootfs export size, duration and capacity; writable-layer
and TMP restoration with numeric metadata; actual server startup/restart and
retained terminal tasks; GPU execution; real ingress/drain/maintenance failure
integration; positive idle admission and a new independently reviewed live
preparation. This is not a full swap, rollback, live-installation or unattended
update qualification. The normal CREATE/INSPECT result does not close actual
daemon-timeout or full native failure-integration gates.
