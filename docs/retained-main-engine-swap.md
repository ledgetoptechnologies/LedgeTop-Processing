# Retained-terminal main-node swap candidate

Status: SOURCE CANDIDATE, not native-qualified or installed. Read-only binding
checks were made on 2026-11-11 while client processing continued. No maintenance
schedule, production data walk, backup, stop, image transfer or replacement was
performed for this candidate. The qualified local child-server tests are a
separate proof; they do not qualify this host swap.

## Exact scope and bindings

`ops/retained_main_engine_swap.py` is one fixed-host operation. It imports the
unchanged small helper's pure comparison/inventory/lock functions; it does not
patch their globals or change that helper. Main's live CID is `a22ca003...`, base
image `a53446d5...`, target local image `d1b3a6af...`; complete immutable pins are
in source. The child retains its exact `ledgetop.retention.main-local-fixture`
label `607b560b94fee3c8ceb3f2bff78aacf6`. This label is provenance metadata, not
execution authority or a runtime resource limit. Source commit label is 984f8bc4.

Main differs from the small node: NodeODX `odx` 2.4.0 / engine 3.8.6, no photo
cap, null default command, root `0:0`, NVIDIA all GPUs, unlimited CPU/memory,
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
POSIX ownership/fsync and actual bind/Compose/GPU mechanics remain unqualified.
Synthetic passes do not establish real job delivery, crash/power-loss safety,
large-data backup duration, autonomous reboot recovery, or a completed goal.
