# Early owned drain: bounded Linux qualification

On November 5 local time (2026-11-06 01:56–01:57 UTC), the exact frozen source
`f589d8193f323a84e24874c5396f30ad16d793b1` passed **16/16 Linux tests**, no skips,
in **3.487 seconds**:

```text
tests.test_pending_maintenance_qualification
tests.test_update_early_drain
```

The source archive SHA-256 was
`3b79f22094c7b5c9283eeaa2cc9b6fea354a9ad09e8c8b64d5cdea386109591c`.
The retained controller-side archive is under
`C:/Users/fstor/AppData/Local/Temp/ltds-early-drain-f589-0f28b73b216a4f7dbae4af6c16cb3213`.
The cached test image was
`sha256:378559e42456017b965d1e2e38b56eb627274734aeb317a758d84c0bc99bb5ab`.
No dependencies were installed or downloaded.

One owned `--rm` runner, `ltds-early-drain-f589-diag`, used the label
`ltds.task=maintenance-early-drain-f589`, one CPU, 768 MiB memory/swap limit,
UID/GID 10001, read-only root and source, network none, bounded private `/tmp`,
96 PIDs, no capabilities and no Docker socket. Execution returned synchronously
with exit 0; there is no continuing test handle. Both exact-name and exact-label
`docker ps -a` inventories were empty afterward. No volumes were created.
Unowned/shared containers were left alone.

The 11 real Queue tests cover same-plan qualification, exact after-job claim
retention, no new node claims, version-pending refusal, current revision/authority/
target/hash comparisons, concurrent bind/replay, changed boundaries, cancelled
plans, a finished job not being replaced by another job, and unchanged legacy
null-version/receipt semantics. The five real localhost HTTP/update pipeline
tests use actual updater/adapter/coordinator/managed code with synthetic Docker
and engine transports. They exercise drain before pulling/qualifying, existing
job completion, same-plan replacement/reopen, lost drain/bind ACK lookup without
repeated POST, qualifier failure, and cancellation refusal.

This is source-contract qualification. It is not installed host, native engine,
systemd, real container replacement, reference-worker handover or unattended timer
acceptance. The broader affected legacy updater and dynamic-host configuration
suites have not yet been rerun on this exact revision. Four pure renderer tests
passed natively; the Windows Queue attempt stopped before behavior because its
Python environment lacks `rfc8785`, without installing or changing dependencies.
No live engine, NAS, service, timer or client data was changed.
