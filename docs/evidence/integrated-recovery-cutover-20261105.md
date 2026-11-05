# Integrated recovery and cutover checkpoints

Frozen Processing `b467c3b12e8f5c0a860ee357967d25cdce4e69eb`, full archive
SHA256 `e525d0082947448f47e17cb561c3c3e3b2a257916502b208ab07cafa4311b911`,
passed **442/442 Linux tests in 85.242 seconds**, no skips. Selection:
node gate, coordinator gate bridge, reference driver, scratch policy,
parent guard, combined preparation, worker recovery/hold, TreeDar engine,
source-grant renewal/mint I/O and joint driver fence. This covers the integrated
pre-first-byte transfer fence, persistent unstarted settlement and per-receipt
operator-resolution delivery bookkeeping. It does not prove all real restart
phases, installed workers, genuine TreeDar outputs or complete cutover.

Frozen Processing `99578a6896dfb53d11295f2582bfc4c4b4c039fa`, full archive
SHA256 `dadc337fe620cc19c3cd6e5e926a6e1d11927b3126d3365bb8ea7ac2e2c4afb5`,
passed **79/79 Linux tests in 3.969 seconds**, no skips:

```text
tests.test_enrolled_coordinator_transport
tests.test_enrolled_coordinator_boundary
tests.test_enrolled_coordinator_driver
tests.test_enrolled_writer_inventory
tests.test_enrolled_config_stage
tests.test_enrolled_coordinator_deployment
```

This combines the projected full Docker writer census with exclusive config
preparation and explicit CLI lifecycle. Read-only source, UID 10001, network none,
one CPU/768 MiB and ordinary test `--init` were used. Log:
`ltds-integrated-99578a6-ov7ktajd/test.log` in local temporary artifacts. The
test container exited and was removed. No client work or live state changed.

The preceding actual full-forward attempt completed original custody and cold
readback of a valid 202-file backup, but reached its outer observation budget
before backup acceptance (journal index 5). No candidate started and admission
never opened. This is an incomplete forward qualification, not a passing
migration. Root-owned original controller/artifact volume were retired only
after terminal state and exact ownership inspection; private diagnostics remain.
Batch census and an explicit larger harness observation budget are prepared for
a fresh operation; production effect deadlines have not been widened.

The subsequent actual config-stage qualification held before writer start on
`reviewed_config_writer_changed`. Its stopped exact controller/artifact volume
were preserved for diagnosis, then removed after exact stopped-state, ownership,
no-mount-user and synthetic-only-content verification. The cause was insertion
order in `repr(settings)` versus the canonically serialized receipt. Fix
`05b0d90` canonicalizes writer inputs without weakening exact byte comparison.
The corrected actual qualifier passed **3/3 checkpoints**, including one create,
one start and one remove despite injected lost confirmations, cold private
readback and unchanged old-config bytes/metadata. Its new controller and volume
were removed and absence verified. This validates local Docker configuration
preparation, not installed host enrollment or runtime activation.

A separate subsequent full-forward operation
`54103935-f4bf-4b40-b078-84f90d4064b5` held after **59.159 seconds** on the
unchanged full-inventory churn fence during backup verification. Its 900-second
outer budget did not expire. It completed five phases, and started neither
bootstrap nor final candidate. Bounded events showed an external unlabeled
container creation in the relevant interval before owned cleanup, but before/
after ID sets were not persisted, so exact attribution is not claimed. No
unowned container was touched. Full forward/declared rollback remains unqualified.

## First-worker installation integration

Root `f1a235f36e04ba3d8d0c8ce8ad1baf7a4c52783f` includes the reviewed first-worker
installer and actual per-host dependency artifact receipts. Frozen archive SHA256:
`a10c0b63543df3781f635b01cfb6aec07257991688bb97f00ae6566b16df4c5d`.
The seven-module non-root Linux selection passed **80 tests**, with **17
root-only installer tests skipped**, in 4.814 seconds. A separate explicit
UID-0 Linux run then passed all **17/17 installer tests in 0.833 seconds**.
Do not describe the first run as 97 passing tests or add these to other suites.
Both runners exited and were removed. New private source/dependency artifacts
exist on both PCs; no worker service has been installed or started. Actual
private config/TLS, host/Gate pins and guarded activation remain required. Main
PC administrative execution remains an operator step once the exact reviewed
manifest is ready; broad sudo or permission changes are unnecessary.
