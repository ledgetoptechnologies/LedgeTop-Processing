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
remain preserved for diagnosis. Synthetic lifecycle tests did not establish
actual Docker serialization compatibility. Full forward/declared rollback and
first installed worker activation remain required.
