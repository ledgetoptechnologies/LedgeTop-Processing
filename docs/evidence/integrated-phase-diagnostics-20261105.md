# Integrated phase diagnostic preservation

The full phase fixture and failure-capture changes are integrated at
`835120c58a277f84ce8099b5a4a13f7ea15187ba`. Frozen full Git archive SHA256:
`63cf6b1b473b8facdb9edd58cf645ec194d35b3e4936472b5d72718241bf009f`.
The independent Linux selection passed **52/52 tests in 37.023 seconds**, no skips:

```text
tests.test_enrolled_phase_failure_capture
tests.test_enrolled_phase_fixture_profile
tests.test_enrolled_coordinator_driver
tests.test_enrolled_coordinator_boundary
tests.test_enrolled_coordinator_transport
```

UID/GID10001, one CPU/768 MiB, network none, read-only source and a bounded
temporary filesystem were used. Log:
`ltds-phase-capture-835120c-ccfv9txs/test.log` in local temporary artifacts.
The named runner exited0 and was removed. Actual factory/TLS profile checks
are included, but Docker control and log-socket reads in this selection are
mock boundaries. It is not actual full cutover or installed acceptance.

Before phase dispatch, the fixture now privately retains its reviewed plan,
bundle and exact configurations. A phase failure preserves all owned app/state
resources; it captures bounded private logs/state and one separately labeled
fresh read-only health observation before any separately reviewed disposition.
The actual owned-image/operation check precedes a fixed Unix-socket exact-ID
Docker logs GET; it accepts no caller URL/socket/action. Both multiplexed log
streams are retained under byte/line/deadline bounds. Public reports expose
hashes and counts. Production startup windows, mutation dispatch, equality
checks, observation semantics and admission policy remain unchanged.

This fixes the test harness's loss of startup diagnostic evidence. It does not
establish that the earlier bootstrap failure was a race, validate actual socket
capture, or qualify a readiness retry. A fresh actual forward operation is
required. Healthy flow remains the complete22-phase sequence; there is no
bootstrap-only shortcut or automatic retry/rollback.
