# Declared rollback post-restart audit: focused qualification

The qualification-only harness audit in `ec3a5cd` (integrated as root
`c731103f49d0250866b3317a20de76eb2dd83ec0`) requires a completed, unadmitted
declared rollback and the accepted original-start/readiness receipts. It then
gracefully stops that exact synthetic original, verifies terminal identity,
starts the already-created original lease-holder once, requires a new observed
incarnation within a fresh window and new socket/kernel/physical boundary,
runs the unchanged strict `rollback-final` custody projection and verifies
holder release. A durable audit intent precedes these effects. Failures retain
state/evidence; there is no original second start, SQLite restore, automatic
retry, production phase or live immutable-SQLite shortcut.

Frozen root c731103 archive SHA256
`813365230158d89a40e2b91a93c07e757c3fb0d2c43a2e4850842d6c7c133303`
ran:

```text
python -m unittest tests.test_enrolled_post_restart_audit tests.test_enrolled_startup_health tests.test_enrolled_coordinator_driver -q
```

**25/25 passed in 1.048 seconds, zero skips**, Linux UID10001, one CPU/512 MiB,
network none, immutable candidate image 378559e4, frozen read-only source.
The audit ordering/failure cases use controlled boundaries; the initial health
suite includes an actual delayed TCP listener. This is not yet an actual full
declared rollback proof.

Controller `d35def25a765a426cbb7c0b64a62a429f5e950b143e9415f796a210dac2d6cac`
exited 0/PID0/no OOM, was removed after ownership validation and its exact label
inventory was empty. No volume or other task runner was left behind.

A new operation `f1d7fbb3-ca10-4e3b-b415-04569246996e` was prepared from this
same frozen tree, with the required post-restart audit and existing immutable
image378/source187 pins. Its one-shot dispatch marker is absent. It has not
been executed and requires a fresh reviewed quiet-slot grant; the earlier
93a rollback launcher remains unexecuted and superseded. Preparation is not a
rollback result or deployment authorization.
