# Combined maintenance qualification — November 6

Independent static review compared maintenance head `315c8760` against its base
`d44d5e26` and integration `0d8f4f8f`. No new blocker was found. Its changed paths
did not overlap the intervening integration edits. Original production commits
`d53b40d8` and `f589d819`, plus the complete-passwd fixture correction `29c87a5e`,
are selected at `a1b0b971`, `7623c15d` and `162f489c` respectively.

Exact combined source `162f489c9c9ca83cd00852c0049e4e5cb1309444` passed
**228/228 Linux cases in52.476seconds, no skips**. This is one combined execution,
including all four actual Node probe cases, rather than the prior two-run
qualification of the isolated maintenance branch.

Selection:

```text
tests.test_maintenance
tests.test_owned_maintenance_schedule
tests.test_pending_maintenance_qualification
tests.test_managed_maintenance
tests.test_managed_update_finalization
tests.test_maintenance_version_gate
tests.test_container_update
tests.test_update_supervisor
tests.test_update_service
tests.test_update_qualifier
tests.test_update_maintenance_adapter
tests.test_update_service_cli
tests.test_update_service_units
tests.test_update_pipeline
tests.test_update_early_drain
```

The source preserves configured node/host/interpreter bindings, an owned exact
job/time drain before staging and qualification, same-plan qualified version
binding, idle-only mutation and immutable unknown-effect receipt reconciliation.
The tests exercise real SQLite, local HTTP, process leases and child-death
fixtures, with synthetic host/Docker transports. The actual fixed Node probe is
run against local HTTP. Installed native engine replacement, systemd timers,
worker handover and unattended resume remain unqualified.

Runner used the cached image
`sha256:378559e42456017b965d1e2e38b56eb627274734aeb317a758d84c0bc99bb5ab`,
its registered UID0 account, explicit `/work`, one CPU,768MiB memory/swap ceiling,
96PID ceiling, network none, readonly source/root and192MiB private tmpfs. It had
no Docker socket or published port and created no volume. All capabilities were
dropped and no-new-privileges was set. A240second in-container alarm and270second
controller timeout bounded the run. It exited0; the exact task-labelled container
was absent after return. No installed host or NAS operation was performed.

Node24.22.2 was reused readonly from the existing cache and checked before tests:
SHA-256 `904a6d3484241d466989f9df9ba3aab822b1496191bdb28230eb491a55c32a52`.
No package downloads or dependency changes occurred.

Retained files under
`C:/Users/fstor/AppData/Local/Temp/ltds-maintenance-integrated-162f-20261106`:

- Canonical LF `source.tar`, SHA-256
  `9d7aa88eb94892b3e40757148874bef9a95c54f9caf8a876ac140453e2193aeb`.
- `test.log`, SHA-256
  `ac892aac451a6610884e19ae58791acea60f508374d0514d43d355a01265396b`.
- `receipt.json` records the immutable source/image, selection and terminal
  result. The owned container name/label was
  `ltds-maintenance-integrated-162f-20261106`.

The earlier staged image `7adf6e6e` and source archive `63c8049b` do **not** include
these new maintenance changes. Their earlier forward/rollback evidence remains
specific to that frozen artifact. A later release using the combined source
requires a new image, source pins and corresponding release qualification.
