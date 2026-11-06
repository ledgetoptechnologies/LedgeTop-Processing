# Configured-node and early-drain regression qualification

The corrected bounded run of exact source
`29c87a5e75c4199eb14873262bdddae73c57cafe` completed on November 5 local time
(2026-11-06 02:10–02:12 UTC): **228 cases, 224 passed, four skipped, zero failures
or errors, 51.661 seconds**. The only source change after the earlier `be7bfb5f`
checkpoint is a test's complete synthetic passwd record (`pw_name` and `pw_dir`).
No production identity, capacity or maintenance guard was relaxed.

## Exact scope

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

This includes the new configured-node/schema-2 host/interpreter tests, legacy
owned receipts and null-version behavior, exact early job/time plans, current
plan/qualification CAS, failed qualification, immutable lost-reply reconciliation,
and the existing update/version/health/finalization path. Host/Docker transports
are synthetic; Queue/SQLite, private files, process leases, localhost HTTP and
supervisor child-death tests are real.

The four skipped cases are `test_update_qualifier.ActualProbeTests`: actual Node.js
GET success, watchdog, permitted launcher banner and refused launcher prefixes.
The cached Python image has no `node` executable. Nothing was downloaded or
installed to hide that limitation; those JavaScript execution cases remain
unqualified by this run.

## Frozen runner and cleanup

- Source archive SHA-256:
  `57acf2ff3ffcb039f9cf0d8f8af9a0239feead9894559d09fb5297fa09361835`.
- Cached image:
  `sha256:378559e42456017b965d1e2e38b56eb627274734aeb317a758d84c0bc99bb5ab`.
- Archive/source/log directory:
  `C:/Users/fstor/AppData/Local/Temp/ltds-maintenance-regression-29c-4b3af42501e64b908f5c183a78a793a8`.
- Full corrected `test.log` SHA-256:
  `36d44ee71a0e9b6e793eaa3c40dc592a7ac14a835bfa6d0462cfcfd2792fe422`.
- Owned `--rm` container: `ltds-maintenance-29c-regression`;
  label `ltds.task=maintenance-29c-regression`; execution handle `37434`, terminal
  exit 0. Both exact-name and exact-label `docker ps -a` inventories were empty.
- One CPU, 768 MiB memory/swap ceiling, 96 PIDs, network none, read-only source and
  root filesystem, private bounded `/tmp`, all capabilities dropped,
  no-new-privileges, no Docker socket, no volumes and no nested live containers.
- The isolated unit-fixture account was the image's registered UID/GID 0, and
  working directory `/work` made existing subprocess tests inherit the reviewed
  source location. This is not production root access or an identity override.
  Shared/unowned containers were preserved.

## First attempt retained, not presented as green

The first identical 228-case selection used exact `be7bfb5f` archive
`1fa88a8df989b1ac6513cb424a4156bdecc5fd31ba732796003d8f9b73d3fbfa`, directory
`C:/Users/fstor/AppData/Local/Temp/ltds-maintenance-regression-be7-7825ee10895645ee9d52791c38a3ff57`.
Execution handle `61754` ended exit 1 after 49.721 seconds: **213 passed, four
skipped, two failed and nine errored**. Its original terminal traces are retained
in that execution's tool records (final output chunk `614ef1`).

The numeric UID 10001 lacked a passwd entry in that image. Nine CLI cases stopped
at `canonical_capacity_path` with `KeyError: getpwuid(): uid not found: 10001`;
the sanitization test consequently observed `KeyError` before its intended mocked
`RuntimeError`. The supervisor owner-death child inherited `/app` instead of the
parent's manually inserted `/work` import root and failed to import `tests`.
Both were runner/fixture setup problems, not a claimed production behavior pass.
The first named/labelled runner was also verified absent. The corrected run used
a real passwd account and explicit working directory, without patching production
lookup/import guards.

No installed engine, reference worker, systemd unit, timer, NAS or client data was
changed. These follow-on maintenance commits stay separate from the first
photo-processing release until their review/integration decision. Update timers
remain disabled, and installed replacement/handover acceptance is still required.
