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
installed to hide that limitation. The four JavaScript cases were subsequently
qualified in the narrowly scoped follow-up below; the first run's skip count is
preserved as observed.

## Four-case Node follow-up

Only `tests.test_update_qualifier.ActualProbeTests` ran on exact evidence head
`72762637b5d7d0deef13adb8934e2f04dc19ecc6`: **4/4 passed, no skips, 0.346 seconds**.
Production and test files are identical to `29c87a5e`; the intervening change is
this evidence document only. The four previously skipped cases therefore close
the affected 228-case selection across two separate runs (224 plus four), not an
invented single no-skip execution. The 224 passing cases were not rerun.

The existing Linux Node cache was mounted read-only at `/usr/local/bin/node`.
Before importing the test class, the runner independently verified version
`v24.22.2` and full binary SHA-256
`904a6d3484241d466989f9df9ba3aab822b1496191bdb28230eb491a55c32a52`.
The host cache file is
`C:/Users/fstor/AppData/Local/Temp/ltds-node-runtime-ac201afbae3c4df6878dd044bd53a59a/node-linux`
(126,881,248 bytes). No cache file, dependency or production source was changed.

The exact follow-up source archive SHA-256 was
`d8154796a23a41d13513c4b9256d35f01fd118757af5e05ec1bace0d1f72b34e`.
Archive/source/full log are under
`C:/Users/fstor/AppData/Local/Temp/ltds-maintenance-node727-837a31232ad04004b0f8bf4171d2ca29`;
the `test.log` SHA-256 is
`34e15b87bf8a258d5fdf6b35b60d6e20d4ec1cfee1a59c60ac60392f09d1e0b4`.
Execution returned synchronously with exit 0 (tool output `a372a7`, no live
handle). The owned container `ltds-maintenance-node727-probes`, labelled
`ltds.task=maintenance-node727-probes`, used the same cached Python image,
registered UID/GID 0 and `/work` working directory, one CPU, 768 MiB ceiling,
network none, read-only source/root, private bounded `/tmp`, no capabilities,
no-new-privileges and no Docker socket. Exact-name and exact-label `docker ps -a`
checks were empty afterward; no volumes were created or shared containers stopped.

These execute the unchanged fixed JavaScript probe against real loopback HTTP,
with its fixture's explicitly scaled watchdog/port. They prove GET wire handling,
watchdog termination and launcher-output acceptance/refusal. They do not contact
an installed NodeODM engine or qualify reconstruction/GPU behavior.

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
