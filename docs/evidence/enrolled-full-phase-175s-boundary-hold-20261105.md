# Actual forward attempt: bounded observation hold

This is an incomplete actual-Docker qualification, not a completed cutover.
The test used frozen harness `9f208ce` and packaged processing image
`sha256:378559e42456017b965d1e2e38b56eb627274734aeb317a758d84c0bc99bb5ab`
(source `18750d4d599026c18e569a9c67135e6c9fdc7d1a`). No installed PC,
TrueNAS dataset, client input or real engine task was touched.

## Executed boundary

One disposable real693 predecessor started normally and exited0 through the
production driver. The real PID1 holder acquired both original kernel flocks.
The production full census and immutable TAR copy/validation executed.

Five production phases completed through `custody-verified`. At the175-second
outer qualification alarm, `copy-verified-backup` remained pending (dispatch
attempt5). Its effect had completed and its private backup receipt was durable;
the final fresh writer/owner inspection was interrupted. The exact stack was
`inspect_backup -> BoundaryProbe.writers -> DockerCLI.inspect`, a read-only
command, reported as `command_read_unavailable`. No bootstrap or final app
had been created, admission stayed closed, and no mutation was replayed.

Wall time from Docker timestamps was176.293seconds. The journal does not
retain individual phase timestamps, so no per-phase duration is inferred.

## Retained evidence

Operation: `532cc128-3d24-4e96-ba0b-33e4cbbf37b1`.
Stopped outer controller: `ltds-phase-forward-532cc1283d244e96`, exact ID
`660183ec23c3f266da74caafb273133f827084b56ea2db3dbebe3c4d3df9fb08`.
Observed PID0, exit1, no OOM. Its private artifact volume
`ltds-phase-forward-artifacts-532cc1283d244e96` remains for evidence.

No uncertain mutation was recorded; known-safe inner cleanup completed. Exact
inner task-label container and volume inventories were independently empty.
The outer operation was not restarted or replayed.

Cold read-only `docker cp` recovered the journal, full baseline and retained
backup receipt/TAR without starting the controller. The unchanged production
`validate_tar` independently revalidated the467968-byte archive against the
original baseline and synthetic census key:

- 202 files,13 directories,262429 content bytes.
- SHA256 `7b9f1866f661236d1ba48e40ee0b5146e146aaad3b8bf784743c5a262bc9d6a9`.
- Exact receipt match, clean stream closure and client exit0.
- `restoreAuthorized=false`; a copy is never original lock authority.

## Measured cost and next qualification

The local Docker Engine had96 containers. The old `writers()` path issued one
full container list plus an individual inspect for every container, twice per
boundary; capture/backup/acceptance repeat these boundaries. That creates
hundreds of CLI round trips before candidate startup. The database and retained
files were small; the held boundary was not a source-data validation refusal.

The reviewed performance change uses a full ID census, one literal JSON
projection of ID/mounts/stopped-state data, and a second full ID census. It
preserves alias/exclusion checks and rejects changed or incomplete inventories;
configuration/environment fields are never requested. Existing unrelated
containers remain untouched.

A future NEW operation may use the explicit qualification-only900-second
outer budget. Production per-command deadlines, startup timestamp windows,
schema/receipt/lease guards and worker readiness are unchanged. It must still
complete the actual forward sequence and a separate declared-v2 rollback.
