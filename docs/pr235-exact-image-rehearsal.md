# PR235 synthetic predecessor rehearsal

This is isolated compatibility evidence, not deployment authorization. The
candidate was built from `3459d9a9867fe9ffcd5981522d3f6f40053d3522` (Git tree
`4698783643dba1496431359ee906e6cff83d9f2d`). The `git archive --format=tar`
SHA-256 was `52b9a82e2118892ce013d6e0345c875073239e8798add13119fdf35bae1a9d30`.
The coordinator Dockerfile pins the locally present Python base
`sha256:e32be0e55814a08977501ca863271d9bd02443710db8f2afaf8372e2c4508530`.
BuildKit was invoked with `--pull=false`, `--network none`, a 1 CPU quota and
1 GiB build memory. Its log nevertheless showed Docker Hub metadata and auth
lookup; this was not a fully offline build. The resulting local candidate ID
was `sha256:81dd310f24f1e711eb333f73f529ca1ae15cb1005abb2eb0f9ef11380ea81b74`.
Its revision label is self-reported, not independently signed provenance. The
Dockerfile copies only `processing/`; this build did not include a NodeODM
engine patch or alter a live engine.

The predecessor was the locally cached image
`sha256:7fae9385fb5ca829783fe05eef2fdb1983abf4d491b2b9f795e55b617a5a0004`,
matching the image inspected on the running .89 coordinator. Its revision
label reports `693cb7eca32295da959a4f42399f317577ef8a8a`; that label alone
does not prove its build provenance. The synthetic helper image was pinned to
`sha256:aa943ce411423c7582288f18707ff66eddcbaeb510502d747a72aeb34f30d159`.

The test harness and offline backup/restore helpers come from exact commit
`8f02e23b4602732962e0a8f58284c2a820d1d20f`, not the candidate image:

- `ops/stopped_viewer_backup.py`: SHA-256 `3506569fd839d07f7752d85214dd09f18092064871f31f88aa479b79e1cbce12`.
- `ops/stopped_viewer_restore.py`: SHA-256 `8a93271ff81b7052fa8f8f0f7b599fd07dad134dd10160c9262ef0fa93110cda`.
- Original test: SHA-256 `5203c7da5a77290c9c79d90af4182eba364ff8274cfc2d05b9ed04dd21492b3f`; the adapted test SHA-256 is `3d3e955cdbaaa50d7d984b1ba1de4ffc160a67abb1a2c0215c0aabd4d42ad612`.

The test substitutions pin the candidate image, candidate revision and helper
image. The `ops/state_census.py` and `ops/update_reference_coordinator.py`
Git blobs are identical in the candidate and helper source revisions. An
independent reviewer compared all 507 files in the prepared candidate export
and found no non-helper mismatches or unexpected files. This is a consistency
check, not signed image provenance.

With the three pinned images available, run the fixture safety checks using
`python -B -m unittest tests.test_stopped_viewer_mount_e2e`. For the opt-in
synthetic Docker test, set `LEDGETOP_DOCKER_MOUNT_E2E=1` and
`LEDGETOP_IMAGE_COMPAT_E2E=1`, then run
`python -B -m unittest tests.test_stopped_viewer_mount_e2e.MountRestoreE2E.test_backup_remount_restore`.
The fixture creates uniquely labelled resources, runs test containers with no
network and resource limits, and retains recovery metadata on uncertain Docker
outcomes. Do not rerun cleanup of unknown resources.

One corrected run passed in 11.037 seconds under owner label
`codex-viewer-e2e-8f4b11d19634448f915bfeac2eadc280`. The predecessor created
a synthetic Viewer task and photo plus 16 inert schedule rows. The test made a
backup and a separate copy of the predecessor state, started the exact
candidate on the copy with automatic archive capture disabled, and checked the
task, photo, schedules and stopped-state census. It independently restored the
untouched backup under the predecessor. Synthetic later file and database
mutations were detected by comparison; these were not rolled back. The final
label-filtered container and volume censuses for that run were empty. The
fixture safety run passed seven tests and skipped the opt-in test.
The candidate source's combined Viewer, capture, admission and reference
runtime suite separately passed 186 tests in a network-disabled container at
1.5 CPU and 512 MiB; it did not use client data.

The first attempt failed before candidate startup because the candidate export
did not contain the rehearsal branch's backup helper. Its Docker client did
not retain the helper's exact exception, so the missing import is the likely
cause rather than a proven traceback. The fixture marked cleanup uncertain and
saved `C:/Users/fstor/AppData/Local/Temp/codex-viewer-docker-recovery-lrz_eemz.json`.
Its labelled backup and scratch volumes remain retained; the known keeper
`bfa7091da18a` self-exited at `2026-11-01T01:16:27Z`. No cleanup was retried.

This does not prove safe live cutover, continuous exclusion of old or external
writers, physical disk durability, or rollback after candidate writes. The
existing updater's strict census rejects the fixture's nonempty Viewer tree and
guarded rows; it is not relaxed here. Admission closure and the format registry
remain unqualified and disabled. Do not restore a backup over new admissions.
