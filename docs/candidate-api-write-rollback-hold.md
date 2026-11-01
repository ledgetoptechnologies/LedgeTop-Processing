# Synthetic candidate API write and rollback hold

This is an isolated test of an existing refusal, not an update stage or an
authorization to deploy. The exact locally built candidate image
`sha256:81dd310f24f1e711eb333f73f529ca1ae15cb1005abb2eb0f9ef11380ea81b74`
was built from a source context exported at candidate `3459d9a`; the
coordinator source and Dockerfile in PR237 match that Git tree. The image label
is self-reported, not signed provenance or independent proof of its contents.
The old image was the cached
`sha256:7fae9385fb5ca829783fe05eef2fdb1983abf4d491b2b9f795e55b617a5a0004`.

The test builds on the PR236 synthetic backup/copy fixture and uses its pinned
offline helper files. Only the unchanged `volume_census` function from `3459d9a`
is kept under a test-fixture path; direct CLI execution is disabled and there
are no updater or activation methods in that fixture. This does not install
the obsolete updater as an `ops` command. The predecessor creates a tiny synthetic Viewer photo and
16 inert schedule rows. After a verified backup, the candidate starts on a
separate copy; a second candidate startup submits a staging job through the
real HTTP `POST /v1/prototype/jobs` endpoint. The test checks the job is queued
only on the candidate, the stopped-state census detects the database change,
and the existing updater's strict `volume_census` refuses the nonempty tree.
An untouched backup is independently restored under the predecessor. The test
never restores that backup over the changed candidate. It also retains the
prior fixture's test of a synthetic new file. All test data are disposable.

The opt-in Docker rehearsal passed once in 12.267 seconds using isolated,
network-none resource-limited containers, owner label
`codex-viewer-e2e-cedfdac4472c401d829e7f730c5adbdf`. A subsequent read-only
label census found no test-owned containers or volumes. Seven fixture safety
checks also passed, with the opt-in case skipped. Run the same fixture with
the three image IDs pinned in `tests/test_stopped_viewer_mount_e2e.py` already
available and `LEDGETOP_DOCKER_MOUNT_E2E=1` plus
`LEDGETOP_IMAGE_COMPAT_E2E=1`; the fixture retains evidence on uncertain Docker
outcomes. Previously retained resources from a different failed rehearsal were
not touched.

After moving the updater source into a disabled test-fixture path, a
second owned run passed in 12.945 seconds under label
`codex-viewer-e2e-94c0901eb53d4160b2b212443cf2f223`; its label-filtered
container and volume censuses were empty. The test fixture was then reduced to
only the unchanged census function. Eight fixture safety checks also passed
with the opt-in Docker case skipped, including the census-only CLI guard.

The real updater is pinned to a different predecessor and refuses the
nonempty fixture before any compatible rollback could be established. This
test does not exercise updater activation or rollback, a live reference
listener, admission-closed candidate startup, an engine job, Viewer custody,
or physical/host writer fencing. In particular, stopping an old coordinator
and releasing its state lock before a candidate acquires it leaves a writer
gap unless a separately qualified external launcher fence exists. No such
fence or no-loss rollback after candidate writes is established here.
