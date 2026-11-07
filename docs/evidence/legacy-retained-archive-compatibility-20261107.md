# Retained legacy archive compatibility — 2026-11-07

The existing production coordinator's database reported seven unremoved Viewer
tasks, all `succeeded`/`complete`. Read-only inspection found an `all.zip` and
`all.zip.receipt` for each, plus an `all.zip.intent` regular file whose stored
identity matched the task, job, node, engine and generation in every case.

In a disposable network-disabled container using the pinned 4705 candidate
image, with the existing state volume mounted read-only and a read-only patched
module, the unpatched candidate's task info succeeded but the first archive
download failed closed because its strict inventory rejected `all.zip.intent`.
The patch accepts that file only when it is a private, owner-controlled regular
single-link file with an exact identity match; it keeps rejecting unknown files,
symlinks, hardlinks, bad receipts and invalid archives.

The patched candidate verified and read all seven real archives, totalling
739,272,909 bytes, through `LegacyViewerReadonly` using a read-only queue/freeze
test shim. No legacy data was modified. This is direct compatibility evidence,
not an installed HTTP, coordinator-lease, or cutover qualification.

The complete synthetic legacy HTTP/security suite passed 14 tests in a bounded,
network-disabled Linux candidate-image container. Its existing metadata check
initially failed for both patched and unpatched code because it did not allow
the already expected `coordinator_identity.source_mode_version` migration;
the fixture now checks that the added marker has the exact value 1. The new
test covers matching and mismatched intents, unsafe permissions, symlinks and
hardlinks. Runtime code is not yet included in the existing 4705 candidate
image; a new immutable runtime candidate and all dependent source, worker,
policy and rollout pins must be reviewed before deployment.
