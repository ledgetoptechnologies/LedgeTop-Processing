# Viewer outage and recovery acceptance

Preparation only. Beau owns TrueNAS operations. Do not stop or update Viewer
services until Beau starts a planned test window. Use only the saved synthetic
Testing and Examples source; do not use client data. Automatic cleanup stays
disabled throughout.

## Candidate prerequisite

The source fix for the archive receipt-before-promotion crash window is in
Viewer commit `f563602585808423773246e3e8de80d69a247a62` on
`codex/viewer-processing-dynamic-options-20261107`. Its focused bounded Linux
suite passed 51 tests before the commit. It is not yet built or installed.

Build the normal Viewer image from this exact commit (or a later reviewed commit
that contains it). The Viewer Dockerfile requires the Six Labors license as a
BuildKit secret for its converter stage. Keep that file outside the repository,
do not pass its contents in a command argument, and do not print it. Before
testing, capture the immutable image ID and confirm API and worker use the same
image with an OCI revision containing this commit. If the candidate cannot be
built or the installed revision does not match, stop here; the outage test would
not exercise this fix.

## Read-only preflight

1. Confirm no unrelated Viewer job is active and the coordinator has no
   unrelated queued/running work. Record the synthetic test task ID and its
   provider-facing task ID.
2. Record the API and worker container image IDs, OCI revisions, health, and
   mounts. Confirm both Viewer containers still use the persistent
   `/app/storage` mount. Confirm the Processing coordinator and both physical
   engines are healthy. Do not expose credentials or client paths in the
   evidence.
3. Confirm the test source has 10–50 photos, the task is submitted, and the
   real engine has started the same provider task. Do not interrupt the
   coordinator or processing engine for this test.

## Worker recovery test

1. With the synthetic photo task processing on a real engine, Beau stops only
   `ix-model-viewer-viewer-worker-1` in TrueNAS. Leave the Viewer API,
   coordinator and processing engine running.
2. Wait until the coordinator records that same provider task as complete and
   still retains its input and result archive. The Viewer task should remain
   pending rather than reporting Ready. Capture sanitized status and counts.
3. Beau starts the same Viewer worker container. It must resume using the same
   provider task ID, fetch and verify the result, and publish exactly one
   Viewer result. Do not resubmit the task or restart the processing engine.
4. Verify Viewer reports Ready, one processing attempt and one result; the
   model and orthophoto open; saved task logs show completion; the archive
   receipt and asset hashes match; and the coordinator has no duplicate engine
   task. Retain all source files, spool data and result archives.

## Pass and abort conditions

Pass only if the same provider task resumes and produces one usable Viewer
result, with source/result bytes retained and no second engine execution.
Abort and restore service if the worker does not recover promptly, health
checks fail, task identity changes, a duplicate engine task appears, or any
input/result is missing. Record the exact before/after image IDs and use the
previously captured TrueNAS app configuration for rollback; do not guess an
image tag or delete data.

This test verifies worker outage recovery. It does not by itself verify an API
container outage, production-versus-staging submission through Viewer, a full
image rollback drill, or 5K/10K-photo capacity.
