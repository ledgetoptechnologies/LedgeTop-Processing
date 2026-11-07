# Spool-first runtime preflight — November 6, 2026

Beau approved always-spool intake for the first photo release. Direct streaming,
GCP and other engines are not prerequisites for this milestone.

## Source and test result

Processing source: `4705ba3fad2d70f01507aa089aa1f5d2eee4e062`, clean before
export. Git TAR SHA-256:
`83080010e1c0d4f63fa76d7a4ed71386f6c3de53a9dc1da3ea297e9115a3198e`.

On cluster, a task-owned source export was mounted read-only at `/processing`
in cached `ledgetop-processing:qualification-63c8049b-lf-20261106`.
The run used no network, no Docker socket, no published ports, one CPU,
768 MiB RAM, 128 PIDs, read-only root and a 256 MiB temporary filesystem.
`PYTHONPATH=/processing` selected the exported candidate.

`python -m unittest tests.test_reference_runtime
tests.test_prepare_reference_runtime_config`: **84 tests passed in 4.689s**.
No skipped tests were reported. The first attempt could not import `tests`
because the task-owned export directory was mode 0700 and the image user was
different. Only that new export directory was changed to 0755 before retry;
no installed permissions, source guards or production code changed.

These are source/configuration tests, not real-photo acceptance. Earlier local
Windows invocation failed at import because `rfc8785` was missing; local Docker
was unavailable. The remote bounded run resolves that testing-environment gap.

## Runtime map

`reference_runtime.py` already constructs SourceSpool, ReferenceControlAPI and
ResultProxy. Schema 6 with `viewerAcceptance: {}` selects Viewer-driven result
download and signed copy acknowledgement; do not enable the legacy push sender
for V2 source jobs. Source and copy key verification still apply.

`ops/reference_worker_daemon.py` already constructs NativeEngine against a fixed
loopback endpoint, checks engine version, and composes ReferenceEngineWorker and
ReferenceDriver. Existing roundtrip tests replace the native engine edge;
passing them does not demonstrate real photogrammetry.

The prior installed-worker inventory records no worker unit or private worker
configuration on either PC. Fresh cluster SSH confirms the reference worker is
inactive, the existing coordinator is healthy, and the root filesystem has
approximately 113 GiB free. These observations do not reserve capacity or prove
complete engine idleness.

## Next acceptance work

Reconcile current coordinator schema, per-node identity/gate bindings, private
TLS/role configuration, source spool mount and Viewer signer/provider settings.
Prepare the existing worker installation path with cleanup disabled. Reuse the
existing real engines only after current queue/gate checks and rollback
preparation. No new native engine client or Viewer API is justified by this map.

No existing container/service/configuration, TrueNAS data, credential or feature
flag was changed. No photo job was submitted. Real Viewer ingestion/publication,
two-node execution and restart recovery remain unproven for this candidate.

## Follow-up observations

The isolated cluster image/source combination above ran
`python -m unittest tests.test_source_spool`: 12 tests passed in 0.027s.
The test container was `--rm` with networking disabled; this is not evidence
of the installed spool, actual disk reservation or Viewer transfer.

The worker routing suite ran 22 tests with 21 passing and one error. The first
attempt lacked a writable `/run`; a second attempt added an owned tmpfs there,
but the deferred direct-enrollment configuration test still reported
`invalid private worker configuration`. Preserve this as an unresolved test
failure, not as a spool-path success or a reason to enable direct streaming.

A fresh cluster `processingctl.py status` reported healthy with zero queued,
running or held jobs and 20 succeeded / 2 failed historical jobs. Both nodes
were marked active; this is coordinator metadata and does not prove workers
are running or all direct writers are idle. Current coordinator image ID is
`sha256:7fae9385fb5ca829783fe05eef2fdb1983abf4d491b2b9f795e55b617a5a0004`.
The reference worker user unit was absent on cluster. Main reported its worker
inactive, the NodeODX container running, and approximately 1.4 TiB available.
The existing dependency artifacts remain inputs, not installation or readiness
evidence. Actual per-host TLS, role, spool, gate and signed Viewer acceptance
configuration must be prepared and checked before launching the candidate.

## Recovery suite — not green

On November 6, the frozen 4705 source was mounted read-only into the cached
cluster qualification image, with no networking, one CPU, 768 MiB RAM and a
private tmpfs. `tests.test_reference_source_fetch_recovery`,
`tests.test_reference_source_prereservation_recovery`, and
`tests.test_reference_source_processing_recovery` ran together: **230 tests,
5 errors, 99.212 seconds**. The five `DeliveryAcrossRestartTests` errors call
`ensure_viewer_transfer`, which rejects source-bound jobs because they use the
Viewer pull acceptance path. The no-restart control also fails at that same
point. Determine whether the tests need migration to the current pull protocol
or whether a real integration gap exists; do not label recovery verified.
The owned runner exited and its `--rm` container was removed. These were
fixture tests, not installed-node or real-engine recovery.

Follow-up: `tests.test_reference_source_fetch_process_restart` passed 15 tests
in 77.363s, with all 59 spawned processes reaped. Its engine and transport
fixtures are synthetic. The five legacy-delivery tests were adapted locally to
use the current authenticated Viewer pull plus signed acceptance endpoint; all
five focused tests passed on Linux in 2.601s. The full 230-case suite has not
yet been rerun after that test-only change. These results do not prove live
worker/coordinator restart or actual Viewer result custody.

The full three-module selection was subsequently rerun against frozen 4705
source and the corrected test file in a separate read-only export: **230 tests
passed in 100.196 seconds**. The bounded no-network container exited and was
removed. This supersedes the incomplete-rerun statement above; the earlier
five failures remain recorded as historical results. Real-engine and installed
host recovery remain unverified.
