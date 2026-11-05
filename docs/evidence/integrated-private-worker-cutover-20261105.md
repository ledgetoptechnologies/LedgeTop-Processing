# Frozen-source private worker and cutover qualification

Processing source: `18750d4d599026c18e569a9c67135e6c9fdc7d1a`.
Full Git archive SHA256:
`17395af4489ce20a8dfddc18f4db5653226cbb53e90c4b314b10a997fdfd6740`
(8,622,080 bytes). All successful runs below mounted this frozen archive's
extracted tree read-only, rather than an actively edited worktree.

## Private dynamic catalog and normal worker wiring

**96/96 Linux tests passed**, no skips, in **98.198 seconds**:

```text
tests.test_reference_worker_catalog
tests.test_reference_runtime
tests.test_options
tests.test_reference_worker_readiness
tests.test_reference_worker_daemon_routing
tests.test_reference_worker_handover.ReferenceWorkerHandoverTests.test_real_factory_pin_migration_tls_readiness_and_gate_yield_complete_before_reopen
```

The relay uses the existing worker TLS/result-reader principal, returns bounded
live definitions/capacity while busy, and has no writable native proxy or direct
native discovery fallback. This selection covers dynamic options/fingerprints,
physical/instance/authority binding, secret reflection, result-owner wrapping,
actual TLS and real worker-factory pin migration. It does not establish an
installed worker, sealed LAN ingress or connected two-node job acceptance.

An earlier run against a live worktree reported six source-pin errors after
root integrated independent ops files during the run. Readiness deliberately
hashes the complete source footprint. That run is invalid as an exact-source
qualification; no product guard was relaxed. The frozen-source rerun above
corrects the test isolation error.

## Concrete cutover components and driver control flow

**118/118 Linux tests passed**, no skips, in **25.031 seconds** across transport,
private deployment bundle, fresh boundary, full custody, cutover journal,
concrete driver, tracked SSH, worker readiness probe and holder-harness ownership
tests. The dependency selection includes the independently reviewed fixes for
delayed start inspection and accepted-resume reply loss. The driver must inspect
the immutable actual start time within its original dispatch window; later
inspection is not expiry. An already accepted resume is recovered from its
exact receipt without reposting or requiring workers to remain idle.

This unit runner used Docker `--init` for ordinary orphan reaping. The earlier
no-init unit runner passed 117 cases but failed the descendant-absence check:
a bounded diagnostic confirmed the killed descendant was state `Z`, with the
test runner itself PID1. The isolated process group was killed, but the Python
unit runner did not reap adopted children. The corrected unit environment uses
the normal reaper; the production lease-holder recipe remains explicitly
**non-init PID1**, with its separate namespace-drain qualification unchanged.

These 118 tests include synthetic Docker/custody/worker boundaries in the full
driver cases. They are not an actual complete Docker migration or installed
SSH/systemd acceptance. The prior actual six-check holder/full-TAR run remains
component evidence; complete forward and declared rollback phases are pending.

Both successful selections used UID/GID10001, one CPU, 768 MiB, no network,
read-only source/root and bounded private temporary storage. Exact runner
containers were removed and absence verified.

## Current local image

The frozen source built using the existing owned one-CPU/2 GiB builder and
cached dependencies. Local tag: `ledgetop-processing:qualification-18750d4`.

- Runnable image/manifest:
  `sha256:378559e42456017b965d1e2e38b56eb627274734aeb317a758d84c0bc99bb5ab`
- Image config:
  `sha256:bfa46aafe54a129ec35b64ad46c9af6a7464aa4132b52fecf29fb9f83cb07d1d`
- Revision and repository OCI labels match the frozen source and Processing repo.

The existing actual Docker transport qualifier then passed all **12** checks:
closed recipe/never-started readback, private mode/link/size refusals, exact
17 MiB streamed archive, packaged health/UID/read-only source, exact probe
retirement, graceful stop and retained rename. Every task resource was removed.
The owned builder was stopped and verified; the unrelated shared builder was
preserved. The image is local only and has not been published or deployed.
