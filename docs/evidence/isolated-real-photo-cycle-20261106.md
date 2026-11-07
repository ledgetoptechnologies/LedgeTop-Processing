# Isolated real-photo cycle — 2026-11-06

This is staging integration evidence, not production deployment or installed
two-node acceptance. The test used 20 authorized photos copied read-only from
their source; each copy matched its source SHA-256. It ran on the staging PC in
isolated containers with a 10 GiB free-space stop threshold and no automatic
cleanup. The source files were not changed.

- Processing candidate image: `sha256:98c37f4b7bcf2ac5626b9368852e8c1d2e9c2553d5606b26b26d3e37dccc7fdb`.
- Viewer candidate image: `sha256:7912cd757c11fff99c165ba81e4ed5d5626cf6a0d9fff34543c5a36f4b29168c`.
- NodeODM image: `sha256:dde9333fe1f89fc6164901439c58e0971a52a67ae3a5c955ae77643aa9d644f8`.
- Real NodeODM processed the 20 photos. Its 112,409,842-byte result archive was
  retained by the worker. Viewer accepted a matching copy with SHA-256
  `cc663251f13e9920cb61cf2990ff75568a34acd96ae58b62cf11d6c2050a1bfd`;
  a deliberately lost acceptance reply was retried successfully.
- The initial harness held a 60-second reconcile lease during the roughly
  17-minute run and failed with `lease_lost`. Recovery used the original
  filesystem identity, normal coordinator epoch registration and a fresh Viewer
  lease; it did not reprocess the photos. A cloned coordinator correctly refused
  startup because its pinned filesystem identity changed.
- Viewer ingested the real archive; the licensed converter completed a verified
  mesh derivative. The isolated Viewer marked the output published and served
  its tileset and referenced binary tile with HTTP 200. The publication
  container `ltds-real-viewer-derivative-20261106` exited 0 and its logs record
  the results. Token denial checks also passed.
- The worker driver subsequently verified the signed acceptance and persisted
  `completed` with `accepted_copy_verified=1`. Its subsequent poll returned
  `retry_wait` because the isolated engine was intentionally stopped; completion
  was checked in persistent state. Both cleanup hooks were disabled, and the
  worker archive and Viewer copy remained present.
- A bounded read-only visual inspection found a recognizable orthophoto mosaic
  (2,529 by 1,213 pixels, about 0.05 m pixel scale) and a GLB model in the
  ingested assets. The overview showed no obvious gross mosaic failure; this is
  not a survey-accuracy, complete-coverage, or 3D-appearance assessment.

The original worker and engine results, test data and backups are retained in
the task-owned `ltds-real-engine-*` volumes on the staging PC. The test used
fixture-level seams for worker identity and gate behavior, and manually recovered
the fixture's expired lease. It does not establish installed hardware gating,
normal production Viewer submission, visual quality, two-node scheduling,
restart resilience across the installed stack, or readiness for large client
datasets. Do not remove retained data based solely on this record.
