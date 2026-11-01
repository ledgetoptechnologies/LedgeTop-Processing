# Existing image archive recovery rehearsal

The locally retained image `sha256:318040006e6b2c333cd1383f14ae7e38fd51921aa0ffebec0eca0efb26e4b29c`
has revision label `4de0de1b5459a5c248d893b640a84262a807889a`. The rehearsal
compares the complete `/app/processing` regular-file path and SHA-256 manifest
inside that image to Git commit `19111a58660773302e143253a1e34b1901e83cbb`.
The source paths and hashes matched in the observed local run. This does not
establish that the entire image was built from 19111a5 or prove image provenance.

The same isolated run executed the three `missing_unrecorded_archive` tests
from committed 4de0de1 test source: recovery after restart, rejection of changed
bytes, and retention on engine outage or competing destination. All three
passed. The synthetic test uses fake engine data; no real engine, Viewer, or
production workload was involved.

From the repository root, with the exact retained image and Git commits present,
run `python -B tests/pr102_image_recovery_fixture.py`. The script requires the
image's exact ID, revision label and nonroot user; it creates a uniquely labelled
container with no network or host mounts, a read-only root filesystem, an owned
temporary filesystem, and resource limits. It verifies the exact container
before stopping or removing it. An ambiguous create or cleanup is reported for
inspection rather than guessed. It does not build or publish an image.

The run does not establish live ingress fencing, safe nonempty-state cutover,
full image provenance, physical worker state, real processing, or Viewer result
ingestion. It does not authorize deployment.
