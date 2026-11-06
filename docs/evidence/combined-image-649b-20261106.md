# Combined source image — November 6

Built from the explicit canonical LF source archive at
`649b8c0e18af67fea06ba1c527d579715b8d5494`, tag
`ledgetop-processing:qualification-649b8c0e-lf-20261106`.
Docker image inspect returns immutable ID
`sha256:5d466eb5744c1b82bf64dc8313d337a4211526a67101cff439145abd0f85cfbe`,
UID/GID10001 and that exact revision label.

The same pinned base and requirements/dependency layer were reused from cache;
the build log reports CACHED for pip installation. No package version changed.
The existing Dockerfile allowlist excludes datasets, credentials, engine source,
tests and generated files from the build context.

An isolated readonly/no-network UID10001 probe independently verified all99
Processing Python files against the frozen source. Its sorted compact
relative-path-to-SHA map hashes to
`a346ef1bc340526b4901af356eb308e2592e436c4420d571020a040584c91daf`.
Distribution checks matched cryptography50.0.2 and rfc8785 0.1.4.

With a synthetic test-only API credential, the real application child returned
HTTP200/statusok from `/healthz`, with routing-probe mode and synthetic/photo/
Viewer modes disabled. The child was terminated and explicitly waited; the
task-owned probe container was verified absent. It used one CPU,512MiB ceiling,
64PID limit, no capabilities/socket/host port, no-new-privileges and private
bounded state/tmp tmpfs. No installed service was replaced or started.

Local evidence lives under
`C:/Users/fstor/AppData/Local/Temp/ltds-processing-canonical-649b8c0e-20261106`:

- `build.log`, SHA-256
  `20cd79e2975c8768f0d5ed064f6a7c1073b02b76126acd7d468a2654a3d3ebdb`.
- `probe.log`, SHA-256
  `6a1a1eac3bfff52d05328a5de6d2b2320a4eeb5f5f11cc51f0c33dffbcc2d8cb`.
- `python-hashes.json` retains the exact map checked in the image.

This image includes the later maintenance and report-contract corrections absent
from earlier image7adf6e6e. Earlier forward/rollback receipts do not qualify this
new artifact. New actual local cutover/rollback/audit runs are assigned separately;
installed NAS/Viewer/worker acceptance remains open. No registry publication or
mutable live/latest tag was performed.
