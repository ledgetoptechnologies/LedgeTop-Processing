# Worker pre-start review: focused qualification

Core source `e9ff5abc036862ca2fc037b6a21674f2e59afc7b`, selected at `ce14dfe8`,
passed independent source review and **20/20 Linux tests in1.740seconds**, no
skips. The selection was explicit:

```text
tests.test_worker_prestart_review.WorkerPrestartReviewTests
tests.test_enrolled_coordinator_deployment
```

The first class has nine cases. Explicit class selection avoids accidentally
running its directly imported older preparation fixture as an additional suite.
These tests use the actual pure worker composer/loader with synthetic private
inputs, including completed manifests, config/unit hashes and preparation
receipts. They check ordered/bound metadata, preserved config/credential privacy,
placeholder refusal, historical enrollment scope, exclusive durable production
of review bytes, and refusal before host/journal locks.

This gate is configuration review, not a claim that workers are installed or
ready. The existing post-bootstrap activation boundary retains fresh installed
tree/config/unit/interpreter, original physical Gate/lease and authenticated
readiness checks. See [the contract](../worker-prestart-review.md).

Exact canonical LF archive SHA-256:
`0bb7836d58d2a657c4b572883b3e7fba4567919c2efce64ef63343cfe216136a`.
Full test log SHA-256:
`e91245a0d83e669596814f8cd430e5eb527c45e2cf85e1a00fe73ebcfc93efe0`.
Archive, source, test log and receipt are retained under
`C:/Users/fstor/AppData/Local/Temp/ltds-prestart-e9ff-20261106`.

One cached-image runner used UID10001 with an explicit passwd/group fixture,
one CPU,768MiB memory/swap ceiling,96PIDs, network none, readonly root/source and
private192MiB tmpfs; no capabilities, Docker socket, host ports or volumes.
The controller had a240second alarm/270second external bound and exited0.
The exact task-labelled container inventory was empty afterward. No host
service, NAS configuration, client dataset or real credential was changed.

The original full-phase fixture's opaque marker now deliberately fails this
typed gate. A separate adaptation must use actual completed preparation metadata,
installer-derived source/interpreter/unit paths, proper CA/server certificates
and per-node factory child processes before its next full qualification.
The earlier649b22/30 Docker runs are not evidence for this later ops-only change.
