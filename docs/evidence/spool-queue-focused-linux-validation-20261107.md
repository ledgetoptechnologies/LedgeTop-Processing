# Spool queue focused Linux validation — 2026-11-07

Candidate source revision: `c5aaceb4756afa8081c2f71983f3bfebcc50fbda`.

The coordinator source-reader lease uses Linux primitives. Running its
multi-node intake fixture natively on Windows returned `409 reference_conflict`
because the runtime deliberately refuses non-Linux source-reader operation;
that Windows result did not establish a dispatch defect. The fixture is now
explicitly skipped on non-Linux hosts.

The following bounded suite ran against the read-only mounted candidate tree
inside `ledgetop-processing:qualification-63c8049b-lf-20261106`
(`sha256:7adf6e6ece0b1a5a6246cc5af4d8707d146b577b58b9df3ac6d27148032a20b4`).
The task-owned container had networking disabled, one CPU, 1 GiB memory, a
128-process limit, and a 384 MiB temporary filesystem:

```text
tests.test_source_intake_multi_node_dispatch
tests.test_two_node_viewer_pipeline
tests.test_reference_source_fetch_recovery
tests.test_reference_source_prereservation_recovery
tests.test_reference_source_processing_recovery
Ran 238 tests in 36.329s
OK (skipped=1)
```

Separately, the Processing start-witness suite ran on the candidate source with
`uv` and the declared `rfc8785` and `cryptography` dependencies:

```text
tests.test_job_presentation
tests.test_reference_engine_worker
tests.test_reference_worker_api
Ran 128 tests
OK (skipped=1)
```

These results establish synthetic Linux dispatch, two-node admission and
selection, plus fixture recovery behavior for this source revision. They do
not establish an installed worker/coordinator, live Viewer authentication or
status ingestion, real-engine scheduling by this candidate, two installed
nodes, physical restart/disconnect recovery, or client-job readiness. No live
service or data was changed by these checks.

## Local image build and import check

At source commit `18fc4f186a20b1778bf908fe73ee79bdb2687e2e`, the candidate
Processing image built locally as `ledgetop-processing:candidate-18fc4f18`
from the pinned Python base in the repository Dockerfile. Its image ID is
`sha256:409dcfb422f0222dc611f89edf6e0f379136331616840f24c692926baf6d6fcc`.
The source tree was not mounted into the import check: the built image itself
successfully imported `processing.reference_runtime`,
`processing.reference_driver` and `processing.reference_worker_api` as UID
10001, inside a network-disabled, read-only container with one CPU, 512 MiB
memory, 64 PIDs and a 64 MiB temporary filesystem. The candidate image remains
local and was not installed on either PC or published to GHCR. This import
check does not load a real private runtime configuration or prove API startup,
spool attachment, engine execution, Viewer handoff or host acceptance.
