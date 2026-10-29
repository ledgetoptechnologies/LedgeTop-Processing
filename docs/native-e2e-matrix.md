# Native-path synthetic end-to-end matrix

`tests/test_native_viewer_matrix.py` runs the actual, unmodified Viewer processing
modules against the real Processing native NodeODM-compatible facade:

- **Viewer modules:** `nodeOdmProvider`, `processingWorker`, `processingRepository`,
  `database`, `repository` and `storageManager`.
- **Processing side:** `processing.app` with `viewer_enabled`, as installed at
  :3005, in front of two synthetic engines (`cluster`, max 20 images, and `main`).

It exercises the native path Viewer main uses today. It covers neither the
reference protocol nor the reference worker daemon.

Everything is synthetic and local:

- loopback HTTP, invented bytes, temporary SQLite and storage;
- no photogrammetry, no NAS, no installed service, no client data;
- the retained pilot is untouched.

`tests/native_matrix/viewer_driver.cjs` loads the Viewer modules read-only from
`VIEWER_SOURCE_ROOT` and writes only under a temporary `MATRIX_ROOT`.

## How to run

Viewer storage uses `/proc`, so the matrix runs on Linux. Viewer's npm
dependencies (`unzipper`, `geotiff`, …) must resolve from an ancestor `node_modules`.
ES modules ignore `NODE_PATH`, hence the `/node_modules` mount.

```
docker run --rm --network none --read-only --tmpfs /tmp:exec,mode=1777 --user 1000:1000 \
  -e HOME=/tmp -e VIEWER_SOURCE_ROOT=/viewer -e MATRIX_RESULTS=/out/results.jsonl \
  -v <processing checkout>:/src:ro -v <LedgeTop-Ops checkout>:/viewer:ro \
  -v <viewer node_modules>:/node_modules:ro -v <output dir>:/out -w /src \
  node:24-bookworm python3 -W error::ResourceWarning -m unittest -v tests.test_native_viewer_matrix
```

Without `VIEWER_SOURCE_ROOT` and Node.js 24 or later, every case is skipped. A skip
is not a pass.

**Simulated time.** Viewer schedules polls and retries through
`processing_jobs.available_at`. The driver sets that column to the past between
steps instead of sleeping. Viewer's own backoff arithmetic and attempt counting
are unchanged. Outages are modelled by a loopback relay that refuses connections
while the real coordinator and its engine job keep running.

## Results

Executed twice with identical outcomes: 8 tests, 7 pass, 1 expected failure, about
47 s, warnings treated as errors.

- **Processing:** `codex/processing-reference-v1` @ `5cd37fd986b7b496caa3f2dfe9f24398baab2ea8` plus these three new files.
- **Viewer:** LedgeTop-Ops `main` @ `9e56370985eca2a0807f7329d3b2b466344d567f`, read-only.
- **npm dependencies:** read-only from an existing local Viewer install (`unzipper` 0.12.3, the pinned version).
- **Runtime:** `node:24-bookworm` (`sha256:3d27e5c1…add0`), Node v24.21.0, Python 3.11.2, network none, read-only root, UID 1000.

| Case | Behaviour checked | Result |
| --- | --- | --- |
| T1 + T12 | 20 synthetic files go through Viewer submit, reconcile polls and ingest. One engine run, on `cluster`. The archive is captured and exactly one model version is committed; the attempt reaches `derivatives` (handed to Viewer's separate derivative worker, not run here). The coordinator's result copy is still on disk afterwards. | PASS |
| T2 / T3 / T4 | With the global slot held, three jobs are queued (staging small, production small, production 21 files) and no dispatch happens. After release they run production-small, then production-large, then staging-small. Small jobs go to `cluster`, 21 files to `main`, and each engine runs at most one job at a time. | PASS |
| T5 | Running cancel: Viewer `cancelAttempt` plus the provider `/task/cancel` the route makes, sent twice. Viewer and upstream both end `cancelled`. | PASS |
| T6 | `cluster` is draining: a small job is accepted but neither engine commits it, so there is no overflow to `main`. After maintenance is cancelled it completes on `cluster`. | PASS |
| T7 | The commit response is lost after the coordinator accepted it. The lease expires and Viewer's worker resumes. Exactly one engine run, and the attempt continues to `ingesting`. | PASS |
| T8 | The download is aborted mid-stream, then fetched twice. Both sha256 hashes are identical and the coordinator archive is retained. | PASS |
| T10 | The coordinator is unreachable for 2 reconcile claims and then returns. The attempt stays `queued_upstream` and proceeds to ingest. | PASS |
| T9 | The coordinator is unreachable for longer than Viewer's retry window. **Desired:** the attempt survives while the upstream job runs. **Actual:** reconcile claims 1 to 4 reschedule, claim 5 marks the job failed and the attempt `failed` (`processing_failed`) while the engine job is still running. | EXPECTED FAILURE (known gap) |

T9 is written to assert the desired behaviour under `unittest.expectedFailure`. A
fix in Viewer's `processingWorker.js` retry path shows up as an unexpected success.

## First failures preserved

Earlier runs of the same harness are kept as evidence, not converted to passes:

- **Windows host:** every case failed at `StorageManager.initialize`, which needs
  Linux `/proc/self/fdinfo`. That's why the Linux container is required.
- **T7, T10 (first Linux run):** harness defects. One assertion counted engine
  commits before the coordinator had dispatched the job; one wait loop had a step
  budget shorter than the engine run. Both were fixed in the harness; no product
  code changed.
- **T1 (first Linux runs):** the transport-only synthetic ZIP is rejected as
  `missing_required_output`. With `NODE_PATH`, the ES-module `geotiff` import
  failed with `ERR_MODULE_NOT_FOUND`. With an ODM-layout synthetic archive and
  `/node_modules`, ingest commits.

## Limits

- **Not real outputs.** The archive uses ODM output paths with invented bytes. It
  proves Viewer's ingest bookkeeping and file handling, not EPT/mesh validity, LOD
  conversion, rendering or photogrammetry.
- **Native path only.** The reference protocol, reference worker daemon, source
  custody/purge pins, acceptance ACKs and last-copy cleanup are outside this
  matrix. The native path has no acceptance ACK and deletes nothing, which T12
  checks.
- **Simulated outages.** The outage is a refusing loopback relay on a simulated
  clock. It is not a coordinator process restart (covered by existing Python
  tests), a power loss, or wall-clock timing over minutes.
- **Viewer state is isolated.** It is a temporary database. The installed Viewer
  image contents are not verified here.
- **T11 is not run.** Scratch backpressure is reference-path only.
