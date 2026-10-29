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

## Qualification of the Viewer poll fix (harness v2)

Harness v2 (`05a9ef9`) adds evidence only. Each case result lists every CommonJS
Viewer module it loaded, with the sha256 of its bytes. Assertions are unchanged
and T9 keeps its `expectedFailure` annotation.

Both runs used the same image and dependencies as above:

- **Baseline:** Viewer `4b4f7075110372241aa139503d1dfd450e0b6c14` (#349). It is
  recorded separately from the earlier `9e56370` run. Every case loaded
  `processingWorker.js` sha256 `cc4c7d86…270a6`.
- **Fix:** Viewer `e66e37b31165035d162109bf6a1faa2ebc47b5cd` ("Keep reconciling when
  provider status is unavailable", on `codex/viewer-poll-cancel-repro-20261029`).
  Every case loaded `processingWorker.js` sha256 `fdb948ee…06752`. The other 36
  loaded Viewer modules are the same set.

| Viewer | T1–T8, T10 | T9 long outage |
| --- | --- | --- |
| `4b4f707` baseline | 7 pass | EXPECTED FAILURE. Reconcile claims 1–4 reschedule; claim 5 marks the attempt `failed` (`processing_failed`) while the engine job runs. |
| `e66e37b` fix | 7 pass | **UNEXPECTED SUCCESS.** All 6 refused claims reschedule as `provider_observation_unavailable` with the attempt still `queued_upstream`. After the outage it proceeds to `ingesting`. |

The unittest summary for the fix run is `FAILED (unexpected successes=1)`. That is
the intended signal that the gap is closed, not a regression. Flipping T9 to an
ordinary assertion is a separate, later harness change. Retry time remains
simulated (`available_at` advanced), the outage is a refusing relay, and the
fix's 5-minute backoff ceiling is not exercised in wall time.

## Versioned fixed-source regression (harness v3)

Harness v3 (`b0f7de2`) adds `CoordinatorOutageFixedPollV1`, a plain-assertion
T9 for Viewer `e66e37b` only:

- It runs only with `MATRIX_VIEWER_TARGET=viewer-e66e37b-poll-fix-v1`; default
  runs skip it.
- It first requires the loaded `processingWorker.js` sha256 to be
  `fdb948ee…06752`.
- It then asserts that six refused reconciles all stay `queued_upstream` with
  `provider_observation_unavailable`, and that the attempt proceeds to ingest.

The v1 classes, including T9's `expectedFailure`, are unchanged. The baseline and
fix suite results above stay as recorded: 7 pass plus 1 expected failure, and 7
pass plus 1 unexpected success.

| Selected target | Viewer tree | Result |
| --- | --- | --- |
| `viewer-e66e37b-poll-fix-v1` | `e66e37b` fix | PASS (1 test, about 11 s) |
| `viewer-e66e37b-poll-fix-v1` | `4b4f707` control | FAIL at the pin check (`cc4c7d86…` is not `fdb948ee…`), as intended: the regression refuses a tree that is not the pinned fix |

## Final poll-only candidate 7de601c (harness v4)

Viewer `7de601c1381a791fe9c9ec7f3385a5645724b91e` ("Keep provider polling
recoverable during outages") is a separate candidate, not a descendant of
`e66e37b`. Its parent is remote `main` `8fe25966235c3485fcc4e7f5b258fb756a7bbeac`
("ci: restrict GitHub Actions to main", workflows and tests only). Content proof:

| Commit | `processingWorker.js` git blob (LF) | Checked-out bytes (CRLF) |
| --- | --- | --- |
| `4b4f707` baseline | `e26bd880…f29b5d` | `cc4c7d86…270a6` |
| `8fe2596` remote main | `e26bd880…f29b5d`, identical to the baseline, **so it does not contain the fix** | — |
| `e66e37b` first fix | `96d06a5e…2413bf` | `fdb948ee…06752` |
| `7de601c` final candidate | `5e32e0af…564cdf` | `d08e2512…aadbf4` |

Against `8fe2596`, `7de601c` changes only `processingWorker.js` and adds a Viewer
test. Beyond `e66e37b`'s behaviour, it also retries when building the provider
adapter or credentials fails (`provider_adapter_unavailable`) and when saving
observations fails (`provider_observation_persistence_unavailable`). Because it
changes the reconcile and retry path, the full v1 suite was repeated on it.

Harness v4 (`8ed0979`) moves the plain T9 checks into `FixedPollRegression`, which
each pinned class configures with its own target and pin. It adds
`CoordinatorOutageFixedPoll7de`, run only with
`MATRIX_VIEWER_TARGET=viewer-7de601c-poll-only-v1` and guarded by the `d08e2512…`
bytes. The `e66e37b` regression keeps its own target and pin.

| Run | Viewer tree | Result |
| --- | --- | --- |
| v1 suite | `7de601c` | 7 pass, T9 **unexpected success**, 2 pinned classes skipped. Every record loaded `processingWorker.js` `d08e2512…`. |
| `viewer-7de601c-poll-only-v1` | `7de601c` | PASS. Six refused claims all `queued_upstream` / `provider_observation_unavailable`, then `ingesting`. |
| `viewer-e66e37b-poll-fix-v1` | `e66e37b` | PASS, unchanged after the v4 refactor |
| `viewer-7de601c-poll-only-v1` | `e66e37b` (wrong-source control) | FAIL at the pin check (`fdb948ee…` is not `d08e2512…`), as intended |

Not exercised: the new adapter and persistence failure branches. The relay
models only an unreachable provider.

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
