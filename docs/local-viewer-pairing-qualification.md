# Local Processing / Viewer qualification

Use immutable exports of both candidate commits. A passing test against an older
image or a moving worktree does not qualify the candidate. Keep the Processing
working directory at `/processing` so Python does not import the image's older
`/app/processing` package instead of the mounted candidate.

## Dependencies and mounts

Record the Node version, Python image ID, both source revisions, archive hashes
and dependency lock hash. Reuse a dependency directory only after its
`package.json` and `package-lock.json` match the immutable Viewer export. If
they differ, create a separate cache using `npm ci --ignore-scripts --omit=dev
--bin-links=false`. Do not modify another chat's worktree or dependency directory.

Use these read-only mounts in the same bounded test container:

| Source | Container target |
| --- | --- |
| Processing candidate | `/processing` |
| Viewer export, containing `apps/viewer` | `/viewer` |
| Exact installed dependency directory | `/deps` |
| The same directory's `node_modules` | `/viewer/apps/viewer/node_modules` |
| Verified Linux Node executable | `/node` |

Create an empty `apps/viewer/node_modules` mountpoint only in a task-owned export
when needed. The nested dependency mount matters: CommonJS fixtures outside the
Viewer tree can use `NODE_PATH`, but production ES modules resolve packages from
the Viewer tree and do not use `NODE_PATH` for package resolution.

Set:

```text
PYTHONDONTWRITEBYTECODE=1
PYTHONPATH=/processing
LTDS_VIEWER_WORKTREE=/viewer
LTDS_VIEWER_APP_ROOT=/viewer/apps/viewer
LTDS_NODE=/node
LTDS_VIEWER_NODE_MODULES=/deps/node_modules
NODE_PATH=/deps/node_modules
```

Keep concurrency conservative: one CPU, 2 GiB memory, a bounded PID count and one
qualification runner at a time. Use a read-only root filesystem and disable
external networking; actual HTTPS tests use loopback and generated test keys.

## Preflight before the suite

In the **same mount and user configuration** as the test run, verify:

- `/node --version` reports the recorded Node version.
- CommonJS resolves `express` and `unzipper` through the intended cache.
- An ES-module import of
  `/viewer/apps/viewer/server/explicitSourceUnitInspection.mjs` succeeds.
- The synthetic filesystem permits owned temporary directories and file/directory
  fsync, and has at least 1 GiB available after the fixture's storage reserve.

The real ingest path deliberately refuses an expansion budget below 1 GiB.
Putting all fixtures in a 512 MiB `/tmp` tmpfs cannot qualify ingestion. Prefer a
dedicated, labeled disposable Docker volume for `/tmp`; do not weaken the
production free-space guard or confuse temporary disk capacity with the
container's memory limit.

## Explicit controlled-conversion qualification

After separately building and verifying the pinned converter, mount its complete
artifact directory read-only at `/opt/obj2tiles`, including `build-info.json`.
Set these additional variables:

```text
LTDS_RUN_CONTROLLED_CONVERSION_QUALIFICATION=1
LTDS_OBJ2TILES_BIN=/opt/obj2tiles/Obj2Tiles
HOME=/tmp
DOTNET_BUNDLE_EXTRACT_BASE_DIR=/tmp/obj2tiles-bundle
```

The derivative guard requires more than 5 GiB free, so use an adequately sized
owned disposable disk volume. Run `tests.test_viewer_result_v2_roundtrip`; all
three cases must execute without skips. Explicitly requesting qualification
with missing converter/Viewer/Node configuration must fail, not report a green
skip. Normal staged-only tests do not require an installed converter.

The opt-in case runs the real converter, auditor, verifier and publication API,
then the genuine published-session and asset routes. It checks publication
replay, binary child delivery, private OBJ exclusion and session revocation.
It does not replace a rendered-browser check or installed photogrammetry test.
Do not use an artifact with unresolved dependency/license release gates for
client processing merely because this controlled synthetic case passed.

## Evidence boundaries and cleanup

Require zero skipped paired tests. Provider health and a successful metadata POST
are insufficient: assert nonempty source uploads, completion and object receipts,
the actual Viewer worker transition, and durable result-copy acknowledgement.
The two-job fixture must submit both jobs through ordinary `processOne` claims
without manually increasing a provider admission limit.

The synthetic result fixture verifies its PNG/GLB/EPT structure, then executes
actual archive ingestion and hash registration. It must leave the required
`mesh_tiles` conversion pending and the model unpublished. A ready/openable
published result still requires the controlled converter and verified derivative
receipt. Fake engines and injected mount/physical probes do not qualify installed
photogrammetry, TrueNAS durability or physical host exclusion.

Track runner/container identities and volume ownership labels. After the runner
is terminal, verify its container has exited and remove only its owned volume.
Keep artifacts needed by active peer tests; never prune shared Docker resources
or retry a previously rejected cleanup through a different mechanism.
