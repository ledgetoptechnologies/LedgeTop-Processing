# Full Viewer candidate image and runtime qualification

The complete Viewer Dockerfile built from exact committed source
`3c90cebc807ce6bc73028772a7a74bbded502d99` in 99 seconds. Local candidate:

`ledgetop-viewer:qualification-3c90cebc`

Loaded manifest digest:
`sha256:46953ec21272e6ca336f1762335566ff66f01e98598f37b6b45064256c6bdeb8`.
This is the Docker/containerd manifest identity, not a claimed config digest.
No registry tag, `latest`, package visibility or installed service was changed.

The existing task-owned builder identity was checked before reuse. Its existing
CFS quota/period already limited it to one CPU, with 2 GiB RAM and swap ceiling;
an attempted redundant NanoCPU setting was rejected by Docker because CFS period
was already configured. No build ran under an unbounded replacement. The builder
was stopped and verified stopped after the successful build. Other chats'
shared builder was preserved.

The Six Labors key was supplied only through the existing BuildKit secret mount.
Build output stayed in the private artifact directory. A never-started,
task-owned container was exported and its final filesystem scanned in bounded
chunks: 14,189 regular files, 532,479,281 file bytes, zero `.lic` files and zero
matches for either the exact supplied key bytes or their trimmed form. The scan
container was removed after exact owner-label verification. This is a final
filesystem scan, not an all-layer-history claim.

Checks against the actual built runtime established:

- Source revision file/label match; revision file mode 0444; UID/GID 568.
- Schema version 77; source release is disabled by default.
- Obj2Tiles 1.6.2 and ImageSharp 3.2.0 build-info match binary SHA-256
  `a68fd91118342b106e521d064a11066611aadf089b79e67a16503fac082bb155`.
- Poisson reconstruction executable starts and exposes its expected usage.
- The packaged converter runtime qualification and packaged Potree visibility
  verification scripts both passed.
- All 26 focused source-release Viewer tests passed inside this exact image in
  632.939 milliseconds, using only mounted test files and packaged dependencies.
- A normal API child and normal worker child started together in production
  configuration with processing enabled and a fresh temporary database. Health
  and readiness returned 200, a wrong virtual host returned 421, the worker
  recorded its real `app_state` heartbeat, and both stopped cleanly after SIGTERM.

Startup harness corrections were limited to reading the actual top-level
`config.sourceRelease`, using `http.request` for the explicit virtual Host
instead of the fetch harness, and checking the documented `app_state` heartbeat
key instead of guessing a separate table. No runtime gate was relaxed.

Runtime checks used bounded, read-only-root, network-none containers with no
published ports, generated in-memory test credentials and temporary local data.
All task-owned containers were removed. No real job, external provider, user
session or NAS data was accessed. The candidate excludes the Viewer's later GCP
workflow/UI commit and is not full UI, genuine TreeDar, installed staging or
production acceptance. The inactive staging service still needs a coordinated
immutable recipe because its old mutable `dev` image is absent.
