# Coordinated presentation and Viewer qualification — 2026-11-06

## Exact selected source

Processing presentation/recovery combination `c9f552788f97ef1c3ba1d32eb0276a24fc93c7aa` was selected at integration `c3d7eac8`. Root independently verified no difference in `processing`, `ops` or `tests` between those revisions. This preserves both the pre-reservation/claim and start-witness process-death checkpoints.

Viewer draft Ops #446 now points to `ca5eedf3549a8d3c1463f9c3cede7c3f696d41a3`, ordinary fast-forward from `76f7c112`. Its source-binding, ambiguity-hold and rendered disconnect corrections passed independent source review. The canonical 84-vector fixture hash is `1977eab229b0cabb71aef94f82b142c8360ccd932a3bca84634a8f91c0716f71`; its LF pin is verified in the exported archive. No feature-branch Actions run was created.

## Separate test selections, not an aggregate suite

| Frozen inputs | Selection | Observed outcome |
| --- | --- | --- |
| Processing `68888a30`, Viewer `ca5eedf3` | Presentation, consumers, shared vectors, real process deaths, actual Viewer HTTPS intake/replay, processing recovery, fetch process restart and runtime | 271/271, 83.013s, no skips |
| Processing `c9f55278` after the non-blank recovery correction | Entire processing-recovery and job-presentation modules | 193/193, 17.481s, no skips |
| Viewer `ca5eedf3`, Processing `68888a30`, Windows host | Four Viewer modules including actual Node signed receipt to Python verifier | 25/25, 1735.8424ms, no skips |
| Processing `c9f55278`, Viewer `ca5eedf3`, corrected Linux disk/dependency runner | Whole-object source/result/lost-copy-ACK/cache cleanup round trip | 1/1, 10.252s |
| Same corrected pair | Three range/GCP result round trips, bootstrap without worker catalogs, and existing TreeDar compatibility test | 5 run: 4 passed, 1 TreeDar cleanup assertion failed, 32.003s |

All four selected photo-result round trips pass across the last two runs. The fifth-case TreeDar failure is preserved: `test_results_compatibility_reads_before_acceptance_then_pins_block_cleanup` expects one partially accepted object to become eligible for cleanup, but the actual candidates remain empty. Data is retained. Claude is auditing the expectation against full-closure/reader retention; do not relax cleanup to obtain a green result.

These exercise actual Viewer/Python code, loopback TLS, signed receipts, SQLite, interruption/replay and native multipart handling with synthetic photos/results and a synthetic engine edge. They do not reconstruct a real map, qualify current licensed conversion/publication, prove installed NAS durability, or establish physical-node operation.

## Frozen artifacts and logs

- Processing `68888a30` archive SHA-256: `e4876acb4549f026fa6f6c2e952e7a8b577ca4795ca35b4c572aedf399f53864`.
- Processing `c9f55278` archive: `6c7a7dea131ea5a52a186939a25a2b4a0d260e8ba575417f835367dfb92c555b`.
- Viewer `ca5eedf3` archive: `9a5bbc65766a673c775c85c94c87a2b1bfb68a54ec96ce8f080e3d0ae1386abc`.
- 271-case log: `C:/Users/fstor/AppData/Local/Temp/ltds-pair688-ca5-qualification-20261106.log`, SHA-256 `0992623aebf91a4ea4fbfc9729e76b56949218d9209465ac127944a0deeb9ad6`.
- 193-case log: `C:/Users/fstor/AppData/Local/Temp/ltds-recovery-c9f55278-20261106.log`, SHA-256 `98f99ab3af4a536abe458b45da9f82ffc474544f1a7102714916c7fe376eaeb5`.
- One-case result log: `C:/Users/fstor/AppData/Local/Temp/ltds-result-c9-ca5-esm-20261106.log`, SHA-256 `c340f1b414d909034fee11ae4ee5c7f8ad231dccfe532729bdbea93f2cdd36c1`.
- Five-case result log: `C:/Users/fstor/AppData/Local/Temp/ltds-result-c9-ca5-esm-rest-20261106.log`, SHA-256 `d9959291405e83451b1664311d783fe77ae5e1806f2d7b18d2575cf984a3fc3c`.

Linux runners used cached image `sha256:378559e42456017b965d1e2e38b56eb627274734aeb317a758d84c0bc99bb5ab`, one CPU, 768MiB ceiling, 128 PIDs, network none, readonly source/root, no capabilities and no Docker socket. Viewer runtime dependencies and Linux Node were reused read-only. This image supplies the test interpreter/dependencies, not the newly installed application.

## Failed runner attempts retained

The first four result cases all failed on the 160MiB temporary filesystem's real ingestion headroom guard (38.695s). A disk-volume rerun still failed (37.775s): its root-owned volume was unwritable to image UID10001, so Python fell back to the small temporary directory. An observation-only statfs preload identified that exact fallback; it changed no returned values. After assigning only this task-owned volume UID10001/mode0700, the next four cases reached ingestion but failed ESM resolution for geotiff (39.807s). NODE_PATH resolves CommonJS dependencies, not ESM package imports. Mounting the existing dependency cache at the actual Viewer node_modules path fixed resolution. No source, expected result, storage guard or dependency version was changed for these corrections.

Retained logs respectively:
`ltds-result-c9-ca5-20261106.log` (SHA `e8a49428951f868bf94ba21f9eb4adaf04cb619933d2dfd3df0760d99d47ec03`);
`ltds-result-c9-ca5-disk-20261106.log` (`c3dbd125418869572e42a103fe733e6112ef44f8421939470f46a61c2a672686`);
`ltds-result-space-diagnostic-20261106.log` (`9598542b43ad90deaccac561449b4545f8b7a41595e294c63e3646795cfb2412`);
`ltds-result-c9-ca5-disk-fixed-20261106.log` (`8ebe21ae9e5cc895e316e6f1cedac79ffe78a2bd120e2f63b24fefa6fefde9d3`).
They are under the same host Temp directory as the passing logs.

Every named task runner is terminal and absent. The exact labelled task volume `ltds-result-c9-ca5-disk-20261106` was identity-checked and removed; its label inventory is empty. Shared BuildKit was preserved. No live PC service, NAS permission, client source or production credential was changed.

## Remaining gates

The new preparation-only helper `7bd22968` and evidence `b14de3da` are selected at `c57c99e8`; its independent review and 14/14 synthetic Linux checks are recorded separately. It has not generated installed credentials or configs.

Rebuild/pin the combined application, prepare actual private bootstrap/final and worker artifacts, verify NAS/Viewer CA and signer configuration, install with the reviewed cutover and activation boundary, and qualify the small real job and concurrent-node queue. Rendered UI, installed maintenance, genuine TreeDar output registration and the full-goal acceptance remain open. Source cleanup remains disabled for the first installed pilot.

## Later fixture and artifact closures

The TreeDar assertion above is superseded only by test-only `07f25a4e`, selected
at `ae8e259d`. Claude reported all18 cases passing, no skips/failures, 0.898s,
in a bounded readonly-source Linux fixture. Root reviewed the patch: it first
establishes a signature-verified ledger fixture before expecting read-pin
release to permit cleanup, and adds an explicit unsigned-acceptance refusal.
Production cleanup is unchanged; genuine signatures remain the worker verifier's
responsibility. This does not establish usable real TreeDar output registration.
The committed test blob SHA is
`53a89790531935e07b7139b3c38b1f0fe82930c0ceee159a1b9ede4904801769`;
Claude's tested CRLF export SHA
`2cfe2839b5078a59e55d669b666c93bfa8b8e0af0bdb1426368adad8a8611868`
was independently reproduced from those exact LF blob bytes. Preserve the byte
representation distinction and the earlier failed run, rather than relabelling
it as green.

The canonical LF image is now built and staged as recorded in
`canonical-build-and-pc-staging-20261106.md`. Its current-image forward22 and
separate rollback30/audit pass, with complete independently revalidated cold
evidence and retired task resources; see the two canonical cutover receipts.
The noncircular host/Gate configuration-preparation path `11e470d6` now has19/19
Linux cases, including actual runtime-to-worker composition. Installed private
inputs, NAS durability, Viewer application/trust and real job acceptance remain
open.
