# Integrated custody and spool rollout inputs — 2026-11-07

This records an inactive candidate and non-mutating validation only. Neither
the running coordinator nor its service-owned config mount was changed. No
worker was installed, no legacy gate was filled, and no cutover was started.

## Exact controller and candidate

The current integrated controller source was exported from commit
`403986bf456db090c12c4c5ef8cce0248b93f5c6` to the private cluster directory
`photo-pilot-coordinator-inputs-20261106/controller-integrated-403986bf-lf/`.
Its LF Git archive SHA-256 is
`5ebb732f76972772bf11571635ee4d7e503ac4d4ce118fc8cb8a482959ca0b9c`.
The initially created CRLF export failed pinned-source validation and is not a
qualifying source. The LF export matched all 15 reviewed startup-policy source
hashes and generated the existing e277 candidate policy byte-for-byte:
`2cc5b8c47e3fdf7665a347a0f01e157baab7a61ff73b4c547f3cf060e1342fcc`.

A separate freshly extracted source tree and launcher-compatible archive are at
`photo-pilot-coordinator-inputs-20261106/controller-exec-403986bf/`. The archive
has the `source/` root required by the launcher and SHA-256
`67c9fd5bfa17bacc72c2beea07e131143c409cd30b2ac0e2d755560f904b7e9f`;
the launcher SHA-256 is
`5f1ae6c622af59d2d178172baa5fa2e2fe0bbba4750b7ae9708240d6bfbfa734`.
The independently pinned launcher verified that archive against its source tree
and invoked `--help` for both supported controller modules through the private
Python environment. This is not a complete-plan execution or runtime readiness
proof. The launcher, interpreter and dependencies require independent pinning
at any later invocation.

## Input corrections and fresh observation

Actual candidate-input validation found that the earlier environment files
explicitly included image-default `PATH`, `PYTHON_SHA256` and `PYTHON_VERSION`,
which the controller rejects as loader overrides, and omitted the required
`PYTHONNOUSERSITE=1`. New private env files omit those overrides and set the
required flag; original files remain unchanged. New operation
`3b78bff3-c4d0-40e0-bcc1-f526a2f2be3c` uses regenerated recipe bindings.
The reviewed predecessor schema was also copied byte-for-byte to a new mode-0600
file because its original mode 0664 fails the private-artifact guard. Its SHA
remains `aa5e64085b3b254e9eae7ee6d52e57b1be5dd6e1a95a02a4b02f96598fe3784b`.

The 403986bf typed local-spool observer ran as UID 10001 in the exact e277
candidate image on the actual cluster spool bind. It checked the pinned marker,
filesystem identity, a 10 GiB reserve plus 1 GiB requested capacity, and a
4 KiB fsync/readback. The host-side verifier accepted the typed record; the
temporary qualification containers and volume were removed. The private record
`candidate-e277-sanitized-3b78bff3-c4d0-40e0-bcc1-f526a2f2be3c/spool-observation-dfd84bb6401a.json`
has SHA-256 `f84a86498321660db84b412224f1ff264f96da9ffc65cacffa68f6baeb88f4c2`.
This is point-in-time evidence, not an installed pipeline, reboot or power-loss
test; the runtime must recheck the spool immediately before admission.

## Incomplete receipt-bound draft

The private `receipt-bound-draft-v2/` under the sanitized candidate contains
schema-validated plan and bundle inputs. The plan SHA-256 is
`e4ed51846dc170487c7506c5e2f4a0661efcbb8902c26a55f3b15f3f588d6d6d`,
bundle SHA-256 is
`1194b0d254ce6a555334b579673e2a92cf61344f3b7fc38097db16200c72f46f`,
and the genuine preparation-only worker review SHA-256 is
`0cb244471bde98922f0b5307888a3675e6fb52a4ccd481830323b027fe8a80d0`.
The integrated controller validated the actual worker review, private input
hashes, sanitized environments, policy and spool receipt. The validation read
candidate config input files through an explicitly supplied callback; it did
not read service-staged config or prove that stage occurred. Workers remain
pinned to their independent 4705 manifests.

The legacy-delivery gate remains null. The historical-pilot input is static
and still needs its post-stop namespace/custody check. Fresh drained counts,
complete backup, exact controller/interpreter binding, worker installation and
readiness, service-owned config staging, coordinated Viewer switch and rollback
are still required. Do not execute this draft or treat a successful structural
validation as installed acceptance.
