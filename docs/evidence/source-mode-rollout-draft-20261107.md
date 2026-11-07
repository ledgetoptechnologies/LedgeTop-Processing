# Source-mode rollout draft — 2026-11-07

This is an inactive, private-input validation draft. No coordinator service,
service-owned config, worker, or TrueNAS Viewer was changed. It is not ready
for cutover and does not prove installed scheduling or recovery.

The external controller is the exact LF export of commit
`56e67ddb36cba651fea5d40eb3f179b982a81889`, staged under
`photo-pilot-coordinator-inputs-20261106/controller-exec-56e67ddb/` on the
cluster. Its launcher-compatible source archive SHA-256 is
`f1348c8cea245718bb56269dfea0e24f2d9bb2b69b5abb2be8b265fcdc523ef9`;
the launcher SHA-256 is
`b34cb3d20345f2defd078293a83f2af83a8e122d80a84fff340d460c42b081e0`.
The pinned launcher validated its archive/source and invoked `--help` for its
three permitted modules; no controller operation was performed.

Candidate operation `5baeb96d-6824-42cf-925c-495c34f47cdc` retains the
revision-labeled e277 runtime image
`sha256:038f822dc96f7db03bf5a8f0d7be7ceb171cba25ad77139114735673e3f6d557`
and the separately pinned 4705 workers. Its mode-aware 17-source startup
policy SHA-256 is
`37fb0b5b437ea6c32202d57cc4e510f000563c964ddca396e7704e202729e56e`.
The exact candidate image independently regenerated the policy byte-for-byte
using read-only inputs and no network. Bootstrap and final recipe SHA-256s are
`3503bb6a91e1530811a5b937a8c330017942ca178917d0d8cd3d1f83a3052fb3` and
`3aab2479fca08fee016ba504529f1e995229c0bbd50eb5686b7ad1ee0ae06f04`.

The new final recipe was bound to a fresh typed observation of the actual
cluster spool, SHA-256
`a4ecdd097d852cb6d1b5885f373ebb599a4c5f13a3dc98d54085a4d8cad40c7e`.
A fresh read-only legacy capture under the same runtime and controller helpers
has SHA-256
`057fb2f7a79febbfdf58a9fb168821d8c8b4849cb421b5037d8ccdeb7673d6b7`.
The isolated containers and temporary volume were removed. These observations
are provisional: the spool requires admission-time revalidation, and legacy
metadata must be compared against the actual post-drain frozen custody baseline.

The private directory
`photo-pilot-coordinator-inputs-20261106/candidate-e277-source-mode-5baeb96d-6824-42cf-925c-495c34f47cdc/full-input-validation-draft/`
contains a plan with SHA-256
`088bb24be74dd8d7b120aa88eaf20fe9b7ed25ec1008ead66000c0af1074e70b`
and bundle with SHA-256
`9aa06e73a7367250552d99d1525533967a6771e21dbb0eeb6b4352bd80056a6b`.
The controller validated the populated private inputs, including the typed
worker-preparation review, spool and legacy records, source pins and policy.
The config callback read candidate input files, not service-owned staged
configuration. Current database counts matched the candidate counts in a
read-only check, which is not a drained-state census.

The full frozen custody/backup, historical pilot namespace check, worker
enrollment and authenticated readiness, service-owned config staging, live
legacy delivery, coordinated Viewer switch and rollback remain necessary.
Do not execute the draft as a substitute for these checks.
