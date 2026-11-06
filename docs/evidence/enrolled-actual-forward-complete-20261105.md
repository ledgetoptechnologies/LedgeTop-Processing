# Actual local Docker forward: 22 phases complete

Operation `5ab0ecec-c12a-4d0f-afb1-821e3d721045` completed all 22 forward
phases, including final worker/custody readiness, authenticated Gate admission
resume and post-resume connected catalog refresh. This is an **actual local
Docker component qualification**, not installed-host, connected SSH/systemd,
NAS, native photo processing or current-release deployment proof. No rollback
was run, and previously failed operations were not reused or reconciled.

## Immutable inputs and execution

- Controller source: exact reviewed root `a1c2bc9d89d7d309a6baf8403eec5c4ce844f355`.
- Frozen archive SHA256: `551b0422e2dbe7c3261630bd3fc97943b1b6e9ae8dfcc8777ec82118d1e7f0d9`.
- Source inventory: 722 files, 8,884,917 bytes; manifest SHA256
  `5728ea689bd7d971b38b37492c4b13730e41c768d1850b283fac033deaa7d31c`.
- Actual candidate image: `sha256:378559e42456017b965d1e2e38b56eb627274734aeb317a758d84c0bc99bb5ab`,
  packaged Processing source `18750d4d599026c18e569a9c67135e6c9fdc7d1a`.
- Genuine predecessor image: `sha256:7fae9385fb5ca829783fe05eef2fdb1983abf4d491b2b9f795e55b617a5a0004`,
  source `693cb7eca32295da959a4f42399f317577ef8a8a`.
- Native Docker CLI SHA256: `6c71cfe71faff949a602946a4df4799b563232e79d0b8b6fd343d0ef081c9621`.
- One-shot launcher SHA256: `d63b24c5cc47ee3276a1cb58ca5d8155c5601a003540c6002d36b70471e1f6e1`.

The launcher verified archive/CLI/all frozen file sizes and hashes before its
durable O_EXCL dispatch marker. It ran once after an explicit coordinated quiet
slot; initial running inventory contained only the preserved shared builder.
Outer container `2c62a652605188a9dd167ff9ec4492043786aedf0b51b3c37d5b3eb4a6941183`
started `2026-11-06T01:04:17.779199031Z`, finished `01:09:00.69618718Z`, exited
0/PID0/no OOM: **282.917 seconds**, inside its unchanged 900-second bound.
It used one CPU/768 MiB, UID10001, frozen read-only source, private artifacts,
no published host ports and bounded inner containers. Handle 90730 was observed
through terminal completion; no mutation retry or start-window refresh occurred.

## Accepted phase sequence

1. restart-disabled
2. predecessor-stopped
3. original-lease-holder-created
4. original-leases-held
5. custody-verified
6. backup-verified
7. original-leases-released
8. bootstrap-created
9. bootstrap-running
10. bootstrap-freeze-intent
11. bootstrap-draining
12. bootstrap-frozen
13. bootstrap-stopped
14. frozen-original-leases-held
15. frozen-transform-verified
16. frozen-original-leases-released
17. predecessor-retained
18. final-created
19. final-frozen
20. final-ready
21. admission-open
22. complete

Production journal, driver, Docker transport, kernel original leases, tracked
observers, application Gate, declared startup policy/projection, custody and
immutable TAR validator were exercised. The startup-only refused-connect wait
passed both actual bootstrap and final application starts. Final remained
frozen until readiness/projection checks passed. The fixture retained exact
legacy table/file projections rather than restoring a SQLite backup.

Both workers came from the real daemon factory/WorkerService/ReaderSession/TLS
stack, with manifest-bound source/config/instance/generation/engine identities.
After resume the actual final app reached both WorkerCatalog API endpoints:
cluster `maxImages=1000`, main `maxImages=null`, both ready and queue count 0.
Adding the fixture backend catalog flag changed the coordinator option set and
fingerprint through the live relay; no hardcoded readiness DTO was substituted.

Explicit fixture seams remain: task-owned 202-file original state, test-scoped
enrollment identity substitution, Docker Desktop read-only mode exemption,
local process control instead of connected SSH/systemd, and synthetic native
engine/coordinator transports. Host enrollment and installed qualification are
false. Packaged app source 18750d4 differs from the later controller checkpoint;
this receipt must not be relabeled as a full latest-release proof.

## Preserved receipts and resource disposition

The complete operation/config/plan/bundle/journal/worker material and controller
logs were exported privately after successful terminal completion: 276 files,
1,359,093 bytes. Private file-manifest SHA256:
`0e5f3499155a60abfa84198f79e1b482778899981595ee8d50cefef345a9aaaf`.
Controller stdout SHA256:
`5e606f063f7afacc0161bc76898c4802edfe46654df3aa46c8891824199e14bf`.

The cold TAR was independently streamed through the unchanged keyed validator:
202 original files, 13 directories, 262,429 content bytes, 467,968 archive bytes;
archive SHA256 `9333efc3f6485d61f4671442fa40f6184c9a2c1628208f66f4453b400abb47a7`.
TAR verification proves retained bytes, not original authority or permission to
restore; the separate original-boundary proofs provide lease/writer evidence.

The successful harness retired its exact owned inner containers and volumes.
Both transport task-label inventories were verified empty. The stopped outer
and its one artifact volume remain retained pending root review; no additional
retirement, rollback or new operation is implied by this receipt. Private host
evidence remains independent of those disposable resources.
