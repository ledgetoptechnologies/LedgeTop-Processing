# Coordinated Processing integration readiness

This record describes the review branch
`codex/viewer-accepted-copy-integration-candidate-20261103`. It is not a deployment
approval. Runtime acceptance remains incomplete.

Identity follow-up `572cf56` integrates duplicate-origin/daemon-ID checks and
requires daemon bindings for multi-node startup. It preserves schema-v3/v4
result-delivery configuration while requiring their recovery bindings too.
The 121-test Linux node-role/runtime/driver/daemon-routing/updater suite passed
without skips. A node-local live identity probe and physical-host binding
remain missing.
The Ops #430 `50ddd6e8` HTTPS/Python source-range pairing was rerun against
this identity follow-up (`572cf56`, documentation head `08e4e9d`): 20/20
passed with no skips. The earlier 20/20 result below applied to code `c7aa1ce`.

Local image `ledgetop-processing:integration-8422a47` was built from repository
head `8422a47` (code `572cf56`) with the pinned Dockerfile/dependencies. Local
image ID reported by Docker inspection is
`sha256:e616f6f4393dddc0a5536b0551a1cf54313f309de5537f9efe633ff58241c881`.
A bounded container with no external network and a read-only root filesystem
started the app and returned HTTP 200 from `/healthz` as UID 10001; cryptography
50.0.2 loaded. The child process and container exited. This checks basic image
startup only, not configured reference dispatch or a deployed PC worker.

## Integrated source

Code commit `c7aa1ce66e91b3b3c985ed3d5789d7527f21402f` combines:

| Component | Exact source | Review |
| --- | --- | --- |
| Durable result sender, signed receipt, cleanup predicates and guarded updater | `70312a4d4887f9546930facb854ca5c6b5d6ba91` | Processing #339 |
| Ordered routing, healthy-node fallback and per-node claim revalidation | `1d24c14a6924d9dd09d31c7265ec5cf3f30a1c43` | Processing #364 / #365 |
| Resumable source ranges, renewal intent and acknowledgement replay | `e10326bf3d2003fb7542afe7e52fdcf6bf6dc4ba` | Processing #342 / #349 |

The integration retains both `viewerReceiptKeys` and `sourceRangeV2Enabled`
worker configuration options. Source-range execution is an explicit opt-in;
its driver code exists, but a qualified source producer and NAS custody path
are not operational.

## Evidence on the combined code

- Ubuntu 24.04 / Python 3.12, unprivileged `pilot`: 186 tests passed with no
  skips across store, runtime, driver, daemon routing, source ranges, Viewer
  transfer/receipt, retry worker and coordinator updater modules.
- Ops #452 at `9e52371c02f0798e294b583bbe2cbd07e46585e9`, Linux Node 24.22.2
  and Python 3.12: two actual Python/Node paired tests passed without skips.
  The first covers v1/v2 result acceptance and lost-commit/status replay. The
  second kills both processes after a lost commit response, reopens the real
  Processing Queue and Viewer SQLite/filesystem ledger, recovers the accepted
  result and checks persistent nonce replay rejection.
- These are synthetic process-crash tests. They do not prove power-loss
  durability, production TLS, TrueNAS custody or installed worker services.
- Ops #430 at `50ddd6e88bd1e26b0cd7cfb2bfbebcec77de987a`, incorporating
  broker #429 `d2cb01a`, paired with this integrated Processing code: 20/20
  source-range tests passed without skips, including actual HTTPS Python
  interruption/restart, bounded range reads and source corruption checks.
- Temporary Node executables and export containers used for pairing were
  removed after the processes exited.

## Remaining delivery gates, in dependency order

1. **Node identity:** reject duplicate node origins and duplicate pinned engine
   IDs; verify the local identity before mutation. Docker Engine ID identifies
   a daemon, not a physical PC. Establish a host binding and enforce a single
   reservation per physical host before enabling concurrent dispatch. The
   configured identity checks are integrated in `572cf56`; they do not provide
   live daemon verification or physical-host attestation.
2. **NAS source custody:** implement the authenticated source producer, immutable
   source generation/manifest, physical capacity reservation, writer/purge fence,
   independently verified custody, recovery and explicit source release. Ops
   issue #427 owns this missing path. Beau controls all TrueNAS changes. The
   selected spool subfolder under existing Model-Viewer Storage is not yet
   provisioned or ACL-qualified.
3. **Source pairing:** the current #429/#430 broker and integrated Python client
   are paired synthetically as recorded above. Qualify authenticated source
   production, grant renewal and source custody as one installed path; the
   passing range test does not establish those missing upstream components.
4. **Result custody and cleanup:** qualify runtime credentials, persistent mounts,
   exact acknowledgement replay, disconnected delivery, disk-full/corruption
   handling and cleanup against the actual installed services. Source release
   and result acceptance are distinct operations. Unknown outcomes retain data.
5. **Maintenance:** rollback is supported only before nodes reopen for work;
   `reopeningStarted` prevents later rollback. Do not interpret conservative
   rejection of new receipt data as permission to delete it for a downgrade.
   Qualify unattended drain/update/health/resume with persisted queues/outboxes.
6. **TreeDar and future engines:** genuine LAS/LAZ admission and the complete
   typed output closure still need real runtime qualification. TreeDar stays on
   main; Gaussian splats remain disabled. Jobs on one host must not overlap
   between engines.
7. **Viewer experience:** review/deploy the current-main hierarchy/UI candidate
   Ops #406 and verify customer/project/task navigation, queue reasons, selected
   node and logs. Historical Claude co-authorship does not prove current review
   ownership. UI completion does not qualify the processing pipeline.
8. **Installed acceptance:** the last read-only checks found reference worker
   services inactive on both processing PCs. Verify credential rotation,
   immutable installed builds, storage and active workers; restore the staging
   service/grant prerequisites; then run synthetic failure cases and a small
   real dataset. Large client datasets remain unqualified.

Production outranks waiting staging jobs without interrupting running jobs.
Routing prefers the first eligible idle node, with cluster capped at 1,000
photos and main uncapped by photo count. The current user requirement is one
job per physical host, allowing distinct hosts to run concurrently. Capacity,
engine eligibility and maintenance must be rechecked atomically at claim.
