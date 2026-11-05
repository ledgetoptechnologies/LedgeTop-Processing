# Coordinated Processing integration readiness

This record describes the review branch
`codex/viewer-accepted-copy-integration-candidate-20261103`. It is not a deployment
approval. Runtime acceptance remains incomplete.

## November 4 combined qualification and inactive host preparation

Runtime code `8ff9a7276493c383f9ef250aeead4fca42117fba` paired with an immutable
Viewer `66031588eb465c8e197acc9db9da3569933a33a3` archive passed **205/205 Linux
tests, no skips**, using Python 3.13.17, Node 24.22.2 and pinned Python runtime
dependencies. The Viewer archive SHA-256 is
`7d0d74625d6041c934b06a005e3160f714771188064000d1c34fdf57ae1f53f5`;
its package/lock bytes matched the isolated `npm ci --ignore-scripts` dependencies.

- Both whole-object and opt-in range-V2 Driver paths pass the actual Viewer
  source-client/outbox, HTTPS spool broker, ReferenceDriver/EngineWorker,
  durable result outbox, worker result API/HTTPS proxy, Viewer result-copy signer,
  Store acknowledgement replay and worker engine-cache cleanup roundtrip.
  Viewer `processSubmit` resumes the accepted source intent and automatically
  queues reconciliation; the fixture uses its normal delay and claim. Lost
  POST/PUT/completion and copy-ACK replies recover without a duplicate engine
  initialization, changed receipt IDs or deletion of the Viewer copy.
- Real transport qualification exposed and fixed dropped ETag/request-ID
  headers, rejection of Viewer's bare canonical dataset UUID, and missing
  configured private-CA trust in the Driver's range client. Signed manifests
  are not rewritten, legacy prefixed identities remain stable, and TLS
  verification stays enabled.
- Range grants/renewal/ACKs use the existing protocol, persist cumulative budgets
  and exact replay, and remain bounded by the fixed reservation deadline. Short
  grants last at most six hours; the existing worker reservation cap is 24 hours.
  Unfinished work holds at that deadline. New range journals, pins and token
  records are included in the metadata quota; quota failure rolls back growth,
  while exact ACK replay does not add metadata.
- Signed V2 intake/receipt tests exercise small-first cluster/main claims and
  leave oversized work queued while an idle cluster takes a later eligible job.
  Counts are scaled to a one-image cluster ceiling in those fixtures; the
  configured installed ceiling remains 1,000. Logs now cross the existing
  public API, fixed-worker proxy, authenticated result reader and execution
  router. A bounded native JSON-prefix reader pages through large synthetic
  tails without downloading the full remainder; TreeDar logs stay unavailable.

These are synthetic execution/transport checks. The native engine, physical
engine probe and mount qualification are injected; Viewer storage resolution
and capacity use a scoped test adapter. The test preserves actual filesystem
copy/fsync and database/outbox/signature code, but does not prove TrueNAS
durability, real photogrammetry, model ingestion/publication, live logs or an
openable result in the installed Viewer. The large-dataset transfer/capacity
boundary and the P1-P13 installed acceptance matrix remain to be qualified.

The exact source is privately staged, inactive, on **both PCs** at
`~/.local/share/ledgetop-processing-reference/dependency-candidates/8ff9a72/source`.
Archive SHA-256:
`cd808291d97314d33f57d70b1dfe5b8a290b6a821cc51b3a0f620905952717ee`.
Using each host's previously prepared `39e0825` dependency venv, all **103**
Processing/ops Python modules parsed and worker/runtime imports passed; **24/24
focused validators per host** passed without skips. Main uses Python 3.10.12
and pycparser 2.23; cluster uses Python 3.14.4 and pycparser 3.12. Both retain
cryptography 50.0.2, cffi 2.1.1 and rfc8785 0.1.4. Each private preparation receipt
records `workerStarted: false`; no current-version link, service, live config,
system package, engine container or TrueNAS setting was changed.

The local coordinator image built from this runtime revision is
`sha256:ed1800bcf8e0eda89ff4f1eb16d98faf7896c8111240fc917801f53432fba73c`.
It passed a network-isolated, read-only UID-10001 import/health-200 smoke check;
the task-owned container and listener were removed/joined. It has not been
published or installed. Reference workers remain an installation gate.

Viewer owner reports 46 focused status/UI/outbox checks at `66031588`, including
persisted node/phase metadata, queued pre-engine phases and progress retention.
Rendered installed acceptance and real ingest/publication remain separate gates.
An exact-lock production dependency audit also reported body-parser
`GHSA-27r3-jxmh-p828` (high, `<1.20.9`); the Viewer owner has been asked to review
the targeted fix before release. No broad dependency update was performed here.

**Immediate Viewer admission code gate:** at `66031588`, provider creation,
refresh/probe and background health still use the legacy `NodeOdmProvider`.
That requests `/info` and query-token `/options`; the coordinated TLS API uses
Bearer-authenticated reference capabilities/options, with the legacy facade
correctly disabled. Fresh health/options are required before HTTP task admission,
so inserting a provider directly in the wire fixture does not prove the UI can
schedule work. The Viewer owner has the concrete shared-adapter/bootstrap and
HTTP acceptance test requirements. This gate must pass before installed Viewer
job scheduling is described as ready; it must not be bypassed by enabling the
legacy executor or fabricating cached health.

## November 4 worker integration checkpoint

### Actual client and host preparation checkpoint

Code `39e0825` passed the expanded **273/273** Ubuntu/pilot combined suite with
no skips, including the V2 metadata budget, worker validation, grant broker,
outside-lock verification/CAS, runtime and service-unit checks. Later source
grant corruption/clock hardening `c45e8b3` passed its 18-test intake module on
Ubuntu; it rejects orphan token history before scanning and at final CAS.

Actual Viewer candidate `b6f03f31208dce2b98314183b7fbc5d5aab45e26` paired with
Processing test integration `e06ccc1` passed once on Windows (Node 24.16/Python
3.13) and once on Ubuntu (Node 24.22.2/Python 3.12), with no skips in either
pair. The test uses real Viewer SQLite migrations, ProcessingRepository,
PreparedSubmissionOutbox, preclaim signing and SourceIntakeClientV2 against the
actual loopback HTTPS control/API/Store/spool. Lost replies after committed POST,
PUT and source completion recover with exact immutable request replay, ACK-based
upload resume, strict persisted receipt replay and the `1e-7` option. The spool's
mount check is explicitly simulated: this proves the wire/persistence seam, not
NAS custody. Listener/child cleanup is asserted; the temporary Linux Node export
container and binary were removed after testing.

The same code/dependency profile `39e0825` is privately staged, inactive, under
`~/.local/share/ledgetop-processing-reference/dependency-candidates/39e0825` on
both PCs. Its source archive SHA-256 is
`d8024c99a411ab646fbba8974d68c8e0d1867ed9104c4cf23df58c74971f4e5e`.
Main Python 3.10.12 and cluster Python 3.14.4 both imported the worker/daemon and
passed 25 selected validator tests with two explicit optional Node checks skipped
(27 total per host). Private dependencies are cryptography 50.0.2, cffi 2.1.1,
rfc8785 0.1.4, and pycparser 2.23 on main / 3.12 on cluster. No system Python
packages, running containers, active-version symlinks, service units or NAS
configuration were changed. These are preparation artifacts, not active workers.

### Source-bound result acceptance checkpoint

Combined code through `32ac9c4` includes the exact signed
`ltds-viewer-accepted-copy-v2` verifier, authenticated source/preclaim binding on
private worker jobs, Store acceptance and exact receipt replay, and worker
acceptance/cache-cleanup verification. V2 source jobs cannot use unsigned or V1
copy acknowledgements; partial or malformed V2 markers cannot select legacy
cleanup. The Store persists receipt identity and rejects conflicting reuse.

Focused worker/cleanup/binding/receipt/result tests passed **59/59 on Linux**
without skips at `e6e0ab1`. Subsequent Store V2 replay tests passed **2/2 on
Windows**, including restart and conflicting receipt reuse. At `32ac9c4`, the
actual Viewer TLS intake pair passed **1/1**, and the source completion test
passed **1/1** after adding worker-progress and reopened-Store coverage. Source
completion now returns its immutable receipt ACK even if a lost reply allows a
worker to progress before Viewer retries; dynamic job state stays on GET job.

The coordinator image built from `32ac9c4776136f26bd51ff6b2d0f125f72d98607` is
`sha256:3ea5232a75c1e00bdd195ac8dd9d7b38c5c6cb68ba330946ef32c907764c011f`.
A network-isolated, read-only container imported the worker/Store/V2 verifier
modules and served health HTTP 200 as UID 10001; its listener was joined and
the task-owned container removed. This proves image construction and basic
startup only, not configured reference execution or installed services.

Runtime follow-up `d05d100` permits an explicit schema-6
`"viewerAcceptance": {}` for the new Viewer pull/copy-ACK path. It avoids
constructing the incompatible legacy push sender while still requiring source
and copy-receipt pins; schemas 4/5 keep their nonempty push-target requirement.
The affected runtime suite passed **38/38 on Ubuntu** without skips, and an
independent read-only review found no actionable defect. The new assembly test
mocks TLS and mount qualification. The image above predates this follow-up;
it must not be described as containing the configuration change.

The production worker daemon already assembles `ReferenceResults` and
`ReferenceResultAPI` on its HTTPS listener. A full synthetic source → worker →
result download → actual Viewer outbox/signer → Store ACK → cleanup test is
being assembled; the separate component tests do not prove that whole path.
The photo workflow remains unqualified for client work until that pairing and
installed acceptance pass. Large transfer lease renewal, installed NAS
qualification and real-data processing remain open too.

### Combined intake/worker checkpoint

Code `e7d63c6` combines source intake `2ba927e`, pinned live-engine worker
checks, explicit V2 digest markers through execution/recovery, and the
parent-claim client shape update. The 260-test Ubuntu/pilot combined suite passed
with no skips: intake, spool, API, runtime, HTTP, driver/routing, engine/cleanup,
parent guard, recovery, store and protocol. An earlier run had one Linux-only
symlink-test setup failure; its fixture created a non-private intermediate
directory. The fixture was corrected, its 11-test module passed, and the full
260-test combined suite then passed on this exact code revision.

The Windows actual-Viewer canonical/proof/intake selection passed 23/23 with no
skips on the joined candidate immediately before that fixture-only correction.
Runtime configuration schema 6 explicitly requires the spool and pinned Viewer
source-attestation keys; existing schemas 3/4 are preserved.

The next missing production seam is source grant/download wiring: the imported
Store mint/read methods initially had no production callsites. That follow-up
must pass a reserved worker's actual HTTPS download before this is an executable
end-to-end workflow. Actual Viewer client pairing is also pending. The 10,000
photo synthetic metadata closure measured 4,920,057 bytes for ordinary filenames,
exceeding the existing 4 MiB V2 intake limit; a bounded V2-only limit and matching
Viewer outbox/node-budget migration are being prepared. Neither of these gaps
is resolved by the passing tests above. No client datasets were processed.

The current NAS preparation is described in
[the source spool checklist](truenas-source-spool-preparation.md). Only the
coordinator needs a NAS spool mount; workers use its authenticated source broker.

Worker integration code `06560c6` (parent `5b2d0f8`) incorporates the pinned
live-engine probe before mutations and cleanup, rejects the alternate legacy
Viewer executor in multi-node reference configuration, and retains the current
V2 admission validator from `49107f3`. The selected Linux source-intake validator,
protocol, engine worker/cleanup, driver/routing, runtime, daemon routing,
execution, results, combined preparation and worker-unit suite passed **179/179
with no skips** on Ubuntu 24.04 as unprivileged `pilot`.

This checkpoint does not include the in-progress Store/HTTP/spool intake
integration or prove installed workers, physical-host attestation, NAS custody,
actual Viewer V2 submission or real processing. The earlier missing live-probe
items below describe the earlier checkpoint and are now implemented in this
candidate, pending installed qualification. See
[the acceptance plan](coordinated-release-acceptance-plan.md) for the current
dependency order and explicit per-host concurrency rule.

### Actual Viewer source contract follow-up

The exact clean Viewer candidate checked on November 4 is
`b25d3234a5875be2be95c99302dfac3605999be4`. Its request includes `viewerTaskId`
and a signed `sourceAttestation`; the historical intake import did not accept
those fields. New source-proof verifier `dff4f44` validates a configured signer
pin, principal/environment, queue authority and exact task/manifest/pipeline
bindings, including the top-level Viewer task ID. A verified proof does not
establish physical NAS custody or grant cleanup authority.

The Viewer signs the pipeline digest using ECMAScript/JCS serialization.
Python's existing V1 JSON digest differs for legal options such as `1e-7`,
`1e-6` and negative zero. V2-only helper `63c6a3b` uses pinned `rfc8785==0.1.4`;
V1 hashing/history remains unchanged. Its upstream implementation is
[rfc8785.py](https://github.com/trailofbits/rfc8785.py).

The combined focused validator/protocol/proof/canonical suite passed 36/36 on
Windows with Node 24.16 and Python 3.13. It explicitly selected that actual
Viewer checkout and paired its exported domain/shape and canonicalization code
with Python verification, including ten numeric-option vectors. On Ubuntu,
34 passed and two optional Node-dependent cases skipped. These are isolated
proof/hash tests, not full application admission, physical custody, or installed
runtime acceptance. The Store/HTTP/runtime wiring is still in progress.

### Read-only installed-state refresh

At 2026-11-05T02:30Z, SSH succeeded to both processing PCs. Both user worker
units reported `LoadState=not-found`, `ActiveState=inactive` and no fragment;
neither expected private worker configuration file exists. The existing
coordinator reported healthy. Main NodeODX and both ingress containers are up.
Cluster's engine candidate exposes container port 3000 only on loopback port
13001; its name alone does not establish qualification or current ownership.
No units, containers, configuration, client jobs or NAS files were changed by
these observations. The new reference workflow is not installed.

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

### Canonical execution path

The coordinated version uses Viewer reference-attempt admission into
ReferenceStore, then the per-node ReferenceDriver/ReferenceEngineWorker pull
path, its persistent completion outbox and the existing Viewer accepted-copy
receipt chain. Source transfer and engine execution must join that same job
and claim identity. The legacy ViewerService direct NodeODM mutation lane does
not have the durable accepted-copy release contract and must not be enabled
as an alternate multi-node executor in the coordinated configuration.
The experimental node-execution gateway is not part of this release branch;
adding another execution/receipt protocol is not the integration plan.
Local worker identity verification, source admission/custody and installed
worker services are still required before this path can run jobs.

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
7. **Viewer experience:** Ops #406 at `bf5c1dbbb5fb49213e429f131c40ad7403628147`
   independently passed 52 workspace behavior/brand/control tests plus 14
   hierarchy migration/API/permission tests, no skips. These verify customer
   project scoping and unchanged access grants plus DOM navigation/error states;
   rendered browser acceptance, deployment, queue/node/log integration and
   current Claude review ownership remain open. The branch has 15 historical
   Claude co-author trailers; that is not evidence of current peer delivery.
   UI completion does not qualify the processing pipeline.
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
