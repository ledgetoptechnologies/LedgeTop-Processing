# Coordinated Processing and Viewer status — 2026-11-02

This is a current evidence checkpoint for the Processing + Viewer delivery goal.
It distinguishes local/synthetic evidence from installed or production behavior.
No real client data was processed as part of this checkpoint.

## Verified in the current work

- Beau selected a `processing-spool` subfolder in the existing Model-Viewer
  Storage dataset for future NAS-backed job inputs, rather than a separate
  dataset. Intended host/container paths are
  `/mnt/Plugins/App_Data/Model-Viewer/Storage/processing-spool` and
  `/app/storage/processing-spool`. This is a design decision only: the folder,
  NAS route, permissions, reservation protocol and retention behavior have not
  been created or activated. Read-only SMB verification on 2026-11-02 confirmed
  the parent Storage path is readable and `processing-spool` does not yet
  exist. The containing ZFS dataset is shared with Viewer
  storage, so a snapshot/hold would cover the dataset, not only this folder;
  retention design must account for that boundary and avoid claiming
  folder-scoped snapshot isolation.

- Processing branch `codex/engine-ingress-guard-candidate` contains commit
  `b8b08c8` (`Stage processing wrapper and integration evidence`) and the
  subsequent evidence checkpoints. The branch is published to origin for
  review and is not merged to `main`.
- The Linux repository suite reported 1,313 passed and 22 skipped. Targeted
  Windows checks passed; these are source-level checks and do not constitute
  deployment acceptance.
- The actual Viewer `NodeOdmProvider` adapter completed an isolated synthetic
  round trip with 62 invented uploads, one cancellation and three completions.
  Ordering was small production on cluster, large production on main, then
  small staging on cluster. Global TreeDar-slot exclusion, staging isolation,
  idempotent commit, output cursor preservation and repeatable download hashes
  passed. The fake engines did no photogrammetry.
- The Viewer pinned-vector test accepted its two exact synthetic Processing
  fixtures and rejected modified bytes. It is an exact-vector inspector, not a
  general v2 parser, LiDAR validator, or artifact acceptance implementation.
- On 2026-11-02, the paired Python/Node wire probe was run against the current
  Viewer validator source (`referenceProtocol.js`, normalized SHA-256
  `17d505c268b53ff5e17c61bd6f89fff8081b9089d35ca00cef6d96906d934159`). The
  synthetic Processing manifest requested ODM and TreeDar but contained only
  the ODM archive and report objects; Viewer accepted it, then rejected five
  negative cases: missing/duplicate ODM archive, zero-byte archive, unsupported
  stage role, and missing TreeDar version. Fixture generation ran under Ubuntu
  24.04 WSL because Processing intentionally holds this fixed pipeline on
  non-POSIX hosts. Viewer's follow-up review confirms this is only a v1 manifest
  validator seam: its current bridge accepts one ODM archive and does not ingest
  a typed TreeDar bundle. This does not prove genuine LiDAR input, TreeDar
  artifacts, NAS custody, Viewer binary ingestion, or runtime acceptance.
- On 2026-11-02, targeted grant, result-proxy, delivery, worker-API, worker
  recovery, routing, cleanup and transfer suites ran in Ubuntu 24.04 WSL as
  nonroot user `pilot`: all 112 tests passed. The first WSL attempt ran as root
  and correctly failed TreeDar's configured nonroot-main-host guard; rerunning
  under the unprivileged account passed. The Windows run had 48 host-guarded
  skips, which this rerun covered. These remain synthetic tests, not runtime
  acceptance.
- On 2026-11-02, the current Windows worktree reran six focused delivery and
  recovery modules (`test_worker_outbox`, reference delivery, transfer, worker
  recovery, worker API and scratch cleanup): 98 tests passed, 51 skipped by
  platform guards. This confirms the current Windows candidate behavior only;
  the Linux-only cases remain covered by the separate WSL run above, and no
  live runtime acceptance is implied.
- On 2026-11-02, the local synthetic Viewer compatibility path passed
  `test_viewer_service`, `test_viewer_tasks` and `test_viewer_http`: 51 passed,
  two skipped. These use mocked engine responses and local HTTP fixtures; they
  do not upload to a processing PC, prove NAS-backed queueing or permit the
  current 50 GB estimate through the live 20 GiB intake cap.
- A manifest-only regression now builds 10,000 synthetic image records totaling
  50,000,000,000 bytes. The full `test_reference_store` module passed 20/20;
  the new case confirms the large job is offered to the main-node slot and not
  the small cluster slot. It writes no image payloads and proves routing only;
  live Viewer admission still rejects the same estimated size at its 20 GiB
  cap, with no end-to-end transfer demonstrated.
- The NodeODM 3 provider probe test now changes the synthetic `/options`
  catalog between successive probes and confirms the second probe sees the
  removed output option. `test_provider_nodeodm3` passed 7/7, including two
  fresh `/info` and `/options` GETs per probe; no option catalog is cached.
  This remains synthetic API evidence, not a fresh live-engine request.
- A fresh read-only `df -B1` on 2026-11-02 reports root-filesystem free space
  of 118,186,557,440 bytes on `.89` and 1,490,274,271,232 bytes on `.88`.
  These are root filesystem values; the installed worker scratch mount/config
  was not confirmed. A synthetic scratch-admission case models those exact
  capacities. With a 50 GB input, 100 GiB output budget and 1 GiB safety floor,
  the ledger rejects cluster admission and reserves the same request on main.
  `test_scratch_policy` passed 36 tests with one optional skip. This remains a
  capacity-policy simulation, not a live reservation or proof the installed
  worker uses this ledger.
- Fresh read-only `systemctl --user show` checks on both `.89` and `.88` report
  `ledgetop-processing-reference-worker.service` as `LoadState=not-found`,
  inactive/dead; the expected private `reference-worker.json` is absent on both
  nodes. The source daemon, scratch ledger, and outbox are therefore not active
  runtime components on these machines. No unit was installed or started.
- Viewer commit `dc48016073d2d37a759b7f753fcfbe0cd16f9fea` on
  `codex/viewer-v2-delivery-ledger` adds
  `apps/viewer/docs/reference-v2-acceptance-seam-proposal.md`. It scopes a
  proposed typed TreeDar bundle, original LAS/LAZ custody, accepted-copy
  receipt, replayable ACK, and separate publication/cleanup authorities. The
  proposal explicitly preserves raw Emlid CSV files through failure/cancel/
  rollback, requires worker and child writer quiescence before heavy-slot
  release, and requires explicit per-stage coverage. It remains unagreed and
  inactive; it is review progress, not a wire contract or runtime behavior.
- On 2026-11-02, Processing and Viewer agreed only the disabled fixture seam
  for `AcceptedCopyAck`: it echoes `PartialAcceptance.ackOperationId` and binds
  the full acceptance through
  `SHA256(ASCII("LTDS-PARTIAL-ACCEPTANCE-V2") || 0x00 ||
  canonicalUTF8(complete PartialAcceptance))`. The candidate now includes the
  closed record, cross-message hash domain, positive/changed-value vector and
  synthetic binding checks. The Processing candidate suite passed 25 tests in
  both Windows Python and Ubuntu 24.04 WSL; one optional JSON Schema oracle
  test skipped because its test-only dependencies are not installed in either
  default environment. A separate Node check matched the agreed positive
  vector and confirmed a changed acceptance produces a different digest.
  Viewer commit `6b33119f` adds a fixture-only test using Viewer’s real
  canonicalizer; its bounded Node test passed 1/1 against the exact packet.
  The
  schema remains marked unagreed/inactive overall. Issuer authentication,
  replay storage/endpoints,
  NAS physical custody, full typed TreeDar acceptance and deployment remain
  unagreed and unimplemented.
- On 2026-11-02, the Viewer chat added a fixture-only cross-record
  `AcceptedCopyAck` check at commit `9d2766aa`. It uses Viewer's canonicalizer
  and checks exact closed keys, operation/receipt/full-hash/binding/result/
  storage/ordered-artifact bindings, modified acceptance, ordering and NUL
  domain handling. Its bounded Node tests passed 2/2. This still does not
  authenticate issuer identity or prove NAS custody. The saved live-task
  receipt check Beau ran matched the task and receipt, validated its 71-file
  inventory digest and listed the mesh tileset; it did not list a point-cloud
  tileset. This is ledger metadata, not a current on-disk custody check.
- Fresh cross-worktree verification on 2026-11-02 passed Viewer Node tests
  `reference-v2-acceptance-hash-packet` and
  `reference-v2-accepted-copy-fixture` (2/2) against Processing's current
  `contracts/reference-v2-candidate` files. Processing's corresponding Python
  `test_reference_v2_candidate` passed 26 tests with one optional schema
  oracle skipped. This confirms only the shared synthetic canonicalization
  and fixture cross-record checks; the payload remains inactive, unauthenticated
  and unwired to either runtime.
- Additional offline source-boundary tests on 2026-11-02 passed in Viewer
  Node (`source-retention-v2-proposal-vector` and
  `reference-source-release-ack`, 6/6) and Processing Ubuntu 24.04 WSL
  (`test_reference_source_retention_v2_candidate` and
  `test_reference_v2_candidate`, 37 tests, one optional schema skip). These
  verify signed-vector, exact-byte, binding and negative-vector behavior for
  candidate contracts only; no NAS filesystem, snapshot authority or release
  was exercised, and the v2 receipt remains a proposal.
- The Viewer chat confirmed the production reference lane remains hard-disabled:
  the worker's production call returns without dispatch, and no production
  transfer adapter, broker route or authenticated issuer is wired. The broker
  currently stores only a token hash, so an encrypted exact-grant outbox cannot
  close the crash-after-mint-before-persist window without a paired atomic
  issuance API/schema or an explicitly weaker deterministic reconstruction
  contract. NAS source pinning, writer/purge fencing and issuer authentication
  remain prerequisites; no live lane was enabled.
- Viewer commit `56cfbe23` adds an isolated, in-memory SQLite/AES-GCM
  persistence sketch for atomic grant issuance. Its synthetic Node check passed
  1/1, but the fixture is not the current broker and does not prove process
  crash durability, restart/unknown-commit recovery, real ACK validation, key
  rotation/rollback, issuer authentication or NAS fencing. It is useful as a
  proposed transaction shape only; it does not close the automatic grant
  handoff or authorize transfer. The existing broker and Viewer journal use
  separate SQLite databases, so a real atomic contract needs an explicit
  storage owner/API/schema migration.
- Viewer fixture follow-up commit `ed601472` binds its envelope version and
  immutable key ID into AEAD associated data and rejects key-ID tampering,
  aliases/remaps and nonce-row replacement within the synthetic local
  registry. The Viewer chat reports the file-backed and memory tests passed
  2/2. This strengthens the example contract but remains test-only: the local
  registry is not production key management and does not prove nonce history
  across databases/restores, real process or power failure, a trusted
  anti-rollback authority, or an atomic transaction with the existing broker.
- Viewer isolated candidate commits `0fd81ce0` and `50a4f506` now exercise a
  new synthetic-only broker method that composes real grant/usage writes and
  an AES-GCM sealed grant row inside the broker's existing `BEGIN IMMEDIATE`.
  The original `mintGrant` path remains unchanged. The candidate checks a
  closed operation binding and runs its injected reservation/source-pin
  verifiers inside the DB transaction; its regression confirms a changed
  verifier outcome leaves grant, usage and sealed-row counts at zero. The
  Viewer chat reports 3/3 focused tests passed, including insert-failure
  rollback and exact close/reopen replay. This is not a production migration,
  key authority, reservation issuer or NAS pin: the new table is fixture-only,
  and unknown commit/restore rollback, durable ACK/reconciliation, global key
  and nonce governance, and NAS writer/purge fencing remain unimplemented.
  Follow-up `71e3dbf8` adds a file-backed synthetic Processing receiver that
  reopens its ACK store, replays an already-accepted exact grant after expiry,
  and holds an expired grant the receiver never accepted. The Viewer chat
  reports 3/3 focused tests passed. This remains simulated state/transport, not
  a real Processing HTTP/authentication path or concurrent sender test.
  The changes are staged in draft PR #409 at head `71e3dbf8`, based on the
  Viewer candidate branch `codex/viewer-v2-delivery-ledger` rather than `main`;
  GitHub reports no status checks. It is reviewable candidate work, not a
  release or installable package.
- The Viewer chat rechecked Claude participation on 2026-11-02: the saved
  Viewer link is historical (last updated October 28, with no current session
  binding), and no Engineering Hub message-bus tool is available there to
  verify fresh delivery or request an acknowledgement. Do not infer active
  Claude ownership from the historical link or branch name. The current-main
  redesign refresh is separately tracked in draft PR #406; its current owner
  and design review remain unconfirmed. The original #347/shared branch was
  left untouched.
- Follow-up Viewer-chat evidence on 2026-11-02 confirms redesign PR #406 is
  still draft at `839e5173` on base `dc8cedfe`, with zero recorded reviews or
  comments. The candidate includes customer → optional grouping → project →
  task and dark/orange workspace styling, but does not establish client
  permission propagation. No fresh Claude acknowledgement or supported
  message-bus evidence is available. The isolated Viewer candidate branch was
  clean at `06685dd5`; these are review candidates, not deployed behavior.
- Viewer follow-up commit `b1f937bc` on draft PR #409 adds a synthetic-only
  bound on aggregate source object bytes before snapshot intent persistence.
  Its overflow regression passed 1/1 using an in-memory database. This guards
  integer accounting in the candidate only; it does not establish an approved
  spool location, snapshot custody or a running storage owner. The Viewer chat
  reviewed the narrow change and found no issue; it did not rerun the test.
- Read-only live probes on `.89` showed the coordinator healthy, provider
  discovery available and a 25-photo plan selecting local execution without
  starting work. The main NodeODM container was observed running. These probes
  did not submit a Viewer task or run photogrammetry.
- A read-only inspect of the live `.89` Processing prototype container on
  2026-11-02 returned `PROCESSING_VIEWER_MAX_INPUT_BYTES=21474836480` (20 GiB)
  and `PROCESSING_VIEWER_MAX_ARCHIVE_BYTES=107374182400` (100 GiB). The planned
  ~50 GB photo dataset exceeds the current Viewer intake cap and cannot be
  uploaded through this deployed route as configured. The archive cap is a
  separate ceiling. No limit was changed and no data was submitted; the
  deployment's local-spool admission cannot safely be raised until capacity and
  NAS-backed queueing are addressed.
- Viewer recorded the same live-cap observation in its candidate planning doc
  at `5668f9a5`, preserving the deployed configuration pending NAS queue,
  capacity and coexistence-budget design. No host, upload or runtime change
  occurred.
- Beau's read-only Viewer metadata query matched the saved test task and
  provider task. The recorded schema v1 archive receipt has matching identity,
  a 71-file bounded inventory and matching manifest digest; `3d-tiles` was a
  boolean `true`; the mesh tileset path was listed. The receipt did not list
  the candidate point-cloud tileset path. This proves the database recorded
  that inventory, not that the files are still present on disk or that a live
  orchestrator-to-Viewer ingestion was completed. The absent path does not
  prove no EPT output exists elsewhere; Viewer discovers EPT separately at
  `entwine_pointcloud/ept.json`.
- The Viewer chat reviewed the read path: `getAttemptTransferProvenance` reads
  stored receipt JSON, and `getVerifiedAttemptProducerReceipt` validates
  stored attempt/input/initialization/provider-result/archive bindings. Neither
  establishes current NAS file existence or hashes. The ingestion worker hashes
  the downloaded archive and inventory during initial capture, and a specific
  archive-receipt recovery path re-hashes the expanded destination tree. That
  point-in-time recovery verification does not prove this saved task followed
  that path or that bytes remain unchanged now; historical unreceipted jobs
  may use a legacy path. The original ZIP hash is recorded but the directory
  verifier does not re-hash an archived copy of the ZIP. This narrows the
  existing evidence to ledger metadata only.
- Follow-up audit confirms the foreground synthetic prototype documents
  completion-only resume from the exact saved receipt: it resends the recorded
  completion and does not claim or rerun the job. Separately, the newer
  reference driver persists execution and delivery stages and reconciles them
  on a same-state restart, but is not installed as a host service. Automatic
  process/boot supervision is not installed. After abrupt worker death, stale
  read pins and uncertain engine state still require inspection before safe
  recovery.
- An untracked Linux pidfd host-launcher experiment is present in
  `ops/host_supervisor.py` with bounded synthetic leaf-process tests. It
  deliberately leaves every launched epoch held because direct-child pidfd
  exit cannot prove descendants, detached containers, engine jobs or I/O have
  stopped. It refuses dirty-journal restart and is not a restart/recovery
  solution or deployment artifact. Do not install it or treat its tests as
  unattended resilience qualification. A reviewed kernel process-tree
  containment and daemon fencing design is still required.
- A separate candidate systemd user unit now directly supervises the durable
  `ops.reference_worker_daemon` process. Its restart loop is rate-limited and
  it never force-kills a daemon that has not drained. Three focused policy
  tests and `systemd-analyze --user verify` pass. This checks the unit contract
  and syntax only: no worker service was installed or run, and automatic
  recovery with a real persistent engine, outbox, Viewer receipt and stale-pin
  fault is still unqualified.

## Still required for the requested end state

1. **NAS-backed queue and reserved just-in-time transfer:** current processing
   uploads are staged by the Viewer adapter to coordinator-local bounded
   temporary storage. A durable NAS source reference, reservation protocol,
   transfer resume and queue/capacity integration are not verified as
   operational.
2. **Per-node control and unattended resilience:** the source has node admission,
   maintenance and durable worker receipt mechanisms, but the current host
   wrapper is local reviewable code. Exact completion-only resume exists, but
   reboot-safe automatic claim/retry is not installed or verified on either
   node. Uncertain claims stay held pending inspection.
3. **Full result acceptance and cleanup:** a worker completion ACK records
   metadata/hashes, not durable Viewer artifact custody. Viewer v1 receipt
   metadata is present for the inspected ODM task, but live worker-to-Viewer
   transfer, exact durable ACK, replay after real restart, and post-ACK bounded
   cleanup are not proven as one production path. Retained result copies must
   not be deleted based on download or remove alone.
4. **TreeDar:** the source candidate requires a genuine-LiDAR admission and a
   12-artifact native output closure. Current Viewer bridge accepts one ODM
   archive and does not accept this typed closure. Viewer has pushed a detailed
   seam proposal, but Processing and Viewer still need paired owner agreement
   and then implementation of the mapping, provenance binding, durable custody
   record and exact acceptance ACK. No TreeDar job was run against real LiDAR.
5. **Credential hygiene:** prior tool output exposed the active candidate
   NodeODM startup token. The user was advised to rotate it and update the
   matching private Processing credential through an approved maintenance
   route. Rotation is not verified. Do not use the exposed value or start
   client processing until the replacement is confirmed.
6. **Viewer redesign and Claude:** current-main draft PR #406 contains the
   customer → optional group/subcategory → project → task hierarchy and
   Operations-aligned dark styling. GitHub commit authorship verifies Claude
   co-authored most of the current-main redesign, branding and hierarchy-control
   commits. Ongoing Claude ownership/review is not verified; the internal
   message-bus MCP is unavailable in this chat. The concrete Greenleaf → New
   Build / Project Y → tasks example fits the candidate group structure, but it
   is not an authorization boundary. PR #406 remains draft, has no recorded
   reviews or checks, and its UI is not verified deployed; the latest focused
   run also exposed stale hierarchy mocks and a missing local `express`
   dependency, as recorded above.
7. **Runtime acceptance:** no real end-to-end small job has been submitted
   through Viewer during the current verification. The previously discussed
   10,000-photo client dataset (estimated ~50 GB) remains unprocessed.

## Next safe gates

1. Rotate the exposed NodeODM credential through the node's approved,
   drain-and-health-checked maintenance path, update the matching coordinator
   secret, then prove the old credential fails and the replacement works
   without printing either value. Do not improvise against the migration
   candidate Compose project.
2. Agree and implement a versioned cross-repo TreeDar closure: exact logical
   artifact list, semantic type, nonzero size, SHA-256, genuine LAS/LAZ
   provenance and binding to job, attempt, input manifest, engine and output.
   Require a durable Viewer custody receipt and exact idempotent ACK before
   permitting cleanup; uncertainty must retain files and the global slot.
3. Close the Viewer-to-coordinator artifact delivery gap with synthetic fault
   tests for partial transfer, lost/duplicated ACK, process restart, storage
   full, hash mismatch and cleanup veto. Keep tests bounded and use synthetic
   artifacts only.
4. Before any real small test, verify credential rotation, installed immutable
   builds, source/result storage capacity, expected option/capability mapping,
   no competing direct engine routes, and rollback/recovery ownership. Start
   with a user-selected small dataset and inspect Viewer acceptance before
   scheduling queued jobs.

Gaussian splats remain scaffold-only and disabled. Production stays ahead of
staging; a running task is not preempted; small-photo routing prefers cluster;
TreeDar remains main-node-only; the shared global execution slot remains one.

## Evidence update — 2026-11-02

- Staged the exact reviewed source archive from commit `465f382` on both
  processing PCs at
  `/home/bkoltz/.local/share/ledgetop-processing-reference/reference-465f382/`.
  The archive SHA256 is
  `0c41f0407b7fc57723c4278d790c902270ac13c98c97bacefe909c8194e8cf5f`; all 90
  extracted regular-file contents matched the archive, no symlinks were
  present, the source directory is mode `0700`, and the retained archive is
  mode `0600`. The prior staged versions remain intact.
- Read-only checks on both hosts show
  `ledgetop-processing-reference-worker.service` is not installed or active
  (`LoadState=not-found`), the `bkoltz` user manager has linger enabled, and
  the candidate private config file at
  `~/.config/ledgetop-processing/reference-worker.json` is absent. No unit was
  installed, enabled, or started; no credentials/configuration were created.
  The staged unit also points at a `current/source` path that has not been
  created, so the host copy is intentionally inert until a reviewed atomic
  activation binds a specific source version and correct config/state paths.
- The prior private coordinator provisioning receipt exists on `.89` and reports
  `brokerConfigured=false`, `workerIntakeConfigured=false`, and
  `servicesActivated=false`; `.88` has no matching receipt. This confirms the
  NAS-backed worker intake route and per-node worker configs are not staged.
  Do not enable the candidate worker unit until that route, private configs,
  storage reservation semantics, and Viewer acceptance endpoint are configured
  and verified.
- Added a reference-driver crash-window regression: after the coordinator
  durably accepts a synthetic successful completion but the response is lost,
  a fresh outbox, worker journal and driver replay the byte-identical completion,
  record only one durable coordinator receipt, and do not initialize the engine
  again. Synthetic Viewer acceptance then completes the driver record while
  retaining both the local archive and input. The focused driver suite passed
  20 tests with one platform-specific cleanup test skipped on Windows. This is
  in-process synthetic coverage, not the missing TrueNAS/Viewer runtime path.
- Viewer commit `3c5528c6` on `codex/viewer-v2-delivery-ledger` adds
  `apps/viewer/docs/reference-nas-broker-seam-proposal.md` and a synthetic
  SQLite identity fixture. Viewer reports 2/2 tests passing in one bounded
  CPU-1/512-MiB/no-network run; the test container is gone. The proposal keeps
  v1 and TreeDar v2 unchanged and calls out that its accept record only checks
  reservation existence, and that its declared-size counter is not a physical
  byte/request meter. It is design evidence, not an agreed wire protocol or a
  NAS custody/issuer implementation.
- A paired bridge rehearsal is now available in the clean, pushed Processing
  branch `codex/paired-v1-http-rehearsal`, commit `59bafc9`, with runbook
  `docs/paired-bridge-drop-plan-DRAFT.md`. Its recorded bounded synthetic pass
  exercised the actual Processing ReferenceAPI/ResultProxy and Viewer
  ReferenceProvider/ReferenceResultJournal/ReferenceAcceptanceBridge around a
  synthetic ZIP. It dropped the first acceptance response after one byte,
  restarted the Viewer process, replayed the same receipt, and recorded one
  coordinator acceptance while transferring the result only once. This proves
  that manually re-entering the bridge can replay this synthetic acceptance
  path; it does not prove a production retry scheduler, real worker TLS/read
  pins, NAS custody, catalog publication, cleanup authority or real data
  acceptance. The named container exited successfully; its volume and outer
  temporary root were retained for review. No runtime was activated.
- Cross-source boundary review: the Processing broker client expects a scoped
  HTTPS GET at `/api/processing/reference/v1/objects/{objectId}`, checks the
  full object size/hash and publishes a private fsynced local object, but has no
  range resume or physical request/byte meter; failed source-object transfers
  restart from byte zero. Worker result bytes are exposed separately by an
  authenticated result GET bound to generation/object/result-manifest hashes
  and durable read pins. Processing now forwards a single explicit byte range
  through its protected HTTP API and result proxy, validates the exact 206
  `Content-Range` and content length, and admits only the requested response
  bytes. The worker revalidates the complete sealed object before each ranged
  response. Synthetic proxy/API/HTTP tests cover range forwarding, malformed
  ranges, mismatched range metadata, and cleanup. This enables a caller to retry
  a partial result download against the same immutable result; it does not add
  resumable source-object downloads or a durable receiver-side partial-file
  assembler. The worker's `/results` POST is a metadata
  completion receipt, not an archive upload; Viewer acceptance is a later,
  separate receipt. No NAS broker service or mounted Viewer accepted-copy
  ingest is implemented. This mapping is an existing-source audit, not a new
  cross-language contract.
- Viewer cross-review of Processing head `b20a804` confirmed the HTTP range
  adapter casing fix and its tests, but found no result client or bridge on
  Viewer `main`; the dormant Viewer candidate only accepts complete `200`
  bodies and checks the whole-object digest before its separate acceptance
  receipt. Therefore the Processing `206` route has no current-main consumer.
  A future resumable receiver must bind each exact range to the same object
  and result manifest, retain offset-safe pieces, then verify the entire
  assembled object before durable acceptance or ACK. No Viewer candidate files
  were changed during this review because that worktree has an unrelated
  uncommitted edit. Claude's current ownership or acknowledgement for the
  workspace redesign and hierarchy remains unverified. Processing PR #264 is
  open at `b20a804` against `codex/nodeodm3-provider`, not `main`; this is a
  staged candidate and not an integrated release.
- Ran the existing Viewer candidate's `reference-result-wire-binding`,
  `reference-result-bridge` and `reference-joint-harness` Node tests from
  `C:\Projects\viewer-v2-delivery-ledger` at `1e5bfa6b`: 9 passed, 1 skipped
  because Linux descriptor and directory-barrier semantics are unavailable on
  this Windows host. These tests qualify full-body identity/hash validation
  and synthetic ACK replay on the candidate only; they contain no range
  consumer and do not qualify Viewer `main`, NAS custody or live ingestion.
- Grant lifecycle audit: the worker claims a queued job, reserves local scratch,
  then polls the claim-bound `/transfer-grant` endpoint; while no grant exists,
  it stays in `awaiting_grant` and does not fetch inputs. An environment-scoped
  Viewer bearer posts the grant to `/transfer-grants`; Processing checks the
  reserved job, exact manifest/object closure and expiry, then seals the first
  grant digest and returns a nonsecret acknowledgement. Processing does not
  mint the broker token. Viewer has broker mint/post helpers, but source review
  found no service call sites connecting them to a durable automatic handoff.
  Expired/replacement grants cannot be silently rotated because the first grant
  is sealed. This is the immediate integration gap: jointly implement and test
  the Viewer-owned durable grant handoff and explicit expiry/replay recovery
  before treating the worker path as schedulable.
- Added `test_worker_waits_for_exact_viewer_transfer_grant_before_fetching_inputs`
  to the Processing driver suite. It verifies an ungranted job remains in
  `awaiting_grant` with no local input, broker download or engine initialization;
  the authenticated existing Viewer API can then post a synthetic grant, exact
  grant replay returns the same nonsecret ACK, changed-token reuse conflicts,
  a production-scoped Viewer token cannot grant access to a staging job, and
  exactly one input download follows. The focused test passed. This closes a
  Processing-side regression gap only; it does not implement or prove the
  missing automatic Viewer grant issuer/handoff.
- Viewer independently reviewed paired rehearsal commit `59bafc9` against its
  acceptance proposal and confirmed the scoped claims are consistent. Viewer
  corrected its range-transfer description in commit `b0c376b6`; this makes
  clear that range support exists at the worker endpoint but is not forwarded
  by the current coordinator proxy. This is documentation review only; no test
  was rerun and no runtime behavior changed.
- Viewer commit `1e5bfa6b` documents why the source broker is not safe to mount
  yet: current database pins and file scans do not fence old app writers,
  external NAS clients or purge/move paths; no authenticated custody issuer,
  protected source epoch or paired physical-retry accounting scheme is wired.
  Viewer and Processing both leave the active NodeODM route unchanged. A
  TrueNAS ZFS snapshot plus hold, exposed read-only to a narrowly scoped broker,
  is being evaluated as a possible immutable source pin. This is only an
  architecture option: atomicity with the Viewer dataset manifest, actual
  dataset boundaries, snapshot/hold owner and recovery, mount visibility,
  queued-snapshot capacity and cleanup/release authority still need proof.
  No TrueNAS state was changed.
- TrueNAS API research for the installed 25.10 line confirms
  `pool.snapshot.create`, `pool.snapshot.hold`, and `pool.snapshot.release`
  are available. Holds require the `SNAPSHOT_WRITE` role. The hold API adds a
  TrueNAS tag, while release removes **all** hold tags from the selected
  snapshot, so sharing one job snapshot with unrelated hold owners would make
  automated release unsafe. A viable candidate design needs a dedicated,
  uniquely owned snapshot per frozen submission (or a separately tested
  job-specific ZFS hold mechanism), one authoritative retention owner, and
  explicit readback before issuing a source-retention receipt. ZFS snapshots
  are read-only point-in-time views and ordinarily do not duplicate the full
  dataset, but that property does not establish atomicity with Viewer upload
  manifests or protect against every privileged administrator/purge path.
  References: [TrueNAS 25.10 snapshot create API](https://api.truenas.com/v25.10/api_methods_pool.snapshot.create.html),
  [hold API](https://api.truenas.com/v25.10/api_methods_pool.snapshot.hold.html),
  [release API](https://api.truenas.com/v25.10/api_methods_pool.snapshot.release.html),
  and [snapshot behavior](https://www.truenas.com/docs/scale/25.10/printpreview/).
- The 25.10 RBAC docs describe custom privileges composed from individual
  method roles; snapshot create/hold/release require `SNAPSHOT_WRITE`. The
  published schema does not document a dataset-scoped role, so the owner
  service must enforce an exact configured dataset and name namespace, and
  Beau should review the actual user/privilege grant before any API key is
  provisioned. TrueNAS describes user-linked API keys as password-equivalent
  access for their user and requires HTTPS for API-key authentication. Do not
  put such a key in chat or a Processing node; keep it on the storage-owner
  host if this design proceeds. References: [25.10 RBAC](https://api.truenas.com/v25.10/rbac.html),
  [TrueNAS API-key guidance](https://www.truenas.com/docs/scale/api/).
- Least-privilege follow-up from current OpenZFS documentation: `zfs allow`
  can scope delegated snapshot/hold/release operations to a dataset, and CLI
  `zfs release <tag>` removes only the named hold. However, `snapshot` also
  requires the `mount` ability, which OpenZFS says cannot be delegated on
  Linux. Therefore a direct unprivileged ZFS owner is not yet a verified
  option for TrueNAS SCALE. A root-owned helper constrained to the dedicated
  spool dataset may be narrower than a TrueNAS API key carrying global
  `SNAPSHOT_WRITE`, but must be designed and verified on the actual host before
  adoption. Do not grant any ZFS permissions or install a helper as part of
  this research. References: [OpenZFS delegated administration](https://openzfs.github.io/openzfs-docs/Basic%20Concepts/Operations/Delegated%20Administration.html),
  [zfs-allow permission requirements](https://openzfs.github.io/openzfs-docs/man/master/8/zfs-allow.8.html),
  [zfs hold/release tag behavior](https://openzfs.github.io/openzfs-docs/man/master/8/zfs-hold.8.html).
- Current Processing source inspection confirms
  `processing/reference_source_retention.py` is only a receipt validator: it
  explicitly supplies no authentication or cleanup and has no production call
  site. `ReferenceStore` currently persists jobs, Viewer acceptance receipts,
  delivery outbox, claims, worker receipts and transfer grants, but no source
  retention receipt. Therefore a validated-looking v1 payload cannot yet pin
  queued inputs, authorize grant issuance, or prevent their cleanup.
- Viewer candidate branch `codex/viewer-atomic-grant-fixture` is pushed at
  `9c77363ae61578d8f6263d30240ec9981108a234`. Its
  `referenceStorageOwnerGate.issueAtomicGrant` deliberately fails closed and
  remains unmounted. `referenceSnapshotOwnerCandidate` records an exact
  synthetic intent, models TrueNAS 25.10 snapshot create/hold/query calls
  through an injected client, and persists ambiguous outcomes before modeled
  mutations. Its focused Node suite passed 4/4, including file-backed
  close/reopen after a lost create response. The candidate is guarded to
  `synthetic-only`, cannot issue a custody receipt or release a hold, and does
  not open a WebSocket, verify bytes/mount boundaries, authenticate an issuer,
  prove purge fencing, or provide hardened migrations and rollback lineage.
  This is not a NAS issuer, physical pin, guarded production migration, or
  accepted-copy receipt/ACK endpoint. No dispatch was enabled.
- Cross-repository contract review found Processing's existing
  `source-retention-v1` (the validator and
  `contracts/source-retention-v1.contract.json`) does not contain authenticated
  issuer/key epoch, immutable dataset/snapshot identity and transaction group,
  source-closure digest, hold identity, or signed per-object custody evidence.
  It is a validator for a different, weaker textual retention claim. The
  Viewer proposal's richer fields must be defined under a new, versioned
  contract and canonicalization; do not silently extend or overclaim v1. A
  separate v2 wire proposal and deterministic public vector are frozen
  between the Viewer and Processing sessions **for fixture-only validation**.
  Viewer committed and pushed them at
  `f4a6ba037ffde506c93930892bc5fb61e6205acc` on
  `codex/viewer-atomic-grant-fixture`; its Node vector test passed 1/1. The
  Processing fixture copy has the same SHA-256
  `b00f4163029c83aa6266dcc813d78bb3074177e00a21dd30a34bdbb1e2f3aff1`.
  This fixture agreement does not define a live authority. The proposal pins
  ID grammars, dataset/snapshot identities, decimal TXG strings, owner/key
  epochs, strict timestamps, manifest closure,
  source-intent snapshot derivation and Ed25519 signed bytes. Processing's
  Python candidate validator verifies the exact signature against the public
  fixture key, manifest digest/full closure, source-intent snapshot derivation,
  context, key-record binding and injected lineage result. The candidate checks
  that the receipt timestamp falls inside the key's signing-validity window;
  this is historical signature validation, not a claim that the key is
  currently valid for new issuance. Tests independently compare every
  published canonical-byte vector and digest. Processing v1 and v2 retention
  tests passed 18/18 together both on the host and inside the
  locally built Linux image. The image imported the verifier under its
  non-root UID 10001 with `cryptography` 50.0.2; its temporary tag was removed
  after the run. The candidate requires pinned
  `cryptography` 50.0.2 plus its `cffi` 2.1.1 and `pycparser` 3.12 runtime
  dependencies, is guarded to `synthetic-only`, has no
  production import/call site, and installs neither the trusted key registry
  nor rollback-resistant lineage. Remaining prerequisites include authenticated
  owner-side object-to-path derivation and file hashing, real TrueNAS pin/hold
  and mount isolation, trusted key/epoch registry and rollback anchor, durable
  release/cleanup authority, and restart/recovery integration. No receipt or
  live custody proof exists.
- Beau's follow-up read-only metadata check matched the same saved task and
  provider attempt. It confirmed `3d-tiles` is a boolean `true`, the recorded
  v1 archive inventory has 71 entries and a valid digest, and the mesh tileset
  is listed. The queried `3d_tiles/pointcloud/tileset.json` path is not listed.
  That observation does not establish whether another point-cloud format (for
  example EPT) is present elsewhere in the recorded inventory, nor prove
  current on-disk custody or Viewer acceptance.
- Read-only SSH to cluster `.89` reported coordinator health `ok`, zero queued,
  running or held jobs, and both logical nodes `active`. The active NodeODM
  candidate is the `nodeodm` service in a separate root-owned migration Compose
  project, bound to `127.0.0.1:13001` with its persistent `/var/www/data`
  volume. Its command token matches `LOCAL_NODE_TOKEN` in the coordinator's
  private runtime environment; no credential value was emitted.
- The candidate Compose file is mode `0600`, owned by root. The installed
  prototype source has no `ops/node_maintenance.py` or node maintenance config.
  The fixed reference-coordinator updater is bound to a different legacy
  container/project (`node-odx-gpu`). The generic source updater can select a
  Compose project, but takes one node token for both pre- and post-replacement
  health checks, so it cannot safely rotate this credential while the engine
  changes from old to new token. The cluster's private `runtime.env` matches all
  24 of its keys in the running coordinator container, so changing the file
  alone would not change the coordinator's in-memory configuration. Rotation
  remains unperformed; client processing must wait for a reviewed dual-token
  transition and a managed coordinator recreate that preserves the state volume.
- Claude's Viewer redesign worktree is on
  `claude/viewer-workspace-redesign`, with Claude-coauthored redesign,
  branding/accessibility and hierarchy commits in open PR #347. A current
  GitHub compare on 2026-11-02 shows that branch 16 commits ahead and 118
  commits behind `main` (`dc8cedfe`); it is not an
  ancestor of the current production branch and must be reconciled on a fresh
  main-based review branch before it can ship. Its focused workspace suite
  passed 60/60 tests, including real headless Edge desktop and mobile browser
  assertions for hierarchy, layout, load failures, task navigation and
  permission behavior. The suite left three test-owned browser profiles/process
  trees; exact Edge process trees were stopped and the test profile paths were
  verified absent afterward. This verifies the branch's candidate behavior,
  not Claude's current participation or production rollout. The separate
  hierarchy-controls candidate contains the customer/group and project
  controls and tests; current ancestry checks show it is 18 commits ahead and
  121 behind `origin/main`, with no matching
  `origin/claude/viewer-hierarchy-controls` ref. It also is not in production.
  Grouping remains separate from client access.
- GitHub reports Processing PR #264 (`Draft inactive bounded engine ingress
  admission guard`) as an open draft targeting `codex/nodeodm3-provider`, not
  `main`. A read-only lookup found no PR for that base branch. The PR is a
  stacked review item and is not directly mergeable into `main`; retargeting it
  would pull a much larger unrelated candidate tree into the diff. Keep the
  current review boundary until the parent work is split or a deliberate
  main-based review plan is prepared.
- On 2026-11-02, a clean Viewer integration branch was created from current
  `main`, applying only the workspace redesign and hierarchy feature commits;
  old merge/workflow commits were excluded. Draft Viewer PR #406 is open against
  `main` with 28 files. Its production build and 117 focused tests passed,
  including Edge desktop and 390px/320px browser checks. No workflows changed,
  and the original Claude branch and PR #347 remain untouched. Current GitHub
  state reports head `839e5173eacaaf0ac549072527ce20b9501aa8ef`, still draft,
  with no recorded review or reviewer request. Recent branch commits
  `85038499` and `9eb655da` are co-authored by Claude and Beau; the newest
  hierarchy paging/contrast fix `839e5173` is by Beau. This verifies Claude
  co-authored the current-main redesign work, but does not establish continuing
  ownership or approval of the draft. The implemented hierarchy is
  organization/customer → optional project group/subcategory → project →
  existing tasks; group metadata does not grant access. The redesign keeps a
  dark theme with Operations-aligned tokens. These are review and test results,
  not a Viewer deployment or live Processing acceptance.
- Rechecked Viewer PR #406 on 2026-11-02: it remains open, draft, and mergeable
  against current `main` `dc8cedfe`; GitHub reports no reviews or checks. The
  Viewer chat confirms the saved Claude link does not prove an active Claude
  session and no supported message-bus tool is available there, so current
  Claude review/ownership remains unverified. A duplicate subset PR #410 was
  briefly created during reconciliation, then closed and its branch deleted
  after finding #406 already includes the same redesign plus hierarchy API and
  controls. The subset's local build and 76 focused tests passed, but they are
  not evidence for #406. Its broader suite was stopped after unrelated
  cleanup/derivative/LOD failures and a browser test reported 3.9 GiB peak
  memory; the complete Viewer suite is not qualified by that run. The test
  browser processes were stopped; automatic review rejected recursive cleanup
  of the remaining task-created temporary browser profiles, which were left in
  place. No Viewer runtime was changed.
- Fresh targeted verification of exact PR #406 head `839e5173` in its clean
  dedicated worktree on 2026-11-02 passed branding (3/3), hierarchy controls
  (6/6), and pure hierarchy grouping/schema tests (7/7). The combined focused
  run returned 22 passes and 6 failures: five workspace-load-error tests still
  stub the pre-hierarchy API and do not provide a valid
  `/api/v1/workspace/hierarchy` response; the remaining server hierarchy test
  could not load `express` because dependencies are absent in that worktree.
  These results are local source evidence only. The failures were sent to the
  Viewer chat for fixture repair; no files in its worktree were changed, and no
  deployment or GitHub Actions run occurred.
- Follow-up verification of exact pushed PR #406 head `b489c4bf` supersedes
  those fixture failures: the hierarchy URL normalization was corrected in the
  load-error test fixture. The same five-module focused command now passes
  34/34 with zero skips or failures, including server migration/API tests.
  `express` and other production dependencies were installed in the dedicated
  Viewer worktree using `npm ci --omit=dev --ignore-scripts`; no background
  process remains. The PR is still open and draft with no GitHub checks. This
  is focused source evidence only, not the broad suite, browser acceptance or
  deployed Viewer behavior.
- Re-ran `scripts/viewer_contract_smoke.py` on 2026-11-02 at Processing commit
  `b50057a` against the Viewer candidate `NodeOdmProvider` from
  `codex/viewer-v2-delivery-ledger` commit
  `1e5bfa6b9565a10d30154a17e0093d8d0af4c14e` (based on Viewer main
  `dc8cedfe`). The actual adapter and Processing coordinator passed the
  synthetic 62-upload/3-completion/1-cancellation flow: production small first
  on cluster, production large on main, then staging small on cluster; global
  TreeDar slot exclusion, staging isolation, duplicate commit, output cursor
  and repeat-download hash checks passed. The adapter hash is recorded with
  the run in `docs/evidence/viewer-contract-smoke-20261102.json`. This validates
  the candidate adapter against in-process synthetic engines only; it does not
  validate current Viewer `main`, a live worker/container, NAS custody,
  photogrammetry, Viewer archive ingestion, or TreeDar acceptance.
- Repeated the actual-adapter smoke on Processing commit `0efb6f0` against
  `apps/viewer/server/nodeOdmProvider.js` from Viewer candidate commit
  `ac1bff1f34e2b095ca36b61e6f055cae6e4f6e6e`. The adapter hash remained
  `167b7bbd66e36fdba3255ca165877e1b821308d2fc9054f1d9ba3a3b77b3c86b`; 62
  synthetic uploads, one cancellation and three completions passed with
  production-small → production-large → staging-small order. The run again
  verified small-node-first routing, global TreeDar exclusion, staging
  isolation, idempotent commit, repeat-download digest and output cursor. Full
  result is in `docs/evidence/viewer-contract-smoke-20261102-rerun.json`. This
  does not validate the live Viewer, source custody, accepted-copy receipt/ACK
  or cleanup authority.
- Re-ran the synthetic TreeDar/admission/publication suites on 2026-11-02 in
  Ubuntu 24.04 WSL as nonroot `pilot`, after installing the test-only LAS/CRS
  dependencies into a dedicated local venv. The admission, engine, delivery,
  runtime, reference API/protocol, bundle and publication suites passed all
  135 tests with zero skips. The generated LAS/LAZ fixtures exercise projected
  CRS, provenance/hash checks, genuine-LiDAR admission rules, delivery/runtime
  custody, bundle closure and publication controls. The Reference API test
  keeps Gaussian splats unavailable/disabled and rejects enabled requests.
  Results and exact dependency versions are in
  `docs/evidence/treedar-synthetic-acceptance-20261102.json`. This is synthetic
  source evidence only, not real-data accuracy or live-node/runtime acceptance.
- On the same source commit, ran the remaining worker/reference failure suites
  (`test_reference_driver`, `test_reference_worker_recovery`, `test_worker_outbox`,
  `test_reference_http`, `test_reference_execution`, `test_reference_pipeline`,
  `test_reference_transfer`, and `test_reference_results`). Together with the
  135 TreeDar/API tests above, 290 tests across 16 focused suites passed with
  zero skips in WSL. Exact per-suite counts and bounds are recorded in
  `docs/evidence/coordinated-boundary-synthetic-20261102.json`. These runs do
  not substitute for live NAS custody, actual Viewer acceptance, or host
  restart/power-loss acceptance.
- Viewer range-resume candidate follow-up: isolated LedgeTop-Ops branch
  `codex/viewer-range-resume-candidate` contains commits `46986b93`,
  `5dff688c` and `dbb267a6`; draft PR #407 is stacked on
  `codex/viewer-v2-delivery-ledger` at
  `1e5bfa6b`. Its current focused Linux tests passed 27 tests with 2
  platform-only skips, covering exact 206 ranges, complete-object digest
  before publication, interruption/restart checkpoints, replay after a lost
  acceptance ACK, exact prior-candidate journal migration, zero-byte report
  compatibility, quota enforcement, crash after link, and corrupt-prefix hold
  behavior. Review follow-up `dbb267a6` establishes WAL, FULL synchronous mode,
  foreign-key enforcement and authority before migration, avoids rollback
  after commit, and verifies that committed schema on reopen after an authority
  failure. The candidate remains unmounted, has no live authority issuer, and
  is 34 commits behind/332 ahead
  of current Viewer `main`; it must not merge to `main` before prerequisite
  reconciliation. It has no operator repair command for a corrupt saved
  prefix. No live transfer or production runtime acceptance was performed.
- On 2026-11-02, Processing branch `codex/engine-ingress-guard-candidate` at
  `d023a81` ran `python3 -m unittest discover -s tests -p
  'test_reference_*.py'` in Ubuntu 24.04 WSL as `pilot`: 672 tests ran, all
  passed, with one platform-specific skip (182.934 seconds). The suite includes
  the Reference API, result range/proxy/HTTP paths and related worker recovery
  contracts. It ran against the current shared Windows-mounted worktree and is
  synthetic test evidence only, not runtime acceptance.
- Lost source-grant acknowledgement follow-up: a separate inactive Viewer
  broker candidate `codex/viewer-grant-replay-candidate` and draft
  stacked PR #408 allow exact token replay after restart when a stable
  explicitly injected 32-byte HMAC key is available; only the token hash is
  stored. `tokenRecoverable` now reflects whether the broker can derive the
  bearer, including for new grants; random legacy grants remain unrecoverable
  if no key existed at creation. The key is held in a private field and cleared
  at close. A proposed dispatch retry seam was reviewed and reverted because
  its string error marker did not prove durable grant identity or fence an old
  sender. The lost-ACK path remains held for reconciliation. Viewer head
  `7c9ff0af` returns a digest of the broker's exact stored identity and tests
  byte-identical serialized grant recovery across broker restart. Draft PR #408
  now also contains a
  dormant migration-57 transfer outbox and repository module: it binds job,
  attempt, environment, reservation, manifest, broker identity digest, exact
  serialized grant digest and ACK digest without storing the bearer. Changed
  grant/ACK identities conflict; a `sending` row left by a process failure
  remains held. Viewer head `e7107a22` further hardens that candidate: the
  object list is derived from the immutable journaled request; `sending` and
  `uncertain` states cannot be resent under a later lease; and only the active
  sender generation can acknowledge using an unchanged response object bound
  to the sending provider instance, job, environment and grant digest. ACK
  evidence is process-local and cannot reconcile a restart. Migration 57 stays
  immutable; migration 58 tightens its transition guard forward-only, and a
  regression upgrades an already-applied v57 schema. Synthetic regression
  coverage rejects forged ACK DTOs, mutation/reuse of ACK evidence across
  providers, and stale-sender replay. This is persistence only: there is no
  production broker adapter,
  process-fence/recovery authority, automatic replay loop, key provisioning,
  or live dispatch wiring; Viewer reference dispatch remains hard-off.
- On Viewer head `e7107a22` (outbox candidate), focused migration, dispatch and
  cumulative-budget suites passed 78/78 on Linux in a disposable Node 24
  container with the required ZIP dependency installed only in its temporary
  filesystem. The same suites passed 76 tests on Windows with two
  platform-specific skips. This is synthetic schema/state evidence, not
  production key provisioning, NAS custody, enabled dispatch or runtime
  acceptance.
- Beau's read-only Viewer metadata check for task
  `a8a93a98-2740-4341-8dc6-0043ef86c72f` and provider task
  `d5463e33-aeed-4136-a81f-a9a7d233da0f` returned a matching attempt, enabled
  `3d-tiles` option, and schema-v1 receipt with matching identity, 71-file
  inventory within limits, and matching inventory digest. The mesh tileset
  path was listed; the specific point-cloud tileset path checked by the
  command was not listed. This confirms stored receipt metadata only; it does
  not establish that no point-cloud artifact exists under another path,
  independently verify archived NAS bytes, or prove LiDAR execution.
- Follow-up against LedgeTop-Ops `main` at `dc8cedfe953ce75eb2f65de846de74d076b820f8`:
  `apps/viewer/server/processingWorker.js` discovers EPT at
  `entwine_pointcloud/ept.json` and point-cloud source files at
  `odm_georeferencing/odm_georeferenced_model.laz` or `.ply`. Current ingest
  requires EPT and a complete textured mesh closure; generating the EPT
  derivative also requires a LAS/LAZ source. The saved receipt check did not
  report whether EPT or the LAZ source appears in its inventory, so the prior
  result cannot yet establish whether this attempt had the point-cloud inputs
  Viewer requires. No deployed Viewer behavior or current on-disk artifact was
  checked by this source review.
- Viewer isolated candidate `38f1e4a8` adds
  `apps/viewer/scripts/verify-archive-readonly.cjs`, a path-free, read-only
  point-in-time check that uses the app config and stored receipt to compare
  exact expanded archive paths, sizes and hashes. I independently reran its
  bounded synthetic test on Windows: 1/1 passed for the 71-file match,
  mutation, missing/extra files and ambiguous/unresolved identity. Windows
  denied symlink creation, so that case was not exercised. The candidate is
  not in the deployed image; Node/runtime/mount equivalence and Linux
  symlink/ancestor-race behavior are unverified. Follow-up candidate commit
  `a27c4430` adds file/directory identity checks around the walk and a synthetic
  parent-replacement case; the focused Windows test again passed 1/1. This is
  still path-based, not descriptor-pinned, and Windows denied symlink creation.
  Candidate follow-up `ff52034d` now distinguishes confirmed structural
  mismatches from read failures and limits: an injected EIO returns generic
  `UNAVAILABLE read`; the focused Windows test passed 1/1. I independently ran
  that exact test on Linux using ephemeral Node 24 image
  `sha256:c86ecead9e55a5210ac1727de38a4ca840bf03dcaf34dc9749addbf383b1920c`;
  1/1 passed, including symlink rejection and injected EIO classification.
  The container was CPU-limited, memory-limited, network-disabled, and removed
  on exit. This verifies the synthetic script on Node 24 Linux only; it does
  not qualify the deployed Viewer image, the NAS filesystem, or durable
  custody.
  A 500-GiB sequential scan is possible. This may provide a manually scheduled
  integrity observation after release qualification, but it is not durable
  custody or an accepted-copy ACK. Do not give Beau a production command until
  the exact installed build is matched and bounded Linux tests pass.

## Live host observation — 2026-11-02

- Read-only SSH `docker ps` to cluster PC `192.168.50.89` showed the Processing
  prototype container healthy, the migration NodeODM candidate running, and
  the ingress and TLS helper containers running. The snapshot did not inspect
  the candidate's readiness endpoint, active job state, credentials, storage
  custody or result handoff.
- Read-only SSH `docker ps` to main PC `192.168.50.88` showed its NodeODM,
  ingress, DCGM exporter, and node exporter containers running. This snapshot
  did not verify NodeODM authentication/health, current workload, TreeDar
  availability or Viewer delivery.
- A read-only, non-disclosing comparison on `.89` found both the coordinator's
  `LOCAL_NODE_TOKEN` and the migration NodeODM `--token` present and matching;
  no token or digest was emitted. This does not establish that the matching
  credential is a newly rotated value. The planned rotation and old-token
  rejection check remain outstanding, so do not treat credential hygiene as
  closed.
- Therefore the two nodes are reachable and their listed containers are up;
  this is not end-to-end runtime acceptance. Do not admit client work on this
  evidence alone. The full receipt, storage-owner, dispatch and durable ACK
  gates above remain in force.
- Refreshed read-only SSH check later on 2026-11-02: `.89` reports the
  Processing prototype container healthy at image
  `sha256:7fae9385fb5ca829783fe05eef2fdb1983abf4d491b2b9f795e55b617a5a0004`
  and the migration NodeODM candidate running at image
  `sha256:5ce31be03711d280e7f97ee3820dd982b68227181165b3f430b037447c3d13b6`.
  `.88` reports its NodeODM container running at image
  `sha256:b9d9a1a4603db9f092b790a6e58586fcf944758b7ca1149daefb2bd55dc6bdde`,
  with DCGM and node exporters also running. This verifies current container
  presence and one Docker healthcheck only; no task was submitted, no engine
  API/options or active-work state was read, and no worker→Viewer completion
  was exercised.
- Same read-only host check found cluster `.89` root filesystem at 295 GiB
  total / 111 GiB available. Its underlying LVM PV is 929.51 GiB, with
  629.51 GiB free in the volume group and only a 300 GiB root LV. This explains
  why the reported 1-TB SSD does not currently appear as 1 TB available to
  Docker. Main `.88` root has 1.8 TiB total / 1.4 TiB available; its NodeODM
  data directory is on that filesystem. A 50-GB-class dataset should route to
  `.88` under the verified size policy. No disk or LVM changes were made.

## Cross-repo grant ACK audit — 2026-11-02

- Processing v1 defines a closed transfer-grant ACK containing
  `reservationId`, `generation`, `manifestSha256`, `nonce`, `accepted: true`,
  and `grantSha256`; the latter hashes the canonical exact grant, including
  its bearer token. The Processing shared-fixture test passes for this shape
  and its token canonicalization vector.
- The Viewer candidate at `codex/viewer-atomic-grant-fixture` commit `5cc4b177`
  initially accepted only the five legacy fields and its fixture omitted the
  ACK/vector. The Viewer working diff now requires and compares
  `grantSha256`, imports the Processing fixture fields, and has synthetic
  provider tests for wrong hashes, changed tokens, and status reconciliation.
  I independently ran `reference-grant-ack-lockstep.test.js` with
  `reference-app-dispatch.test.js`: 26 passed, 1 skipped (Linux descriptor and
  directory barriers unavailable on this host), 0 failed. The Viewer changes
  remain uncommitted in that candidate worktree as of this review.
- This does **not** yet provide durable lost-ACK recovery. The actual
  `referenceAppDispatch` builds its status binding without `grantSha256`, and
  neither the application journal nor dispatcher persists and reloads an
  immutable grant digest before sending the grant. Consequently a status
  response containing the six-field ACK fails closed with
  `BINDING_REQUIRED` unless the caller supplies an intent digest. Provider
  unit tests do not establish restart or owner-handoff recovery. Viewer traced
  the wider path and confirmed a journal-only hash callback would still be
  unsafe: grant minting commits in a separate broker SQLite transaction, so a
  crash or unknown commit can occur before the Viewer journal records the hash;
  the callback is not an enforceable pre-POST boundary when the dispatcher
  hands the provider directly to `dispatchOnce`; and an expired lease is not
  safely reclaimable without fencing the physical writer. The broker-owned
  encrypted same-transaction issuance fixture remains disconnected from
  dispatch and lacks production authentication, rollback lineage, source
  fencing and trusted receiver reconciliation. A safe recovery design needs
  broker-owned atomic issuance plus durable exact-grant outbox, authenticated
  binding, and fenced recovery; a Viewer journal hash alone is insufficient.
  The production reference lane remains hard-disabled. No live deployment or
  data operation occurred.

## Local candidate validation — 2026-11-02

- Commit `68c9d4b` on `codex/accepted-copy-ack-candidate` adds the fixture-only
  v2 verifier, pinned crypto requirements, and an install step in the existing
  main-only GitHub check job. It is pushed to the existing draft PR #335, which
  remains stacked on `codex/engine-ingress-guard-candidate`. The feature push
  started no GitHub Actions runs. The main-branch checks still require a later
  review/merge to execute on GitHub.
- The full local Processing unittest discovery completed 1,334 tests with
  578 platform skips and no failures on Python 3.13. This included the current
  untracked host-supervisor experiment test file, which is not part of the
  commit. TreeDar prototype-worker tests passed 36/36; package-publication
  boundary tests passed 5/5. The focused v1/v2 retention suite passed 18/18
  both on the host and inside the rebuilt Linux image as UID 10001. The
  temporary image tag was removed after the run.
- These are local synthetic/code checks; GitHub's Python 3.10 job has not run
  for this branch, and no live dispatch, NAS write, client processing, Viewer
  acceptance, or cleanup path was exercised.

## Read-only Viewer storage inventory — 2026-11-02

- Read-only SMB metadata enumeration found 497.95 GiB across Viewer Storage:
  cache 104.93 GiB, datasets 174.27 GiB, imports 33.57 GiB, models 174.06 GiB,
  trash 10.99 GiB, and data 0.14 GiB. WebODM's `Media/project` tree held
  388.08 GiB. This compares folder contents only; it does not equate the
  applications' accounting or prove broad duplication.
- The Viewer `imports` and `trash` trees each contain a same-name/same-size
  set of 1,120 JPG files totaling 11,796,893,696 bytes; `datasets` contains
  same-name/same-size matches for all 1,120. A ZIP in imports contains the same
  1,120 JPG basenames and uncompressed sizes. Windows SMB file identities for
  all 1,120 matches differ pairwise across imports, trash and datasets,
  establishing three separate sets of file objects, not byte-identical
  content. No payload hashes were computed and no files were changed or
  removed.
- A read-only Viewer API SQLite query on 2026-11-02 matched one live
  `dataset` trash record of exactly 11,796,893,696 bytes, with a purge deadline
  of `2026-11-18T03:23:59.389Z`; it is not overdue. The corrected query joined
  through a mutation whose destination root is `trash`, because `root_key`
  stores the original root. The size and type strongly suggest this record
  corresponds to the inspected folder, but its stored relative path was not
  compared to the on-disk folder; this is not yet an exact path-to-record
  reconciliation. No data was changed or removed.
- The ZIP in imports also has 2 TIFF, 118 PNG, 4,559 LAZ, and other entries.
  Five `.zip` files in models (16.76 GiB each) start with a ZIP local-file
  header but lack an end-of-central-directory or ZIP64 locator in the last
  65,557 bytes; `ZipArchive.OpenRead` could not parse the first archive. Their
  producer/database ownership and intended lifecycle are unknown. Do not purge
  them based on extension or size alone.
- Current Viewer source (`dc8cedfe`) schedules `purgeExpiredTrash` from the
  worker maintenance loop; manual assets receive a 14-day trash retention
  deadline and automatic output retirement receives 7 days. The live DB
  retention row and due state for the 1,120-file trash folder were not read,
  so whether it will be automatically purged is unverified. No manual cleanup
  was performed.
- Read-only review of the untracked host-supervisor candidate confirmed its
  header's inactive scope: no installed service or deployment wiring, no
  kernel descendant containment proof, and no safe restart after an uncertain
  child epoch. Its synthetic tests cover one direct Python leaf and parent
  death only. It is not evidence that the worker service is installed or that
  Docker/NodeODM descendants can be fenced; the live user service remains
  absent on both processing PCs.
