# Coordinated Processing and Viewer status — 2026-11-01

This is a current evidence checkpoint for the Processing + Viewer delivery goal.
It distinguishes local/synthetic evidence from installed or production behavior.
No real client data was processed as part of this checkpoint.

## Verified in the current work

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
- Read-only live probes on `.89` showed the coordinator healthy, provider
  discovery available and a 25-photo plan selecting local execution without
  starting work. The main NodeODM container was observed running. These probes
  did not submit a Viewer task or run photogrammetry.
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
6. **Viewer redesign and Claude:** the Viewer chat has historical redesign
   work and a candidate customer → project → task hierarchy. Current Claude
   acknowledgement or active participation is not verified; the internal
   message-bus MCP is unavailable in this chat. Historical design notes assign
   frontend ownership and specify Operations-matched dark accents. The concrete
   Greenleaf → New Build / Project Y → tasks example fits the candidate
   `groupId=null` structure, but this is not an authorization boundary. The UI
   work is not verified deployed.
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
  state still shows #406 as a draft with no recorded review, approval, or PR
  comment. The Viewer chat also reports no current Claude acknowledgement or
  owner assignment for the refreshed branch; historical co-authorship on #347
  does not verify active participation. This is review evidence only, not a
  Viewer deployment or live Processing acceptance.
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
