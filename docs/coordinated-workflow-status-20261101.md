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
  prove no EPT output exists elsewhere.
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
  range resume or physical request/byte meter; failed object transfers restart
  from byte zero. Worker result bytes are exposed separately by an authenticated
  result GET bound to generation/object/result-manifest hashes and durable read
  pins, and the worker endpoint accepts ranges. The coordinator's current
  `ResultProxy.open` requests the whole object and requires HTTP 200, so there
  is no end-to-end range resume. The worker's `/results` POST is a metadata
  completion receipt, not an archive upload; Viewer acceptance is a later,
  separate receipt. No NAS broker service or mounted Viewer accepted-copy
  ingest is implemented. This mapping is an existing-source audit, not a new
  cross-language contract.
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
  `claude/viewer-workspace-redesign`, three commits ahead of its remote branch
  at the time previously checked and not merged into the checked production
  revision. A fresh ancestry check on 2026-11-02 shows that branch is now 17
  commits ahead and 116 commits behind the current `origin/main`; it is not an
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
