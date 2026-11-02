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
  at the time checked and not merged into the checked production revision. Its
  focused workspace suite passed 60/60 tests, including real headless Edge
  desktop and mobile browser assertions for hierarchy, layout, load failures,
  task navigation and permission behavior. The suite left three test-owned
  browser profiles/process trees; exact Edge process trees were stopped and the
  test profile paths were verified absent afterward. This verifies the branch's
  candidate behavior, not Claude's current participation or production rollout.
  The separate hierarchy-controls candidate contains the customer/group and
  project controls and tests, but that commit also is not an ancestor of the
  checked production revision. Grouping remains separate from client access.
