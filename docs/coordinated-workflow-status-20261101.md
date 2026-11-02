# Coordinated Processing and Viewer status — 2026-11-01

This is a current evidence checkpoint for the Processing + Viewer delivery goal.
It distinguishes local/synthetic evidence from installed or production behavior.
No real client data was processed as part of this checkpoint.

## Verified in the current work

- Processing branch `codex/engine-ingress-guard-candidate` contains local commit
  `b8b08c8` (`Stage processing wrapper and integration evidence`), one commit
  ahead of its origin. It has not been pushed or merged.
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

## Still required for the requested end state

1. **NAS-backed queue and reserved just-in-time transfer:** current processing
   uploads are staged by the Viewer adapter to coordinator-local bounded
   temporary storage. A durable NAS source reference, reservation protocol,
   transfer resume and queue/capacity integration are not verified as
   operational.
2. **Per-node control and unattended resilience:** the source has node admission,
   maintenance and durable worker receipt mechanisms, but current host wrapper
   is local reviewable code. It is not installed as a supervised service on
   both nodes. No reboot-safe automatic claim/retry service is verified.
3. **Full result acceptance and cleanup:** a worker completion ACK records
   metadata/hashes, not durable Viewer artifact custody. Viewer v1 receipt
   metadata is present for the inspected ODM task, but live worker-to-Viewer
   transfer, exact durable ACK, replay after real restart, and post-ACK bounded
   cleanup are not proven as one production path. Retained result copies must
   not be deleted based on download or remove alone.
4. **TreeDar:** the source candidate requires a genuine-LiDAR admission and a
   12-artifact native output closure. Current Viewer bridge accepts one ODM
   archive and does not accept this typed closure. Processing and Viewer still
   need an agreed mapping, provenance binding, durable custody record and exact
   acceptance ACK. No TreeDar job was run against real LiDAR.
5. **Credential hygiene:** prior tool output exposed the active candidate
   NodeODM startup token. The user was advised to rotate it and update the
   matching private Processing credential through an approved maintenance
   route. Rotation is not verified. Do not use the exposed value or start
   client processing until the replacement is confirmed.
6. **Viewer redesign and Claude:** the Viewer chat has historical redesign
   work and a candidate customer → project → task hierarchy. Current Claude
   acknowledgement/ownership is not verified; the internal message-bus MCP
   is unavailable in this chat. The UI work is not verified deployed.
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
