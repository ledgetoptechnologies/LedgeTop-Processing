# Coordinated release acceptance plan

This is an execution checklist, not evidence of deployment or approval for client
data. The complete Processing/Viewer goal remains active. Photo-job acceptance is
the first operational milestone, not a replacement for the remaining scope.

## Integration decisions

- Use V2 Viewer source admission, ReferenceStore, the node-local reference worker,
  and the existing durable result/accepted-copy receipt chain as one path.
- Coordinated multi-node configuration must refuse the legacy direct Viewer
  executor. Do not add a second node execution or receipt protocol.
- Allow one job per physical host, with different hosts executing concurrently.
  This supersedes the original global one-job execution rule. All engines on a
  host share its reservation. Production outranks waiting staging work without
  interrupting an already running job.
- Prefer the first eligible idle node. Cluster accepts at most 1,000 photos;
  main has no photo-count cap. Node order and eligibility are configuration,
  not hard-coded node-count assumptions.
- Start deployment qualification with the existing systemd worker unit and
  queue/node gates. The inactive custom host-supervisor experiment is outside
  this release; it cannot currently establish restartable engine quiescence.
- Keep results and uncertain outcomes. Unknown acceptance, failed fsync, expired
  connectivity or restart never authorizes deletion or duplicate engine work.

## Work ownership and sequence

1. Source-intake owner integrates spool, Store, HTTP and runtime configuration,
   preserving existing receipt configurations. Verify interrupted publication,
   directory-fsync failure, immutable identity, capacity and replay behavior.
2. Worker integration owner incorporates pinned live-engine probes, cleanup
   checks and refusal of the alternate direct executor. Preserve reservations
   when identity is unavailable or mismatched.
3. Combine both on one exact revision. Run affected Linux tests and the actual
   Viewer V2 client against it; historical tests on separate heads do not qualify
   this combined revision.
4. Prepare immutable builds, configuration and installation/rollback records.
   Verify installed identities, persistent paths, TLS, gate ownership and unit
   lifecycle. Only Beau performs TrueNAS changes. Its spool uses a separate
   subfolder in the existing Viewer Storage dataset.
5. Run the acceptance matrix below, then a small real mapping dataset. Large
   client jobs remain unqualified until these operational gates pass.

## Photo-job acceptance matrix

Every row needs the exact Processing and Viewer revisions, test/run identity,
observed result and retained-state/cleanup outcome. A synthetic test does not
establish installed or real-data acceptance.

| ID | Exercise | Required observation |
| --- | --- | --- |
| P1 | Actual Viewer V2 submit and interrupted POST replay | One immutable job; no alternate legacy submission or duplicate admission |
| P2 | Two eligible small jobs while both hosts are idle | First uses cluster; next can use main; at most one engine job per host |
| P3 | Oversized job while main is busy and cluster becomes idle | Job remains queued for an eligible node; cluster never receives it |
| P4 | Reorder/add/remove configured nodes | Selection obeys current eligibility/order; unavailable nodes do not strand otherwise runnable jobs |
| P5 | Waiting production and staging jobs | Production goes first; running jobs retain ownership |
| P6 | Queue, engine execution and logs | Viewer reports queued until actual execution, then exposes correct status and node logs |
| P7 | Restart during upload, engine submission and execution | Replay reconciles durable identity; no duplicate engine start or lost source |
| P8 | Worker finishes with coordinator unavailable | Result/outbox persists; reconnect delivers the original completion |
| P9 | Viewer unavailable or acceptance reply lost | Durable delivery retries and reconciles acceptance without deleting the only copy |
| P10 | Disk full, corrupt content, changed engine identity | New/unsafe work holds; no false success, silent deletion or competing execution |
| P11 | Confirmed result acceptance and cleanup | Only explicitly releasable processing copies disappear; Viewer result stays readable |
| P12 | Drain, update, failed health check and resume | No new host claims while drained; running work finishes; rollback only in its supported pre-reopen window |
| P13 | Installed small real mapping job | Results return to Viewer and can be opened; retained sources and logs are accounted for |

## Remaining full-goal deliverables

### Source retention and release integration

The live intake stores source/derived custody receipts with
`until_explicit_source_release_v1`, but has no release Store/API operation or
completed-object deletion executor. The older V1 retention validator and
unmounted V2 proposals use different guarantees and must not be substituted.
Viewer creates held source snapshots, but its accept/release helpers currently
have no production caller. Keep source deletion disabled for the pilot.

Complete this through the existing intake, Store and API perimeter after the
provider-admission/pilot gate; do not add another processing controller:

1. Agree the explicit release operation with the Viewer owner, binding exact
   job/generation/environment, authenticated owner, source/derived receipts,
   accepted result-copy identity, release set and retention policy. A copy ACK
   is a prerequisite, not source-delete authorization. Proposed initial policy
   also waits for successful Viewer publication; this still needs the coupled
   worker decision and is not an active policy.
2. Persist authorization, deletion intent, per-object outcomes and a replayable
   release receipt/tombstone. Fence new grants and drain existing source readers
   before effects. Recheck the configured mount/root/marker and each exact
   no-follow, owned, single-link file identity; require directory fsync before
   acknowledging deletion. Uncertain outcomes retain the durable intent and
   hold further cleanup rather than inventing success.
3. Release only eligible processing-spool raw-input and attempt-derived copies.
   Preserve permanent GCP holds, Viewer originals and accepted result objects.
   Startup reconciliation must recognize exact release tombstones; unexplained
   missing files must still hold. Retire only the exact Viewer's snapshot hold
   after its agreed release boundary, never by clearing unrelated holds.
4. Qualify lost replies, restart at each filesystem boundary, active readers,
   grant revocation, wrong identity/receipt, mutation, mount replacement and
   failed fsync using task-owned synthetic data. Beau still controls TrueNAS
   administrative changes; no live deletion follows merely from this plan.

### Unattended maintenance integration

The existing updater can create an owned drain plan, wait for the scheduled
boundary, empty running/held queue, free physical gate and idle backend, then
perform health-checked replacement and supported pre-reopen rollback. Reference
claims use the same authoritative queue node state, so they are blocked once
draining starts. Installed qualification must prove both APIs use the same
authority/database; the historical October 28 updater receipts do not do so for
this new release.

Before enabling its timer, bind the exact source/config/state and use a qualified
dependency interpreter. The current updater renderer uses system Python, unlike
the reference worker's prepared venv. Preserve the maintenance loopback/tunnel
transport separately from reference worker TLS. Exercise scheduled drain,
running-job completion, failed health/rollback and successful reopen with the
installed reference worker and durable outboxes. The adapter currently chooses
time-based plans, not an `afterJobId`; qualify that job-specific path before
offering it. Updater/release inventories still hardcode main/cluster and must be
made config-driven before claiming automatic maintenance for a future third
node. Container updates remain in scope; OS updates stay deferred.

The worker also pins its configured engine version. `ReferenceDriver._ready`
refuses claims when `/info.engineVersion` differs from that pin; it refreshes
allowed options only after that check. A container upgrade must therefore pair
its qualified version with the worker's existing idle pin-migration/config
handover before reopening the node. Merely swapping an image and reopening the
queue is insufficient. Preserve old result provenance and pending outboxes;
never weaken the version check to make an update appear successful.

After the photo milestone, qualify genuine LAS/LAZ TreeDar input and typed outputs
on main using the same per-host reservation and durable handoff. Keep Gaussian
splats disabled while exposing only its agreed future scaffold. Finish NAS source
custody, retention and disk/capacity safeguards; do not describe logical holds as
physical quotas. Qualify unattended maintenance and reboot recovery. Complete
Viewer rendered UI acceptance, Operations-aligned dark colors, and the customer
to project/subcategory to task hierarchy. Record current Claude ownership from
an actual acknowledgement, not historical co-author trailers.
