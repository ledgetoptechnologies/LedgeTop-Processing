# Authenticated source release and Viewer operational holds: review proposal

Status: design only. Baseline Processing `c217d98`; Viewer read-only baseline
`6718d1b`. No release executor is enabled and no storage is changed. Beau approved
the gate: verified Viewer outputs ready plus 24 hours, verified delivery, and no
active readers/uploads. Reader fencing and durable worker
observation are prerequisites, not deletion authority.

## Existing evidence and concrete gaps

* `reference_source_intake_store.py` records signed preclaim authority, exact raw
  and derived manifests, per-object storage receipts, and completion receipts
  promising `until_explicit_source_release_v1`. `reference_acceptance_observed.py`
  now durably records the assigned worker's verification of exact accepted-copy
  receipt pairs with full result coverage. Its ACK is a historical fact, not
  permission to delete. Completed/custody-held replay must remain possible.
* Viewer API Bearers currently map to environment, not owner principal.
  Original preclaim attestation does bind principal/issuer/key/authority and
  snapshot SHA. It does not include a snapshot ID. A release operation must
  compare to that persisted identity, and must not advertise current
  environment-only routes as partner isolation.
* `reference_source_readers.py` owns durable read pins, the private process lease,
  and permanent per-job/generation fences. Fences currently block grant/read
  admission. Streaming source PUT uses expectation -> receive -> receipt with
  no equivalent in-flight operation pin. Completion and reconciliation also
  access source files. These operations need the same fence/lease, including
  checks before filesystem effects and before committing an accepted state.
* Range-transfer physical inode pins are only captured for selected engine
  objects. They are not a complete physical inventory for raw and derived
  release candidates. Logical source receipts do not contain inode identity.
* `SourceSpool` has verified reads and owned partial-file cleanup, but no safe
  completed-object deletion method. Its `_directory` creates missing directories;
  a deletion/recovery path must never call it to recreate released trees.
  `audit_job` and source reconciliation presently require every expected file.
  They need exact authorized tombstones, not a generic missing-is-released rule.
* `reference_source_intakes` only allows receiving/accepted/held. Public job
  projection treats nonaccepted source as still receiving. Release needs an
  independent custody projection, without changing completed result history.
  `reserved_bytes` is upload reservation, already zero at completed intake;
  deletion must not subtract retained bytes from it again.
* Viewer `heldSourceSnapshot.js` accept/release helpers are unmounted candidates,
  with generic verifier callbacks and no exact retry contract. Repository
  `hasHeldSourceSnapshot` checks only held, while `hasRetainedSourceSnapshot`
  checks ANY snapshot row, including released. Consequently accepted would
  prematurely stop one mutation guard, while released would never retire the
  dataset/project trash/purge guard. Immutable audit history and active custody
  must be separate concepts.
* Viewer candidate-generation reservations independently prevent snapshot
  release. That review-only path is not proven to be the live source path; its
  independent hold must not be silently cleared. Permanent GCP retention tables
  and source objects marked `permanent_viewer_hold` remain outside release.
* Existing source-retention/acceptance-release candidate modules use older
  incompatible contracts and are not mounted. Do not mount them as a shortcut.

## Policy decisions still required

Beau approved release only AFTER Viewer outputs are verified ready plus 24 hours,
with verified delivery and no active readers/uploads. Actual derived-output readiness
must be proved; ingestion alone, unpublished conversion-pending outputs, or an
accepted-copy ACK alone is insufficient. Viewer originals, accepted results, and
raw GCP CSVs remain retained. This policy decision does not qualify storage or
approve unsafe deletion. No unrelated trash retention is reused.

Production release stays disabled until the approved policy has a qualified implementation and filesystem qualification.
Synthetic tests can inject an explicitly test-only zero-delay policy. A positive
minimum duration needs conservative elapsed-time accounting: only within-process
monotonic progress under verified ownership counts, persisted checkpoints survive
restart, and uncheckpointed progress/downtime is not credited. UTC `observedAt`
is audit evidence, not proof against forward clock jumps. The existing read
admission rollback check is not a durable clock guarantee.

## Proposed closed authorization and API

New owner-scoped Viewer operations, provisionally:

* `POST /ltds/v2/jobs/{jobId}/source/releases` accepts a persisted signed intent.
* `GET /ltds/v2/jobs/{jobId}/source/releases/{releaseId}` returns its exact status
  or terminal receipt, never paths or worker claim credentials.

Require protected transport, existing Bearer plus a server-configured authenticated
principal binding, and a domain-separated Ed25519 release signature. Pin
environment, issuer, key, principal, and coordinator authority for a release role;
compare principal to the original preclaim principal. An environment Bearer or a
browser boolean alone cannot authorize deletion. Reusing a signing key requires
explicit release-role provisioning; no implicit new permission for existing keys.

Proposed signature domain: `ltds-viewer-source-release-v1\0`. Closed signed payload:

```
schemaVersion, protocol, releaseId,
issuerId, keyId, principalId, coordinatorAuthorityId,
jobId, generation, environment, viewerTaskId, viewerAttemptId, activationId,
preparedEnvelopeSha256, sourceSnapshotId, sourceSnapshotSha256,
inputManifestSha256, pipelineDigest, resultManifestSha256,
sourceManifestSha256, derivedManifestSha256,
sourceReceiptSha256, derivedReceiptSha256,
processingAcceptanceId, processingAcceptanceSha256,
viewerCopyReceiptId, viewerCopyReceiptSha256, copyAckSha256,
workerObservationId, workerObservationAckSha256,
viewerMilestone:{kind, attemptId, modelId, modelVersionId, evidenceSha256},
policyId, policySha256,
eligibleObjects:[{id, objectKind, byteSize, sha256}], eligibleClosureSha256
```

The wrapper contains the canonical payload, its digest, and signature. Exact
field spelling, maximum canonical bytes/counts, sorting and integer bounds must
be frozen in shared JS/Python vectors before implementation. The selected receipt
pair must be among the observation's verified pairs and cover the exact complete
result. If more than one pair is needed, replace the singular fields with a
sorted exact pair list; do not accept a partial result closure.

Coordinator derives the eligible set from persisted source+derived manifests:
all and only `release_after_acceptance` objects. Every `permanent_viewer_hold`
object is excluded on success, failure, cancel, and expiry. Source paths, storage
keys, inode claims, `fullyAccepted`, and client deletion timestamps are forbidden.
The supplied snapshot ID is opaque local Viewer context; Processing validates its
SHA against the original preclaim proof, and Viewer must verify its own exact ID
mapping on receipt. Do not pretend Processing previously knew that ID.

Viewer's trusted release signer must verify the server-owned durable milestone
against the exact attempt/output before signing. No existing separately signed
publication proof is assumed. Signature-key revocation/resume behavior needs a
closed rule: explicit revocation holds unfinished operations; key rotation must
retain verification material for exact historical receipts. Reject mutation of
an already accepted intent or reuse of a release UUID for another generation.

## Coordinator state machine and storage mechanics

Job/generation release journal: requested -> waiting_policy -> fenced/draining ->
deleting -> released; discrepancies become held. No automatic cancel/failure/TTL
deletion path. Source execution/result phase and immutable completion receipts
remain independent. Exact journal and receipt replay survives later custody holds.

1. Verify owner, signatures, immutable digests, complete receipt coverage, assigned
   worker observation, policy, milestone assertion, and canonical eligible set.
   Reserve bounded terminal journal/tombstone quota before any destructive effect.
2. Once policy permits effects, atomically commit immutable release intent and
   per-job/generation fence in ONE queue DB transaction. The current standalone
   `fence()` transaction needs a transaction-scoped seam for that future caller.
   Block new read/grant/renewal/ACK, source PUT/completion/adoption/reconciliation
   access; drain all pre-existing registered operations. A fence is not a global
   idle lock and must not stall unrelated jobs/nodes.
3. Under the existing verified coordinator process ownership, persist the exact
   physical inventory for every eligible object. Open root/job/generation via
   descriptor-relative nofollow traversal without creation. Verify mount marker,
   parents, private writer isolation, regular single-link ownership, size, hash,
   and stable physical identity. Unknown entries or changed identity hold.
4. Use a per-object durable phase journal: planned -> verified -> quarantining ->
   quarantined -> unlinking -> removed+directory-fsynced -> tombstoned. Rename
   with qualified no-replace semantics into a private same-filesystem quarantine;
   verify descriptor/entry identities and fsync affected directories before
   acknowledging progress. Persist enough identity before each effect to recover
   ambiguous outcomes. Missing alone is never success. Never recursively delete
   a job tree, follow links, or delete permanent-retained objects.
5. Reconcile and complete/replay APIs recognize only matching authorized durable
   tombstones and never recreate released objects or revive grants. Capacity
   becomes available based on durable removals/filesystem measurements, not an
   invented decrement of the completed upload reservation. Retained permanent
   files remain visible to custody audits.
6. Terminal release receipt is minted once only after every eligible object's
   required fsync barrier and tombstone. Bind authority/job/generation/owner,
   authorization hash, observation ACK hash, policy, exact eligible closure,
   removed count/bytes, preserved permanent closure, and server time. Status before
   that is pending/held, never terminal success. Exact retries return stored bytes.

`ScratchCleanupExecutor` supplies useful verified-open/quarantine/fsync/journal
patterns, but its worker ScratchLedger is NOT NAS authority and must not be reused
as the release controller. Qualify actual NAS rename/no-replace/fsync semantics;
do not assume Linux renameat2 support on an unqualified remote filesystem. Windows
fails closed for enabled Linux lease/deletion features. Bounded background work
and retry backoff preserve scheduler fairness. No new indefinite global scratch
pin: existing signed-copy cleanup remains valid while source release is pending.

## Viewer operational hold retirement

Add a separate durable release outbox/job that runs after the approved milestone,
including already published attempts. Do not reuse expired source-submit/reconcile
leases. Read the authoritative worker-observation ACK, build and persist exact
canonical signed intent before POST, then retry that identity with bounded backoff.
After a lost reply, query/replay; never invent a second release intent/controller.

Persist and verify the exact terminal release receipt before retiring the exact
Processing custody hold in a local transaction. Use a separate qualified
operational-hold/retirement receipt table keyed by snapshot, authority, job,
generation and attempt. Preserve immutable snapshot/receipt history. An ACK for a
different snapshot/attempt/generation cannot clear any hold. Exact retry is
idempotent; changed receipt is a conflict.

Unify repository guards around active Processing operational holds PLUS permanent
GCP holds PLUS independent retained review/legacy-unknown holds. Audit every
dataset/project/task mutation, trash, restore and purge guard. Existing snapshots
are conservatively held until exact live custody bindings can be reconstructed;
no blanket historical release and no dropping candidate immutability triggers.
Existing dataset/snapshot FK audit rows can remain: ordinary storage purge already
uses tombstones rather than deleting all metadata. Release itself never deletes
Viewer originals, published outputs, or retained GCP sources; later normal user
storage lifecycle is a separate action.

## Implementation slices and qualification

1. Shared source-operation lease/fence coverage for PUT, completion and audit;
   transaction seam and race/error/restart tests. No deletion.
2. Freeze owner-role/signature/policy/receipt wire contract and cross-language
   vectors; add disabled coordinator journal/status and Viewer release outbox.
3. Add qualified SourceSpool physical inventory/quarantine/delete primitive and
   per-object recovery journal behind disabled configuration; tombstone-aware
   audits, replay, quota, custody projection.
4. Add Viewer exact operational retirement and consistent guards; preserve GCP,
   independent review holds, and immutable metadata. Viewer owner handles edits.
5. Operator-reviewed policy and NAS-only synthetic qualification before enablement;
   then a disposable end-to-end test, never client data first.

Required tests: JS/Python canonical/signature vectors and negative type/size/owner/
environment/generation/digest/closure cases; actual HTTPS intent/status lost replies
and restart; one wrong receipt cannot retire a hold; source reader+writer races
and fence atomicity; generic result cleanup while release waits; inode/hardlink/
symlink/mount/parent changes; each DB/fsync/rename/unlink crash boundary; unknown
missing files versus exact tombstones; metadata quota and disk-full before effects;
exact capacity and permanent-object preservation; complete/PUT/grant replay cannot
recreate bytes; real source reconciliation after authorized deletion; unrelated
jobs continue; all Viewer trash/mutation/purge paths retain other holds; positive
window clock/reboot safety. Filesystem synthetic tests do not prove physical NAS
power-loss durability or provisioned ACL isolation.
