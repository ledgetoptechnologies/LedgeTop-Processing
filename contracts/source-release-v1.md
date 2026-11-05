# Authenticated source release v1

Review contract. Default disabled; this document and its vectors alone do not
qualify physical deletion. Viewer must create the proof from server-verified
durable output closure, never a caller-provided ready flag or processing status.

POST `/ltds/v2/jobs/{jobId}/source/releases`; GET
`/ltds/v2/jobs/{jobId}/source/releases/{releaseId}`. Both require protected transport,
the actual environment Bearer plus explicit owner-principal SHA-256 binding and a
separately provisioned release-role signature/key pin. Intake signing permission
does not imply release permission. The coordinator compares the original verified
preclaim principal/attempt/activation/snapshot/envelope binding and persisted
source/result/acceptance/worker-observation metadata.

Closed wrapper: `authorization`, `signature`, `authorizationSha256`.
Sign `ASCII("ltds-viewer-source-release-v1\0") || canonical_json(authorization)`
with Ed25519. `authorizationSha256` is SHA-256 of those exact signed bytes;
signature is canonical base64url without padding (86 characters / 64 bytes).
Canonical JSON follows existing Reference protocol canonicalization (ASCII,
sorted keys, no whitespace); array order is significant. Wrapper maximum 4 MiB.

Closed authorization fields:

* `schemaVersion`: integer 1; `protocol`: `ltds-viewer-source-release-v1`.
* UUIDs: `releaseId`, `viewerTaskId`, `providerTaskId`, `viewerAttemptId`,
  `activationId`, `sourceSnapshotId`, `workerObservationId`.
* ASCII slugs: `issuerId`, `keyId`, `principalId`, `coordinatorAuthorityId`, `jobId`.
  `providerTaskId` must equal `jobId`; the actual private V2 job ID is a UUID.
  This is the preassigned V2 coordinator intake identity for every pipeline,
  including TreeDar-only. It does not require a native NodeODM task. The nullable
  generic/V1 diagnostic field does not replace the immutable V2 binding.
* `generation`: positive safe integer. `environment`: production or staging.
* Lowercase hex64: `preparedEnvelopeSha256`, `sourceSnapshotSha256`,
  `inputManifestSha256`, `pipelineDigest`, `resultManifestSha256`,
  `sourceManifestSha256`, `derivedManifestSha256`, `sourceReceiptSha256`,
  `derivedReceiptSha256`, `workerObservationAckSha256`, `eligibleClosureSha256`.
* `receipts`: exact worker-observation pair list, sorted/unique by acceptance ID;
  each is closed `{acceptanceReceiptId,acceptanceSha256,viewerReceiptId,
  viewerReceiptSha256}`. IDs are slugs, digests hex64; Viewer IDs also unique;
  1–10,000 pairs. Pairs must equal persisted fully covered signed-copy observation
  evidence. There is no nonpersisted transport copy-ACK digest.
* `readyProof`: closed `{kind,attemptId,modelId,modelVersionId,
  outputInventorySha256,verifiedAt}`. Kind is `verified_outputs_ready`; IDs are
  UUIDs, attempt equals `viewerAttemptId`, digest hex64, timestamp exact valid
  `YYYY-MM-DDTHH:mm:ss.sssZ`. This is a signed historical verified-ready milestone,
  not publication or continuous future UI availability. Viewer owns generation
  and physical verification of the exact complete usable derived-output inventory.
  The coordinator does not infer that inventory from a UI boolean.
* `policy`: closed `{id:"viewer-ready-plus-24h-v1",minimumAgeSeconds:86400}`.
  Minimum starts conservatively at first accepted signed ready intent; no credit
  for a backdated ready timestamp. Only persisted same-owner-epoch monotonic
  elapsed progress counts; restart/downtime/UTC jumps cannot shorten 24 hours.
* `eligibleObjects`: 0–20,000 closed `{id,objectKind,byteSize,sha256}` records,
  sorted/unique by `(objectKind,id)` ASCII. Kind source or derived, ID slug,
  size nonnegative safe integer, digest hex64. Total bytes safe integer.
  Digest is SHA-256 of the canonical list. It must equal all and only persisted
  `release_after_acceptance` source+derived objects. Permanent Viewer holds and
  raw GCP CSVs are excluded, always. No caller paths/storage keys are accepted.

No unknown fields, boolean integers, duplicate identities, changed replay bytes,
cross-owner/environment/generation/receipt/object closures, or unapproved policy.
Snapshot ID is opaque Viewer context tied locally to the snapshot SHA/attempt;
Processing originally attested the snapshot SHA, not the snapshot ID.

Closed status: `{schemaVersion:1,protocol:"ltds-source-release-status-v1",
coordinatorAuthorityId,jobId,generation,releaseId,authorizationSha256,state,
remainingMinimumAgeSeconds,holdReason,receipt}`. State waiting_policy, draining,
deleting, held or released. Remaining seconds is a nonnegative integer ceiling.
Receipt is null until terminal. Hold reason is null or one of: disabled,
release_role_unavailable, qualification_changed, custody_held, binding_changed,
timer_clock_invalid, active_source_io, metadata_quota, source_identity_changed,
source_missing, quarantine_conflict, durability_uncertain, filesystem_unsupported,
journal_invalid, filesystem_unavailable. No paths/tokens/private diagnostics.

Terminal receipt is closed `{schemaVersion:1,protocol:"ltds-source-release-receipt-v1",
coordinatorAuthorityId,jobId,generation,environment,principalId,releaseId,
authorizationSha256,sourceSnapshotId,sourceSnapshotSha256,workerObservationAckSha256,
eligibleClosureSha256,preservedClosureSha256,removedObjectCount,removedBytes,
completedAt,receiptSha256}`. IDs/digests use the authorization types; counts/bytes
are nonnegative safe integers. Completed time is canonical UTC milliseconds.
Receipt digest hashes all other receipt fields canonical. Mint once after exact
durable object removal and required directory fsync/tombstones; exact retries return
stored bytes. Receipt is an authenticated TLS acknowledgement, not a new standalone
signature or generic result-cleanup authority. Viewer must durably verify/store
this exact binding before retiring only its matching operational source hold.

`preservedClosureSha256` hashes the canonical array of every raw-source and
attempt-derived object whose original accepted manifest declares
`retentionClass:"permanent_viewer_hold"`. Use the same closed object records and
ASCII `(objectKind,id)` order as `eligibleObjects`; include raw GCP CSVs. The
empty closure is the canonical array `[]`, never null or an omitted value.
Viewer derives this expected closure from its immutable accepted source/derived
manifests pinned by the authorization and receipts, not a current mutable file
listing or changed retention flag. Before minting the terminal receipt, Processing
must physically reverify this preserved closure as well as prove all authorized
removals. Missing or changed permanent objects hold completion. This is evidence
at release completion, not a claim of immunity to future storage failure.

The receipt's removed count and bytes must exactly equal the signed eligible
closure. Neither the preserved digest nor a ready status permits retiring a
permanent GCP hold, deleting Viewer originals/results, or releasing another attempt.

Vectors: `fixtures/source-release-v1-vectors.json`. Its deterministic private seed
is synthetic test material only. Positive validator evidence is not proof that
the fixture's placeholder readiness digest refers to real verified outputs.
