# Reference source intake v1 (candidate)

**Status: proposed cross-system wire contract; not implemented or enabled.** It
defines how Viewer can transfer a full retained dataset to a Processing NAS
spool, while the engine input manifest remains a separately validated subset.
It does not establish TrueNAS mount identity, protect against NAS administrators
or other app writers, or prove that an external purge cannot remove a file.
Those physical storage controls must be implemented and qualified before a
source receipt can be treated as production custody.

## Identities and manifests

The Viewer has three different IDs and must not conflate them:

- `task_submissions.submission_id` is the caller's user-facing idempotency key.
  It stays inside Viewer and is not the Processing attempt identity.
- `processing_attempts.id` is the stable attempt UUID for this Processing
  attempt. It is sent as wire `submissionId` and is Processing's idempotency
  key. An exact retry of the same attempt uses the same UUID.
- `processing_jobs.id` is Viewer’s internal work-row ID. It stays internal and
  is never replaced by a Processing ID.
- `processing_attempts.providerTaskId` is the Viewer-preassigned NodeODM UUID.
  It is sent as `providerTaskId`; Processing adopts it as `jobId` and uses that
  exact value when initializing NodeODM.

Processing returns `attemptId`, `jobId`/`providerTaskId`, `generation`,
`environment`, `pipelineManifestSha256`, `sourceManifestSha256`,
`derivedManifestSha256`, and `pipelineDigest`. Viewer persists this immutable
crosswalk on the attempt.

The request contains three manifests:

1. `sourceManifest` is a closed object with exactly `schemaVersion`, `id`,
   `datasetManifestSha256`, `sha256`, `totalBytes`, `objectCount`, and
   `objects`. `id` is Viewer `datasets.id`; `datasetManifestSha256` is its
   already-persisted `datasets.manifest_sha256`. Viewer has no dataset revision
   column, so the protocol does not invent one. It is the complete raw Viewer
   dataset closure to retain. Each object is closed with
   exactly `id`, `name`, `byteSize`, `sha256`, `sourceType`, `retentionClass`,
   and `retentionHoldId`; that key is always present and is `null` when no hold
   applies. Each `objects[].id` is exactly one Viewer
   `dataset_files.id`. `name` is the original leaf filename, may contain
   Unicode, and is descriptive only; the spool uses opaque generated storage
   keys and never uses this value as a path. IDs must be unique, sorted by
   ASCII byte order, and match the complete selected raw dataset:
   no missing, extra, or duplicate dataset file is accepted. Every source
   object records its original name, byte size, SHA-256, `sourceType`,
   `retentionClass`, and optional `retentionHoldId`.
2. `derivedManifest` is closed with exactly `schemaVersion`, `attemptId`,
   `sha256`, and `objects`. With no frozen GCP snapshot, `objects` is empty.
   When `processing_attempt_gcp_snapshots` exists for the attempt, it contains
   exactly one generated `gcp_list.txt` object. Its stable ID is
   `gcp_{attemptId}` and its bytes are the exact UTF-8 `content_text`, with
   exact byte size and SHA-256. The object binds `snapshotAttemptId`,
   `snapshotContentSha256`, and `snapshotEvidenceSha256`; the latter is SHA-256
   of the exact stored UTF-8 `evidence_json` bytes. It uses
   `sourceType: attempt_derived_gcp_list`,
   `retentionClass: release_after_acceptance`, and `retentionHoldId: null`. It
   is not a `dataset_files.id` and never appears in the raw source closure.
3. The existing `manifest` is the input metadata sent to the selected
   processing pipeline. Each pipeline object ID must map to exactly one raw
   source or derived object with the same name, byte size, and SHA-256. GCP
   source CSVs may be retained in `sourceManifest` without being sent to
   NodeODM; the derived `gcp_list.txt` is sent by its attempt-scoped ID. TreeDar
   inputs remain explicitly selected LAS/LAZ objects.

Allowed `sourceType` values are `raw_input` and `gcp_source_csv`. Allowed
`retentionClass` values are `release_after_acceptance` and
`permanent_viewer_hold`. A `gcp_source_csv` must be a CSV and set
`retentionHoldId` to the exact `dataset_files.id`. Before constructing this
manifest, Viewer verifies a matching immutable `retained_dataset_files` row:
`dataset_file_id` equals that ID, and its `dataset_id`, `sha256`, `byte_size`,
and `retention_reason` (`gcp_source`) match the dataset file and manifest
object. `retained_gcp_sources.id` identifies a separate retained content copy;
it is not the raw file hold ID. Other objects may be marked permanent only
when Viewer has an equally explicit matching hold record; in this version
`retentionHoldId` must be that exact held `dataset_files.id`. The Processing
release protocol must exclude every `permanent_viewer_hold` object regardless
of success, failure, cancellation, elapsed time, or any result-acceptance ACK.
Viewer retains the permanent hold and never clears it on a Processing response.

### Canonical hashes

All JSON digests use the Processing v1 canonical JSON rule: UTF-8 without BOM,
Unicode emitted literally, object keys sorted lexicographically, compact
`,` and `:` separators, arrays preserved in their specified order, no NaN or
Infinity, and no floating-point values in source manifests or receipts.

- `pipelineManifestSha256` is SHA-256 of the canonical pipeline `manifest`
  object with only its `sha256` field omitted, matching
  `processing.reference_protocol.manifest_sha256`.
- `sourceManifestSha256` is SHA-256 of the canonical `sourceManifest` object
  with only its `sha256` field omitted. `schemaVersion` is integer `1`; `id`
  and `datasetManifestSha256` bind immutable Viewer dataset identity and its
  persisted `datasets.manifest_sha256`;
  `objectCount` and `totalBytes` exactly equal the full object closure. Its
  object list is sorted by ASCII `id` bytes before hashing.
- `derivedManifestSha256` is SHA-256 of the canonical `derivedManifest` object
  with only its `sha256` field omitted. `attemptId` must equal
  `processing_attempts.id`; derived objects are sorted by ASCII `id` bytes.
  The no-GCP case uses the same closed object with `objects: []`.
- `pipelineDigest` is SHA-256 of the canonical `pipeline` object, including
  the ODM options, TreeDar selection/provenance reference, and
  `gaussian_splat.requested: false`.
- Each object SHA-256 is over its exact raw bytes.
- `sourceReceiptSha256` is SHA-256 of the canonical raw-source receipt object.
- `derivedReceiptSha256` is SHA-256 of the canonical derived receipt object;
  generated inputs intentionally do not enter the raw source receipt.

Before sending a source manifest, Viewer reads the protected
`GET /ltds/v2/capabilities` response. It includes a `sourceIntake` object with
`enabled`, `maxObjects`, `maxManifestBytes`, `maxObjectBytes`,
`maxSourceBytes`, and `chunkBytes`. `maxSourceBytes` is a current admission
ceiling, not a promise: POST reserves the full closure against live free bytes,
other reservations, and the configured safety floor. The source manifest must
fit the advertised limits and the fixed 4 MiB metadata body bound. The server
rechecks every limit at metadata admission and before reading each PUT body.

## Create or replay an attempt

The Viewer sends a v2 envelope; the environment comes only from the
environment-scoped Viewer credential, not from the request body:

```http
POST /ltds/v2/jobs
Authorization: Bearer <production-or-staging Viewer credential>
Content-Type: application/json
```

The closed body contains `schemaVersion: 2`, `submissionId` (the
`processing_attempts.id`), `providerTaskId`, `sourceManifest`,
`derivedManifest`, the existing pipeline `manifest`, `pipeline`, and
`capabilityFingerprint`. The accepted immutable identity is attempt ID,
provider task ID, trusted environment, source, derived and pipeline manifest
digests, and pipeline digest. Exact replay returns the same `jobId` and
generation. Changed bytes or fields under the same attempt return `409`. The
caller's `task_submissions.submission_id` does not participate in this
identity.

Metadata acceptance is **not** source acceptance. Until finalization the
response has `status: "queued"`, `phase: "source_receiving"`,
`sourceState: "receiving"`, `derivedState: "receiving"`, and both receipts
null. It includes and binds `sourceManifestSha256`, `derivedManifestSha256`,
and `pipelineManifestSha256`. A worker cannot discover or claim that job.
Viewer must persist the immutable response fields before retrying uploads, but
must not advance the attempt to `processing`.

## Stream each full-source object

The Viewer streams one object at a time using the exact ID and bytes in either
`sourceManifest` or `derivedManifest`. The same route accepts this closed union;
the server resolves each ID against both immutable manifests and rejects an
ID missing from or duplicated across them:

```http
PUT /ltds/v2/jobs/{jobId}/source/{objectId}?generation={generation}
Authorization: Bearer <same environment-scoped Viewer credential>
Content-Length: <exact byteSize from the matching manifest>
Content-SHA256: <lowercase SHA-256 from the matching manifest>
Content-Type: application/octet-stream

<raw bytes>
```

The server streams bounded chunks directly to a private temporary file on the
configured storage mount, verifies exact byte count and digest, fsyncs the
file, atomically publishes it under an opaque job/generation/object key, fsyncs
the containing directory, then durably records the object receipt before
returning success. It does not buffer whole photos or archives in memory and
does not use user filenames as paths. `201` means newly stored; `200` means an
exact verified replay. A different body under the same immutable object
identity returns `409`; an unknown object returns `404`.

Content-Length is required and must match the manifest. Transfer-Encoding,
redirects, duplicate identity headers, stale generation, short/extra body,
content hash mismatch, and an object outside either manifest closure are
rejected. Per-object and combined-source limits plus a real free-space
reservation and safety floor are checked before accepting bytes. Capacity
refusal returns `507`; a missing or changed mount returns `503`. Job status
exposes separate source and derived per-object durable ACKs bound to the
corresponding manifest digest and generation. It exposes the immutable derived
receipt and digest after finalization, including the empty-object receipt when
there is no GCP snapshot. Any missing ACK leaves dispatch prohibited.

## Finalize and durably accept source

After every raw-source and derived object has a durable PUT ACK, Viewer
requests closure validation:

```http
POST /ltds/v2/jobs/{jobId}/source/complete
Authorization: Bearer <same environment-scoped Viewer credential>
Content-Type: application/json

{"generation": 1, "sourceManifestSha256": "<full raw source closure digest>",
 "derivedManifestSha256": "<attempt-derived closure digest>",
 "pipelineManifestSha256": "<engine input manifest digest>"}
```

Processing reopens and verifies all raw and derived objects, the exact Viewer
dataset file-ID closure, attempt GCP snapshot provenance, retention
classes/holds, and the pipeline-manifest subset. It then durably commits
separate raw-source and derived receipts and binds both to the queued job. Only
after every file and both receipt bindings are durable can it return `201` for
the first completion or `200` for an exact replay. Status and replay return the
same manifest crosswalk and both receipts:

```json
{
  "jobId": "<Processing/NodeODM UUID>",
  "attemptId": "<Viewer processing_attempts.id>",
  "providerTaskId": "<same NodeODM UUID>",
  "generation": 1,
  "environment": "production",
  "pipelineManifestSha256": "<engine input manifest digest>",
  "sourceManifestSha256": "<full retained source digest>",
  "derivedManifestSha256": "<attempt-derived closure digest>",
  "pipelineDigest": "<pipeline digest>",
  "status": "queued",
  "phase": "queued",
  "sourceState": "retained",
  "derivedState": "retained",
  "sourceReceipt": {
    "schemaVersion": 2,
    "receiptType": "source_retention",
    "receiptId": "<receipt ID>",
    "storageReceiptId": "<storage receipt ID>",
    "jobId": "<Processing UUID>",
    "attemptId": "<Viewer attempt UUID>",
    "generation": 1,
    "environment": "production",
    "pipelineManifestSha256": "<engine input manifest digest>",
    "sourceManifestSha256": "<full retained source digest>",
    "pipelineDigest": "<pipeline digest>",
    "objects": [{
      "id": "<dataset_files.id>",
      "byteSize": 123,
      "sha256": "<raw object digest>",
      "sourceType": "raw_input",
      "retentionClass": "release_after_acceptance",
      "retentionHoldId": null
    }],
    "retainedAt": "<UTC milliseconds>",
    "retentionGuarantee": "until_explicit_source_release_v1"
  },
  "sourceReceiptSha256": "<canonical raw-source receipt digest>",
  "derivedReceipt": {
    "schemaVersion": 1,
    "receiptType": "attempt_derived_input",
    "receiptId": "<receipt ID>",
    "jobId": "<Processing UUID>",
    "attemptId": "<Viewer attempt UUID>",
    "generation": 1,
    "environment": "production",
    "derivedManifestSha256": "<attempt-derived closure digest>",
    "objects": [{
      "id": "gcp_<attempt UUID>",
      "name": "gcp_list.txt",
      "byteSize": 123,
      "sha256": "<generated bytes digest>",
      "snapshotAttemptId": "<same Viewer attempt UUID>",
      "snapshotContentSha256": "<GCP content digest>",
      "snapshotEvidenceSha256": "<stored evidence_json digest>",
      "sourceType": "attempt_derived_gcp_list",
      "retentionHoldId": null,
      "retentionClass": "release_after_acceptance"
    }],
    "retainedAt": "<UTC milliseconds>"
  },
  "derivedReceiptSha256": "<canonical derived receipt digest>"
}
```

When `derivedManifest.objects` is empty, `derivedReceipt` is still mandatory.
It has the same closed receipt fields and bindings shown above (`jobId`,
`attemptId`, `generation`, `environment`, and `derivedManifestSha256`), but its
`objects` value is exactly `[]`. It contains
no fabricated GCP object or snapshot IDs. `derivedReceiptSha256` hashes this
complete empty-closure receipt, making acceptance of the zero-object manifest
durable and replay-verifiable.

The raw `sourceReceipt` inventory is the complete `sourceManifest` closure,
sorted by ID, and every object repeats all seven source fields. It includes
every permanent hold. The `derivedReceipt` separately binds generated
attempt-scoped inputs to the immutable GCP snapshot; its objects are not
`dataset_files` records and do not enter the raw receipt. Viewer stores both
receipts atomically with the attempt crosswalk, recomputes each canonical
digest, and verifies IDs, hashes, sizes, classes, hold IDs, attempt/job/provider
IDs, generation, environment, and all three manifest digests. A changed
receipt on replay is a conflict. If response delivery is lost, Viewer GETs
attempt/job status and validates the same receipts; it does not resubmit
metadata as a new attempt.

## Node reservation and source broker

After both receipts are bound, the job remains in the single production /
staging queue. The queue head is dispatched only to the first available
configured node whose refreshed photo, byte, engine, and TreeDar capabilities
admit the pipeline. Later jobs do not overtake the queue head. After claim, the
worker makes a fresh scratch reservation, then polls
the existing claim-bound `GET /ltds/worker/v1/jobs/{jobId}/transfer-grant`.
In NAS mode Processing mints and durably stores the short-lived grant only
after verifying both retained raw-source and derived receipts and the worker
reservation. The
existing closed grant fields are `reservationId`, `generation`, `expiresAt`,
`manifestSha256`, `byteBudget`, `nonce`, `jobId`, `environment`, `objectIds`,
and opaque `token`.

Worker downloads use its fixed private Processing broker origin:

```http
GET /api/processing/reference/v1/objects/{objectId}
Authorization: Bearer <opaque job-scoped source grant token>
Accept-Encoding: identity
```

The broker streams one exact pipeline-input object from the union of the raw
source and derived receipts only if the job generation,
environment, reservation, token, and object manifest binding match. It returns
`200`, exact `Content-Length`, `application/octet-stream`, and `no-store`; it
rejects redirects, compression, ranges, expired grants, IDs outside the
pipeline closure, and inputs outside the accepted receipts. The worker verifies
full size/hash in private scratch before engine upload. Only confirmed engine
start changes Viewer state to `processing`. The scheduler first selects the
earliest production job; staging is considered only when no production job is
queued. For that queue head, it scans nodes in configured order and chooses the
first currently available node whose discovered limits and engine capabilities
admit the job. If none can admit it, it stays queued and later jobs do not
overtake it. This sends small jobs to the cluster node first and keeps an
oversized job waiting for a capable larger node.

## Recovery, holds, and release

Viewer polls the attempt/job status after timeout, `503`, restart, or lost
response. Status is always bound to the persisted attempt and exact generation:

- `sourceState: receiving` or `derivedState: receiving`: upload may continue;
  query each manifest's per-object receipts and replay only exact
  missing/unacknowledged objects.
- `sourceState: reconciling`: restart is verifying final files; do not dispatch.
- `sourceState: retained`: a validated complete source receipt is available;
  queue waiting still remains `queued`.
- `sourceState: held`: source, mount, receipt, or file identity mismatch needs
  operator review; do not retry with altered bytes or run work.

On startup, Processing verifies the configured mount identity before serving.
It reconciles object files against the immutable manifests and persisted
receipt. A final file without its DB object ACK may be adopted only after full
length/hash verification. A receipt without every exact file, a changed mount,
or any extra/missing/duplicate dataset ID is held; it is never repaired by
trusting a filename or silently replacing bytes. Every non-success finalize
keeps dispatch prohibited. A lost completion response is resolved by exact
status/receipt lookup and replay, not by changing generation.

This version does not delete source files. Future release is an authenticated,
durable, idempotent operation requiring Viewer result acceptance and its
configured retention window. It must exclude every `permanent_viewer_hold`
object even if the job succeeds, fails, is cancelled, or expires. Viewer
permanent holds remain authoritative. No expiry, disconnect, restart,
cancellation, or failed upload authorizes deletion.

## Implementation gates

Current Processing v1 metadata validation does not accept `sourceManifest`, its
HTTP server buffers request bodies under the metadata limit, and there is no
streaming PUT, source-complete route, durable source spool, source receipt
claim gate, or NAS-backed worker broker. The current Viewer has no immutable
attempt crosswalk fields for Processing generation, environment, digests,
source state, or accepted source receipt. Processing now has a pure
`reference_source_intake` validator and shared canonical hash vectors, but they
are not wired into its API or storage. Neither side is ready to execute the
contract. Required synthetic
coverage includes idempotent/lost-response replay, changed-body conflict,
manifest/crosswalk closure, permanent GCP CSV exclusion, disk-full and
mount-identity changes, crashes before/after file fsync/rename/DB commit,
restart receipt reconciliation, and refusal to dispatch until exact receipt
binding is durable.
