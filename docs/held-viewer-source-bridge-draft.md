# Held Viewer source bridge: review draft

Status: design only. No endpoint, authorization, transfer, custody or cleanup is enabled by this draft.

The existing Viewer local snapshot and the existing Processing authority-scoped snapshot use different signature domains and statements. Neither signature can be treated as the other. A future versioned bridge must authenticate both independently before it can identify a source selection. It must not lift Viewer source retention merely because a statement is signed or journaled.

## Two separately authenticated statements

1. Viewer retains its existing canonical `ltds-viewer-held-source-snapshot-v1` intent and detached Ed25519 signature under the existing domain and configured local issuer key. The exact intent bytes, signature, source frames, selected frames and, when present, GCP evidence and generated-file frames must remain available for verification.
2. A new, versioned authority-scoped statement uses a distinct signature domain and an independently configured key scoped to coordinator authority, principal and environment. It carries the job, generation, submission, activation, Viewer task and attempt, dataset identity, pipeline and processing-closure bindings already required by `paged_snapshot_journal_candidate.py`.

The new statement additionally binds SHA-256 and byte length of the exact local intent bytes and detached signature, plus the exact selected/source frame and optional GCP evidence/file streams. A verifier checks the digest and length of each byte sequence before checking either signature. The issuer must not choose its own trusted key, principal, environment, authority or processing closure from request data.

### Proposed v2 wire fields

For joint review, use `protocol` value `ltds-viewer-paged-source-snapshot-v2` and sign `b'LTDS-VIEWER-PAGED-SOURCE-SNAPSHOT-V2\0' + canonical_unsigned_statement` with the configured scoped Ed25519 key. Preserve the v1 statement fields and semantics, set `schemaVersion` to integer `2`, and add one closed `viewerHeldSource` object with exactly:

```
protocol, snapshotId, issuerId, keyId,
intentSha256, intentByteCount, signatureSha256, signatureByteCount,
sourceFrameStreamSha256, sourceFrameStreamByteCount,
selectedFrameStreamSha256, selectedFrameStreamByteCount,
gcpEvidenceStreamSha256, gcpEvidenceStreamByteCount,
gcpFileStreamSha256, gcpFileStreamByteCount
```

The outer detached signature retains the v1 statement's canonical base64url representation and signs the complete v2 body excluding only `signature`. The local intent and signature travel as exact bytes, separately from the outer statement. `signatureByteCount` must equal 64; intent and stream sizes must be positive integers bounded by the existing frame/intent limits. Each GCP digest/length pair is either both null or both present, and GCP evidence and file pairs are both present exactly when the authenticated local intent declares generated GCP. Digests are lowercase SHA-256 hex of the exact transported bytes, including frame delimiters. The new object links the local evidence but never provisions a key or establishes custody.

The source and selected per-page byte arrays in the outer statement must match the actual canonical frame sizes and the local intent's page hashes, frame totals and counts. Both statements must agree on dataset, task, attempt, input manifest and snapshot ID. The outer `processingClosureSha256` and selected mapping must match the independently verified processing pages, including object IDs, sizes, hashes, order and role. GCP stream/page/content hashes and generated-file metadata are checked against the local statement and the applicable evidence and file validators. All records stay held until a separately reviewed authorization and durable custody protocol succeeds.

## Required reconciliation before authorization

The verifier independently checks both configured signatures and their domains, current authority/principal/environment and validity interval, and exact equality of dataset, task, attempt, frame hashes, frame sizes, counts and input-manifest bindings. It verifies selected records against the retained source records in order, and the processing closure against those selected records and their object hashes. For GCP it verifies correspondence evidence and generated `gcp_list` bytes, attempt identity, raw-source references, and their selected-frame and processing-closure bindings. The current correspondence stream is not the raw GCP CSV: if Processing claims custody of the CSV, the protocol must provide a separate bounded raw-source stream bound to its dataset-file ID, exact byte count, content digest and frame hashes. Otherwise the receipt must explicitly describe the CSV as Viewer-only and cannot authorize its release. Coordinate reference systems and height datum must be validated separately; a byte-valid GCP file is not proof of correct coordinates.

Replay records must bind both statement identities and hashes to the authenticated job, generation and activation in one durable transaction. A changed statement, key scope, frame, generation, principal or authority must fail closed; exact replay must be idempotent. A crash or network loss cannot turn an unverified source into a staged input.

An authenticated, durable acceptance covering the exact transferred objects is a separate later step. Viewer source data and raw GCP evidence remain retained until that acceptance is verified under an explicit retention policy. Processing-copy cleanup must use its own verified receipt and must not delete Viewer originals.

## Decisions still requiring review

- Review the proposed version, canonical encoding, exact field names and signature-domain bytes jointly with Viewer and Claude; do not silently extend the existing v1 schema.
- Specify issuer key provisioning, rotation and revocation, freshness, persistent replay handling and atomic journal/stream staging.
- Define a separate authorization/custody proof and its failure, recovery and retention semantics. Passing this bridge alone is not such proof.
- Exercise the real Viewer issuer and Processing verifier with synthetic large datasets, GCP data, changed and truncated streams, cross-principal replay, crash recovery and conflicting generations before activation.
