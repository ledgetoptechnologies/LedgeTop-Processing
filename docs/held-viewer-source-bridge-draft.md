# Held Viewer source bridge: review draft

Status: design only. No endpoint, authorization, transfer, custody or cleanup is enabled by this draft.

The existing Viewer local snapshot and the existing Processing authority-scoped snapshot use different signature domains and statements. Neither signature can be treated as the other. A future versioned bridge must authenticate both independently before it can identify a source selection. It must not lift Viewer source retention merely because a statement is signed or journaled.

## Two separately authenticated statements

1. Viewer retains its existing canonical `ltds-viewer-held-source-snapshot-v1` intent and detached Ed25519 signature under the existing domain and configured local issuer key. The exact intent bytes, signature, source frames, selected frames and, when present, GCP evidence and generated-file frames must remain available for verification.
2. A new, versioned authority-scoped statement uses a distinct signature domain and an independently configured key scoped to coordinator authority, principal and environment. It carries the job, generation, submission, activation, Viewer task and attempt, dataset identity, pipeline and processing-closure bindings already required by `paged_snapshot_journal_candidate.py`.

The new statement additionally binds SHA-256 and byte length of the exact local intent bytes and detached signature, plus the exact selected/source frame and optional GCP evidence/file streams. A verifier checks the digest and length of each byte sequence before checking either signature. The issuer must not choose its own trusted key, principal, environment, authority or processing closure from request data.

## Required reconciliation before authorization

The verifier independently checks both configured signatures and their domains, current authority/principal/environment and validity interval, and exact equality of dataset, task, attempt, frame hashes, frame sizes, counts and input-manifest bindings. It verifies selected records against the retained source records in order, and the processing closure against those selected records and their object hashes. For GCP it verifies the retained raw-source evidence stream, generated `gcp_list` bytes, attempt identity, source references, correspondence evidence, and their selected-frame and processing-closure bindings. Coordinate reference systems and height datum must be validated separately; a byte-valid GCP file is not proof of correct coordinates.

Replay records must bind both statement identities and hashes to the authenticated job, generation and activation in one durable transaction. A changed statement, key scope, frame, generation, principal or authority must fail closed; exact replay must be idempotent. A crash or network loss cannot turn an unverified source into a staged input.

An authenticated, durable acceptance covering the exact transferred objects is a separate later step. Viewer source data and raw GCP evidence remain retained until that acceptance is verified under an explicit retention policy. Processing-copy cleanup must use its own verified receipt and must not delete Viewer originals.

## Decisions still requiring review

- Specify the new protocol version, canonical encoding, exact field names and signature-domain bytes jointly with Viewer; do not silently extend the existing v1 schema.
- Specify issuer key provisioning, rotation and revocation, freshness, persistent replay handling and atomic journal/stream staging.
- Define a separate authorization/custody proof and its failure, recovery and retention semantics. Passing this bridge alone is not such proof.
- Exercise the real Viewer issuer and Processing verifier with synthetic large datasets, GCP data, changed and truncated streams, cross-principal replay, crash recovery and conflicting generations before activation.
