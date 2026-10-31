# Proposed Viewer result identity v2 — review draft

This is a proposed contract, not an implemented endpoint, authorization policy, rollout, receipt of Viewer custody, or permission to delete data. Existing closed v1 request and response types remain byte-for-byte unchanged. Both owners must approve exact fields, canonicalization, indexes and failure semantics before implementation. Legacy jobs lacking authoritative canonical input/pipeline identity remain on their existing path and are **ineligible** for this commitment; do not fabricate fields.

## Authority and scope

Processing authenticates a distinct provisioned Viewer service principal over server-authenticated TLS. An explicit authorization policy binds that principal to environment, coordinator authority/audience and eligible job/generation. The bearer is sent only in the Authorization header, is never written in a commitment, URL or logs, and is checked afresh for every request including replay. A shared production/staging token that identifies only an environment is insufficient. Provisioning, rotation and how physical fences are established are prerequisites, not features implemented here.

Processing job ID/generation and coordinator authority are Processing identities. Viewer task ID and attempt ID are authenticated **claims made by Viewer**; Processing cannot infer their membership from the service credential. Viewer validates their durable mapping to the Processing job and generation against its own authoritative database before registration and after every response. Neither a commitment nor an authenticated claim proves Viewer acceptance or storage. Server-authenticated TLS protects the exchange; this proposal does not claim a portable signed proof.

The proposed separate, opt-in routes are `GET /ltds/v2/capabilities`, `PUT /ltds/v2/viewer-bindings/{bindingId}`, `GET /ltds/v2/viewer-bindings/{bindingId}`, and `GET /ltds/v2/viewer-bindings/{bindingId}/commitment`. Capabilities explicitly names this v2 and its serializer. A missing capability/route means use existing v1 behavior only, with **no** claim of this stronger binding. Routes, media type and client fallback require Viewer sign-off. The path UUID and body `bindingId` must match. No v1 fields are added or reinterpreted.

## Canonical bytes and hashes (proposed)

Binding object is closed to exactly `schemaVersion`, `bindingId`, `principal`, `authorityId`, `environment`, `processingJobId`, `processingGeneration`, `viewerTaskId`, `viewerAttemptId`. `schemaVersion` is integer 2; environment is exactly `production` or `staging`; UUIDs use lowercase canonical hyphenated spelling; generation is an integer 1..9007199254740991. `principal` is a provisioned case-sensitive ASCII slug `[A-Za-z0-9][A-Za-z0-9._-]{0,63}`. Processing job IDs in this proposed v2 are UUIDs; non-UUID legacy IDs are ineligible until a separately reviewed type is agreed. Principal and authority come from the authenticated server context and must exactly match the body, not be trusted from it alone.

Commitment object is closed to exactly `schemaVersion`, `bindingSha256`, `archiveSha256`, `archiveBytes`, `receiptSha256`, `inputManifestSha256`, `pipelineSha256`, `resultManifestSha256`. SHA-256 fields are 64 lowercase hex characters; `archiveBytes` is an integer 22..9007199254740991. `resultManifestSha256` is null only when no canonical producer result manifest exists; it must never be an invented manifest. `receiptSha256` is SHA-256 of the **exact retained receipt file bytes**, not of reparsed or reformatted JSON. Its format/version, input-manifest canonicalization and pipeline canonicalization must each be identified by the negotiated v2 profile before a real commitment is eligible. The JSON object does not carry secret material.

For these flat schemas only, canonical bytes are UTF-8 JSON with lexicographically sorted ASCII keys, no insignificant whitespace, integer decimal without exponent or leading zeros, JSON null, and ASCII string values with JSON escaping as required. Reject duplicate keys at parse time, unknown/missing fields, non-ASCII/control characters in values, booleans in integer fields, floating values, negative zero, out-of-range values and alternate UUID/hash spellings. Implementations must compare their output byte-for-byte on the vectors below; never hash arbitrary incoming JSON bytes. Binding digest is `SHA256(ASCII("ledgetop.viewer.binding.v2") || 0x00 || canonicalBinding)`; commitment digest uses `ASCII("ledgetop.viewer.commitment.v2") || 0x00 || canonicalCommitment`. `0x00` is one binary NUL byte.

Binding vector, exact canonical line (no trailing newline):

```json
{"authorityId":"22222222-2222-4222-8222-222222222222","bindingId":"11111111-1111-4111-8111-111111111111","environment":"staging","principal":"viewer-1","processingGeneration":1,"processingJobId":"33333333-3333-4333-8333-333333333333","schemaVersion":2,"viewerAttemptId":"55555555-5555-4555-8555-555555555555","viewerTaskId":"44444444-4444-4444-8444-444444444444"}
```

Binding digest: `62c5d5e1a1cbeaabcfa72cae66f7aab11d669a1e253e20e65fb432d51a818de5`.

Commitment vector uses illustrative repeated-digit digests, not a real archive or receipt; exact canonical line (no trailing newline):

```json
{"archiveBytes":22,"archiveSha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","bindingSha256":"62c5d5e1a1cbeaabcfa72cae66f7aab11d669a1e253e20e65fb432d51a818de5","inputManifestSha256":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","pipelineSha256":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee","receiptSha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","resultManifestSha256":null,"schemaVersion":2}
```

Commitment digest: `0f8765e5fae5c30705f5fdc23554a4227716046d25f29b0468bbe99399ee9126`.

Negative vectors: adding a duplicate `bindingId`, changing generation to `1.0` or `true`, uppercase hex/UUID, adding `token`, or replacing `null` with an invented manifest digest must fail validation or eligibility. Whitespace/key order differences in otherwise valid request JSON canonicalize to the same binding; a field change does not. Receipt hashing is over the exact file bytes and therefore does not normalize whitespace.

## Durable transition and exclusion requirements

A first authenticated PUT creates a **provisional** immutable binding transactionally. Global uniqueness is scoped by `(authorityId, environment, bindingId)`, with immutable principal and audience; reverse conflict keys cover `(authorityId, environment, viewerTaskId, viewerAttemptId)` and `(authorityId, environment, processingJobId, processingGeneration)`. Exact treatment of intentional multiple consumers requires a future explicitly versioned policy, not a silent exception. Durable tombstones retain these keys after any revocation or lifecycle change; they prevent reuse and cross-principal rebinding. A same-principal identical retry after fresh authentication returns the same binding; changed bytes, changed principal, conflicting reverse keys or stale generation never create a second binding. No cross-principal response may expose whether another principal owns an identifier.

A commitment may be recorded only after a stable retained archive and exact receipt bytes are read and hashed while the actual physical single-writer fence and the relevant file/receipt exclusion remain continuously held. The same barrier must cover durable commit and task/generation/hold/authority validation; a fresh database recheck after dropping the physical fence is insufficient. Any lock loss, uncertain writer, missing receipt, changed inode/content, invalid source or unqualified storage holds without commitment. The file hash is self-computed evidence of the retained bytes, not source authentication or Viewer custody. Fencing old binaries, mount aliases, host/Docker administrators and recovery from fence loss are not solved by this document.

Provisional and committed records persist across restart. Once committed, exact replay returns identical canonical fields and digest; any different archive, receipt, input, pipeline, authority or generation is a conflict requiring inspection. Revocation/expiry policies restrict **new authorized requests**; they do not rewrite an existing proof or retract transmitted bytes. No expiry duration, clock-skew tolerance, freshness window or automatic revocation policy is selected here. Freshness/authentication must be specified separately before use.

## Failure and response proposal

| Condition | Proposed external result | Required durable effect |
| --- | --- | --- |
| Missing/invalid/revoked credential | 401, fixed sanitized error | No write or secret echo. |
| Authenticated but unavailable/not authorized identifier, including another principal's | 404, same fixed error | No existence/ownership disclosure. |
| Valid principal, malformed closed schema | 400, fixed sanitized error | No write. |
| Same authorized principal, exact PUT replay | 200, identical provisional/committed record | No second binding. |
| Fresh valid binding | 201, provisional record | Commit once under unique keys. |
| Authorized changed payload, reverse-key conflict, stale generation or changed authority | 409, generic conflict | Preserve prior binding/tombstone; no new result. Global collision may be masked as 404 when ownership cannot be disclosed. |
| Provisional job has no qualified retained result, missing canonical input/pipeline or unknown manifest provenance | 409, fixed `identity_not_ready` or `identity_ineligible` | No commitment; legacy processing remains available. |
| Hold, physical fence loss, unavailable storage, ambiguous prior write or failed revalidation | 503 or 409 per reviewed transient/inspection taxonomy | Preserve all artifacts and intent; never issue or revise a commitment. |
| Commit response lost after durable write | Client performs freshly authenticated GET; exact immutable record returns | No duplicate, no silent replacement. |

The route must use bounded request/response bodies and sanitized errors. A status code is not evidence that an in-flight stream was revoked. An archive download must separately bind and verify the exact bytes and generation consumed; a commitment lookup followed by an unfenced unbound download is insufficient. No acceptance endpoint, Viewer ingestion, queue completion, deletion or cleanup is authorized here.

## Required review and tests

Before enabling: cross-language canonical vectors and negative encodings; real distinct-principal/environment/audience authorization; Viewer-owned database membership validation; both directions of uniqueness and durable tombstones; identical replay/lost response/crash at each commit boundary; stale generation, changed authority and revocation; concurrent binding/hold/cancel races; independent source/receipt/file hash verification under a continuously held fence; no publication if physical exclusion is unavailable; secret-free logs; and migration/backup/rollback of nonempty state. Tests must distinguish no result manifest from an actual canonical one and demonstrate legacy v1 remains unchanged. Current PR187 is not eligible and no data or route is modified by this proposal.
