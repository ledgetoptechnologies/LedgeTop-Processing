# Proposed Viewer-signed accepted-copy v2 — unagreed and disabled

This is a review proposal and synthetic test vector, not an enabled API,
production authorization, cleanup rule, or evidence that Viewer implements
v2. It requires agreement by Viewer and Processing owners. It preserves the
existing v1 receipt as a separate protocol; a v1 receipt cannot be upgraded by
adding unsigned fields, and a v2 signature must never verify as v1.

## Current evidence and gap

Viewer's `referenceReceipt.js` signs `ltds-viewer-accepted-copy-v1` using
Ed25519 over `LTDS-VIEWER-ACCEPTED-COPY-ACK-V1\0` plus canonical body bytes.
Its job/generation/environment, manifest and object bindings do not include a
coordinator authority, authenticated Viewer principal, prepared submission,
activation, Viewer task/attempt or prepared envelope digest. Processing's
unmounted prepared-v3 `POST /ltds/prepared/v3/submissions/{submissionId}/accepted-copies`
currently requires `{acceptance,viewerReceipt}`, authenticates bearer-derived
principal/environment, checks `X-LTDS-Expected-Coordinator-Authority`, and
returns `202 verified_held` after v1 verification when its hard prerequisites
are overridden. It does not make durable final acceptance or authorize cleanup.
The proposed change below is not yet implemented by either endpoint.

## Candidate wire shape

Keep that endpoint and outer closed object `{acceptance,viewerReceipt}`;
select v2 only through an explicitly negotiated server-owned policy, durably
pinned to the prepared submission before acceptance. A v2-bound submission
must never silently downgrade or fall back to v1 on error. The
Viewer signs the following closed `viewerReceipt` object (the unsigned fields
are exactly the v1 fields except protocol/schema version, plus those listed):

* `protocol`: literal `ltds-viewer-accepted-copy-v2`; `schemaVersion`: integer 2.
* Existing v1 `issuerId`, `keyId`, `receiptId`, `storageReceiptId`, `jobId`,
  `generation`, `environment`, `inputManifestSha256`, `pipelineDigest`,
  `resultManifestSha256`, `acceptedObjectIds`, `objects`, and `acceptedAt`.
* `coordinatorAuthorityId`, `viewerPrincipalId`: safe ASCII IDs from the
  authenticated, server-owned binding. The principal identifies the Viewer
  acceptance service, never a human. Viewer derives the authority from an
  independently authenticated Processing peer, not request JSON.
* `submissionId`, `activationId`, `viewerTaskId`, `viewerAttemptId`: lowercase
  canonical UUIDs bound to the persisted prepared envelope/activation.
* `preparedEnvelopeSha256`: the exact v3 protocol envelope digest, not an
  arbitrary raw-body hash; compare to stored prepare/activation identity.
* `acceptanceReceiptId`, `acceptanceStorageReceiptId`: exact outer acceptance IDs.
* `acceptancePayloadSha256`: lowercase SHA-256 of `ASCII("LTDS-OUTER-ACCEPTANCE-V1") ||
  0x00 || canonical(acceptance)`, binding all outer fields including IDs,
  validation version, timestamp, scope and ordered accepted-object IDs. The
  outer acceptance must be closed, duplicate-free and restricted to the same
  ASCII/integer canonical profile before computing this digest.
* `signature`: canonical unpadded base64url encoding of 64 Ed25519 bytes.

Receipt IDs and identifiers use the existing bounded v1 ASCII identifier
grammar; `generation` is 1..2^53-1, byte sizes are 0..2^53-1, hashes lowercase
64 hex, timestamps valid UTC millisecond `YYYY-MM-DDTHH:mm:ss.sssZ`.
All strings in this closed receipt are ASCII. Require unique `objects` and
`acceptedObjectIds` in the exact same order as the outer acceptance and the
applicable trusted result-manifest closure. Never silently sort a different
outer order: the current v1 signer sorts IDs while schema-v2 acceptance can
require manifest order. For partial versus full TreeDar results, the exact
required closure/order must be agreed from trusted result policy before
release; this proposal defaults to the complete designated closure, not a
caller-selected subset. Require exact size/hash/role mapping through the trusted persisted
result manifest. Receipt object records remain `{id,byteSize,sha256}` as in
v1; role is checked through the pinned result manifest digest. Do not allow
duplicate keys in raw JSON, unexpected properties, duplicate IDs, booleans as
integers, noncanonical UUIDs/signatures, nonfinite or fractional numbers, or
ambiguous Unicode key/identifier handling. These restrictions are candidate
rules and need agreement, particularly for future result types.

Use Ed25519 sign/verify on `ASCII("LTDS-VIEWER-ACCEPTED-COPY-ACK-V2") ||
0x00 || canonical(receipt_without_signature)`. Canonical JSON uses recursively
lexicographically sorted ASCII keys, compact separators, UTF-8, unescaped
ASCII-safe strings and decimal integers only; arrays retain order. The closed
ASCII/integer profile intentionally makes Viewer's existing v1 canonicalizer
and Processing's `reference_protocol.canonical_json` agree for this proposal;
it is **not** a claim that their general Unicode/number serialization is RFC
8785. The prepared envelope remains separately checked using its actual v3
canonicalization and domain. Compare bytes, not only parsed object equality.
Vectors under `tests/fixtures/accepted_copy_v2_proposal.json` use a published
test key only and must never be used for any real identity.

## Scope, evidence and replay

For the proposed deployed HTTP path, Processing must terminate TLS with
hostname verification and an independently pinned server CA; Viewer presents
its scoped client certificate and bearer credential. Processing verifies the
client certificate against a pinned client CA and leaf-identity registry,
maps the certificate AND bearer to the same single Viewer principal and
environment, and rejects mismatch or ambiguity before looking up a job. The
bearer/certificate mapping, server authority pin, issuer and Ed25519 public
key must be installed as server-owned policy, never accepted from the request.
The existing candidate prepared handler only consumes protected-transport
evidence and bearer; mTLS wiring is a separate disabled candidate, not an
existing deployment. Distinct staging/production credentials and keys are
required by this proposal; concrete certificate/key rotation overlap and
revocation procedures remain to be agreed and tested.

The Viewer **acceptance service**, after verifying and durably recording its
copy, owns or invokes the Viewer Ed25519 signing private key via a narrowly
scoped signer; Processing receives only its public key and must never sign a
receipt on Viewer's behalf. The certificate's client key, bearer and receipt
signing key are separate secrets/roles. Viewer must independently authenticate
Processing for the result-copy request and bind that caller to the exact
expected authority/job; the reverse-direction mTLS/bearer contract and storage
durability semantics are not yet agreed. An issuer string, HTTP 200/202, or a
valid signature without that trusted scope is not proof of identity/custody.

Processing derives principal/environment from its authenticated credential
registry (with proposed transport identity binding) and the authority from its
own durable queue and required request header. Resolve the submission by that
scope. Load expected activation, task, attempt, envelope digest, job,
generation, input and pipeline digests from its durable prepared records;
load the expected result and complete object closure from independently
verified durable result state. Key IDs map to a server-owned pinned Viewer
public key and issuer, scoped to the principal/environment; reject a key or
issuer reused across identities unless a separately reviewed rotation policy
explicitly permits it. Check all these values against the signed body; never
construct expected values from the receipt. Reject duplicate keys from the raw
HTTP bytes at every ingress accepting or signing the acceptance, before
ordinary JSON parsing can collapse them; authenticate the transport separately
from JSON interpretation. Then compare the
outer acceptance digest, IDs and binding/closure against signed values and
trusted state. Require four distinct
receipt IDs if retaining the current v3 envelope's namespace rule, and require
the same identity across both receipts and all referenced manifests.

Viewer must ACK only after exact closure and file/directory fsync under an
agreed persistent-mount and underlying filesystem/ZFS durability boundary;
mount identity, filesystem guarantees and user-controlled TrueNAS topology
are not established by a signature. After those barriers and verified copy, Viewer must
persist the exact receipt bytes, stable receipt IDs and verified copied-object
inventory in one database transaction before returning/sending it. Database
and filesystem writes are not assumed to be jointly atomic; crash recovery
must reconcile their boundary. A retry
replays the same bytes, not a fresh signature/ID/time. Processing journals the
exact canonical payload and bound authority/principal/submission before
acknowledging. Exact replay returns the original durable outcome; changed bytes,
same receipt/storage IDs in a different scope, different binding, or conflicting
acceptance is a conflict/hold, with no replacement. Lost response or uncertain
commit is resolved by authenticated scoped receipt lookup, not by making a new
receipt. Namespace ownership must be protected against older/unconfigured
writers and concurrent installation before any acceptance is released.

`202 verified_held` is only verification/journaling. A future explicit
durable Viewer-copy acceptance receipt proves only the bound copies, under a
defined storage durability policy. Coordinator metadata receipt proves only
coordinator persistence. Neither alone proves Viewer import/publication,
node termination, worker deletion safety, physical disk redundancy, nor
permission to delete inputs/results. Cleanup stays separately gated on a
complete acknowledged custody chain, object identity, retention, writer
fencing and ownership. `acceptedAt` is informational, not a replay expiry.
The result-copy receipt is never evidence for deletion of the raw Emlid GCP
CSV: that input needs a separate immutable/no-purge ownership and retention
contract with its own independent evidence.

The current proposal has no independently validated source snapshot epoch or
worker-to-coordinator deletion authority. Those remain hard release blockers;
do not invent an epoch field or flip existing readiness/namespace guards.
Negative vectors must include scope swaps, changed IDs and object bytes,
outer acceptance substitution, reordered/partial closure, replay conflict,
wrong key/domain, duplicate JSON keys, noncanonical signatures, wrong protocol,
credential revocation/rotation, crash between copy/receipt and every journal
boundary, protocol-negotiation pinning/no fallback, key rotation and revocation
and ZFS/mount boundaries. They establish interoperability only, not production durability.
The current synthetic vector signs placeholder manifest digests; it validates
canonical/signature interoperability and illustrates independent scope checks,
not actual manifest closure, strict raw JSON parsing, or an implemented v2
acceptance verifier. The mutation list is a specification for further tests;
the current executable negative tests verify signature failure on unchanged
signatures, not all semantic rejection scenarios after valid re-signing.
