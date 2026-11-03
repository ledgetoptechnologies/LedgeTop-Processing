# Held Viewer dataset and transport identity gap

The Viewer paged export identifies the Viewer dataset and its dataset manifest
digest. The current prepared-v3 store derives `manifestSha256` from a separately
validated reference-v1 transport manifest. Those values identify different
objects; equality cannot be assumed and substituting either digest invalidates
the source provenance claim. The 1f11929 synthetic GCP bundle does not use the
prepared store and therefore does not prove this binding.

Viewer's `inputManifestSha256` is a third commitment: SHA-256 of the exact
Node JSON.stringify inputFiles in provider order after exclusion/replacement.
It is neither the raw dataset digest nor the reference-v1 transport manifest
digest. Signed source-page hashes are yet another commitment.

A future separately versioned prepared envelope must bind at least the actual
`viewerDatasetId`, `viewerDatasetManifestSha256`, the independent reference
transport input-manifest/closure digest, principal, environment, coordinator
authority, Viewer task and attempt, and activation/submission identities. Its
fields, hash/signature domains, exact page/closure representation and replay
behavior require agreement with the Viewer producer. Existing v3 must retain its
existing meaning. A signed caller-supplied pair is insufficient: a trusted,
transactionally coherent Viewer dataset/attempt provenance source and an
independently verified transport closure must establish both values before the
new binding can stage or authorize anything. Neither source is currently wired
to the prepared store.

Required synthetic integration includes actual Viewer captured records and
stored attempt evidence, independently derived distinct dataset and transport
hashes, swapped/collapsed/altered hash rejection, owner/environment and
attempt isolation, exact replay after restart, and same-transaction rollback
across journal, frames, generated file and evidence. Existing source/grant,
activation and cleanup holds remain. This document introduces no endpoint,
schema migration, secret, deployment or executable data path.

`viewer_dual_digest_binding_candidate.py` is an unmounted, explicitly synthetic
binding/journal exercise with all three identities and page/GCP/file pins. Its
trust fixture is caller-supplied test data; no signature, authenticated Viewer
provenance, source-byte custody or production admission is implied. It writes
only a held synthetic row and does not atomically stage frame or GCP content.
The version and fields are a review proposal, not an agreed Viewer contract.
Its page metadata checks are not a replacement for complete stream, GCP or
quota verification. No old-binary downgrade or external writer fencing has
been implemented. The paired test separately reconstructs the captured Viewer
input hash with synthetic trust; that reconstruction is not yet in the same
transaction as the held binding row.

The later unmounted `viewer_dual_gcp_atomic_candidate.py` composes that held
binding with actual source/selection/evidence/generated-file synthetic bundle
validation in one coordinator SQLite write transaction. It also requires the
persisted reference transport manifest objects to match the independently
validated selected closure exactly. Tests use Viewer 4ed exporter frames and
explicitly injected synthetic provenance, image and correspondence facts.
Selected dataset object IDs must equal their dataset file IDs; generated GCP
uses `gcp_<attemptId>` with `text/plain` and cannot collide with an original
dataset file ID. Nested paths remain bound by the Viewer input-list digest,
while transport objects carry basenames. These rules need independent review.
Failure during any phase rolls back both synthetic rows; exact replay compares
and revalidates retained bytes without deleting or rewriting the bundle. There
is still no signature over this new
binding, no authenticated loader or production trust, and no independently
verified raw source custody, JS correspondence provenance, revocation/fencing,
old-binary downgrade protection or production schema migration. It always
returns held. Process reopen tests are orderly and do not demonstrate power
failure or host crash recovery.

The `viewer_no_gcp_atomic_candidate` is a separate unmounted synthetic
prototype. It binds finalized no-GCP Viewer source/selected frames to the
prepared Processing transport objects and distinct dataset, input, and transport
hashes in one coordinator-owned SQLite transaction. It uses test-injected
provenance, buffered synthetic inputs and candidate tables; it supplies no
signature, network authentication, current epoch, trusted source loader or
production admission. Its held result cannot authorize processing or cleanup.
A killed child test checks SQLite process recovery, not hardware power loss.
