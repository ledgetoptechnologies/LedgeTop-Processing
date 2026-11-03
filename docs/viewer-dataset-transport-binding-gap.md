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
