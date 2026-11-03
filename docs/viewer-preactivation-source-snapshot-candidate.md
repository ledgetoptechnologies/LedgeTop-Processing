# Inactive pre-activation Viewer source snapshot

This pure experiment combines the lossless inventory/GCP candidate with
Viewer's attempt `submission` provenance. It exposes no network route, does
not authorize a source transfer, activation, engine launch or cleanup, and
always returns a held state. Its scope (principal/environment/coordinator
authority, task/attempt, dataset and pipeline), source mapping, resolved
selection, GCP evidence and policy must come from independent authenticated,
transactionally coherent sources. No such Processing-side loader or trusted
cross-service transport is implemented here.

The candidate verifies the complete source union against the supplied dataset
file mapping and matches Viewer `sourceFiles` by exact path/byte size/hash.
The stable dataset file IDs in the inventory are not present in Viewer
`sourceFiles`, so unique-path matching to the independently loaded dataset
mapping is required. The attempt's captured source roles are checked against
the explicitly selected processing set and Viewer extension rules. The
projected transmitted list preserves Viewer order and path spelling: photos
first, auxiliary files next, and generated `gcp_list.txt` as an auxiliary. Its
exact bytes/size/hash and source correspondence metadata are checked by the
underlying inactive inventory validator. The raw CSV remains retained.

The two existing Viewer hashes are distinct from the JCS inventory hash:
`sourceFilesSha256` hashes stable sorted-key JSON of sourceFiles in captured
order; `inputManifestSha256` hashes UTF-8 JavaScript JSON.stringify of the
ordered array with each object's insertion order `relativePath`, `byteSize`,
`sha256`. Neither hash by itself authenticates the dataset mapping, principal,
environment or source bytes. The candidate computes a separate domain-separated
JCS snapshot hash over the source/attempt binding, these hashes, GCP snapshot
hash, exact engine object count/bytes and independently supplied byte policy.
The actual engine's object ceiling includes the generated GCP.
For compatibility with the current source-range renewal candidate, the
transfer byte budget must equal three times the input bytes, capped at the
safe-integer maximum. Other retry budgets remain held until a versioned broker
contract explicitly supports them.

This proposal holds historical or absent evidence, nonrepresentable paths,
unsupported source role/extension combinations, and the case where a raw
`gcp_list.txt` is replaced by a generated snapshot unless the independently
resolved inventory can represent its retained original without silently
changing the original role. Viewer and Processing must agree on that
disposition. No path is normalized or flattened. Future materialization must
also preflight provider-specific basename collisions before writes.

Synthetic tests call the real Viewer `processingProvenance.inputInventory`
from commit `5513f92b6cf45221475a3c38347e3ca1dbf0c32d` (Git blob
`f9adb05b62e1ffec2a90dc06038f77199f0c3436`) and actual GCP generator; they
do not test a real Viewer database, authenticated channel, source bytes,
current coordinator epoch, durable source reservation, or live handoff.
The current evidence cap is below Viewer's maximum. Epoch/authority
revocation, per-file custody/byte verification, source/selection snapshot
authenticity, budget reservation and Viewer schema agreement remain blockers.
