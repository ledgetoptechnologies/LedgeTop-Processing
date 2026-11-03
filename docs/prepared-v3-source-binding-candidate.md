# Prepared-v3 source reservation binding candidate

This branch integrates an unmounted authenticated inspection endpoint and store
method with the isolated Viewer-v2 source reconciliation model. The inspection
loads the v3 owner, canonical prepared envelope, immutable options-policy snapshot,
manifest and saved reservation within one SQLite read transaction. It derives the
Viewer attempt, object closure, dataset source version and strict renewal budget
from those records; the incoming grant supplies none of those authoritative
snapshots. A structurally matching grant returns `held` because bearer validation,
broker authorization, current epoch and transport fencing are not implemented.
It is not an ACK, transfer authorization, result acceptance or cleanup authority.

For v3-prepared jobs, the real store reservation method now requires a Viewer
attempt equal to the verified activation/envelope attempt in its reservation
transaction. It requires the same fixed-chunk, unique-request and retry budget
formulas as the Viewer renewal candidate. V3 source/activation readiness and
activation-bound worker/grant readiness remain hard-disabled. Legacy v2 reservation
shape remains unchanged. Existing differently shaped reservations and historical
ledgers are not automatically migrated; ambiguous state requires reconciliation.

A synthetic loopback test exercises real HTTP prepared submission, activation,
claim, reservation and inspection, then reconstructs the store and reinspects.
It patches the two readiness predicates only in the test, as no real broker or
source-provenance capability currently exists. Wrong principal/environment,
attempt, object IDs, mutable reservation budgets and persisted manifest changes
are rejected. No real job, Viewer runtime, broker route or NAS data was used.
The current reference driver, request builder and v2 client still do not include
the Viewer attempt field and cannot use this path without deliberate integration.
The JCS optional dependency must be available only in the isolated test setup; no
runtime image or deployment was changed.
