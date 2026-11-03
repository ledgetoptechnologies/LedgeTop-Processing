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
The attempt-bound client candidate below adds the missing Viewer attempt field.
The JCS optional dependency must be available only in the isolated test setup; no
runtime image or deployment was changed.

## Attempt-bound client candidate

For prepared-v3 claims, the private worker response now derives
`sourceViewerAttemptId` from the verified durable activation. The existing driver
passes it through its real request builder; the v2 grant validator, renewal identity
and range request ID include that attempt. Worker ownership is checked before the
private record is returned. Legacy requests keep their existing wire format. A
new attempt identity cannot silently create a new client budget over an existing
legacy or different attempt ledger for the same job/generation; it holds pending
reconciliation. This is not an automatic migration.

A paired synthetic test runs the actual changed Processing downloader over verified
local TLS against the real Viewer c92 source broker code. It checks request replay,
attempt accounting, revocation and rejection of untrusted/wrong-host certificates.
The broker is a test harness, not an installed Viewer route; credentials, source
bytes and certificates are synthetic. Actual source activation, network access,
production authority fencing and cleanup are not enabled or proved. The candidate
includes the independently configured coordinator authority pin and requires one
matching authority header for prepared-v3 routes, including inspection. A database
clone with the same authority ID and rollback still need external fencing.

The Viewer source-grant and accepted-copy delivery routes remain owner-scoped 503
holds. The new inspection result is not either route, a grant ACK, actual broker
authorization, or Viewer custody proof. It does not authenticate the grant bearer
or enforce revocation/epoch fencing. The existing v2 ledger and this candidate's
attempt-bearing identity have no automatic migration; ambiguous older state is
held. The synthetic test patches readiness only within its test process.
