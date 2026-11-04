# Synthetic Viewer v2 source identity reconciliation

This isolated candidate is not mounted or imported by the Processing service. It
performs no authentication, network transfer, broker authorization, worker
activation, revocation, receipt, source release or cleanup. All source-fetch and
activation holds remain unchanged.

The Viewer broker contract inspected was Viewer commit
`c92deb5ba143fd302bdf787ba832e0ac1bd0e2f0`; the Processing comparison was
`6e989439b1e7b7edc41ac3d372655480f38ca214`. The Viewer v2 contract includes
`viewerAttemptId` in both the grant/transfer and the canonical accounting
identity. The referenced Processing v2 client does not yet carry that field.
A binding passed to `reconcile` MUST come from independently authenticated,
durable prepared-attempt state, not the presented grant or arbitrary caller
JSON. This prototype does not construct or authenticate that binding.

The candidate rejects any attempt, source, reservation, job, manifest, policy or
object closure mismatch; it does not return bearer or nonce. Request identifiers
are computed with the exact Viewer v2 identity JSON and request tuple. The focused
test executes `canonicalIdentity` and `requestIdFor` from the exact Viewer commit
for ordinary and escaped ASCII source versions, including a trailing short chunk.
The test requires a local Viewer checkout and Node; absence of the checkout causes
that one cross-language test to skip, so a skip is not interoperability evidence.

The object closure must also match a separately supplied authenticated immutable
manifest whose complete digest is recomputed and matched to the binding. The
source version must match that dataset identity and digest. Limits must match a
separate trusted immutable policy and the Viewer renewal route's specific policy:
the minimum number of fixed chunks, three times the input bytes (capped at the
safe integer limit), and three times the minimum attempts (likewise capped).
These narrower renewal values are not a claim that every v2 broker issuance uses
the same policy. Neither trusted manifest nor policy is authenticated here.

The separate synthetic SQLite model holds a stable environment/job/generation
lineage and immutable attempt/source/policy/closure. It accounts every simulated
HTTP attempt, including exact retries, while charging unique ranges once, matching
Viewer v2 counting semantics. It journals each attempt and verifies totals and
canonical request IDs on reopen. It serves no bytes and asserts no actual broker
or transport behavior. Source credentials, revocation and authority epochs are not
represented or enforced by this test ledger. Changes in Viewer attempt or source
identity hold rather than minting another budget.
Viewer's actual broker indexes usage by the complete identity, including attempt;
this stricter synthetic parent grouping is a hold for ambiguous rollover, not a
representation of an installed Viewer ledger or proof of migration compatibility.

The v3 test model has different request and attempt accounting semantics. No
conversion, reset, successor migration or cross-version budget transfer is allowed
by this candidate. Do not import its ledger over an existing broker/client ledger.
Production still requires authenticated prepared-store linkage and canonical
caller identity, actual client and Viewer route integration, stable revocation and
epoch fencing, reconciliation of existing v2 accounting, recovery and concurrency
tests, and an independently reviewed deployment and rollback plan.
