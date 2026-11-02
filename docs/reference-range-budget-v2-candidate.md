# Processing range budget v2 candidate

This PR adds a Processing-side arithmetic and SQLite ledger oracle for the
Viewer synthetic range-budget fixtures. It is a review candidate, not a wire
contract or a runtime feature. No v1 validator, driver, worker, HTTP route,
configuration default or deployment is changed to call it.

The shared proposal vectors are pinned to their Viewer Git blob SHA-256 values:

- Range planning vectors: `ca46a7e0aeda6cc6c945880b477b4799acc509d8a39f16f5603870022fc2ee68`
- Range ledger event vectors: `ffe1aef6c0d8b4114bcaa52aea4587cb50843a0bb2783724f3dba7f8b3d02d85`

The oracle checks integer-safe request and byte ceilings, v1's positive object
size rule, the 10,000-object/50 GB arithmetic example, and the synthetic SQLite
ledger sequence for charge-before-use, replacement generations, overlapping
ranges, reopen, proof mismatch, proof success, and budget exhaustion. The HMAC
proof constant is pinned across runtimes.

The ledger uses a disposable local SQLite database and an injected synthetic
proof key. It is not an authenticated broker, source owner, ZFS snapshot/hold,
atomic grant outbox, worker offset journal, or rollback/restore test. It does
not verify that an object belongs to a manifest. The real Viewer integration
must supply those properties before any runtime activation. Existing v1
transfer reservations still allow up to 3× manifest bytes; this candidate does
not reinterpret that rule.
