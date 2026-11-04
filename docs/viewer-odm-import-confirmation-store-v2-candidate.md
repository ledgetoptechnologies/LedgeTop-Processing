# Inactive ODM v2 import confirmation journal

This isolated candidate combines the provisional ODM-only signed assertion
verifier with the durable first-confirmation journal. It uses a separate v2
table and retains the older candidate table unchanged. It does not migrate,
reconcile, or assert equivalence between records in the two versions. Neither
table authorizes cleanup, execution, or publication. No endpoint is mounted.
Both tables can contain records for the same job/generation/environment; the
v2 record does not supersede, revoke, or validate the v1 record, and neither
can be replayed as the other. A future consumer must not choose between them
or release cleanup merely because one or both rows exist. Migration and any
supersession semantics require their own authenticated protocol and review.

The journal independently reloads job/generation/environment, prebound Viewer
task and attempt, issuer pins, result manifest, and signed accepted-copy
closure inside its SQLite transaction. It checks the archive ID, size and hash
against the trusted result manifest and records exactly the signed canonical
proof and digest. Viewer model/output IDs and both inventory digests are signed
post-ingest assertions; they are not independently verified Processing facts.
Replays reverify signatures, current key pins, exact bytes and the original
binding; changed validly signed output assertions hold. POSIX directory fsync
and FULL SQLite synchronization are required; unsupported systems fail closed.

The trusted loader used in tests is synthetic. No production mechanism yet
establishes an authenticated, transactionally coherent task/attempt-to-job
binding, accepted-copy closure or environment-scoped key pins. Viewer signing
schema, writer-fenced inventory, durable readiness outbox, cross-language proof
vectors, source provenance and downstream cleanup policy require agreement and
end-to-end verification. Existing unverified Viewer files or caller-created
verification objects must never authorize cleanup. Successful synthetic tests
do not prove survival of every power failure or a live Viewer result handoff.
