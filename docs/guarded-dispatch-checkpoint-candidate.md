# Guarded dispatch checkpoint candidate

This isolated candidate records the coordinator's current guarded native
mutation before calling the node. It preserves the task incarnation, job, node,
engine UUID, monotonically increasing sequence and either the init request hash,
exact upload name/size/hash, or commit UUID. A reply observed by the coordinator
is marked separately. A missing or changed checkpoint holds the task; this
record is evidence of coordinator intent, not proof that a node executed it.

No automatic resume is implemented. A safe resume must independently obtain an
authenticated node-journal response tied to the same authority, generation,
job, task, operation and request digest, and verify all previous operations and
source bytes before it can continue. Node ownership, old-writer exclusion and
the receipt/release lifecycle remain open. Legacy records and preexisting held
tasks have no checkpoint and require reconciliation. Existing coordinator
binaries ignore this table; rollback or an already-running old writer is not
fenced. The updater's retained-volume census must explicitly review this new
table before any deployment. No activation, cleanup, or data deletion is
authorized by this candidate.
