# Candidate receipt namespace database gate

This is a default-off candidate, not an installed cross-host or cross-process
writer fence. `ReceiptNamespaceGate` requires the actual v1 and v2 importer
instances to refer to the coordinator's same existing file by path and device/
inode, checks their current file identities and the active Queue's authority,
and refuses other databases. It cannot establish that every possible writer is
using those objects, prevent pathname replacement after a check, fence an older
binary or detect a database clone with the same authority ID.

Configuration runs a bounded, read-only SQLite snapshot census after the existing
stores have already been initialized. It checks IDs in the success acceptance,
signed Viewer copy, terminal, both import-confirmation journals and retained v3
held-copy namespaces. Existing collisions, malformed proof/identity, absent
required journals and row/byte limits hold; no data is repaired or deleted.
Held-copy rows undergo the existing digest and provenance integrity checks. Those
checks are still a trusted-database integrity check and do not reverify signatures
against a persistent historical key registry. Imported historical rows need a
separately reviewed provenance and signature audit.

V3 held-copy verification is unavailable unless a gate is explicitly configured;
the writer rechecks the gate in its `BEGIN IMMEDIATE` transaction. The exact v1/v2
instances attached to that gate check newly proposed confirmation IDs in their
own write transactions, and exact replay remains possible. Competing cooperative
writers serialize on the same SQLite file. Other importer instances, separate
processes, legacy binaries, and all writers that do not call this gate can bypass
it; configuring it is not authority to enable result acceptance or cleanup. It
must be re-established on restart. Before any production activation, establish an
external writer fence and configured DB identity, independently inventory every
journal location and existing writer, reconcile any cross-database history without
resetting identities, and review migration/rollback and current signing keys.

Synthetic POSIX tests exercise real journal instances, exact same-file checks,
nonmutating clean/collision census, separate DB refusal, concurrent cooperative
SQLite writers, and signed v1 proof handling. No live database is opened.
