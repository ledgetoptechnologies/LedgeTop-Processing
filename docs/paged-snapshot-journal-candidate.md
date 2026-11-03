# Inactive synthetic replay journal

This isolated experiment binds a canonical, independently signed synthetic
statement to the existing prepared job in one SQLite write transaction. It
checks the configured authority, principal/environment, owner, policy and
prepared envelope before recording the exact signed bytes and digest. The
conservative unique key `(issuerId, snapshotId)` spans jobs and key rotation.
Exact replay after expiry returns held status only. A different statement or
scope conflicts; the caller receives no transfer or execution authority.

**The statement schema is provisional and not the Viewer agreement.** It
currently lacks the negotiated GCP evidence/trailer fields and holds non-null
GCP hashes; this is only a synthetic exercise of transaction and replay
behavior. It cannot establish absence of GCP, authenticate source bytes, prove
custody, perform revocation, issue grants, activate work, or release cleanup.
The six nullable GCP fields and exact evidence/trailer version and accounting
must be coordinated with Viewer before a differently versioned integration.
No route mounts this module.

Journal initialization is explicit and only for isolated tests. No production
migration, old-binary downgrade guard, external writer/clone fence, quota,
backup/restore or corruption recovery has been established. An older binary
could ignore this table. The signer registry and clock are trusted in-process
test configuration, not independently managed lifecycle or revocation systems.
The concurrent unit tests share the queue lifecycle lock; process restart tests
verify an orderly close/reopen after the caller discards a result, not power
loss or competing independent coordinators. Retain all existing holds.
