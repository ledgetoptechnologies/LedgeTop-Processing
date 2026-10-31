# Draft same-inode output slot

`processing.direct_output_slot` is an isolated Linux filesystem primitive. It
does not run a node service, authenticate a caller, own a NodeGate, start an
engine, assert completion or Viewer custody, or delete anything. It is not
wired into direct Viewer processing.

The caller must independently provision a private persistent root and supply
its previously verified `(device, inode, Linux mount ID)` and an operator-owned
maximum allocation budget. `open_slot(..., create=True)` exclusively creates
an identity-named regular file, physically requests its full logical budget via
`posix_fallocate`, checks file size and reported allocated blocks, syncs it,
and persists and syncs an exact binding/root/inode/budget metadata record. The
slot retains a nonblocking exclusive OS flock. `write_at` writes bounded bytes
to that very inode, with identity checks before and after; operations on the
same slot object are serialized. A short or failed write and a failed sync
hold that object for inspection; `sync` requests a
file sync. Closing releases the lock and does not remove the slot. Existing
names are never overwritten or unlinked. A missing or incomplete metadata
record, changed file/root/mount, changed budget, missing allocation or lock
conflict is a hold. On reopen a complete matching record and allocation are
rechecked and re-synced; this also resolves a lost response after a fully
written metadata record. No data-content completeness or checksum is implied.

The caller must maintain exclusive access, stable trusted ancestor directories
and mount topology, and exclusive ownership of the root identity. A same-UID,
privileged or noncooperating writer can race identity checks, alter or remove
files, or consume unrelated space. `flock` is cooperative and does not prove
any physical engine fence. This primitive does not globally reserve filesystem
metadata space, processing scratch, or a free-space floor for other writers.
The filesystem's fallocate and `st_blocks` reports do not prove physical device
capacity through thin provisioning, compression, copy-on-write, network
storage, quotas, controller caches or power loss. An operator must qualify the
filesystem and persistence guarantees; a tmpfs test proves no reboot survival.
Mount IDs may change after reboot and will fail closed until separately
reconciled. A fully written but unacknowledged metadata record is usable only
after revalidation and fresh sync; partial metadata and preallocated orphans
remain for explicit inspection, not automatic retry or cleanup.

Named root validation reopens the root and compares device, inode and mount ID;
it remains a point-in-time observation, not a fence against a malicious mount
or ancestor replacement between checks. There is no content-write journal: a
write failure or process death can leave partially changed bytes. The in-memory
hold does not survive reopening; reopening proves allocation and identity only,
never that a prior write completed. An integrating capture writer must maintain
its own durable write-intent and content-completion protocol before reuse.

Before integration, an independently reviewed capture writer must consume this
same inode without truncating it or replacing its path, validate the actual
archive length/hash/identity, record the used length, and provide its own
crash-safe publication and custody protocol. Authentication, a real NodeGate
reservation, admission, update/rollback, persistent mount attestation, engine
cleanup and Viewer acceptance remain separate unfinished requirements.
