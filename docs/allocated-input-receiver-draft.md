# Allocated indexed input receiver (unqualified draft)

This source experiment is disabled by default and is not deployable. A fresh
bootstrap can explicitly select `inputCopyAccounting` schema 5 with the
`indexed-worker-fetch-three-retained-v1` journal and closed
`allocatedReceiver: {schemaVersion: 1}` marker. The bootstrap binds that choice
to its private receipts and latches the receiver only before any reservation.
Schema 5 is a local proposal and has not been coordinated or reserved across
other worker profiles or branches.
Existing state is not migrated. Normal startup validates the completed marker;
it does not create or latch it. An external host-owned fence must establish that no
old or competing worker can write the scratch filesystem. The marker makes
older ledger implementations reject subsequent transactions; it does not stop
an older process that is already writing outside a transaction.

The existing indexed maintenance census expects a completion receipt with
`schemaVersion: 2`; this profile writes a schema-3 completion receipt and an
additional ledger marker. The current maintenance census will refuse it. This
is a separate, intentional deployment blocker: do not relax the census or use
this profile in maintenance until its exact schema and lifecycle are reviewed
and supported. The factory test runs with `cleanupEnabled` and
`engineCleanupEnabled` both false; it does not qualify either cleanup path.

The indexed path first records its existing attempt, then the scratch ledger
atomically binds the attempt and input identity and changes the reservation to
`receiving`. This holds accounting through lease expiry. It creates a private
same-directory inode, calls `posix_fallocate` on that inode, fsyncs it and its
directory, records file and parent device/inode/mount identities, and streams
the authorized object to the same descriptor. It hashes the streamed bytes and
re-reads and hashes the inode before no-replace publication. Successful
publication and allocation accounting precede normal driver confirmation.

Failures and crashes preserve uncertain input data and hold the job. In
particular, death after publication but before indexed progress is committed
requires manual reconciliation; this draft does not automatically replay or
adopt the file. File and parent mount identities are recorded, but the scratch
root identity and every ancestor are not durably bound for future adoption.
Existing legacy ledgers with reservations are not migrated. Only a fresh ledger
can latch this format.

This design assumes trusted private ancestors, cooperating same-user processes,
and truthful filesystem allocation semantics. Fallocate is not proof of
physical capacity on thin-provisioned or remote storage. It does not reserve
the separate NodeODM engine's working space, authenticate source provenance
beyond the existing broker grant, qualify 50 GB transfers, or provide a
production cutover or a result-return guarantee.

In a bounded Linux container, focused tests exercised preallocation and inode
identity, actual indexed-driver and synthetic HTTPS paths, interruption and
process death with retention. A broader indexed suite had one failure in its
existing 10,000-object legacy-publisher test at object 1,206, reported as
`indexed_transfer_unavailable`; its underlying cause was not exposed by the
publisher and remains unknown. One later broader run passed the 10,000-object
legacy-publisher test, but the allocated receiver was disabled for that case.
Neither result qualifies 10,000-object allocated transfer or a 50 GB job.
