# Operation-held cooperative locks: isolated draft

`OperationFence` holds the existing authority lease and predecessor
`coordinator.lock` on one process for a caller-owned operation. The PR197
stopped census and stopped backup can borrow duplicate descriptors from that
same owner. Both recheck ownership and named inodes; backup verifies the locks
again before reporting its snapshot. Closing the duplicates does not release
the owner's open-file-description locks.

This is not cutover authority. There is no candidate activation or handoff;
the authority lease remains held until the context exits. Releasing it to let
the candidate start creates an unqualified interval, and owner death releases
locks. The cooperating locks do not prevent privileged or uncooperative writers,
alternate lock inodes or admin-controlled Docker mounts. An external owner and
host writer fence must be designed and qualified before deployment.

The exact-image synthetic test holds both locks over the census and backup.
Separate local process tests demonstrate competing flock refusal, inode
replacement detection, and lock release on SIGKILL. The latter is evidence of
a failure mode, not permission to activate. Backup remains the existing
prototype, including its own unbounded file and row census; the newer PR197
census caps do not impose a bound on backup's separate work. There is no hard
I/O deadline or automatic rollback. No live state or client data was used.
