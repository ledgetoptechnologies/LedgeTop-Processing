# XFS archive quota observation (draft)

This opt-in, read-only host observation is not wired into the coordinator and
does not authorize writes, retries, cleanup, or deployment. It assumes trusted
host administrators, the executing mount namespace, and root-owned system
utilities. It checks an exact XFS mount, directory project inheritance, and
the reported project accounting/enforcement and hard quota. It does not prove
exclusive writers, current or future physical capacity, or reservation of
space. The mount and project can change immediately after a successful snapshot.
The public observation entry point is disabled. The current draft uses the
Linux x86-64 `quotactl_fd` ABI and reads mount IDs directly; it has not yet
been exercised against the real disposable XFS mount and needs independent
review before use.

On 2026-10-31 a disposable 512 MiB loop-backed XFS filesystem on the cluster
host was mounted with project quotas. Its project 42001 had a 64 MiB hard
limit. `xfs_quota state -p` reported accounting and enforcement ON; a numeric
report showed a 65536 KiB hard limit; `xfs_io stat` showed project inheritance
on the directory and the same ID on its child. A 72 MiB synthetic write hit
ENOSPC at 65536 KiB. The test filesystem, loop and temporary directory were
removed and verified absent. This was an isolated observation, not a production
archive volume qualification. A trailing CRLF command after the probes made
the test shell exit nonzero; its cleanup completed and was separately verified.

The mock tests exercise parsing and mismatch holds. A positive end-to-end test
of this exact source against a newly provisioned disposable XFS mount remains
necessary before relying on its output for any purpose.
