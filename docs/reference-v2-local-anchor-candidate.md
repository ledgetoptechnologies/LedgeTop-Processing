# Disabled local registry checkpoint candidate

This candidate owns a SQLite checkpoint on a filesystem distinct from the
private signed-registry file. Explicit offline bootstrap pins root digest, epoch,
revision and exact registry bytes digest; missing state never auto-bootstraps.
Under an owned process lock it verifies the current registry signature and role
policy, rejects lower revisions and same-revision conflicting bytes, and accepts
only exact replay or a contiguous revision increment committed with a compare-
and-swap. Anchor ahead of the registry holds until the exact registry returns.
Root rotation is deliberately unsupported and requires a separately designed
offline re-enrollment procedure; never overwrite an existing checkpoint.

The separate `st_dev` test checks observed device identity only, not separate
hardware, administrator, backup, failure or restore domains. A rollback of the
checkpoint itself, especially alongside the registry, can silently restore an
older accepted state. Independently protected monotonic authority or explicit
offline re-enrollment that isolates all old writers remains mandatory. The
filesystem/inode observations do not exclude hostile concurrent path or mount
replacement. A returned key snapshot cannot receive later revocation; callers
must obtain independently current authority each time. No such independent
authority, root lifecycle, freshness source or physical stream fence exists here.

Three isolated nonroot synthetic tests exercised two filesystem devices, exact
replay, reopen, stale/conflicting/ahead revisions, wrong root, missing checkpoint
and same-device rejection. A separate owned child exited immediately after a
committed increment; after restart exact replay passed and an older registry held.
This is process-death evidence, not a power-loss, multi-host concurrency,
external-writer or anti-rollback qualification. Candidate remains false-gated,
not connected to admission, and no network, production key or client data used.
