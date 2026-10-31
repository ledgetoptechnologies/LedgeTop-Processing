# PR197 predecessor stopped census: isolated draft

`ops/pr197_stopped_census.py` wraps the PR122 keyed census with two exact
schema profiles generated from synthetic state using predecessor image
`sha256:7fae9385fb5ca829783fe05eef2fdb1983abf4d491b2b9f795e55b617a5a0004`.
It requires Linux, a read-only mount root with no nested mounts, an ordinary
checkpointed SQLite database with no sidecars and recognized schema. It has
configurable, hard-ceiling limits on entries, bytes per file and overall,
database size, row count, individual row size and materialized row bytes.
Defaults are 5,000 entries, 64 MiB per file, 256 MiB for the sum of file sizes
hashed in the tree (not a bound on all SQLite and metadata I/O), 8 MiB database,
4,096 rows, 256 KiB per row and 8 MiB of rendered rows; raising them remains
bounded by the explicit limits in the source. Exceeding any limit holds and
preserves all source files. It records the complete keyed table digests and file-tree
census; invalid, changing or unsupported state holds. Schema equality does not
prove which binary created or last wrote the state.

The module has no cutover or rollback operation and always reports that
writer fencing, cutover and rollback are not qualified. A read-only mount and
point-in-time comparisons do not prevent another mount namespace or privileged
writer from modifying data, including transient writes between checks. Do not
use this as sole authority to deploy or restore a production coordinator.
The census now also exclusively flocks the existing `coordinator.lock` inode
throughout the read and refuses a busy, missing, replaced or cross-mount lock.
This rejects the tested cooperating predecessor while it is running; the lock
is released when the census returns and cannot constrain writers that ignore
it, administrators, or alternate lock inodes. It is not a cutover lease.
The caps bound application-level materialization and file bytes read; they do
not impose a wall-clock deadline on filesystem or SQLite operations, protect
against an uncooperative privileged writer, or reserve memory/storage on a
thin-provisioned filesystem. Full hashes are never truncated to meet a limit.
The 256 MiB default does not qualify real image datasets or large state trees.

The actual-image synthetic rehearsal passed after comparing a stopped old
source and candidate on separate read-only mount roots: table digests and
Viewer file contents matched, including 16 synthetic schedule rows. Explicit
allowances cover the database file's physical bytes, size and mtime because the
separate process may rewrite SQLite without changing logical rows/schema; no
table contents or Viewer file contents are relaxed. A restore of the earlier
backup is not rollback of subsequent candidate writes. Six focused tests
cover both generated predecessor schema profiles, nonempty schedules,
unknown schema/sidecars, changes during census and changed rows/files.

The first new rehearsal failed because an allowlisted fingerprint was copied
with one missing character. A separate read-only inspection identified and
corrected the typo. Its synthetic volumes and task-owned keeper remain under
the uncertainty policy; recovery metadata is
`C:/Users/fstor/AppData/Local/Temp/codex-viewer-docker-recovery-fhjzwhip.json`.
No previously rejected cleanup was retried.
