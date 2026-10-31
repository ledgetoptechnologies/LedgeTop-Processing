# PR197 predecessor stopped census: isolated draft

`ops/pr197_stopped_census.py` wraps the PR122 keyed census with two exact
schema profiles generated from synthetic state using predecessor image
`sha256:7fae9385fb5ca829783fe05eef2fdb1983abf4d491b2b9f795e55b617a5a0004`.
It requires Linux, a read-only mount root with no nested mounts, an ordinary
checkpointed SQLite database with no sidecars, a maximum 64 MiB database, and
recognized schema. It records the complete keyed table digests and file-tree
census; invalid, changing or unsupported state holds. Schema equality does not
prove which binary created or last wrote the state.

The module has no cutover or rollback operation and always reports that
writer fencing, cutover and rollback are not qualified. A read-only mount and
point-in-time comparisons do not prevent another mount namespace or privileged
writer from modifying data, including transient writes between checks. Do not
use this as sole authority to deploy or restore a production coordinator.

The actual-image synthetic rehearsal passed after comparing a stopped old
source and candidate on separate read-only mount roots: table digests and
Viewer file contents matched, including 16 synthetic schedule rows. Explicit
allowances cover the database file's physical bytes, size and mtime because the
separate process may rewrite SQLite without changing logical rows/schema; no
table contents or Viewer file contents are relaxed. A restore of the earlier
backup is not rollback of subsequent candidate writes. Five focused tests
cover both generated predecessor schema profiles, nonempty schedules,
unknown schema/sidecars, changes during census and changed rows/files.

The first new rehearsal failed because an allowlisted fingerprint was copied
with one missing character. A separate read-only inspection identified and
corrected the typo. Its synthetic volumes and task-owned keeper remain under
the uncertainty policy; recovery metadata is
`C:/Users/fstor/AppData/Local/Temp/codex-viewer-docker-recovery-fhjzwhip.json`.
No previously rejected cleanup was retried.
