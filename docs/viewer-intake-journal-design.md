# Intake journal: proposed design, not implemented or qualified

The current guarded scan in `TaskStore._check_input_recovery` is deliberately
inactive. Scanning the whole task directory on every input is quadratic over a
10,000-photo upload, and directory names and regular-file type alone do not
prove content identity. This document describes a replacement; none of its
migration, fencing or recovery claims should be assumed to exist today.

## Ownership and offline qualification

All TaskStore writers, including old coordinators, must be demonstrably stopped
and fenced from the data and database for the entire offline migration and
reconciliation. The current configured authority lease is cooperative: a
legacy process or direct filesystem writer can ignore it. The lease alone is
therefore insufficient evidence for qualification. Complete this ownership
mechanism before enabling the feature. Pin a migration/version marker with exact
schema and required triggers, verify it before admitting uploads or commits,
and fail closed when it is absent, malformed, or partially migrated. Install
mutation-fencing triggers and attest the schema on each modifying transaction;
a privileged writer or actor that can replace the database is outside this
cooperative boundary. No silent migration or automatic downgrade.

## Index and write protocol

A durable intake journal keyed by task UUID, generation, canonical filename
and server-generated operation ID records the bound task directory, private
random temporary and final basenames, expected size, and state. Unique indexes
on `(uuid,generation,name_key)` for active/committed entries and on both owned
basenames permit O(log n) indexed lookup. States include `prepared`, `sealed`,
`committed`, and `inspection`. The journal and `viewer_files` must be changed
in the same SQLite transaction for the final `committed` transition. Exact
DDL, migration version and trigger attestations require independent review.

1. In a short immediate transaction, check that the task generation, directory,
   state, limit accounting and lease still match. Create `prepared` with random
   names before creating a file. Serialize duplicate names through the index.
2. Create the temp exclusively under a previously checked task directory and
   stream bytes with a bound, hash them, fsync the file and directory. Update
   `sealed` in a short transaction, persisting byte count and digest with the
   same journal identity. The writer holds authority throughout; a journal row
   is not evidence by itself that the writer has stopped.
3. Rename the exact sealed temp to the predetermined final name without
   overwriting any existing file. Sync the directory. Insert `viewer_files` and
   transition the exact journal row to `committed` in one DB transaction,
   rechecking generation and task state. No file is unlinked on uncertainty.
4. Return success only after commit. A retry of a committed name must compare
   the received length and digest to the recorded identity. Because the current
   multipart parser spools before TaskStore knows the part name, eliminating
   retry spooling requires restructuring the actual HTTP parser to pass headers
   before streaming and a read-only hash sink for existing names. It must still
   consume and validate the complete body, preserve bounded streaming and never
   report success based on filename alone. Existing clients need no API change.

## Startup and interrupted writes

Before upload or commit admission, start held and perform a bounded, paginated
reconciliation under proven writer fencing. Inspect indexed noncommitted rows;
compare file type, path, size and digest against the journal using open file
descriptors, no-follow and stable directory identities. Scan each task directory
once in bounded batches for unregistered input/incoming names. Persist a
checkpoint and do not open admission for an incomplete or corrupt scan. Large
history must be processed incrementally, not capped in a way that makes normal
history permanently unusable. Metadata-to-disk and disk-to-metadata checks are
both required; do not infer deletion permission from a missing row.

A `prepared` row with no file, a partially written temp, or a matching sealed
temp without a completed publication remains inspection-held until a qualified
resolver establishes the preceding writer cannot resume. A sealed matching final
with missing DB commit may be committed only after exact identity, content,
task generation, task state, filesystem durability and fencing are independently
verified. A mismatch, ambiguous temp/final pair, unreadable file, or unknown
writer stays held. A committed row must match `viewer_files`; absent bytes or
an unexpected leftover is held. The qualified resolver should be idempotent;
its own crash repeats inspection, never deletes uncertain bytes. POSIX fsync and
rename durability depend on the actual mounted filesystem, so testing is
necessary but does not substitute for deployment-specific qualification.

Successful reconciliation can maintain a trigger-attested unresolved count for
O(1) task commit admission, and use indexed per-filename lookups for uploads.
All state-mutating paths—including remove, reinitialize, retry, and old recovery
code—must participate; cross-resource races remain unless the writer-fencing
boundary actually holds. Explicit rollback must retain journal and uncertain
files, keep admissions closed, and use a reviewed backward-compatible exporter;
a pre-journal binary cannot safely be run against active guarded storage.

Required tests include controlled crash/restart at each durable boundary,
parallel exact and conflicting retries, stale generations, corrupted schema,
unknown directory entries, uncooperative old writer, interrupted migration,
10,000-entry bounded reconciliation and per-upload lookup measurement, and
real-filesystem fault qualification. None of those future tests has been
claimed by this design.
