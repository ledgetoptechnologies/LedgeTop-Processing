# Viewer per-photo phase update candidate

This isolated candidate avoids reconstructing every input for each upload phase
write. For those writes it checks task identity and option evidence plus the
current file's name, metadata and confined path inside the same immediate
SQLite transaction as the update. The normal `update` API still returns a full
task; the full task is checked again before the engine commit.

This changes validation timing: a different input's path corruption can be
detected later, when that file is reached or at the full pre-commit check. It
does not establish equivalent behavior under external writers, path races,
or process failure. A failed pre-commit phase uses the previously captured job
identity to hold the queue even if reloading a corrupt task raises; after a
commit call starts, the result remains uncertain and must not be replayed.
The queue hold and Viewer-row update are separate transactions. If the second
write fails, the queue can be held while the Viewer row still reports its prior
state. Corrupt paths or evidence can also make full task reads or status fail;
the synthetic tests do not establish a usable operator UI for those cases.

The proposed 10,000-row test is metadata-only, uses a single owned file and
mocks path resolution for counting. It is not a 10,000-photo filesystem,
transfer, throughput, durability, or deployment benchmark. The candidate and
tests require independent review and must not be enabled in production on
this evidence.

On 2026-11-04, five targeted cases and the TaskStore and ViewerService modules
passed on Ubuntu (97 tests in the module run). These cover synthetic corruption
and metadata only; they do not prove safety against concurrent external writers
or qualify this candidate for deployment.
