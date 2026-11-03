# Guarded dispatch history candidate

This inactive candidate retains the coordinator's ordered intent and observed
acknowledgement for each guarded init, upload and commit. The existing current
checkpoint remains the recovery boundary. History and checkpoint changes occur
in one SQLite transaction; if their current rows disagree, dispatch holds.
Checkpoints written by an older version have no recoverable prior sequence and
therefore hold rather than acquire a fabricated history.

This is not proof that the node applied a mutation and does not authorize
resume, retry, cleanup, or a change to a held task. Authenticated job-bound node
journal proof, coordinator authority, exclusive engine ingress, old-writer
fencing and a transactional recovery protocol are still needed. History must
not be used as a complete cursor without a full integrity check. The new
table requires an explicit upgrade, rollback, retention and capacity review;
older binaries do not maintain it. This candidate has no production activation.
