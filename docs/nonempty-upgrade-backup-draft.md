# Nonempty coordinator backup stage (draft)

This unwired offline stage combines the archive preflight with the existing
stopped-volume backup primitive. It first requires the prior keyed census and
qualified schema map to match, then checks authority identity. Under the
existing advisory authority and queue locks the backup takes a fresh census and
compares it to that exact preflight before creating a new destination. It never
adopts an existing destination, deletes a failed copy, or restores over source.
It refuses any nonempty `reference_*` table because reference-work quiescence
is not yet established; nonempty retained native history can be copied.

The caller must independently authenticate the baseline and schema, associate
them with the candidate image, exclude all writers including old implementations
and direct clients, and establish destination independence. Boolean parameters
are assertions, not proof. The source must be a read-only mount root and the
destination a separate filesystem per the reused primitive. These checks do not
create an external writer fence and do not cover symlink swaps by hostile
writers. Failure after destination creation retains the incomplete copy for
inspection. A successful backup is not a candidate activation or rollback.

Synthetic tests use actual temporary-file copy and advisory locks, but mock the
read-only and independent mount checks; they do not qualify mount topology,
concurrent hostile writers, crash or power-loss behavior. This is not wired into
a production updater and must not be used for live cutover.
