# Synthetic native Viewer upload capacity gate

This candidate is disabled unless an explicitly constructed `ViewerUploadCapacity`
is passed to `ViewerFacade`; no application configuration or deployment enables
it. Existing Viewer clients do not supply a total. An opt-in client must send
one `X-LTDS-Declared-Input-Bytes` decimal header on every upload and commit,
equal to the total bytes of all inputs; a missing or malformed header fails
before the multipart parser consumes the body. The declaration is authenticated
only by the existing task credential; it is not trusted dataset evidence.

The existing ScratchLedger records the task/generation/environment/declaration,
retains an uncertain-state hold for declared inputs plus the 2 GiB request-spool
maximum, reconciles indexed input bytes, and rechecks cooperative free space.
Partial or duplicate batches keep their hold; commit requires exact input bytes.
No release is provided. Existing clients and behavior remain unchanged unless
the gate is explicitly injected. This is a synthetic prototype, not a release.

The lock serializes only this Python object. It does not fence other processes,
legacy coordinators, direct TaskStore callers, Docker, external filesystem
writers, or multiple gates. Ledger and task accounting are separate databases;
a crash can leave conservative holds or unindexed files requiring inspection.
The floor is cooperative, not a disk reservation or quota. Path mount identity,
symlink/rename races, per-request temporary storage, ENOSPC, archive capacity,
worker scratch, cleanup, process restart, current Viewer integration, and
large-dataset throughput need independent qualification before use. The current
implementation may conservatively double-charge an in-progress spool. Never
enable this candidate on a live service.
