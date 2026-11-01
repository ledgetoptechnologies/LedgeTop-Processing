# Disabled native input-identity binding draft

This unqualified feature defaults off. Normal environment and direct app
startup refuse it unless a test fixture explicitly patches the qualification
constant and supplies Viewer, persistent state and an authority lease. It must
not be deployed: older binaries ignore the new marker, and a host-level writer
and rollback fence is not established.

The proposed authenticated test route binds a canonical candidate manifest to
an existing task-capability reservation only if no task row, incarnation row or
matching orphan task directory exists. For this deliberately narrow fresh-UUID
case the coordinator issues generation 1. It stores exact manifest bytes,
digest, environment, queue authority, UUID and capability hash in one SQLite
transaction. Exact replay returns the same metadata; changed data or a reused
manifest identity in the same environment conflicts. A status read requires the
same environment token and task capability and revalidates the stored manifest.

The test receipt contains `protocol`, `version`, `taskUuid`, coordinator
`generation`, `environment`, `manifestId`, `manifestRevision`,
`manifestSha256`, `bindingId`, `state: identity_bound`, and
`capacityReady: false`. The binding ID hashes the server's authority and
capability-binding metadata; it is not a signature, user identity, a transfer
grant, or a physical-space receipt. Viewer-local initialization generation is
not the coordinator generation. The environment bearer identifies a shared
environment, not a distinct user. Credentials and input contents are not put
in the receipt.

The feature intentionally cannot initialize a bound task or upload/dispatch
it. Legacy init, upload and commit are rejected for a UUID with a binding.
Cancellation keeps the marker and causes subsequent binding/status requests to
hold. Historical, removed, or ambiguous task state is not adopted, and no
files or markers are deleted. The marker is retained indefinitely with a 1,000
record / 64 MiB aggregate canonical-manifest limit; this is not an SQLite disk
quota. The filesystem orphan census is bounded by entry count, not a hard I/O
deadline, and relies on trusted directory ancestors and cooperating writers.
The disabled-start marker check currently occurs after queue construction, so
it is not a no-side-effect startup preflight.

The 20 GiB total input ceiling, 4 MiB manifest limit and 10,000 total-object
limit remain. A manifest is identity metadata only: exact-inode capacity
allocation, multipart spool handling, transfer verification, process-crash
recovery, old-writer exclusion, resource release, and enabling dispatch remain
unimplemented. A 50 GB dataset and 10,000 photos plus GCP remain unsupported.
