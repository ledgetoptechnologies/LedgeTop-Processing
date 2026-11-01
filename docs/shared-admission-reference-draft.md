# Shared admission with the reference listener (unqualified)

This draft extends the existing coordinator-local admission barrier to a
closed-start configuration with the reference listener configured. Reference
submissions enter the same barrier in `ReferenceStore.submit`, with a second
check in their SQLite write transaction. Reference worker claims already use
the barrier and transaction check. The unsupported-shape submission replay
also enters the barrier, even though that replay is read-only. An exact replay of an existing worker claim
is a read-only recovery operation and can still return the prior claim while
closed; it is not permission to start or resume an engine.

An admitted reference submission holds the barrier until its transaction
finishes; a close in progress rejects new native and reference admissions and
waits for prior holders before committing its receipt. Authentication remains
separate for native Viewer, reference Viewer and worker principals. Other
reference operations, in-flight claims, result transfer, already admitted
engine work, background writes, direct engine access, old coordinators and
host writers are outside this admission barrier. Closing does not establish
quiescence, a physical writer fence, a safe update, or rollback authority.

`ADMISSION_DRAIN_QUALIFIED` remains false; both environment parsing and direct
handler construction refuse an unqualified enable before constructing state.
These changes must not be enabled on
a deployed coordinator until its admission coverage, transport, external
writer exclusion, nonempty migration and rollback have been separately
qualified. The focused reference tests use synthetic data and mocked TLS
assembly while exercising the reference adapter; native requests in the race
test use loopback HTTP. They do not prove live network or engine fencing.
