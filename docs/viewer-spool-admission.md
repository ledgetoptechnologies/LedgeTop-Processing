# Default-off legacy Viewer spool admission

The candidate is enabled only by an explicit closed
`PROCESSING_VIEWER_SPOOL_ADMISSION` JSON policy. An absent setting leaves a truly
unmarked installation unchanged. No compose/deployment setting enables it, and
the existing20GiB task-input,2GiB HTTP request and100GiB result ceilings remain
unchanged. This is not approval to enable it or submit a large client dataset.

The JSON fields are `schemaVersion:1`, `retainedBudgetBytes`, `safetyFloorBytes`,
`maxConcurrentRequests` and `maxRecords`. Values are positive integers;
concurrency is at most64 and history at most4096. Duplicate/unknown fields and
booleans are rejected. There are no approved production values in this change.

`SpoolCapacity` is shared by the actual factory's facade, TaskStore and result
download service. It binds the state/spool/task directories to the same device,
owner and exact inode identities. Multipart capacity is reserved before any body
read, with2×Content-Length covering the parser temporary and TaskStore copy.
Temporary generators close before settlement. Direct TaskStore uploads without
the real current process/thread/task/generation lease are refused when enabled.
Archive downloads reserve their declared bound, or the existing configured
maximum when absent; chunks exceeding that bound are refused before disk write.
Both upload and archive writers use the same durable ledger and physical floor.
The bounded init multipart form also reserves its one-copy temporary before any
body read and authorizes only its exact prospective task generation. Direct
initialization without that current form window is refused when enabled.

The ledger has a fixed4MiB page cap, DELETE journaling, FULL synchronous commits
and explicit rollback/control allowance. Transactions serialize capacity
admission across distinct guard instances; a Python per-task lock is not the
reservation authority. The available-space probe uses the writing UID's
`f_bavail` on Linux. Unknown probes, changed roots/devices/inodes, schema drift
and hot/unknown SQLite sidecars hold intake.

Accounting is deliberately conservative and has **no reclamation API**. Initial
logical charge is a whole-filesystem used/unavailable-byte upper bound, not a
measurement of this application's files. Every attempted request permanently
keeps its full payload bound, including multipart overhead, exact duplicates,
failed transfers and old/reset task incarnations. After synchronous writers
close, an active future-allocation promise becomes a retained upper bound. A
process death keeps the larger active promise; TTL, restart, remove and queue
completion cannot release it. History/concurrency/budget exhaustion holds new
intake. This profile does not provide sustained quota reuse or cleanup authority.

An immutable capacity binding plus a separate private marker latch the policy
and database inode. The read-only guard runs at the top of `make_handler`, before
Queue/TaskStore constructors can mutate schema or restart state. Removing or
changing the policy, or losing either marker/database, holds rather than silently
starting legacy unaccounted writes. Losing both through a privileged consistent
state deletion is outside that evidence. Already built pre-feature binaries do
not implement this latch: safe deployment/update/downgrade containment must be
qualified separately. No old-image downgrade protection is claimed here.

First enable requires draining old unguarded writers and exclusive ownership;
the code does not attest their absence. Accounting coordinates participating LTDS
writers, not unrelated host/container processes, and is not a kernel disk quota
or physical preallocation guarantee. Floor checks and ENOSPC handling cannot
promise completion in the presence of uncontrolled external disk consumption.
Existing committed files remain retained when admission or a later write fails.
The4MiB cap/control allowance describes this ledger, not the entire legacy
coordinator database or every other service's metadata growth. This data-spool
gate is not qualification of a whole-system storage bound.

The native init request does not declare a whole dataset's byte total. This gate
can refuse safely between requests; it cannot promise at init that a50GB batch
will fit. The per-task upload cap is intentionally unchanged. Before any later
cap increase, separately qualify this source in the actual packaged service,
measure input/result coexistence and check Viewer client limits. Reliable
reclamation/reconciliation and kernel-level isolation remain future work.

Tests cover shared concurrent upload/archive promises, before-body refusal,
real multipart closure, direct-write refusal, conservative duplicate/error/reset
charges, immutable policy/latch restart checks, stream bounds, retained native
results, the actual factory and a Linux subprocess death after reservation.
Small byte streams and arithmetic checks do not constitute a50GB transfer test.
