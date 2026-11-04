# Parallel Viewer transfers: isolated candidate

This prototype can send an already-claimed Viewer task in a dedicated thread,
allowing the scheduler to poll or dispatch a different node while a transfer
waits. `PROCESSING_VIEWER_PARALLEL_DISPATCH=1` enables it together with the
inbound candidate (`parallel-inbound-candidate.md`); absent or `0` keeps the
synchronous path. Any other value, or `1` without
`PROCESSING_PER_NODE_SLOTS=1`, refuses startup before state is touched. No
deployment sets it.

The job claim and engine UUID remain durable before transfer starts. In-flight
submissions cannot be cancelled or operator-resolved through ViewerService:
both refuse with a conflict rather than wait, and Viewer's handling of that
conflict is unqualified. Uncertain submission is held without repeating a
mutation; a failure escaping the sender holds its job, or the next tick does. Shutdown waits for
task-owned senders before releasing coordinator queue authority. A slow sender
may therefore delay graceful shutdown. Synthetic process death during upload
holds at the next tick, and during commit reconciles by reads, without replay.

Senders cooperatively check shutdown and node maintenance between engine
mutations. If either is observed before commit, they hold the job and its slot
without replaying partial work. An in-flight HTTP mutation cannot be stopped by
these checks, and a stop can race the gap between a check and the next HTTP
call: these checks are not a cross-process fence or a guarantee that no request
starts after maintenance begins. A held submission also requires operator
reconciliation before maintenance can complete.
An explicit `afterJobId` maintenance plan for the already-claimed task allows
that task to finish submitting; node update remains blocked while it runs.

With the flag off, inbound `TaskStore.upload` still holds the store-wide lock
while reading and syncing a file, so a stalled upload can block the scheduler
and both nodes. The flag removes that lock under the per-task fence; broker
`update` writes bypass the fence so a same-task request cannot force a false
hold. Safe shared-space reservation remains unimplemented.

Tests use fake engines and exact synthetic bytes. This candidate does not fence
direct engine clients, another coordinator with separate state, or external
filesystem writers; it does not reserve shared disk space. Node identity and
storage authority, mixed-version migration, real-engine and real-upload tests,
and the newer reference path remain unresolved. Do not deploy or activate it
without qualifying those.
