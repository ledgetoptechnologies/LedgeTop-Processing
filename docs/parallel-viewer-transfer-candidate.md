# Parallel Viewer transfers: isolated candidate

This prototype can send an already-claimed Viewer task in a dedicated thread,
allowing the scheduler to poll or dispatch a different node while a transfer
waits. It requires both per-node slots and the explicit internal
`viewer_parallel_dispatch` configuration value. The shipped configuration has
no such setting, so this candidate is not activated by an environment flag.

The job claim and engine UUID remain durable before transfer starts. In-flight
submissions cannot be cancelled or operator-resolved through ViewerService;
uncertain submission is held without repeating a mutation. Shutdown waits for
task-owned senders before releasing coordinator queue authority. A slow sender
may therefore delay graceful shutdown; forced process death still needs the
existing held/reconciliation path and has not been newly qualified here.

Senders cooperatively check shutdown and node maintenance between engine
mutations. If either is observed before commit, they hold the job and its slot
without replaying partial work. An in-flight HTTP mutation cannot be stopped by
these checks, and a stop can race the gap between a check and the next HTTP
call: these checks are not a cross-process fence or a guarantee that no request
starts after maintenance begins. A held submission also requires operator
reconciliation before maintenance can complete.

An additional synthetic characterization demonstrates a deployment blocker:
the existing inbound `TaskStore.upload` holds the store-wide lock while reading
and syncing a file, so a stalled upload can still block the scheduler and both
nodes. Removing that lock needs a per-task identity fence and safe shared-space
reservation; this candidate deliberately leaves it unchanged.

Tests use fake engines and exact synthetic bytes. This candidate does not fence
direct engine clients, another coordinator with separate state, or external
filesystem writers; it does not reserve shared disk space. Node identity and
storage authority, mixed-version migration, process-death and real-engine tests,
and the newer reference path remain unresolved. Do not deploy or activate it.
