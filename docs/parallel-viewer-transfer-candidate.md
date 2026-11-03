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

Tests use fake engines and exact synthetic bytes. This candidate does not fence
direct engine clients, another coordinator with separate state, or external
filesystem writers; it does not reserve shared disk space. Node identity and
storage authority, mixed-version migration, process-death and real-engine tests,
and the newer reference path remain unresolved. Do not deploy or activate it.
