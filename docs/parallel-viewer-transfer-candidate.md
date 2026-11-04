# Parallel Viewer transfers: isolated candidate

This prototype can send an already-claimed Viewer task in a dedicated thread,
allowing the scheduler to poll or dispatch a different node while a transfer
waits. `PROCESSING_VIEWER_PARALLEL_DISPATCH=1` enables outbound transfers only;
absent or `0` keeps the synchronous path. Any other value, `1` without
`PROCESSING_PER_NODE_SLOTS=1`, or `1` with `PROCESSING_REFERENCE_CONFIG` refuses
startup before any lease or state is touched: this mode is not qualified with
the reference runtime. No deployment sets it.

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

The flag does not enable parallel inbound uploads (`parallel-inbound-candidate.md`),
which stay blocked until shared disk reservation exists. When the flag is on,
one Queue-owned mutex serializes inbound file streams, while store locks cover
only the preflight and publication transactions. A stalled inbound stream no
longer holds the store lock during reads and file sync. Same-task controls
still conflict rather than wait. This mutex is process-local: it neither
reserves global disk capacity nor fences another process or external writer.
The Queue fixes this mode at construction and rejects a mismatched service.
The existing ability to inject a different TaskStore is not newly qualified.
Broker `update` and `hold`
writes bypass the per-task fence under the flag, so a same-task request cannot
force a false hold or stall a claim; other same-task controls get a retryable
conflict rather than wait for a fenced request.

Tests use fake engines and exact synthetic bytes. This candidate does not fence
direct engine clients, another coordinator with separate state, or external
filesystem writers; it does not reserve shared disk space. Node identity and
storage authority, mixed-version migration, real-engine and real-upload tests,
and the newer reference path remain unresolved. Do not deploy or activate it
without qualifying those.

The revised three-module synthetic regression ran 193 tests on Linux with
one skipped, using fake engines and pinned test dependencies. It does not
establish real HTTP upload behavior, global capacity, or cross-process fencing.

An additional owned-child test uses a real loopback HTTP listener and partial
multipart request: shutdown retains Queue authority until the client half-closes,
then exits and leaves no committed partial input. Node readiness is stubbed.
It does not establish a hard shutdown deadline, abrupt process/power loss, a
real engine, or production network and filesystem behavior.
