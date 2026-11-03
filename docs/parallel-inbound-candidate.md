# Parallel inbound upload candidate

This candidate is off by default and has no environment or deployment switch.
It is an isolated test of queue-owned per-task upload fencing, not permission
to enable parallel uploads. Mutating operations on the same task serialize
across `TaskStore` instances sharing one `Queue`; unrelated task IO can
proceed under the opt-in. The queue retains its process ownership lease until
the inflight mutation finishes, and `close()` refuses during that interval.
Same-task control operations fail with a retryable conflict while an upload
holds its fence, so a cancellation request cannot block the service-wide
scheduler lock while waiting for a slow inbound stream.
Queued upload callers are counted before waiting on the per-task mutex; commit
cannot overtake a registered upload waiter even if the mutex is non-FIFO.

Tests exercise a slow upload against another task, same-task commit and
cancellation, and process exit during a write. An interrupted `.incoming-*`
file is retained outside the manifest; no cleanup is authorized by this test.

The protection relies on the single coordinator OS lock for one state root.
Alternate coordinators using another root, direct SQLite or file mutations,
mixed old processes, external clients and physical-engine exclusivity are not
fenced. It does not reserve shared disk capacity, prove behavior on network
filesystems or establish an upload total in-flight across different tasks.
Process exit was tested, not hardware power loss. Enabling it requires review,
an upgrade/rollback exclusion plan, shared capacity control and integration
with the outbound dispatch candidate; do not deploy it as-is.
Viewer's existing submit worker retries generic failures only a limited number
of times. It has not been qualified against repeated HTTP 409 conflicts; this
candidate must not be described as guaranteeing eventual client retry.
