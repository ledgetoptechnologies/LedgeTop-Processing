# Disabled in-process guarded dispatcher experiment

This is an unwired synthetic integration. Both hard-coded qualification flags
are false; no environment variable, listener, deployment or production
configuration enables it. Explicit constructor injection in a test must supply
the actual same-process Queue/TaskStore and two distinct, correctly bound
`EngineIngressGuard` objects. The existing default ViewerService path is
unchanged. Constructor injection is an internal test mechanism, not a security
boundary against callers with Python code execution.

The actual dispatcher routes init, file upload and commit through the guard.
Before upload, it records the selected stored file identity and dispatcher
generation in the coordinator DB, opens a no-follow nonblocking descriptor,
checks the named and opened inode, size, link count and timestamps, and streams
from that descriptor in bounded chunks through the existing socket guard. The
forwarded bytes are hashed and the producer checks the descriptor and named
identity again before either upload journal can report completion. The guard
holds on a detected early response or producer failure. Its current request
limit is 2 GiB including multipart framing; this does not qualify a 50 GB job.
Filesystem syscalls and OS scheduling can stall; the descriptor's nonblocking
flag prevents waiting for a FIFO writer, not arbitrary disk stalls. Source
directory/mount authority and independent physical writer exclusion remain
unqualified. If the memory-only socket sender does not exit after shutdown and
bounded join, its thread handle is retained and the job holds; there is no
qualified reconciliation or service-close hook for that unexpected condition.

Upload uncertainty retains the durable file/ingress intents, NodeGate
reservation and coordinator hold; no automatic repeat is attempted. After a
coordinator restart, existing unfinished upload phases hold rather than retry.
An uncertain commit remains `commit_pending` and the existing dispatcher polls
the exact engine UUID without resubmitting. The test backend is synthetic; no
real engine or process-kill recovery was tested on this integration. Guarded
cancel, output queries with cursors, result download, physical engine fencing,
network isolation, durable storage reservation and large-dataset capacity are
not qualified. Do not enable this path on live systems.
