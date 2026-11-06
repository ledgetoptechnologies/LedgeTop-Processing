# Coordinator intake direction

Beau confirmed on November 6, 2026 that the cluster coordinator owns a job's
queue and scheduling after it accepts the job; Viewer displays coordinator
status and receives the result. Viewer keeps source files until durable handoff
is established. The empty `processing-spool` directory created under Viewer
Storage is not part of this design; no TrueNAS NFS spool is required.

When an eligible node is immediately available, the intended path is an
authenticated stream through the coordinator directly into that node's
reserved durable scratch space, without a full coordinator-disk source copy.
When all eligible nodes are busy, the coordinator reserves local disk and
durably queues the upload until a suitable node is available. If space cannot
be reserved, intake must give retryable backpressure without claiming custody.
The small node is preferred when eligible, with one active job per physical
node and production ahead of waiting staging jobs.

As of this review, V2 writes every accepted source object to the coordinator
spool before worker claim; direct-to-worker streaming is not implemented.
Existing spool capacity, hash, publication and restart checks must remain in
force for local-queue mode. A new direct path needs atomic node and scratch
reservation, durable worker receipts, full object/hash verification and
reconciliation after interrupted transfers before acknowledging custody.
Do not report processing until the engine actually starts, or delete original
inputs before durable handoff and verified result retention requirements.

The last cluster root/Docker filesystem observation was approximately 113 GiB
free, a point-in-time value rather than a reservation. Recheck it and account
for queued source bytes, worker scratch, the coordinator database and a free
space floor immediately before admission. The local mount and recovery behavior
remain unqualified on the installed system.
