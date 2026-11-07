# Coordinator intake direction

Beau confirmed on November 6, 2026 that the cluster coordinator owns a job's
queue and scheduling after it accepts the job; Viewer displays coordinator
status and receives the result. Viewer keeps source files until durable handoff
is established. On November 7, he approved always-spooling for the first photo
release: every accepted upload is durably written to a private spool on the
cluster PC's local SSD, even when a worker is already free. The empty
`processing-spool` directory created under Viewer Storage is not part of this
design; no TrueNAS NFS spool is required.

The coordinator reserves cluster-local spool space before acknowledging
custody, then queues the job until an eligible worker can claim it. If space
cannot be reserved, intake must give retryable backpressure without claiming
custody. The small node is preferred when eligible, with one active job per
physical node and production ahead of waiting staging jobs. Existing input
bytes remain on Viewer until durable coordinator custody is confirmed.

As of this review, V2 writes every accepted source object to the coordinator
spool before worker claim, matching the approved first-release path. Direct
streaming to workers is deferred. Existing spool capacity, hash, publication
and restart checks must remain in force. Do not report processing until the
engine actually starts, or delete original inputs before durable handoff and
verified result retention requirements.

The last cluster root/Docker filesystem observation was approximately 113 GiB
free, a point-in-time value rather than a reservation. Recheck it and account
for queued source bytes, worker scratch, the coordinator database and a free
space floor immediately before admission. The local mount and recovery behavior
remain unqualified on the installed system.
