# Inactive started-transfer fence and lifetime pin

This is an offline migration and guarded-upload prerequisite experiment. It is
not enabled or wired into production. The migration refuses an existing upload
journal or an active job, installs the known upload schema and nine attested
SQLite triggers, and protects inserts/updates/deletes of the matching job and
Viewer task and file rows, including rebinding another task to the job, while an
upload journal row is `started`. It permits only exact running-to-held
transitions for coordinator restart or uncertain transfer, with identity and
claim unchanged; other operator holds and
progress updates are conservatively blocked. A restarted coordinator can thus
open and report the job held while retaining the started upload and shared slot.
A legacy started job with a null node binding cannot take the startup repair
transition and intentionally prevents startup pending inspection. The migration,
attestation and startup exception require the exact supported jobs schema; a
future schema change fails closed instead of silently widening the exception.
A killed upload leaves `started`, so a cooperating old transaction cannot
accidentally terminalize the job. The
migration requires an independent externally fenced idle coordinator; checking
SQLite alone is not an external fence.

`Queue.pin_transfer` retains the current process's coordinator lease while
allowing other threads to acquire the lifecycle mutex. The candidate bound upload
commits its started intent in a short transaction, retains NodeGate and the Queue
pin during the network exchange, then rechecks identity and completes both
journals in another short transaction. Synthetic tests pause HTTP and demonstrate
another Queue read and SQLite write transaction can proceed while conflicting
completion, task updates and Queue close are refused. A dropped response remains
held without replay. Pin acquisition consumes the operation deadline, although
releasing it can wait for the lifecycle mutex. The inactive candidate uses a
separate bounded per-file upload deadline (default 300 seconds, maximum 900),
while bounded control requests keep their shorter deadline. Timeout is not proof
that the remote engine rejected any bytes, so the durable hold is retained.

Bound uploads also require the dispatcher's exact in-process sender completion
object bound to the same socket and upload ID; the producer must report verified
EOF and exit before journal completion. This object is not an authentication
boundary against code in the same interpreter. The guard checks the named gate
directory and held lock inode before and after the exchange and rechecks node
maintenance state at completion. Detection does not prevent a malicious or
privileged writer from replacing and restoring a pathname between checks.

The fence is not protection against a writer able to update/delete the upload
journal, alter/drop triggers, replace the database or access the engine
directly. Existing legacy unbound code and older executables are not qualified.
Neither a timeout nor process death proves a NodeODM task stopped. The worker
must preserve the started journal and physical gate until separately reconciled;
there is no automatic retry, expiry, reset or operator bypass here. The pre-held
NodeGate reverses the normal lifecycle/gate acquisition order; NodeGate uses a
nonblocking lock, but concurrency and deadlock behavior require further review.
Synthetic interruption is not process death, real NodeODM, a remote broker or
power loss. Schema installation and tests make no change to live nodes.
