# Guarded node read candidate

This disabled, loopback-only experiment adds bound info, output, and archive
reads over the existing pinned mTLS transport. Each request presents the signed
job claim and is checked against the active node reservation and its durable
Viewer-job binding while the node gate is held. Until a matching committed
operation is durably acknowledged, info reports held, output is empty, and
archive download is refused. Archive access also requires NodeODM status 40;
bytes stream directly without a path supplied by the caller.

This is not production authorization or a delivery acknowledgment. The claim
verifier deliberately permits expired claims for inspection, but current
coordinator authority, revocation, job-scoped replay and coordinator epoch are
not proven. Direct NodeODM access and older writers remain unfenced. Archive
streaming does not by itself prove archive integrity, durable Viewer receipt,
or permission to clean up any source or result. The client methods are not
connected to ViewerService or any operational listener. Network or process
failure must retain work for reconciliation. Long downloads hold the cooperative
node gate for their duration; physical exclusivity remains a rollout gate.
