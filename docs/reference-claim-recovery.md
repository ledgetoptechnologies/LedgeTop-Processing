# Original-claim continuation after coordinator restart

`ClaimRecoveryService` is an optional, inactive coordinator component. It does
not claim another job, recreate an engine task, renew a transfer grant, release
the physical execution fence, or finalize delivery. Runtime configuration and
worker integration are separate acceptance gates.

Each acquired Queue ownership period has a new UUID. When the recovery service
is explicitly constructed before its HTTP listener, it records that epoch and
the fixed physical-node bindings in the same FULL-synchronous SQLite transaction
as its restart hold overlay. Reconstructing the API during that same ownership
period does not hold a running job again. Queue transactions retain the actual
coordinator OS lease through commit or rollback; closing from another thread
waits, while closing inside the current transaction is refused. Forked process
access is refused before acquiring an inherited mutex.

The optional worker API exposes these authenticated routes beneath
`/ltds/worker/v1/jobs/{jobId}`:

| Method | Route | Effect |
|---|---|---|
| GET | `/recovery-state` | Read current epoch, original claim and reservation digest, fixed physical binding and maintenance revision. |
| POST | `/recover` | Atomically resume only the original claim held for `coordinator_restart` and record the request receipt. |
| GET | `/recovery-receipts/{requestId}` | Read the historical receipt without changing execution state. |

All routes require the configured worker bearer identity and the original
`X-Claim-Token`. The token is never returned or stored in recovery requests or
receipts. Disabled recovery returns 404. Deployment must use the configured
verified HTTPS boundary; the loopback HTTP fixture tests only server routing.

The closed request contains version 1, a stable request UUID, coordinator epoch,
expected maintenance revision, the original eighteen-field parent binding,
original transfer-request SHA and worker-evidence SHA. Physical engine identity,
authority, coordinator gate origin, gate generation and output allowance come
from trusted configuration, not submitted overrides. Original job identity,
manifest, pipeline, worker incarnation, claim-token hash and reservation must
still match the stored claim. The only occupied global slot must be this job.

Only a processing-phase restart hold with no result or terminal error may
resume. Active, scheduled and draining node states permit the existing task to
continue; updating, failed and held node states refuse continuation. This path
does not refresh maintenance state or grant a new job during draining. Node
revision and recovery changes are checked within one write transaction.

The receipt and held-to-running transition commit together. Replaying the exact
same request returns its historical receipt with no status writes, including
after another restart or an operator hold. A different body under the same UUID
is refused. Every stored receipt is revalidated against its entire expected
shape and original claim. Recovery journal bytes count against metadata quota.

Worker evidence is an authenticated worker attestation, not remote hardware
attestation. Before requesting continuation, the worker must prove its actual
original private claim, daemon lease, physical fence and whole-parent scratch
reservation. After the response, it must obtain fresh `ProductionParentGuard`
authorization before any engine mutation. A receipt alone never grants current
permission. The worker must retain a durable intent before POST, resolve an
unknown response using receipt lookup, and distinguish later coordinator epochs.

Synthetic tests exercise actual Queue and ReferenceStore transactions, repeated
ownership changes, response loss, concurrent replay, maintenance revisions,
quota refusal, authority drift, receipt corruption and close-versus-commit
ordering. They do not prove installed runtime restart recovery, NAS custody,
native engine continuation or final Viewer acceptance. Those remain explicit
integration work.
