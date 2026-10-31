# Private parent-claim observations

`GET /ltds/worker/v1/jobs/{jobId}/claim-state` is an additive private worker
endpoint. It requires protected transport, the existing configured worker bearer
and `X-Claim-Token`. The bearer fixes node, worker and instance; requests cannot
select those identities or the production/staging environment. Request bodies
and query controls are not accepted.

The response is a closed `ltds-parent-claim-v1` observation containing:

```text
schemaVersion, protocol, authorityId, jobId, generation, environment,
nodeId, workerId, workerInstanceId, claimRequestId, claimTokenSha256,
manifestSha256, pipelineDigest, status, executionPhase, heldReason
```

It returns neither the plaintext claim token nor the input manifest, grants,
filesystem paths or result payload. The store reads coordinator identity,
reference metadata, queue ownership and the exact claim in one SQLite read
transaction. Inconsistent ownership, token hash or submission digests are
refused. Repeated observations do not refresh node maintenance, replay a claim,
advance a phase, update timestamps or rewrite the database.

The snapshot is evidence for the real host's parent guard, not authorization by
itself. Before a new engine mutation, that guard must compare the complete
expected identity, require a running parent in its expected processing phase,
and independently verify the actual scratch reservation and retained physical
gate receipt under the host's owner lease. A planned maintenance drain may let
the already claimed whole pipeline finish; it must not cause a second claim or
an intermediate gate release.

After a coordinator restart the observation preserves the original execution
phase and claim identity while reporting `status=held` and
`heldReason=coordinator_restart`. Read-only engine reconciliation may use that
evidence. It does not permit another engine mutation. Operator-held and terminal
states are also observations; neither implies file acceptance or cleanup.

Older coordinators return 404 for this new endpoint. A new combined worker must
wait for the reviewed coordinator API version; it must not fall back to POSTing
the claim route to obtain a snapshot. Runtime deployment, concrete host guards,
intermediate adapters and combined capability advertisement remain separate
integration and acceptance work.

Synthetic tests use the actual Queue, ReferenceStore, private worker API and
loopback HTTP listener. They cover exact identity and token separation,
cross-worker/instance/node refusal, corrupt binding refusal, byte-identical
database reads, real coordinator restart, operator hold/resolution and HTTP
transport. No runtime worker, credential grant, production job or NAS change is
introduced by these tests.
