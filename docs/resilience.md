# Node ownership, orchestrator testing and outage recovery

The active coordinator owns both production and staging jobs; production is
selected first without interrupting running staging work. Candidate orchestrator
code uses simulated workers or a separately reserved physical node. Two queue
databases are never an acceptable way to coordinate one shared node.

This increment applies to the synthetic TreeDar worker. It retains the global
single execution slot; it does not activate parallel fleet execution, replace
ClusterODX, deploy to the cluster PC or install a background service.

## Host admission

Each coordinator state database has a stable random `authorityId`, exposed by
authenticated `GET /v1/prototype/authority` and included in job responses. An
independent test database gets a different ID. Keep the database durable; do not
clone production state/credentials into staging.

Before worker activation, an operator inspects the actual local Docker Engine ID
and coordinator authority, then provisions the host gate explicitly:

```powershell
python -m processing.node_gate --engine-id ENGINE_ID provision --node-id cluster --coordinator http://127.0.0.1:18080 --authority-id AUTHORITY_UUID
python -m processing.node_gate --engine-id ENGINE_ID status
```

Replace the uppercase values with independently verified identities. Provisioning
refuses to overwrite an existing binding. The gate lives under the OS user's
local state directory, keyed by Engine ID, independently of scratch and queue
paths. Worker and dispatcher commands require `--docker-engine-id ENGINE_ID` and
`--node-generation GENERATION` from the provisioned status. A changed Engine,
origin, authority, node ID or generation fails closed. No claim or engine run may
precede the durable reservation. A process lock prevents simultaneous workers;
the reservation survives process death and has no expiry.

This is a cooperative admission boundary, not protection against Docker
administrators. All writers must use the same OS account/state location and gate;
lock down the supervisor's environment and Docker access. Direct NodeODX,
ClusterODX, other users or an alternate state-home environment can bypass this
code. Fence those routes before real shared-node use. Initial provisioning is an
operator action, not evidence that existing workloads are idle.

An idle handover uses the gate's `handover` command with the exact current
generation, `--old-worker-stopped` and an inspection note. It increments the
generation, fencing stale invocations. Busy or uncertain reservations prevent
handover. For genuinely abandoned work, first inspect the exact worker/container
and resolve the remote claim; only then use explicit gate `resolve` with receipt
ID, expected generation, both stopped-worker and reconciled-coordinator
attestations and a note. This records abandonment, never successful completion.
No automatic election, lease expiry or replacement coordinator is enabled.

## Durable completion outbox

The processing worker owns its output files and a private receipt. It verifies
the container has stopped and output hashes match, flushes the artifacts, then
persists the exact completion payload before its first send. While pending,
neither another scratch root nor another coordinator can acquire that Engine's
gate. Output files are retained on both success and delivery failure.

The running foreground worker retries transient transport/server failures with
jittered exponential backoff, starting around one second and capped at five
minutes. Retry attempts and the next due time are durable. In-process pacing uses
a monotonic clock; restart delays are capped against wall-clock changes. It
retries until acknowledged by default. `--delivery-wait-seconds 0` makes one due
attempt; a positive value bounds the foreground wait. Permanent authentication,
conflict or invalid acknowledgment errors stop retries and preserve the receipt
for inspection. Credentials must be repaired without altering the original
completion payload; a blocked receipt needs explicit operator review.

After a worker process restart, run the same worker with the same Engine,
generation, origin, node and worker identity plus `--resume EXACT_RECEIPT_PATH`.
Resume delivers only the saved completion; it never claims or reruns a job.
Exact completed receipts also reconcile the crash window between the durable
acknowledgment and releasing the gate. A different reservation is never cleared.
Unknown claims or uncertain running containers require inspection, not resume.

Automatic process/boot restart supervision is not installed by this increment.
The operator must run/supervise the worker or resume command using persistent
local storage. RAM scratch cannot provide worker-reboot durability. A disk
failure still requires a separately verified backup/transfer strategy.

When the coordinator restarts, running jobs become held with reason
`coordinator_restart`; ownership and the original claim token remain unchanged.
The original worker may finish that exact claim. Other claims remain blocked
until it does. Explicit operator holds, including a hold applied after restart,
cannot be released by worker completion. An identical completion whose reply
was lost is acknowledged idempotently, including after another restart.

Completion currently delivers bounded metadata and output hashes, not artifact
bytes. A coordinator acknowledgment means the job record accepted the result;
it is not a TrueNAS artifact acceptance receipt. Do not automatically delete
worker output after this acknowledgment. Verified resumable artifact transfer,
parent/dependent jobs and NodeODX-to-TreeDar admission remain in
[the engine handoff plan](engine-handoff.md).

## Testing a candidate

1. Run queue, admission, maintenance and outbox tests with fake engines and
   separate temporary state. Outages, lost acknowledgments, restart-held
   completion, operator vetoes and two-authority contention require no photos.
2. Run real synthetic Docker integration only against an explicitly isolated
   test profile and small resource limits. The dispatcher smoke script provisions
   only its private test profile and removes its own fixtures; this is not proof
   of real shared-node deployment or a security boundary against another profile.
3. For shadow mode, configure a separate `PROCESSING_PROTOTYPE_OBSERVER_TOKEN` on
   the coordinator and supply it as `PROCESSING_OBSERVER_TOKEN` to
   `python scripts/orchestrator_shadow.py --coordinator ORIGIN`. Non-loopback
   origins also require matching `--verified-origin`. The observer can read
   prototype snapshots but cannot submit, claim, complete or maintain nodes.
   The command sends GETs only and prints an advisory decision with
   `executesWork: false`; separate snapshots are not a capacity reservation.
4. Promote by pausing dispatch, draining existing work and pending deliveries,
   stopping the old coordinator, backing up its state and starting the reviewed
   replacement against that same state. Verify authority/queue readback before
   resuming. The first version uses a drained handover, not rolling failover.

The existing deployed cluster prototype predates this increment. Upgrade and
provision it deliberately before using the new worker; legacy coordinators
without the authority API fail closed. Leave existing production containers
and routing untouched during candidate tests.
