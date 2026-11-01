# Disabled multi-authority capacity observations

This source experiment is not deployed, and all new protocol/coordinator entry
points are false-qualified. It is not a transfer admission grant, distributed
lease, physical disk reservation, quota, host-wide writer fence, or NAS service.

A coordinator with an owned private journal records a fixed per-attempt plan,
remote endpoint, distinct authority IDs and distinct TLS certificate pins before
contacting per-host authorities. A pinned mTLS session may reserve and later
re-observe the same fixed server plan. A lost reply leaves an intent that is
retried with identical attempt and plan; remote capacity holds remain uncertain
and are not automatically released. Sequential pin observations return only
`observed_only`, because one authority may change while another is being checked.
The local Unix variant verifies a same-UID peer and records a bound historical
receipt; same UID is not individual process or remote host authentication.

Linux local owner experiments hold O_PATH directory descriptors and compare
mount IDs, device and inode from `/proc/self/fdinfo` on checks. The subsequent
SQLite and storage operations still use pathnames, so this is detection at
observation points, not an atomic mount fence. Cleanup attempts to close all
owned descriptors and release the owner even if a close raises; behavior of an
actual kernel close error with uncertain FD state is not proven.

Validation uses synthetic local roots, private loopback TLS endpoints, synthetic
certificates and no NAS. Tests exercise two logical authorities on one physical
machine, distinct cert pins, second-authority lost reply and exact replay, fresh
pin calls, endpoint mutation rejection, and a separate coordinator child killed
with `os._exit` after one authority replied but before receipt persistence. The
parent then reacquires its lock and retries, seeing one reservation. This is not
host restart, separate physical machines, power-loss durability or proof of a
concurrent atomic multi-host barrier. Test listeners/threads and children are
owned, closed/joined and bounded; synthetic roots remain for inspection.

To qualify a transfer prerequisite, first implement and verify a provisioned
host-wide authority that mediates **every managed writer**, exact generation-
bound durable host hold attestations, held sessions or another atomic distributed
barrier across transfer, actual mount/FD-bound storage operations, safe writer
termination and downstream custody/cleanup, authenticated provisioned remote
identity and NAS owner participation. Existing direct ScratchLedger/NodeODM and
Docker/external writers bypass these sketches. Capacity budgets are estimates,
not enforced filesystem quotas; external usage or underestimated NodeODM scratch
can exhaust space. SQLite and filesystem work are not bounded by socket timeouts.
Do not enable or wire this code as a production scheduler.

## Disabled same-process managed writer experiment

`ManagedCapacityGate` signs a local host hold with a provisioned in-memory HMAC
key. It binds authority, attempt, job, generation, environment, input manifest,
pipeline, exact local worker ledger reservation and a digest of the currently
observed non-expiring cooperative capacity holds. These are synthetic local
permits; remote mTLS hold observations are not independently host-signed durable
attestations. Key provisioning, rotation, authentication and authority restart
policy are not implemented. `MANAGED_CAPACITY_QUALIFIED` remains false.

When explicitly injected into the reference worker and driver, the candidate
persists a deterministic local capacity attempt and signed permit before the
coordinator reservation request, reuses the same worker reservation on replay,
and requires the permit and local owner mutex during controlled input fetch and
worker start/poll/archive publication. The driver refuses managed TreeDar,
combined routing and automatic cleanup. Existing legacy behavior remains available
when no gate is injected; therefore this is not a host-wide managed mode and old
workers/direct NativeEngine, direct NodeODM and Docker/external writers can bypass
it. Protecting actual engine ingress and making every writer join one host-owned
capacity authority are mandatory before any production qualification.

For terminal handling, the controlled profile validates exact coordinator result
or failure receipts and node binding under the local permit. A per-job HMAC-signed
release intent is persisted before the node gate is released; pending release
intents block later managed starts until exact tombstone reconciliation and a
completed per-job release proof are durable. This lets a delayed Viewer success
receipt be validated after a later managed job replaces NodeGate's single recent
acknowledgment. Managed failure delivery and terminal Viewer receipt checkpoints
use corresponding local signed bindings. Viewer success acceptance remains an
exact receipt validation; none of these metadata receipts authorizes file deletion.

Twenty-nine selected bounded synthetic tests passed in this worktree, including
wrong/tampered permit holds, exact shared reservation and object recreation after
an accepted coordinator reservation response was lost, local fake-engine archive
and outbox, delayed Viewer acceptance after a second job, release-record tamper,
lost failure acknowledgment retry, and a *narrow* subprocess exit after direct
synthetic release-intent/NodeGate ACK. The process-death test does not run the
actual worker/outbox/Viewer flow. Tests use synthetic local files and loopback
services, and do not establish physical-host separation, power-loss durability,
real NodeODM or NAS behavior, 10,000-photo performance, or a complete outage
recovery guarantee. The candidate plan format changed and requires explicit
migration and rollback qualification before any persistent journal can be reused.

Remaining requirements include atomic/pinned mount operations, a provisioned
host-wide exclusive writer authority with no legacy ingress bypass, remotely
signed generation-bound host holds and an atomic or fenced multi-host barrier,
long-lived key lifecycle, bounded database and filesystem work, automatic
reconciliation of all crash boundaries, safe release/cleanup after downstream
custody, real per-filesystem quota or constrained-writer capacity safeguards,
and full 10k-photo manifest/inventory/working-space qualification. All tests
remain isolated; no live storage, services or production queue were modified.
