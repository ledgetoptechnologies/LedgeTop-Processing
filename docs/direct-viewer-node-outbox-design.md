# Direct Viewer node outbox: design only

The direct Viewer dispatcher assigns a node and engine UUID after claiming the
global queue, then sends engine initialization directly. The reference worker's
claim, ScratchLedger, NodeGate and result sender use a different submission
protocol; they do not currently own that direct task. A receipt-shaped JSON
object or a free-space check cannot establish shared authority, exclusive
execution or reserved space. Therefore this branch adds **no endpoint, worker,
dispatcher switch, result acknowledgement, or cleanup**. The offline
`direct_outbox_contract.validate_binding` function is a shape/hash test seam,
not a gate or proof of a reservation. It cannot make the live system safe.

## Required integration

1. The coordinator claims the global slot and durably records the Viewer UUID,
   generation, job ID, environment, selected node, a fresh engine UUID and a
   sealed digest of the exact ordered input names, byte counts and file hashes.
   The direct path does not currently persist that manifest digest. No engine
   init, upload or commit is sent until the following steps succeed.
2. An authenticated, node-bound service verifies the current coordinator
   authority and the node's exact engine identity and obtains the same physical
   NodeGate used by all job types and maintenance. It must retain the durable
   reservation after crashes. Existing direct dispatch and direct engine ingress
   bypass this gate; those paths must be fenced before this can be enabled.
3. The service reserves a **real, bounded output allocation** on the persistent
   node spool before acknowledging prepare. The guarantee must account for
   filesystem allocation, shared writers, delayed allocation, quota and power
   loss; a caller's budget or a free-space observation is not a reservation.
   If a conservative upper bound cannot be allocated, hold the job before
   engine init. Input/engine scratch capacity and output capacity are separate.
4. The node durably commits one immutable binding and reservation before it
   returns an authenticated, replayable prepare response. The coordinator
   durably saves that exact response before asking the node to start. A lost
   prepare/start response is reconciled by binding and generation; it is never
   interpreted as permission to generate another engine UUID or replay an
   uncertain init/commit. The eventual authenticated transport, principal and
   replay/expiry rules are not defined by this draft.
5. The node, rather than the coordinator, owns dispatch and polling under that
   reservation. It reconciles an uncertain init/upload/commit using the exact
   engine UUID and retains the execution fence when physical state is unknown.
   Cancellation status alone does not prove engine processes stopped. On
   success, it captures the archive in the allocated persistent spool, validates
   identity, length and content, fsyncs data and directory metadata, and seals
   a task-bound receipt before advertising completion. Incomplete, corrupt or
   ambiguously published files are held, not overwritten or deleted.
6. A persistent outbox retries the identical sealed completion through outages.
   A coordinator persistence receipt binds the full identity and bytes but is
   not Viewer custody. Retain input and output copies until a separate verified
   Viewer acceptance and explicitly qualified cleanup policy. This draft does
   not implement either acknowledgement or cleanup.

Startup must reconstruct local claims and unfinished receipts before admitting
new work. Fence identities, mount identities and capacity reservations must be
revalidated at each trust boundary. Lost reservation authority, disk pressure,
conflicting ownership, invalid engine state and ambiguous completion are holds.
Both production and staging use the same coordinator and physical gate; queued
production remains preferred, running work is not preempted, and small-node
preference and maintenance drain remain enforced by the existing authority.

The node agent cannot capture while the PC is off. The installed engines may
delete old results after their retention interval, including during startup;
an outage beyond that interval needs a separately reviewed preservation solution.
This design does not alter an engine image or bypass a blocked engine change.

## Qualification before integration

Exercise two actual isolated engine images and a real local agent with
synthetic data, exact authenticated identities, persistent spools and no live
services. Test enrollment/response loss at each write boundary, coordinator and
agent process death, replayed or mismatched credentials and engine UUIDs,
storage exhaustion and allocation failure, receipt fsync failure, network
outage, clock movement, duplicate reservation, restart with interrupted output,
node offline during maintenance and receipt replay. Prove the original files
remain until real Viewer custody; inject daemon/host restart and power-loss
boundaries separately. Test uncooperative direct submissions and fail closed
until the ingress/gate boundary really excludes them. Qualified installer,
upgrade/rollback and trusted disk ownership are prerequisites for activation.
