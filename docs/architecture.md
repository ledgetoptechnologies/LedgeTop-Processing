# Processing architecture, first increment

## Deployment and ownership

```text
Ops UI -> Viewer API + durable client/job records on TrueNAS
                    |
                    | one authenticated processing endpoint
                    v
           cluster PC: execution coordinator
                    |                         \
          local engine (small jobs)     main PC engine (large jobs)
                    \                         /
                     -> verified result -> TrueNAS
```

The cluster PC and the staging PC are separate computers. The cluster PC (an
older 3rd-generation Intel Core i7 with 48 GB RAM) runs the coordinator and
processes both production jobs and staging test datasets. Its Docker service
and roughly 1 TB SSD are for orchestration and bounded scratch space. The
staging PC (a Lenovo ThinkCentre M720q) is dedicated to Engineering Hub staging,
CI and deployment tests; it is not a processing node.

The main processing PC returned from repair on October 26 and is reachable as
`node` at `192.168.50.88`: Threadripper PRO 3955WX, 16 cores/32 threads and about
252 GiB RAM. Its initial synthetic worker enrollment uses the existing cluster
coordinator; a separate main-PC orchestrator is not started. Real workload
admission and large-job capacity still require validation. The SSD must
not be the only copy of either a source dataset or a completed product.

For the first release, the Viewer remains the authority for projects, datasets,
client permissions, task submissions, and published outputs. The coordinator
owns execution leases, node selection, engine status, and temporary-copy
cleanup. Its execution ID must map to a Viewer attempt ID. This avoids two
independent systems claiming to own the same client job. Moving the Viewer job
state later requires an explicit data migration and rollback plan.

## Job lifecycle

1. The Viewer records a request using its existing idempotent submission path.
   A source manifest identifies the immutable dataset and expected files,
   byte sizes, and hashes. A retry uses the same attempt identity.
2. The coordinator records the attempt before acknowledging it. It validates
   the requested engine and node capabilities, reserves enough scratch space,
   and selects a healthy node. Assignment is durable so a restart cannot
   quietly start the same expensive job twice.
3. The node receives the input by an authenticated, bounded transfer. Every
   file is checked against the manifest. No node receives unrestricted TrueNAS
   filesystem access or a reusable client download URL.
4. The coordinator polls or receives engine progress and records the engine
   task ID. Cancellation and retry are idempotent and reconcile with the engine
   before another attempt is started.
5. Outputs return to TrueNAS. The Viewer verifies archive integrity, required
   products, and provenance before publishing them. The coordinator receives
   an explicit acceptance receipt. Only then may it remove temporary inputs
   and outputs, with a retention window for recovery.

The first transport should reuse the Viewer's existing provider upload and
result-download contract where it fits. Direct TrueNAS-to-node mounts and
automatic deletion are deferred until the trust and recovery behavior is
specified and tested.

## Node selection

- `local` is eligible only when healthy, idle or within its configured slots,
  able to run the selected engine, below its verified photo/byte/RAM limits,
  and able to reserve scratch space. The current 1,000-photo ceiling still needs
  benchmarking. Small jobs wait for local while it is busy or unavailable.
- `main` is selected for jobs above the local limit or requiring capabilities
  unavailable locally. If `main` is down, the job remains pending with a clear
  reason. It must not overflow to `local` merely to clear the queue.
- An operator may explicitly override the preferred node within hard resource
  limits. Every choice and override is recorded with the policy version.
- Engine adapters publish comparable capabilities and health, but have their
  own input, output, progress, cancellation, and cleanup methods.

With a configured node inventory, one job runs per node. The scheduler scans
production before staging and gives each node the earliest queued job eligible
for it, in configured node order, so a 5,000-photo job waits for a large node
without blocking later work a small node can run. A job passes an earlier node
only when that node is occupied, observed busy or not ready, or proven unable to
run it by configured capability, configured photo cap, or known input bytes
against a current scratch observation; a node-reported cap alone proves nothing.
A node in an unknown state (probe in flight, failed, or no observation) holds any
job it might run from later nodes for one readiness TTL after its last valid
probe, then counts as unavailable until it answers. Later nodes may idle for that
TTL, which is the cost of not sending small work to a large node on a routine probe.

Staging may run beside queued production only on a node that no queued
production job can use. Each such job must be proven unable to run there by the
same evidence: configured capability, configured photo cap, or, for a reference
job, hash-verified input bytes against a current scratch observation. Unknown,
missing or tampered evidence proves nothing and blocks staging, and a reported
cap alone is not proof. So a staging job can run on an idle cluster while a
5,000-photo production job waits for main, and production still claims main when
main is free. Any production job that can run on a node takes it first. A node
also waits while a production reference job assigned to another node lands.
Reference and Viewer claims recheck this in the claim transaction. A running
production job keeps only its own node, so staging can use another idle node
that no queued production job can use. Held production still blocks staging on
every node until it is resolved.

ClusterODX already handles a NodeODX-compatible single address, node health,
queue slots, and `maxImages`. We can use it behind an ODM adapter if its actual
responses pass the current Viewer's probe and task contract. Its capacity-based
routing alone does not prove the desired local-first rule; do not assume that
a photo ceiling alone makes all smaller jobs land on `local`. The native
coordinator now applies explicit small-node-first policy at its atomic claim.

## Security and API boundary

The first endpoint is private to the LAN or an authenticated tunnel. It uses
separate machine credentials for the Viewer and each processing node. Tokens
are scoped and rotatable; no client username/password is passed to an engine.
Never expose an engine administration or ClusterODX telnet port to clients.
Dataset and result IDs remain tenant-bound in the Viewer. A future partner API
needs tenant-scoped credentials, quotas, audit records, and short-lived result
handles before it is opened outside the internal network.

## Implementation sequence

1. Capture the current Viewer's provider contract (`/info`, `/options`, task
   initialization/upload/commit/status/output/cancel/download) as fixtures.
   Test current NodeODM and pinned NodeODX/ClusterODX image responses against
   it. Confirm engine options and required Viewer outputs.
2. Implement a Dockerized coordinator with durable execution records, health,
   policy-driven local/main routing, and a NodeODX adapter. Keep its management
   API separate from any NodeODX-compatible facade.
3. Run a small internal dataset end to end and verify result return, restart
   recovery, cancellation, and cleanup after the Viewer acceptance receipt.
4. Benchmark local limits, then bring the main PC online and test unavailable,
   saturated, and failover states. Only then switch the Viewer's provider URL.
5. Add OpenSPLAT and TreeDar adapters after their input/output contracts and
   Viewer display or delivery paths are verified.

TreeDar's existing local CLI and worker source are in `engines/treedar`.
Their presence in this repository does not implement a coordinator adapter;
the routing probe's engine contract remains unchanged by the code migration.

No live deployment, DNS, TrueNAS configuration, or existing Viewer provider
setting is changed by this document.
