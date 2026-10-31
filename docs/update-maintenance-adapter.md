# UpdateSupervisor maintenance adapter

The actual main-PC owned schedule, managed update, durable pin, ingress reopen,
coordinator finish and restarted reconciliation passed on October 28 using source
`671fd2a`. See [runtime evidence](evidence/main-owned-update-20261028.json).
This was one explicitly supervised operation; no recurring service is installed.

`ops.update_maintenance_adapter.UpdateMaintenanceAdapter` connects the source
`UpdateSupervisor` protocol to the real coordinator owned-schedule endpoints and
`ops.node_maintenance.run`. It is a Linux library, with no installed detector,
supervisor loop, timer, image pull, qualifier, or deployment entry point.

## Composition

Construct it with an existing private managed maintenance root, a dedicated
private SQLite `state_path`, and `receipt_root=supervisor_path.parent/'receipts'`.
Supply trusted `processing.odx_smoke_worker.Client` instances as `api` and
`operator`: distinct credentials for the same fixed loopback coordinator origin.
Also supply fixed `node_id`, canonical `authority_id`, Docker `engine_id`, and
`adapter_id`, plus an explicit `ScheduleWindow(drain_after_seconds,
not_before_after_seconds, wait_seconds, drain_seconds)`. The first two offsets
are relative to the first scheduling attempt; `notBefore` cannot precede drain.
The injected clock, when used, must return an aware datetime.

The adapter's public `binding` is the exact four-field supervisor binding:
`adapterId`, `authorityId`, `nodeId`, `engineId`. Its methods are
`inspect_host()`, `schedule(request)`, `lookup_schedule(request)`,
`ready_to_run(request)`, `run(request)`, and `lookup_completion(request)`.
Requests and sealed plan/completion receipts use the existing supervisor
protocol, including `hostGeneration` and mandatory `localImageId`. Each request's directory must already be
the private exact `receipts/{operationId}/{schedule|run}` supplied by the
supervisor. Arbitrary operation directories are refused.

The journal freezes the managed root, storage identity, authority, node, engine,
adapter identity, coordinator origin, window, maintenance settings, gateway
configuration, and Compose content with only the selected service image omitted.
This permits a later completed update cycle to adopt the new pinned image while
preserving all other settings. Configuration changes require an explicit new
adapter binding, not silent adoption. Credentials are never exported or stored
by the adapter; mutation of its live client objects is refused. A new process
may supply rotated credentials for the same configured roles and origin.

`inspect_host` verifies the coordinator authority, Compose service/container and
local Docker engine identity, actual private backend image, authoritative Compose
image pin, fixed expected NodeODM dialect/layout, serving health, and ingress
identity. It returns only `nodeId`, `authorityId`, `engineId`, and `imageId`.
`configDigest` is the registry manifest's configuration blob, while `localImageId`
is the actual verified Docker runtime ID supplied by staging. They can differ
on containerd-backed Docker. The adapter uses only `localImageId` for cached
runtime image checks, coordinator targets, replacement, Compose pins, and
completion comparisons, while preserving both identities in sealed receipts.
Missing `localImageId` is rejected; no config-digest fallback is permitted.
The existing managed configuration supplies backend port/token and explicit
`odx` or `odm` layout; this adapter introduces no resource limits or overrides.

## Scheduling and readiness

Before the first operator POST, a FULL SQLite transaction persists the exact
owned request UUID, authority, revision, target, drain time, not-before time,
and supervisor request. The POST is
`/v1/prototype/nodes/{nodeId}/schedule-owned`; proof is the operator-only GET
of that path plus `/{requestId}`. Receipt hashes, immutable request binding,
current plan, authority, target, and scheduling fields are checked before sealing
the supervisor plan. See [owned scheduling](owned-maintenance-schedule.md).

An interrupted POST is never issued again, even when no response reached the
client. A repeated `schedule` call or `lookup_schedule` reads only the exact
owned receipt. A missing or unavailable receipt holds the operation; a matching
image on the node is not ownership evidence. Concurrent callers are serialized
by the unique durable intent before any POST.

`ready_to_run` performs readbacks and an ephemeral physical NodeGate lock only.
It writes no run intent or maintenance receipt, closes no ingress, and changes
no host state. A future window, node-bound active job, busy engine, gate lock,
or occupied gate returns `False`. Invalid ownership, changed host/plan, or
unavailable evidence raises bounded `MaintenanceAdapterHeld`. The supervisor
keeps normal waits scheduled without a run request and rechecks the host after
readiness. Readiness remains advisory: the actual managed operation fences the
exact plan and target again using coordinator CAS.

## Execution and recovery

`run` commits a one-shot FULL run intent before calling `node_maintenance.run`
with the frozen `expected_plan_id`, `expected_target`, supplied receipt directory,
and configured wait/drain bounds. The managed updater reserves the exact plan
before closing ingress, checks final idle after draining, then performs its
existing replacement/finalization sequence. See
[managed finalization](managed-update-finalization.md).

A success receipt requires completed private managed and low-level receipts,
the exact plan/input/target/binding, authoritative successful finish readback,
the actual healthy target image and durable Compose pin, and the exact owned
open-ingress proof. The supervisor receipt contains no private update token,
credentials, filesystem paths, or Docker command arguments.

`lookup_completion` uses read-only SQLite and private receipt reads plus
read-only coordinator, Docker, health, and ingress checks. It never calls
`run`, finalizes a coordinator plan, writes a pin, reloads ingress, or modifies
an incomplete receipt. It can reconstruct a completed receipt after a lost
return response without repeating a replacement. Changed or unavailable proof
holds; target image identity alone cannot prove completion.

Any issued run lacking full success proof remains held, including an unexpected
`waiting` or `not_eligible` result after the one-shot run intent. There is no
automatic retry of that run. A race after readiness, uncertain begin/replace,
or unfinished pin/open/finish therefore requires the existing explicit managed
reconciliation/inspection procedure before a supervisor can receive completion.
This conservative boundary prevents an unattended replay from mutating a peer
plan or repeating Docker actions.

## Verification scope

`tests.test_update_maintenance_adapter` uses actual local coordinator HTTP and
SQLite owned scheduling, and the actual managed updater with synthetic Docker
and ingress boundaries. It covers exact receipts/privacy, lost scheduling and
completion responses, restart readback, future/busy waits, changed plans,
one-shot waiting/uncertain outcomes, and frozen request/config rejection.
These are source tests; they do not qualify an image or install unattended
updating on either processing host.

The prototype job inventory check rejects malformed responses, unknown statuses,
and running or held jobs without a node ID. A syntactically valid but incomplete
inventory is not detectable by this check. It covers prototype jobs only: direct
NodeODM activity and work appearing after the inventory read require the separate
engine idle, ingress-drain and node-gate protections. This check alone is not a
physical fence or proof that every process has stopped.
