# Host update supervisor

## Actual main-PC acceptance — October 28, 2026

The previously startup-qualified operation `1157e072` completed the actual owned
maintenance workflow on the main PC using source `671fd2a`. One owned schedule
and one managed run drained the node, replaced image `84117734` with immutable
image `b9d9a1a4`, verified readiness, saved the Compose pin, reopened ingress and
confirmed the coordinator finish. The supervisor adopted the image at generation
one. A restarted inspection and completed reconciliation returned the same sealed
completion without replaying the update. Independent read-only review accepted
the journal/receipt chain, host state and coordinator acknowledgement.

Both GPUs remain visible; CPU, memory, PID and other container resource settings
are unchanged. Existing data mount and monitoring services remain in place. The
node is active, its engine task list is empty, and all 12 historical coordinator
jobs are unchanged. The old image remains cached for rollback. This validates the
idle container-update pipeline under the explicit startup/API/options-only policy;
no real imagery, reconstruction or GPU kernels were tested. No recurring updater
service or timer is installed. See the [runtime evidence](evidence/main-owned-update-20261028.json).

## Source and API

`processing/update_supervisor.py` connects the existing `DockerRegistry` to
explicit local qualification and maintenance adapters. It installs no daemon,
timer, approved channel, credential, qualifier, or deployment adapter. Importing
it performs no work. Tests use a real registry component with a fake Docker
transport, SQLite adapter receipts, and synthetic temporary host state. No live
image, NAS, coordinator, ingress, or running container is changed by these tests.

## API and ownership

Construct a frozen `SupervisorPolicy(node_id, channel, current_image_id,
authority_id, qualifier_id, qualification_policy_sha256,
maintenance_adapter_id, capacity_bytes)` and
`UpdateSupervisor(path, policy, registry=..., qualifier=..., maintenance=...)`.
The registry must be an actual `DockerRegistry`, with the exact configured
channel and capacity reservation. The qualifier and maintenance bindings must
match the policy, registry Docker Engine identity, physical node, and sole
coordinator authority. There are no default production selectors.

Use the supervisor as a context manager. It owns one fixed private OS-exclusive
lease beside its SQLite journal. The binding records the immutable policy and
registry configuration/path plus lease/root device and inode. Same-process and
subprocess competitors fail. Replaced, symlinked, hardlinked, foreign-owner, or
publicly accessible lease/journal paths fail closed. An inherited child cannot
use or close its parent's session. Close waits for another thread's operation;
a reentrant callback cannot close or start another operation. The lease file and
journals are retained, including after uncertain operations.

- `discover()` inspects only the frozen channel and records one operation, or
  returns `None` only when registry read-only cached-image verification proves
  that the candidate's actual local Docker image ID is already adopted.
- `advance(operation_id)` attempts one next stage: stage, qualify, schedule, run.
  Each external mutation has a FULL SQLite intent committed first.
- `reconcile(operation_id)` uses only registry inspection or an adapter's
  read-only receipt lookup. It never repeats a possibly issued mutation.
- `status(operation_id)` returns bounded local state, candidate, sealed plan and
  completion; no credentials or update tokens belong in these records.

The configured current image is the initial baseline. A verified, finalized
completion atomically adopts the new image and increments a durable host
generation. Operation identity binds that generation and previous image as well
as the immutable candidate. A long-lived A→B→C→A→B release sequence therefore
has a fresh final A→B operation. Registry artifact/pull identities remain
separate, so a prior staged immutable image needs no new pull. No new cycle is
allowed while an earlier operation is unresolved, regardless of tag,
descriptor, attestation, or candidate-ID changes.

## Required adapter contracts

`Qualifier.binding` is exactly `{qualifierId, policySha256}`. It implements
`qualify(request)` and read-only `lookup(request)`. The fixed request binds the
operation, candidate/manifest/config, host generation/previous image, physical
engine/node, coordinator authority, supervisor and qualification policies, and
an owned qualification receipt directory. Its `requestSha256` excludes itself.

The closed successful qualification receipt has `schemaVersion:1`,
`requestSha256`, `operationId`, `nodeId`, `engineId`, `authorityId`, `candidateId`,
`manifestDigest`, `configDigest`, `localImageId`, `supervisorPolicySha256`, `hostGeneration`,
`qualifierId`, `qualificationPolicySha256`, `passed:true`, bounded
`engineVersion`, `evidenceSha256`, and `qualificationSha256`. The final field is
the canonical SHA256 of all other receipt fields. `seal_receipt(value, field)`
is provided for adapters. No failed, stale, changed-host, or changed-policy
receipt authorizes scheduling.

A real processing qualifier must independently acquire the same physical/global
execution admission used by production processing, or run qualification on an
isolated off-host executor. `--network none` does not prevent concurrent resource
use. This supervisor does not implement that admission. Before enablement, the
qualifier needs reviewed immutable fixture/evidence policy, actual engine and
platform verification, bounded execution/owned-output handling, and durable
lookup semantics. It must not pretend NodeODM and NodeODX are interchangeable.

`MaintenanceAdapter.binding` is exactly `{adapterId, authorityId, nodeId,
engineId}`. Required methods are `inspect_host()`, `schedule(request)`,
`lookup_schedule(request)`, `ready_to_run(request)`, `run(request)`, and
`lookup_completion(request)`.
`inspect_host()` returns exactly `{nodeId, authorityId, engineId, imageId}`;
the last two lookup methods are read-only. Schedule receives the accepted
qualification SHA. Its closed plan receipt contains the qualification receipt's
common host/candidate/policy/request fields, `planId` (canonical UUID),
`previousImageId`, `targetImageId`, `qualificationSha256`, and self-excluded
`planSha256`. The schedule request has its own fixed owned receipt directory.

The run request additionally contains that exact sealed plan and a fixed owned
run receipt directory. Completion has the common fields plus `planId`,
`planSha256`, `previousImageId`, `targetImageId`, `qualificationSha256`, `imageId`,
`healthy:true`, `hostPinDurable:true`, `ingressOpen:true`,
`coordinatorFinished:true`, and self-excluded `completionSha256`. The physical
host must independently report the exact target image. A completion claiming
only container health or replacement is insufficient. Unknown or failed
qualification, schedule, replacement, rollback, pin, ingress, or coordinator
finish outcomes remain held; there is no automatic replacement or rollback
retry in this component.

Before recording a run intent, the supervisor calls the read-only
`ready_to_run(request)` probe. It must return an exact boolean: `False` leaves
the operation scheduled without a run request, so a future window or busy node
can wait across restart. Exceptions and invalid results also cannot issue a run.
After `True`, the supervisor rechecks its ownership and the current host image
before committing the run intent. Readiness is a point-in-time observation;
managed run must still reserve the exact current plan before touching ingress.

## Sole-coordinator integration boundary

An adapter must use the existing sole coordinator's maintenance authority and
the existing managed local-container update/finalization behavior. It must not
create another scheduler, choose a peer plan, update the sole coordinator as if
it were a processing node, or release unrelated gates. It must preserve main's
processing resource settings. `manifestDigest` identifies the selected registry
manifest and `configDigest` identifies its configuration blob. Neither alone
predicts Docker's actual runtime ID: classic storage may report the config ID,
while containerd-backed storage may report the manifest ID. Staging must verify
the actual local ID against immutable cached artifact evidence. Its metadata
contains `configDigest`, `imageId`, and `imageIdKind` (`config` or `manifest`).
The supervisor derives mandatory request/receipt `localImageId` from that
persisted `imageId`. All target/current/previous runtime pins, schedule targets,
qualification containers, and completion image checks use actual local IDs;
`configDigest` remains artifact identity. A matching digest guess never skips
staging or proves adoption.

New registry discovery candidates use schema version 2 and omit the old guessed
`localImageId`; legacy version 1 candidate evidence can remain readable in held
registry journals, but its guessed ID is never runtime proof. Old supervisor
staged metadata or issued requests without the mandatory runtime identity are
refused rather than silently reinterpreted. Such never-deployed supervisor
journals require explicit inspection and a fresh bound journal, with uncertain
operations retained; this is not an automatic journal migration.

The real scheduling adapter must persist its exact request ID, dynamic
timestamps, expected coordinator revision, and complete schedule body before a
POST. A lost response requires a durable owned-request receipt lookup. Plain
node GET with a matching target does **not** prove ownership. The legacy
maintenance schedule API generates a plan UUID without caller correlation;
that API alone cannot satisfy this recovery contract. The source now provides
an [owned scheduling endpoint](owned-maintenance-schedule.md), but it must be
installed and connected by the production adapter before use. Before run, the adapter must verify the
coordinator still exposes its exact frozen plan/revision and not a later peer
plan. Run and completion lookup must use only the supplied own receipt directory
and actual managed finalization evidence; credentials/update tokens stay private
inside the adapter's existing receipts.

Concrete source adapters are now available: the
[maintenance adapter](update-maintenance-adapter.md) connects owned scheduling
and managed finalization, and the [startup qualifier](update-qualifier.md) checks
only isolated startup/API/options behavior. `tests/test_update_pipeline.py`
composes these with real journals, coordinator HTTP and the physical gate, using
synthetic Docker/ingress transports. This does not prove a real image upgrade.

Runtime composition/installation, approved policies/channels/windows,
sole-coordinator authentication and end-to-end host acceptance remain required.
Heavy processing qualification additionally needs the same global/physical
execution admission and valid fixtures. The installed coordinator lacks the new
owned-scheduling API; its [reference-to-reference update path](reference-coordinator-update-plan.md)
must be completed before enabling the maintenance adapter there. No unattended
runtime installation or live automatic update availability is claimed.
